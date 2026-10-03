const std = @import("std");
const assert = std.debug.assert;

const event = @import("event.zig");
const limits = @import("limits.zig");
const vc_mod = @import("vc.zig");

const Io = std.Io;

pub const capacity_default = 256;

pub const Command = struct {
    payload: Payload,
    outcome: *Outcome,
    done: *Io.Semaphore,

    pub const Payload = union(enum) {
        ping,
        vc_summary: struct {
            out: *[vc_mod.entry_max]vc_mod.Entry,
            out_len: *usize,
        },
        accumulate: struct {
            value: u64,
            total: *u64,
        },
    };
};

pub const Outcome = union(enum) {
    done,
    failed: anyerror,
};

// NOTE: Vyukov bounded MPMC ring: a cell is writable at seq == pos, readable at seq == pos + 1.
const Cell = struct {
    seq: std.atomic.Value(u64),
    command: Command,
};

pub const SubmitError = error{QueueFull};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    store: *@import("store.zig").EventStore,
    io: Io,
    cells: []Cell,
    mask: u64,
    work: Io.Semaphore,
    enqueue_pos: std.atomic.Value(u64),
    dequeue_pos: std.atomic.Value(u64),
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool),

    pub fn init(
        allocator: std.mem.Allocator,
        store: *@import("store.zig").EventStore,
        io: Io,
        capacity: usize,
    ) !Owner {
        assert(capacity > 0);
        assert(capacity <= capacity_default);
        const cells = try allocator.alloc(Cell, capacity);
        errdefer allocator.free(cells);
        for (cells, 0..) |*cell, index| {
            cell.* = .{ .seq = .init(@intCast(index)), .command = undefined };
        }
        return .{
            .allocator = allocator,
            .store = store,
            .io = io,
            .cells = cells,
            .mask = capacity - 1,
            .work = .{ .permits = 0 },
            .enqueue_pos = .init(0),
            .dequeue_pos = .init(0),
            .stopping = .init(false),
        };
    }

    pub fn deinit(self: *Owner) void {
        self.allocator.free(self.cells);
    }

    pub fn start(self: *Owner) !void {
        if (self.thread != null) return;
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    pub fn stop(self: *Owner) void {
        if (self.thread) |thread| {
            self.stopping.store(true, .release);
            self.work.post(self.io);
            thread.join();
            self.thread = null;
        }
    }

    /// Blocks until the owner finishes; results are in the caller's buffers on return.
    pub fn submit(self: *Owner, command: Command) SubmitError!void {
        var spins: usize = 0;
        const pos = blk: {
            while (true) {
                const pos = self.enqueue_pos.load(.acquire);
                const cell = &self.cells[pos & self.mask];
                const seq = cell.seq.load(.acquire);
                const diff = @as(i64, @bitCast(seq)) -% @as(i64, @bitCast(pos));
                if (diff == 0) {
                    if (self.enqueue_pos.cmpxchgWeak(
                        pos,
                        pos + 1,
                        .acq_rel,
                        .acquire,
                    )) |observed| {
                        _ = observed;
                        spinBackoff(&spins);
                        continue;
                    }
                    break :blk pos;
                }
                if (diff < 0) return error.QueueFull;
                spinBackoff(&spins);
            }
        };

        const cell = &self.cells[pos & self.mask];
        cell.command = command;
        // NOTE: release pairs with the owner's acquire load.
        cell.seq.store(pos + 1, .release);
        self.work.post(self.io);

        // WARN: the cell may already be reused; touch nothing in it after the wait.
        command.done.waitUncancelable(self.io);
    }

    fn run(self: *Owner) void {
        while (true) {
            if (self.stopping.load(.acquire)) return;
            self.work.waitUncancelable(self.io);
            if (self.stopping.load(.acquire)) return;

            const pos = self.dequeue_pos.load(.acquire);
            const cell = &self.cells[pos & self.mask];
            const seq = cell.seq.load(.acquire);
            if (seq != pos + 1) continue;

            self.execute(&cell.command);

            // NOTE: free the cell before waking; the requester never reads it again.
            cell.seq.store(pos + self.cells.len, .release);
            self.dequeue_pos.store(pos + 1, .release);
            cell.command.done.post(self.io);
        }
    }

    fn execute(self: *Owner, command: *Command) void {
        switch (command.payload) {
            .ping => command.outcome.* = .done,
            .vc_summary => |op| {
                const entries = self.store.vc_summary();
                const count = @min(entries.len, op.out.len);
                @memcpy(op.out[0..count], entries[0..count]);
                op.out_len.* = count;
                command.outcome.* = .done;
            },
            .accumulate => |op| {
                op.total.* += op.value;
                command.outcome.* = .done;
            },
        }
    }
};

fn spinBackoff(counter: *usize) void {
    counter.* += 1;
    if (counter.* < 16) {
        std.Thread.yield() catch {};
        return;
    }
    std.Thread.yield() catch {};
}

test "owner accumulates concurrent submissions exactly" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try @import("store.zig").EventStore.init(
        allocator,
        "data/test_owner_accumulate.wal",
        io,
        .{},
    );
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_owner_accumulate.wal") catch {};
    }

    var owner = try Owner.init(allocator, &event_store, io, capacity_default);
    defer owner.deinit();
    try owner.start();
    defer owner.stop();

    var total: u64 = 0;
    var outcomes: [8]Outcome = undefined;
    var dones: [8]Io.Semaphore = undefined;
    var workers: [8]std.Thread = undefined;

    for (&dones) |*done| done.* = .{ .permits = 0 };

    const Worker = struct {
        fn run(o: *Owner, t: *u64, outcome: *Outcome, done: *Io.Semaphore) void {
            var i: usize = 0;
            while (i < 250) : (i += 1) {
                o.submit(.{
                    .payload = .{ .accumulate = .{ .value = 1, .total = t } },
                    .outcome = outcome,
                    .done = done,
                }) catch @panic("queue full in test");
            }
        }
    };

    for (0..8) |index| {
        workers[index] = try std.Thread.spawn(.{}, Worker.run, .{
            &owner, &total, &outcomes[index], &dones[index],
        });
    }
    for (&workers) |*worker| worker.join();

    try std.testing.expectEqual(@as(u64, 8 * 250), total);
    inline for (&outcomes) |*outcome| {
        try std.testing.expectEqual(Outcome.done, outcome.*);
    }
}

test "owner answers ping and vc_summary through caller buffers" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_owner_summary.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_owner_summary.wal") catch {};

    var event_store = try @import("store.zig").EventStore.init(
        allocator,
        "data/test_owner_summary.wal",
        io,
        .{ .identity = "svc", .vc_node_key = vc_mod.nodeKey("n1") },
    );
    defer event_store.deinit();

    var owner = try Owner.init(allocator, &event_store, io, 16);
    defer owner.deinit();
    try owner.start();
    defer owner.stop();

    var outcome: Outcome = .{ .failed = error.Unset };
    var done: Io.Semaphore = .{ .permits = 0 };
    owner.submit(.{ .payload = .ping, .outcome = &outcome, .done = &done }) catch
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(Outcome.done, outcome);

    const append_events = [_]@import("protocol.zig").AppendEvent{
        .{ .event_type = "e", .schema_version = 1, .payload = "x" },
    };
    _ = try event_store.append_stream(.{
        .stream_id = "s",
        .expected_version = .empty,
        .command_id = "ownercmd-1",
        .events = &append_events,
    }, 1);

    var entries: [vc_mod.entry_max]vc_mod.Entry = undefined;
    var entries_len: usize = 0;
    outcome = .{ .failed = error.Unset };
    owner.submit(.{ .payload = .{ .vc_summary = .{
        .out = &entries,
        .out_len = &entries_len,
    } }, .outcome = &outcome, .done = &done }) catch
        return error.TestUnexpectedResult;
    try std.testing.expectEqual(Outcome.done, outcome);
    try std.testing.expectEqual(@as(usize, 1), entries_len);
    try std.testing.expectEqual(vc_mod.nodeKey("n1"), entries[0].node);
    try std.testing.expectEqual(@as(u64, 1), entries[0].counter);
}
