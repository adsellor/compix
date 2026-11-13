/// WARN: the ownership is messed up a bit.
///
const std = @import("std");
const assert = std.debug.assert;

const event = @import("event.zig");
const limits = @import("limits.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const stream_store_mod = @import("stream_store.zig");
const vc_mod = @import("vc.zig");

const Io = std.Io;
const EventStore = store_mod.EventStore;
const SubscriptionQuery = stream_store_mod.StreamStore.SubscriptionQuery;
const SubscriptionPage = stream_store_mod.StreamStore.SubscriptionPage;

pub const capacity_default = 256;
pub const capacity_max = std.math.ceilPowerOfTwoAssert(u32, limits.connections_max);

comptime {
    assert(std.math.isPowerOfTwo(capacity_default));
    assert(capacity_max >= limits.connections_max);
}

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
        subscription_page: struct {
            query: SubscriptionQuery,
            output: []protocol.ReadEvent,
            output_bytes: []u8,
            page: *SubscriptionPage,
        },
        durable_sync,
    };
};

pub const Outcome = union(enum) {
    done,
    failed: anyerror,
};

pub const DurableListener = struct {
    context: *anyopaque,
    on_durable: *const fn (context: *anyopaque, position: protocol.LogPosition) void,
};

// NOTE: Vyukov bounded MPMC ring: a cell is writable at seq == pos, readable at seq == pos + 1.
// fuck if I understand this fully, but looks good https://pskrgag.github.io/post/mpmc-vuykov/
const Cell = struct {
    seq: std.atomic.Value(u64),
    command: Command,
};

pub const SubmitError = error{QueueFull};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    store: *EventStore,
    io: Io,
    cells: []Cell,
    mask: u64,
    work: Io.Semaphore,
    enqueue_pos: std.atomic.Value(u64),
    dequeue_pos: std.atomic.Value(u64),
    thread: ?std.Thread = null,
    stopping: std.atomic.Value(bool),
    /// WARN: transitional. Appends and request reads still take the server's legacy store lock
    /// on their own threads, so the owner holds it around every store access until they move
    /// onto the owner too. Remove it with the last such caller.
    legacy_store_lock: ?*Io.Mutex,
    /// Only the owner thread writes it.
    durable_position: std.atomic.Value(protocol.LogPosition),
    durable_listener: ?DurableListener,

    pub const Options = struct {
        capacity: u32 = capacity_default,
        legacy_store_lock: ?*Io.Mutex = null,
        durable_listener: ?DurableListener = null,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        store: *EventStore,
        io: Io,
        options: Options,
    ) !Owner {
        assert(options.capacity > 0);
        assert(options.capacity <= capacity_max);
        assert(std.math.isPowerOfTwo(options.capacity));
        const cells = try allocator.alloc(Cell, options.capacity);
        errdefer allocator.free(cells);
        for (cells, 0..) |*cell, index| {
            cell.* = .{ .seq = .init(@intCast(index)), .command = undefined };
        }
        return .{
            .allocator = allocator,
            .store = store,
            .io = io,
            .cells = cells,
            .mask = options.capacity - 1,
            .work = .{ .permits = 0 },
            .enqueue_pos = .init(0),
            .dequeue_pos = .init(0),
            .stopping = .init(false),
            .legacy_store_lock = options.legacy_store_lock,
            .durable_position = .init(store.log_position_durable()),
            .durable_listener = options.durable_listener,
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
            self.drain();
        }
    }

    // NOTE: a later slot can publish and post before an earlier one, so a wake that handled at
    // most one cell would strand it. Draining is safe: the unpublished head posts once it lands.
    fn drain(self: *Owner) void {
        while (true) {
            const pos = self.dequeue_pos.load(.acquire);
            const cell = &self.cells[pos & self.mask];
            const seq = cell.seq.load(.acquire);
            if (seq != pos + 1) return;

            self.execute(&cell.command);

            // NOTE: free the cell before waking; the requester never reads it again. Freeing
            // lets a submitter overwrite it, so take `done` first.
            const done = cell.command.done;
            cell.seq.store(pos + self.cells.len, .release);
            self.dequeue_pos.store(pos + 1, .release);
            done.post(self.io);
        }
    }

    fn execute(self: *Owner, command: *Command) void {
        command.outcome.* = switch (command.payload) {
            .ping => .done,
            .vc_summary => |op| blk: {
                self.store_access_begin();
                defer self.store_access_end();
                const entries = self.store.vc_summary();
                const count = @min(entries.len, op.out.len);
                @memcpy(op.out[0..count], entries[0..count]);
                op.out_len.* = count;
                break :blk .done;
            },
            .accumulate => |op| blk: {
                op.total.* += op.value;
                break :blk .done;
            },
            .subscription_page => |op| blk: {
                self.store_access_begin();
                defer self.store_access_end();
                op.page.* = self.store.read_subscription_page(
                    op.query,
                    op.output,
                    op.output_bytes,
                ) catch |err| break :blk .{ .failed = err };
                assert(op.page.committed <= self.store.log_position_durable());
                break :blk .done;
            },
            .durable_sync => self.execute_durable_sync(),
        };
    }

    fn execute_durable_sync(self: *Owner) Outcome {
        const position = sync: {
            self.store_access_begin();
            defer self.store_access_end();
            self.store.durable_sync() catch |err| return .{ .failed = err };
            break :sync self.store.log_position_durable();
        };
        // NOTE: seq_cst pairs with a subscriber's load before it waits; see subscription.zig.
        const previous = self.durable_position.swap(position, .seq_cst);
        assert(previous <= position);
        if (previous != position) {
            if (self.durable_listener) |listener| listener.on_durable(listener.context, position);
        }
        return .done;
    }

    fn store_access_begin(self: *Owner) void {
        if (self.legacy_store_lock) |lock| lock.lockUncancelable(self.io);
    }

    fn store_access_end(self: *Owner) void {
        if (self.legacy_store_lock) |lock| lock.unlock(self.io);
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

    var owner = try Owner.init(allocator, &event_store, io, .{});
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

    var owner = try Owner.init(allocator, &event_store, io, .{ .capacity = 16 });
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
