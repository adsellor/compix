//! Bounded adapter between Raft commit indexes and canonical stream appends.
//!
//! An append result is exposed only after its Raft entry is committed and the
//! local EventStore has durably applied it. Network and persistence orchestration
//! remain caller-owned; command bytes must be checkpointed together with the
//! corresponding Raft metadata before an AppendResult is transmitted.

const std = @import("std");
const limits = @import("limits.zig");
const protocol = @import("protocol.zig");
const raft = @import("raft.zig");
const store_mod = @import("store.zig");

const EventStore = store_mod.EventStore;

pub const ResultSlot = struct {
    index: raft.Index = 0,
    response: protocol.AppendStreamResponse = .command_conflict,
};

pub const Machine = struct {
    node: raft.Node,
    store: *EventStore,
    command_bytes: []u8,
    command_indices: []raft.Index,
    command_lengths: []limits.ByteCount,
    results: []ResultSlot,
    decode_events: []protocol.AppendEvent,
    slot_size: limits.ByteCount,
    poisoned: bool,

    pub const InitOptions = struct {
        raft_config: raft.Config,
        store: *EventStore,
        command_bytes: []u8,
        command_indices: []raft.Index,
        command_lengths: []limits.ByteCount,
        results: []ResultSlot,
        decode_events: []protocol.AppendEvent,
        slot_size: limits.ByteCount,
        restored_commands: bool = false,
    };

    pub fn init(options: InitOptions) !Machine {
        if (options.slot_size < 16 or options.slot_size > protocol.frame_size_max + 8)
            return error.InvalidReplicationSlotSize;
        const capacity = options.raft_config.log_storage.len;
        if (options.command_indices.len != capacity or
            options.command_lengths.len != capacity or options.results.len != capacity)
            return error.InvalidReplicationCapacity;
        const expected_bytes = std.math.mul(usize, capacity, options.slot_size) catch
            return error.InvalidReplicationCapacity;
        if (options.command_bytes.len != expected_bytes)
            return error.InvalidReplicationCapacity;
        if (options.decode_events.len < protocol.append_events_max)
            return error.AppendEventBufferTooSmall;
        if (!options.restored_commands) {
            @memset(options.command_indices, 0);
            @memset(options.command_lengths, 0);
        }
        for (options.results) |*stored_result| stored_result.* = .{};
        return .{
            .node = try raft.Node.init(options.raft_config),
            .store = options.store,
            .command_bytes = options.command_bytes,
            .command_indices = options.command_indices,
            .command_lengths = options.command_lengths,
            .results = options.results,
            .decode_events = options.decode_events,
            .slot_size = options.slot_size,
            .poisoned = false,
        };
    }

    pub fn propose_append(
        self: *Machine,
        request: protocol.AppendStream,
        timestamp: limits.Timestamp,
        outbox: *raft.Outbox,
    ) !raft.Index {
        if (self.poisoned) return error.ReplicationPoisoned;
        if (self.node.role != .leader) return error.NotLeader;
        const index = std.math.add(raft.Index, self.node.last_index(), 1) catch
            return error.RaftIndexOverflow;
        const slot_index = self.slot_for_index(index);
        const target = self.slot_bytes(slot_index);
        std.mem.writeInt(i64, target[0..8], timestamp, .big);
        const frame = try protocol.encode_into(target[8..], .{ .append_stream = request });
        const command = target[0 .. 8 + frame.len];
        const digest = command_checksum(command);
        const proposed = try self.node.propose(digest, outbox);
        if (proposed != index) return error.InvalidRaftIndex;
        self.command_indices[slot_index] = index;
        self.command_lengths[slot_index] = @intCast(command.len);
        return index;
    }

    pub fn receive(
        self: *Machine,
        envelope: raft.Envelope,
        command: ?[]const u8,
        outbox: *raft.Outbox,
    ) !void {
        if (self.poisoned) return error.ReplicationPoisoned;
        const incoming = switch (envelope.message) {
            .append_entries => |append| append.entry,
            else => null,
        };
        if (incoming) |entry| {
            const bytes = command orelse return error.MissingReplicatedCommand;
            if (!std.mem.eql(u8, &command_checksum(bytes), &entry.command_checksum))
                return error.ReplicatedCommandChecksumMismatch;
        } else if (command != null) {
            return error.UnexpectedReplicatedCommand;
        }
        self.node.receive(envelope, outbox) catch |err| {
            self.poisoned = true;
            return err;
        };
        if (incoming) |entry| {
            if (self.node.snapshot.last_index < entry.index and
                entry.index <= self.node.last_index())
            {
                const offset: u16 = @intCast(entry.index - self.node.snapshot.last_index - 1);
                const stored = self.node.entries[offset];
                if (stored.term == entry.term and std.mem.eql(
                    u8,
                    &stored.command_checksum,
                    &entry.command_checksum,
                )) try self.store_command(entry.index, command.?);
            }
        }
    }

    pub fn command_for(self: *const Machine, envelope: raft.Envelope) ?[]const u8 {
        const entry = switch (envelope.message) {
            .append_entries => |append| append.entry orelse return null,
            else => return null,
        };
        return self.get_command(entry.index);
    }

    pub fn command_at(self: *const Machine, index: raft.Index) ?[]const u8 {
        return self.get_command(index);
    }

    pub fn apply_committed(self: *Machine) !void {
        if (self.poisoned) return error.ReplicationPoisoned;
        while (self.node.next_committed()) |entry| {
            const command = self.get_command(entry.index) orelse {
                self.poisoned = true;
                return error.MissingReplicatedCommand;
            };
            try self.apply_archived(entry.index, entry.command_checksum, command);
            try self.node.mark_applied(entry.index);
        }
    }

    pub fn apply_archived(
        self: *Machine,
        index: raft.Index,
        expected_checksum: [32]u8,
        command: []const u8,
    ) !void {
        if (self.poisoned) return error.ReplicationPoisoned;
        if (!std.mem.eql(u8, &command_checksum(command), &expected_checksum)) {
            self.poisoned = true;
            return error.ReplicatedCommandChecksumMismatch;
        }
        const response = self.execute_command(command) catch |err| {
            self.poisoned = true;
            return err;
        };
        const result_slot = self.slot_for_index(index);
        self.results[result_slot] = .{ .index = index, .response = response };
    }

    fn execute_command(self: *Machine, command: []const u8) !protocol.AppendStreamResponse {
        if (command.len < 8) return error.InvalidReplicatedCommand;
        const timestamp = std.mem.readInt(i64, command[0..8], .big);
        var buffers: protocol.DecodeBuffers = .{ .append_events = self.decode_events };
        const message = try protocol.decode(command[8..], &buffers);
        const request = switch (message) {
            .append_stream => |append| append,
            else => return error.InvalidReplicatedCommand,
        };
        return try self.store.append_stream(request, timestamp);
    }

    pub fn result(self: *const Machine, index: raft.Index) ?protocol.AppendStreamResponse {
        const stored = self.results[self.slot_for_index(index)];
        return if (stored.index == index) stored.response else null;
    }

    fn store_command(self: *Machine, index: raft.Index, command: []const u8) !void {
        if (command.len > self.slot_size) return error.ReplicatedCommandTooLarge;
        const slot_index = self.slot_for_index(index);
        if (self.command_indices[slot_index] == index) {
            const existing = self.slot_bytes(slot_index)[0..self.command_lengths[slot_index]];
            if (!std.mem.eql(u8, existing, command)) return error.ReplicatedCommandMismatch;
            return;
        }
        @memcpy(self.slot_bytes(slot_index)[0..command.len], command);
        self.command_indices[slot_index] = index;
        self.command_lengths[slot_index] = @intCast(command.len);
        self.results[slot_index] = .{};
    }

    fn get_command(self: *const Machine, index: raft.Index) ?[]const u8 {
        const slot_index = self.slot_for_index(index);
        if (self.command_indices[slot_index] != index) return null;
        return self.slot_bytes_const(slot_index)[0..self.command_lengths[slot_index]];
    }

    fn slot_for_index(self: *const Machine, index: raft.Index) u16 {
        return @intCast(index % self.command_indices.len);
    }

    fn slot_bytes(self: *Machine, slot_index: u16) []u8 {
        const start = @as(usize, slot_index) * self.slot_size;
        return self.command_bytes[start..][0..self.slot_size];
    }

    fn slot_bytes_const(self: *const Machine, slot_index: u16) []const u8 {
        const start = @as(usize, slot_index) * self.slot_size;
        return self.command_bytes[start..][0..self.slot_size];
    }
};

fn command_checksum(command: []const u8) [32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(command);
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

const TestNode = struct {
    store: EventStore,
    log: [8]raft.Entry = undefined,
    commands: [8 * 4096]u8 = undefined,
    command_indices: [8]raft.Index = undefined,
    command_lengths: [8]limits.ByteCount = undefined,
    results: [8]ResultSlot = undefined,
    decode_events: [protocol.append_events_max]protocol.AppendEvent = undefined,
    machine: Machine = undefined,
};

fn cleanup_test(io: std.Io) void {
    for (1..4) |id| {
        var path_buffer: [128]u8 = undefined;
        inline for (.{ ".wal", ".crumb", ".manifest" }) |suffix| {
            const path = std.fmt.bufPrint(
                &path_buffer,
                "data/test_replicated_store_{d}{s}",
                .{ id, suffix },
            ) catch unreachable;
            std.Io.Dir.cwd().deleteFile(io, path) catch {};
        }
    }
}

test "canonical result appears only after quorum and applies identically" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    cleanup_test(io);
    defer cleanup_test(io);
    const members = [_]raft.NodeId{ 1, 2, 3 };
    const nodes = try allocator.alloc(TestNode, 3);
    defer allocator.free(nodes);
    const timeouts = [_]u16{ 3, 5, 7 };
    for (nodes, 0..) |*node, index| {
        var path_buffer: [128]u8 = undefined;
        const path = try std.fmt.bufPrint(
            &path_buffer,
            "data/test_replicated_store_{d}.wal",
            .{index + 1},
        );
        node.store = try EventStore.init(allocator, path, io, .{
            .stream_capacity = 16,
            .stream_id_bytes_capacity = 4096,
        });
        node.machine = try Machine.init(.{
            .raft_config = .{
                .id = @intCast(index + 1),
                .members = &members,
                .log_storage = &node.log,
                .election_timeout_ticks = timeouts[index],
                .heartbeat_ticks = 1,
            },
            .store = &node.store,
            .command_bytes = &node.commands,
            .command_indices = &node.command_indices,
            .command_lengths = &node.command_lengths,
            .results = &node.results,
            .decode_events = &node.decode_events,
            .slot_size = 4096,
        });
    }
    defer for (nodes) |*node| node.store.deinit();
    var messages: [128]raft.Envelope = undefined;
    var outbox = try raft.Outbox.init(&messages);
    for (0..3) |_| try nodes[0].machine.node.tick(&outbox);
    try deliver_all(nodes, &outbox);
    try std.testing.expectEqual(raft.Role.leader, nodes[0].machine.node.role);

    const events = [_]protocol.AppendEvent{
        .{ .event_type = "object.created", .schema_version = 1, .payload = "one" },
        .{ .event_type = "object.named", .schema_version = 1, .payload = "two" },
    };
    const request: protocol.AppendStream = .{
        .stream_id = "object/1",
        .expected_version = .empty,
        .command_id = "replicated-command-1",
        .events = &events,
    };
    const index = try nodes[0].machine.propose_append(request, 100, &outbox);
    try std.testing.expect(nodes[0].machine.result(index) == null);
    const first_request = outbox.items()[0];
    const command = nodes[0].machine.command_for(first_request).?;
    outbox.reset();
    try nodes[1].machine.receive(first_request, command, &outbox);
    try nodes[1].machine.apply_committed();
    try std.testing.expect(nodes[1].machine.result(index) == null);
    const durable_response = outbox.items()[0];
    outbox.reset();
    try nodes[0].machine.receive(durable_response, null, &outbox);
    try nodes[0].machine.apply_committed();
    try std.testing.expect(nodes[0].machine.result(index).? == .committed);
    try std.testing.expectEqual(@as(raft.Index, 1), nodes[0].machine.node.commit_index);
    try deliver_all(nodes, &outbox);
    for (nodes) |*node| {
        const verification = try node.store.verify_integrity();
        try std.testing.expectEqual(@as(u64, 2), verification.events);
        try std.testing.expectEqual(@as(u64, 2), verification.committed_log_position);
    }

    const second_events = [_]protocol.AppendEvent{.{
        .event_type = "object.updated",
        .schema_version = 1,
        .payload = "three",
    }};
    const second = try nodes[0].machine.propose_append(.{
        .stream_id = "object/1",
        .expected_version = .{ .exact = 2 },
        .command_id = "replicated-command-2",
        .events = &second_events,
    }, 200, &outbox);
    outbox.reset(); // Complete loss of quorum.
    try std.testing.expect(nodes[0].machine.result(second) == null);
    try std.testing.expectEqual(@as(raft.Index, 1), nodes[0].machine.node.commit_index);
}

fn deliver_all(nodes: []TestNode, outbox: *raft.Outbox) !void {
    var cursor: u16 = 0;
    while (cursor < outbox.count) : (cursor += 1) {
        const envelope = outbox.storage[cursor];
        const sender = &nodes[envelope.from - 1].machine;
        const receiver = &nodes[envelope.to - 1].machine;
        try receiver.receive(envelope, sender.command_for(envelope), outbox);
        try receiver.apply_committed();
    }
    outbox.reset();
}
