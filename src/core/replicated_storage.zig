//! Atomic persistence of Raft metadata and the canonical commands it references.

const std = @import("std");
const limits = @import("limits.zig");
const protocol = @import("protocol.zig");
const raft = @import("raft.zig");
const replicated = @import("replicated_store.zig");

const Io = std.Io;
const IoDir = Io.Dir;
const IoFile = Io.File;
const magic = "CPXRPL01";
const format_version: u16 = 1;
const checksum_size: u16 = 32;
const entry_header_size: u16 = 8 + 8 + 32 + 4;
const header_size: u16 = 8 + 2 + 2 + 4 + 1 + 1 + 8 + 2 + 8 + 8 + 8 + 32 + 8 + 8 + 2;

pub const Recovered = struct {
    hard_state: raft.HardState,
    snapshot: raft.Snapshot,
    entry_count: u16,
    commit_index: raft.Index,
    applied_index: raft.Index,
};

pub fn state_size_max(log_capacity: usize, command_slot_size: usize) !usize {
    const per_entry = std.math.add(usize, entry_header_size, command_slot_size) catch
        return error.ReplicatedStateTooLarge;
    const entries_size = std.math.mul(usize, log_capacity, per_entry) catch
        return error.ReplicatedStateTooLarge;
    return std.math.add(
        usize,
        header_size + raft.nodes_max + checksum_size,
        entries_size,
    ) catch return error.ReplicatedStateTooLarge;
}

pub fn save(io: Io, path: []const u8, machine: *replicated.Machine, scratch: []u8) !void {
    if (path.len == 0 or path.len > limits.wal_path_length_max)
        return error.InvalidReplicatedStatePath;
    const required = try encoded_size(machine);
    if (scratch.len < required) return error.ReplicatedStateBufferTooSmall;
    var writer: Writer = .{ .bytes = scratch[0..required] };
    try writer.write_bytes(magic);
    try writer.write_int(u16, format_version);
    try writer.write_int(u16, 0);
    try writer.write_int(u32, @intCast(required));
    try writer.write_int(u8, machine.node.id);
    try writer.write_int(u8, @intCast(machine.node.members.len));
    try writer.write_int(u64, machine.node.hard.term);
    try writer.write_int(
        u16,
        if (machine.node.hard.voted_for) |id| id else std.math.maxInt(u16),
    );
    try writer.write_int(u64, machine.node.snapshot.last_index);
    try writer.write_int(u64, machine.node.snapshot.last_term);
    try writer.write_int(u64, machine.node.snapshot.state_size);
    try writer.write_bytes(&machine.node.snapshot.state_checksum);
    try writer.write_int(u64, machine.node.commit_index);
    try writer.write_int(u64, machine.node.applied_index);
    try writer.write_int(u16, machine.node.entry_count);
    try writer.write_bytes(machine.node.members);
    for (machine.node.entries[0..machine.node.entry_count]) |entry| {
        const command = machine.command_at(entry.index) orelse
            return error.MissingReplicatedCommand;
        if (!std.mem.eql(u8, &checksum_command(command), &entry.command_checksum))
            return error.ReplicatedCommandChecksumMismatch;
        try writer.write_int(u64, entry.term);
        try writer.write_int(u64, entry.index);
        try writer.write_bytes(&entry.command_checksum);
        try writer.write_int(u32, @intCast(command.len));
        try writer.write_bytes(command);
    }
    const checksum = checksum_bytes(scratch[0..writer.offset]);
    try writer.write_bytes(&checksum);
    if (writer.offset != required) return error.InvalidReplicatedState;

    var atomic = try IoDir.cwd().createFileAtomic(io, path, .{ .replace = true });
    errdefer atomic.deinit(io);
    try atomic.file.writePositionalAll(io, scratch[0..required], 0);
    try atomic.file.sync(io);
    try atomic.replace(io);
    try sync_parent_directory(path, io);
    atomic.deinit(io);
    machine.node.mark_hard_state_persisted();
}

pub fn load(
    io: Io,
    path: []const u8,
    expected_node: raft.NodeId,
    expected_members: []const raft.NodeId,
    entries: []raft.Entry,
    command_bytes: []u8,
    command_indices: []raft.Index,
    command_lengths: []limits.ByteCount,
    slot_size: limits.ByteCount,
    scratch: []u8,
) !?Recovered {
    if (entries.len == 0 or entries.len != command_indices.len or
        entries.len != command_lengths.len)
        return error.InvalidReplicationCapacity;
    const expected_command_bytes = std.math.mul(usize, entries.len, slot_size) catch
        return error.InvalidReplicationCapacity;
    if (command_bytes.len != expected_command_bytes)
        return error.InvalidReplicationCapacity;
    const file = IoDir.cwd().openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    const size_u64 = (try file.stat(io)).size;
    if (size_u64 < header_size + raft.nodes_min + checksum_size)
        return error.TruncatedReplicatedState;
    if (size_u64 > scratch.len) return error.ReplicatedStateBufferTooSmall;
    const size: usize = @intCast(size_u64);
    const data = scratch[0..size];
    if (try file.readPositionalAll(io, data, 0) != size)
        return error.TruncatedReplicatedState;
    if (!std.mem.eql(u8, data[0..magic.len], magic)) return error.InvalidReplicatedState;
    if (std.mem.readInt(u16, data[8..10], .big) != format_version)
        return error.UnsupportedReplicatedStateVersion;
    const encoded_size_value = std.mem.readInt(u32, data[12..16], .big);
    if (encoded_size_value > size) return error.TruncatedReplicatedState;
    if (encoded_size_value < size) return error.InvalidReplicatedState;
    const content = data[0 .. size - checksum_size];
    const expected_checksum: [checksum_size]u8 =
        data[size - checksum_size ..][0..checksum_size].*;
    if (!std.crypto.timing_safe.eql(
        [checksum_size]u8,
        checksum_bytes(content),
        expected_checksum,
    )) return error.ReplicatedStateChecksumMismatch;

    var reader: Reader = .{ .bytes = content };
    if (!std.mem.eql(u8, try reader.read_bytes(magic.len), magic))
        return error.InvalidReplicatedState;
    if (try reader.read_int(u16) != format_version)
        return error.UnsupportedReplicatedStateVersion;
    if (try reader.read_int(u16) != 0) return error.InvalidReplicatedState;
    if (try reader.read_int(u32) != size) return error.InvalidReplicatedState;
    if (try reader.read_int(u8) != expected_node)
        return error.ReplicatedStateIdentityMismatch;
    const member_count = try reader.read_int(u8);
    if (member_count != expected_members.len or member_count < raft.nodes_min or
        member_count > raft.nodes_max or member_count % 2 == 0)
        return error.ReplicatedStateMembershipMismatch;
    const term = try reader.read_int(u64);
    const voted_wire = try reader.read_int(u16);
    const voted_for: ?raft.NodeId = if (voted_wire == std.math.maxInt(u16))
        null
    else if (voted_wire <= std.math.maxInt(raft.NodeId))
        @intCast(voted_wire)
    else
        return error.InvalidReplicatedState;
    const snapshot: raft.Snapshot = .{
        .last_index = try reader.read_int(u64),
        .last_term = try reader.read_int(u64),
        .state_size = try reader.read_int(u64),
        .state_checksum = (try reader.read_bytes(32))[0..32].*,
    };
    const commit_index = try reader.read_int(u64);
    const applied_index = try reader.read_int(u64);
    const entry_count = try reader.read_int(u16);
    if (entry_count > entries.len) return error.RaftLogCapacityTooSmall;
    const members = try reader.read_bytes(member_count);
    if (!std.mem.eql(u8, members, expected_members))
        return error.ReplicatedStateMembershipMismatch;
    @memset(command_indices, 0);
    @memset(command_lengths, 0);
    for (entries[0..entry_count]) |*entry| {
        entry.* = .{
            .term = try reader.read_int(u64),
            .index = try reader.read_int(u64),
            .command_checksum = (try reader.read_bytes(32))[0..32].*,
        };
        const command_size = try reader.read_int(u32);
        if (command_size == 0 or command_size > slot_size)
            return error.InvalidReplicatedState;
        const command = try reader.read_bytes(command_size);
        if (!std.mem.eql(u8, &checksum_command(command), &entry.command_checksum))
            return error.ReplicatedCommandChecksumMismatch;
        const slot: u16 = @intCast(entry.index % entries.len);
        const start = @as(usize, slot) * slot_size;
        @memcpy(command_bytes[start..][0..command.len], command);
        command_indices[slot] = entry.index;
        command_lengths[slot] = command_size;
    }
    if (reader.offset != content.len) return error.InvalidReplicatedState;
    return .{
        .hard_state = .{ .term = term, .voted_for = voted_for },
        .snapshot = snapshot,
        .entry_count = entry_count,
        .commit_index = commit_index,
        .applied_index = applied_index,
    };
}

fn encoded_size(machine: *const replicated.Machine) !usize {
    var size = std.math.add(usize, header_size, machine.node.members.len) catch
        return error.ReplicatedStateTooLarge;
    for (machine.node.entries[0..machine.node.entry_count]) |entry| {
        const command = machine.command_at(entry.index) orelse
            return error.MissingReplicatedCommand;
        size = std.math.add(usize, size, entry_header_size) catch
            return error.ReplicatedStateTooLarge;
        size = std.math.add(usize, size, command.len) catch
            return error.ReplicatedStateTooLarge;
    }
    return std.math.add(usize, size, checksum_size) catch
        return error.ReplicatedStateTooLarge;
}

const Writer = struct {
    bytes: []u8,
    offset: usize = 0,

    fn write_int(self: *Writer, comptime T: type, value: T) !void {
        if (@sizeOf(T) > self.bytes.len - self.offset)
            return error.ReplicatedStateBufferTooSmall;
        std.mem.writeInt(T, self.bytes[self.offset..][0..@sizeOf(T)], value, .big);
        self.offset += @sizeOf(T);
    }

    fn write_bytes(self: *Writer, value: []const u8) !void {
        if (value.len > self.bytes.len - self.offset)
            return error.ReplicatedStateBufferTooSmall;
        @memcpy(self.bytes[self.offset..][0..value.len], value);
        self.offset += value.len;
    }
};

const Reader = struct {
    bytes: []const u8,
    offset: usize = 0,

    fn read_int(self: *Reader, comptime T: type) !T {
        if (@sizeOf(T) > self.bytes.len - self.offset)
            return error.TruncatedReplicatedState;
        const value = std.mem.readInt(T, self.bytes[self.offset..][0..@sizeOf(T)], .big);
        self.offset += @sizeOf(T);
        return value;
    }

    fn read_bytes(self: *Reader, size: usize) ![]const u8 {
        if (size > self.bytes.len - self.offset) return error.TruncatedReplicatedState;
        const value = self.bytes[self.offset..][0..size];
        self.offset += size;
        return value;
    }
};

fn checksum_command(command: []const u8) [32]u8 {
    return checksum_bytes(command);
}

fn checksum_bytes(bytes: []const u8) [checksum_size]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(bytes);
    var result: [checksum_size]u8 = undefined;
    hash.final(&result);
    return result;
}

fn sync_parent_directory(path: []const u8, io: Io) !void {
    const parent = std.fs.path.dirname(path) orelse ".";
    const handle = try std.posix.openat(
        std.posix.AT.FDCWD,
        parent,
        .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true },
        0,
    );
    const file: IoFile = .{
        .handle = handle,
        .flags = .{ .nonblocking = false },
    };
    defer file.close(io);
    file.sync(io) catch return error.DirectorySyncFailed;
}

fn cleanup_test(io: Io) void {
    inline for (.{
        "data/test_replicated_restart.wal",
        "data/test_replicated_restart.crumb",
        "data/test_replicated_restart.manifest",
        "data/test_replicated_restart.replication",
    }) |path| IoDir.cwd().deleteFile(io, path) catch {};
}

test "committed command checkpoint replays after crash before state-machine apply" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    cleanup_test(io);
    defer cleanup_test(io);
    const wal_path = "data/test_replicated_restart.wal";
    const state_path = "data/test_replicated_restart.replication";
    const members = [_]raft.NodeId{ 1, 2, 3 };
    var checkpoint: [64 * 1024]u8 = undefined;
    {
        var store = try @import("store.zig").EventStore.init(allocator, wal_path, io, .{
            .stream_capacity = 16,
            .stream_id_bytes_capacity = 4096,
        });
        defer store.deinit();
        var log: [8]raft.Entry = undefined;
        var commands: [8 * 4096]u8 = undefined;
        var command_indices: [8]raft.Index = undefined;
        var command_lengths: [8]limits.ByteCount = undefined;
        var results: [8]replicated.ResultSlot = undefined;
        var decode_events: [protocol.append_events_max]protocol.AppendEvent = undefined;
        var machine = try replicated.Machine.init(.{
            .raft_config = .{
                .id = 1,
                .members = &members,
                .log_storage = &log,
                .election_timeout_ticks = 3,
                .heartbeat_ticks = 1,
            },
            .store = &store,
            .command_bytes = &commands,
            .command_indices = &command_indices,
            .command_lengths = &command_lengths,
            .results = &results,
            .decode_events = &decode_events,
            .slot_size = 4096,
        });
        var messages: [32]raft.Envelope = undefined;
        var outbox = try raft.Outbox.init(&messages);
        for (0..3) |_| try machine.node.tick(&outbox);
        outbox.reset();
        try machine.node.receive(.{
            .from = 2,
            .to = 1,
            .message = .{ .vote_result = .{
                .term = machine.node.hard.term,
                .voter = 2,
                .granted = true,
            } },
        }, &outbox);
        outbox.reset();
        const events = [_]protocol.AppendEvent{.{
            .event_type = "restart.created",
            .schema_version = 1,
            .payload = "payload",
        }};
        const index = try machine.propose_append(.{
            .stream_id = "restart/1",
            .expected_version = .empty,
            .command_id = "restart-replicated-command",
            .events = &events,
        }, 100, &outbox);
        outbox.reset();
        try machine.receive(.{
            .from = 2,
            .to = 1,
            .message = .{ .append_result = .{
                .term = machine.node.hard.term,
                .follower = 2,
                .success = true,
                .match_index = index,
                .conflict_index = 0,
            } },
        }, null, &outbox);
        try std.testing.expectEqual(index, machine.node.commit_index);
        try std.testing.expectEqual(@as(raft.Index, 0), machine.node.applied_index);
        try save(io, state_path, &machine, &checkpoint);
    }
    {
        var store = try @import("store.zig").EventStore.init(allocator, wal_path, io, .{
            .stream_capacity = 16,
            .stream_id_bytes_capacity = 4096,
        });
        defer store.deinit();
        var log: [8]raft.Entry = undefined;
        var commands: [8 * 4096]u8 = undefined;
        var command_indices: [8]raft.Index = undefined;
        var command_lengths: [8]limits.ByteCount = undefined;
        var results: [8]replicated.ResultSlot = undefined;
        var decode_events: [protocol.append_events_max]protocol.AppendEvent = undefined;
        const recovered = (try load(
            io,
            state_path,
            1,
            &members,
            &log,
            &commands,
            &command_indices,
            &command_lengths,
            4096,
            &checkpoint,
        )).?;
        var machine = try replicated.Machine.init(.{
            .raft_config = .{
                .id = 1,
                .members = &members,
                .log_storage = &log,
                .hard_state = recovered.hard_state,
                .snapshot = recovered.snapshot,
                .election_timeout_ticks = 3,
                .heartbeat_ticks = 1,
                .restored_entry_count = recovered.entry_count,
                .restored_commit_index = recovered.commit_index,
                .restored_applied_index = recovered.applied_index,
            },
            .store = &store,
            .command_bytes = &commands,
            .command_indices = &command_indices,
            .command_lengths = &command_lengths,
            .results = &results,
            .decode_events = &decode_events,
            .slot_size = 4096,
            .restored_commands = true,
        });
        try machine.apply_committed();
        try std.testing.expect(machine.result(1).? == .committed);
        const verification = try store.verify_integrity();
        try std.testing.expectEqual(@as(u64, 1), verification.events);
        try std.testing.expectEqual(@as(u64, 1), verification.committed_log_position);
        try save(io, state_path, &machine, &checkpoint);
    }

    const state_file = try IoDir.cwd().openFile(io, state_path, .{ .mode = .read_write });
    defer state_file.close(io);
    var byte: [1]u8 = undefined;
    _ = try state_file.readPositionalAll(io, &byte, 24);
    byte[0] ^= 1;
    try state_file.writePositionalAll(io, &byte, 24);
    try state_file.sync(io);
    var log: [8]raft.Entry = undefined;
    var commands: [8 * 4096]u8 = undefined;
    var indices: [8]raft.Index = undefined;
    var lengths: [8]limits.ByteCount = undefined;
    try std.testing.expectError(
        error.ReplicatedStateChecksumMismatch,
        load(
            io,
            state_path,
            1,
            &members,
            &log,
            &commands,
            &indices,
            &lengths,
            4096,
            &checkpoint,
        ),
    );
}
