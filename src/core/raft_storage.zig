//! Checksummed, atomically replaced persistence for the bounded Raft state.

const std = @import("std");
const limits = @import("limits.zig");
const raft = @import("raft.zig");

const Io = std.Io;
const IoDir = Io.Dir;
const IoFile = Io.File;
const magic = "CPXRAFT1";
const format_version: u16 = 1;
const checksum_size: u16 = 32;
const entry_size: u16 = 8 + 8 + 32;
const header_size: u16 = 8 + 2 + 2 + 4 + 1 + 1 + 8 + 2 + 8 + 8 + 8 + 32 + 8 + 8 + 2;

pub const Recovered = struct {
    hard_state: raft.HardState,
    snapshot: raft.Snapshot,
    entry_count: u16,
    commit_index: raft.Index,
    applied_index: raft.Index,
};

pub fn state_size_max(log_capacity: usize) !usize {
    const entries_size = std.math.mul(usize, log_capacity, entry_size) catch
        return error.RaftStateTooLarge;
    return std.math.add(
        usize,
        header_size + raft.nodes_max + checksum_size,
        entries_size,
    ) catch return error.RaftStateTooLarge;
}

pub fn save(io: Io, path: []const u8, node: *raft.Node, scratch: []u8) !void {
    if (path.len == 0 or path.len > limits.wal_path_length_max)
        return error.InvalidRaftStatePath;
    const required = try encoded_size(node.members.len, node.entry_count);
    if (scratch.len < required) return error.RaftStateBufferTooSmall;
    var writer: Writer = .{ .bytes = scratch[0..required] };
    try writer.write_bytes(magic);
    try writer.write_int(u16, format_version);
    try writer.write_int(u16, 0);
    try writer.write_int(u32, @intCast(required));
    try writer.write_int(u8, node.id);
    try writer.write_int(u8, @intCast(node.members.len));
    try writer.write_int(u64, node.hard.term);
    try writer.write_int(
        u16,
        if (node.hard.voted_for) |id| id else std.math.maxInt(u16),
    );
    try writer.write_int(u64, node.snapshot.last_index);
    try writer.write_int(u64, node.snapshot.last_term);
    try writer.write_int(u64, node.snapshot.state_size);
    try writer.write_bytes(&node.snapshot.state_checksum);
    try writer.write_int(u64, node.commit_index);
    try writer.write_int(u64, node.applied_index);
    try writer.write_int(u16, node.entry_count);
    try writer.write_bytes(node.members);
    for (node.entries[0..node.entry_count]) |entry| {
        try writer.write_int(u64, entry.term);
        try writer.write_int(u64, entry.index);
        try writer.write_bytes(&entry.command_checksum);
    }
    const checksum = checksum_bytes(scratch[0..writer.offset]);
    try writer.write_bytes(&checksum);
    if (writer.offset != required) return error.InvalidRaftState;

    var atomic = try IoDir.cwd().createFileAtomic(io, path, .{ .replace = true });
    errdefer atomic.deinit(io);
    try atomic.file.writePositionalAll(io, scratch[0..required], 0);
    try atomic.file.sync(io);
    try atomic.replace(io);
    try sync_parent_directory(path, io);
    atomic.deinit(io);
    node.mark_hard_state_persisted();
}

pub fn load(
    io: Io,
    path: []const u8,
    expected_node: raft.NodeId,
    expected_members: []const raft.NodeId,
    entries: []raft.Entry,
    scratch: []u8,
) !?Recovered {
    const file = IoDir.cwd().openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    const size_u64 = (try file.stat(io)).size;
    if (size_u64 < header_size + raft.nodes_min + checksum_size)
        return error.TruncatedRaftState;
    if (size_u64 > scratch.len) return error.RaftStateBufferTooSmall;
    const size: usize = @intCast(size_u64);
    const data = scratch[0..size];
    if (try file.readPositionalAll(io, data, 0) != size)
        return error.TruncatedRaftState;
    if (!std.mem.eql(u8, data[0..magic.len], magic)) return error.InvalidRaftState;
    if (std.mem.readInt(u16, data[8..10], .big) != format_version)
        return error.UnsupportedRaftStateVersion;
    const encoded_size_value = std.mem.readInt(u32, data[12..16], .big);
    if (encoded_size_value > size) return error.TruncatedRaftState;
    if (encoded_size_value < size) return error.InvalidRaftState;
    const content = data[0 .. size - checksum_size];
    const expected_checksum: [checksum_size]u8 =
        data[size - checksum_size ..][0..checksum_size].*;
    const actual_checksum = checksum_bytes(content);
    if (!std.crypto.timing_safe.eql([checksum_size]u8, actual_checksum, expected_checksum))
        return error.RaftStateChecksumMismatch;

    var reader: Reader = .{ .bytes = content };
    if (!std.mem.eql(u8, try reader.read_bytes(magic.len), magic))
        return error.InvalidRaftState;
    if (try reader.read_int(u16) != format_version) return error.UnsupportedRaftStateVersion;
    if (try reader.read_int(u16) != 0) return error.InvalidRaftState;
    if (try reader.read_int(u32) != size) return error.InvalidRaftState;
    if (try reader.read_int(u8) != expected_node) return error.RaftStateIdentityMismatch;
    const member_count = try reader.read_int(u8);
    if (member_count != expected_members.len or member_count < raft.nodes_min or
        member_count > raft.nodes_max or member_count % 2 == 0)
        return error.RaftStateMembershipMismatch;
    const term = try reader.read_int(u64);
    const voted_wire = try reader.read_int(u16);
    const voted_for: ?raft.NodeId = if (voted_wire == std.math.maxInt(u16))
        null
    else if (voted_wire <= std.math.maxInt(raft.NodeId))
        @intCast(voted_wire)
    else
        return error.InvalidRaftState;
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
        return error.RaftStateMembershipMismatch;
    for (entries[0..entry_count]) |*entry| {
        entry.* = .{
            .term = try reader.read_int(u64),
            .index = try reader.read_int(u64),
            .command_checksum = (try reader.read_bytes(32))[0..32].*,
        };
    }
    if (reader.offset != content.len) return error.InvalidRaftState;
    return .{
        .hard_state = .{ .term = term, .voted_for = voted_for },
        .snapshot = snapshot,
        .entry_count = entry_count,
        .commit_index = commit_index,
        .applied_index = applied_index,
    };
}

fn encoded_size(member_count: usize, count: u16) !usize {
    const entries_size = std.math.mul(usize, count, entry_size) catch
        return error.RaftStateTooLarge;
    var size = std.math.add(usize, header_size, member_count) catch
        return error.RaftStateTooLarge;
    size = std.math.add(usize, size, entries_size) catch
        return error.RaftStateTooLarge;
    return std.math.add(usize, size, checksum_size) catch
        return error.RaftStateTooLarge;
}

const Writer = struct {
    bytes: []u8,
    offset: usize = 0,

    fn write_int(self: *Writer, comptime T: type, value: T) !void {
        if (@sizeOf(T) > self.bytes.len - self.offset) return error.RaftStateBufferTooSmall;
        std.mem.writeInt(T, self.bytes[self.offset..][0..@sizeOf(T)], value, .big);
        self.offset += @sizeOf(T);
    }

    fn write_bytes(self: *Writer, value: []const u8) !void {
        if (value.len > self.bytes.len - self.offset) return error.RaftStateBufferTooSmall;
        @memcpy(self.bytes[self.offset..][0..value.len], value);
        self.offset += value.len;
    }
};

const Reader = struct {
    bytes: []const u8,
    offset: usize = 0,

    fn read_int(self: *Reader, comptime T: type) !T {
        if (@sizeOf(T) > self.bytes.len - self.offset) return error.TruncatedRaftState;
        const value = std.mem.readInt(T, self.bytes[self.offset..][0..@sizeOf(T)], .big);
        self.offset += @sizeOf(T);
        return value;
    }

    fn read_bytes(self: *Reader, size: usize) ![]const u8 {
        if (size > self.bytes.len - self.offset) return error.TruncatedRaftState;
        const value = self.bytes[self.offset..][0..size];
        self.offset += size;
        return value;
    }
};

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

test "raft state survives restart and detects mutation and truncation" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_raft_state.raft";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};
    const members = [_]raft.NodeId{ 1, 2, 3 };
    var entries: [8]raft.Entry = undefined;
    var node = try raft.Node.init(.{
        .id = 1,
        .members = &members,
        .log_storage = &entries,
        .election_timeout_ticks = 3,
        .heartbeat_ticks = 1,
    });
    var messages: [16]raft.Envelope = undefined;
    var outbox = try raft.Outbox.init(&messages);
    for (0..3) |_| try node.tick(&outbox);
    outbox.reset();
    try node.receive(.{
        .from = 2,
        .to = 1,
        .message = .{ .vote_result = .{
            .term = node.hard.term,
            .voter = 2,
            .granted = true,
        } },
    }, &outbox);
    outbox.reset();
    _ = try node.propose(@splat(0x44), &outbox);
    var scratch: [1024]u8 = undefined;
    try save(io, path, &node, &scratch);
    try std.testing.expect(!node.hard_state_needs_persist());

    var recovered_entries: [8]raft.Entry = undefined;
    const recovered = (try load(
        io,
        path,
        1,
        &members,
        &recovered_entries,
        &scratch,
    )).?;
    var restarted = try raft.Node.init(.{
        .id = 1,
        .members = &members,
        .log_storage = &recovered_entries,
        .hard_state = recovered.hard_state,
        .snapshot = recovered.snapshot,
        .election_timeout_ticks = 3,
        .heartbeat_ticks = 1,
        .restored_entry_count = recovered.entry_count,
        .restored_commit_index = recovered.commit_index,
        .restored_applied_index = recovered.applied_index,
    });
    try std.testing.expectEqual(node.hard, restarted.hard);
    try std.testing.expectEqual(@as(raft.Index, 1), restarted.last_index());
    try std.testing.expectEqual(@as(u8, 0x44), restarted.entries[0].command_checksum[0]);

    const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    const original_size = (try file.stat(io)).size;
    var byte: [1]u8 = undefined;
    _ = try file.readPositionalAll(io, &byte, 24);
    byte[0] ^= 1;
    try file.writePositionalAll(io, &byte, 24);
    try file.sync(io);
    try std.testing.expectError(
        error.RaftStateChecksumMismatch,
        load(io, path, 1, &members, &recovered_entries, &scratch),
    );
    byte[0] ^= 1;
    try file.writePositionalAll(io, &byte, 24);
    try file.setLength(io, original_size - 1);
    try file.sync(io);
    try std.testing.expectError(
        error.TruncatedRaftState,
        load(io, path, 1, &members, &recovered_entries, &scratch),
    );
}
