//! Checksummed, bounded Raft command snapshots and chunked transfer staging.
//!
//! The archive is a deterministic command prefix shared by every node. Replaying
//! it through canonical command IDs rebuilds an empty replacement or safely
//! completes an interrupted installation without inventing a second state format.

const std = @import("std");
const limits = @import("limits.zig");
const raft = @import("raft.zig");
const replicated = @import("replicated_store.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const IoDir = Io.Dir;
const IoFile = Io.File;

const magic = "CPXSNP01";
const format_version: u16 = 1;
const checksum_size: u8 = 32;
const header_fixed_size: u8 = 16;
const record_header_size: u8 = 8 + 8 + 4 + checksum_size;

pub const chunk_size_max = limits.snapshot_chunk_size_max;
pub const snapshot_size_default_max = limits.snapshot_size_default_max;
pub const snapshot_size_hard_max = limits.snapshot_size_hard_max;

pub const Paths = struct {
    archive: []u8,
    incoming: []u8,
    quarantine: []u8,

    pub fn init(allocator: Allocator, state_path: []const u8) !Paths {
        if (state_path.len == 0 or state_path.len > limits.wal_path_length_max)
            return error.InvalidReplicatedStatePath;
        const archive = try std.fmt.allocPrint(allocator, "{s}.snapshot", .{state_path});
        errdefer allocator.free(archive);
        const incoming = try std.fmt.allocPrint(allocator, "{s}.snapshot.incoming", .{state_path});
        errdefer allocator.free(incoming);
        const quarantine = try std.fmt.allocPrint(
            allocator,
            "{s}.snapshot.quarantine",
            .{state_path},
        );
        return .{ .archive = archive, .incoming = incoming, .quarantine = quarantine };
    }

    pub fn deinit(self: Paths, allocator: Allocator) void {
        allocator.free(self.quarantine);
        allocator.free(self.incoming);
        allocator.free(self.archive);
    }
};

pub fn quarantine_incoming(io: Io, paths: Paths) !bool {
    const incoming = IoDir.cwd().openFile(io, paths.incoming, .{ .mode = .read_only }) catch |err|
        switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
    incoming.close(io);
    try IoDir.cwd().renamePreserve(
        paths.incoming,
        IoDir.cwd(),
        paths.quarantine,
        io,
    );
    try sync_parent_directory(paths.incoming, io);
    return true;
}

pub fn verify(
    io: Io,
    path: []const u8,
    snapshot: raft.Snapshot,
    members: []const raft.NodeId,
    scratch: []u8,
) !void {
    try validate_metadata(snapshot, members, scratch);
    if (snapshot.last_index == 0) return;
    const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_only });
    defer file.close(io);
    const size = (try file.stat(io)).size;
    if (size < snapshot.state_size) return error.TruncatedSnapshot;
    const actual = try hash_prefix(file, io, snapshot.state_size, scratch);
    if (!std.crypto.timing_safe.eql([checksum_size]u8, actual, snapshot.state_checksum))
        return error.SnapshotChecksumMismatch;
    try walk_archive(file, io, snapshot, members, scratch, null);
}

pub fn build(
    io: Io,
    path: []const u8,
    machine: *replicated.Machine,
    through: raft.Index,
    size_max: u64,
    scratch: []u8,
) !raft.Snapshot {
    if (through <= machine.node.snapshot.last_index or through > machine.node.applied_index)
        return error.InvalidSnapshot;
    if (size_max == 0 or size_max > snapshot_size_hard_max)
        return error.InvalidSnapshotCapacity;
    const old = machine.node.snapshot;
    if (old.last_index > 0) try verify(io, path, old, machine.node.members, scratch);
    var atomic = try IoDir.cwd().createFileAtomic(io, path, .{ .replace = true });
    var atomic_open = true;
    errdefer if (atomic_open) atomic.deinit(io);
    var hash = std.crypto.hash.Blake3.init(.{});
    var position: u64 = 0;
    if (old.last_index == 0) {
        position = try write_header(atomic.file, io, machine.node.members, &hash);
    } else {
        position = try copy_prefix(io, path, atomic.file, old.state_size, scratch, &hash);
    }
    var index = old.last_index + 1;
    while (index <= through) : (index += 1) {
        const entry = entry_at(&machine.node, index) orelse return error.InvalidSnapshot;
        const command = machine.command_at(index) orelse return error.MissingReplicatedCommand;
        position = try append_record(
            atomic.file,
            io,
            position,
            entry,
            command,
            size_max,
            &hash,
        );
    }
    try atomic.file.sync(io);
    try atomic.replace(io);
    try sync_parent_directory(path, io);
    atomic.deinit(io);
    atomic_open = false;
    var state_checksum: [checksum_size]u8 = undefined;
    hash.final(&state_checksum);
    return .{
        .last_index = through,
        .last_term = entry_at(&machine.node, through).?.term,
        .state_size = position,
        .state_checksum = state_checksum,
    };
}

pub fn read_chunk(
    io: Io,
    path: []const u8,
    snapshot: raft.Snapshot,
    offset: u64,
    target: []u8,
) ![]const u8 {
    if (target.len == 0 or target.len > chunk_size_max) return error.InvalidSnapshotChunk;
    if (offset >= snapshot.state_size) return error.InvalidSnapshotOffset;
    const count: usize = @intCast(@min(
        @as(u64, target.len),
        snapshot.state_size - offset,
    ));
    const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_only });
    defer file.close(io);
    if (try file.readPositionalAll(io, target[0..count], offset) != count)
        return error.TruncatedSnapshot;
    return target[0..count];
}

pub fn stage_chunk(
    io: Io,
    incoming_path: []const u8,
    snapshot: raft.Snapshot,
    offset: u64,
    chunk: []const u8,
    done: bool,
) !u64 {
    if (chunk.len == 0 or chunk.len > chunk_size_max) return error.InvalidSnapshotChunk;
    if (offset > snapshot.state_size or chunk.len > snapshot.state_size - offset)
        return error.InvalidSnapshotOffset;
    const next = offset + chunk.len;
    if (done != (next == snapshot.state_size)) return error.InvalidSnapshotChunk;
    const file = if (offset == 0)
        try IoDir.cwd().createFile(io, incoming_path, .{ .truncate = true, .read = true })
    else
        try IoDir.cwd().openFile(io, incoming_path, .{ .mode = .read_write });
    defer file.close(io);
    if ((try file.stat(io)).size != offset) return error.InvalidSnapshotOffset;
    try file.writePositionalAll(io, chunk, offset);
    if (done) try file.sync(io);
    return next;
}

pub fn replay(
    io: Io,
    path: []const u8,
    snapshot: raft.Snapshot,
    members: []const raft.NodeId,
    machine: *replicated.Machine,
    scratch: []u8,
) !void {
    try verify(io, path, snapshot, members, scratch);
    const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_only });
    defer file.close(io);
    try walk_archive(file, io, snapshot, members, scratch, machine);
}

pub fn publish(io: Io, incoming_path: []const u8, archive_path: []const u8) !void {
    try IoDir.cwd().rename(incoming_path, IoDir.cwd(), archive_path, io);
    try sync_parent_directory(archive_path, io);
}

fn validate_metadata(
    snapshot: raft.Snapshot,
    members: []const raft.NodeId,
    scratch: []u8,
) !void {
    if (members.len < raft.nodes_min or members.len > raft.nodes_max or members.len % 2 == 0)
        return error.InvalidSnapshotMembership;
    if (scratch.len < chunk_size_max) return error.SnapshotScratchTooSmall;
    if (snapshot.last_index == 0) {
        if (snapshot.last_term != 0 or snapshot.state_size != 0 or
            !std.mem.allEqual(u8, &snapshot.state_checksum, 0)) return error.InvalidSnapshot;
    } else if (snapshot.last_term == 0 or snapshot.state_size < header_fixed_size + members.len) {
        return error.InvalidSnapshot;
    }
}

fn walk_archive(
    file: IoFile,
    io: Io,
    snapshot: raft.Snapshot,
    members: []const raft.NodeId,
    scratch: []u8,
    machine: ?*replicated.Machine,
) !void {
    const header_size = header_fixed_size + members.len;
    try read_exact(file, io, scratch[0..header_size], 0);
    if (!std.mem.eql(u8, scratch[0..magic.len], magic) or
        std.mem.readInt(u16, scratch[8..10], .big) != format_version or
        std.mem.readInt(u16, scratch[10..12], .big) != 0 or
        scratch[12] != members.len or !std.mem.allEqual(u8, scratch[13..16], 0) or
        !std.mem.eql(u8, scratch[16..header_size], members))
        return error.InvalidSnapshot;
    var position: u64 = header_size;
    var expected_index: raft.Index = 1;
    var last_term: raft.Term = 0;
    while (position < snapshot.state_size) {
        if (snapshot.state_size - position < record_header_size)
            return error.TruncatedSnapshot;
        try read_exact(file, io, scratch[0..record_header_size], position);
        const index = std.mem.readInt(u64, scratch[0..8], .big);
        const term = std.mem.readInt(u64, scratch[8..16], .big);
        const command_size = std.mem.readInt(u32, scratch[16..20], .big);
        const expected_checksum: [checksum_size]u8 = scratch[20..52].*;
        if (index != expected_index or term == 0 or command_size == 0 or
            command_size > scratch.len or
            command_size > snapshot.state_size - position - record_header_size)
            return error.InvalidSnapshot;
        position += record_header_size;
        const command = scratch[0..command_size];
        try read_exact(file, io, command, position);
        if (!std.crypto.timing_safe.eql(
            [checksum_size]u8,
            checksum(command),
            expected_checksum,
        )) return error.SnapshotCommandChecksumMismatch;
        if (machine) |target| try target.apply_archived(index, expected_checksum, command);
        position += command_size;
        expected_index += 1;
        last_term = term;
    }
    if (position != snapshot.state_size or expected_index - 1 != snapshot.last_index or
        last_term != snapshot.last_term) return error.InvalidSnapshot;
}

fn write_header(
    file: IoFile,
    io: Io,
    members: []const raft.NodeId,
    hash: *std.crypto.hash.Blake3,
) !u64 {
    var header: [header_fixed_size + raft.nodes_max]u8 = @splat(0);
    @memcpy(header[0..magic.len], magic);
    std.mem.writeInt(u16, header[8..10], format_version, .big);
    header[12] = @intCast(members.len);
    @memcpy(header[16..][0..members.len], members);
    const bytes = header[0 .. header_fixed_size + members.len];
    try file.writePositionalAll(io, bytes, 0);
    hash.update(bytes);
    return bytes.len;
}

fn copy_prefix(
    io: Io,
    source_path: []const u8,
    target: IoFile,
    size: u64,
    scratch: []u8,
    hash: *std.crypto.hash.Blake3,
) !u64 {
    const source = try IoDir.cwd().openFile(io, source_path, .{ .mode = .read_only });
    defer source.close(io);
    var position: u64 = 0;
    while (position < size) {
        const count: usize = @intCast(@min(@as(u64, scratch.len), size - position));
        const bytes = scratch[0..count];
        if (try source.readPositionalAll(io, bytes, position) != count)
            return error.TruncatedSnapshot;
        try target.writePositionalAll(io, bytes, position);
        hash.update(bytes);
        position += count;
    }
    return position;
}

fn append_record(
    file: IoFile,
    io: Io,
    position_start: u64,
    entry: raft.Entry,
    command: []const u8,
    size_max: u64,
    hash: *std.crypto.hash.Blake3,
) !u64 {
    if (command.len == 0 or command.len > std.math.maxInt(u32))
        return error.InvalidReplicatedCommand;
    if (!std.crypto.timing_safe.eql(
        [checksum_size]u8,
        checksum(command),
        entry.command_checksum,
    )) return error.ReplicatedCommandChecksumMismatch;
    const record_size = record_header_size + command.len;
    if (record_size > size_max -| position_start) return error.SnapshotCapacityExceeded;
    var header: [record_header_size]u8 = undefined;
    std.mem.writeInt(u64, header[0..8], entry.index, .big);
    std.mem.writeInt(u64, header[8..16], entry.term, .big);
    std.mem.writeInt(u32, header[16..20], @intCast(command.len), .big);
    @memcpy(header[20..52], &entry.command_checksum);
    try file.writePositionalAll(io, &header, position_start);
    try file.writePositionalAll(io, command, position_start + header.len);
    hash.update(&header);
    hash.update(command);
    return position_start + record_size;
}

fn entry_at(node: *const raft.Node, index: raft.Index) ?raft.Entry {
    if (index <= node.snapshot.last_index or index > node.last_index()) return null;
    const offset: usize = @intCast(index - node.snapshot.last_index - 1);
    return node.entries[offset];
}

fn hash_prefix(file: IoFile, io: Io, size: u64, scratch: []u8) ![checksum_size]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    var position: u64 = 0;
    while (position < size) {
        const count: usize = @intCast(@min(@as(u64, scratch.len), size - position));
        if (try file.readPositionalAll(io, scratch[0..count], position) != count)
            return error.TruncatedSnapshot;
        hash.update(scratch[0..count]);
        position += count;
    }
    var result: [checksum_size]u8 = undefined;
    hash.final(&result);
    return result;
}

fn checksum(bytes: []const u8) [checksum_size]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(bytes);
    var result: [checksum_size]u8 = undefined;
    hash.final(&result);
    return result;
}

fn read_exact(file: IoFile, io: Io, target: []u8, offset: u64) !void {
    if (try file.readPositionalAll(io, target, offset) != target.len)
        return error.TruncatedSnapshot;
}

fn sync_parent_directory(path: []const u8, io: Io) !void {
    const parent = std.fs.path.dirname(path) orelse ".";
    const handle = try std.posix.openat(
        std.posix.AT.FDCWD,
        parent,
        .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true },
        0,
    );
    const file: IoFile = .{ .handle = handle, .flags = .{ .nonblocking = false } };
    defer file.close(io);
    file.sync(io) catch return error.DirectorySyncFailed;
}

test "staged snapshot chunks enforce offsets and preserve interrupted evidence" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const paths = try Paths.init(allocator, "data/test_snapshot_stage.raft");
    defer paths.deinit(allocator);
    inline for (.{ paths.archive, paths.incoming, paths.quarantine }) |path|
        IoDir.cwd().deleteFile(io, path) catch {};
    defer {
        inline for (.{ paths.archive, paths.incoming, paths.quarantine }) |path|
            IoDir.cwd().deleteFile(io, path) catch {};
    }
    const snapshot: raft.Snapshot = .{
        .last_index = 1,
        .last_term = 1,
        .state_size = 6,
        .state_checksum = @splat(0x11),
    };
    try std.testing.expectEqual(
        @as(u64, 3),
        try stage_chunk(io, paths.incoming, snapshot, 0, "abc", false),
    );
    try std.testing.expectError(
        error.InvalidSnapshotOffset,
        stage_chunk(io, paths.incoming, snapshot, 4, "de", true),
    );
    try std.testing.expect(try quarantine_incoming(io, paths));
    try std.testing.expect(!(try quarantine_incoming(io, paths)));
    const evidence = try IoDir.cwd().openFile(io, paths.quarantine, .{ .mode = .read_only });
    evidence.close(io);
}
