const std = @import("std");
const assert = std.debug.assert;
const event = @import("event.zig");
const limits = @import("limits.zig");
const memtable_mod = @import("memtable.zig");
const wal_cursor = @import("wal_cursor.zig");
const EventValue = event.EventValue;
const Memtable = memtable_mod.Memtable;
const WalCursor = wal_cursor.WalCursor;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;

pub const magic_number: u32 = 0xC0BEEFD9;
pub const format_version: u16 = 1;
pub const checksum_size: u32 = 16;
pub const header_size: u32 = 36;
pub const entry_size_max: u32 = limits.wal_entry_size_max;

const write_buf_size: u32 = 512 * 1024;

const value_serialized_reachable_max: u32 = limits.payload_size_max +
    limits.event_type_length_max + limits.service_name_length_max + 64;
const entry_reachable_max: u32 = 4 + limits.key_length_max + 4 +
    value_serialized_reachable_max + 1;

comptime {
    // Header layout: [4B magic][2B version][2B flags][8B sequence]
    // [4B entry_len][16B BLAKE3 checksum]. The checksum covers the first
    // 20 header bytes and the complete entry payload.
    assert(header_size == 4 + 2 + 2 + 8 + 4 + checksum_size);
    assert(entry_size_max > 0);
    assert(write_buf_size > 0);
    assert(entry_reachable_max <= entry_size_max);
    assert(header_size + entry_reachable_max <= write_buf_size);
}

fn write_all_positional(file: IoFile, io: Io, buf: []const u8, offset: u64) !void {
    file.writePositionalAll(io, buf, offset) catch return error.InputOutput;
}

fn read_exact_io(file: IoFile, io: Io, buf: []u8, offset: u64) !void {
    var total_read: limits.ByteCount = 0;
    while (total_read < buf.len) {
        var iovecs = [1][]u8{buf[total_read..]};
        const n = file.readPositional(io, &iovecs, offset + total_read) catch
            return error.InvalidFormat;
        if (n == 0) return error.InvalidFormat;
        total_read += try limits.byte_count(n);
    }
}

pub const WriteAheadLog = struct {
    file: IoFile,
    io: Io,
    allocator: std.mem.Allocator,
    wal_path: []const u8,
    flushing_path: []const u8,
    cursor_path: []const u8,
    write_pos: u64,
    write_buf: []u8,
    buf_len: limits.ByteCount,
    replay_buf: []u8,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8, io: Io) !WriteAheadLog {
        const dir = IoDir.cwd();

        dir.createDir(io, "data", .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const open_options = IoDir.OpenFileOptions{
            .mode = .read_write,
            .lock = .exclusive,
            .lock_nonblocking = true,
        };
        var created = false;
        const file = dir.openFile(io, file_path, open_options) catch |err| switch (err) {
            error.FileNotFound => created_file: {
                const new_file = try dir.createFile(
                    io,
                    file_path,
                    .{
                        .truncate = false,
                        .read = true,
                        .lock = .exclusive,
                        .lock_nonblocking = true,
                    },
                );
                created = true;
                break :created_file new_file;
            },
            else => return err,
        };
        errdefer file.close(io);
        if (created) {
            file.sync(io) catch return error.SyncFailed;
            try sync_parent_directory(file_path, io);
        }

        const file_size = (try file.stat(io)).size;

        const wb = try allocator.alloc(u8, write_buf_size);
        errdefer allocator.free(wb);
        const replay_buf = try allocator.alloc(u8, entry_size_max);
        errdefer allocator.free(replay_buf);
        const cursor_path_buf = try allocator.alloc(u8, limits.wal_path_length_max);
        defer allocator.free(cursor_path_buf);
        const cursor_path_view = try wal_cursor.derive_path_into(cursor_path_buf, file_path);
        const cursor_path = try allocator.dupe(u8, cursor_path_view);
        errdefer allocator.free(cursor_path);
        const flushing_path = try std.fmt.allocPrint(allocator, "{s}.flushing", .{file_path});
        errdefer allocator.free(flushing_path);

        return WriteAheadLog{
            .file = file,
            .io = io,
            .allocator = allocator,
            .wal_path = try allocator.dupe(u8, file_path),
            .flushing_path = flushing_path,
            .cursor_path = cursor_path,
            .write_pos = file_size,
            .write_buf = wb,
            .buf_len = 0,
            .replay_buf = replay_buf,
        };
    }

    pub fn deinit(self: *WriteAheadLog) void {
        // A failed fsync means buffered durability is already lost and retrying
        // cannot recover it; crashing is the only honest option.
        self.sync() catch @panic("wal: sync failed during deinit");
        self.file.close(self.io);
        self.allocator.free(self.write_buf);
        self.allocator.free(self.replay_buf);
        self.allocator.free(self.cursor_path);
        self.allocator.free(self.flushing_path);
        self.allocator.free(self.wal_path);
    }

    pub fn sync(self: *WriteAheadLog) !void {
        if (self.buf_len == 0) return;
        const buffered = self.write_buf[0..self.buf_len];
        try write_all_positional(self.file, self.io, buffered, self.write_pos);
        self.write_pos += self.buf_len;
        self.buf_len = 0;
        self.file.sync(self.io) catch return error.SyncFailed;
        assert(self.buf_len == 0);
    }

    pub fn flush(self: *WriteAheadLog) !void {
        try self.sync();
    }

    pub fn append_event(self: *WriteAheadLog, key: []const u8, value: EventValue) !void {
        assert(key.len > 0);
        assert(key.len <= limits.key_length_max);

        const serialized_value_size = value.serialized_size();
        const entry_len: u32 = @intCast(4 + key.len + 4 + serialized_value_size + 1);
        const total_size: limits.ByteCount = header_size + entry_len;
        if (entry_len > entry_size_max or total_size > self.write_buf.len)
            return error.WalEntryTooLarge;

        if (self.buf_len + total_size > self.write_buf.len) {
            try self.sync();
        }

        const target = self.write_buf[self.buf_len..][0..total_size];
        try serialize_entry(target, key, value, serialized_value_size, entry_len);
        self.buf_len += total_size;
    }

    fn serialize_entry(
        buf: []u8,
        key: []const u8,
        value: EventValue,
        serialized_value_size: u32,
        entry_len: u32,
    ) !void {
        assert(buf.len == header_size + entry_len);

        var pos: limits.ByteCount = 0;
        std.mem.writeInt(u32, buf[pos..][0..4], magic_number, .little);
        pos += 4;
        std.mem.writeInt(u16, buf[pos..][0..2], format_version, .little);
        pos += 2;
        std.mem.writeInt(u16, buf[pos..][0..2], 0, .little);
        pos += 2;
        std.mem.writeInt(u64, buf[pos..][0..8], value.sequence, .little);
        pos += 8;
        std.mem.writeInt(u32, buf[pos..][0..4], entry_len, .little);
        pos += 4;
        const checksum_position = pos;
        pos += checksum_size;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(key.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..key.len], key);
        pos += try limits.byte_count(key.len);
        std.mem.writeInt(u32, buf[pos..][0..4], serialized_value_size, .little);
        pos += 4;
        const serialized_value = try value.serialize_into(buf[pos..][0..serialized_value_size]);
        assert(serialized_value.len == serialized_value_size);
        pos += serialized_value_size;
        buf[pos] = '\n';
        assert(pos + 1 == buf.len);
        const checksum = entry_checksum(buf[0..20], buf[header_size..]);
        @memcpy(buf[checksum_position..][0..checksum_size], &checksum);
    }

    pub fn truncate(self: *WriteAheadLog) !void {
        try self.sync();
        self.file.close(self.io);
        const dir = IoDir.cwd();
        self.file = try dir.createFile(self.io, self.wal_path, .{
            .truncate = true,
            .read = true,
            .lock = .exclusive,
            .lock_nonblocking = true,
        });
        self.file.sync(self.io) catch return error.SyncFailed;
        self.write_pos = 0;
        self.buf_len = 0;
    }

    pub fn replay(self: *WriteAheadLog, target: anytype) !WalCursor {
        const crumb = try self.replay_file(self.file, target);
        try wal_cursor.save_path(self.cursor_path, crumb, self.io);
        self.write_pos = crumb.file_offset;
        return crumb;
    }

    pub fn flushing_exists(self: *WriteAheadLog) !bool {
        const file = IoDir.cwd().openFile(
            self.io,
            self.flushing_path,
            .{ .mode = .read_only },
        ) catch |err| switch (err) {
            error.FileNotFound => return false,
            else => return err,
        };
        file.close(self.io);
        return true;
    }

    pub fn replay_flushing(self: *WriteAheadLog, target: anytype) !void {
        const file = IoDir.cwd().openFile(self.io, self.flushing_path, .{
            .mode = .read_write,
            .lock = .exclusive,
            .lock_nonblocking = true,
        }) catch |err| switch (err) {
            error.FileNotFound => return,
            else => return err,
        };
        defer file.close(self.io);
        _ = try self.replay_file(file, target);
    }

    pub fn retire_flushing(self: *WriteAheadLog) !void {
        IoDir.cwd().deleteFile(self.io, self.flushing_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return error.InputOutput,
        };
        try sync_parent_directory(self.flushing_path, self.io);
    }

    pub fn rotate(self: *WriteAheadLog) !void {
        try self.sync();
        self.file.close(self.io);
        const dir = IoDir.cwd();
        try dir.renamePreserve(self.wal_path, dir, self.flushing_path, self.io);
        self.file = try dir.createFile(self.io, self.wal_path, .{
            .truncate = true,
            .read = true,
            .lock = .exclusive,
            .lock_nonblocking = true,
        });
        self.file.sync(self.io) catch return error.SyncFailed;
        try sync_parent_directory(self.wal_path, self.io);
        self.write_pos = 0;
        self.buf_len = 0;
    }

    fn replay_file(self: *WriteAheadLog, file: IoFile, target: anytype) !WalCursor {
        const file_size = (try file.stat(self.io)).size;
        var offset: u64 = 0;
        var crumb = WalCursor{ .file_offset = 0, .last_sequence = 0 };

        while (offset < file_size) {
            if (offset + header_size > file_size) break;

            var header: [header_size]u8 = undefined;
            try read_exact_io(file, self.io, &header, offset);
            const magic = std.mem.readInt(u32, header[0..4], .little);
            if (magic != magic_number) return error.InvalidFormat;
            if (std.mem.readInt(u16, header[4..6], .little) != format_version)
                return error.UnsupportedWalVersion;
            if (std.mem.readInt(u16, header[6..8], .little) != 0)
                return error.InvalidFormat;
            const sequence = std.mem.readInt(u64, header[8..16], .little);
            const entry_len = std.mem.readInt(u32, header[16..20], .little);
            if (entry_len < 9 or entry_len > entry_size_max) return error.InvalidFormat;

            if (offset + header_size + entry_len > file_size) break;
            const entry_buf = self.replay_buf[0..entry_len];
            try read_exact_io(file, self.io, entry_buf, offset + header_size);
            const checksum = entry_checksum(header[0..20], entry_buf);
            if (!std.crypto.timing_safe.eql(
                [checksum_size]u8,
                checksum,
                header[20..header_size].*,
            )) return error.WalChecksumMismatch;

            const key_len = std.mem.readInt(u32, entry_buf[0..4], .little);
            if (key_len > limits.key_length_max) return error.InvalidFormat;
            const key_end: u32 = 4 + key_len;
            if (key_end + 5 > entry_len) return error.InvalidFormat;
            const key = entry_buf[4..key_end];
            const value_len = std.mem.readInt(u32, entry_buf[key_end..][0..4], .little);
            const value_start = key_end + 4;
            const expected_entry_len: u32 = value_start + value_len + 1;
            if (entry_len != expected_entry_len) return error.InvalidFormat;
            if (entry_buf[entry_len - 1] != '\n') return error.InvalidFormat;
            const value_bytes = entry_buf[value_start .. value_start + value_len];

            const deserialized = try EventValue.deserialize_borrowed(value_bytes);
            if (deserialized.sequence != sequence) return error.InvalidFormat;
            try target.insert(key, deserialized);
            offset += header_size + entry_len;

            crumb.file_offset = offset;
            crumb.last_sequence = sequence;
        }

        assert(offset <= file_size);
        assert(crumb.file_offset <= offset);

        if (offset != file_size) {
            file.setLength(self.io, offset) catch return error.InputOutput;
            file.sync(self.io) catch return error.SyncFailed;
        }

        return crumb;
    }

    pub fn find_by_sequence(self: *WriteAheadLog, target: u64) !?u64 {
        const file_size = (try self.file.stat(self.io)).size;
        if (file_size == 0) return null;

        var lo: u64 = 0;
        var hi: u64 = file_size;

        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;

            if (try self.scan_for_entry(mid)) |entry_offset| {
                if (entry_offset >= hi) {
                    hi = mid;
                    continue;
                }

                const seq = try self.read_sequence_at(entry_offset);
                if (seq == target) return entry_offset;
                if (seq < target) {
                    const entry_size = try self.read_entry_len_at(entry_offset);
                    lo = entry_offset + header_size + entry_size;
                } else {
                    hi = entry_offset;
                }
            } else {
                hi = mid;
            }
        }

        return null;
    }

    fn scan_for_entry(self: *WriteAheadLog, start: u64) !?u64 {
        const file_size = (try self.file.stat(self.io)).size;
        const search_limit = @min(start + entry_size_max, file_size);
        var pos = start;

        while (pos < search_limit) {
            var buf: [4096]u8 = undefined;
            const to_read: limits.ByteCount = @intCast(
                @min(@as(u64, buf.len), search_limit - pos),
            );
            var iovecs = [1][]u8{buf[0..to_read]};
            const bytes_read = self.file.readPositional(self.io, &iovecs, pos) catch return null;
            if (bytes_read == 0) return null;

            if (bytes_read < 4) {
                pos += bytes_read;
                continue;
            }

            var i: limits.ByteCount = 0;
            while (i + 4 <= bytes_read) : (i += 1) {
                if (std.mem.readInt(u32, buf[i..][0..4], .little) == magic_number) {
                    const candidate_offset = pos + i;
                    if (try self.validate_entry_at(candidate_offset)) {
                        return candidate_offset;
                    }
                }
            }
            pos += bytes_read - 3;
        }

        return null;
    }

    fn validate_entry_at(self: *WriteAheadLog, offset: u64) !bool {
        var header: [header_size]u8 = undefined;
        var iovecs = [1][]u8{&header};
        const n = self.file.readPositional(self.io, &iovecs, offset) catch return false;
        if (n < header_size) return false;

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != magic_number) return false;
        if (std.mem.readInt(u16, header[4..6], .little) != format_version) return false;
        if (std.mem.readInt(u16, header[6..8], .little) != 0) return false;
        const entry_len = std.mem.readInt(u32, header[16..20], .little);
        if (entry_len < 9 or entry_len > entry_size_max) return false;
        const file_size = (try self.file.stat(self.io)).size;
        if (offset + header_size + entry_len > file_size) return false;
        const entry_buf = self.replay_buf[0..entry_len];
        read_exact_io(self.file, self.io, entry_buf, offset + header_size) catch return false;
        const expected = entry_checksum(header[0..20], entry_buf);
        return std.crypto.timing_safe.eql(
            [checksum_size]u8,
            expected,
            header[20..header_size].*,
        );
    }

    fn read_sequence_at(self: *WriteAheadLog, offset: u64) !u64 {
        var seq_bytes: [8]u8 = undefined;
        try read_exact_io(self.file, self.io, &seq_bytes, offset + 8);
        return std.mem.readInt(u64, &seq_bytes, .little);
    }

    fn read_entry_len_at(self: *WriteAheadLog, offset: u64) !u32 {
        var len_bytes: [4]u8 = undefined;
        try read_exact_io(self.file, self.io, &len_bytes, offset + 16);
        return std.mem.readInt(u32, &len_bytes, .little);
    }
};

fn entry_checksum(header_prefix: []const u8, entry: []const u8) [checksum_size]u8 {
    assert(header_prefix.len == 20);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(header_prefix);
    hash.update(entry);
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hash.final(&digest);
    return digest[0..checksum_size].*;
}

fn sync_parent_directory(path: []const u8, io: Io) !void {
    const parent_path = std.fs.path.dirname(path) orelse ".";
    const handle = try std.posix.openat(
        std.posix.AT.FDCWD,
        parent_path,
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

test "find_by_sequence returns correct offsets" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_find_seq.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_find_seq.crumb") catch {};

    var wal_log = try WriteAheadLog.init(allocator, "data/test_find_seq.wal", io);
    defer {
        wal_log.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_find_seq.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_find_seq.crumb") catch {};
    }

    for (0..10) |i| {
        const seq: u64 = (i + 1) * 100;
        const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
        defer allocator.free(key);
        const val = EventValue{
            .sequence = seq,
            .event_type = "test.event",
            .payload = "payload",
            .timestamp = 0,
            .origin_service = "test-service",
        };
        try wal_log.append_event(key, val);
    }

    try wal_log.flush();

    const offset_100 = try wal_log.find_by_sequence(100);
    try std.testing.expect(offset_100 != null);
    try std.testing.expectEqual(@as(u64, 0), offset_100.?);

    const offset_500 = try wal_log.find_by_sequence(500);
    try std.testing.expect(offset_500 != null);
    const seq_at_500 = try wal_log.read_sequence_at(offset_500.?);
    try std.testing.expectEqual(@as(u64, 500), seq_at_500);

    const offset_1000 = try wal_log.find_by_sequence(1000);
    try std.testing.expect(offset_1000 != null);
    const seq_at_1000 = try wal_log.read_sequence_at(offset_1000.?);
    try std.testing.expectEqual(@as(u64, 1000), seq_at_1000);

    const offset_999 = try wal_log.find_by_sequence(999);
    try std.testing.expect(offset_999 == null);
}

test "replay with crumb resumes from correct position" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_replay_crumb.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_replay_crumb.crumb") catch {};

    {
        var wal_log = try WriteAheadLog.init(allocator, "data/test_replay_crumb.wal", io);
        defer wal_log.deinit();

        var tree1 = try Memtable.init(allocator);
        defer tree1.deinit();
        _ = try wal_log.replay(&tree1);

        for (0..5) |i| {
            const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
            defer allocator.free(key);
            const val = EventValue{
                .sequence = i + 1,
                .event_type = "test.event",
                .payload = "payload",
                .timestamp = 0,
                .origin_service = "test-service",
            };
            try wal_log.append_event(key, val);
        }
    }

    {
        var wal_log = try WriteAheadLog.init(allocator, "data/test_replay_crumb.wal", io);
        defer wal_log.deinit();

        var tree2 = try Memtable.init(allocator);
        defer tree2.deinit();

        const crumb2 = try wal_log.replay(&tree2);
        try std.testing.expectEqual(@as(u64, 5), crumb2.last_sequence);
        _ = tree2.get_ptr("key_0") orelse return error.ExpectedValueNotFound;
        _ = tree2.get_ptr("key_4") orelse return error.ExpectedValueNotFound;

        for (5..8) |i| {
            const key = try std.fmt.allocPrint(allocator, "key_{}", .{i});
            defer allocator.free(key);
            const val = EventValue{
                .sequence = i + 1,
                .event_type = "test.event",
                .payload = "payload",
                .timestamp = 0,
                .origin_service = "test-service",
            };
            try wal_log.append_event(key, val);
        }
    }

    {
        var wal_log = try WriteAheadLog.init(allocator, "data/test_replay_crumb.wal", io);
        defer wal_log.deinit();

        var tree3 = try Memtable.init(allocator);
        defer tree3.deinit();

        const crumb3 = try wal_log.replay(&tree3);
        try std.testing.expectEqual(@as(u64, 8), crumb3.last_sequence);

        // Full replay from offset 0: all 8 keys should be present
        _ = tree3.get_ptr("key_0") orelse return error.ExpectedValueNotFound;
        _ = tree3.get_ptr("key_4") orelse return error.ExpectedValueNotFound;
        _ = tree3.get_ptr("key_5") orelse return error.ExpectedValueNotFound;
        _ = tree3.get_ptr("key_7") orelse return error.ExpectedValueNotFound;
    }

    Io.Dir.cwd().deleteFile(io, "data/test_replay_crumb.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_replay_crumb.crumb") catch {};
}

test "replay rejects a one-bit mutation in a complete record" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_wal_checksum.wal";
    IoDir.cwd().deleteFile(io, path) catch {};
    IoDir.cwd().deleteFile(io, "data/test_wal_checksum.crumb") catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, "data/test_wal_checksum.crumb") catch {};

    {
        var wal = try WriteAheadLog.init(allocator, path, io);
        defer wal.deinit();
        try wal.append_event("key", .{
            .sequence = 1,
            .event_type = "test.event",
            .payload = "payload",
            .timestamp = 0,
            .origin_service = "test-service",
        });
        try wal.sync();
    }
    {
        const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
        defer file.close(io);
        var byte: [1]u8 = undefined;
        try read_exact_io(file, io, &byte, header_size + 4);
        byte[0] ^= 1;
        try file.writePositionalAll(io, &byte, header_size + 4);
        try file.sync(io);
    }
    var wal = try WriteAheadLog.init(allocator, path, io);
    defer wal.deinit();
    var tree = try Memtable.init(allocator);
    defer tree.deinit();
    try std.testing.expectError(error.WalChecksumMismatch, wal.replay(&tree));
}

test "replay truncates a torn final record before accepting new writes" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_wal_torn_tail.wal";
    IoDir.cwd().deleteFile(io, path) catch {};
    IoDir.cwd().deleteFile(io, "data/test_wal_torn_tail.crumb") catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, "data/test_wal_torn_tail.crumb") catch {};

    var second_offset: u64 = 0;
    var full_size: u64 = 0;
    {
        var wal = try WriteAheadLog.init(allocator, path, io);
        defer wal.deinit();
        for (1..3) |sequence| {
            var key_buffer: [8]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "key-{d}", .{sequence});
            try wal.append_event(key, .{
                .sequence = sequence,
                .event_type = "test.event",
                .payload = "payload",
                .timestamp = 0,
                .origin_service = "test-service",
            });
        }
        try wal.sync();
        second_offset = (try wal.find_by_sequence(2)).?;
        full_size = (try wal.file.stat(io)).size;
    }
    const original = try allocator.alloc(u8, full_size);
    defer allocator.free(original);
    {
        const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_only });
        defer file.close(io);
        if (try file.readPositionalAll(io, original, 0) != original.len)
            return error.TruncatedTestWal;
    }
    const second_record_size = full_size - second_offset;
    for (0..second_record_size) |partial_size| {
        const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
        try file.setLength(io, full_size);
        try file.writePositionalAll(io, original, 0);
        try file.setLength(io, second_offset + partial_size);
        try file.sync(io);
        file.close(io);
        IoDir.cwd().deleteFile(io, "data/test_wal_torn_tail.crumb") catch {};
        var wal = try WriteAheadLog.init(allocator, path, io);
        defer wal.deinit();
        var tree = try Memtable.init(allocator);
        defer tree.deinit();
        const cursor = try wal.replay(&tree);
        try std.testing.expectEqual(@as(u64, 1), cursor.last_sequence);
        try std.testing.expectEqual(second_offset, (try wal.file.stat(io)).size);
    }
    {
        var wal = try WriteAheadLog.init(allocator, path, io);
        defer wal.deinit();
        try wal.append_event("key-3", .{
            .sequence = 3,
            .event_type = "test.event",
            .payload = "payload",
            .timestamp = 0,
            .origin_service = "test-service",
        });
        try wal.sync();
    }
    {
        var wal = try WriteAheadLog.init(allocator, path, io);
        defer wal.deinit();
        var tree = try Memtable.init(allocator);
        defer tree.deinit();
        const cursor = try wal.replay(&tree);
        try std.testing.expectEqual(@as(u64, 3), cursor.last_sequence);
        try std.testing.expect(tree.contains("key-1"));
        try std.testing.expect(!tree.contains("key-2"));
        try std.testing.expect(tree.contains("key-3"));
    }
}

test "append_event accepts the largest reachable entry and rejects oversize entries" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_wal_entry_bounds.wal";
    IoDir.cwd().deleteFile(io, path) catch {};
    IoDir.cwd().deleteFile(io, "data/test_wal_entry_bounds.crumb") catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, "data/test_wal_entry_bounds.crumb") catch {};

    var wal = try WriteAheadLog.init(allocator, path, io);
    defer wal.deinit();

    const payload = try allocator.alloc(u8, limits.payload_size_max);
    defer allocator.free(payload);
    @memset(payload, 'x');
    const key = try allocator.alloc(u8, limits.key_length_max);
    defer allocator.free(key);
    @memset(key, 'k');
    const max_value = try EventValue.init(.{
        .sequence = 1,
        .origin_service = "compix",
        .event_type = "compix.append-batch",
        .payload = payload,
        .timestamp = 0,
    });
    try wal.append_event(key, max_value);
    try wal.sync();

    const oversize_payload = try allocator.alloc(u8, write_buf_size);
    defer allocator.free(oversize_payload);
    @memset(oversize_payload, 'y');
    const oversize_value = EventValue{
        .sequence = 2,
        .origin_service = "compix",
        .event_type = "compix.append-batch",
        .payload = oversize_payload,
        .timestamp = 0,
    };
    try std.testing.expectError(
        error.WalEntryTooLarge,
        wal.append_event(key, oversize_value),
    );
}

test "rotate splits the log into a flushing segment recovered before the active one" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_wal_rotate.wal";
    IoDir.cwd().deleteFile(io, path) catch {};
    IoDir.cwd().deleteFile(io, "data/test_wal_rotate.wal.flushing") catch {};
    IoDir.cwd().deleteFile(io, "data/test_wal_rotate.crumb") catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, "data/test_wal_rotate.wal.flushing") catch {};
    defer IoDir.cwd().deleteFile(io, "data/test_wal_rotate.crumb") catch {};

    {
        var wal = try WriteAheadLog.init(allocator, path, io);
        defer wal.deinit();
        for (1..3) |sequence| {
            var key_buffer: [8]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "pre-{d}", .{sequence});
            try wal.append_event(key, .{
                .sequence = sequence,
                .event_type = "test.event",
                .payload = "payload",
                .timestamp = 0,
                .origin_service = "test-service",
            });
        }
        try wal.rotate();
        try std.testing.expect(try wal.flushing_exists());
        for (3..5) |sequence| {
            var key_buffer: [8]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "post-{d}", .{sequence});
            try wal.append_event(key, .{
                .sequence = sequence,
                .event_type = "test.event",
                .payload = "payload",
                .timestamp = 0,
                .origin_service = "test-service",
            });
        }
        try wal.sync();
    }

    var wal = try WriteAheadLog.init(allocator, path, io);
    defer wal.deinit();
    try std.testing.expect(try wal.flushing_exists());
    var tree = try Memtable.init(allocator);
    defer tree.deinit();
    try wal.replay_flushing(&tree);
    const active = try wal.replay(&tree);
    try std.testing.expectEqual(@as(u64, 4), active.last_sequence);
    try std.testing.expect(tree.contains("pre-1"));
    try std.testing.expect(tree.contains("pre-2"));
    try std.testing.expect(tree.contains("post-3"));
    try std.testing.expect(tree.contains("post-4"));
    try wal.retire_flushing();
    try std.testing.expect(!try wal.flushing_exists());
}
