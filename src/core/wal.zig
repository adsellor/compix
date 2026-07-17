const std = @import("std");
const assert = std.debug.assert;
const radix = @import("radix.zig");
const event = @import("event.zig");
const wal_cursor = @import("wal_cursor.zig");
const EventValue = event.EventValue;
const RadixTree = radix.RadixTree;
const WalCursor = wal_cursor.WalCursor;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;

pub const magic_number: u32 = 0xC0BEEFD8;
pub const header_size: u32 = 16;
pub const entry_size_max: u32 = 4 * 1024 * 1024;

const write_buf_size: u32 = 256 * 1024;

comptime {
    // Header layout: [4B magic][8B sequence][4B entry_len].
    assert(header_size == 4 + 8 + 4);
    assert(entry_size_max > 0);
    assert(write_buf_size > 0);
}

fn write_all_positional(file: IoFile, io: Io, buf: []const u8, offset: u64) !void {
    file.writePositionalAll(io, buf, offset) catch return error.InputOutput;
}

fn read_exact_io(file: IoFile, io: Io, buf: []u8, offset: u64) !void {
    var total_read: usize = 0;
    while (total_read < buf.len) {
        var iovecs = [1][]u8{buf[total_read..]};
        const n = file.readPositional(io, &iovecs, offset + total_read) catch
            return error.InvalidFormat;
        if (n == 0) return error.InvalidFormat;
        total_read += n;
    }
}

pub const WriteAheadLog = struct {
    file: IoFile,
    io: Io,
    allocator: std.mem.Allocator,
    wal_path: []const u8,
    write_pos: u64,
    write_buf: []u8,
    buf_len: usize,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8, io: Io) !WriteAheadLog {
        const dir = IoDir.cwd();

        dir.createDir(io, "data", .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const open_options = IoDir.OpenFileOptions{ .mode = .read_write };
        const file = dir.openFile(io, file_path, open_options) catch |err| switch (err) {
            error.FileNotFound => try dir.createFile(
                io,
                file_path,
                .{ .truncate = false, .read = true },
            ),
            else => return err,
        };

        const file_size = (try file.stat(io)).size;

        const wb = try allocator.alloc(u8, write_buf_size);
        errdefer allocator.free(wb);

        return WriteAheadLog{
            .file = file,
            .io = io,
            .allocator = allocator,
            .wal_path = try allocator.dupe(u8, file_path),
            .write_pos = file_size,
            .write_buf = wb,
            .buf_len = 0,
        };
    }

    pub fn deinit(self: *WriteAheadLog) void {
        // A failed fsync means buffered durability is already lost and retrying
        // cannot recover it; crashing is the only honest option.
        self.sync() catch @panic("wal: sync failed during deinit");
        self.file.close(self.io);
        self.allocator.free(self.write_buf);
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
        assert(key.len <= radix.key_length_max);

        const serialized_value = try value.serialize(self.allocator);
        defer self.allocator.free(serialized_value);

        const entry_len: u32 = @intCast(4 + key.len + 4 + serialized_value.len + 1);
        const total_size: usize = header_size + entry_len;
        assert(entry_len <= entry_size_max);

        if (self.buf_len + total_size > self.write_buf.len) {
            try self.sync();
            if (total_size > self.write_buf.len) {
                const buf = try self.allocator.alloc(u8, total_size);
                defer self.allocator.free(buf);
                serialize_entry(buf, key, value, serialized_value, entry_len);
                try write_all_positional(self.file, self.io, buf, self.write_pos);
                self.write_pos += total_size;
                return;
            }
        }

        const target = self.write_buf[self.buf_len..][0..total_size];
        serialize_entry(target, key, value, serialized_value, entry_len);
        self.buf_len += total_size;
    }

    fn serialize_entry(
        buf: []u8,
        key: []const u8,
        value: EventValue,
        serialized_value: []const u8,
        entry_len: u32,
    ) void {
        assert(buf.len == header_size + entry_len);

        var pos: usize = 0;
        std.mem.writeInt(u32, buf[pos..][0..4], magic_number, .little);
        pos += 4;
        std.mem.writeInt(u64, buf[pos..][0..8], value.sequence, .little);
        pos += 8;
        std.mem.writeInt(u32, buf[pos..][0..4], entry_len, .little);
        pos += 4;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(key.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..key.len], key);
        pos += key.len;
        std.mem.writeInt(u32, buf[pos..][0..4], @intCast(serialized_value.len), .little);
        pos += 4;
        @memcpy(buf[pos..][0..serialized_value.len], serialized_value);
        pos += serialized_value.len;
        buf[pos] = '\n';
        assert(pos + 1 == buf.len);
    }

    pub fn truncate(self: *WriteAheadLog) !void {
        try self.sync();
        self.file.close(self.io);
        const dir = IoDir.cwd();
        self.file = try dir.createFile(self.io, self.wal_path, .{ .truncate = true, .read = true });
        self.write_pos = 0;
        self.buf_len = 0;
    }

    pub fn replay(self: *WriteAheadLog, target: anytype) !WalCursor {
        const file_size = (try self.file.stat(self.io)).size;
        var offset: u64 = 0;
        var crumb = WalCursor{ .file_offset = 0, .last_sequence = 0 };

        while (offset < file_size) {
            if (offset + 4 > file_size) break;

            var magic_bytes: [4]u8 = undefined;
            read_exact_io(self.file, self.io, &magic_bytes, offset) catch break;
            offset += 4;

            const magic = std.mem.readInt(u32, &magic_bytes, .little);
            if (magic != magic_number) return error.InvalidFormat;

            var seq_bytes: [8]u8 = undefined;
            try read_exact_io(self.file, self.io, &seq_bytes, offset);
            const sequence = std.mem.readInt(u64, &seq_bytes, .little);
            offset += 8;

            var entry_len_bytes: [4]u8 = undefined;
            try read_exact_io(self.file, self.io, &entry_len_bytes, offset);
            const entry_len = std.mem.readInt(u32, &entry_len_bytes, .little);
            if (entry_len > entry_size_max) return error.InvalidFormat;
            offset += 4;

            var key_len_bytes: [4]u8 = undefined;
            try read_exact_io(self.file, self.io, &key_len_bytes, offset);
            const key_len = std.mem.readInt(u32, &key_len_bytes, .little);
            if (key_len > entry_len) return error.InvalidFormat;
            offset += 4;

            const key_buf = try self.allocator.alloc(u8, key_len);
            defer self.allocator.free(key_buf);
            try read_exact_io(self.file, self.io, key_buf, offset);
            offset += key_len;

            var value_len_bytes: [4]u8 = undefined;
            try read_exact_io(self.file, self.io, &value_len_bytes, offset);
            const value_len = std.mem.readInt(u32, &value_len_bytes, .little);
            offset += 4;

            const expected_entry_len: u32 = @intCast(4 + key_len + 4 + value_len + 1);
            if (entry_len != expected_entry_len) return error.InvalidFormat;

            const value_buf = try self.allocator.alloc(u8, value_len);
            defer self.allocator.free(value_buf);
            try read_exact_io(self.file, self.io, value_buf, offset);
            offset += value_len;

            // Skip newline
            offset += 1;

            const deserialized = try EventValue.deserialize(self.allocator, value_buf);
            try target.insert(key_buf, deserialized);

            crumb.file_offset = offset;
            crumb.last_sequence = sequence;
        }

        assert(offset <= file_size);
        assert(crumb.file_offset <= offset);

        try wal_cursor.save(self.allocator, self.wal_path, crumb);

        self.write_pos = offset;

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
            const to_read: usize = @intCast(@min(@as(u64, buf.len), search_limit - pos));
            var iovecs = [1][]u8{buf[0..to_read]};
            const bytes_read = self.file.readPositional(self.io, &iovecs, pos) catch return null;
            if (bytes_read == 0) return null;

            if (bytes_read < 4) {
                pos += bytes_read;
                continue;
            }

            var i: usize = 0;
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

        const entry_len = std.mem.readInt(u32, header[12..16], .little);
        if (entry_len == 0 or entry_len > entry_size_max) return false;

        return true;
    }

    fn read_sequence_at(self: *WriteAheadLog, offset: u64) !u64 {
        var seq_bytes: [8]u8 = undefined;
        try read_exact_io(self.file, self.io, &seq_bytes, offset + 4);
        return std.mem.readInt(u64, &seq_bytes, .little);
    }

    fn read_entry_len_at(self: *WriteAheadLog, offset: u64) !u32 {
        var len_bytes: [4]u8 = undefined;
        try read_exact_io(self.file, self.io, &len_bytes, offset + 12);
        return std.mem.readInt(u32, &len_bytes, .little);
    }
};

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

        var tree1 = try RadixTree.init(allocator);
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

        var tree2 = try RadixTree.init(allocator);
        defer tree2.deinit();

        const crumb2 = try wal_log.replay(&tree2);
        try std.testing.expectEqual(@as(u64, 5), crumb2.last_sequence);
        _ = tree2.get("key_0") orelse return error.ExpectedValueNotFound;
        _ = tree2.get("key_4") orelse return error.ExpectedValueNotFound;

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

        var tree3 = try RadixTree.init(allocator);
        defer tree3.deinit();

        const crumb3 = try wal_log.replay(&tree3);
        try std.testing.expectEqual(@as(u64, 8), crumb3.last_sequence);

        // Full replay from offset 0: all 8 keys should be present
        _ = tree3.get("key_0") orelse return error.ExpectedValueNotFound;
        _ = tree3.get("key_4") orelse return error.ExpectedValueNotFound;
        _ = tree3.get("key_5") orelse return error.ExpectedValueNotFound;
        _ = tree3.get("key_7") orelse return error.ExpectedValueNotFound;
    }

    Io.Dir.cwd().deleteFile(io, "data/test_replay_crumb.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_replay_crumb.crumb") catch {};
}
