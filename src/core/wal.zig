const std = @import("std");
const radix = @import("radix.zig");
const event = @import("event.zig");
const wal_cursor = @import("wal_cursor.zig");
const EventValue = event.EventValue;
const RadixTree = radix.RadixTree;
const WalCursor = wal_cursor.WalCursor;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;

pub const MAGIC: u32 = 0xC0BEEFD8;
pub const HEADER_SIZE: usize = 16;
pub const MAX_ENTRY_SIZE: usize = 4 * 1024 * 1024;

const SYNC_INITIAL_BACKOFF_NS: u64 = 1_000_000; // 1ms
const SYNC_MAX_BACKOFF_NS: u64 = 500_000_000; // 500ms
const SYNC_MAX_RETRIES: u32 = 10;

const SyncError = error{SyncFailed};

fn syncWithBackoff(fd: std.posix.fd_t) SyncError!void {
    var backoff_ns: u64 = SYNC_INITIAL_BACKOFF_NS;
    var attempt: u32 = 0;
    while (attempt < SYNC_MAX_RETRIES) : (attempt += 1) {
        std.posix.fsync(fd) catch {
            std.posix.nanosleep(0, backoff_ns);
            backoff_ns = @min(backoff_ns * 2, SYNC_MAX_BACKOFF_NS);
            continue;
        };
        return;
    }
    return error.SyncFailed;
}

fn writeAllPwrite(fd: std.posix.fd_t, buf: []const u8, offset: u64) !void {
    var written: usize = 0;
    while (written < buf.len) {
        const n = std.posix.pwrite(fd, buf[written..], offset + written) catch return error.InputOutput;
        if (n == 0) return error.InputOutput;
        written += n;
    }
}

fn readExactIo(file: IoFile, io: Io, buf: []u8, offset: u64) !void {
    var total_read: usize = 0;
    while (total_read < buf.len) {
        var iovecs = [1][]u8{buf[total_read..]};
        const n = file.readPositional(io, &iovecs, offset + total_read) catch return error.InvalidFormat;
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
    dirty: bool,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8, io: Io) !WriteAheadLog {
        const dir = IoDir.cwd();

        dir.makeDir(io, "data") catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = dir.openFile(io, file_path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try dir.createFile(io, file_path, .{ .truncate = false, .read = true }),
            else => return err,
        };

        const file_size = (try file.stat(io)).size;

        return WriteAheadLog{
            .file = file,
            .io = io,
            .allocator = allocator,
            .wal_path = try allocator.dupe(u8, file_path),
            .write_pos = file_size,
            .dirty = false,
        };
    }

    pub fn deinit(self: *WriteAheadLog) void {
        self.flush() catch {};
        self.file.close(self.io);
        self.allocator.free(self.wal_path);
    }

    pub fn flush(self: *WriteAheadLog) !void {
        if (self.dirty) {
            try syncWithBackoff(self.file.handle);
            self.dirty = false;
        }
    }

    /// [4B magic][8B sequence][4B entry_len][4B key_len][key][4B value_len][value][1B newline]
    pub fn append_event(self: *WriteAheadLog, key: []const u8, value: EventValue) !void {
        const serialized_value = try value.serialize(self.allocator);
        defer self.allocator.free(serialized_value);

        const entry_len: u32 = @intCast(4 + key.len + 4 + serialized_value.len + 1);
        const total_size: usize = 4 + 8 + 4 + 4 + key.len + 4 + serialized_value.len + 1;

        var stack_buf: [4096]u8 = undefined;
        const buf = if (total_size <= stack_buf.len)
            stack_buf[0..total_size]
        else
            try self.allocator.alloc(u8, total_size);
        defer if (total_size > stack_buf.len) self.allocator.free(buf);

        var pos: usize = 0;
        std.mem.writeInt(u32, buf[pos..][0..4], MAGIC, .little);
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

        try writeAllPwrite(self.file.handle, buf, self.write_pos);
        self.write_pos += total_size;
        self.dirty = true;
    }

    pub fn replay(self: *WriteAheadLog, radix_tree: *RadixTree) !WalCursor {
        const file_size = (try self.file.stat(self.io)).size;
        var offset: u64 = 0;
        var crumb = WalCursor{ .file_offset = 0, .last_sequence = 0 };

        while (offset < file_size) {
            if (offset + 4 > file_size) break;

            var magic_bytes: [4]u8 = undefined;
            readExactIo(self.file, self.io, &magic_bytes, offset) catch break;
            offset += 4;

            const magic = std.mem.readInt(u32, &magic_bytes, .little);
            if (magic != MAGIC) return error.InvalidFormat;

            var seq_bytes: [8]u8 = undefined;
            try readExactIo(self.file, self.io, &seq_bytes, offset);
            const sequence = std.mem.readInt(u64, &seq_bytes, .little);
            offset += 8;

            var entry_len_bytes: [4]u8 = undefined;
            try readExactIo(self.file, self.io, &entry_len_bytes, offset);
            const entry_len = std.mem.readInt(u32, &entry_len_bytes, .little);
            if (entry_len > MAX_ENTRY_SIZE) return error.InvalidFormat;
            offset += 4;

            var key_len_bytes: [4]u8 = undefined;
            try readExactIo(self.file, self.io, &key_len_bytes, offset);
            const key_len = std.mem.readInt(u32, &key_len_bytes, .little);
            offset += 4;

            const key_buf = try self.allocator.alloc(u8, key_len);
            defer self.allocator.free(key_buf);
            try readExactIo(self.file, self.io, key_buf, offset);
            offset += key_len;

            var value_len_bytes: [4]u8 = undefined;
            try readExactIo(self.file, self.io, &value_len_bytes, offset);
            const value_len = std.mem.readInt(u32, &value_len_bytes, .little);
            offset += 4;

            const expected_entry_len: u32 = @intCast(4 + key_len + 4 + value_len + 1);
            if (entry_len != expected_entry_len) return error.InvalidFormat;

            const value_buf = try self.allocator.alloc(u8, value_len);
            defer self.allocator.free(value_buf);
            try readExactIo(self.file, self.io, value_buf, offset);
            offset += value_len;

            // Skip newline
            offset += 1;

            const deserialized = try EventValue.deserialize(self.allocator, value_buf);
            try radix_tree.insert(key_buf, deserialized);

            crumb.file_offset = offset;
            crumb.last_sequence = sequence;
        }

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
                    lo = entry_offset + HEADER_SIZE + entry_size;
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
        const search_limit = @min(start + MAX_ENTRY_SIZE, file_size);
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
                if (std.mem.readInt(u32, buf[i..][0..4], .little) == MAGIC) {
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
        var header: [HEADER_SIZE]u8 = undefined;
        var iovecs = [1][]u8{&header};
        const n = self.file.readPositional(self.io, &iovecs, offset) catch return false;
        if (n < HEADER_SIZE) return false;

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != MAGIC) return false;

        const entry_len = std.mem.readInt(u32, header[12..16], .little);
        if (entry_len == 0 or entry_len > MAX_ENTRY_SIZE) return false;

        return true;
    }

    fn read_sequence_at(self: *WriteAheadLog, offset: u64) !u64 {
        var seq_bytes: [8]u8 = undefined;
        try readExactIo(self.file, self.io, &seq_bytes, offset + 4);
        return std.mem.readInt(u64, &seq_bytes, .little);
    }

    fn read_entry_len_at(self: *WriteAheadLog, offset: u64) !u32 {
        var len_bytes: [4]u8 = undefined;
        try readExactIo(self.file, self.io, &len_bytes, offset + 12);
        return std.mem.readInt(u32, &len_bytes, .little);
    }
};

test "find_by_sequence returns correct offsets" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_find_seq.wal") catch {};
    std.fs.cwd().deleteFile("data/test_find_seq.crumb") catch {};

    var wal_log = try WriteAheadLog.init(allocator, "data/test_find_seq.wal", io);
    defer {
        wal_log.deinit();
        std.fs.cwd().deleteFile("data/test_find_seq.wal") catch {};
        std.fs.cwd().deleteFile("data/test_find_seq.crumb") catch {};
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
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_replay_crumb.wal") catch {};
    std.fs.cwd().deleteFile("data/test_replay_crumb.crumb") catch {};

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

    std.fs.cwd().deleteFile("data/test_replay_crumb.wal") catch {};
    std.fs.cwd().deleteFile("data/test_replay_crumb.crumb") catch {};
}
