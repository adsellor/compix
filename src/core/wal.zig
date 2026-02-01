const std = @import("std");
const radix = @import("radix.zig");
const event = @import("event.zig");
const wal_cursor = @import("wal_cursor.zig");
const EventValue = event.EventValue;
const RadixTree = radix.RadixTree;
const WalCursor = wal_cursor.WalCursor;

pub const MAGIC: u32 = 0xC0BEEFD8;
pub const HEADER_SIZE: usize = 16;
pub const MAX_ENTRY_SIZE: usize = 4 * 1024 * 1024;

pub const WriteAheadLog = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,
    wal_path: []const u8,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) !WriteAheadLog {
        std.fs.cwd().makeDir("data") catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try std.fs.cwd().createFile(file_path, .{ .truncate = false, .read = true });
        return WriteAheadLog{
            .file = file,
            .allocator = allocator,
            .wal_path = try allocator.dupe(u8, file_path),
        };
    }

    pub fn deinit(self: *WriteAheadLog) void {
        self.file.close();
        self.allocator.free(self.wal_path);
    }

    /// [4B magic][8B sequence][4B entry_len][4B key_len][key][4B value_len][value][1B newline]
    pub fn append_event(self: *WriteAheadLog, key: []const u8, value: EventValue) !void {
        const serialized_value = try value.serialize(self.allocator);
        defer self.allocator.free(serialized_value);

        const entry_len: u32 = @intCast(4 + key.len + 4 + serialized_value.len + 1);

        var magic_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &magic_bytes, MAGIC, .little);
        try self.file.writeAll(&magic_bytes);

        var seq_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &seq_bytes, value.sequence, .little);
        try self.file.writeAll(&seq_bytes);

        var entry_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &entry_len_bytes, entry_len, .little);
        try self.file.writeAll(&entry_len_bytes);

        var key_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &key_len_bytes, @intCast(key.len), .little);
        try self.file.writeAll(&key_len_bytes);
        try self.file.writeAll(key);

        var value_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &value_len_bytes, @intCast(serialized_value.len), .little);
        try self.file.writeAll(&value_len_bytes);
        try self.file.writeAll(serialized_value);
        try self.file.writeAll("\n");
        try self.file.sync();
    }

    pub fn replay(self: *WriteAheadLog, radix_tree: *RadixTree) !WalCursor {
        try self.file.seekTo(0);
        var crumb = WalCursor{ .file_offset = 0, .last_sequence = 0 };

        while (true) {
            var magic_bytes: [4]u8 = undefined;
            const bytes_read = try self.file.read(&magic_bytes);
            if (bytes_read == 0) break;
            if (bytes_read < 4) return error.InvalidFormat;

            const magic = std.mem.readInt(u32, &magic_bytes, .little);
            if (magic != MAGIC) return error.InvalidFormat;

            var seq_bytes: [8]u8 = undefined;
            try readExact(self.file, &seq_bytes);
            const sequence = std.mem.readInt(u64, &seq_bytes, .little);

            var entry_len_bytes: [4]u8 = undefined;
            try readExact(self.file, &entry_len_bytes);
            const entry_len = std.mem.readInt(u32, &entry_len_bytes, .little);
            if (entry_len > MAX_ENTRY_SIZE) return error.InvalidFormat;

            var key_len_bytes: [4]u8 = undefined;
            try readExact(self.file, &key_len_bytes);
            const key_len = std.mem.readInt(u32, &key_len_bytes, .little);

            const key_buf = try self.allocator.alloc(u8, key_len);
            defer self.allocator.free(key_buf);
            try readExact(self.file, key_buf);

            var value_len_bytes: [4]u8 = undefined;
            try readExact(self.file, &value_len_bytes);
            const value_len = std.mem.readInt(u32, &value_len_bytes, .little);

            const expected_entry_len: u32 = @intCast(4 + key_len + 4 + value_len + 1);
            if (entry_len != expected_entry_len) return error.InvalidFormat;

            const value_buf = try self.allocator.alloc(u8, value_len);
            defer self.allocator.free(value_buf);
            try readExact(self.file, value_buf);

            // TODO: see if can get rid of this by having more effecient way to store stuff
            var newline: [1]u8 = undefined;
            _ = self.file.read(&newline) catch {};

            const deserialized = try EventValue.deserialize(self.allocator, value_buf);
            try radix_tree.insert(key_buf, deserialized);

            crumb.file_offset = try self.file.getPos();
            crumb.last_sequence = sequence;
        }

        try wal_cursor.save(self.allocator, self.wal_path, crumb);

        // NOTE: Ensure file position is at end for subsequent appends
        try self.file.seekFromEnd(0);

        return crumb;
    }

    pub fn find_by_sequence(self: *WriteAheadLog, target: u64) !?u64 {
        const file_size = try self.file.getEndPos();
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
        const file_size = try self.file.getEndPos();
        const search_limit = @min(start + MAX_ENTRY_SIZE, file_size);
        var pos = start;

        while (pos < search_limit) {
            try self.file.seekTo(pos);
            var buf: [4096]u8 = undefined;
            const to_read: usize = @intCast(@min(@as(u64, buf.len), search_limit - pos));
            const bytes_read = try self.file.read(buf[0..to_read]);
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
        try self.file.seekTo(offset);
        var header: [HEADER_SIZE]u8 = undefined;
        const n = try self.file.read(&header);
        if (n < HEADER_SIZE) return false;

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != MAGIC) return false;

        const entry_len = std.mem.readInt(u32, header[12..16], .little);
        if (entry_len == 0 or entry_len > MAX_ENTRY_SIZE) return false;

        return true;
    }

    fn read_sequence_at(self: *WriteAheadLog, offset: u64) !u64 {
        try self.file.seekTo(offset + 4); // skip magic
        var seq_bytes: [8]u8 = undefined;
        try readExact(self.file, &seq_bytes);
        return std.mem.readInt(u64, &seq_bytes, .little);
    }

    fn read_entry_len_at(self: *WriteAheadLog, offset: u64) !u32 {
        try self.file.seekTo(offset + 12); // skip magic + sequence
        var len_bytes: [4]u8 = undefined;
        try readExact(self.file, &len_bytes);
        return std.mem.readInt(u32, &len_bytes, .little);
    }
};

fn readExact(file: std.fs.File, buf: []u8) !void {
    var total_read: usize = 0;
    while (total_read < buf.len) {
        const bytes_read = try file.read(buf[total_read..]);
        if (bytes_read == 0) return error.InvalidFormat;
        total_read += bytes_read;
    }
}

test "find_by_sequence returns correct offsets" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.fs.cwd().deleteFile("data/test_find_seq.wal") catch {};
    std.fs.cwd().deleteFile("data/test_find_seq.crumb") catch {};

    var wal_log = try WriteAheadLog.init(allocator, "data/test_find_seq.wal");
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

    std.fs.cwd().deleteFile("data/test_replay_crumb.wal") catch {};
    std.fs.cwd().deleteFile("data/test_replay_crumb.crumb") catch {};

    {
        var wal_log = try WriteAheadLog.init(allocator, "data/test_replay_crumb.wal");
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
        var wal_log = try WriteAheadLog.init(allocator, "data/test_replay_crumb.wal");
        defer wal_log.deinit();

        var tree2 = try RadixTree.init(allocator);
        defer tree2.deinit();

        const crumb2 = try wal_log.replay(&tree2);
        try std.testing.expectEqual(@as(u64, 5), crumb2.last_sequence);
        {
            var v = tree2.get("key_0") orelse return error.ExpectedValueNotFound;
            v.deinit(allocator);
        }
        {
            var v = tree2.get("key_4") orelse return error.ExpectedValueNotFound;
            v.deinit(allocator);
        }

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
        var wal_log = try WriteAheadLog.init(allocator, "data/test_replay_crumb.wal");
        defer wal_log.deinit();

        var tree3 = try RadixTree.init(allocator);
        defer tree3.deinit();

        const crumb3 = try wal_log.replay(&tree3);
        try std.testing.expectEqual(@as(u64, 8), crumb3.last_sequence);

        // Full replay from offset 0: all 8 keys should be present
        {
            var v = tree3.get("key_0") orelse return error.ExpectedValueNotFound;
            v.deinit(allocator);
        }
        {
            var v = tree3.get("key_4") orelse return error.ExpectedValueNotFound;
            v.deinit(allocator);
        }
        {
            var v = tree3.get("key_5") orelse return error.ExpectedValueNotFound;
            v.deinit(allocator);
        }
        {
            var v = tree3.get("key_7") orelse return error.ExpectedValueNotFound;
            v.deinit(allocator);
        }
    }

    std.fs.cwd().deleteFile("data/test_replay_crumb.wal") catch {};
    std.fs.cwd().deleteFile("data/test_replay_crumb.crumb") catch {};
}
