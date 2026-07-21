const std = @import("std");
const assert = std.debug.assert;
const event = @import("event.zig");
const fixed = @import("fixed.zig");
const limits = @import("limits.zig");
const EventValue = event.EventValue;

pub const Entry = struct {
    key: []const u8,
    value: EventValue,
};

pub const ScanEntry = struct {
    key: []const u8,
    value: *const EventValue,
};

pub const ScanResult = struct {
    count: limits.EventCount,
    complete: bool,
};

pub const Memtable = struct {
    entries: fixed.List(Entry),
    entry_storage: []Entry,
    bytes: fixed.ByteArena,
    byte_storage: []u8,
    allocator: std.mem.Allocator,
    size_bytes: limits.ByteCount,

    pub fn init(allocator: std.mem.Allocator) !Memtable {
        const entry_storage = try allocator.alloc(Entry, limits.memtable_entries_max);
        errdefer allocator.free(entry_storage);
        const byte_storage = try allocator.alloc(u8, limits.memtable_bytes_max);

        return .{
            .entries = try fixed.List(Entry).init(entry_storage),
            .entry_storage = entry_storage,
            .bytes = try fixed.ByteArena.init(byte_storage),
            .byte_storage = byte_storage,
            .allocator = allocator,
            .size_bytes = 0,
        };
    }

    pub fn deinit(self: *Memtable) void {
        self.allocator.free(self.byte_storage);
        self.allocator.free(self.entry_storage);
    }

    pub fn count(self: *const Memtable) u32 {
        return self.entries.count;
    }

    fn stored_bytes(key: []const u8, value: *const EventValue) !limits.ByteCount {
        const total = key.len +
            value.event_type.len +
            value.payload.len +
            value.origin_service.len +
            value.signature.len;
        return try limits.byte_count(total);
    }

    const FindResult = struct {
        index: limits.MemtableEntryIndex,
        found: bool,
    };

    fn find_index(self: *const Memtable, key: []const u8) FindResult {
        const items = self.entries.items_const();
        var lo: limits.MemtableEntryIndex = 0;
        var hi: limits.MemtableEntryIndex = self.entries.count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, items[limits.usize_from(mid)].key, key)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return .{ .index = mid, .found = true },
            }
        }
        assert(lo <= self.entries.count);
        return .{ .index = lo, .found = false };
    }

    pub fn insert(self: *Memtable, key: []const u8, value: EventValue) !void {
        assert(key.len > 0);
        const count_before = self.entries.count;

        const result = self.find_index(key);
        if (!result.found and self.entries.count == self.entries.capacity())
            return error.MemtableEntryCapacityExceeded;

        const bytes_required = try stored_bytes(key, &value);
        if (bytes_required > self.bytes.remaining()) return error.MemtableByteCapacityExceeded;

        const key_stored = self.bytes.write(key) catch unreachable;
        const origin_stored = self.bytes.write(value.origin_service) catch unreachable;
        const type_stored = self.bytes.write(value.event_type) catch unreachable;
        const payload_stored = self.bytes.write(value.payload) catch unreachable;
        const signature_stored = self.bytes.write(value.signature) catch unreachable;
        const stored = Entry{
            .key = key_stored,
            .value = .{
                .sequence = value.sequence,
                .event_type = type_stored,
                .payload = payload_stored,
                .timestamp = value.timestamp,
                .origin_service = origin_stored,
                .signature = signature_stored,
            },
        };

        if (result.found) {
            self.entries.items()[limits.usize_from(result.index)] = stored;
        } else {
            self.entries.insert(result.index, stored) catch
                return error.MemtableEntryCapacityExceeded;
        }
        self.size_bytes += bytes_required;

        if (result.found) {
            assert(self.entries.count == count_before);
        } else {
            assert(self.entries.count == count_before + 1);
        }
        const items = self.entries.items_const();
        if (result.index > 0) {
            const prev = items[limits.usize_from(result.index - 1)].key;
            assert(std.mem.order(u8, prev, key) == .lt);
        }
        if (result.index + 1 < self.entries.count) {
            const next = items[limits.usize_from(result.index + 1)].key;
            assert(std.mem.order(u8, key, next) == .lt);
        }
    }

    pub fn can_insert(self: *const Memtable, key: []const u8, value: *const EventValue) bool {
        const result = self.find_index(key);
        if (!result.found and self.entries.count == self.entries.capacity()) return false;
        const bytes_required = stored_bytes(key, value) catch return false;
        return bytes_required <= self.bytes.remaining();
    }

    pub fn contains(self: *const Memtable, key: []const u8) bool {
        return self.find_index(key).found;
    }

    pub fn get_ptr(self: *const Memtable, key: []const u8) ?*const EventValue {
        const result = self.find_index(key);
        if (!result.found) return null;
        return &self.entries.items_const()[limits.usize_from(result.index)].value;
    }

    pub fn scan_prefix_into(
        self: *const Memtable,
        prefix: []const u8,
        output: []ScanEntry,
    ) ScanResult {
        const items = self.entries.items_const();
        var lo: limits.MemtableEntryIndex = 0;
        var hi: limits.MemtableEntryIndex = self.entries.count;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, items[limits.usize_from(mid)].key, prefix) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        var result_count: limits.EventCount = 0;
        while (lo < self.entries.count) : (lo += 1) {
            const entry = &items[limits.usize_from(lo)];
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            if (result_count == output.len) {
                return .{ .count = result_count, .complete = false };
            }
            output[result_count] = .{ .key = entry.key, .value = &entry.value };
            result_count += 1;
        }
        return .{ .count = result_count, .complete = true };
    }

    pub fn visit_prefix(
        self: *const Memtable,
        prefix: []const u8,
        context: anytype,
        comptime visit: anytype,
    ) !event.VisitControl {
        const items = self.entries.items_const();
        var index: limits.MemtableEntryIndex = 0;
        while (index < self.entries.count) : (index += 1) {
            const entry = &items[limits.usize_from(index)];
            if (!std.mem.startsWith(u8, entry.key, prefix)) continue;
            const control = try visit(context, entry.key, entry.value);
            if (control == .stop) return .stop;
        }
        return .continue_scan;
    }

    pub fn iterator(self: *const Memtable) EntryIterator {
        return .{ .entries = self.entries.items_const(), .pos = 0 };
    }

    pub fn reset(self: *Memtable) void {
        self.entries.reset();
        self.bytes.reset();
        self.size_bytes = 0;
    }

    pub const EntryIterator = struct {
        entries: []const Entry,
        pos: limits.MemtableEntryIndex,

        pub fn next(self: *EntryIterator) ?*const Entry {
            if (self.pos >= self.entries.len) return null;
            const entry = &self.entries[limits.usize_from(self.pos)];
            self.pos += 1;
            return entry;
        }

        pub fn reset(self: *EntryIterator) void {
            self.pos = 0;
        }
    };
};

fn test_event_value(
    sequence: limits.Sequence,
    origin_service: []const u8,
    event_type: []const u8,
    payload: []const u8,
    timestamp: limits.Timestamp,
) !EventValue {
    return try EventValue.init(.{
        .sequence = sequence,
        .origin_service = origin_service,
        .event_type = event_type,
        .payload = payload,
        .timestamp = timestamp,
    });
}

test "memtable basic insert and get" {
    const allocator = std.testing.allocator;
    var mt = try Memtable.init(allocator);
    defer mt.deinit();

    const val = try test_event_value(1, "svc", "evt", "{}", 100);
    try mt.insert("key1", val);

    const got = mt.get_ptr("key1").?;
    try std.testing.expectEqual(@as(u64, 1), got.sequence);
    try std.testing.expectEqualStrings("svc", got.origin_service);

    try std.testing.expect(mt.get_ptr("missing") == null);
}

test "memtable sorted order" {
    const allocator = std.testing.allocator;
    var mt = try Memtable.init(allocator);
    defer mt.deinit();

    const keys = [_][]const u8{ "c", "a", "b", "d" };
    for (keys, 0..) |k, i| {
        const val = try test_event_value(i + 1, "svc", "evt", "{}", 0);
        try mt.insert(k, val);
    }

    try std.testing.expectEqual(@as(usize, 4), mt.count());
    try std.testing.expectEqualStrings("a", mt.entries.items_const()[0].key);
    try std.testing.expectEqualStrings("b", mt.entries.items_const()[1].key);
    try std.testing.expectEqualStrings("c", mt.entries.items_const()[2].key);
    try std.testing.expectEqualStrings("d", mt.entries.items_const()[3].key);
}

test "memtable overwrite" {
    const allocator = std.testing.allocator;
    var mt = try Memtable.init(allocator);
    defer mt.deinit();

    const v1 = try test_event_value(1, "svc", "first", "{}", 0);
    try mt.insert("key", v1);

    const v2 = try test_event_value(2, "svc", "second", "{}", 0);
    try mt.insert("key", v2);

    try std.testing.expectEqual(@as(usize, 1), mt.count());
    const got = mt.get_ptr("key").?;
    try std.testing.expectEqual(@as(u64, 2), got.sequence);
    try std.testing.expectEqualStrings("second", got.event_type);
}

test "memtable scan_prefix" {
    const allocator = std.testing.allocator;
    var mt = try Memtable.init(allocator);
    defer mt.deinit();

    const pairs = [_]struct { k: []const u8, seq: u64 }{
        .{ .k = "a:1", .seq = 1 },
        .{ .k = "a:2", .seq = 2 },
        .{ .k = "b:1", .seq = 3 },
        .{ .k = "b:2", .seq = 4 },
    };
    for (pairs) |p| {
        const val = try test_event_value(p.seq, "svc", "evt", "{}", 0);
        try mt.insert(p.k, val);
    }

    var entries: [4]ScanEntry = undefined;
    const result = mt.scan_prefix_into("a:", &entries);
    try std.testing.expect(result.complete);
    try std.testing.expectEqual(@as(limits.EventCount, 2), result.count);
    try std.testing.expectEqualStrings("a:1", entries[0].key);
    try std.testing.expectEqualStrings("a:2", entries[1].key);
}

test "memtable contains" {
    const allocator = std.testing.allocator;
    var mt = try Memtable.init(allocator);
    defer mt.deinit();

    const val = try test_event_value(1, "svc", "evt", "{}", 0);
    try mt.insert("exists", val);

    try std.testing.expect(mt.contains("exists"));
    try std.testing.expect(!mt.contains("missing"));
}

test "memtable size_bytes tracking" {
    const allocator = std.testing.allocator;
    var mt = try Memtable.init(allocator);
    defer mt.deinit();

    try std.testing.expectEqual(@as(usize, 0), mt.size_bytes);

    const v1 = try test_event_value(1, "svc", "evt", "payload", 0);
    try mt.insert("key1", v1);
    try std.testing.expect(mt.size_bytes > 0);

    const before = mt.size_bytes;
    const v2 = try test_event_value(2, "svc", "evt", "payload", 0);
    try mt.insert("key2", v2);
    try std.testing.expect(mt.size_bytes > before);
}

test "memtable iterator" {
    const allocator = std.testing.allocator;
    var mt = try Memtable.init(allocator);
    defer mt.deinit();

    const keys = [_][]const u8{ "c", "a", "b" };
    for (keys, 0..) |k, i| {
        const val = try test_event_value(i + 1, "svc", "evt", "{}", 0);
        try mt.insert(k, val);
    }

    var iter = mt.iterator();
    const first = iter.next().?;
    try std.testing.expectEqualStrings("a", first.key);
    const second = iter.next().?;
    try std.testing.expectEqualStrings("b", second.key);
    const third = iter.next().?;
    try std.testing.expectEqualStrings("c", third.key);
    try std.testing.expect(iter.next() == null);
}
