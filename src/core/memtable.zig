const std = @import("std");
const ArrayList = std.ArrayList;
const event = @import("event.zig");
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

pub const Entry = struct {
    key: []const u8,
    value: EventValue,

    pub fn deinit(self: *Entry, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        self.value.deinit(allocator);
    }
};

pub const Memtable = struct {
    entries: ArrayList(Entry),
    allocator: std.mem.Allocator,
    size_bytes: usize,

    pub fn init(allocator: std.mem.Allocator) Memtable {
        return Memtable{
            .entries = ArrayList(Entry){},
            .allocator = allocator,
            .size_bytes = 0,
        };
    }

    pub fn deinit(self: *Memtable) void {
        for (self.entries.items) |*e| {
            e.deinit(self.allocator);
        }
        self.entries.deinit(self.allocator);
    }

    pub fn count(self: *const Memtable) usize {
        return self.entries.items.len;
    }

    fn estimateEntrySize(key: []const u8, value: *const EventValue) usize {
        return key.len + value.event_type.len + value.payload.len + value.origin_service.len + 64;
    }

    fn findIndex(self: *const Memtable, key: []const u8) struct { index: usize, found: bool } {
        const items = self.entries.items;
        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            switch (std.mem.order(u8, items[mid].key, key)) {
                .lt => lo = mid + 1,
                .gt => hi = mid,
                .eq => return .{ .index = mid, .found = true },
            }
        }
        return .{ .index = lo, .found = false };
    }

    pub fn insert(self: *Memtable, key: []const u8, value: EventValue) !void {
        const result = self.findIndex(key);
        if (result.found) {
            const old_size = estimateEntrySize(
                self.entries.items[result.index].key,
                &self.entries.items[result.index].value,
            );
            self.entries.items[result.index].value.deinit(self.allocator);
            self.entries.items[result.index].value = value;
            const new_size = estimateEntrySize(key, &value);
            if (new_size >= old_size) {
                self.size_bytes += new_size - old_size;
            } else {
                self.size_bytes -= old_size - new_size;
            }
        } else {
            const owned_key = try self.allocator.dupe(u8, key);
            errdefer self.allocator.free(owned_key);
            try self.entries.insert(self.allocator, result.index, Entry{
                .key = owned_key,
                .value = value,
            });
            self.size_bytes += estimateEntrySize(key, &value);
        }
    }

    /// Returns an owned copy of the value. Caller must deinit.
    pub fn get(self: *const Memtable, allocator: std.mem.Allocator, key: []const u8) !?EventValue {
        const result = self.findIndex(key);
        if (!result.found) return null;
        const orig = &self.entries.items[result.index].value;
        return EventValue{
            .sequence = orig.sequence,
            .event_type = try allocator.dupe(u8, orig.event_type),
            .payload = try allocator.dupe(u8, orig.payload),
            .timestamp = orig.timestamp,
            .origin_service = try allocator.dupe(u8, orig.origin_service),
        };
    }

    pub fn contains(self: *const Memtable, key: []const u8) bool {
        return self.findIndex(key).found;
    }

    pub fn getPtr(self: *const Memtable, key: []const u8) ?*const EventValue {
        const result = self.findIndex(key);
        if (!result.found) return null;
        return &self.entries.items[result.index].value;
    }

    pub fn scan_prefix(self: *const Memtable, prefix: []const u8, results: *ArrayList(KeyValuePair)) !void {
        var lo: usize = 0;
        var hi: usize = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, self.entries.items[mid].key, prefix) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        while (lo < self.entries.items.len) {
            const entry = &self.entries.items[lo];
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            try results.append(self.allocator, KeyValuePair{
                .key = try self.allocator.dupe(u8, entry.key),
                .value = EventValue{
                    .sequence = entry.value.sequence,
                    .event_type = try self.allocator.dupe(u8, entry.value.event_type),
                    .payload = try self.allocator.dupe(u8, entry.value.payload),
                    .timestamp = entry.value.timestamp,
                    .origin_service = try self.allocator.dupe(u8, entry.value.origin_service),
                },
            });
            lo += 1;
        }
    }

    pub fn iterator(self: *const Memtable) EntryIterator {
        return EntryIterator{ .entries = self.entries.items, .pos = 0 };
    }

    pub const EntryIterator = struct {
        entries: []const Entry,
        pos: usize,

        pub fn next(self: *EntryIterator) ?*const Entry {
            if (self.pos >= self.entries.len) return null;
            const entry = &self.entries[self.pos];
            self.pos += 1;
            return entry;
        }

        pub fn reset(self: *EntryIterator) void {
            self.pos = 0;
        }
    };
};

test "memtable basic insert and get" {
    const allocator = std.testing.allocator;
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const val = try EventValue.init(allocator, 1, "svc", "evt", "{}", 100);
    try mt.insert("key1", val);

    var got = (try mt.get(allocator, "key1")).?;
    defer got.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), got.sequence);
    try std.testing.expectEqualStrings("svc", got.origin_service);

    const miss = try mt.get(allocator, "missing");
    try std.testing.expect(miss == null);
}

test "memtable sorted order" {
    const allocator = std.testing.allocator;
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const keys = [_][]const u8{ "c", "a", "b", "d" };
    for (keys, 0..) |k, i| {
        const val = try EventValue.init(allocator, i + 1, "svc", "evt", "{}", 0);
        try mt.insert(k, val);
    }

    try std.testing.expectEqual(@as(usize, 4), mt.count());
    try std.testing.expectEqualStrings("a", mt.entries.items[0].key);
    try std.testing.expectEqualStrings("b", mt.entries.items[1].key);
    try std.testing.expectEqualStrings("c", mt.entries.items[2].key);
    try std.testing.expectEqualStrings("d", mt.entries.items[3].key);
}

test "memtable overwrite" {
    const allocator = std.testing.allocator;
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const v1 = try EventValue.init(allocator, 1, "svc", "first", "{}", 0);
    try mt.insert("key", v1);

    const v2 = try EventValue.init(allocator, 2, "svc", "second", "{}", 0);
    try mt.insert("key", v2);

    try std.testing.expectEqual(@as(usize, 1), mt.count());
    var got = (try mt.get(allocator, "key")).?;
    defer got.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 2), got.sequence);
    try std.testing.expectEqualStrings("second", got.event_type);
}

test "memtable scan_prefix" {
    const allocator = std.testing.allocator;
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const pairs = [_]struct { k: []const u8, seq: u64 }{
        .{ .k = "a:1", .seq = 1 },
        .{ .k = "a:2", .seq = 2 },
        .{ .k = "b:1", .seq = 3 },
        .{ .k = "b:2", .seq = 4 },
    };
    for (pairs) |p| {
        const val = try EventValue.init(allocator, p.seq, "svc", "evt", "{}", 0);
        try mt.insert(p.k, val);
    }

    var results = ArrayList(KeyValuePair){};
    try mt.scan_prefix("a:", &results);
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), results.items.len);
    try std.testing.expectEqualStrings("a:1", results.items[0].key);
    try std.testing.expectEqualStrings("a:2", results.items[1].key);
}

test "memtable contains" {
    const allocator = std.testing.allocator;
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const val = try EventValue.init(allocator, 1, "svc", "evt", "{}", 0);
    try mt.insert("exists", val);

    try std.testing.expect(mt.contains("exists"));
    try std.testing.expect(!mt.contains("missing"));
}

test "memtable size_bytes tracking" {
    const allocator = std.testing.allocator;
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    try std.testing.expectEqual(@as(usize, 0), mt.size_bytes);

    const v1 = try EventValue.init(allocator, 1, "svc", "evt", "payload", 0);
    try mt.insert("key1", v1);
    try std.testing.expect(mt.size_bytes > 0);

    const before = mt.size_bytes;
    const v2 = try EventValue.init(allocator, 2, "svc", "evt", "payload", 0);
    try mt.insert("key2", v2);
    try std.testing.expect(mt.size_bytes > before);
}

test "memtable iterator" {
    const allocator = std.testing.allocator;
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const keys = [_][]const u8{ "c", "a", "b" };
    for (keys, 0..) |k, i| {
        const val = try EventValue.init(allocator, i + 1, "svc", "evt", "{}", 0);
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
