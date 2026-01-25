const std = @import("std");
const ArrayList = std.ArrayList;
const radix = @import("radix.zig");
const bloom = @import("bloom.zig");
const event = @import("event.zig");
const RadixTree = radix.RadixTree;
const BloomFilter = bloom.BloomFilter;
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

pub const SSTable = struct {
    file_path: []const u8,
    min_key: []const u8,
    max_key: []const u8,
    record_count: u32,
    bloom_filter: BloomFilter,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) SSTable {
        return SSTable{
            .file_path = allocator.dupe(u8, file_path) catch unreachable,
            .min_key = "",
            .max_key = "",
            .record_count = 0,
            .bloom_filter = BloomFilter.init(allocator, 10000),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SSTable) void {
        self.bloom_filter.deinit(self.allocator);
        self.allocator.free(self.file_path);
        if (self.min_key.len > 0) self.allocator.free(self.min_key);
        if (self.max_key.len > 0) self.allocator.free(self.max_key);
    }

    pub fn write_from_radix(self: *SSTable, radix_tree: *const RadixTree) !void {
        std.fs.cwd().makeDir("data") catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        var results = ArrayList(KeyValuePair){};
        defer {
            for (results.items) |item| {
                self.allocator.free(item.key);
            }
            results.deinit(self.allocator);
        }

        try radix_tree.scan_prefix("", &results);
        std.mem.sort(KeyValuePair, results.items, {}, key_less_than);

        const file = try std.fs.cwd().createFile(self.file_path, .{});
        defer file.close();

        for (results.items) |item| {
            const serialized = try item.value.serialize(self.allocator);
            defer self.allocator.free(serialized);

            var key_len_bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &key_len_bytes, @intCast(item.key.len), .little);
            try file.writeAll(&key_len_bytes);
            try file.writeAll(item.key);

            var serialized_len_bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &serialized_len_bytes, @intCast(serialized.len), .little);
            try file.writeAll(&serialized_len_bytes);
            try file.writeAll(serialized);

            self.bloom_filter.add(item.key);
        }

        self.record_count = @intCast(results.items.len);
        if (results.items.len > 0) {
            self.min_key = try self.allocator.dupe(u8, results.items[0].key);
            self.max_key = try self.allocator.dupe(u8, results.items[results.items.len - 1].key);
        }
    }

    pub fn get(self: *const SSTable, key: []const u8) ?EventValue {
        if (!self.bloom_filter.might_contain(key)) return null;
        if (self.min_key.len > 0 and std.mem.lessThan(u8, key, self.min_key)) return null;
        if (self.max_key.len > 0 and std.mem.lessThan(u8, self.max_key, key)) return null;

        const file = std.fs.cwd().openFile(self.file_path, .{}) catch return null;
        defer file.close();

        // TODO: replace the linear scan, maybe with a binary search
        while (true) {
            var key_len_bytes: [4]u8 = undefined;
            _ = file.read(&key_len_bytes) catch break;
            const key_len = std.mem.readInt(u32, &key_len_bytes, .little);

            const key_buf = self.allocator.alloc(u8, key_len) catch break;
            defer self.allocator.free(key_buf);
            _ = file.read(key_buf) catch break;

            var value_len_bytes: [4]u8 = undefined;
            _ = file.read(&value_len_bytes) catch break;
            const value_len = std.mem.readInt(u32, &value_len_bytes, .little);
            const value_buf = self.allocator.alloc(u8, value_len) catch break;
            defer self.allocator.free(value_buf);

            _ = file.read(value_buf) catch break;

            if (std.mem.eql(u8, key_buf, key)) {
                return EventValue.deserialize(self.allocator, value_buf) catch null;
            }
        }

        return null;
    }

    fn key_less_than(_: void, a: KeyValuePair, b: KeyValuePair) bool {
        return std.mem.lessThan(u8, a.key, b.key);
    }
};
