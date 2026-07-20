const std = @import("std");
const assert = std.debug.assert;
const limits = @import("limits.zig");

/// A caller-backed list. Capacity is fixed when the list is initialized and no
/// operation allocates memory.
pub fn List(comptime T: type) type {
    return struct {
        storage: []T,
        count: limits.CollectionCount,

        const Self = @This();

        pub fn init(storage: []T) !Self {
            if (storage.len > std.math.maxInt(limits.CollectionCount)) {
                return error.CapacityTooLarge;
            }
            return .{ .storage = storage, .count = 0 };
        }

        pub fn capacity(self: *const Self) limits.CollectionCount {
            return @intCast(self.storage.len);
        }

        pub fn items(self: *Self) []T {
            return self.storage[0..limits.usize_from(self.count)];
        }

        pub fn items_const(self: *const Self) []const T {
            return self.storage[0..limits.usize_from(self.count)];
        }

        pub fn append(self: *Self, value: T) !void {
            if (self.count == self.capacity()) return error.CapacityExceeded;
            self.storage[limits.usize_from(self.count)] = value;
            self.count += 1;
            assert(self.count <= self.capacity());
        }

        pub fn insert(self: *Self, index: limits.CollectionIndex, value: T) !void {
            if (index > self.count) return error.InvalidIndex;
            if (self.count == self.capacity()) return error.CapacityExceeded;

            const index_usize = limits.usize_from(index);
            const count_usize = limits.usize_from(self.count);
            std.mem.copyBackwards(
                T,
                self.storage[index_usize + 1 .. count_usize + 1],
                self.storage[index_usize..count_usize],
            );
            self.storage[index_usize] = value;
            self.count += 1;
        }

        pub fn ordered_remove(self: *Self, index: limits.CollectionIndex) T {
            assert(index < self.count);
            const index_usize = limits.usize_from(index);
            const count_usize = limits.usize_from(self.count);
            const removed = self.storage[index_usize];
            std.mem.copyForwards(
                T,
                self.storage[index_usize .. count_usize - 1],
                self.storage[index_usize + 1 .. count_usize],
            );
            self.count -= 1;
            return removed;
        }

        pub fn swap_remove(self: *Self, index: limits.CollectionIndex) T {
            assert(index < self.count);
            const index_usize = limits.usize_from(index);
            const last_index = limits.usize_from(self.count - 1);
            const removed = self.storage[index_usize];
            self.storage[index_usize] = self.storage[last_index];
            self.count -= 1;
            return removed;
        }

        pub fn reset(self: *Self) void {
            self.count = 0;
        }
    };
}

/// A caller-backed bump arena for bytes whose lifetime ends together. Reset is
/// constant-time; individual slices are never freed.
pub const ByteArena = struct {
    storage: []u8,
    used: limits.ByteCount,

    pub fn init(storage: []u8) !ByteArena {
        if (storage.len > std.math.maxInt(limits.ByteCount)) {
            return error.CapacityTooLarge;
        }
        return .{ .storage = storage, .used = 0 };
    }

    pub fn capacity(self: *const ByteArena) limits.ByteCount {
        return @intCast(self.storage.len);
    }

    pub fn remaining(self: *const ByteArena) limits.ByteCount {
        assert(self.used <= self.capacity());
        return self.capacity() - self.used;
    }

    pub fn write(self: *ByteArena, source: []const u8) ![]u8 {
        const source_count = try limits.byte_count(source.len);
        if (source_count > self.remaining()) return error.CapacityExceeded;

        const start = self.used;
        self.used += source_count;
        const target = self.storage[limits.usize_from(start)..limits.usize_from(self.used)];
        @memcpy(target, source);
        return target;
    }

    pub fn reset(self: *ByteArena) void {
        self.used = 0;
    }
};

test "fixed list rejects overflow without allocation" {
    var storage: [2]u32 = undefined;
    var list = try List(u32).init(&storage);
    try list.append(10);
    try list.append(20);
    try std.testing.expectError(error.CapacityExceeded, list.append(30));
    try std.testing.expectEqualSlices(u32, &.{ 10, 20 }, list.items());
}

test "fixed list insertion and removal preserve order" {
    var storage: [4]u32 = undefined;
    var list = try List(u32).init(&storage);
    try list.append(10);
    try list.append(30);
    try list.insert(1, 20);
    try std.testing.expectEqualSlices(u32, &.{ 10, 20, 30 }, list.items());
    try std.testing.expectEqual(@as(u32, 20), list.ordered_remove(1));
    try std.testing.expectEqualSlices(u32, &.{ 10, 30 }, list.items());
}

test "byte arena rejects overflow and resets" {
    var storage: [5]u8 = undefined;
    var arena = try ByteArena.init(&storage);
    try std.testing.expectEqualStrings("abc", try arena.write("abc"));
    try std.testing.expectError(error.CapacityExceeded, arena.write("def"));
    arena.reset();
    try std.testing.expectEqualStrings("12345", try arena.write("12345"));
}
