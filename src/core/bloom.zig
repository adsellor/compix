const std = @import("std");
const ArrayList = std.ArrayList;

pub const BloomFilter = struct {
    bits: ArrayList(bool),
    capacity: usize,
    hash_functions: u32,

    pub fn init(allocator: std.mem.Allocator, capacity: usize) BloomFilter {
        const bit_count = capacity * 8;
        var bits = ArrayList(bool){};

        for (0..bit_count) |_| {
            bits.append(allocator, false) catch unreachable;
        }

        return BloomFilter{
            .bits = bits,
            .capacity = capacity,
            .hash_functions = 3,
        };
    }

    pub fn deinit(self: *BloomFilter, allocator: std.mem.Allocator) void {
        self.bits.deinit(allocator);
    }

    pub fn add(self: *BloomFilter, key: []const u8) void {
        for (0..self.hash_functions) |i| {
            const hash = self.hash_key(key, @intCast(i));
            const index = hash % self.bits.items.len;
            self.bits.items[index] = true;
        }
    }

    pub fn might_contain(self: *const BloomFilter, key: []const u8) bool {
        for (0..self.hash_functions) |i| {
            const hash = self.hash_key(key, @intCast(i));
            const index = hash % self.bits.items.len;
            if (!self.bits.items[index]) {
                return false;
            }
        }
        return true;
    }

    fn hash_key(self: *const BloomFilter, key: []const u8, seed: u32) usize {
        _ = self;
        var hash: u32 = seed;
        for (key) |byte| {
            hash = hash *% 31 +% byte;
        }
        return hash;
    }
};
