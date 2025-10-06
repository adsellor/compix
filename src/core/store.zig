const std = @import("std");
const ArrayList = std.ArrayList;
const print = std.debug.print;

const radix = @import("radix.zig");
const lsm = @import("lsm.zig");
const wal = @import("wal.zig");
const event = @import("event.zig");

const RadixTree = radix.RadixTree;
const SSTable = lsm.SSTable;
const WriteAheadLog = wal.WriteAheadLog;
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

pub const HybridEventStore = struct {
    allocator: std.mem.Allocator,

    memtable: RadixTree,
    memtable_size: usize,
    max_memtable_size: usize,

    sstables: ArrayList(SSTable),

    wal: WriteAheadLog,

    compaction_generation: u32,

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8) !HybridEventStore {
        var store = HybridEventStore{
            .allocator = allocator,
            .memtable = try RadixTree.init(allocator),
            .memtable_size = 0,
            .max_memtable_size = 64 * 1024,
            .sstables = ArrayList(SSTable){},
            .wal = try WriteAheadLog.init(allocator, wal_path),
            .compaction_generation = 0,
        };

        try store.wal.replay(&store.memtable);
        return store;
    }

    pub fn deinit(self: *HybridEventStore) void {
        self.memtable.deinit();
        for (self.sstables.items) |*sstable| {
            sstable.deinit();
        }
        self.sstables.deinit(self.allocator);
        self.wal.deinit();
    }

    pub fn put(self: *HybridEventStore, key: []const u8, value: EventValue) !void {
        try self.wal.append_event(key, value);
        try self.memtable.insert(key, value);
        self.memtable_size += key.len + 100;

        if (self.memtable_size > self.max_memtable_size) {
            try self.flush_memtable();
        }

        print("PUT: {s} -> seq:{} type:{s}\n", .{ key, value.sequence, value.event_type });
    }

    pub fn get(self: *const HybridEventStore, key: []const u8) ?EventValue {
        if (self.memtable.get(key)) |value| {
            return value;
        }

        var i = self.sstables.items.len;
        while (i > 0) {
            i -= 1;
            if (self.sstables.items[i].get(key)) |value| {
                return value;
            }
        }

        return null;
    }

    pub fn scan_range(self: *const HybridEventStore, prefix: []const u8) !ArrayList(KeyValuePair) {
        var results = ArrayList(KeyValuePair){};
        try self.memtable.scan_prefix(prefix, &results);
        return results;
    }

    fn flush_memtable(self: *HybridEventStore) !void {
        print("Flushing memtable to SSTable...\n", .{});

        const sstable_path = try std.fmt.allocPrint(self.allocator, "data/sstable_{}.dat", .{self.compaction_generation});
        defer self.allocator.free(sstable_path);

        var sstable = SSTable.init(self.allocator, sstable_path);
        try sstable.write_from_radix(&self.memtable);
        try self.sstables.append(self.allocator, sstable);

        self.memtable.deinit();
        self.memtable = try RadixTree.init(self.allocator);
        self.memtable_size = 0;
        self.compaction_generation += 1;

        print("Memtable flushed to: {s}\n", .{sstable_path});
    }

    pub fn get_stats(self: *const HybridEventStore) void {
        print("Store Statitics:\n", .{});
        print("Memtable entries: {}\n", .{self.memtable.size});
        print("SSTables: {}\n", .{self.sstables.items.len});
        print("Generation: {}\n", .{self.compaction_generation});
        print("Estimated memtable size: {} bytes\n", .{self.memtable_size});
    }
};
