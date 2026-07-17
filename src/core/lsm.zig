const std = @import("std");
const assert = std.debug.assert;
const ArrayList = std.ArrayList;

const event = @import("event.zig");
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

const memtable_mod = @import("memtable.zig");
const Memtable = memtable_mod.Memtable;

const sstable = @import("sstable.zig");
const SSTableReader = sstable.SSTableReader;
const SSTInfo = sstable.SSTInfo;
const RawEntry = sstable.RawEntry;

const manifest_mod = @import("manifest.zig");
const Manifest = manifest_mod.Manifest;

const merge_iter_mod = @import("merge_iterator.zig");
const MergeIterator = merge_iter_mod.MergeIterator;

const wal_mod = @import("wal.zig");
const WriteAheadLog = wal_mod.WriteAheadLog;

const Io = std.Io;
const IoDir = Io.Dir;

const flush_bytes_max: u64 = 16 * 1024 * 1024;
const flush_count_max: u32 = 50_000;
const compaction_trigger_l0: u32 = 4;

comptime {
    assert(flush_bytes_max > 0);
    assert(flush_count_max > 0);
    assert(compaction_trigger_l0 >= 2);
}

/// An open SSTable kept resident so reads never reopen or reparse table files.
/// The list mirrors manifest.live exactly, sorted newest-first (level ascending,
/// then id descending) so the first key hit is the freshest value.
const CachedTable = struct {
    id: u64,
    level: u32,
    reader: *SSTableReader,
};

pub const LSMEngine = struct {
    allocator: std.mem.Allocator,
    io: Io,
    wal: *WriteAheadLog,
    active_memtable: *Memtable,
    manifest: *Manifest,
    tables: ArrayList(CachedTable),
    base_path: []const u8,
    wal_path: []const u8,
    next_sst_id: u64,

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8, io: Io) !LSMEngine {
        assert(wal_path.len > 0);

        const base = try derive_base(allocator, wal_path);
        errdefer allocator.free(base);

        const owned_wal_path = try allocator.dupe(u8, wal_path);
        errdefer allocator.free(owned_wal_path);

        const manifest_path = try std.fmt.allocPrint(allocator, "{s}.manifest", .{base});
        defer allocator.free(manifest_path);

        const manifest_ptr = try allocator.create(Manifest);
        errdefer allocator.destroy(manifest_ptr);
        manifest_ptr.* = try Manifest.init(allocator, manifest_path, io);
        errdefer manifest_ptr.deinit();

        var next_id: u64 = 0;
        for (manifest_ptr.live.items) |s| {
            if (s.id > next_id) next_id = s.id;
        }
        next_id += 1;

        const wal_ptr = try allocator.create(WriteAheadLog);
        errdefer allocator.destroy(wal_ptr);
        wal_ptr.* = try WriteAheadLog.init(allocator, wal_path, io);
        errdefer wal_ptr.deinit();

        const mt_ptr = try allocator.create(Memtable);
        errdefer allocator.destroy(mt_ptr);
        mt_ptr.* = Memtable.init(allocator);
        errdefer mt_ptr.deinit();

        _ = try wal_ptr.replay(mt_ptr);

        var self = LSMEngine{
            .allocator = allocator,
            .io = io,
            .wal = wal_ptr,
            .active_memtable = mt_ptr,
            .manifest = manifest_ptr,
            .tables = ArrayList(CachedTable).empty,
            .base_path = base,
            .wal_path = owned_wal_path,
            .next_sst_id = next_id,
        };
        errdefer self.close_tables();

        const sorted = try manifest_ptr.get_live_sorted(allocator);
        defer allocator.free(sorted);
        for (sorted) |info| {
            try self.open_table(info.level, info.id);
        }

        assert(self.tables.items.len == self.manifest.live.items.len);
        return self;
    }

    pub fn deinit(self: *LSMEngine) void {
        self.close_tables();
        self.active_memtable.deinit();
        self.allocator.destroy(self.active_memtable);
        self.wal.deinit();
        self.allocator.destroy(self.wal);
        self.manifest.deinit();
        self.allocator.destroy(self.manifest);
        self.allocator.free(self.base_path);
        self.allocator.free(self.wal_path);
    }

    fn close_tables(self: *LSMEngine) void {
        for (self.tables.items) |t| {
            t.reader.deinit();
            self.allocator.destroy(t.reader);
        }
        self.tables.deinit(self.allocator);
        self.tables = ArrayList(CachedTable).empty;
    }

    /// Opens the table file and inserts it into the cache, keeping the
    /// newest-first order (level ascending, id descending within a level).
    fn open_table(self: *LSMEngine, level: u32, id: u64) !void {
        assert(id > 0);
        const path = try self.sst_path(level, id);
        defer self.allocator.free(path);

        const reader = try self.allocator.create(SSTableReader);
        errdefer self.allocator.destroy(reader);
        reader.* = try SSTableReader.open(self.allocator, path, self.io);
        errdefer reader.deinit();

        var insert_at: usize = self.tables.items.len;
        for (self.tables.items, 0..) |t, i| {
            const goes_before = level < t.level or (level == t.level and id > t.id);
            if (goes_before) {
                insert_at = i;
                break;
            }
        }
        try self.tables.insert(self.allocator, insert_at, CachedTable{
            .id = id,
            .level = level,
            .reader = reader,
        });
    }

    fn remove_table(self: *LSMEngine, id: u64) void {
        for (self.tables.items, 0..) |t, i| {
            if (t.id == id) {
                t.reader.deinit();
                self.allocator.destroy(t.reader);
                _ = self.tables.orderedRemove(i);
                return;
            }
        }
        unreachable;
    }

    pub fn put(self: *LSMEngine, key: []const u8, value: EventValue) !void {
        assert(key.len > 0);

        try self.wal.append_event(key, value);
        try self.active_memtable.insert(key, value);
        assert(self.active_memtable.count() > 0);

        if (self.active_memtable.size_bytes >= flush_bytes_max or
            self.active_memtable.count() >= flush_count_max)
        {
            try self.flush();
        }
    }

    pub fn sync(self: *LSMEngine) !void {
        try self.wal.sync();
    }

    pub fn get(self: *LSMEngine, allocator: std.mem.Allocator, key: []const u8) !?EventValue {
        assert(key.len > 0);

        if (try self.active_memtable.get(allocator, key)) |v| return v;

        for (self.tables.items) |t| {
            if (try t.reader.get(allocator, key)) |v| return v;
        }

        return null;
    }

    pub fn contains(self: *LSMEngine, key: []const u8) !bool {
        assert(key.len > 0);

        if (self.active_memtable.contains(key)) return true;

        for (self.tables.items) |t| {
            if (try t.reader.contains(key)) return true;
        }

        return false;
    }

    pub fn scan_prefix(
        self: *LSMEngine,
        prefix: []const u8,
        results: *ArrayList(KeyValuePair),
    ) !void {
        var mt_results = ArrayList(KeyValuePair).empty;
        try self.active_memtable.scan_prefix(prefix, &mt_results);
        defer mt_results.deinit(self.allocator);

        var sst_results = ArrayList(KeyValuePair).empty;
        defer {
            for (sst_results.items) |*item| item.deinit(self.allocator);
            sst_results.deinit(self.allocator);
        }

        for (self.tables.items) |t| {
            try t.reader.scan_prefix(prefix, &sst_results);
        }

        var seen = std.StringHashMap(void).init(self.allocator);
        defer seen.deinit();

        for (mt_results.items) |item| {
            try results.append(self.allocator, item);
            try seen.put(item.key, {});
        }

        for (sst_results.items) |*item| {
            if (seen.get(item.key) == null) {
                try results.append(self.allocator, KeyValuePair{
                    .key = try self.allocator.dupe(u8, item.key),
                    .value = EventValue{
                        .sequence = item.value.sequence,
                        .event_type = try self.allocator.dupe(u8, item.value.event_type),
                        .payload = try self.allocator.dupe(u8, item.value.payload),
                        .timestamp = item.value.timestamp,
                        .origin_service = try self.allocator.dupe(u8, item.value.origin_service),
                    },
                });
                try seen.put(item.key, {});
            }
        }
    }

    pub fn memtable_count(self: *const LSMEngine) usize {
        return self.active_memtable.count();
    }

    pub fn sstable_count(self: *const LSMEngine) usize {
        assert(self.tables.items.len == self.manifest.live.items.len);
        return self.tables.items.len;
    }

    pub fn flush(self: *LSMEngine) !void {
        if (self.active_memtable.count() == 0) return;
        const tables_before = self.tables.items.len;

        const frozen = self.active_memtable;
        const new_mt = try self.allocator.create(Memtable);
        new_mt.* = Memtable.init(self.allocator);
        self.active_memtable = new_mt;

        var raw_entries = ArrayList(RawEntry).empty;
        defer {
            for (raw_entries.items) |e| {
                self.allocator.free(e.key);
                self.allocator.free(e.value);
            }
            raw_entries.deinit(self.allocator);
        }

        var iter = frozen.iterator();
        while (iter.next()) |entry| {
            const serialized = try entry.value.serialize(self.allocator);
            errdefer self.allocator.free(serialized);
            const key_copy = try self.allocator.dupe(u8, entry.key);
            errdefer self.allocator.free(key_copy);
            try raw_entries.append(self.allocator, RawEntry{
                .key = key_copy,
                .value = serialized,
            });
        }
        assert(raw_entries.items.len == frozen.count());

        const sst_id = self.next_sst_id;
        self.next_sst_id += 1;

        try self.write_and_register(0, sst_id, raw_entries.items);
        try self.wal.truncate();

        frozen.deinit();
        self.allocator.destroy(frozen);

        assert(self.active_memtable.count() == 0);
        assert(self.tables.items.len == tables_before + 1);
        assert(self.tables.items.len == self.manifest.live.items.len);

        if (self.manifest.count_at_level(0) >= compaction_trigger_l0) {
            try self.compact_level(0);
        }
    }

    /// Writes entries as a new SSTable, records it in the manifest, and caches
    /// an open reader for it. Entries must be sorted by key ascending.
    fn write_and_register(
        self: *LSMEngine,
        level: u32,
        sst_id: u64,
        entries: []const RawEntry,
    ) !void {
        if (entries.len > 1) {
            const first = entries[0].key;
            const last = entries[entries.len - 1].key;
            assert(std.mem.order(u8, first, last) != .gt);
        }

        const path = try self.sst_path(level, sst_id);
        defer self.allocator.free(path);

        const footer = try sstable.write_sstable(self.allocator, path, self.io, entries);
        assert(footer.num_entries == entries.len);

        const min_key = if (entries.len > 0) entries[0].key else "";
        const max_key = if (entries.len > 0) entries[entries.len - 1].key else "";
        const file_size = footer.index_offset + footer.index_size + sstable.footer_size;

        try self.manifest.add_sstable(.{
            .id = sst_id,
            .level = level,
            .min_key = min_key,
            .max_key = max_key,
            .num_entries = footer.num_entries,
            .min_seq = footer.min_seq,
            .max_seq = footer.max_seq,
            .file_size = file_size,
        });

        try self.open_table(level, sst_id);
    }

    pub fn compact_level(self: *LSMEngine, level: u32) !void {
        var readers_list = ArrayList(*SSTableReader).empty;
        defer readers_list.deinit(self.allocator);
        var ids_list = ArrayList(u64).empty;
        defer ids_list.deinit(self.allocator);

        for (self.tables.items) |t| {
            if (t.level == level) {
                try readers_list.append(self.allocator, t.reader);
                try ids_list.append(self.allocator, t.id);
            }
        }
        if (ids_list.items.len < 2) return;

        var merge = try MergeIterator.init(self.allocator, readers_list.items, ids_list.items);
        defer merge.deinit();

        const merged = try merge_iter_mod.collect_merged(self.allocator, &merge);
        defer {
            for (merged) |e| {
                self.allocator.free(e.key);
                self.allocator.free(e.value);
            }
            self.allocator.free(merged);
        }

        if (merged.len == 0) return;

        const tables_before = self.tables.items.len;
        const new_id = self.next_sst_id;
        self.next_sst_id += 1;
        const new_level = level + 1;

        const new_path = try self.sst_path(new_level, new_id);
        defer self.allocator.free(new_path);

        const footer = try sstable.write_sstable(self.allocator, new_path, self.io, merged);
        const file_size = footer.index_offset + footer.index_size + sstable.footer_size;

        try self.manifest.atomic_compaction(ids_list.items, .{
            .id = new_id,
            .level = new_level,
            .min_key = merged[0].key,
            .max_key = merged[merged.len - 1].key,
            .num_entries = footer.num_entries,
            .min_seq = footer.min_seq,
            .max_seq = footer.max_seq,
            .file_size = file_size,
        });

        for (ids_list.items) |id| self.remove_table(id);
        try self.open_table(new_level, new_id);

        assert(self.tables.items.len == tables_before - ids_list.items.len + 1);
        assert(self.tables.items.len == self.manifest.live.items.len);
        assert(self.manifest.count_at_level(level) == 0);

        for (ids_list.items) |id| {
            const old_path = try self.sst_path(level, id);
            defer self.allocator.free(old_path);
            try IoDir.cwd().deleteFile(self.io, old_path);
        }
    }

    fn sst_path(self: *const LSMEngine, level: u32, id: u64) ![]const u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}_L{d}_{d:0>8}.sst", .{
            self.base_path,
            level,
            id,
        });
    }

    fn derive_base(allocator: std.mem.Allocator, wal_path: []const u8) ![]const u8 {
        if (std.mem.endsWith(u8, wal_path, ".wal")) {
            return try allocator.dupe(u8, wal_path[0 .. wal_path.len - 4]);
        }
        return try allocator.dupe(u8, wal_path);
    }
};

fn cleanup_test_lsm(allocator: std.mem.Allocator, io: Io, base: []const u8) void {
    const wal_path = std.fmt.allocPrint(allocator, "{s}.wal", .{base}) catch return;
    defer allocator.free(wal_path);
    const crumb = std.fmt.allocPrint(allocator, "{s}.crumb", .{base}) catch return;
    defer allocator.free(crumb);
    const manifest = std.fmt.allocPrint(allocator, "{s}.manifest", .{base}) catch return;
    defer allocator.free(manifest);

    IoDir.cwd().deleteFile(io, wal_path) catch {};
    IoDir.cwd().deleteFile(io, crumb) catch {};
    IoDir.cwd().deleteFile(io, manifest) catch {};

    for (0..20) |i| {
        for (0..3) |lvl| {
            const p = std.fmt.allocPrint(allocator, "{s}_L{d}_{d:0>8}.sst", .{
                base,
                lvl,
                i + 1,
            }) catch continue;
            defer allocator.free(p);
            IoDir.cwd().deleteFile(io, p) catch {};
        }
    }
}

test "lsm basic put and get" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_basic";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_basic.wal", io);
    defer lsm.deinit();

    const val = try EventValue.init(allocator, 1, "svc", "evt", "{}", 100);
    try lsm.put("key1", val);

    var got = (try lsm.get(allocator, "key1")).?;
    defer got.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), got.sequence);

    const miss = try lsm.get(allocator, "missing");
    try std.testing.expect(miss == null);
}

test "lsm scan_prefix" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_scan";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_scan.wal", io);
    defer lsm.deinit();

    for (0..5) |i| {
        const key = try std.fmt.allocPrint(allocator, "a:{}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i + 1, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }
    for (0..3) |i| {
        const key = try std.fmt.allocPrint(allocator, "b:{}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i + 10, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    var results = ArrayList(KeyValuePair).empty;
    try lsm.scan_prefix("a:", &results);
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 5), results.items.len);
}

test "lsm contains" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_contains";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_contains.wal", io);
    defer lsm.deinit();

    const val = try EventValue.init(allocator, 1, "svc", "evt", "{}", 0);
    try lsm.put("exists", val);

    try std.testing.expect(try lsm.contains("exists"));
    try std.testing.expect(!try lsm.contains("missing"));
}

test "lsm wal replay recovery" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_recovery";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    {
        var lsm = try LSMEngine.init(allocator, "data/test_lsm_recovery.wal", io);
        defer lsm.deinit();

        for (1..4) |i| {
            const key = try std.fmt.allocPrint(allocator, "k{}", .{i});
            defer allocator.free(key);
            const val = try EventValue.init(allocator, i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
    }

    {
        var lsm = try LSMEngine.init(allocator, "data/test_lsm_recovery.wal", io);
        defer lsm.deinit();

        var v1 = (try lsm.get(allocator, "k1")).?;
        defer v1.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 1), v1.sequence);

        var v3 = (try lsm.get(allocator, "k3")).?;
        defer v3.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 3), v3.sequence);
    }
}

test "lsm manual flush writes sstable" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_flush";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_flush.wal", io);
    defer lsm.deinit();

    for (1..101) |i| {
        const key = try std.fmt.allocPrint(allocator, "flush:k{d:0>5}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    try std.testing.expectEqual(@as(usize, 100), lsm.active_memtable.count());
    try std.testing.expectEqual(@as(usize, 0), lsm.sstable_count());

    try lsm.flush();

    try std.testing.expectEqual(@as(usize, 0), lsm.active_memtable.count());
    try std.testing.expectEqual(@as(usize, 1), lsm.sstable_count());

    var val = (try lsm.get(allocator, "flush:k00050")).?;
    defer val.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 50), val.sequence);

    var results = ArrayList(KeyValuePair).empty;
    try lsm.scan_prefix("flush:", &results);
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 100), results.items.len);
}

test "lsm cross-flush read" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_crossflush";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_crossflush.wal", io);
    defer lsm.deinit();

    for (1..6) |i| {
        const key = try std.fmt.allocPrint(allocator, "cf:k{}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }
    try lsm.flush();

    for (6..11) |i| {
        const key = try std.fmt.allocPrint(allocator, "cf:k{}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    var v3 = (try lsm.get(allocator, "cf:k3")).?;
    defer v3.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 3), v3.sequence);

    var v8 = (try lsm.get(allocator, "cf:k8")).?;
    defer v8.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 8), v8.sequence);

    var results = ArrayList(KeyValuePair).empty;
    try lsm.scan_prefix("cf:", &results);
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 10), results.items.len);
}

test "lsm flush and recovery" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_flush_recovery";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    {
        var lsm = try LSMEngine.init(allocator, "data/test_lsm_flush_recovery.wal", io);
        defer lsm.deinit();

        for (1..6) |i| {
            const key = try std.fmt.allocPrint(allocator, "fr:k{}", .{i});
            defer allocator.free(key);
            const val = try EventValue.init(allocator, i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
        try lsm.flush();

        for (6..9) |i| {
            const key = try std.fmt.allocPrint(allocator, "fr:k{}", .{i});
            defer allocator.free(key);
            const val = try EventValue.init(allocator, i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
    }

    {
        var lsm = try LSMEngine.init(allocator, "data/test_lsm_flush_recovery.wal", io);
        defer lsm.deinit();

        var v2 = (try lsm.get(allocator, "fr:k2")).?;
        defer v2.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 2), v2.sequence);

        var v7 = (try lsm.get(allocator, "fr:k7")).?;
        defer v7.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 7), v7.sequence);
    }
}

test "lsm compaction merges L0 tables and reads survive" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const base = "data/test_lsm_compaction";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_compaction.wal", io);
    defer lsm.deinit();

    // Four flushes trip the L0 compaction trigger; keys overlap so the newest
    // sequence must win after the merge.
    for (0..4) |round| {
        for (1..6) |i| {
            const key = try std.fmt.allocPrint(allocator, "c:k{}", .{i});
            defer allocator.free(key);
            const val = try EventValue.init(allocator, round * 10 + i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
        try lsm.flush();
    }

    try std.testing.expectEqual(@as(usize, 1), lsm.sstable_count());

    var v1 = (try lsm.get(allocator, "c:k1")).?;
    defer v1.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 31), v1.sequence);

    var results = ArrayList(KeyValuePair).empty;
    try lsm.scan_prefix("c:", &results);
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 5), results.items.len);
}
