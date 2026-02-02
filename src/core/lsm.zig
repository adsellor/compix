const std = @import("std");
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

const FLUSH_SIZE_BYTES: usize = 16 * 1024 * 1024; // 16 MB
const FLUSH_COUNT: usize = 50_000;
const L0_COMPACTION_TRIGGER: usize = 4;

pub const LSMEngine = struct {
    allocator: std.mem.Allocator,
    io: Io,
    wal: *WriteAheadLog,
    active_memtable: *Memtable,
    manifest: *Manifest,
    base_path: []const u8,
    wal_path: []const u8,
    next_sst_id: u64,

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8, io: Io) !LSMEngine {
        const base = try deriveBase(allocator, wal_path);
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

        return LSMEngine{
            .allocator = allocator,
            .io = io,
            .wal = wal_ptr,
            .active_memtable = mt_ptr,
            .manifest = manifest_ptr,
            .base_path = base,
            .wal_path = owned_wal_path,
            .next_sst_id = next_id,
        };
    }

    pub fn deinit(self: *LSMEngine) void {
        self.active_memtable.deinit();
        self.allocator.destroy(self.active_memtable);
        self.wal.deinit();
        self.allocator.destroy(self.wal);
        self.manifest.deinit();
        self.allocator.destroy(self.manifest);
        self.allocator.free(self.base_path);
        self.allocator.free(self.wal_path);
    }

    pub fn put(self: *LSMEngine, key: []const u8, value: EventValue) !void {
        try self.wal.append_event(key, value);
        try self.active_memtable.insert(key, value);

        if (self.active_memtable.size_bytes >= FLUSH_SIZE_BYTES or
            self.active_memtable.count() >= FLUSH_COUNT)
        {
            try self.flush();
        }
    }

    pub fn sync(self: *LSMEngine) !void {
        try self.wal.sync();
    }

    pub fn get(self: *LSMEngine, allocator: std.mem.Allocator, key: []const u8) !?EventValue {
        if (try self.active_memtable.get(allocator, key)) |v| return v;

        const sorted = try self.manifest.getLiveSorted(self.allocator);
        defer self.allocator.free(sorted);

        for (sorted) |info| {
            const sst_path = try self.sstPath(info.level, info.id);
            defer self.allocator.free(sst_path);

            var reader = SSTableReader.open(self.allocator, sst_path, self.io) catch continue;
            defer reader.deinit();

            if (try reader.get(allocator, key)) |v| return v;
        }

        return null;
    }

    pub fn contains(self: *LSMEngine, key: []const u8) !bool {
        if (self.active_memtable.contains(key)) return true;

        const sorted = try self.manifest.getLiveSorted(self.allocator);
        defer self.allocator.free(sorted);

        for (sorted) |info| {
            const sst_path = try self.sstPath(info.level, info.id);
            defer self.allocator.free(sst_path);

            var reader = SSTableReader.open(self.allocator, sst_path, self.io) catch continue;
            defer reader.deinit();

            if (try reader.contains(key)) return true;
        }

        return false;
    }

    pub fn scan_prefix(self: *LSMEngine, prefix: []const u8, results: *ArrayList(KeyValuePair)) !void {
        var mt_results = ArrayList(KeyValuePair){};
        try self.active_memtable.scan_prefix(prefix, &mt_results);
        defer mt_results.deinit(self.allocator);

        var sst_results = ArrayList(KeyValuePair){};
        defer {
            for (sst_results.items) |*item| item.deinit(self.allocator);
            sst_results.deinit(self.allocator);
        }

        const sorted = try self.manifest.getLiveSorted(self.allocator);
        defer self.allocator.free(sorted);

        for (sorted) |info| {
            const sst_path = try self.sstPath(info.level, info.id);
            defer self.allocator.free(sst_path);

            var reader = SSTableReader.open(self.allocator, sst_path, self.io) catch continue;
            defer reader.deinit();

            try reader.scan_prefix(prefix, &sst_results);
        }

        var seen = std.StringHashMap(void).init(self.allocator);
        defer seen.deinit();

        for (mt_results.items) |item| {
            try results.append(self.allocator, item);
            try seen.put(item.key, {});
        }

        var sst_seen = std.StringHashMap(void).init(self.allocator);
        defer sst_seen.deinit();

        for (sst_results.items) |*item| {
            const in_mt = seen.get(item.key) != null;
            const in_sst = sst_seen.get(item.key) != null;
            if (!in_mt and !in_sst) {
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
                try sst_seen.put(item.key, {});
            }
        }
    }

    pub fn memtableCount(self: *const LSMEngine) usize {
        return self.active_memtable.count();
    }

    pub fn sstableCount(self: *const LSMEngine) usize {
        return self.manifest.live.items.len;
    }

    pub fn flush(self: *LSMEngine) !void {
        if (self.active_memtable.count() == 0) return;

        const frozen = self.active_memtable;
        const new_mt = try self.allocator.create(Memtable);
        new_mt.* = Memtable.init(self.allocator);
        self.active_memtable = new_mt;

        var raw_entries = ArrayList(RawEntry){};
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

        const sst_id = self.next_sst_id;
        self.next_sst_id += 1;

        const sst_path = try self.sstPath(0, sst_id);
        defer self.allocator.free(sst_path);

        const footer = try sstable.writeSSTable(self.allocator, sst_path, self.io, raw_entries.items);

        const min_key = if (raw_entries.items.len > 0) raw_entries.items[0].key else "";
        const max_key = if (raw_entries.items.len > 0) raw_entries.items[raw_entries.items.len - 1].key else "";

        const file_size = footer.index_offset + footer.index_size + sstable.FOOTER_SIZE;

        try self.manifest.addSSTable(.{
            .id = sst_id,
            .level = 0,
            .min_key = min_key,
            .max_key = max_key,
            .num_entries = footer.num_entries,
            .min_seq = footer.min_seq,
            .max_seq = footer.max_seq,
            .file_size = file_size,
        });

        try self.wal.truncate();

        frozen.deinit();
        self.allocator.destroy(frozen);

        if (self.manifest.countAtLevel(0) >= L0_COMPACTION_TRIGGER) {
            self.compactLevel(0) catch {};
        }
    }

    pub fn compactLevel(self: *LSMEngine, level: u32) !void {
        const ids = try self.manifest.getIdsAtLevel(self.allocator, level);
        defer self.allocator.free(ids);
        if (ids.len < 2) return;

        var readers_list = ArrayList(*SSTableReader){};
        defer {
            for (readers_list.items) |r| {
                r.deinit();
                self.allocator.destroy(r);
            }
            readers_list.deinit(self.allocator);
        }

        for (ids) |id| {
            const p = try self.sstPath(level, id);
            defer self.allocator.free(p);
            const rdr = try self.allocator.create(SSTableReader);
            rdr.* = SSTableReader.open(self.allocator, p, self.io) catch {
                self.allocator.destroy(rdr);
                continue;
            };
            try readers_list.append(self.allocator, rdr);
        }

        if (readers_list.items.len < 2) return;

        var merge = try MergeIterator.init(
            self.allocator,
            readers_list.items,
            ids,
        );
        defer merge.deinit();

        const merged = try merge_iter_mod.collectMerged(self.allocator, &merge);
        defer {
            for (merged) |e| {
                self.allocator.free(e.key);
                self.allocator.free(e.value);
            }
            self.allocator.free(merged);
        }

        if (merged.len == 0) return;

        const new_id = self.next_sst_id;
        self.next_sst_id += 1;
        const new_level = level + 1;

        const new_path = try self.sstPath(new_level, new_id);
        defer self.allocator.free(new_path);

        const footer = try sstable.writeSSTable(self.allocator, new_path, self.io, merged);

        const min_key = merged[0].key;
        const max_key = merged[merged.len - 1].key;
        const file_size = footer.index_offset + footer.index_size + sstable.FOOTER_SIZE;

        try self.manifest.atomicCompaction(ids, .{
            .id = new_id,
            .level = new_level,
            .min_key = min_key,
            .max_key = max_key,
            .num_entries = footer.num_entries,
            .min_seq = footer.min_seq,
            .max_seq = footer.max_seq,
            .file_size = file_size,
        });

        for (readers_list.items) |r| {
            r.deinit();
            self.allocator.destroy(r);
        }
        readers_list.items.len = 0;

        for (ids) |id| {
            const old_path = try self.sstPath(level, id);
            defer self.allocator.free(old_path);
            IoDir.cwd().deleteFile(self.io, old_path) catch {};
        }
    }

    fn sstPath(self: *const LSMEngine, level: u32, id: u64) ![]const u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}_L{d}_{d:0>8}.sst", .{ self.base_path, level, id });
    }

    fn deriveBase(allocator: std.mem.Allocator, wal_path: []const u8) ![]const u8 {
        if (std.mem.endsWith(u8, wal_path, ".wal")) {
            return try allocator.dupe(u8, wal_path[0 .. wal_path.len - 4]);
        }
        return try allocator.dupe(u8, wal_path);
    }

    pub fn destroyData(self: *LSMEngine) void {
        for (self.manifest.live.items) |info| {
            const p = self.sstPath(info.level, info.id) catch continue;
            defer self.allocator.free(p);
            IoDir.cwd().deleteFile(self.io, p) catch {};
        }
        if (self.manifest.path.len > 0) {
            IoDir.cwd().deleteFile(self.io, self.manifest.path) catch {};
        }
        IoDir.cwd().deleteFile(self.io, self.wal_path) catch {};
        const crumb_path = std.fmt.allocPrint(self.allocator, "{s}.crumb", .{self.base_path}) catch return;
        defer self.allocator.free(crumb_path);
        IoDir.cwd().deleteFile(self.io, crumb_path) catch {};
    }
};

fn cleanupTestLSM(allocator: std.mem.Allocator, io: Io, base: []const u8) void {
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
            const p = std.fmt.allocPrint(allocator, "{s}_L{d}_{d:0>8}.sst", .{ base, lvl, i + 1 }) catch continue;
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
    cleanupTestLSM(allocator, io, base);
    defer cleanupTestLSM(allocator, io, base);

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
    cleanupTestLSM(allocator, io, base);
    defer cleanupTestLSM(allocator, io, base);

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

    var results = ArrayList(KeyValuePair){};
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
    cleanupTestLSM(allocator, io, base);
    defer cleanupTestLSM(allocator, io, base);

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
    cleanupTestLSM(allocator, io, base);
    defer cleanupTestLSM(allocator, io, base);

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
    cleanupTestLSM(allocator, io, base);
    defer cleanupTestLSM(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_flush.wal", io);
    defer lsm.deinit();

    for (1..101) |i| {
        const key = try std.fmt.allocPrint(allocator, "flush:k{d:0>5}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    try std.testing.expectEqual(@as(usize, 100), lsm.active_memtable.count());
    try std.testing.expectEqual(@as(usize, 0), lsm.sstableCount());

    try lsm.flush();

    try std.testing.expectEqual(@as(usize, 0), lsm.active_memtable.count());
    try std.testing.expectEqual(@as(usize, 1), lsm.sstableCount());

    var val = (try lsm.get(allocator, "flush:k00050")).?;
    defer val.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 50), val.sequence);

    var results = ArrayList(KeyValuePair){};
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
    cleanupTestLSM(allocator, io, base);
    defer cleanupTestLSM(allocator, io, base);

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

    var results = ArrayList(KeyValuePair){};
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
    cleanupTestLSM(allocator, io, base);
    defer cleanupTestLSM(allocator, io, base);

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
