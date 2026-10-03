const std = @import("std");
const assert = std.debug.assert;

const event = @import("event.zig");
const fixed = @import("fixed.zig");
const EventValue = event.EventValue;

const memtable_mod = @import("memtable.zig");
const Memtable = memtable_mod.Memtable;

const sstable = @import("sstable.zig");
const SSTableReader = sstable.SSTableReader;
const SSTInfo = sstable.SSTInfo;
const bloom = @import("bloom.zig");

const manifest_mod = @import("manifest.zig");
const Manifest = manifest_mod.Manifest;

const merge_iter_mod = @import("merge_iterator.zig");
const FixedMergeIterator = merge_iter_mod.FixedMergeIterator;
const FixedMergeSource = merge_iter_mod.FixedMergeSource;

const wal_mod = @import("wal.zig");
const WriteAheadLog = wal_mod.WriteAheadLog;

const Io = std.Io;
const IoDir = Io.Dir;
const limits = @import("limits.zig");

const flush_bytes_max: u64 = limits.memtable_bytes_max;
const flush_count_max: u32 = limits.memtable_entries_max;
const flush_high_water_bytes: u64 = flush_bytes_max / 2;
const flush_high_water_count: u32 = flush_count_max / 2;
const transaction_entries_max: u32 = 3 * @as(u32, limits.append_events_max) + 2;
const compaction_trigger_l0: u32 = 4;

const compaction_trigger_level: u32 = 8;

const compaction_levels_max: u32 = 7;

const orphan_candidates_max: u16 = 4096;

comptime {
    assert(flush_bytes_max > 0);
    assert(flush_count_max > 0);
    assert(compaction_trigger_l0 >= 2);
    assert(compaction_trigger_level >= limits.compaction_fan_in_max);
    assert(compaction_levels_max >= 2);
    assert(flush_count_max - flush_high_water_count > transaction_entries_max);
    assert(flush_bytes_max - flush_high_water_bytes > limits.payload_size_max + 2 * 1024 * 1024);
}

pub const FaultPoint = enum {
    flush_after_sstable_publish,
    flush_after_manifest_publish,
    flush_after_cache_publish,
    flush_after_wal_retire,
    compaction_after_sstable_publish,
    compaction_after_manifest_publish,
    compaction_after_cache_publish,
    compaction_after_directory_sync,
};

pub const FaultInjector = struct {
    fail_at: ?FaultPoint = null,
    fail_after_retired_table: ?u16 = null,
    retired_tables: u16 = 0,
    triggered: bool = false,

    fn check(self: *FaultInjector, point: FaultPoint) !void {
        if (self.fail_at != point) return;
        self.triggered = true;
        return error.InjectedLSMFault;
    }

    fn after_retired_table(self: *FaultInjector) !void {
        self.retired_tables += 1;
        if (self.fail_after_retired_table != self.retired_tables) return;
        self.triggered = true;
        return error.InjectedLSMFault;
    }
};

// NOTE: sorted newest-first, so the first key hit is the freshest value.
const CachedTable = struct {
    id: u64,
    level: u32,
    reader: SSTableReader,
    bloom_bits: []u8,
    bloom_params: bloom.Params,

    fn definitely_absent(self: *const CachedTable, key: []const u8) bool {
        if (self.bloom_bits.len == 0) return false;
        return !bloom.mayContain(self.bloom_bits, self.bloom_params, key);
    }
};

fn index_cache_cost(footer: sstable.Footer) u64 {
    return @as(u64, footer.index_size) + @as(u64, footer.num_blocks) * @sizeOf(u32);
}

const BloomScratch = struct {
    params: bloom.Params,
    bits: []u8,
    serialize: []u8,

    fn init(allocator: std.mem.Allocator, entry_upper_bound: u64) !BloomScratch {
        const params = bloom.chooseParams(
            entry_upper_bound,
            bloom.target_false_positive_default,
        );
        const bits = try allocator.alloc(u8, params.byte_len());
        errdefer allocator.free(bits);
        const serialize = try allocator.alloc(u8, bloom.serialized_size_max(params));
        return .{ .params = params, .bits = bits, .serialize = serialize };
    }

    fn enable(self: *BloomScratch, writer: *sstable.StreamingWriter) void {
        writer.enable_bloom(self.params, self.bits, self.serialize);
    }

    fn deinit(self: *BloomScratch, allocator: std.mem.Allocator) void {
        allocator.free(self.bits);
        allocator.free(self.serialize);
    }
};

const FlushResult = struct {
    id: u64,
    level: u32,
    num_entries: u64,
    min_seq: u64,
    max_seq: u64,
    file_size: u64,
    min_key_buffer: [limits.key_length_max]u8,
    min_key_length: limits.ByteCount,
    max_key_buffer: [limits.key_length_max]u8,
    max_key_length: limits.ByteCount,

    fn min_key(self: *const FlushResult) []const u8 {
        return self.min_key_buffer[0..self.min_key_length];
    }

    fn max_key(self: *const FlushResult) []const u8 {
        return self.max_key_buffer[0..self.max_key_length];
    }
};

const CompactionPlan = struct {
    level: u32,
    new_level: u32,
    new_id: u64,
    ids: [limits.compaction_fan_in_max]u64,
    count: limits.SSTableCount,
};

pub const CompactionSummary = struct {
    tables_before: limits.CollectionCount,
    tables_after: limits.CollectionCount,
    merges: u32,
};

fn make_flush_result(
    id: u64,
    level: u32,
    footer: anytype,
    file_size: u64,
    min_key: []const u8,
    max_key: []const u8,
) FlushResult {
    var result = FlushResult{
        .id = id,
        .level = level,
        .num_entries = footer.num_entries,
        .min_seq = footer.min_seq,
        .max_seq = footer.max_seq,
        .file_size = file_size,
        .min_key_buffer = undefined,
        .min_key_length = @intCast(min_key.len),
        .max_key_buffer = undefined,
        .max_key_length = @intCast(max_key.len),
    };
    @memcpy(result.min_key_buffer[0..min_key.len], min_key);
    @memcpy(result.max_key_buffer[0..max_key.len], max_key);
    return result;
}

const ScanSource = struct {
    cursor: u64,
    key: [limits.key_length_max]u8,
    key_length: limits.ByteCount,
    value_offset: u64,
    value_length: limits.ByteCount,
    current: bool,

    fn current_key(self: *const ScanSource) []const u8 {
        assert(self.current);
        return self.key[0..self.key_length];
    }
};

const StartupMemory = struct {
    read_scratch: []u8,
    event_scratch: []u8,
    flush_event_scratch: []u8,
    sst_data_scratch: []u8,
    sst_index_scratch: []u8,
    compaction_sources: []FixedMergeSource,
    compaction_blocks: []u8,
    compaction_result: []u8,
    table_storage: []CachedTable,
    scan_sources: []ScanSource,

    fn init(allocator: std.mem.Allocator) !StartupMemory {
        const read_scratch = try allocator.alloc(u8, limits.sstable_block_size_max);
        errdefer allocator.free(read_scratch);
        const event_scratch = try allocator.alloc(u8, limits.event_serialized_size_max);
        errdefer allocator.free(event_scratch);
        const flush_event_scratch = try allocator.alloc(u8, limits.event_serialized_size_max);
        errdefer allocator.free(flush_event_scratch);
        const sst_data_scratch = try allocator.alloc(u8, limits.sstable_block_size_max);
        errdefer allocator.free(sst_data_scratch);
        const sst_index_scratch = try allocator.alloc(u8, limits.sstable_index_size_max);
        errdefer allocator.free(sst_index_scratch);
        const compaction_sources = try allocator.alloc(
            FixedMergeSource,
            limits.compaction_fan_in_max,
        );
        errdefer allocator.free(compaction_sources);
        const compaction_blocks = try allocator.alloc(
            u8,
            @as(u32, limits.compaction_fan_in_max) * limits.sstable_block_size_max,
        );
        errdefer allocator.free(compaction_blocks);
        const compaction_result = try allocator.alloc(u8, limits.sstable_block_size_max);
        errdefer allocator.free(compaction_result);
        const table_storage = try allocator.alloc(CachedTable, limits.sstables_max);
        errdefer allocator.free(table_storage);
        const scan_sources = try allocator.alloc(ScanSource, limits.sstables_max);
        errdefer allocator.free(scan_sources);
        return .{
            .read_scratch = read_scratch,
            .event_scratch = event_scratch,
            .flush_event_scratch = flush_event_scratch,
            .sst_data_scratch = sst_data_scratch,
            .sst_index_scratch = sst_index_scratch,
            .compaction_sources = compaction_sources,
            .compaction_blocks = compaction_blocks,
            .compaction_result = compaction_result,
            .table_storage = table_storage,
            .scan_sources = scan_sources,
        };
    }

    fn deinit(self: StartupMemory, allocator: std.mem.Allocator) void {
        allocator.free(self.read_scratch);
        allocator.free(self.event_scratch);
        allocator.free(self.flush_event_scratch);
        allocator.free(self.sst_data_scratch);
        allocator.free(self.sst_index_scratch);
        allocator.free(self.compaction_sources);
        allocator.free(self.compaction_blocks);
        allocator.free(self.compaction_result);
        allocator.free(self.table_storage);
        allocator.free(self.scan_sources);
    }
};

pub const LSMEngine = struct {
    allocator: std.mem.Allocator,
    io: Io,
    wal: *WriteAheadLog,
    active_memtable: *Memtable,
    flushing_memtable: *Memtable,
    flush_pending: bool,
    flushing_sst_id: u64,
    pending_flush: ?FlushResult,
    background_flush: bool,
    manifest: *Manifest,
    tables: fixed.List(CachedTable),
    table_storage: []CachedTable,
    scan_sources: []ScanSource,
    base_path: []const u8,
    wal_path: []const u8,
    index_cache_bytes: u64,
    next_sst_id: u64,
    read_scratch: []u8,
    event_scratch: []u8,
    flush_event_scratch: []u8,
    sst_data_scratch: []u8,
    sst_index_scratch: []u8,
    compaction_sources: []FixedMergeSource,
    compaction_blocks: []u8,
    compaction_result: []u8,
    compaction_active: bool,
    compaction_failures: u64,
    startup_quarantined_files: u16,
    fault_injector: ?*FaultInjector,

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

        const startup_quarantined_files = try quarantine_orphan_tables(
            allocator,
            io,
            base,
            manifest_ptr,
        );

        var next_id: u64 = 0;
        for (manifest_ptr.live.items_const()) |s| {
            if (s.id > next_id) next_id = s.id;
        }
        next_id += 1;

        const wal_ptr = try allocator.create(WriteAheadLog);
        errdefer allocator.destroy(wal_ptr);
        wal_ptr.* = try WriteAheadLog.init(allocator, wal_path, io);
        errdefer wal_ptr.deinit();

        const mt_ptr = try allocator.create(Memtable);
        errdefer allocator.destroy(mt_ptr);
        mt_ptr.* = try Memtable.init(allocator);
        errdefer mt_ptr.deinit();

        const flushing_mt_ptr = try allocator.create(Memtable);
        errdefer allocator.destroy(flushing_mt_ptr);
        flushing_mt_ptr.* = try Memtable.init(allocator);
        errdefer flushing_mt_ptr.deinit();

        const memory = try StartupMemory.init(allocator);
        errdefer memory.deinit(allocator);

        var self = LSMEngine{
            .allocator = allocator,
            .io = io,
            .wal = wal_ptr,
            .active_memtable = mt_ptr,
            .flushing_memtable = flushing_mt_ptr,
            .flush_pending = false,
            .flushing_sst_id = 0,
            .pending_flush = null,
            .background_flush = false,
            .manifest = manifest_ptr,
            .tables = try fixed.List(CachedTable).init(memory.table_storage),
            .table_storage = memory.table_storage,
            .scan_sources = memory.scan_sources,
            .base_path = base,
            .wal_path = owned_wal_path,
            .index_cache_bytes = 0,
            .next_sst_id = next_id,
            .read_scratch = memory.read_scratch,
            .event_scratch = memory.event_scratch,
            .flush_event_scratch = memory.flush_event_scratch,
            .sst_data_scratch = memory.sst_data_scratch,
            .sst_index_scratch = memory.sst_index_scratch,
            .compaction_sources = memory.compaction_sources,
            .compaction_blocks = memory.compaction_blocks,
            .compaction_result = memory.compaction_result,
            .compaction_active = false,
            .compaction_failures = 0,
            .startup_quarantined_files = startup_quarantined_files,
            .fault_injector = null,
        };
        errdefer self.close_tables();

        for (manifest_ptr.live.items_const()) |info| {
            try self.open_table(info.level, info.id);
        }

        assert(self.tables.count == self.manifest.live.count);

        if (try self.wal.flushing_exists()) {
            try self.wal.replay_flushing(self.active_memtable);
            if (self.active_memtable.count() > 0) {
                const sst_id = self.next_sst_id;
                self.next_sst_id += 1;
                const result = try self.materialize_table(self.active_memtable, sst_id);
                try self.publish_table(result);
            }
            try self.wal.retire_flushing();
            self.active_memtable.reset();
        }

        _ = try self.wal.replay(self.active_memtable);

        return self;
    }

    pub fn deinit(self: *LSMEngine) void {
        self.close_tables();
        self.active_memtable.deinit();
        self.allocator.destroy(self.active_memtable);
        self.flushing_memtable.deinit();
        self.allocator.destroy(self.flushing_memtable);
        self.wal.deinit();
        self.allocator.destroy(self.wal);
        self.manifest.deinit();
        self.allocator.destroy(self.manifest);
        self.allocator.free(self.read_scratch);
        self.allocator.free(self.event_scratch);
        self.allocator.free(self.flush_event_scratch);
        self.allocator.free(self.sst_data_scratch);
        self.allocator.free(self.sst_index_scratch);
        self.allocator.free(self.compaction_sources);
        self.allocator.free(self.compaction_blocks);
        self.allocator.free(self.compaction_result);
        self.allocator.free(self.table_storage);
        self.allocator.free(self.scan_sources);
        self.allocator.free(self.base_path);
        self.allocator.free(self.wal_path);
    }

    fn close_tables(self: *LSMEngine) void {
        for (self.tables.items()) |*t| {
            self.allocator.free(t.bloom_bits);
            t.reader.unload_index(self.allocator);
            t.reader.deinit();
        }
        self.tables.reset();
        self.index_cache_bytes = 0;
    }

    fn open_table(self: *LSMEngine, level: u32, id: u64) !void {
        assert(id > 0);
        if (self.tables.count >= limits.sstables_max) return error.TooManySSTables;
        var path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
        const path = try self.sst_path_into(&path_buffer, level, id);
        var reader = try SSTableReader.open_uncached(path, self.io);
        errdefer reader.deinit();
        const loaded = try reader.load_bloom(self.allocator);
        errdefer if (loaded) |l| self.allocator.free(l.bits);
        const index_bytes = index_cache_cost(reader.footer);
        const cache_index =
            self.index_cache_bytes + index_bytes <= limits.sstable_index_cache_bytes_max;
        if (cache_index) _ = try reader.load_index(self.allocator);
        errdefer reader.unload_index(self.allocator);

        var insert_at: limits.SSTableIndex = @intCast(self.tables.count);
        for (self.tables.items_const(), 0..) |t, i| {
            const goes_before = level < t.level or (level == t.level and id > t.id);
            if (goes_before) {
                insert_at = @intCast(i);
                break;
            }
        }
        self.tables.insert(insert_at, CachedTable{
            .id = id,
            .level = level,
            .reader = reader,
            .bloom_bits = if (loaded) |l| l.bits else &.{},
            .bloom_params = if (loaded) |l| l.params else .{ .bits = 0, .hashes = 0 },
        }) catch unreachable;
        if (reader.index != null) self.index_cache_bytes += index_bytes;
    }

    fn remove_table(self: *LSMEngine, id: u64) void {
        for (self.tables.items_const(), 0..) |t, i| {
            if (t.id == id) {
                var removed = self.tables.ordered_remove(@intCast(i));
                self.allocator.free(removed.bloom_bits);
                if (removed.reader.index != null)
                    self.index_cache_bytes -= index_cache_cost(removed.reader.footer);
                removed.reader.unload_index(self.allocator);
                removed.reader.deinit();
                return;
            }
        }
        unreachable;
    }

    pub fn put(self: *LSMEngine, key: []const u8, value: EventValue) !void {
        assert(key.len > 0);

        if (self.background_flush) {
            if (!self.active_memtable.can_insert(key, &value)) {
                return error.EventExceedsMemtableCapacity;
            }
        } else {
            // NOTE: flush before insertion so no fallible operation follows the pool copy.
            if (self.active_memtable.size_bytes >= flush_bytes_max) try self.flush();
            if (self.active_memtable.count() >= flush_count_max) try self.flush();
            if (!self.active_memtable.can_insert(key, &value)) try self.flush();
            if (!self.active_memtable.can_insert(key, &value)) {
                return error.EventExceedsMemtableCapacity;
            }
        }

        try self.wal.append_event(key, value);
        try self.active_memtable.insert(key, value);
        assert(self.active_memtable.count() > 0);
        assert(self.active_memtable.count() <= flush_count_max);
    }

    pub fn delete(self: *LSMEngine, key: []const u8, timestamp: limits.Timestamp) !void {
        assert(key.len > 0);
        const tombstone = event.makeTombstone(timestamp);
        if (self.background_flush) {
            if (!self.active_memtable.can_insert(key, &tombstone)) {
                return error.EventExceedsMemtableCapacity;
            }
        } else {
            if (self.active_memtable.size_bytes >= flush_bytes_max) try self.flush();
            if (!self.active_memtable.can_insert(key, &tombstone)) try self.flush();
            if (!self.active_memtable.can_insert(key, &tombstone)) {
                return error.EventExceedsMemtableCapacity;
            }
        }
        try self.wal.append_event(key, tombstone);
        try self.active_memtable.insert(key, tombstone);
    }

    pub fn sync(self: *LSMEngine) !void {
        try self.wal.sync();
    }

    pub fn get_bounded(
        self: *LSMEngine,
        key: []const u8,
        byte_buffer: []u8,
    ) !?EventValue {
        assert(key.len > 0);
        var arena = try fixed.ByteArena.init(byte_buffer);
        if (self.active_memtable.get_ptr(key)) |value| {
            if (event.isTombstone(value.*)) return null;
            return try copy_event_bounded(&arena, value.*);
        }
        if (self.flushing_memtable.get_ptr(key)) |value| {
            if (event.isTombstone(value.*)) return null;
            return try copy_event_bounded(&arena, value.*);
        }
        for (self.tables.items()) |*table| {
            if (table.definitely_absent(key)) continue;
            if (try table.reader.get_into(key, self.read_scratch)) |value| {
                if (event.isTombstone(value)) return null;
                return try copy_event_bounded(&arena, value);
            }
        }
        return null;
    }

    fn copy_event_bounded(arena: *fixed.ByteArena, value: EventValue) !EventValue {
        const bytes_required = try limits.byte_count(
            value.origin_service.len + value.event_type.len + value.payload.len +
                value.signature.len + value.vector_clock.len,
        );
        if (bytes_required > arena.remaining()) return error.ResultBufferTooSmall;
        const origin = arena.write(value.origin_service) catch unreachable;
        const event_type = arena.write(value.event_type) catch unreachable;
        const payload = arena.write(value.payload) catch unreachable;
        const signature = arena.write(value.signature) catch unreachable;
        const vector_clock = arena.write(value.vector_clock) catch unreachable;
        return .{
            .sequence = value.sequence,
            .event_type = event_type,
            .payload = payload,
            .timestamp = value.timestamp,
            .origin_service = origin,
            .signature = signature,
            .vector_clock = vector_clock,
        };
    }

    pub fn contains(self: *LSMEngine, key: []const u8) !bool {
        assert(key.len > 0);

        if (self.active_memtable.get_ptr(key)) |value| {
            return !event.isTombstone(value.*);
        }
        if (self.flushing_memtable.get_ptr(key)) |value| {
            return !event.isTombstone(value.*);
        }
        for (self.tables.items()) |*t| {
            if (t.definitely_absent(key)) continue;
            if (try t.reader.get_into(key, self.read_scratch)) |value| {
                return !event.isTombstone(value);
            }
        }

        return false;
    }

    pub fn visit_prefix(
        self: *LSMEngine,
        prefix: []const u8,
        context: anytype,
        comptime visit: anytype,
    ) !event.VisitControl {
        return self.visit_prefix_from(prefix, prefix, context, visit);
    }

    pub fn visit_prefix_from(
        self: *LSMEngine,
        prefix: []const u8,
        start_key: []const u8,
        context: anytype,
        comptime visit: anytype,
    ) !event.VisitControl {
        assert(std.mem.startsWith(u8, start_key, prefix));
        try self.initialize_scan_sources(start_key);
        var active_iterator = self.active_memtable.iterator_from(start_key);
        var active_entry = active_iterator.next();
        var flushing_iterator = self.flushing_memtable.iterator_from(start_key);
        var flushing_entry = flushing_iterator.next();
        var key_storage: [limits.key_length_max]u8 = undefined;
        while (true) {
            var minimum: ?[]const u8 = null;
            if (active_entry) |entry| minimum = entry.key;
            if (flushing_entry) |entry| {
                if (minimum == null or std.mem.order(u8, entry.key, minimum.?) == .lt)
                    minimum = entry.key;
            }
            for (self.scan_sources[0..self.tables.count]) |*source| {
                if (!source.current) continue;
                const key = source.current_key();
                if (minimum == null or std.mem.order(u8, key, minimum.?) == .lt)
                    minimum = key;
            }
            const borrowed_key = minimum orelse return .continue_scan;
            @memcpy(key_storage[0..borrowed_key.len], borrowed_key);
            const key = key_storage[0..borrowed_key.len];
            const order = std.mem.order(u8, key, prefix);
            if (!std.mem.startsWith(u8, key, prefix) and order == .gt)
                return .continue_scan;

            if (std.mem.startsWith(u8, key, prefix)) {
                const value = value_blk: {
                    if (active_entry) |entry| {
                        if (std.mem.eql(u8, entry.key, key)) break :value_blk entry.value;
                    }
                    if (flushing_entry) |entry| {
                        if (std.mem.eql(u8, entry.key, key)) break :value_blk entry.value;
                    }
                    break :value_blk try self.scan_value(key);
                };
                if (!event.isTombstone(value)) {
                    if (try visit(context, key, value) == .stop) return .stop;
                }
            }
            if (active_entry) |entry| {
                if (std.mem.eql(u8, entry.key, key)) active_entry = active_iterator.next();
            }
            if (flushing_entry) |entry| {
                if (std.mem.eql(u8, entry.key, key)) flushing_entry = flushing_iterator.next();
            }
            for (self.scan_sources[0..self.tables.count], 0..) |*source, index| {
                if (source.current and std.mem.eql(u8, source.current_key(), key))
                    try self.advance_scan_source(index);
            }
        }
    }

    fn initialize_scan_sources(self: *LSMEngine, start_key: []const u8) !void {
        assert(self.tables.count <= self.scan_sources.len);
        var seek_key_buffer: [limits.key_length_max]u8 = undefined;
        for (self.scan_sources[0..self.tables.count], 0..) |*source, index| {
            const reader = &self.tables.items()[index].reader;
            source.* = .{
                .cursor = try reader.scan_cursor_at(start_key, &seek_key_buffer),
                .key = undefined,
                .key_length = 0,
                .value_offset = 0,
                .value_length = 0,
                .current = false,
            };
            try self.advance_scan_source(index);
            var skipped: u32 = 0;
            while (source.current and std.mem.order(u8, source.current_key(), start_key) == .lt) {
                skipped += 1;
                assert(skipped <= sstable.block_size_target);
                try self.advance_scan_source(index);
            }
        }
    }

    fn advance_scan_source(self: *LSMEngine, index: usize) !void {
        assert(index < self.tables.count);
        const source = &self.scan_sources[index];
        const table = &self.tables.items()[index];
        const entry = try table.reader.scan_next_key(&source.cursor, &source.key);
        if (entry) |current| {
            source.key_length = try limits.byte_count(current.key.len);
            source.value_offset = current.value_offset;
            source.value_length = current.value_length;
            source.current = true;
        } else {
            source.current = false;
        }
    }

    fn scan_value(self: *LSMEngine, key: []const u8) !EventValue {
        for (self.scan_sources[0..self.tables.count], 0..) |*source, index| {
            if (!source.current or !std.mem.eql(u8, source.current_key(), key)) continue;
            return try self.tables.items()[index].reader.scan_value_into(.{
                .key = source.current_key(),
                .value_offset = source.value_offset,
                .value_length = source.value_length,
            }, self.event_scratch);
        }
        unreachable;
    }

    pub fn memtable_count(self: *const LSMEngine) u32 {
        return self.active_memtable.count();
    }

    pub fn memtable_bytes(self: *const LSMEngine) limits.ByteCount {
        return self.active_memtable.size_bytes;
    }

    pub fn wal_bytes(self: *const LSMEngine) u64 {
        return self.wal.write_pos + self.wal.buf_len;
    }

    pub fn sstable_count(self: *const LSMEngine) u16 {
        assert(self.tables.count == self.manifest.live.count);
        return @intCast(self.tables.count);
    }

    pub fn compaction_is_active(self: *const LSMEngine) bool {
        return self.compaction_active;
    }

    pub fn compaction_failure_count(self: *const LSMEngine) u64 {
        return self.compaction_failures;
    }

    pub fn startup_quarantine_count(self: *const LSMEngine) u16 {
        return self.startup_quarantined_files;
    }

    pub fn set_fault_injector(self: *LSMEngine, injector: ?*FaultInjector) void {
        self.fault_injector = injector;
    }

    pub fn flush(self: *LSMEngine) !void {
        if (self.active_memtable.count() == 0) return;
        const tables_before = self.tables.count;
        const sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        const result = try self.materialize_table(self.active_memtable, sst_id);
        try self.publish_table(result);
        try self.wal.truncate();
        try self.inject_fault(.flush_after_wal_retire);

        self.active_memtable.reset();

        assert(self.active_memtable.count() == 0);
        assert(self.tables.count == tables_before + 1);
        assert(self.tables.count == self.manifest.live.count);

        try self.compact_overfull_levels();
    }

    // NOTE: one merge per level per flush, so a backlog drains without stalling the store.
    fn compact_overfull_levels(self: *LSMEngine) !void {
        var level: u32 = 0;
        while (level < compaction_levels_max) : (level += 1) {
            const trigger = if (level == 0) compaction_trigger_l0 else compaction_trigger_level;
            if (self.manifest.count_at_level(level) < trigger) continue;
            self.compaction_active = true;
            self.compact_level(level) catch |err| {
                self.compaction_active = false;
                self.compaction_failures += 1;
                return err;
            };
            self.compaction_active = false;
        }
    }

    pub fn compact_until_stable(self: *LSMEngine) !CompactionSummary {
        const tables_before = self.manifest.live.count;
        var merges: u32 = 0;
        while (true) {
            const before = self.manifest.live.count;
            try self.compact_overfull_levels();
            const after = self.manifest.live.count;
            if (after == before) break;
            merges += 1;
        }
        return .{
            .tables_before = tables_before,
            .tables_after = self.manifest.live.count,
            .merges = merges,
        };
    }

    fn materialize_table(
        self: *LSMEngine,
        memtable: *Memtable,
        sst_id: u64,
    ) !FlushResult {
        var path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
        const path = try self.sst_path_into(&path_buffer, 0, sst_id);
        var writer = try sstable.StreamingWriter.init(
            path,
            self.io,
            self.sst_data_scratch,
            self.sst_index_scratch,
        );
        errdefer writer.abort();
        var bloom_scratch = try BloomScratch.init(self.allocator, memtable.count());
        defer bloom_scratch.deinit(self.allocator);
        bloom_scratch.enable(&writer);
        var iter = memtable.iterator();
        while (iter.next()) |entry| {
            const serialized = try entry.value.serialize_into(self.flush_event_scratch);
            try writer.append(.{ .key = entry.key, .value = serialized });
        }
        const footer = try writer.finish();
        try self.inject_fault(.flush_after_sstable_publish);
        assert(footer.num_entries == memtable.count());
        const file_size = footer.index_offset + footer.index_size +
            footer.bloom_length + sstable.footer_size;
        return make_flush_result(
            sst_id,
            0,
            footer,
            file_size,
            writer.minimum_key(),
            writer.maximum_key(),
        );
    }

    fn publish_table(self: *LSMEngine, result: FlushResult) !void {
        try self.manifest.add_sstable(.{
            .id = result.id,
            .level = result.level,
            .min_key = result.min_key(),
            .max_key = result.max_key(),
            .num_entries = result.num_entries,
            .min_seq = result.min_seq,
            .max_seq = result.max_seq,
            .file_size = result.file_size,
        });
        try self.inject_fault(.flush_after_manifest_publish);
        try self.open_table(result.level, result.id);
        try self.inject_fault(.flush_after_cache_publish);
    }

    pub fn enable_background_flush(self: *LSMEngine) void {
        self.background_flush = true;
    }

    pub fn active_has_room(self: *const LSMEngine) bool {
        return self.active_memtable.size_bytes < flush_high_water_bytes and
            self.active_memtable.count() < flush_high_water_count;
    }

    pub fn flush_pending_now(self: *const LSMEngine) bool {
        return self.flush_pending;
    }

    pub fn can_freeze(self: *const LSMEngine) bool {
        return !self.flush_pending and self.active_memtable.count() > 0;
    }

    pub fn freeze(self: *LSMEngine) !void {
        assert(!self.flush_pending);
        assert(self.flushing_memtable.count() == 0);
        assert(self.active_memtable.count() > 0);
        const spare = self.flushing_memtable;
        self.flushing_memtable = self.active_memtable;
        self.active_memtable = spare;
        self.flushing_sst_id = self.next_sst_id;
        self.next_sst_id += 1;
        try self.wal.rotate();
        self.flush_pending = true;
    }

    pub fn flush_frozen_write(self: *LSMEngine) !void {
        assert(self.flush_pending);
        assert(self.pending_flush == null);
        self.pending_flush = try self.materialize_table(
            self.flushing_memtable,
            self.flushing_sst_id,
        );
    }

    pub fn flush_frozen_publish(self: *LSMEngine) !void {
        assert(self.flush_pending);
        const result = self.pending_flush orelse return error.NoPendingFlush;
        try self.publish_table(result);
        try self.wal.retire_flushing();
        try self.inject_fault(.flush_after_wal_retire);
        self.flushing_memtable.reset();
        self.pending_flush = null;
        self.flush_pending = false;
    }

    pub fn compact_level(self: *LSMEngine, level: u32) !void {
        const level_tables_before = self.manifest.count_at_level(level);
        var readers: [limits.compaction_fan_in_max]*SSTableReader = undefined;
        var ids: [limits.compaction_fan_in_max]u64 = undefined;
        var count: limits.SSTableCount = 0;
        for (self.tables.items()) |*t| {
            if (t.level == level) {
                if (count == limits.compaction_fan_in_max) break;
                readers[count] = &t.reader;
                ids[count] = t.id;
                count += 1;
            }
        }
        if (count < 2) return;
        var merge = try FixedMergeIterator.init(
            self.compaction_sources,
            readers[0..count],
            ids[0..count],
            self.compaction_blocks,
            self.compaction_result,
        );

        const tables_before = self.tables.count;
        const new_id = self.next_sst_id;
        self.next_sst_id += 1;
        const new_level = level + 1;

        var new_path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
        const new_path = try self.sst_path_into(&new_path_buffer, new_level, new_id);
        var writer = try sstable.StreamingWriter.init(
            new_path,
            self.io,
            self.sst_data_scratch,
            self.sst_index_scratch,
        );
        errdefer writer.abort();
        var entry_upper_bound: u64 = 0;
        for (readers[0..count]) |r| entry_upper_bound += r.footer.num_entries;
        var bloom_scratch = try BloomScratch.init(self.allocator, entry_upper_bound);
        defer bloom_scratch.deinit(self.allocator);
        bloom_scratch.enable(&writer);
        while (try merge.next()) |entry| try writer.append(entry);
        const footer = try writer.finish();
        try self.inject_fault(.compaction_after_sstable_publish);
        if (footer.num_entries == 0) return;
        const file_size = footer.index_offset + footer.index_size +
            footer.bloom_length + sstable.footer_size;

        try self.manifest.atomic_compaction(ids[0..count], .{
            .id = new_id,
            .level = new_level,
            .min_key = writer.minimum_key(),
            .max_key = writer.maximum_key(),
            .num_entries = footer.num_entries,
            .min_seq = footer.min_seq,
            .max_seq = footer.max_seq,
            .file_size = file_size,
        });
        try self.inject_fault(.compaction_after_manifest_publish);

        for (ids[0..count]) |id| self.remove_table(id);
        try self.open_table(new_level, new_id);
        try self.inject_fault(.compaction_after_cache_publish);

        assert(self.tables.count == tables_before - count + 1);
        assert(self.tables.count == self.manifest.live.count);
        assert(self.manifest.count_at_level(level) == level_tables_before - count);

        for (ids[0..count]) |id| {
            var old_path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
            const old_path = try self.sst_path_into(&old_path_buffer, level, id);
            try IoDir.cwd().deleteFile(self.io, old_path);
            try self.after_retired_table();
        }
        try sync_parent_directory(self.base_path, self.io);
        try self.inject_fault(.compaction_after_directory_sync);
    }

    pub fn compaction_prepare(self: *LSMEngine, level: u32) ?CompactionPlan {
        var plan = CompactionPlan{
            .level = level,
            .new_level = level + 1,
            .new_id = 0,
            .ids = undefined,
            .count = 0,
        };
        for (self.tables.items()) |*t| {
            if (t.level == level) {
                if (plan.count == limits.compaction_fan_in_max) break;
                plan.ids[plan.count] = t.id;
                plan.count += 1;
            }
        }
        if (plan.count < 2) return null;
        plan.new_id = self.next_sst_id;
        self.next_sst_id += 1;
        self.compaction_active = true;
        return plan;
    }

    pub fn compaction_write(self: *LSMEngine, plan: CompactionPlan) !?FlushResult {
        var readers: [limits.compaction_fan_in_max]SSTableReader = undefined;
        var reader_ptrs: [limits.compaction_fan_in_max]*SSTableReader = undefined;
        var opened: usize = 0;
        defer for (0..opened) |i| readers[i].deinit();
        for (0..plan.count) |i| {
            var path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
            const path = try self.sst_path_into(&path_buffer, plan.level, plan.ids[i]);
            readers[i] = try SSTableReader.open_uncached(path, self.io);
            opened += 1;
            reader_ptrs[i] = &readers[i];
        }
        var merge = try FixedMergeIterator.init(
            self.compaction_sources,
            reader_ptrs[0..plan.count],
            plan.ids[0..plan.count],
            self.compaction_blocks,
            self.compaction_result,
        );
        var new_path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
        const new_path = try self.sst_path_into(&new_path_buffer, plan.new_level, plan.new_id);
        var writer = try sstable.StreamingWriter.init(
            new_path,
            self.io,
            self.sst_data_scratch,
            self.sst_index_scratch,
        );
        errdefer writer.abort();
        var entry_upper_bound: u64 = 0;
        for (0..plan.count) |i| entry_upper_bound += readers[i].footer.num_entries;
        var bloom_scratch = try BloomScratch.init(self.allocator, entry_upper_bound);
        defer bloom_scratch.deinit(self.allocator);
        bloom_scratch.enable(&writer);
        while (try merge.next()) |entry| try writer.append(entry);
        const footer = try writer.finish();
        try self.inject_fault(.compaction_after_sstable_publish);
        if (footer.num_entries == 0) return null;
        const file_size = footer.index_offset + footer.index_size +
            footer.bloom_length + sstable.footer_size;
        return make_flush_result(
            plan.new_id,
            plan.new_level,
            footer,
            file_size,
            writer.minimum_key(),
            writer.maximum_key(),
        );
    }

    pub fn compaction_publish(
        self: *LSMEngine,
        plan: CompactionPlan,
        result: FlushResult,
    ) !void {
        try self.manifest.atomic_compaction(plan.ids[0..plan.count], .{
            .id = result.id,
            .level = result.level,
            .min_key = result.min_key(),
            .max_key = result.max_key(),
            .num_entries = result.num_entries,
            .min_seq = result.min_seq,
            .max_seq = result.max_seq,
            .file_size = result.file_size,
        });
        try self.inject_fault(.compaction_after_manifest_publish);
        for (plan.ids[0..plan.count]) |id| self.remove_table(id);
        try self.open_table(plan.new_level, plan.new_id);
        try self.inject_fault(.compaction_after_cache_publish);
        for (plan.ids[0..plan.count]) |id| {
            var old_path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
            const old_path = try self.sst_path_into(&old_path_buffer, plan.level, id);
            try IoDir.cwd().deleteFile(self.io, old_path);
            try self.after_retired_table();
        }
        try sync_parent_directory(self.base_path, self.io);
        try self.inject_fault(.compaction_after_directory_sync);
        self.compaction_active = false;
    }

    pub fn compaction_abandon(self: *LSMEngine) void {
        self.compaction_active = false;
        self.compaction_failures += 1;
    }

    fn inject_fault(self: *LSMEngine, point: FaultPoint) !void {
        if (self.fault_injector) |injector| try injector.check(point);
    }

    fn after_retired_table(self: *LSMEngine) !void {
        if (self.fault_injector) |injector| try injector.after_retired_table();
    }

    fn sst_path_into(
        self: *const LSMEngine,
        buffer: []u8,
        level: u32,
        id: u64,
    ) ![]const u8 {
        return std.fmt.bufPrint(buffer, "{s}_L{d}_{d:0>8}.sst", .{
            self.base_path,
            level,
            id,
        }) catch return error.PathTooLong;
    }

    fn derive_base(allocator: std.mem.Allocator, wal_path: []const u8) ![]const u8 {
        if (std.mem.endsWith(u8, wal_path, ".wal")) {
            return try allocator.dupe(u8, wal_path[0 .. wal_path.len - 4]);
        }
        return try allocator.dupe(u8, wal_path);
    }
};

const TableName = struct {
    level: u32,
    id: u64,
    temporary: bool,
};

fn quarantine_orphan_tables(
    allocator: std.mem.Allocator,
    io: Io,
    base: []const u8,
    manifest: *const Manifest,
) !u16 {
    const parent_path = std.fs.path.dirname(base) orelse ".";
    const base_name = std.fs.path.basename(base);
    const quarantine_path = try std.fmt.allocPrint(allocator, "{s}.quarantine", .{base});
    defer allocator.free(quarantine_path);
    var parent = try IoDir.cwd().openDir(io, parent_path, .{ .iterate = true });
    defer parent.close(io);
    var quarantine: ?IoDir = null;
    defer if (quarantine) |dir| dir.close(io);
    var count: u16 = 0;
    var iterator = parent.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind == .directory) continue;
        const table = parse_table_name(base_name, entry.name) orelse continue;
        if (!table.temporary and manifest_has_table(manifest, table)) continue;
        count = std.math.add(u16, count, 1) catch return error.TooManyOrphanCandidates;
        if (count > orphan_candidates_max) return error.TooManyOrphanCandidates;
        if (quarantine == null) {
            IoDir.cwd().createDir(io, quarantine_path, .default_dir) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
            quarantine = try IoDir.cwd().openDir(io, quarantine_path, .{});
        }
        try parent.renamePreserve(entry.name, quarantine.?, entry.name, io);
    }
    if (count > 0) {
        try sync_directory(parent_path, io);
        try sync_directory(quarantine_path, io);
    }
    return count;
}

fn manifest_has_table(manifest: *const Manifest, table: TableName) bool {
    for (manifest.live.items_const()) |live| {
        if (live.level == table.level and live.id == table.id) return true;
    }
    return false;
}

fn parse_table_name(base_name: []const u8, name: []const u8) ?TableName {
    if (!std.mem.startsWith(u8, name, base_name)) return null;
    const temporary = std.mem.endsWith(u8, name, ".sst.tmp");
    const canonical = if (temporary) name[0 .. name.len - ".tmp".len] else name;
    const suffix = canonical[base_name.len..];
    if (!std.mem.startsWith(u8, suffix, "_L") or
        !std.mem.endsWith(u8, suffix, ".sst")) return null;
    const body = suffix[2 .. suffix.len - ".sst".len];
    const separator = std.mem.indexOfScalar(u8, body, '_') orelse return null;
    if (separator == 0 or separator + 1 == body.len) return null;
    const level = std.fmt.parseInt(u32, body[0..separator], 10) catch return null;
    const id = std.fmt.parseInt(u64, body[separator + 1 ..], 10) catch return null;
    if (id == 0) return null;
    var expected_buffer: [IoDir.max_name_bytes]u8 = undefined;
    const expected = std.fmt.bufPrint(
        &expected_buffer,
        "{s}_L{d}_{d:0>8}.sst",
        .{ base_name, level, id },
    ) catch return null;
    if (!std.mem.eql(u8, expected, canonical)) return null;
    return .{ .level = level, .id = id, .temporary = temporary };
}

fn sync_parent_directory(path: []const u8, io: Io) !void {
    try sync_directory(std.fs.path.dirname(path) orelse ".", io);
}

fn sync_directory(path: []const u8, io: Io) !void {
    const handle = try std.posix.openat(
        std.posix.AT.FDCWD,
        path,
        .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true },
        0,
    );
    const file: Io.File = .{
        .handle = handle,
        .flags = .{ .nonblocking = false },
    };
    defer file.close(io);
    file.sync(io) catch return error.DirectorySyncFailed;
}

fn cleanup_test_lsm(allocator: std.mem.Allocator, io: Io, base: []const u8) void {
    const wal_path = std.fmt.allocPrint(allocator, "{s}.wal", .{base}) catch return;
    defer allocator.free(wal_path);
    const crumb = std.fmt.allocPrint(allocator, "{s}.crumb", .{base}) catch return;
    defer allocator.free(crumb);
    const manifest = std.fmt.allocPrint(allocator, "{s}.manifest", .{base}) catch return;
    defer allocator.free(manifest);

    const flushing = std.fmt.allocPrint(allocator, "{s}.wal.flushing", .{base}) catch return;
    defer allocator.free(flushing);

    IoDir.cwd().deleteFile(io, wal_path) catch {};
    IoDir.cwd().deleteFile(io, flushing) catch {};
    IoDir.cwd().deleteFile(io, crumb) catch {};
    IoDir.cwd().deleteFile(io, manifest) catch {};
    const quarantine = std.fmt.allocPrint(allocator, "{s}.quarantine", .{base}) catch return;
    defer allocator.free(quarantine);
    IoDir.cwd().deleteTree(io, quarantine) catch {};

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

test "bloom-filtered tables never miss a present key across many tables" {
    const base = "data/test_lsm_bloom_correctness";
    const wal_path = "data/test_lsm_bloom_correctness.wal";
    const io = Io.Threaded.global_single_threaded.io();
    cleanup_test_lsm(std.testing.allocator, io, base);
    defer cleanup_test_lsm(std.testing.allocator, io, base);

    var lsm = try LSMEngine.init(std.testing.allocator, wal_path, io);
    defer lsm.deinit();

    const total = 1500;
    var seq: u64 = 0;
    for (0..total) |i| {
        seq += 1;
        var key_buffer: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "present:{d}", .{i});
        try lsm.put(key, .{
            .sequence = seq,
            .event_type = "e",
            .payload = "{}",
            .timestamp = 1,
            .origin_service = "svc",
        });
        if (i % 25 == 24) try lsm.flush();
    }
    try lsm.flush();

    var buffer: [limits.event_serialized_size_max]u8 = undefined;
    for (0..total) |i| {
        var key_buffer: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "present:{d}", .{i});
        const value = try lsm.get_bounded(key, &buffer);
        try std.testing.expect(value != null);
    }
    for (0..total) |i| {
        var key_buffer: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "absent:{d}", .{i});
        try std.testing.expect((try lsm.get_bounded(key, &buffer)) == null);
    }

    lsm.deinit();
    lsm = try LSMEngine.init(std.testing.allocator, wal_path, io);
    for (0..total) |i| {
        var key_buffer: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "present:{d}", .{i});
        try std.testing.expect((try lsm.get_bounded(key, &buffer)) != null);
    }
}

fn expect_sequence(lsm: *LSMEngine, key: []const u8, expected: u64) !void {
    var buffer: [limits.event_serialized_size_max]u8 = undefined;
    const value = try lsm.get_bounded(key, &buffer);
    try std.testing.expect(value != null);
    try std.testing.expectEqual(expected, value.?.sequence);
}

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

const TestScanCount = struct {
    count: limits.CollectionCount = 0,

    fn visit(self: *TestScanCount, _: []const u8, _: EventValue) !event.VisitControl {
        self.count += 1;
        return .continue_scan;
    }
};

fn scan_count(lsm: *LSMEngine, prefix: []const u8) !limits.CollectionCount {
    var context = TestScanCount{};
    _ = try lsm.visit_prefix(prefix, &context, TestScanCount.visit);
    return context.count;
}

test "background flush serves the flushing tier then publishes to a table" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const base = "data/test_lsm_bg_flush";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_bg_flush.wal", io);
    defer lsm.deinit();
    lsm.enable_background_flush();

    try lsm.put("k1", try test_event_value(1, "svc", "e", "a", 0));
    try lsm.put("k2", try test_event_value(2, "svc", "e", "b", 0));
    try lsm.sync();

    try lsm.freeze();
    try std.testing.expect(lsm.flush_pending_now());
    try expect_sequence(&lsm, "k1", 1);
    try expect_sequence(&lsm, "k2", 2);
    try std.testing.expectEqual(@as(limits.CollectionCount, 2), try scan_count(&lsm, "k"));

    try lsm.put("k3", try test_event_value(3, "svc", "e", "c", 0));
    try lsm.sync();
    try std.testing.expectEqual(@as(limits.CollectionCount, 3), try scan_count(&lsm, "k"));

    try lsm.flush_frozen_write();
    try lsm.flush_frozen_publish();
    try std.testing.expect(!lsm.flush_pending_now());

    try expect_sequence(&lsm, "k1", 1);
    try expect_sequence(&lsm, "k3", 3);
    try std.testing.expectEqual(@as(limits.CollectionCount, 3), try scan_count(&lsm, "k"));
    try std.testing.expect(!try lsm.wal.flushing_exists());
}

test "startup consolidates an interrupted background flush from the flushing segment" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const base = "data/test_lsm_bg_consolidate";
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    {
        var lsm = try LSMEngine.init(allocator, "data/test_lsm_bg_consolidate.wal", io);
        defer lsm.deinit();
        lsm.enable_background_flush();
        try lsm.put("s1", try test_event_value(1, "svc", "e", "x", 0));
        try lsm.put("s2", try test_event_value(2, "svc", "e", "y", 0));
        try lsm.sync();
        try lsm.freeze();
        try lsm.put("s3", try test_event_value(3, "svc", "e", "z", 0));
        try lsm.sync();
    }

    var lsm = try LSMEngine.init(allocator, "data/test_lsm_bg_consolidate.wal", io);
    defer lsm.deinit();
    try std.testing.expect(!lsm.flush_pending_now());
    try std.testing.expect(!try lsm.wal.flushing_exists());
    try expect_sequence(&lsm, "s1", 1);
    try expect_sequence(&lsm, "s2", 2);
    try expect_sequence(&lsm, "s3", 3);
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

    const val = try test_event_value(1, "svc", "evt", "{}", 100);
    try lsm.put("key1", val);

    try expect_sequence(&lsm, "key1", 1);
    var buffer: [limits.event_serialized_size_max]u8 = undefined;
    try std.testing.expect(try lsm.get_bounded("missing", &buffer) == null);
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
        const val = try test_event_value(i + 1, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }
    for (0..3) |i| {
        const key = try std.fmt.allocPrint(allocator, "b:{}", .{i});
        defer allocator.free(key);
        const val = try test_event_value(i + 10, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    try std.testing.expectEqual(@as(limits.CollectionCount, 5), try scan_count(&lsm, "a:"));
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

    const val = try test_event_value(1, "svc", "evt", "{}", 0);
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
            const val = try test_event_value(i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
    }

    {
        var lsm = try LSMEngine.init(allocator, "data/test_lsm_recovery.wal", io);
        defer lsm.deinit();

        try expect_sequence(&lsm, "k1", 1);
        try expect_sequence(&lsm, "k3", 3);
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
        const val = try test_event_value(i, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    try std.testing.expectEqual(@as(usize, 100), lsm.active_memtable.count());
    try std.testing.expectEqual(@as(usize, 0), lsm.sstable_count());

    try lsm.flush();

    try std.testing.expectEqual(@as(usize, 0), lsm.active_memtable.count());
    try std.testing.expectEqual(@as(usize, 1), lsm.sstable_count());

    try expect_sequence(&lsm, "flush:k00050", 50);
    try std.testing.expectEqual(
        @as(limits.CollectionCount, 100),
        try scan_count(&lsm, "flush:"),
    );
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
        const val = try test_event_value(i, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }
    try lsm.flush();

    for (6..11) |i| {
        const key = try std.fmt.allocPrint(allocator, "cf:k{}", .{i});
        defer allocator.free(key);
        const val = try test_event_value(i, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    try expect_sequence(&lsm, "cf:k3", 3);
    try expect_sequence(&lsm, "cf:k8", 8);
    try std.testing.expectEqual(@as(limits.CollectionCount, 10), try scan_count(&lsm, "cf:"));
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
            const val = try test_event_value(i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
        try lsm.flush();

        for (6..9) |i| {
            const key = try std.fmt.allocPrint(allocator, "fr:k{}", .{i});
            defer allocator.free(key);
            const val = try test_event_value(i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
    }

    {
        var lsm = try LSMEngine.init(allocator, "data/test_lsm_flush_recovery.wal", io);
        defer lsm.deinit();

        try expect_sequence(&lsm, "fr:k2", 2);
        try expect_sequence(&lsm, "fr:k7", 7);
    }
}

fn run_flush_fault_recovery_case(
    allocator: std.mem.Allocator,
    io: Io,
    point: FaultPoint,
    case_index: usize,
) !void {
    var base_buffer: [96]u8 = undefined;
    const base = try std.fmt.bufPrint(&base_buffer, "data/test_lsm_flush_fault_{d}", .{case_index});
    var wal_buffer: [100]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&wal_buffer, "{s}.wal", .{base});
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        for (1..5) |sequence| {
            var key_buffer: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "fault:k{d}", .{sequence});
            try lsm.put(key, try test_event_value(sequence, "svc", "evt", "{}", 0));
        }
        try lsm.sync();
        var injector = FaultInjector{ .fail_at = point };
        lsm.set_fault_injector(&injector);
        try std.testing.expectError(error.InjectedLSMFault, lsm.flush());
        try std.testing.expect(injector.triggered);
    }

    {
        var recovered = try LSMEngine.init(allocator, wal_path, io);
        defer recovered.deinit();
        const before_manifest = point == .flush_after_sstable_publish;
        try std.testing.expectEqual(@as(u16, @intFromBool(before_manifest)), recovered.startup_quarantine_count());
        try std.testing.expectEqual(@as(u16, @intFromBool(!before_manifest)), recovered.sstable_count());
        try std.testing.expectEqual(@as(limits.CollectionCount, 4), try scan_count(&recovered, "fault:"));
        try expect_sequence(&recovered, "fault:k4", 4);

        try recovered.put("fault:after-restart", try test_event_value(5, "svc", "evt", "{}", 0));
        try recovered.sync();
        try recovered.flush();
        try expect_sequence(&recovered, "fault:after-restart", 5);
    }
}

test "flush crash boundaries recover complete data and quarantine pre-manifest tables" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const points = [_]FaultPoint{
        .flush_after_sstable_publish,
        .flush_after_manifest_publish,
        .flush_after_cache_publish,
        .flush_after_wal_retire,
    };
    for (points, 0..) |point, case_index| {
        try run_flush_fault_recovery_case(allocator, io, point, case_index);
    }
}

fn run_compaction_fault_recovery_case(
    allocator: std.mem.Allocator,
    io: Io,
    injector: FaultInjector,
    expected_quarantine: u16,
    case_index: usize,
) !void {
    var base_buffer: [96]u8 = undefined;
    const base = try std.fmt.bufPrint(&base_buffer, "data/test_lsm_compact_fault_{d}", .{case_index});
    var wal_buffer: [100]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&wal_buffer, "{s}.wal", .{base});
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        for (0..3) |round| try put_compaction_round(&lsm, round);
        var active_injector = injector;
        lsm.set_fault_injector(&active_injector);
        try put_compaction_entries(&lsm, 3);
        try lsm.sync();
        try std.testing.expectError(error.InjectedLSMFault, lsm.flush());
        try std.testing.expect(active_injector.triggered);
    }

    {
        var recovered = try LSMEngine.init(allocator, wal_path, io);
        defer recovered.deinit();
        try std.testing.expectEqual(expected_quarantine, recovered.startup_quarantine_count());
        try std.testing.expectEqual(@as(limits.CollectionCount, 3), try scan_count(&recovered, "compact-fault:"));
        try expect_sequence(&recovered, "compact-fault:k2", 33);
        try recovered.put("compact-fault:after-restart", try test_event_value(100, "svc", "evt", "{}", 0));
        try recovered.sync();
        try recovered.flush();
        try expect_sequence(&recovered, "compact-fault:after-restart", 100);
    }
}

fn put_compaction_entries(lsm: *LSMEngine, round: usize) !void {
    for (0..3) |key_index| {
        var key_buffer: [40]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "compact-fault:k{d}", .{key_index});
        const sequence = round * 10 + key_index + 1;
        try lsm.put(key, try test_event_value(sequence, "svc", "evt", "{}", 0));
    }
}

fn put_compaction_round(lsm: *LSMEngine, round: usize) !void {
    try put_compaction_entries(lsm, round);
    try lsm.sync();
    try lsm.flush();
}

test "compaction crash boundaries preserve data and quarantine superseded tables" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const points = [_]FaultPoint{
        .compaction_after_sstable_publish,
        .compaction_after_manifest_publish,
        .compaction_after_cache_publish,
        .compaction_after_directory_sync,
    };
    const quarantine_counts = [_]u16{ 1, 4, 4, 0 };
    for (points, quarantine_counts, 0..) |point, quarantine_count, case_index| {
        try run_compaction_fault_recovery_case(
            allocator,
            io,
            .{ .fail_at = point },
            quarantine_count,
            case_index,
        );
    }
    for (1..5) |retired_count| {
        try run_compaction_fault_recovery_case(
            allocator,
            io,
            .{ .fail_after_retired_table = @intCast(retired_count) },
            @intCast(4 - retired_count),
            points.len + retired_count,
        );
    }
}

fn run_background_flush_fault_recovery_case(
    allocator: std.mem.Allocator,
    io: Io,
    point: FaultPoint,
    expected_quarantine: u16,
    case_index: usize,
) !void {
    var base_buffer: [96]u8 = undefined;
    const base = try std.fmt.bufPrint(
        &base_buffer,
        "data/test_lsm_bg_flush_fault_{d}",
        .{case_index},
    );
    var wal_buffer: [104]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&wal_buffer, "{s}.wal", .{base});
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        lsm.enable_background_flush();
        for (1..5) |sequence| {
            var key_buffer: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "bgf:k{d}", .{sequence});
            try lsm.put(key, try test_event_value(sequence, "svc", "evt", "{}", 0));
        }
        try lsm.sync();
        try lsm.freeze();
        for (5..8) |sequence| {
            var key_buffer: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "bgf:k{d}", .{sequence});
            try lsm.put(key, try test_event_value(sequence, "svc", "evt", "{}", 0));
        }
        try lsm.sync();
        var injector = FaultInjector{ .fail_at = point };
        lsm.set_fault_injector(&injector);
        if (point == .flush_after_sstable_publish) {
            try std.testing.expectError(error.InjectedLSMFault, lsm.flush_frozen_write());
        } else {
            try lsm.flush_frozen_write();
            try std.testing.expectError(error.InjectedLSMFault, lsm.flush_frozen_publish());
        }
        try std.testing.expect(injector.triggered);
    }

    {
        var recovered = try LSMEngine.init(allocator, wal_path, io);
        defer recovered.deinit();
        try std.testing.expectEqual(expected_quarantine, recovered.startup_quarantine_count());
        try std.testing.expect(!try recovered.wal.flushing_exists());
        try std.testing.expectEqual(@as(limits.CollectionCount, 7), try scan_count(&recovered, "bgf:"));
        for (1..8) |sequence| {
            var key_buffer: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "bgf:k{d}", .{sequence});
            try expect_sequence(&recovered, key, sequence);
        }
        try recovered.put("bgf:after-restart", try test_event_value(100, "svc", "evt", "{}", 0));
        try recovered.sync();
        try recovered.flush();
        try expect_sequence(&recovered, "bgf:after-restart", 100);
    }
}

test "background flush crash boundaries recover complete data via the flushing segment" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const points = [_]FaultPoint{
        .flush_after_sstable_publish,
        .flush_after_manifest_publish,
        .flush_after_cache_publish,
        .flush_after_wal_retire,
    };
    const quarantine_counts = [_]u16{ 1, 0, 0, 0 };
    for (points, quarantine_counts, 0..) |point, quarantine, case_index| {
        try run_background_flush_fault_recovery_case(allocator, io, point, quarantine, case_index);
    }
}

fn run_bg_compaction_fault_recovery_case(
    allocator: std.mem.Allocator,
    io: Io,
    point: FaultPoint,
    expected_quarantine: u16,
    case_index: usize,
) !void {
    var base_buffer: [96]u8 = undefined;
    const base = try std.fmt.bufPrint(
        &base_buffer,
        "data/test_lsm_bg_compact_fault_{d}",
        .{case_index},
    );
    var wal_buffer: [104]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&wal_buffer, "{s}.wal", .{base});
    cleanup_test_lsm(allocator, io, base);
    defer cleanup_test_lsm(allocator, io, base);

    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        for (0..3) |round| try put_compaction_round(&lsm, round);
        var injector = FaultInjector{ .fail_at = point };
        lsm.set_fault_injector(&injector);
        const plan = lsm.compaction_prepare(0) orelse return error.ExpectedCompactionPlan;
        if (point == .compaction_after_sstable_publish) {
            try std.testing.expectError(error.InjectedLSMFault, lsm.compaction_write(plan));
        } else {
            const result = (try lsm.compaction_write(plan)) orelse
                return error.ExpectedCompactionResult;
            try std.testing.expectError(
                error.InjectedLSMFault,
                lsm.compaction_publish(plan, result),
            );
        }
        try std.testing.expect(injector.triggered);
    }

    {
        var recovered = try LSMEngine.init(allocator, wal_path, io);
        defer recovered.deinit();
        try std.testing.expectEqual(expected_quarantine, recovered.startup_quarantine_count());
        try std.testing.expectEqual(
            @as(limits.CollectionCount, 3),
            try scan_count(&recovered, "compact-fault:"),
        );
        try expect_sequence(&recovered, "compact-fault:k2", 23);
        try recovered.put("compact-fault:after-restart", try test_event_value(100, "svc", "evt", "{}", 0));
        try recovered.sync();
        try recovered.flush();
        try expect_sequence(&recovered, "compact-fault:after-restart", 100);
    }
}

test "background compaction crash boundaries preserve data and quarantine orphans" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const points = [_]FaultPoint{
        .compaction_after_sstable_publish,
        .compaction_after_manifest_publish,
        .compaction_after_cache_publish,
        .compaction_after_directory_sync,
    };
    const quarantine_counts = [_]u16{ 1, 3, 3, 0 };
    for (points, quarantine_counts, 0..) |point, quarantine, case_index| {
        try run_bg_compaction_fault_recovery_case(allocator, io, point, quarantine, case_index);
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

    for (0..4) |round| {
        for (1..6) |i| {
            const key = try std.fmt.allocPrint(allocator, "c:k{}", .{i});
            defer allocator.free(key);
            const val = try test_event_value(round * 10 + i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
        try lsm.flush();
    }

    try std.testing.expectEqual(@as(usize, 1), lsm.sstable_count());

    try expect_sequence(&lsm, "c:k1", 31);
    try std.testing.expectEqual(@as(limits.CollectionCount, 5), try scan_count(&lsm, "c:"));
}

test "read operations allocate nothing after initialization" {
    const base = "data/test_lsm_no_post_init_alloc";
    const wal_path = "data/test_lsm_no_post_init_alloc.wal";
    const io = Io.Threaded.global_single_threaded.io();
    cleanup_test_lsm(std.testing.allocator, io, base);
    defer cleanup_test_lsm(std.testing.allocator, io, base);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var lsm = try LSMEngine.init(allocator, wal_path, io);
    defer lsm.deinit();

    for (0..4) |round| {
        for (0..8) |entry_index| {
            var key_buffer: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "fixed:{d}:{d}", .{
                round,
                entry_index,
            });
            const value = EventValue{
                .sequence = @intCast(round * 8 + entry_index + 1),
                .event_type = "fixed.event",
                .payload = "{}",
                .timestamp = 1,
                .origin_service = "fixed-service",
            };
            try lsm.put(key, value);
        }
        try lsm.sync();
        try lsm.flush();
    }

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;

    var result_buffer: [limits.event_serialized_size_max]u8 = undefined;
    const found = try lsm.get_bounded("fixed:3:7", &result_buffer);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u64, 32), found.?.sequence);

    const ScanCount = struct {
        count: limits.CollectionCount = 0,

        fn visit(
            self: *@This(),
            key: []const u8,
            _: EventValue,
        ) !event.VisitControl {
            assert(std.mem.startsWith(u8, key, "fixed:"));
            self.count += 1;
            return .continue_scan;
        }
    };
    var scan_result = ScanCount{};
    _ = try lsm.visit_prefix("fixed:", &scan_result, ScanCount.visit);
    try std.testing.expectEqual(@as(limits.CollectionCount, 32), scan_result.count);

    try std.testing.expectEqual(@as(usize, 1), lsm.sstable_count());
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(failing.fail_index, failing.alloc_index);
    try std.testing.expectEqual(failing.resize_fail_index, failing.resize_index);
}

test "tombstones hide keys across memtable flush and replay" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_tombstones.wal";
    const stem = "test_tombstones";
    inline for (.{ wal_path, "data/" ++ stem ++ ".manifest" }) |p| {
        Io.Dir.cwd().deleteFile(io, p) catch {};
    }
    Io.Dir.cwd().deleteTree(io, "data/" ++ stem ++ ".quarantine") catch {};
    for (0..4) |level| {
        for (1..64) |id| {
            var path_buffer: [64]u8 = undefined;
            const path = std.fmt.bufPrint(
                &path_buffer,
                "data/" ++ stem ++ "_L{d}_{d:0>8}.sst",
                .{ level, id },
            ) catch continue;
            Io.Dir.cwd().deleteFile(io, path) catch {};
        }
    }
    defer {
        inline for (.{ wal_path, "data/" ++ stem ++ ".manifest" }) |p| {
            Io.Dir.cwd().deleteFile(io, p) catch {};
        }
        Io.Dir.cwd().deleteTree(io, "data/" ++ stem ++ ".quarantine") catch {};
        for (0..4) |level| {
            for (1..64) |id| {
                var path_buffer: [64]u8 = undefined;
                const path = std.fmt.bufPrint(
                    &path_buffer,
                    "data/" ++ stem ++ "_L{d}_{d:0>8}.sst",
                    .{ level, id },
                ) catch continue;
                Io.Dir.cwd().deleteFile(io, path) catch {};
            }
        }
    }
    var scratch: [limits.event_serialized_size_max]u8 = undefined;

    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();

        const survivor = try EventValue.init(.{
            .sequence = 1,
            .origin_service = "svc",
            .event_type = "e",
            .payload = "keep",
            .timestamp = 1,
        });
        const doomed = try EventValue.init(.{
            .sequence = 2,
            .origin_service = "svc",
            .event_type = "e",
            .payload = "drop",
            .timestamp = 2,
        });
        try lsm.put("evt:svc:e:0000000000000001", survivor);
        try lsm.put("evt:svc:e:0000000000000002", doomed);

        try lsm.delete("evt:svc:e:0000000000000002", 3);
        try std.testing.expect(!try lsm.contains("evt:svc:e:0000000000000002"));
        try std.testing.expectEqual(@as(?EventValue, null), try lsm.get_bounded(
            "evt:svc:e:0000000000000002",
            &scratch,
        ));
        try std.testing.expect(try lsm.contains("evt:svc:e:0000000000000001"));

        try lsm.flush();
        try std.testing.expect(!try lsm.contains("evt:svc:e:0000000000000002"));
        try std.testing.expectEqual(@as(?EventValue, null), try lsm.get_bounded(
            "evt:svc:e:0000000000000002",
            &scratch,
        ));

        const revived = try EventValue.init(.{
            .sequence = 4,
            .origin_service = "svc",
            .event_type = "e",
            .payload = "back",
            .timestamp = 4,
        });
        try lsm.put("evt:svc:e:0000000000000002", revived);
        try std.testing.expect(try lsm.contains("evt:svc:e:0000000000000002"));
        const got = (try lsm.get_bounded("evt:svc:e:0000000000000002", &scratch)).?;
        try std.testing.expectEqualStrings("back", got.payload);
        try lsm.sync();
    }

    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        try std.testing.expect(try lsm.contains("evt:svc:e:0000000000000002"));
        try lsm.delete("evt:svc:e:0000000000000002", 5);
        try lsm.flush();
    }
    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        var scratch2: [limits.event_serialized_size_max]u8 = undefined;
        try std.testing.expect(!try lsm.contains("evt:svc:e:0000000000000002"));
        try std.testing.expectEqual(@as(?EventValue, null), try lsm.get_bounded(
            "evt:svc:e:0000000000000002",
            &scratch2,
        ));
        const kept = (try lsm.get_bounded("evt:svc:e:0000000000000001", &scratch2)).?;
        try std.testing.expectEqualStrings("keep", kept.payload);

        const Counter = struct {
            count: usize = 0,
            fn visit(ctx: *(@This()), _: []const u8, _: EventValue) !event.VisitControl {
                ctx.count += 1;
                return .continue_scan;
            }
        };
        var counter: Counter = .{};
        _ = try lsm.visit_prefix("evt:svc:", &counter, Counter.visit);
        try std.testing.expectEqual(@as(usize, 1), counter.count);
    }
}

test "levels above zero are compacted upward, so the probe set stays bounded" {
    const base = "data/test_lsm_level_compaction";
    const wal_path = "data/test_lsm_level_compaction.wal";
    const io = Io.Threaded.global_single_threaded.io();
    cleanup_test_lsm(std.testing.allocator, io, base);
    defer cleanup_test_lsm(std.testing.allocator, io, base);

    var lsm = try LSMEngine.init(std.testing.allocator, wal_path, io);
    defer lsm.deinit();

    const flushes = compaction_trigger_l0 * compaction_trigger_level * 2;
    var seq: u64 = 0;
    for (0..flushes) |flush_index| {
        for (0..8) |i| {
            seq += 1;
            var key_buffer: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "k:{d}:{d}", .{ flush_index, i });
            try lsm.put(key, .{
                .sequence = seq,
                .event_type = "e",
                .payload = "{}",
                .timestamp = 1,
                .origin_service = "svc",
            });
        }
        try lsm.flush();
    }

    try std.testing.expect(lsm.manifest.count_at_level(0) < compaction_trigger_l0);
    try std.testing.expect(lsm.manifest.count_at_level(1) < compaction_trigger_level);
    try std.testing.expect(lsm.manifest.count_at_level(2) > 0);

    var buffer: [limits.event_serialized_size_max]u8 = undefined;
    for (0..flushes) |flush_index| {
        for (0..8) |i| {
            var key_buffer: [64]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buffer, "k:{d}:{d}", .{ flush_index, i });
            const found = try lsm.get_bounded(key, &buffer);
            try std.testing.expect(found != null);
        }
    }
}

test "visit_prefix_from seeks every source and never visits keys before the start" {
    const base = "data/test_lsm_seek";
    const wal_path = "data/test_lsm_seek.wal";
    const io = Io.Threaded.global_single_threaded.io();
    cleanup_test_lsm(std.testing.allocator, io, base);
    defer cleanup_test_lsm(std.testing.allocator, io, base);

    var lsm = try LSMEngine.init(std.testing.allocator, wal_path, io);
    defer lsm.deinit();

    const payload: [96]u8 = @splat('p');
    const sequence_last: u64 = 600;
    var sequence: u64 = 1;
    while (sequence <= sequence_last) : (sequence += 1) {
        var key_buffer: [64]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "evt:svc:e:{x:0>16}", .{sequence});
        const value: EventValue = .{
            .sequence = sequence,
            .event_type = "e",
            .payload = &payload,
            .timestamp = 1,
            .origin_service = "svc",
        };
        try lsm.put(key, value);
        var other_buffer: [64]u8 = undefined;
        try lsm.put(try std.fmt.bufPrint(&other_buffer, "evt:svc:f:{x:0>16}", .{sequence}), value);
        try lsm.put(try std.fmt.bufPrint(&other_buffer, "aaa:{x:0>16}", .{sequence}), value);
        if (sequence == 200 or sequence == 400) try lsm.flush();
    }

    const Recorder = struct {
        start_key: []const u8,
        count: u64 = 0,
        fn visit(self: *@This(), key: []const u8, _: EventValue) !event.VisitControl {
            try std.testing.expect(std.mem.startsWith(u8, key, "evt:svc:e:"));
            try std.testing.expect(std.mem.order(u8, key, self.start_key) != .lt);
            self.count += 1;
            return .continue_scan;
        }
    };
    const starts = [_]u64{ 0, 1, 150, 200, 201, 333, 401, 555, 600, 601 };
    for (starts) |start| {
        var start_buffer: [64]u8 = undefined;
        const start_key = try std.fmt.bufPrint(&start_buffer, "evt:svc:e:{x:0>16}", .{start});
        var recorder: Recorder = .{ .start_key = start_key };
        _ = try lsm.visit_prefix_from("evt:svc:e:", start_key, &recorder, Recorder.visit);
        const first = @max(start, 1);
        const expected = if (first > sequence_last) 0 else sequence_last - first + 1;
        try std.testing.expectEqual(expected, recorder.count);
    }
}
