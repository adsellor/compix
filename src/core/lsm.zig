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
const compaction_trigger_l0: u32 = 4;
const orphan_candidates_max: u16 = 4096;

comptime {
    assert(flush_bytes_max > 0);
    assert(flush_count_max > 0);
    assert(compaction_trigger_l0 >= 2);
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

/// An open SSTable kept resident so reads never reopen or reparse table files.
/// The list mirrors manifest.live exactly, sorted newest-first (level ascending,
/// then id descending) so the first key hit is the freshest value.
const CachedTable = struct {
    id: u64,
    level: u32,
    reader: SSTableReader,
};

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
    manifest: *Manifest,
    tables: fixed.List(CachedTable),
    table_storage: []CachedTable,
    scan_sources: []ScanSource,
    base_path: []const u8,
    wal_path: []const u8,
    next_sst_id: u64,
    read_scratch: []u8,
    event_scratch: []u8,
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

        const memory = try StartupMemory.init(allocator);
        errdefer memory.deinit(allocator);

        _ = try wal_ptr.replay(mt_ptr);

        var self = LSMEngine{
            .allocator = allocator,
            .io = io,
            .wal = wal_ptr,
            .active_memtable = mt_ptr,
            .manifest = manifest_ptr,
            .tables = try fixed.List(CachedTable).init(memory.table_storage),
            .table_storage = memory.table_storage,
            .scan_sources = memory.scan_sources,
            .base_path = base,
            .wal_path = owned_wal_path,
            .next_sst_id = next_id,
            .read_scratch = memory.read_scratch,
            .event_scratch = memory.event_scratch,
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
        self.allocator.free(self.read_scratch);
        self.allocator.free(self.event_scratch);
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
            t.reader.deinit();
        }
        self.tables.reset();
    }

    /// Opens the table file and inserts it into the cache, keeping the
    /// newest-first order (level ascending, id descending within a level).
    fn open_table(self: *LSMEngine, level: u32, id: u64) !void {
        assert(id > 0);
        if (self.tables.count >= limits.sstables_max) return error.TooManySSTables;
        var path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
        const path = try self.sst_path_into(&path_buffer, level, id);
        var reader = try SSTableReader.open_uncached(path, self.io);
        errdefer reader.deinit();

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
        }) catch unreachable;
    }

    fn remove_table(self: *LSMEngine, id: u64) void {
        for (self.tables.items_const(), 0..) |t, i| {
            if (t.id == id) {
                var removed = self.tables.ordered_remove(@intCast(i));
                removed.reader.deinit();
                return;
            }
        }
        unreachable;
    }

    pub fn put(self: *LSMEngine, key: []const u8, value: EventValue) !void {
        assert(key.len > 0);

        // Flush before insertion so no fallible operation follows the pool copy.
        if (self.active_memtable.size_bytes >= flush_bytes_max) try self.flush();
        if (self.active_memtable.count() >= flush_count_max) try self.flush();
        if (!self.active_memtable.can_insert(key, &value)) try self.flush();
        if (!self.active_memtable.can_insert(key, &value)) {
            return error.EventExceedsMemtableCapacity;
        }

        try self.wal.append_event(key, value);
        try self.active_memtable.insert(key, value);
        assert(self.active_memtable.count() > 0);
        assert(self.active_memtable.count() <= flush_count_max);
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
            return try copy_event_bounded(&arena, value.*);
        }
        for (self.tables.items()) |*table| {
            if (try table.reader.get_into(key, self.read_scratch)) |value| {
                return try copy_event_bounded(&arena, value);
            }
        }
        return null;
    }

    fn copy_event_bounded(arena: *fixed.ByteArena, value: EventValue) !EventValue {
        const bytes_required = try limits.byte_count(
            value.origin_service.len + value.event_type.len + value.payload.len,
        );
        if (bytes_required > arena.remaining()) return error.ResultBufferTooSmall;
        const origin = arena.write(value.origin_service) catch unreachable;
        const event_type = arena.write(value.event_type) catch unreachable;
        const payload = arena.write(value.payload) catch unreachable;
        return .{
            .sequence = value.sequence,
            .event_type = event_type,
            .payload = payload,
            .timestamp = value.timestamp,
            .origin_service = origin,
        };
    }

    pub fn contains(self: *LSMEngine, key: []const u8) !bool {
        assert(key.len > 0);

        if (self.active_memtable.contains(key)) return true;

        for (self.tables.items()) |*t| {
            if (try t.reader.contains_into(key, self.read_scratch)) return true;
        }

        return false;
    }

    pub fn visit_prefix(
        self: *LSMEngine,
        prefix: []const u8,
        context: anytype,
        comptime visit: anytype,
    ) !event.VisitControl {
        try self.initialize_scan_sources();
        var memtable_iterator = self.active_memtable.iterator();
        var memtable_entry = memtable_iterator.next();
        var key_storage: [limits.key_length_max]u8 = undefined;
        while (true) {
            var minimum: ?[]const u8 = if (memtable_entry) |entry| entry.key else null;
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
                const value = if (memtable_entry) |entry|
                    if (std.mem.eql(u8, entry.key, key)) entry.value else try self.scan_value(key)
                else
                    try self.scan_value(key);
                if (try visit(context, key, value) == .stop) return .stop;
            }
            if (memtable_entry) |entry| {
                if (std.mem.eql(u8, entry.key, key)) memtable_entry = memtable_iterator.next();
            }
            for (self.scan_sources[0..self.tables.count], 0..) |*source, index| {
                if (source.current and std.mem.eql(u8, source.current_key(), key))
                    try self.advance_scan_source(index);
            }
        }
    }

    fn initialize_scan_sources(self: *LSMEngine) !void {
        assert(self.tables.count <= self.scan_sources.len);
        for (self.scan_sources[0..self.tables.count], 0..) |*source, index| {
            source.* = .{
                .cursor = 0,
                .key = undefined,
                .key_length = 0,
                .value_offset = 0,
                .value_length = 0,
                .current = false,
            };
            try self.advance_scan_source(index);
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

        const frozen = self.active_memtable;

        const sst_id = self.next_sst_id;
        self.next_sst_id += 1;

        var path_buffer: [limits.wal_path_length_max + 64]u8 = undefined;
        const path = try self.sst_path_into(&path_buffer, 0, sst_id);
        var writer = try sstable.StreamingWriter.init(
            path,
            self.io,
            self.sst_data_scratch,
            self.sst_index_scratch,
        );
        errdefer writer.abort();
        var iter = frozen.iterator();
        while (iter.next()) |entry| {
            const serialized = try entry.value.serialize_into(self.event_scratch);
            try writer.append(.{ .key = entry.key, .value = serialized });
        }
        const footer = try writer.finish();
        try self.inject_fault(.flush_after_sstable_publish);
        assert(footer.num_entries == frozen.count());
        const file_size = footer.index_offset + footer.index_size + sstable.footer_size;
        try self.manifest.add_sstable(.{
            .id = sst_id,
            .level = 0,
            .min_key = writer.minimum_key(),
            .max_key = writer.maximum_key(),
            .num_entries = footer.num_entries,
            .min_seq = footer.min_seq,
            .max_seq = footer.max_seq,
            .file_size = file_size,
        });
        try self.inject_fault(.flush_after_manifest_publish);
        try self.open_table(0, sst_id);
        try self.inject_fault(.flush_after_cache_publish);
        try self.wal.truncate();
        try self.inject_fault(.flush_after_wal_retire);

        frozen.reset();

        assert(self.active_memtable.count() == 0);
        assert(self.tables.count == tables_before + 1);
        assert(self.tables.count == self.manifest.live.count);

        if (self.manifest.count_at_level(0) >= compaction_trigger_l0) {
            self.compaction_active = true;
            self.compact_level(0) catch |err| {
                self.compaction_active = false;
                self.compaction_failures += 1;
                return err;
            };
            self.compaction_active = false;
        }
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
        while (try merge.next()) |entry| try writer.append(entry);
        const footer = try writer.finish();
        try self.inject_fault(.compaction_after_sstable_publish);
        if (footer.num_entries == 0) return;
        const file_size = footer.index_offset + footer.index_size + sstable.footer_size;

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

    IoDir.cwd().deleteFile(io, wal_path) catch {};
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
            const val = try test_event_value(round * 10 + i, "svc", "evt", "{}", 0);
            try lsm.put(key, val);
        }
        try lsm.flush();
    }

    try std.testing.expectEqual(@as(usize, 1), lsm.sstable_count());

    try expect_sequence(&lsm, "c:k1", 31);
    try std.testing.expectEqual(@as(limits.CollectionCount, 5), try scan_count(&lsm, "c:"));
}

test "storage operations allocate nothing after initialization" {
    const base = "data/test_lsm_no_post_init_alloc";
    const wal_path = "data/test_lsm_no_post_init_alloc.wal";
    const io = Io.Threaded.global_single_threaded.io();
    cleanup_test_lsm(std.testing.allocator, io, base);
    defer cleanup_test_lsm(std.testing.allocator, io, base);

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var lsm = try LSMEngine.init(allocator, wal_path, io);
    defer lsm.deinit();

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;

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
