const std = @import("std");
const assert = std.debug.assert;
const event = @import("event.zig");
const limits = @import("limits.zig");
const bloom = @import("bloom.zig");
const EventValue = event.EventValue;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;

pub const magic_number: u32 = 0xC0BEEF55;
pub const format_version: u16 = 2;
pub const checksum_size: u32 = 16;
pub const footer_size: u32 = 116;
pub const block_size_target: u32 = 4096;
const writer_path_capacity: u32 = limits.wal_path_length_max + 68;

pub const FaultPoint = enum {
    after_index_write,
    after_footer_write,
    after_file_sync,
    after_rename,
    after_directory_sync,
};

pub const FaultInjector = struct {
    fail_after_entry: ?u64 = null,
    fail_at: ?FaultPoint = null,
    entries_written: u64 = 0,
    triggered: bool = false,

    fn after_entry(self: *FaultInjector) !void {
        self.entries_written += 1;
        if (self.fail_after_entry == self.entries_written) return self.fail();
    }

    fn check(self: *FaultInjector, point: FaultPoint) !void {
        if (self.fail_at != point) return;
        return self.fail();
    }

    fn fail(self: *FaultInjector) error{InjectedSSTableFault} {
        self.triggered = true;
        return error.InjectedSSTableFault;
    }
};

comptime {
    // Footer layout: [8B index_offset][4B index_size][4B num_blocks]
    // [8B num_entries][8B min_seq][8B max_seq][4B bloom_length][4B magic]
    // [2B version][2B flags][16B data checksum][16B index checksum]
    // [16B bloom checksum][16B footer checksum]. The bloom filter section sits
    // between the index and the footer: [data][index][bloom][footer].
    assert(footer_size == 8 + 4 + 4 + 8 + 8 + 8 + 4 + 4 + 2 + 2 + 4 * checksum_size);
    assert(block_size_target > 0);
}

pub const Footer = struct {
    index_offset: u64,
    index_size: u32,
    num_blocks: u32,
    num_entries: u64,
    min_seq: u64,
    max_seq: u64,
    bloom_length: u32,
    data_checksum: [checksum_size]u8,
    index_checksum: [checksum_size]u8,
    bloom_checksum: [checksum_size]u8,
};

pub const SSTInfo = struct {
    id: u64,
    level: u32,
    min_key: []const u8,
    max_key: []const u8,
    num_entries: u64,
    min_seq: u64,
    max_seq: u64,
    file_size: u64,
    storage_slot: limits.SSTableIndex = std.math.maxInt(limits.SSTableIndex),
};

pub const RawEntry = struct {
    key: []const u8,
    value: []const u8,
};

pub const ScanEntry = struct {
    key: []const u8,
    value_offset: u64,
    value_length: limits.ByteCount,
};

/// Allocation-free SSTable construction. All transient storage belongs to the
/// caller and may be reused after `finish` returns.
pub const StreamingWriter = struct {
    file: IoFile,
    parent_file: IoFile,
    final_path: [writer_path_capacity]u8,
    final_path_length: limits.ByteCount,
    temporary_path: [writer_path_capacity]u8,
    temporary_path_length: limits.ByteCount,
    file_open: bool,
    temporary_exists: bool,
    io: Io,
    data_scratch: []u8,
    index_scratch: []u8,
    data_size: u64,
    index_size: limits.ByteCount,
    block_start: u64,
    block_size: limits.ByteCount,
    block_count: limits.CollectionCount,
    entry_count: u64,
    min_sequence: u64,
    max_sequence: u64,
    first_key: [limits.key_length_max]u8,
    first_key_length: limits.ByteCount,
    min_key: [limits.key_length_max]u8,
    min_key_length: limits.ByteCount,
    max_key: [limits.key_length_max]u8,
    max_key_length: limits.ByteCount,
    finished: bool,
    data_hash: std.crypto.hash.Blake3,
    fault_injector: ?*FaultInjector,
    bloom_enabled: bool,
    bloom_params: bloom.Params,
    bloom_bits: []u8,
    bloom_serialize_scratch: []u8,

    pub fn init(
        path: []const u8,
        io: Io,
        data_scratch: []u8,
        index_scratch: []u8,
    ) !StreamingWriter {
        if (data_scratch.len < limits.sstable_block_size_max)
            return error.DataScratchTooSmall;
        const parent_path = std.fs.path.dirname(path) orelse ".";
        const parent_handle = try std.posix.openat(
            std.posix.AT.FDCWD,
            parent_path,
            .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true },
            0,
        );
        const parent_file: IoFile = .{
            .handle = parent_handle,
            .flags = .{ .nonblocking = false },
        };
        errdefer parent_file.close(io);
        if (path.len + ".tmp".len > writer_path_capacity) return error.PathTooLong;
        var final_path: [writer_path_capacity]u8 = undefined;
        @memcpy(final_path[0..path.len], path);
        var temporary_path: [writer_path_capacity]u8 = undefined;
        const temporary = try std.fmt.bufPrint(&temporary_path, "{s}.tmp", .{path});
        const file = try IoDir.cwd().createFile(io, temporary, .{
            .exclusive = true,
            .read = true,
        });
        errdefer file.close(io);
        return .{
            .file = file,
            .parent_file = parent_file,
            .final_path = final_path,
            .final_path_length = @intCast(path.len),
            .temporary_path = temporary_path,
            .temporary_path_length = @intCast(temporary.len),
            .file_open = true,
            .temporary_exists = true,
            .io = io,
            .data_scratch = data_scratch,
            .index_scratch = index_scratch,
            .data_size = 0,
            .index_size = 0,
            .block_start = 0,
            .block_size = 0,
            .block_count = 0,
            .entry_count = 0,
            .min_sequence = std.math.maxInt(u64),
            .max_sequence = 0,
            .first_key = undefined,
            .first_key_length = 0,
            .min_key = undefined,
            .min_key_length = 0,
            .max_key = undefined,
            .max_key_length = 0,
            .finished = false,
            .data_hash = std.crypto.hash.Blake3.init(.{}),
            .fault_injector = null,
            .bloom_enabled = false,
            .bloom_params = .{ .bits = 0, .hashes = 0 },
            .bloom_bits = &.{},
            .bloom_serialize_scratch = &.{},
        };
    }

    /// Opt in to writing a Bloom filter over the table's keys. `bit_scratch`
    /// must hold `params.byte_len()` bytes and `serialize_scratch` at least
    /// `bloom.serialized_size_max(params)`; both belong to the caller and stay
    /// borrowed until `finish`. Call before the first `append`.
    pub fn enable_bloom(
        self: *StreamingWriter,
        params: bloom.Params,
        bit_scratch: []u8,
        serialize_scratch: []u8,
    ) void {
        assert(self.entry_count == 0);
        assert(bit_scratch.len >= params.byte_len());
        assert(serialize_scratch.len >= bloom.serialized_size_max(params));
        @memset(bit_scratch[0..params.byte_len()], 0);
        self.bloom_enabled = true;
        self.bloom_params = params;
        self.bloom_bits = bit_scratch[0..params.byte_len()];
        self.bloom_serialize_scratch = serialize_scratch;
    }

    pub fn abort(self: *StreamingWriter) void {
        if (!self.finished) {
            if (self.file_open) {
                self.file.close(self.io);
                self.file_open = false;
            }
            if (self.temporary_exists) {
                IoDir.cwd().deleteFile(
                    self.io,
                    self.temporary_path[0..self.temporary_path_length],
                ) catch |err| std.log.err("failed to remove SSTable temporary file: {}", .{err});
                self.temporary_exists = false;
            }
            self.parent_file.close(self.io);
            self.finished = true;
        }
    }

    pub fn append(self: *StreamingWriter, entry: RawEntry) !void {
        assert(!self.finished);
        if (entry.key.len == 0) return error.EmptyKey;
        if (entry.key.len > limits.key_length_max) return error.KeyTooLong;
        const entry_size = try limits.byte_count(4 + entry.key.len + 4 + entry.value.len);
        if (entry_size > self.data_scratch.len) return error.EntryTooLarge;

        if (self.entry_count > 0) {
            const previous_key = self.max_key[0..self.max_key_length];
            if (std.mem.order(u8, previous_key, entry.key) == .gt) return error.KeysOutOfOrder;
        }
        if (self.block_size > 0 and self.block_size + entry_size > block_size_target) {
            try self.close_block();
        }
        if (self.block_size == 0) {
            self.block_start = self.data_size;
            self.first_key_length = try limits.byte_count(entry.key.len);
            @memcpy(self.first_key[0..entry.key.len], entry.key);
        }

        var position: limits.ByteCount = 0;
        std.mem.writeInt(
            u32,
            self.data_scratch[position..][0..4],
            @intCast(entry.key.len),
            .little,
        );
        position += 4;
        @memcpy(self.data_scratch[position..][0..entry.key.len], entry.key);
        position += try limits.byte_count(entry.key.len);
        std.mem.writeInt(
            u32,
            self.data_scratch[position..][0..4],
            @intCast(entry.value.len),
            .little,
        );
        position += 4;
        @memcpy(self.data_scratch[position..][0..entry.value.len], entry.value);
        position += try limits.byte_count(entry.value.len);
        assert(position == entry_size);
        try self.file.writePositionalAll(self.io, self.data_scratch[0..position], self.data_size);
        if (self.fault_injector) |injector| try injector.after_entry();
        self.data_hash.update(self.data_scratch[0..position]);

        const sequence = parse_leading_u64(entry.value);
        if (sequence < self.min_sequence) self.min_sequence = sequence;
        if (sequence > self.max_sequence) self.max_sequence = sequence;
        if (self.entry_count == 0) {
            self.min_key_length = try limits.byte_count(entry.key.len);
            @memcpy(self.min_key[0..entry.key.len], entry.key);
        }
        self.max_key_length = try limits.byte_count(entry.key.len);
        @memcpy(self.max_key[0..entry.key.len], entry.key);
        if (self.bloom_enabled) bloom.insert(self.bloom_bits, self.bloom_params, entry.key);
        self.entry_count += 1;
        self.data_size += entry_size;
        self.block_size += entry_size;
    }

    fn close_block(self: *StreamingWriter) !void {
        if (self.block_size == 0) return;
        const record_size = 4 + self.first_key_length + 8 + 4;
        if (record_size > self.index_scratch.len - self.index_size)
            return error.IndexScratchTooSmall;
        var position = self.index_size;
        std.mem.writeInt(u32, self.index_scratch[position..][0..4], self.first_key_length, .little);
        position += 4;
        @memcpy(
            self.index_scratch[position..][0..self.first_key_length],
            self.first_key[0..self.first_key_length],
        );
        position += self.first_key_length;
        std.mem.writeInt(u64, self.index_scratch[position..][0..8], self.block_start, .little);
        position += 8;
        std.mem.writeInt(u32, self.index_scratch[position..][0..4], self.block_size, .little);
        position += 4;
        self.index_size = position;
        self.block_count += 1;
        self.block_size = 0;
    }

    pub fn finish(self: *StreamingWriter) !Footer {
        assert(!self.finished);
        try self.close_block();
        try self.file.writePositionalAll(
            self.io,
            self.index_scratch[0..self.index_size],
            self.data_size,
        );
        try self.inject_fault(.after_index_write);

        // The Bloom filter section sits between the index and the footer.
        var bloom_length: u32 = 0;
        var bloom_checksum: [checksum_size]u8 = @splat(0);
        if (self.bloom_enabled) {
            const encoded = try bloom.serialize(
                self.bloom_bits,
                self.bloom_params,
                self.bloom_serialize_scratch,
            );
            try self.file.writePositionalAll(
                self.io,
                encoded,
                self.data_size + self.index_size,
            );
            bloom_length = @intCast(encoded.len);
            bloom_checksum = checksum_bytes(encoded);
        }

        const footer = Footer{
            .index_offset = self.data_size,
            .index_size = self.index_size,
            .num_blocks = self.block_count,
            .num_entries = self.entry_count,
            .min_seq = if (self.entry_count == 0) 0 else self.min_sequence,
            .max_seq = self.max_sequence,
            .bloom_length = bloom_length,
            .data_checksum = checksum_final(self.data_hash),
            .index_checksum = checksum_bytes(self.index_scratch[0..self.index_size]),
            .bloom_checksum = bloom_checksum,
        };
        try write_footer(self.file, self.io, footer);
        try self.inject_fault(.after_footer_write);
        try self.file.sync(self.io);
        try self.inject_fault(.after_file_sync);
        self.file.close(self.io);
        self.file_open = false;
        try IoDir.cwd().renamePreserve(
            self.temporary_path[0..self.temporary_path_length],
            IoDir.cwd(),
            self.final_path[0..self.final_path_length],
            self.io,
        );
        self.temporary_exists = false;
        try self.inject_fault(.after_rename);
        self.parent_file.sync(self.io) catch return error.DirectorySyncFailed;
        try self.inject_fault(.after_directory_sync);
        self.parent_file.close(self.io);
        self.finished = true;
        return footer;
    }

    pub fn set_fault_injector(self: *StreamingWriter, injector: ?*FaultInjector) void {
        self.fault_injector = injector;
    }

    fn inject_fault(self: *StreamingWriter, point: FaultPoint) !void {
        if (self.fault_injector) |injector| try injector.check(point);
    }

    pub fn minimum_key(self: *const StreamingWriter) []const u8 {
        return self.min_key[0..self.min_key_length];
    }

    pub fn maximum_key(self: *const StreamingWriter) []const u8 {
        return self.max_key[0..self.max_key_length];
    }
};

fn write_footer(file: IoFile, io: Io, footer: Footer) !void {
    var footer_buf: [footer_size]u8 = undefined;
    std.mem.writeInt(u64, footer_buf[0..8], footer.index_offset, .little);
    std.mem.writeInt(u32, footer_buf[8..12], footer.index_size, .little);
    std.mem.writeInt(u32, footer_buf[12..16], footer.num_blocks, .little);
    std.mem.writeInt(u64, footer_buf[16..24], footer.num_entries, .little);
    std.mem.writeInt(u64, footer_buf[24..32], footer.min_seq, .little);
    std.mem.writeInt(u64, footer_buf[32..40], footer.max_seq, .little);
    std.mem.writeInt(u32, footer_buf[40..44], footer.bloom_length, .little);
    std.mem.writeInt(u32, footer_buf[44..48], magic_number, .little);
    std.mem.writeInt(u16, footer_buf[48..50], format_version, .little);
    std.mem.writeInt(u16, footer_buf[50..52], 0, .little);
    @memcpy(footer_buf[52..68], &footer.data_checksum);
    @memcpy(footer_buf[68..84], &footer.index_checksum);
    @memcpy(footer_buf[84..100], &footer.bloom_checksum);
    const checksum = checksum_bytes(footer_buf[0..100]);
    @memcpy(footer_buf[100..116], &checksum);
    try file.writePositionalAll(
        io,
        &footer_buf,
        footer.index_offset + footer.index_size + footer.bloom_length,
    );
}

pub const LoadedBloom = struct {
    params: bloom.Params,
    /// Decompressed filter bits, owned by the caller's allocator.
    bits: []u8,
};

pub const SSTableReader = struct {
    file: IoFile,
    io: Io,
    footer: Footer,

    pub fn open_uncached(path: []const u8, io: Io) !SSTableReader {
        const file = IoDir.cwd().openFile(
            io,
            path,
            .{ .mode = .read_only },
        ) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => return err,
        };
        errdefer file.close(io);

        const file_size = (try file.stat(io)).size;
        if (file_size < footer_size) return error.InvalidFormat;
        var footer_buffer: [footer_size]u8 = undefined;
        try read_exact(file, io, &footer_buffer, file_size - footer_size);
        const footer_checksum = checksum_bytes(footer_buffer[0..100]);
        if (!std.crypto.timing_safe.eql(
            [checksum_size]u8,
            footer_checksum,
            footer_buffer[100..116].*,
        )) return error.SSTableChecksumMismatch;
        if (std.mem.readInt(u32, footer_buffer[44..48], .little) != magic_number)
            return error.InvalidFormat;
        if (std.mem.readInt(u16, footer_buffer[48..50], .little) != format_version)
            return error.UnsupportedSSTableVersion;
        if (std.mem.readInt(u16, footer_buffer[50..52], .little) != 0)
            return error.InvalidFormat;
        const footer = Footer{
            .index_offset = std.mem.readInt(u64, footer_buffer[0..8], .little),
            .index_size = std.mem.readInt(u32, footer_buffer[8..12], .little),
            .num_blocks = std.mem.readInt(u32, footer_buffer[12..16], .little),
            .num_entries = std.mem.readInt(u64, footer_buffer[16..24], .little),
            .min_seq = std.mem.readInt(u64, footer_buffer[24..32], .little),
            .max_seq = std.mem.readInt(u64, footer_buffer[32..40], .little),
            .bloom_length = std.mem.readInt(u32, footer_buffer[40..44], .little),
            .data_checksum = footer_buffer[52..68].*,
            .index_checksum = footer_buffer[68..84].*,
            .bloom_checksum = footer_buffer[84..100].*,
        };
        if (footer.index_offset + footer.index_size + footer.bloom_length + footer_size !=
            file_size)
            return error.InvalidFormat;
        try verify_region(file, io, 0, footer.index_offset, footer.data_checksum);
        try verify_region(
            file,
            io,
            footer.index_offset,
            footer.index_size,
            footer.index_checksum,
        );
        if (footer.bloom_length > 0) try verify_region(
            file,
            io,
            footer.index_offset + footer.index_size,
            footer.bloom_length,
            footer.bloom_checksum,
        );

        return .{
            .file = file,
            .io = io,
            .footer = footer,
        };
    }

    /// Read and decompress the table's Bloom filter into a freshly allocated
    /// buffer the caller owns and frees. Returns null when the table has no
    /// filter. The section bytes were already checksum-verified at open.
    pub fn load_bloom(
        self: *SSTableReader,
        allocator: std.mem.Allocator,
    ) !?LoadedBloom {
        if (self.footer.bloom_length == 0) return null;
        const section = try allocator.alloc(u8, self.footer.bloom_length);
        defer allocator.free(section);
        try read_exact(
            self.file,
            self.io,
            section,
            self.footer.index_offset + self.footer.index_size,
        );
        const bits = try bloom.bits_of(section);
        const filter = try allocator.alloc(u8, @intCast(bits / 8));
        errdefer allocator.free(filter);
        const decoded = try bloom.deserialize(section, filter);
        return .{ .params = decoded.params, .bits = filter };
    }

    pub fn deinit(self: *SSTableReader) void {
        self.file.close(self.io);
    }

    const DiskIndexEntry = struct {
        first_key: []const u8,
        block_offset: u64,
        block_size: limits.ByteCount,
    };

    fn read_disk_index_entry(
        self: *SSTableReader,
        cursor: *limits.ByteCount,
        key_buffer: []u8,
    ) !?DiskIndexEntry {
        if (cursor.* == self.footer.index_size) return null;
        if (cursor.* > self.footer.index_size) return error.InvalidFormat;
        if (self.footer.index_size - cursor.* < 16) return error.InvalidFormat;

        const position = self.footer.index_offset + cursor.*;
        var key_length_buffer: [4]u8 = undefined;
        try read_exact(self.file, self.io, &key_length_buffer, position);
        const key_length = std.mem.readInt(u32, &key_length_buffer, .little);
        if (key_length > limits.key_length_max) return error.InvalidFormat;
        if (key_length > key_buffer.len) return error.KeyBufferTooSmall;
        if (cursor.* + 4 + key_length + 12 > self.footer.index_size)
            return error.InvalidFormat;

        const first_key = key_buffer[0..key_length];
        try read_exact(self.file, self.io, first_key, position + 4);
        var metadata: [12]u8 = undefined;
        try read_exact(self.file, self.io, &metadata, position + 4 + key_length);
        cursor.* += 4 + key_length + 12;
        return .{
            .first_key = first_key,
            .block_offset = std.mem.readInt(u64, metadata[0..8], .little),
            .block_size = std.mem.readInt(u32, metadata[8..12], .little),
        };
    }

    fn find_disk_block(
        self: *SSTableReader,
        key: []const u8,
        key_buffer: []u8,
    ) !?DiskIndexEntry {
        var cursor: limits.ByteCount = 0;
        var found: ?DiskIndexEntry = null;
        while (try self.read_disk_index_entry(&cursor, key_buffer)) |entry| {
            if (std.mem.order(u8, entry.first_key, key) == .gt) break;
            found = .{
                .first_key = "",
                .block_offset = entry.block_offset,
                .block_size = entry.block_size,
            };
        }
        return found;
    }

    fn read_disk_block_into(
        self: *SSTableReader,
        entry: DiskIndexEntry,
        block_buffer: []u8,
    ) ![]u8 {
        if (entry.block_size > block_buffer.len) return error.BlockBufferTooSmall;
        const data = block_buffer[0..entry.block_size];
        try read_exact(self.file, self.io, data, entry.block_offset);
        return data;
    }

    pub fn get_into(
        self: *SSTableReader,
        key: []const u8,
        block_buffer: []u8,
    ) !?EventValue {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        const index_entry = try self.find_disk_block(key, &key_buffer) orelse return null;
        const data = try self.read_disk_block_into(index_entry, block_buffer);
        var offset: limits.ByteCount = 0;
        while (offset < data.len) {
            const parsed = parse_raw_entry(data, offset) orelse
                return error.InvalidFormat;
            offset = try limits.byte_count(parsed.next_offset);
            switch (std.mem.order(u8, parsed.key, key)) {
                .lt => {},
                .eq => return try EventValue.deserialize_borrowed(parsed.value),
                .gt => return null,
            }
        }
        return null;
    }

    pub fn contains_into(
        self: *SSTableReader,
        key: []const u8,
        block_buffer: []u8,
    ) !bool {
        return (try self.get_into(key, block_buffer)) != null;
    }

    /// Advances through the checksummed data region without retaining a block
    /// per table. This lets the LSM perform a bounded k-way scan across every
    /// live table with only one key slot per source.
    pub fn scan_next_key(
        self: *SSTableReader,
        cursor: *u64,
        key_buffer: []u8,
    ) !?ScanEntry {
        if (cursor.* == self.footer.index_offset) return null;
        if (cursor.* > self.footer.index_offset or
            self.footer.index_offset - cursor.* < 8) return error.InvalidFormat;
        var key_length_buffer: [4]u8 = undefined;
        try read_exact(self.file, self.io, &key_length_buffer, cursor.*);
        const key_length = std.mem.readInt(u32, &key_length_buffer, .little);
        if (key_length == 0 or key_length > limits.key_length_max)
            return error.InvalidFormat;
        if (key_length > key_buffer.len) return error.KeyBufferTooSmall;
        const value_length_offset = cursor.* + 4 + key_length;
        if (value_length_offset + 4 > self.footer.index_offset)
            return error.InvalidFormat;
        const key = key_buffer[0..key_length];
        try read_exact(self.file, self.io, key, cursor.* + 4);
        var value_length_buffer: [4]u8 = undefined;
        try read_exact(self.file, self.io, &value_length_buffer, value_length_offset);
        const value_length = std.mem.readInt(u32, &value_length_buffer, .little);
        if (value_length > limits.event_serialized_size_max) return error.InvalidFormat;
        const value_offset = value_length_offset + 4;
        if (value_offset + value_length > self.footer.index_offset)
            return error.InvalidFormat;
        cursor.* = value_offset + value_length;
        return .{
            .key = key,
            .value_offset = value_offset,
            .value_length = value_length,
        };
    }

    pub fn scan_value_into(
        self: *SSTableReader,
        entry: ScanEntry,
        value_buffer: []u8,
    ) !EventValue {
        if (entry.value_length > value_buffer.len) return error.ResultBufferTooSmall;
        if (entry.value_offset + entry.value_length > self.footer.index_offset)
            return error.InvalidFormat;
        const serialized = value_buffer[0..entry.value_length];
        try read_exact(self.file, self.io, serialized, entry.value_offset);
        return try EventValue.deserialize_borrowed(serialized);
    }

    pub fn visit_prefix(
        self: *SSTableReader,
        prefix: []const u8,
        block_buffer: []u8,
        context: anytype,
        comptime visit: anytype,
    ) !event.VisitControl {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        var index_cursor: limits.ByteCount = 0;
        while (try self.read_disk_index_entry(&index_cursor, &key_buffer)) |index_entry| {
            const data = try self.read_disk_block_into(index_entry, block_buffer);
            var offset: limits.ByteCount = 0;
            while (offset < data.len) {
                const parsed = parse_raw_entry(data, offset) orelse
                    return error.InvalidFormat;
                offset = try limits.byte_count(parsed.next_offset);
                if (std.mem.startsWith(u8, parsed.key, prefix)) {
                    const value = try EventValue.deserialize_borrowed(parsed.value);
                    if (try visit(context, parsed.key, value) == .stop) return .stop;
                } else if (std.mem.order(u8, parsed.key, prefix) == .gt) {
                    return .continue_scan;
                }
            }
        }
        return .continue_scan;
    }
};

fn checksum_bytes(bytes: []const u8) [checksum_size]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(bytes);
    return checksum_final(hash);
}

fn checksum_final(hash_start: std.crypto.hash.Blake3) [checksum_size]u8 {
    var hash = hash_start;
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hash.final(&digest);
    return digest[0..checksum_size].*;
}

fn verify_region(
    file: IoFile,
    io: Io,
    offset: u64,
    length: u64,
    expected: [checksum_size]u8,
) !void {
    var hash = std.crypto.hash.Blake3.init(.{});
    var scratch: [64 * 1024]u8 = undefined;
    var consumed: u64 = 0;
    while (consumed < length) {
        const count: usize = @intCast(@min(@as(u64, scratch.len), length - consumed));
        try read_exact(file, io, scratch[0..count], offset + consumed);
        hash.update(scratch[0..count]);
        consumed += count;
    }
    const actual = checksum_final(hash);
    if (!std.crypto.timing_safe.eql([checksum_size]u8, actual, expected))
        return error.SSTableChecksumMismatch;
}

pub const RawIteratorBuffered = struct {
    reader: *SSTableReader,
    index_cursor: limits.ByteCount,
    block_data: []u8,
    block_offset: limits.ByteCount,
    block_loaded: bool,
    block_buffer: []u8,
    index_key_buffer: [limits.key_length_max]u8,

    pub fn init(reader: *SSTableReader, block_buffer: []u8) RawIteratorBuffered {
        return .{
            .reader = reader,
            .index_cursor = 0,
            .block_data = block_buffer[0..0],
            .block_offset = 0,
            .block_loaded = false,
            .block_buffer = block_buffer,
            .index_key_buffer = undefined,
        };
    }

    pub fn next(self: *RawIteratorBuffered) !?RawEntry {
        while (true) {
            if (self.block_loaded and self.block_offset < self.block_data.len) {
                const parsed = parse_raw_entry(
                    self.block_data,
                    self.block_offset,
                ) orelse return error.InvalidFormat;
                self.block_offset = try limits.byte_count(parsed.next_offset);
                return .{ .key = parsed.key, .value = parsed.value };
            }

            const index_entry = try self.reader.read_disk_index_entry(
                &self.index_cursor,
                &self.index_key_buffer,
            ) orelse return null;
            self.block_data = try self.reader.read_disk_block_into(
                index_entry,
                self.block_buffer,
            );
            self.block_offset = 0;
            self.block_loaded = true;
        }
    }
};

const ParsedEntry = struct {
    key: []const u8,
    value: []const u8,
    next_offset: limits.ByteCount,
};

fn parse_raw_entry(data: []const u8, off: limits.ByteCount) ?ParsedEntry {
    assert(off <= data.len);
    if (off + 4 > data.len) return null;
    const key_len = std.mem.readInt(u32, data[off..][0..4], .little);
    const key_start = off + 4;
    if (key_start + key_len > data.len) return null;
    const key = data[key_start..][0..key_len];
    const vlen_start = key_start + key_len;
    if (vlen_start + 4 > data.len) return null;
    const val_len = std.mem.readInt(u32, data[vlen_start..][0..4], .little);
    const val_start = vlen_start + 4;
    if (val_start + val_len > data.len) return null;
    const value = data[val_start..][0..val_len];
    const next_offset: limits.ByteCount = @intCast(val_start + val_len);
    assert(next_offset > off);
    assert(next_offset <= data.len);
    return ParsedEntry{
        .key = key,
        .value = value,
        .next_offset = next_offset,
    };
}

fn parse_leading_u64(data: []const u8) u64 {
    var end: limits.ByteCount = 0;
    while (end < data.len and data[end] != '|') : (end += 1) {}
    return std.fmt.parseInt(u64, data[0..end], 10) catch 0;
}

fn read_exact(file: IoFile, io: Io, buf: []u8, offset: u64) !void {
    var total: limits.ByteCount = 0;
    while (total < buf.len) {
        var iovecs = [1][]u8{buf[total..]};
        const n = file.readPositional(io, &iovecs, offset + total) catch return error.InputOutput;
        if (n == 0) return error.InvalidFormat;
        total += try limits.byte_count(n);
    }
}

fn write_test_table(
    allocator: std.mem.Allocator,
    path: []const u8,
    io: Io,
    entries: []const RawEntry,
) !Footer {
    const data = try allocator.alloc(u8, limits.sstable_block_size_max);
    defer allocator.free(data);
    const index = try allocator.alloc(u8, limits.sstable_index_size_max);
    defer allocator.free(index);
    var writer = try StreamingWriter.init(path, io, data, index);
    errdefer writer.abort();
    for (entries) |entry| try writer.append(entry);
    return try writer.finish();
}

const TestVisitContext = struct {
    keys: [4][]const u8 = undefined,
    count: limits.CollectionCount = 0,

    fn visit(self: *TestVisitContext, key: []const u8, _: EventValue) !event.VisitControl {
        self.keys[self.count] = key;
        self.count += 1;
        return .continue_scan;
    }
};

test "bloom-enabled table round-trips its filter and detects corruption" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_sst_bloom.sst";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    const data = try allocator.alloc(u8, limits.sstable_block_size_max);
    defer allocator.free(data);
    const index = try allocator.alloc(u8, limits.sstable_index_size_max);
    defer allocator.free(index);

    const keys = [_][]const u8{ "apple", "banana", "cherry", "date", "elderberry" };
    const params = bloom.chooseParams(keys.len, 0.01);
    const bloom_bits = try allocator.alloc(u8, params.byte_len());
    defer allocator.free(bloom_bits);
    const bloom_serialize = try allocator.alloc(u8, bloom.serialized_size_max(params));
    defer allocator.free(bloom_serialize);

    {
        var writer = try StreamingWriter.init(path, io, data, index);
        errdefer writer.abort();
        writer.enable_bloom(params, bloom_bits, bloom_serialize);
        for (keys, 0..) |key, i| {
            var value_buffer: [64]u8 = undefined;
            const value = try std.fmt.bufPrint(&value_buffer, "{d}|100|2|s|e||{{}}", .{i + 1});
            try writer.append(.{ .key = key, .value = value });
        }
        const footer = try writer.finish();
        try std.testing.expect(footer.bloom_length > 0);
    }

    {
        var reader = try SSTableReader.open_uncached(path, io);
        defer reader.deinit();
        const loaded = (try reader.load_bloom(allocator)).?;
        defer allocator.free(loaded.bits);
        // Every present key must survive the filter round-trip.
        for (keys) |key| try std.testing.expect(bloom.mayContain(loaded.bits, loaded.params, key));
    }

    // Corrupt a byte inside the Bloom section: open must fail the checksum.
    {
        const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
        defer file.close(io);
        const file_size = (try file.stat(io)).size;
        var footer_buffer: [footer_size]u8 = undefined;
        try read_exact(file, io, &footer_buffer, file_size - footer_size);
        const index_offset = std.mem.readInt(u64, footer_buffer[0..8], .little);
        const index_size = std.mem.readInt(u32, footer_buffer[8..12], .little);
        const bloom_offset = index_offset + index_size;
        var byte: [1]u8 = undefined;
        try read_exact(file, io, &byte, bloom_offset);
        byte[0] +%= 1;
        try file.writePositionalAll(io, &byte, bloom_offset);
        try std.testing.expectError(
            error.SSTableChecksumMismatch,
            SSTableReader.open_uncached(path, io),
        );
    }
}

test "sstable write and read single entry" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_sst_single.sst";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    const entries = [_]RawEntry{.{
        .key = "hello",
        .value = "1|100|2|svc|evt||{}",
    }};

    const footer = try write_test_table(allocator, path, io, &entries);
    try std.testing.expectEqual(@as(u64, 1), footer.num_entries);
    try std.testing.expectEqual(@as(u64, 1), footer.min_seq);

    var reader = try SSTableReader.open_uncached(path, io);
    defer reader.deinit();

    var block_buffer: [limits.sstable_block_size_max]u8 = undefined;
    const val = (try reader.get_into("hello", &block_buffer)).?;
    try std.testing.expectEqual(@as(u64, 1), val.sequence);
    try std.testing.expectEqualStrings("evt", val.event_type);

    try std.testing.expect(try reader.get_into("missing", &block_buffer) == null);
}

test "sstable write and read multiple entries" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_sst_multi.sst";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    const entries = [_]RawEntry{
        .{ .key = "a:1", .value = "1|100|2|svc|evt||{}" },
        .{ .key = "a:2", .value = "2|200|2|svc|evt||{}" },
        .{ .key = "b:1", .value = "3|300|2|svc|evt||{}" },
        .{ .key = "c:1", .value = "4|400|2|svc|evt||{}" },
    };

    const footer = try write_test_table(allocator, path, io, &entries);
    try std.testing.expectEqual(@as(u64, 4), footer.num_entries);
    try std.testing.expectEqual(@as(u64, 1), footer.min_seq);
    try std.testing.expectEqual(@as(u64, 4), footer.max_seq);

    var reader = try SSTableReader.open_uncached(path, io);
    defer reader.deinit();
    var block_buffer: [limits.sstable_block_size_max]u8 = undefined;

    {
        const val = (try reader.get_into("b:1", &block_buffer)).?;
        try std.testing.expectEqual(@as(u64, 3), val.sequence);
    }
    {
        const val = (try reader.get_into("c:1", &block_buffer)).?;
        try std.testing.expectEqual(@as(u64, 4), val.sequence);
    }
    try std.testing.expect(try reader.get_into("z:1", &block_buffer) == null);

    var context = TestVisitContext{};
    _ = try reader.visit_prefix("a:", &block_buffer, &context, TestVisitContext.visit);
    try std.testing.expectEqual(@as(limits.CollectionCount, 2), context.count);
    try std.testing.expectEqualStrings("a:1", context.keys[0]);
    try std.testing.expectEqualStrings("a:2", context.keys[1]);
}

test "sstable raw iterator" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_sst_iter.sst";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    const entries = [_]RawEntry{
        .{ .key = "k1", .value = "1|0|2|s|e|{}" },
        .{ .key = "k2", .value = "2|0|2|s|e|{}" },
        .{ .key = "k3", .value = "3|0|2|s|e|{}" },
    };

    _ = try write_test_table(allocator, path, io, &entries);

    var reader = try SSTableReader.open_uncached(path, io);
    defer reader.deinit();

    var block_buffer: [limits.sstable_block_size_max]u8 = undefined;
    var iter = RawIteratorBuffered.init(&reader, &block_buffer);

    const e1 = (try iter.next()).?;
    try std.testing.expectEqualStrings("k1", e1.key);
    const e2 = (try iter.next()).?;
    try std.testing.expectEqualStrings("k2", e2.key);
    const e3 = (try iter.next()).?;
    try std.testing.expectEqualStrings("k3", e3.key);
    try std.testing.expect(try iter.next() == null);
}

test "sstable contains" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_sst_contains.sst";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    const entries = [_]RawEntry{
        .{ .key = "alpha", .value = "1|0|2|s|e||{}" },
        .{ .key = "beta", .value = "2|0|2|s|e||{}" },
    };

    _ = try write_test_table(allocator, path, io, &entries);

    var reader = try SSTableReader.open_uncached(path, io);
    defer reader.deinit();
    var block_buffer: [limits.sstable_block_size_max]u8 = undefined;

    try std.testing.expect(try reader.contains_into("alpha", &block_buffer));
    try std.testing.expect(try reader.contains_into("beta", &block_buffer));
    try std.testing.expect(!try reader.contains_into("gamma", &block_buffer));
    try std.testing.expect(!try reader.contains_into("a", &block_buffer));
}

test "sstable startup verification rejects data index and footer mutations" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_sst_checksums.sst";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};
    const entries = [_]RawEntry{
        .{ .key = "alpha", .value = "1|0|2|s|e|one" },
        .{ .key = "beta", .value = "2|0|2|s|e|two" },
    };
    const footer = try write_test_table(allocator, path, io, &entries);
    const file_size = footer.index_offset + footer.index_size + footer_size;
    const positions = [_]u64{ 0, footer.index_offset, file_size - 1 };
    for (positions) |position| {
        {
            const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
            defer file.close(io);
            var byte: [1]u8 = undefined;
            try read_exact(file, io, &byte, position);
            byte[0] ^= 1;
            try file.writePositionalAll(io, &byte, position);
            try file.sync(io);
        }
        try std.testing.expectError(
            error.SSTableChecksumMismatch,
            SSTableReader.open_uncached(path, io),
        );
        {
            const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
            defer file.close(io);
            var byte: [1]u8 = undefined;
            try read_exact(file, io, &byte, position);
            byte[0] ^= 1;
            try file.writePositionalAll(io, &byte, position);
            try file.sync(io);
        }
    }
    var reader = try SSTableReader.open_uncached(path, io);
    reader.deinit();
}

test "aborted sstable construction never publishes its final name" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const path = "data/test_sst_abort.sst";
    const temporary_path = "data/test_sst_abort.sst.tmp";
    IoDir.cwd().deleteFile(io, path) catch {};
    IoDir.cwd().deleteFile(io, temporary_path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, temporary_path) catch {};
    const data = try allocator.alloc(u8, limits.sstable_block_size_max);
    defer allocator.free(data);
    const index = try allocator.alloc(u8, limits.sstable_index_size_max);
    defer allocator.free(index);
    var writer = try StreamingWriter.init(path, io, data, index);
    try writer.append(.{ .key = "key", .value = "1|0|2|s|e|value" });
    writer.abort();
    try std.testing.expectError(
        error.FileNotFound,
        IoDir.cwd().openFile(io, path, .{ .mode = .read_only }),
    );
    try std.testing.expectError(
        error.FileNotFound,
        IoDir.cwd().openFile(io, temporary_path, .{ .mode = .read_only }),
    );
}

test "sstable publication faults leave no partial final table" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const data = try allocator.alloc(u8, limits.sstable_block_size_max);
    defer allocator.free(data);
    const index = try allocator.alloc(u8, limits.sstable_index_size_max);
    defer allocator.free(index);
    const points = [_]?FaultPoint{
        null,
        .after_index_write,
        .after_footer_write,
        .after_file_sync,
        .after_rename,
        .after_directory_sync,
    };
    for (points, 0..) |point, point_index| {
        var path_buffer: [128]u8 = undefined;
        const path = try std.fmt.bufPrint(
            &path_buffer,
            "data/test_sst_fault_{d}.sst",
            .{point_index},
        );
        var temporary_buffer: [132]u8 = undefined;
        const temporary = try std.fmt.bufPrint(&temporary_buffer, "{s}.tmp", .{path});
        IoDir.cwd().deleteFile(io, path) catch {};
        IoDir.cwd().deleteFile(io, temporary) catch {};
        defer IoDir.cwd().deleteFile(io, path) catch {};
        defer IoDir.cwd().deleteFile(io, temporary) catch {};
        var writer = try StreamingWriter.init(path, io, data, index);
        var injector: FaultInjector = if (point) |selected|
            .{ .fail_at = selected }
        else
            .{ .fail_after_entry = 1 };
        writer.set_fault_injector(&injector);
        if (point == null) {
            try std.testing.expectError(
                error.InjectedSSTableFault,
                writer.append(.{ .key = "key", .value = "1|0|2|s|e|value" }),
            );
        } else {
            try writer.append(.{ .key = "key", .value = "1|0|2|s|e|value" });
            try std.testing.expectError(error.InjectedSSTableFault, writer.finish());
        }
        try std.testing.expect(injector.triggered);
        writer.abort();
        const published = point == .after_rename or point == .after_directory_sync;
        if (published) {
            var reader = try SSTableReader.open_uncached(path, io);
            reader.deinit();
        } else {
            try std.testing.expectError(
                error.FileNotFound,
                IoDir.cwd().openFile(io, path, .{ .mode = .read_only }),
            );
        }
        try std.testing.expectError(
            error.FileNotFound,
            IoDir.cwd().openFile(io, temporary, .{ .mode = .read_only }),
        );
    }
}
