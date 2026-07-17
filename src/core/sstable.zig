const std = @import("std");
const assert = std.debug.assert;
const ArrayList = std.ArrayList;
const event = @import("event.zig");
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;

pub const magic_number: u32 = 0xC0BEEF55;
pub const footer_size: u32 = 48;
pub const block_size_target: u32 = 4096;

comptime {
    // Footer layout: [8B index_offset][4B index_size][4B num_blocks]
    // [8B num_entries][8B min_seq][8B max_seq][4B magic][4B padding].
    assert(footer_size == 8 + 4 + 4 + 8 + 8 + 8 + 4 + 4);
    assert(block_size_target > 0);
}

pub const Footer = struct {
    index_offset: u64,
    index_size: u32,
    num_blocks: u32,
    num_entries: u64,
    min_seq: u64,
    max_seq: u64,
};

pub const IndexEntry = struct {
    first_key: []const u8,
    block_offset: u64,
    block_size: u32,
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

    pub fn deinit(self: *SSTInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.min_key);
        allocator.free(self.max_key);
    }
};

pub const RawEntry = struct {
    key: []const u8,
    value: []const u8,
};

pub fn write_sstable(
    allocator: std.mem.Allocator,
    path: []const u8,
    io: Io,
    entries: []const RawEntry,
) !Footer {
    const dir = IoDir.cwd();
    const file = try dir.createFile(io, path, .{ .truncate = true, .read = true });
    defer file.close(io);

    var index_entries = ArrayList(IndexEntry).empty;
    defer {
        for (index_entries.items) |ie| allocator.free(ie.first_key);
        index_entries.deinit(allocator);
    }

    const blocks = try write_data_blocks(allocator, file, io, entries, &index_entries);

    const index_offset = blocks.data_size;
    const index_size = try write_index(allocator, file, io, index_entries.items, index_offset);

    const footer = Footer{
        .index_offset = index_offset,
        .index_size = index_size,
        .num_blocks = @intCast(index_entries.items.len),
        .num_entries = @intCast(entries.len),
        .min_seq = blocks.min_seq,
        .max_seq = blocks.max_seq,
    };
    try write_footer(file, io, footer);
    return footer;
}

const DataBlocks = struct {
    data_size: u64,
    min_seq: u64,
    max_seq: u64,
};

/// Streams entries into fixed-target-size blocks, recording an index entry per
/// block. Entries must be sorted by key ascending, which is asserted.
fn write_data_blocks(
    allocator: std.mem.Allocator,
    file: IoFile,
    io: Io,
    entries: []const RawEntry,
    index_entries: *ArrayList(IndexEntry),
) !DataBlocks {
    var write_offset: u64 = 0;
    var block_start: u64 = 0;
    var block_size: u32 = 0;
    var first_key_in_block: ?[]const u8 = null;
    var min_seq: u64 = std.math.maxInt(u64);
    var max_seq: u64 = 0;

    var prev_key: ?[]const u8 = null;
    for (entries) |entry| {
        assert(entry.key.len > 0);
        if (prev_key) |prev| assert(std.mem.order(u8, prev, entry.key) != .gt);
        prev_key = entry.key;

        const entry_size: u32 = @intCast(4 + entry.key.len + 4 + entry.value.len);

        const seq = parse_leading_u64(entry.value);
        if (seq < min_seq) min_seq = seq;
        if (seq > max_seq) max_seq = seq;

        if (block_size > 0 and block_size + entry_size > block_size_target) {
            try index_entries.append(allocator, .{
                .first_key = try allocator.dupe(u8, first_key_in_block.?),
                .block_offset = block_start,
                .block_size = block_size,
            });
            block_start = write_offset;
            block_size = 0;
            first_key_in_block = null;
        }

        if (first_key_in_block == null) first_key_in_block = entry.key;

        try write_entry(allocator, file, io, entry, write_offset);
        write_offset += entry_size;
        block_size += entry_size;
    }

    if (block_size > 0) {
        try index_entries.append(allocator, .{
            .first_key = try allocator.dupe(u8, first_key_in_block.?),
            .block_offset = block_start,
            .block_size = block_size,
        });
    }

    if (entries.len == 0) {
        min_seq = 0;
        max_seq = 0;
    }
    assert(min_seq <= max_seq);

    return DataBlocks{
        .data_size = write_offset,
        .min_seq = min_seq,
        .max_seq = max_seq,
    };
}

/// Entry layout: [4B key_len][key][4B value_len][value].
fn write_entry(
    allocator: std.mem.Allocator,
    file: IoFile,
    io: Io,
    entry: RawEntry,
    write_offset: u64,
) !void {
    const entry_size = 4 + entry.key.len + 4 + entry.value.len;
    const buf = try allocator.alloc(u8, entry_size);
    defer allocator.free(buf);

    var pos: usize = 0;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(entry.key.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..entry.key.len], entry.key);
    pos += entry.key.len;
    std.mem.writeInt(u32, buf[pos..][0..4], @intCast(entry.value.len), .little);
    pos += 4;
    @memcpy(buf[pos..][0..entry.value.len], entry.value);
    assert(pos + entry.value.len == entry_size);

    try file.writePositionalAll(io, buf, write_offset);
}

fn write_index(
    allocator: std.mem.Allocator,
    file: IoFile,
    io: Io,
    index_entries: []const IndexEntry,
    index_offset: u64,
) !u32 {
    var write_offset = index_offset;
    for (index_entries) |ie| {
        const ie_size = 4 + ie.first_key.len + 8 + 4;
        const ie_buf = try allocator.alloc(u8, ie_size);
        defer allocator.free(ie_buf);
        var ipos: usize = 0;
        std.mem.writeInt(u32, ie_buf[ipos..][0..4], @intCast(ie.first_key.len), .little);
        ipos += 4;
        @memcpy(ie_buf[ipos..][0..ie.first_key.len], ie.first_key);
        ipos += ie.first_key.len;
        std.mem.writeInt(u64, ie_buf[ipos..][0..8], ie.block_offset, .little);
        ipos += 8;
        std.mem.writeInt(u32, ie_buf[ipos..][0..4], ie.block_size, .little);
        ipos += 4;
        assert(ipos == ie_size);
        try file.writePositionalAll(io, ie_buf, write_offset);
        write_offset += ie_size;
    }
    assert(write_offset >= index_offset);
    return @intCast(write_offset - index_offset);
}

fn write_footer(file: IoFile, io: Io, footer: Footer) !void {
    var footer_buf: [footer_size]u8 = undefined;
    std.mem.writeInt(u64, footer_buf[0..8], footer.index_offset, .little);
    std.mem.writeInt(u32, footer_buf[8..12], footer.index_size, .little);
    std.mem.writeInt(u32, footer_buf[12..16], footer.num_blocks, .little);
    std.mem.writeInt(u64, footer_buf[16..24], footer.num_entries, .little);
    std.mem.writeInt(u64, footer_buf[24..32], footer.min_seq, .little);
    std.mem.writeInt(u64, footer_buf[32..40], footer.max_seq, .little);
    std.mem.writeInt(u32, footer_buf[40..44], magic_number, .little);
    @memset(footer_buf[44..48], 0);
    try file.writePositionalAll(io, &footer_buf, footer.index_offset + footer.index_size);
}

pub const SSTableReader = struct {
    file: IoFile,
    io: Io,
    allocator: std.mem.Allocator,
    footer: Footer,
    index: ArrayList(IndexEntry),
    path: []const u8,

    pub fn open(allocator: std.mem.Allocator, path: []const u8, io: Io) !SSTableReader {
        const dir = IoDir.cwd();
        const file = dir.openFile(io, path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => return err,
        };
        errdefer file.close(io);

        const file_size = (try file.stat(io)).size;
        if (file_size < footer_size) return error.InvalidFormat;

        var footer_buf: [footer_size]u8 = undefined;
        try read_exact(file, io, &footer_buf, file_size - footer_size);

        const magic = std.mem.readInt(u32, footer_buf[40..44], .little);
        if (magic != magic_number) return error.InvalidFormat;

        const footer = Footer{
            .index_offset = std.mem.readInt(u64, footer_buf[0..8], .little),
            .index_size = std.mem.readInt(u32, footer_buf[8..12], .little),
            .num_blocks = std.mem.readInt(u32, footer_buf[12..16], .little),
            .num_entries = std.mem.readInt(u64, footer_buf[16..24], .little),
            .min_seq = std.mem.readInt(u64, footer_buf[24..32], .little),
            .max_seq = std.mem.readInt(u64, footer_buf[32..40], .little),
        };

        var index = try parse_index(allocator, file, io, footer);
        errdefer {
            for (index.items) |ie| allocator.free(ie.first_key);
            index.deinit(allocator);
        }

        if (index.items.len != footer.num_blocks) return error.InvalidFormat;

        return SSTableReader{
            .file = file,
            .io = io,
            .allocator = allocator,
            .footer = footer,
            .index = index,
            .path = try allocator.dupe(u8, path),
        };
    }

    fn parse_index(
        allocator: std.mem.Allocator,
        file: IoFile,
        io: Io,
        footer: Footer,
    ) !ArrayList(IndexEntry) {
        var index = ArrayList(IndexEntry).empty;
        errdefer {
            for (index.items) |ie| allocator.free(ie.first_key);
            index.deinit(allocator);
        }
        if (footer.index_size == 0) return index;

        const index_buf = try allocator.alloc(u8, footer.index_size);
        defer allocator.free(index_buf);
        try read_exact(file, io, index_buf, footer.index_offset);

        var off: usize = 0;
        while (off < footer.index_size) {
            if (off + 4 > footer.index_size) return error.InvalidFormat;
            const key_len = std.mem.readInt(u32, index_buf[off..][0..4], .little);
            off += 4;
            if (off + key_len > footer.index_size) return error.InvalidFormat;
            const first_key = try allocator.dupe(u8, index_buf[off..][0..key_len]);
            errdefer allocator.free(first_key);
            off += key_len;
            if (off + 12 > footer.index_size) return error.InvalidFormat;
            const block_offset = std.mem.readInt(u64, index_buf[off..][0..8], .little);
            off += 8;
            const block_size = std.mem.readInt(u32, index_buf[off..][0..4], .little);
            off += 4;
            try index.append(allocator, .{
                .first_key = first_key,
                .block_offset = block_offset,
                .block_size = block_size,
            });
        }
        assert(off == footer.index_size);
        return index;
    }

    pub fn deinit(self: *SSTableReader) void {
        for (self.index.items) |ie| self.allocator.free(ie.first_key);
        self.index.deinit(self.allocator);
        self.file.close(self.io);
        self.allocator.free(self.path);
    }

    pub fn get(self: *SSTableReader, allocator: std.mem.Allocator, key: []const u8) !?EventValue {
        const block_idx = self.find_block(key) orelse return null;
        return try self.scan_block_for_key(allocator, block_idx, key);
    }

    pub fn scan_prefix(
        self: *SSTableReader,
        prefix: []const u8,
        results: *ArrayList(KeyValuePair),
    ) !void {
        const start_block = self.find_block_for_prefix(prefix);
        var block_idx = start_block;
        while (block_idx < self.index.items.len) {
            try self.scan_block_for_prefix(self.allocator, block_idx, prefix, results);
            block_idx += 1;
            if (block_idx < self.index.items.len) {
                if (!std.mem.startsWith(u8, self.index.items[block_idx].first_key, prefix)) break;
            }
        }
    }

    pub fn contains(self: *SSTableReader, key: []const u8) !bool {
        const block_idx = self.find_block(key) orelse return false;
        return self.scan_block_contains(block_idx, key);
    }

    pub fn raw_iterator(self: *SSTableReader) RawIterator {
        return RawIterator{
            .reader = self,
            .block_idx = 0,
            .block_data = null,
            .offset = 0,
        };
    }

    fn find_block(self: *const SSTableReader, key: []const u8) ?usize {
        if (self.index.items.len == 0) return null;
        var lo: usize = 0;
        var hi: usize = self.index.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, self.index.items[mid].first_key, key) != .gt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if (lo == 0) return null;
        return lo - 1;
    }

    fn find_block_for_prefix(self: *const SSTableReader, prefix: []const u8) usize {
        if (self.index.items.len == 0) return 0;
        var lo: usize = 0;
        var hi: usize = self.index.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (std.mem.order(u8, self.index.items[mid].first_key, prefix) == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if (lo > 0) lo -= 1;
        return lo;
    }

    fn read_block(self: *SSTableReader, block_idx: usize) ![]u8 {
        const ie = self.index.items[block_idx];
        const buf = try self.allocator.alloc(u8, ie.block_size);
        errdefer self.allocator.free(buf);
        try read_exact(self.file, self.io, buf, ie.block_offset);
        return buf;
    }

    fn scan_block_for_key(
        self: *SSTableReader,
        allocator: std.mem.Allocator,
        block_idx: usize,
        key: []const u8,
    ) !?EventValue {
        const data = try self.read_block(block_idx);
        defer self.allocator.free(data);

        var off: usize = 0;
        while (off < data.len) {
            const parsed = parse_raw_entry(data, off) orelse break;
            if (std.mem.eql(u8, parsed.key, key)) {
                return try EventValue.deserialize(allocator, parsed.value);
            }
            off = parsed.next_offset;
        }
        return null;
    }

    fn scan_block_contains(self: *SSTableReader, block_idx: usize, key: []const u8) !bool {
        const data = try self.read_block(block_idx);
        defer self.allocator.free(data);

        var off: usize = 0;
        while (off < data.len) {
            const parsed = parse_raw_entry(data, off) orelse break;
            if (std.mem.eql(u8, parsed.key, key)) return true;
            // Keys are sorted; stop early if we've passed the target
            if (std.mem.order(u8, parsed.key, key) == .gt) return false;
            off = parsed.next_offset;
        }
        return false;
    }

    fn scan_block_for_prefix(
        self: *SSTableReader,
        allocator: std.mem.Allocator,
        block_idx: usize,
        prefix: []const u8,
        results: *ArrayList(KeyValuePair),
    ) !void {
        const data = try self.read_block(block_idx);
        defer self.allocator.free(data);

        var off: usize = 0;
        while (off < data.len) {
            const parsed = parse_raw_entry(data, off) orelse break;
            if (std.mem.startsWith(u8, parsed.key, prefix)) {
                const deserialized = try EventValue.deserialize(allocator, parsed.value);
                errdefer {
                    var v = deserialized;
                    v.deinit(allocator);
                }
                try results.append(allocator, KeyValuePair{
                    .key = try allocator.dupe(u8, parsed.key),
                    .value = deserialized,
                });
            } else if (std.mem.order(u8, parsed.key, prefix) == .gt) {
                break;
            }
            off = parsed.next_offset;
        }
    }
};

pub const RawIterator = struct {
    reader: *SSTableReader,
    block_idx: usize,
    block_data: ?[]u8,
    offset: usize,

    pub fn next(self: *RawIterator) !?RawEntry {
        while (true) {
            if (self.block_data) |data| {
                if (self.offset < data.len) {
                    const parsed = parse_raw_entry(data, self.offset) orelse {
                        //NOTE: Bad block data, need to handle this differently
                        self.reader.allocator.free(data);
                        self.block_data = null;
                        self.block_idx += 1;
                        continue;
                    };
                    self.offset = parsed.next_offset;
                    return RawEntry{ .key = parsed.key, .value = parsed.value };
                }
                self.reader.allocator.free(data);
                self.block_data = null;
                self.block_idx += 1;
            }

            if (self.block_idx >= self.reader.index.items.len) return null;

            self.block_data = try self.reader.read_block(self.block_idx);
            self.offset = 0;
        }
    }

    pub fn deinit(self: *RawIterator) void {
        if (self.block_data) |data| {
            self.reader.allocator.free(data);
            self.block_data = null;
        }
    }
};

const ParsedEntry = struct {
    key: []const u8,
    value: []const u8,
    next_offset: usize,
};

fn parse_raw_entry(data: []const u8, off: usize) ?ParsedEntry {
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
    const next_offset = val_start + val_len;
    assert(next_offset > off);
    assert(next_offset <= data.len);
    return ParsedEntry{
        .key = key,
        .value = value,
        .next_offset = next_offset,
    };
}

fn parse_leading_u64(data: []const u8) u64 {
    var end: usize = 0;
    while (end < data.len and data[end] != '|') : (end += 1) {}
    return std.fmt.parseInt(u64, data[0..end], 10) catch 0;
}

fn read_exact(file: IoFile, io: Io, buf: []u8, offset: u64) !void {
    var total: usize = 0;
    while (total < buf.len) {
        var iovecs = [1][]u8{buf[total..]};
        const n = file.readPositional(io, &iovecs, offset + total) catch return error.InputOutput;
        if (n == 0) return error.InvalidFormat;
        total += n;
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
        .value = "1|100|2|svc|evt|{}",
    }};

    const footer = try write_sstable(allocator, path, io, &entries);
    try std.testing.expectEqual(@as(u64, 1), footer.num_entries);
    try std.testing.expectEqual(@as(u64, 1), footer.min_seq);

    var reader = try SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    var val = (try reader.get(allocator, "hello")).?;
    defer val.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), val.sequence);
    try std.testing.expectEqualStrings("evt", val.event_type);

    const miss = try reader.get(allocator, "missing");
    try std.testing.expect(miss == null);
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
        .{ .key = "a:1", .value = "1|100|2|svc|evt|{}" },
        .{ .key = "a:2", .value = "2|200|2|svc|evt|{}" },
        .{ .key = "b:1", .value = "3|300|2|svc|evt|{}" },
        .{ .key = "c:1", .value = "4|400|2|svc|evt|{}" },
    };

    const footer = try write_sstable(allocator, path, io, &entries);
    try std.testing.expectEqual(@as(u64, 4), footer.num_entries);
    try std.testing.expectEqual(@as(u64, 1), footer.min_seq);
    try std.testing.expectEqual(@as(u64, 4), footer.max_seq);

    var reader = try SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    {
        var val = (try reader.get(allocator, "b:1")).?;
        defer val.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 3), val.sequence);
    }
    {
        var val = (try reader.get(allocator, "c:1")).?;
        defer val.deinit(allocator);
        try std.testing.expectEqual(@as(u64, 4), val.sequence);
    }
    try std.testing.expect(try reader.get(allocator, "z:1") == null);

    var results = ArrayList(KeyValuePair).empty;
    try reader.scan_prefix("a:", &results);
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 2), results.items.len);
    try std.testing.expectEqualStrings("a:1", results.items[0].key);
    try std.testing.expectEqualStrings("a:2", results.items[1].key);
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

    _ = try write_sstable(allocator, path, io, &entries);

    var reader = try SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    var iter = reader.raw_iterator();
    defer iter.deinit();

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
        .{ .key = "alpha", .value = "1|0|2|s|e|{}" },
        .{ .key = "beta", .value = "2|0|2|s|e|{}" },
    };

    _ = try write_sstable(allocator, path, io, &entries);

    var reader = try SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    try std.testing.expect(try reader.contains("alpha"));
    try std.testing.expect(try reader.contains("beta"));
    try std.testing.expect(!try reader.contains("gamma"));
    try std.testing.expect(!try reader.contains("a"));
}
