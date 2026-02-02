const std = @import("std");
const ArrayList = std.ArrayList;
const event = @import("event.zig");
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;

pub const MAGIC: u32 = 0xC0BEEF55;
pub const FOOTER_SIZE: usize = 48;
pub const TARGET_BLOCK_SIZE: usize = 4096;

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

pub fn writeSSTable(
    allocator: std.mem.Allocator,
    path: []const u8,
    io: Io,
    entries: []const RawEntry,
) !Footer {
    const dir = IoDir.cwd();
    const file = try dir.createFile(io, path, .{ .truncate = true, .read = true });
    defer file.close(io);

    var index_entries = ArrayList(IndexEntry){};
    defer {
        for (index_entries.items) |ie| allocator.free(ie.first_key);
        index_entries.deinit(allocator);
    }

    var write_offset: u64 = 0;
    var block_start: u64 = 0;
    var block_size: u32 = 0;
    var first_key_in_block: ?[]const u8 = null;
    var min_seq: u64 = std.math.maxInt(u64);
    var max_seq: u64 = 0;

    for (entries) |entry| {
        const entry_size: u32 = @intCast(4 + entry.key.len + 4 + entry.value.len);

        const seq = parseLeadingU64(entry.value);
        if (seq < min_seq) min_seq = seq;
        if (seq > max_seq) max_seq = seq;

        if (block_size > 0 and block_size + entry_size > TARGET_BLOCK_SIZE) {
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

        // [4B key_len][key][4B value_len][value]
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

        try file.writePositionalAll(io, buf, write_offset);
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

    const index_offset = write_offset;
    for (index_entries.items) |ie| {
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
        try file.writePositionalAll(io, ie_buf, write_offset);
        write_offset += ie_size;
    }
    const index_size: u32 = @intCast(write_offset - index_offset);

    var footer_buf: [FOOTER_SIZE]u8 = undefined;
    std.mem.writeInt(u64, footer_buf[0..8], index_offset, .little);
    std.mem.writeInt(u32, footer_buf[8..12], index_size, .little);
    std.mem.writeInt(u32, footer_buf[12..16], @intCast(index_entries.items.len), .little);
    std.mem.writeInt(u64, footer_buf[16..24], @intCast(entries.len), .little);
    std.mem.writeInt(u64, footer_buf[24..32], min_seq, .little);
    std.mem.writeInt(u64, footer_buf[32..40], max_seq, .little);
    std.mem.writeInt(u32, footer_buf[40..44], MAGIC, .little);
    @memset(footer_buf[44..48], 0);

    try file.writePositionalAll(io, &footer_buf, write_offset);

    return Footer{
        .index_offset = index_offset,
        .index_size = index_size,
        .num_blocks = @intCast(index_entries.items.len),
        .num_entries = @intCast(entries.len),
        .min_seq = min_seq,
        .max_seq = max_seq,
    };
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
        if (file_size < FOOTER_SIZE) return error.InvalidFormat;

        var footer_buf: [FOOTER_SIZE]u8 = undefined;
        try readExact(file, io, &footer_buf, file_size - FOOTER_SIZE);

        const magic = std.mem.readInt(u32, footer_buf[40..44], .little);
        if (magic != MAGIC) return error.InvalidFormat;

        const footer = Footer{
            .index_offset = std.mem.readInt(u64, footer_buf[0..8], .little),
            .index_size = std.mem.readInt(u32, footer_buf[8..12], .little),
            .num_blocks = std.mem.readInt(u32, footer_buf[12..16], .little),
            .num_entries = std.mem.readInt(u64, footer_buf[16..24], .little),
            .min_seq = std.mem.readInt(u64, footer_buf[24..32], .little),
            .max_seq = std.mem.readInt(u64, footer_buf[32..40], .little),
        };

        var index = ArrayList(IndexEntry){};
        errdefer {
            for (index.items) |ie| allocator.free(ie.first_key);
            index.deinit(allocator);
        }

        if (footer.index_size > 0) {
            const index_buf = try allocator.alloc(u8, footer.index_size);
            defer allocator.free(index_buf);
            try readExact(file, io, index_buf, footer.index_offset);

            var off: usize = 0;
            while (off < footer.index_size) {
                if (off + 4 > footer.index_size) break;
                const key_len = std.mem.readInt(u32, index_buf[off..][0..4], .little);
                off += 4;
                if (off + key_len > footer.index_size) break;
                const first_key = try allocator.dupe(u8, index_buf[off..][0..key_len]);
                off += key_len;
                if (off + 12 > footer.index_size) {
                    allocator.free(first_key);
                    break;
                }
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
        }

        return SSTableReader{
            .file = file,
            .io = io,
            .allocator = allocator,
            .footer = footer,
            .index = index,
            .path = try allocator.dupe(u8, path),
        };
    }

    pub fn deinit(self: *SSTableReader) void {
        for (self.index.items) |ie| self.allocator.free(ie.first_key);
        self.index.deinit(self.allocator);
        self.file.close(self.io);
        self.allocator.free(self.path);
    }

    pub fn get(self: *SSTableReader, allocator: std.mem.Allocator, key: []const u8) !?EventValue {
        const block_idx = self.findBlock(key) orelse return null;
        return try self.scanBlockForKey(allocator, block_idx, key);
    }

    pub fn scan_prefix(self: *SSTableReader, prefix: []const u8, results: *ArrayList(KeyValuePair)) !void {
        const start_block = self.findBlockForPrefix(prefix);
        var block_idx = start_block;
        while (block_idx < self.index.items.len) {
            try self.scanBlockForPrefix(self.allocator, block_idx, prefix, results);
            block_idx += 1;
            if (block_idx < self.index.items.len) {
                if (!std.mem.startsWith(u8, self.index.items[block_idx].first_key, prefix)) break;
            }
        }
    }

    pub fn contains(self: *SSTableReader, key: []const u8) !bool {
        const block_idx = self.findBlock(key) orelse return false;
        return self.scanBlockContains(block_idx, key);
    }

    pub fn rawIterator(self: *SSTableReader) RawIterator {
        return RawIterator{
            .reader = self,
            .block_idx = 0,
            .block_data = null,
            .offset = 0,
        };
    }

    fn findBlock(self: *const SSTableReader, key: []const u8) ?usize {
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

    fn findBlockForPrefix(self: *const SSTableReader, prefix: []const u8) usize {
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

    fn readBlock(self: *SSTableReader, block_idx: usize) ![]u8 {
        const ie = self.index.items[block_idx];
        const buf = try self.allocator.alloc(u8, ie.block_size);
        errdefer self.allocator.free(buf);
        try readExact(self.file, self.io, buf, ie.block_offset);
        return buf;
    }

    fn scanBlockForKey(self: *SSTableReader, allocator: std.mem.Allocator, block_idx: usize, key: []const u8) !?EventValue {
        const data = try self.readBlock(block_idx);
        defer self.allocator.free(data);

        var off: usize = 0;
        while (off < data.len) {
            const parsed = parseRawEntry(data, off) orelse break;
            if (std.mem.eql(u8, parsed.key, key)) {
                return try EventValue.deserialize(allocator, parsed.value);
            }
            off = parsed.next_offset;
        }
        return null;
    }

    fn scanBlockContains(self: *SSTableReader, block_idx: usize, key: []const u8) !bool {
        const data = try self.readBlock(block_idx);
        defer self.allocator.free(data);

        var off: usize = 0;
        while (off < data.len) {
            const parsed = parseRawEntry(data, off) orelse break;
            if (std.mem.eql(u8, parsed.key, key)) return true;
            // Keys are sorted; stop early if we've passed the target
            if (std.mem.order(u8, parsed.key, key) == .gt) return false;
            off = parsed.next_offset;
        }
        return false;
    }

    fn scanBlockForPrefix(
        self: *SSTableReader,
        allocator: std.mem.Allocator,
        block_idx: usize,
        prefix: []const u8,
        results: *ArrayList(KeyValuePair),
    ) !void {
        const data = try self.readBlock(block_idx);
        defer self.allocator.free(data);

        var off: usize = 0;
        while (off < data.len) {
            const parsed = parseRawEntry(data, off) orelse break;
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
            } else if (std.mem.order(u8, parsed.key, prefix) == .gt and !std.mem.startsWith(u8, parsed.key, prefix)) {
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
                    const parsed = parseRawEntry(data, self.offset) orelse {
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

            self.block_data = try self.reader.readBlock(self.block_idx);
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

fn parseRawEntry(data: []const u8, off: usize) ?ParsedEntry {
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
    return ParsedEntry{
        .key = key,
        .value = value,
        .next_offset = val_start + val_len,
    };
}

fn parseLeadingU64(data: []const u8) u64 {
    var end: usize = 0;
    while (end < data.len and data[end] != '|') : (end += 1) {}
    return std.fmt.parseInt(u64, data[0..end], 10) catch 0;
}

fn readExact(file: IoFile, io: Io, buf: []u8, offset: u64) !void {
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

    const footer = try writeSSTable(allocator, path, io, &entries);
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

    const footer = try writeSSTable(allocator, path, io, &entries);
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

    var results = ArrayList(KeyValuePair){};
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

    _ = try writeSSTable(allocator, path, io, &entries);

    var reader = try SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    var iter = reader.rawIterator();
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

    _ = try writeSSTable(allocator, path, io, &entries);

    var reader = try SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    try std.testing.expect(try reader.contains("alpha"));
    try std.testing.expect(try reader.contains("beta"));
    try std.testing.expect(!try reader.contains("gamma"));
    try std.testing.expect(!try reader.contains("a"));
}
