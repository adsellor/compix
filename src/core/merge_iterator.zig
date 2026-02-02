const std = @import("std");
const ArrayList = std.ArrayList;
const sstable = @import("sstable.zig");
const RawEntry = sstable.RawEntry;
const SSTableReader = sstable.SSTableReader;
const RawIterator = sstable.RawIterator;

pub const MergeIterator = struct {
    allocator: std.mem.Allocator,
    sources: []Source,
    result_buf: ?[]u8,

    const Source = struct {
        iter: RawIterator,
        sst_id: u64,
        current_key: ?[]u8,
        current_value: ?[]u8,

        fn freeCurrentEntry(self: *Source, allocator: std.mem.Allocator) void {
            if (self.current_key) |k| allocator.free(k);
            if (self.current_value) |v| allocator.free(v);
            self.current_key = null;
            self.current_value = null;
        }
    };

    pub fn init(
        allocator: std.mem.Allocator,
        readers: []*SSTableReader,
        sst_ids: []const u64,
    ) !MergeIterator {
        const sources = try allocator.alloc(Source, readers.len);
        errdefer allocator.free(sources);

        for (readers, 0..) |reader, i| {
            sources[i] = Source{
                .iter = reader.rawIterator(),
                .sst_id = sst_ids[i],
                .current_key = null,
                .current_value = null,
            };
        }

        var self = MergeIterator{
            .allocator = allocator,
            .sources = sources,
            .result_buf = null,
        };
        for (self.sources) |*s| {
            try self.advance(s);
        }
        return self;
    }

    pub fn deinit(self: *MergeIterator) void {
        for (self.sources) |*s| {
            s.freeCurrentEntry(self.allocator);
            s.iter.deinit();
        }
        if (self.result_buf) |buf| self.allocator.free(buf);
        self.allocator.free(self.sources);
    }

    pub fn next(self: *MergeIterator) !?RawEntry {
        if (self.result_buf) |buf| {
            self.allocator.free(buf);
            self.result_buf = null;
        }

        var min_key: ?[]const u8 = null;
        for (self.sources) |s| {
            if (s.current_key) |k| {
                if (min_key == null or std.mem.order(u8, k, min_key.?) == .lt) {
                    min_key = k;
                }
            }
        }
        if (min_key == null) return null;

        var winner_idx: ?usize = null;
        var winner_id: u64 = 0;
        for (self.sources, 0..) |s, i| {
            if (s.current_key) |k| {
                if (std.mem.eql(u8, k, min_key.?)) {
                    if (winner_idx == null or s.sst_id > winner_id) {
                        winner_idx = i;
                        winner_id = s.sst_id;
                    }
                }
            }
        }

        const win = winner_idx.?;
        const win_key = self.sources[win].current_key.?;
        const win_value = self.sources[win].current_value.?;

        const buf = try self.allocator.alloc(u8, win_key.len + win_value.len);
        // TODO: see if I can get rid of this
        @memcpy(buf[0..win_key.len], win_key);
        @memcpy(buf[win_key.len..], win_value);
        self.result_buf = buf;

        const result = RawEntry{
            .key = buf[0..win_key.len],
            .value = buf[win_key.len..],
        };

        for (self.sources) |*s| {
            if (s.current_key) |k| {
                if (std.mem.eql(u8, k, result.key)) {
                    try self.advance(s);
                }
            }
        }

        return result;
    }

    fn advance(self: *MergeIterator, source: *Source) !void {
        source.freeCurrentEntry(self.allocator);
        if (try source.iter.next()) |entry| {
            source.current_key = try self.allocator.dupe(u8, entry.key);
            source.current_value = try self.allocator.dupe(u8, entry.value);
        }
    }
};

pub fn collectMerged(
    allocator: std.mem.Allocator,
    merge_iter: *MergeIterator,
) ![]RawEntry {
    var result = ArrayList(RawEntry){};
    errdefer {
        for (result.items) |e| {
            allocator.free(e.key);
            allocator.free(e.value);
        }
        result.deinit(allocator);
    }

    while (try merge_iter.next()) |entry| {
        try result.append(allocator, RawEntry{
            .key = try allocator.dupe(u8, entry.key),
            .value = try allocator.dupe(u8, entry.value),
        });
    }
    return result.toOwnedSlice(allocator);
}

const Io = std.Io;
const IoDir = Io.Dir;

test "merge_iterator single source" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_merge_single.sst";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    const entries = [_]sstable.RawEntry{
        .{ .key = "a", .value = "1|0|2|s|e|{}" },
        .{ .key = "b", .value = "2|0|2|s|e|{}" },
        .{ .key = "c", .value = "3|0|2|s|e|{}" },
    };
    _ = try sstable.writeSSTable(allocator, path, io, &entries);

    var reader = try SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    var readers = [_]*SSTableReader{&reader};
    const ids = [_]u64{1};

    var merge = try MergeIterator.init(allocator, &readers, &ids);
    defer merge.deinit();

    const e1 = (try merge.next()).?;
    try std.testing.expectEqualStrings("a", e1.key);
    const e2 = (try merge.next()).?;
    try std.testing.expectEqualStrings("b", e2.key);
    const e3 = (try merge.next()).?;
    try std.testing.expectEqualStrings("c", e3.key);
    try std.testing.expect(try merge.next() == null);
}

test "merge_iterator deduplication newest wins" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path1 = "data/test_merge_dedup1.sst";
    const path2 = "data/test_merge_dedup2.sst";
    IoDir.cwd().deleteFile(io, path1) catch {};
    IoDir.cwd().deleteFile(io, path2) catch {};
    defer IoDir.cwd().deleteFile(io, path1) catch {};
    defer IoDir.cwd().deleteFile(io, path2) catch {};

    // SST 1 (older): a=1, b=2, c=3
    const entries1 = [_]sstable.RawEntry{
        .{ .key = "a", .value = "1|0|2|s|e|{}" },
        .{ .key = "b", .value = "2|0|2|s|e|{}" },
        .{ .key = "c", .value = "3|0|2|s|e|{}" },
    };
    _ = try sstable.writeSSTable(allocator, path1, io, &entries1);

    // SST 2 (newer): b=20, d=40
    const entries2 = [_]sstable.RawEntry{
        .{ .key = "b", .value = "20|0|2|s|e|{}" },
        .{ .key = "d", .value = "40|0|2|s|e|{}" },
    };
    _ = try sstable.writeSSTable(allocator, path2, io, &entries2);

    var r1 = try SSTableReader.open(allocator, path1, io);
    defer r1.deinit();
    var r2 = try SSTableReader.open(allocator, path2, io);
    defer r2.deinit();

    var readers = [_]*SSTableReader{ &r1, &r2 };
    const ids = [_]u64{ 1, 2 }; // r2 is newer (id=2)

    var merge = try MergeIterator.init(allocator, &readers, &ids);
    defer merge.deinit();

    const collected = try collectMerged(allocator, &merge);
    defer {
        for (collected) |e| {
            allocator.free(e.key);
            allocator.free(e.value);
        }
        allocator.free(collected);
    }

    try std.testing.expectEqual(@as(usize, 4), collected.len);
    try std.testing.expectEqualStrings("a", collected[0].key);
    try std.testing.expectEqualStrings("1|0|2|s|e|{}", collected[0].value);
    try std.testing.expectEqualStrings("b", collected[1].key);
    try std.testing.expectEqualStrings("20|0|2|s|e|{}", collected[1].value); // newest wins
    try std.testing.expectEqualStrings("c", collected[2].key);
    try std.testing.expectEqualStrings("d", collected[3].key);
}
