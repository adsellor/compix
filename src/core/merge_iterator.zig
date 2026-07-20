const std = @import("std");
const assert = std.debug.assert;
const limits = @import("limits.zig");
const sstable = @import("sstable.zig");
const RawEntry = sstable.RawEntry;
const RawIteratorBuffered = sstable.RawIteratorBuffered;
const SSTableReader = sstable.SSTableReader;

pub const FixedMergeSource = struct {
    iterator: RawIteratorBuffered,
    sstable_id: u64,
    current: ?RawEntry,
};

pub const FixedMergeIterator = struct {
    sources: []FixedMergeSource,
    result_buffer: []u8,

    pub fn init(
        source_storage: []FixedMergeSource,
        readers: []*SSTableReader,
        sstable_ids: []const u64,
        block_storage: []u8,
        result_buffer: []u8,
    ) !FixedMergeIterator {
        assert(readers.len == sstable_ids.len);
        if (readers.len > source_storage.len) return error.SourceCapacityExceeded;
        const blocks_size = readers.len * limits.sstable_block_size_max;
        if (blocks_size > block_storage.len) return error.BlockStorageTooSmall;

        const sources = source_storage[0..readers.len];
        for (sources, readers, sstable_ids, 0..) |*source, reader, sstable_id, index| {
            const block_start = index * limits.sstable_block_size_max;
            const block = block_storage[block_start .. block_start + limits.sstable_block_size_max];
            source.* = .{
                .iterator = RawIteratorBuffered.init(reader, block),
                .sstable_id = sstable_id,
                .current = null,
            };
            source.current = try source.iterator.next();
        }
        return .{ .sources = sources, .result_buffer = result_buffer };
    }

    pub fn next(self: *FixedMergeIterator) !?RawEntry {
        var minimum_key: ?[]const u8 = null;
        for (self.sources) |source| {
            if (source.current) |entry| {
                if (minimum_key == null or
                    std.mem.order(u8, entry.key, minimum_key.?) == .lt)
                {
                    minimum_key = entry.key;
                }
            }
        }
        const key = minimum_key orelse return null;

        var winner: ?limits.CollectionIndex = null;
        var winner_id: u64 = 0;
        for (self.sources, 0..) |source, index| {
            if (source.current) |entry| {
                if (std.mem.eql(u8, entry.key, key)) {
                    if (winner == null or source.sstable_id > winner_id) {
                        winner = @intCast(index);
                        winner_id = source.sstable_id;
                    }
                }
            }
        }
        const winning_entry = self.sources[limits.usize_from(winner.?)].current.?;
        const result_size = winning_entry.key.len + winning_entry.value.len;
        if (result_size > self.result_buffer.len) return error.ResultBufferTooSmall;
        @memcpy(self.result_buffer[0..winning_entry.key.len], winning_entry.key);
        @memcpy(
            self.result_buffer[winning_entry.key.len..result_size],
            winning_entry.value,
        );
        const result = RawEntry{
            .key = self.result_buffer[0..winning_entry.key.len],
            .value = self.result_buffer[winning_entry.key.len..result_size],
        };

        for (self.sources) |*source| {
            if (source.current) |entry| {
                if (std.mem.eql(u8, entry.key, result.key)) {
                    source.current = try source.iterator.next();
                }
            }
        }
        return result;
    }
};

fn write_test_table(
    allocator: std.mem.Allocator,
    path: []const u8,
    io: std.Io,
    entries: []const RawEntry,
) !void {
    const data = try allocator.alloc(u8, limits.sstable_block_size_max);
    defer allocator.free(data);
    const index = try allocator.alloc(u8, limits.sstable_index_size_max);
    defer allocator.free(index);
    var writer = try sstable.StreamingWriter.init(path, io, data, index);
    errdefer writer.abort();
    for (entries) |entry| try writer.append(entry);
    _ = try writer.finish();
}

test "fixed merge iterator deduplicates with newest table winning" {
    const allocator = std.testing.allocator;
    const io = std.Io.Threaded.global_single_threaded.io();
    const path_old = "data/test_merge_fixed_old.sst";
    const path_new = "data/test_merge_fixed_new.sst";
    std.Io.Dir.cwd().deleteFile(io, path_old) catch {};
    std.Io.Dir.cwd().deleteFile(io, path_new) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, path_old) catch {};
    defer std.Io.Dir.cwd().deleteFile(io, path_new) catch {};

    try write_test_table(allocator, path_old, io, &.{
        .{ .key = "a", .value = "1|0|2|s|e|{}" },
        .{ .key = "b", .value = "2|0|2|s|e|{}" },
        .{ .key = "c", .value = "3|0|2|s|e|{}" },
    });
    try write_test_table(allocator, path_new, io, &.{
        .{ .key = "b", .value = "20|0|2|s|e|{}" },
        .{ .key = "d", .value = "40|0|2|s|e|{}" },
    });

    var reader_old = try SSTableReader.open_uncached(path_old, io);
    defer reader_old.deinit();
    var reader_new = try SSTableReader.open_uncached(path_new, io);
    defer reader_new.deinit();
    var readers = [_]*SSTableReader{ &reader_old, &reader_new };
    var sources: [2]FixedMergeSource = undefined;
    var blocks: [2 * limits.sstable_block_size_max]u8 = undefined;
    var result_buffer: [limits.sstable_block_size_max]u8 = undefined;
    var merge = try FixedMergeIterator.init(
        &sources,
        &readers,
        &.{ 1, 2 },
        &blocks,
        &result_buffer,
    );

    const expected_keys = [_][]const u8{ "a", "b", "c", "d" };
    var count: limits.CollectionCount = 0;
    while (try merge.next()) |entry| {
        try std.testing.expectEqualStrings(expected_keys[count], entry.key);
        if (std.mem.eql(u8, entry.key, "b")) {
            try std.testing.expectEqualStrings("20|0|2|s|e|{}", entry.value);
        }
        count += 1;
    }
    try std.testing.expectEqual(@as(limits.CollectionCount, 4), count);
}
