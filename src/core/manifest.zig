const std = @import("std");
const ArrayList = std.ArrayList;
const sstable = @import("sstable.zig");
const SSTInfo = sstable.SSTInfo;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;

pub const Manifest = struct {
    allocator: std.mem.Allocator,
    io: Io,
    path: []const u8,
    file: ?IoFile,
    write_pos: u64,

    live: ArrayList(SSTInfo),
    version: u64,

    pub fn init(allocator: std.mem.Allocator, path: []const u8, io: Io) !Manifest {
        var self = Manifest{
            .allocator = allocator,
            .io = io,
            .path = try allocator.dupe(u8, path),
            .file = null,
            .write_pos = 0,
            .live = ArrayList(SSTInfo){},
            .version = 0,
        };
        errdefer {
            allocator.free(self.path);
            for (self.live.items) |*s| s.deinit(allocator);
            self.live.deinit(allocator);
        }

        const dir = IoDir.cwd();
        const file = dir.openFile(io, path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => {
                self.file = try dir.createFile(io, path, .{ .truncate = false, .read = true });
                return self;
            },
            else => return err,
        };
        self.file = file;

        try self.load();

        return self;
    }

    pub fn deinit(self: *Manifest) void {
        if (self.file) |f| f.close(self.io);
        for (self.live.items) |*s| s.deinit(self.allocator);
        self.live.deinit(self.allocator);
        self.allocator.free(self.path);
    }

    pub fn addSSTable(self: *Manifest, info: SSTInfo) !void {
        self.version += 1;
        const line = try formatAdd(self.allocator, self.version, info);
        defer self.allocator.free(line);
        try self.appendLine(line);

        try self.live.append(self.allocator, SSTInfo{
            .id = info.id,
            .level = info.level,
            .min_key = try self.allocator.dupe(u8, info.min_key),
            .max_key = try self.allocator.dupe(u8, info.max_key),
            .num_entries = info.num_entries,
            .min_seq = info.min_seq,
            .max_seq = info.max_seq,
            .file_size = info.file_size,
        });
    }

    pub fn atomicCompaction(
        self: *Manifest,
        remove_ids: []const u64,
        new_info: SSTInfo,
    ) !void {
        self.version += 1;
        const ver = self.version;

        for (remove_ids) |id| {
            const line = try formatDel(self.allocator, ver, id);
            defer self.allocator.free(line);
            try self.appendLine(line);
        }

        const add_line = try formatAdd(self.allocator, ver, new_info);
        defer self.allocator.free(add_line);
        try self.appendLine(add_line);

        if (self.file) |f| f.sync(self.io) catch {};

        var i: usize = 0;
        while (i < self.live.items.len) {
            var found = false;
            for (remove_ids) |rid| {
                if (self.live.items[i].id == rid) {
                    found = true;
                    break;
                }
            }
            if (found) {
                self.live.items[i].deinit(self.allocator);
                _ = self.live.swapRemove(i);
            } else {
                i += 1;
            }
        }

        try self.live.append(self.allocator, SSTInfo{
            .id = new_info.id,
            .level = new_info.level,
            .min_key = try self.allocator.dupe(u8, new_info.min_key),
            .max_key = try self.allocator.dupe(u8, new_info.max_key),
            .num_entries = new_info.num_entries,
            .min_seq = new_info.min_seq,
            .max_seq = new_info.max_seq,
            .file_size = new_info.file_size,
        });
    }

    pub fn getLiveSorted(self: *const Manifest, allocator: std.mem.Allocator) ![]SSTInfo {
        const copy = try allocator.alloc(SSTInfo, self.live.items.len);
        @memcpy(copy, self.live.items);
        std.mem.sort(SSTInfo, copy, {}, struct {
            fn lessThan(_: void, a: SSTInfo, b: SSTInfo) bool {
                if (a.level != b.level) return a.level < b.level;
                return a.id > b.id;
            }
        }.lessThan);
        return copy;
    }

    pub fn countAtLevel(self: *const Manifest, level: u32) usize {
        var n: usize = 0;
        for (self.live.items) |s| {
            if (s.level == level) n += 1;
        }
        return n;
    }

    pub fn getIdsAtLevel(self: *const Manifest, allocator: std.mem.Allocator, level: u32) ![]u64 {
        var ids = ArrayList(u64){};
        for (self.live.items) |s| {
            if (s.level == level) try ids.append(allocator, s.id);
        }
        return ids.toOwnedSlice(allocator);
    }

    fn load(self: *Manifest) !void {
        const file = self.file orelse return;
        const file_size = (try file.stat(self.io)).size;
        if (file_size == 0) return;

        const buf = try self.allocator.alloc(u8, file_size);
        defer self.allocator.free(buf);

        var total: usize = 0;
        while (total < buf.len) {
            var iovecs = [1][]u8{buf[total..]};
            const n = file.readPositional(self.io, &iovecs, total) catch break;
            if (n == 0) break;
            total += n;
        }

        const data = buf[0..total];
        var line_iter = std.mem.splitScalar(u8, data, '\n');
        while (line_iter.next()) |line| {
            if (line.len == 0) continue;
            self.applyRecord(line) catch continue;
        }

        self.write_pos = file_size;
    }

    fn applyRecord(self: *Manifest, line: []const u8) !void {
        var iter = std.mem.splitScalar(u8, line, ' ');
        const action = iter.next() orelse return error.InvalidFormat;

        if (std.mem.eql(u8, action, "ADD")) {
            const ver = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
            const sst_id = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
            const level = try std.fmt.parseInt(u32, iter.next() orelse return error.InvalidFormat, 10);
            const num_entries = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
            const min_seq = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
            const max_seq = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
            const file_size = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
            const min_key = iter.next() orelse return error.InvalidFormat;
            const max_key = iter.rest();

            if (ver > self.version) self.version = ver;

            try self.live.append(self.allocator, SSTInfo{
                .id = sst_id,
                .level = level,
                .min_key = try self.allocator.dupe(u8, min_key),
                .max_key = try self.allocator.dupe(u8, max_key),
                .num_entries = num_entries,
                .min_seq = min_seq,
                .max_seq = max_seq,
                .file_size = file_size,
            });
        } else if (std.mem.eql(u8, action, "DEL")) {
            const ver = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
            const sst_id = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);

            if (ver > self.version) self.version = ver;

            // Remove from live set
            var i: usize = 0;
            while (i < self.live.items.len) {
                if (self.live.items[i].id == sst_id) {
                    self.live.items[i].deinit(self.allocator);
                    _ = self.live.swapRemove(i);
                    break;
                }
                i += 1;
            }
        }
    }

    fn appendLine(self: *Manifest, line: []const u8) !void {
        const file = self.file orelse return error.InputOutput;
        file.writePositionalAll(self.io, line, self.write_pos) catch return error.InputOutput;
        self.write_pos += line.len;
    }

    fn formatAdd(allocator: std.mem.Allocator, version: u64, info: SSTInfo) ![]u8 {
        return try std.fmt.allocPrint(allocator, "ADD {} {} {} {} {} {} {} {s} {s}\n", .{
            version,
            info.id,
            info.level,
            info.num_entries,
            info.min_seq,
            info.max_seq,
            info.file_size,
            info.min_key,
            info.max_key,
        });
    }

    fn formatDel(allocator: std.mem.Allocator, version: u64, sst_id: u64) ![]u8 {
        return try std.fmt.allocPrint(allocator, "DEL {} {}\n", .{ version, sst_id });
    }
};

test "manifest add and reload round-trip" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_manifest_rt.manifest";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    {
        var m = try Manifest.init(allocator, path, io);
        defer m.deinit();

        try m.addSSTable(.{
            .id = 1,
            .level = 0,
            .min_key = "aaa",
            .max_key = "zzz",
            .num_entries = 100,
            .min_seq = 1,
            .max_seq = 100,
            .file_size = 4096,
        });

        try m.addSSTable(.{
            .id = 2,
            .level = 0,
            .min_key = "bbb",
            .max_key = "yyy",
            .num_entries = 50,
            .min_seq = 101,
            .max_seq = 150,
            .file_size = 2048,
        });

        try std.testing.expectEqual(@as(usize, 2), m.live.items.len);
    }

    {
        var m = try Manifest.init(allocator, path, io);
        defer m.deinit();

        try std.testing.expectEqual(@as(usize, 2), m.live.items.len);
        try std.testing.expectEqual(@as(u64, 2), m.version);
    }
}

test "manifest atomic compaction" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_manifest_compact.manifest";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    var m = try Manifest.init(allocator, path, io);
    defer m.deinit();

    for (1..4) |i| {
        try m.addSSTable(.{
            .id = i,
            .level = 0,
            .min_key = "a",
            .max_key = "z",
            .num_entries = 100,
            .min_seq = 1,
            .max_seq = 100,
            .file_size = 4096,
        });
    }
    try std.testing.expectEqual(@as(usize, 3), m.countAtLevel(0));

    const remove_ids = [_]u64{ 1, 2, 3 };
    try m.atomicCompaction(&remove_ids, .{
        .id = 4,
        .level = 1,
        .min_key = "a",
        .max_key = "z",
        .num_entries = 300,
        .min_seq = 1,
        .max_seq = 100,
        .file_size = 12000,
    });

    try std.testing.expectEqual(@as(usize, 0), m.countAtLevel(0));
    try std.testing.expectEqual(@as(usize, 1), m.countAtLevel(1));
    try std.testing.expectEqual(@as(usize, 1), m.live.items.len);
    try std.testing.expectEqual(@as(u64, 4), m.live.items[0].id);
}

test "manifest reload after compaction" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_manifest_compact_reload.manifest";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    {
        var m = try Manifest.init(allocator, path, io);
        defer m.deinit();

        try m.addSSTable(.{
            .id = 1,
            .level = 0,
            .min_key = "a",
            .max_key = "m",
            .num_entries = 50,
            .min_seq = 1,
            .max_seq = 50,
            .file_size = 2000,
        });
        try m.addSSTable(.{
            .id = 2,
            .level = 0,
            .min_key = "n",
            .max_key = "z",
            .num_entries = 50,
            .min_seq = 51,
            .max_seq = 100,
            .file_size = 2000,
        });

        const remove = [_]u64{ 1, 2 };
        try m.atomicCompaction(&remove, .{
            .id = 3,
            .level = 1,
            .min_key = "a",
            .max_key = "z",
            .num_entries = 100,
            .min_seq = 1,
            .max_seq = 100,
            .file_size = 4000,
        });
    }

    {
        var m = try Manifest.init(allocator, path, io);
        defer m.deinit();

        try std.testing.expectEqual(@as(usize, 1), m.live.items.len);
        try std.testing.expectEqual(@as(u64, 3), m.live.items[0].id);
        try std.testing.expectEqual(@as(u32, 1), m.live.items[0].level);
    }
}

test "manifest getLiveSorted" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const path = "data/test_manifest_sorted.manifest";
    IoDir.cwd().deleteFile(io, path) catch {};
    defer IoDir.cwd().deleteFile(io, path) catch {};

    var m = try Manifest.init(allocator, path, io);
    defer m.deinit();

    try m.addSSTable(.{
        .id = 1,
        .level = 0,
        .min_key = "a",
        .max_key = "z",
        .num_entries = 10,
        .min_seq = 1,
        .max_seq = 10,
        .file_size = 100,
    });
    try m.addSSTable(.{
        .id = 2,
        .level = 1,
        .min_key = "a",
        .max_key = "z",
        .num_entries = 20,
        .min_seq = 1,
        .max_seq = 20,
        .file_size = 200,
    });
    try m.addSSTable(.{
        .id = 3,
        .level = 0,
        .min_key = "a",
        .max_key = "z",
        .num_entries = 5,
        .min_seq = 11,
        .max_seq = 15,
        .file_size = 50,
    });

    const sorted = try m.getLiveSorted(allocator);
    defer allocator.free(sorted);

    try std.testing.expectEqual(@as(u64, 3), sorted[0].id);
    try std.testing.expectEqual(@as(u64, 1), sorted[1].id);
    try std.testing.expectEqual(@as(u64, 2), sorted[2].id);
}
