const std = @import("std");
const assert = std.debug.assert;
const fixed = @import("fixed.zig");
const sstable = @import("sstable.zig");
const SSTInfo = sstable.SSTInfo;

const Io = std.Io;
const IoFile = Io.File;
const IoDir = Io.Dir;
const limits = @import("limits.zig");

const KeySlot = struct {
    min_key: [limits.key_length_max]u8,
    max_key: [limits.key_length_max]u8,
};

const checksum_size: u32 = 16;
const checksum_hex_size: u32 = 2 * checksum_size;
const record_size_max = 2 * limits.key_length_max + 512;
const rewrite_buffer_size: u32 = 64 * 1024;

pub const FaultPoint = enum {
    before_copy,
    after_copy,
    after_record_write,
    after_file_sync,
    after_replace,
    after_directory_sync,
};

pub const FaultInjector = struct {
    fail_at: FaultPoint,
    triggered: bool = false,

    fn check(self: *FaultInjector, point: FaultPoint) !void {
        if (self.fail_at != point) return;
        self.triggered = true;
        return error.InjectedManifestFault;
    }
};

pub const Manifest = struct {
    allocator: std.mem.Allocator,
    io: Io,
    path: []const u8,
    file: ?IoFile,
    write_pos: u64,

    live: fixed.List(SSTInfo),
    live_storage: []SSTInfo,
    key_slots: []KeySlot,
    key_slot_used: [limits.sstables_max]bool,
    version: u64,
    rewrite_buffer: []u8,
    fault_injector: ?*FaultInjector,

    pub fn init(allocator: std.mem.Allocator, path: []const u8, io: Io) !Manifest {
        const live_storage = try allocator.alloc(SSTInfo, limits.sstables_max);
        errdefer allocator.free(live_storage);
        const key_slots = try allocator.alloc(KeySlot, limits.sstables_max);
        errdefer allocator.free(key_slots);
        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);
        const rewrite_buffer = try allocator.alloc(u8, rewrite_buffer_size);
        errdefer allocator.free(rewrite_buffer);
        var self = Manifest{
            .allocator = allocator,
            .io = io,
            .path = owned_path,
            .file = null,
            .write_pos = 0,
            .live = try fixed.List(SSTInfo).init(live_storage),
            .live_storage = live_storage,
            .key_slots = key_slots,
            .key_slot_used = @splat(false),
            .version = 0,
            .rewrite_buffer = rewrite_buffer,
            .fault_injector = null,
        };
        errdefer if (self.file) |open_file| open_file.close(io);

        const dir = IoDir.cwd();
        const file = dir.openFile(io, path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => {
                self.file = try dir.createFile(io, path, .{ .truncate = false, .read = true });
                const created = self.file.?;
                created.sync(io) catch return error.SyncFailed;
                try sync_parent_directory(path, io);
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
        self.allocator.free(self.live_storage);
        self.allocator.free(self.key_slots);
        self.allocator.free(self.rewrite_buffer);
        self.allocator.free(self.path);
    }

    pub fn add_sstable(self: *Manifest, info: SSTInfo) !void {
        assert(info.id > 0);
        assert(info.min_seq <= info.max_seq);
        const live_before = self.live.count;
        if (live_before >= limits.sstables_max) return error.TooManySSTables;

        const stored = try self.store_info(info);
        errdefer self.release_slot(stored.storage_slot);

        const version = self.version + 1;
        var line_buffer: [record_size_max]u8 = undefined;
        const line = try format_add_into(&line_buffer, version, info);
        try self.append_record(line);

        self.live.append(stored) catch unreachable;
        self.version = version;

        assert(self.live.count == live_before + 1);
    }

    pub fn atomic_compaction(
        self: *Manifest,
        remove_ids: []const u64,
        new_info: SSTInfo,
    ) !void {
        assert(remove_ids.len > 0);
        assert(new_info.id > 0);
        const live_before = self.live.count;
        if (remove_ids.len > live_before) return error.InvalidCompaction;
        for (self.live.items_const()) |existing| {
            if (existing.id == new_info.id) return error.DuplicateSSTable;
        }
        for (remove_ids, 0..) |remove_id, index| {
            if (remove_id == new_info.id) return error.InvalidCompaction;
            var found = false;
            for (self.live.items_const()) |existing| {
                if (existing.id == remove_id) found = true;
            }
            if (!found) return error.InvalidCompaction;
            for (remove_ids[0..index]) |prior| {
                if (prior == remove_id) return error.InvalidCompaction;
            }
        }

        const stored = try self.store_info(new_info);
        errdefer self.release_slot(stored.storage_slot);

        const ver = self.version + 1;
        var record_buffer: [record_size_max]u8 = undefined;
        const record = try format_compaction_into(
            &record_buffer,
            ver,
            remove_ids,
            new_info,
        );
        try self.append_record(record);

        var i: limits.SSTableIndex = 0;
        while (i < self.live.count) {
            var found = false;
            for (remove_ids) |rid| {
                if (self.live.items()[i].id == rid) {
                    found = true;
                    break;
                }
            }
            if (found) {
                const removed = self.live.swap_remove(i);
                self.release_slot(removed.storage_slot);
            } else {
                i += 1;
            }
        }

        self.live.append(stored) catch unreachable;
        self.version = ver;

        assert(self.live.count == live_before - remove_ids.len + 1);
    }

    pub fn count_at_level(self: *const Manifest, level: u32) u16 {
        var count: u16 = 0;
        for (self.live.items_const()) |s| {
            if (s.level == level) count += 1;
        }
        return count;
    }

    pub fn set_fault_injector(self: *Manifest, injector: ?*FaultInjector) void {
        self.fault_injector = injector;
    }

    fn load(self: *Manifest) !void {
        const file = self.file orelse return;
        const file_size = (try file.stat(self.io)).size;
        if (file_size == 0) return;
        if (file_size > limits.manifest_file_size_max) return error.ManifestTooLarge;

        const buf = try self.allocator.alloc(u8, file_size);
        defer self.allocator.free(buf);

        var total: limits.ByteCount = 0;
        while (total < buf.len) {
            var iovecs = [1][]u8{buf[total..]};
            const n = file.readPositional(self.io, &iovecs, total) catch return error.InputOutput;
            if (n == 0) break;
            total += try limits.byte_count(n);
        }
        assert(total <= buf.len);

        const last_newline = std.mem.lastIndexOfScalar(u8, buf[0..total], '\n');
        const valid_size: limits.ByteCount = if (last_newline) |index|
            @intCast(index + 1)
        else
            0;
        if (valid_size != total) {
            file.setLength(self.io, valid_size) catch return error.InputOutput;
            file.sync(self.io) catch return error.SyncFailed;
        }
        const data = buf[0..valid_size];
        var line_iter = std.mem.splitScalar(u8, data, '\n');
        while (line_iter.next()) |line| {
            if (line.len == 0) continue;
            try self.apply_record(try verify_record(line));
        }

        self.write_pos = valid_size;
    }

    fn apply_record(self: *Manifest, line: []const u8) !void {
        var iter = std.mem.splitScalar(u8, line, ' ');
        const action = iter.next() orelse return error.InvalidFormat;

        if (std.mem.eql(u8, action, "ADD")) {
            const ver_str = iter.next() orelse return error.InvalidFormat;
            const ver = try std.fmt.parseInt(u64, ver_str, 10);
            if (ver != self.version + 1) return error.InvalidManifestVersion;
            const sst_id_str = iter.next() orelse return error.InvalidFormat;
            const sst_id = try std.fmt.parseInt(u64, sst_id_str, 10);
            const level_str = iter.next() orelse return error.InvalidFormat;
            const level = try std.fmt.parseInt(u32, level_str, 10);
            const num_entries_str = iter.next() orelse return error.InvalidFormat;
            const num_entries = try std.fmt.parseInt(u64, num_entries_str, 10);
            const min_seq_str = iter.next() orelse return error.InvalidFormat;
            const min_seq = try std.fmt.parseInt(u64, min_seq_str, 10);
            const max_seq_str = iter.next() orelse return error.InvalidFormat;
            const max_seq = try std.fmt.parseInt(u64, max_seq_str, 10);
            const file_size_str = iter.next() orelse return error.InvalidFormat;
            const file_size = try std.fmt.parseInt(u64, file_size_str, 10);
            const min_key = iter.next() orelse return error.InvalidFormat;
            const max_key = iter.rest();

            if (self.live.count >= limits.sstables_max) return error.TooManySSTables;
            for (self.live.items_const()) |existing| {
                if (existing.id == sst_id) return error.DuplicateSSTable;
            }
            const stored = try self.store_info(.{
                .id = sst_id,
                .level = level,
                .min_key = min_key,
                .max_key = max_key,
                .num_entries = num_entries,
                .min_seq = min_seq,
                .max_seq = max_seq,
                .file_size = file_size,
            });
            self.live.append(stored) catch unreachable;
            self.version = ver;
        } else if (std.mem.eql(u8, action, "COMPACT")) {
            const ver_str = iter.next() orelse return error.InvalidFormat;
            const ver = try std.fmt.parseInt(u64, ver_str, 10);
            if (ver != self.version + 1) return error.InvalidManifestVersion;
            const remove_count = try std.fmt.parseInt(
                limits.SSTableCount,
                iter.next() orelse return error.InvalidFormat,
                10,
            );
            if (remove_count == 0 or remove_count > limits.compaction_fan_in_max)
                return error.InvalidCompaction;
            var remove_ids: [limits.compaction_fan_in_max]u64 = undefined;
            for (remove_ids[0..remove_count], 0..) |*id, index| {
                id.* = try std.fmt.parseInt(
                    u64,
                    iter.next() orelse return error.InvalidFormat,
                    10,
                );
                for (remove_ids[0..index]) |prior| {
                    if (prior == id.*) return error.InvalidCompaction;
                }
            }
            const new_info: SSTInfo = .{
                .id = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10),
                .level = try std.fmt.parseInt(u32, iter.next() orelse return error.InvalidFormat, 10),
                .num_entries = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10),
                .min_seq = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10),
                .max_seq = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10),
                .file_size = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10),
                .min_key = iter.next() orelse return error.InvalidFormat,
                .max_key = iter.next() orelse return error.InvalidFormat,
            };
            if (iter.next() != null) return error.InvalidFormat;
            if (new_info.id == 0 or new_info.min_seq > new_info.max_seq)
                return error.InvalidFormat;
            for (remove_ids[0..remove_count]) |removed_id| {
                if (removed_id == new_info.id) return error.InvalidCompaction;
                var found = false;
                for (self.live.items_const()) |existing| {
                    if (existing.id == removed_id) found = true;
                    if (existing.id == new_info.id) return error.DuplicateSSTable;
                }
                if (!found) return error.InvalidCompaction;
            }
            const stored = try self.store_info(new_info);
            errdefer self.release_slot(stored.storage_slot);
            var i: limits.SSTableIndex = 0;
            while (i < self.live.count) {
                var remove = false;
                for (remove_ids[0..remove_count]) |removed_id| {
                    if (self.live.items()[i].id == removed_id) remove = true;
                }
                if (remove) {
                    const removed = self.live.swap_remove(i);
                    self.release_slot(removed.storage_slot);
                } else {
                    i += 1;
                }
            }
            self.live.append(stored) catch unreachable;
            self.version = ver;
        } else return error.InvalidFormat;
    }

    fn append_record(self: *Manifest, content: []const u8) !void {
        const file = self.file orelse return error.InputOutput;
        var record: [record_size_max + checksum_hex_size + 2]u8 = undefined;
        if (content.len + checksum_hex_size + 2 > record.len) return error.RecordTooLarge;
        @memcpy(record[0..content.len], content);
        record[content.len] = ' ';
        const checksum = manifest_checksum(content);
        const hex = std.fmt.bytesToHex(checksum, .lower);
        @memcpy(record[content.len + 1 ..][0..hex.len], &hex);
        const size = content.len + 1 + hex.len + 1;
        record[size - 1] = '\n';
        const new_size = std.math.add(u64, self.write_pos, size) catch
            return error.ManifestTooLarge;
        if (new_size > limits.manifest_file_size_max) return error.ManifestTooLarge;

        // Each update publishes a complete manifest generation. A crash before
        // replace leaves the old file; a crash after replace exposes the fully
        // synced old history plus the new checksummed transaction.
        var atomic = try IoDir.cwd().createFileAtomic(self.io, self.path, .{ .replace = true });
        var atomic_open = true;
        errdefer if (atomic_open) atomic.deinit(self.io);
        try self.inject_fault(.before_copy);
        var offset: u64 = 0;
        while (offset < self.write_pos) {
            const chunk_size: usize = @intCast(@min(
                @as(u64, self.rewrite_buffer.len),
                self.write_pos - offset,
            ));
            const chunk = self.rewrite_buffer[0..chunk_size];
            const read = file.readPositionalAll(self.io, chunk, offset) catch
                return error.InputOutput;
            if (read != chunk.len) return error.InputOutput;
            atomic.file.writePositionalAll(self.io, chunk, offset) catch
                return error.InputOutput;
            offset += chunk.len;
        }
        try self.inject_fault(.after_copy);
        atomic.file.writePositionalAll(self.io, record[0..size], self.write_pos) catch
            return error.InputOutput;
        try self.inject_fault(.after_record_write);
        atomic.file.sync(self.io) catch return error.SyncFailed;
        try self.inject_fault(.after_file_sync);
        try atomic.replace(self.io);
        try self.inject_fault(.after_replace);
        try sync_parent_directory(self.path, self.io);
        try self.inject_fault(.after_directory_sync);
        atomic.deinit(self.io);
        atomic_open = false;

        file.close(self.io);
        self.file = null;
        self.file = IoDir.cwd().openFile(self.io, self.path, .{ .mode = .read_write }) catch
            return error.InputOutput;
        self.write_pos += size;
        assert(self.write_pos == new_size);
    }

    fn inject_fault(self: *Manifest, point: FaultPoint) !void {
        if (self.fault_injector) |injector| try injector.check(point);
    }

    fn acquire_slot(self: *Manifest) !limits.SSTableIndex {
        for (&self.key_slot_used, 0..) |*used, index| {
            if (!used.*) {
                used.* = true;
                return @intCast(index);
            }
        }
        return error.TooManySSTables;
    }

    fn release_slot(self: *Manifest, slot: limits.SSTableIndex) void {
        assert(slot < self.key_slots.len);
        assert(self.key_slot_used[slot]);
        self.key_slot_used[slot] = false;
    }

    fn store_info(self: *Manifest, info: SSTInfo) !SSTInfo {
        if (info.min_key.len > limits.key_length_max) return error.KeyTooLong;
        if (info.max_key.len > limits.key_length_max) return error.KeyTooLong;
        const slot = try self.acquire_slot();
        const keys = &self.key_slots[slot];
        @memcpy(keys.min_key[0..info.min_key.len], info.min_key);
        @memcpy(keys.max_key[0..info.max_key.len], info.max_key);
        return .{
            .id = info.id,
            .level = info.level,
            .min_key = keys.min_key[0..info.min_key.len],
            .max_key = keys.max_key[0..info.max_key.len],
            .num_entries = info.num_entries,
            .min_seq = info.min_seq,
            .max_seq = info.max_seq,
            .file_size = info.file_size,
            .storage_slot = slot,
        };
    }

    fn format_add_into(buffer: []u8, version: u64, info: SSTInfo) ![]const u8 {
        return std.fmt.bufPrint(buffer, "ADD {} {} {} {} {} {} {} {s} {s}", .{
            version,
            info.id,
            info.level,
            info.num_entries,
            info.min_seq,
            info.max_seq,
            info.file_size,
            info.min_key,
            info.max_key,
        }) catch return error.RecordTooLarge;
    }

    fn format_compaction_into(
        buffer: []u8,
        version: u64,
        remove_ids: []const u64,
        info: SSTInfo,
    ) ![]const u8 {
        var stream = std.Io.Writer.fixed(buffer);
        try stream.print("COMPACT {} {}", .{ version, remove_ids.len });
        for (remove_ids) |id| try stream.print(" {}", .{id});
        try stream.print(" {} {} {} {} {} {} {s} {s}", .{
            info.id,
            info.level,
            info.num_entries,
            info.min_seq,
            info.max_seq,
            info.file_size,
            info.min_key,
            info.max_key,
        });
        return stream.buffered();
    }
};

fn manifest_checksum(content: []const u8) [checksum_size]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("compix/manifest/v1");
    hash.update(content);
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hash.final(&digest);
    return digest[0..checksum_size].*;
}

fn verify_record(line: []const u8) ![]const u8 {
    const separator = std.mem.lastIndexOfScalar(u8, line, ' ') orelse
        return error.InvalidFormat;
    const content = line[0..separator];
    const encoded = line[separator + 1 ..];
    if (encoded.len != checksum_hex_size) return error.InvalidFormat;
    var stored: [checksum_size]u8 = undefined;
    _ = std.fmt.hexToBytes(&stored, encoded) catch return error.InvalidFormat;
    const expected = manifest_checksum(content);
    if (!std.crypto.timing_safe.eql([checksum_size]u8, expected, stored))
        return error.ManifestChecksumMismatch;
    return content;
}

fn sync_parent_directory(path: []const u8, io: Io) !void {
    const parent_path = std.fs.path.dirname(path) orelse ".";
    const handle = try std.posix.openat(
        std.posix.AT.FDCWD,
        parent_path,
        .{ .ACCMODE = .RDONLY, .DIRECTORY = true, .CLOEXEC = true },
        0,
    );
    const file: IoFile = .{
        .handle = handle,
        .flags = .{ .nonblocking = false },
    };
    defer file.close(io);
    file.sync(io) catch return error.DirectorySyncFailed;
}

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

        try m.add_sstable(.{
            .id = 1,
            .level = 0,
            .min_key = "aaa",
            .max_key = "zzz",
            .num_entries = 100,
            .min_seq = 1,
            .max_seq = 100,
            .file_size = 4096,
        });

        try m.add_sstable(.{
            .id = 2,
            .level = 0,
            .min_key = "bbb",
            .max_key = "yyy",
            .num_entries = 50,
            .min_seq = 101,
            .max_seq = 150,
            .file_size = 2048,
        });

        try std.testing.expectEqual(@as(u32, 2), m.live.count);
    }

    {
        var m = try Manifest.init(allocator, path, io);
        defer m.deinit();

        try std.testing.expectEqual(@as(u32, 2), m.live.count);
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
        try m.add_sstable(.{
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
    try std.testing.expectEqual(@as(usize, 3), m.count_at_level(0));

    const remove_ids = [_]u64{ 1, 2, 3 };
    try m.atomic_compaction(&remove_ids, .{
        .id = 4,
        .level = 1,
        .min_key = "a",
        .max_key = "z",
        .num_entries = 300,
        .min_seq = 1,
        .max_seq = 100,
        .file_size = 12000,
    });

    try std.testing.expectEqual(@as(usize, 0), m.count_at_level(0));
    try std.testing.expectEqual(@as(usize, 1), m.count_at_level(1));
    try std.testing.expectEqual(@as(u32, 1), m.live.count);
    try std.testing.expectEqual(@as(u64, 4), m.live.items_const()[0].id);
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

        try m.add_sstable(.{
            .id = 1,
            .level = 0,
            .min_key = "a",
            .max_key = "m",
            .num_entries = 50,
            .min_seq = 1,
            .max_seq = 50,
            .file_size = 2000,
        });
        try m.add_sstable(.{
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
        try m.atomic_compaction(&remove, .{
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

        try std.testing.expectEqual(@as(u32, 1), m.live.count);
        try std.testing.expectEqual(@as(u64, 3), m.live.items_const()[0].id);
        try std.testing.expectEqual(@as(u32, 1), m.live.items_const()[0].level);
    }
}

test "manifest rejects complete corruption and truncates a torn transaction tail" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const corrupt_path = "data/test_manifest_checksum.manifest";
    IoDir.cwd().deleteFile(io, corrupt_path) catch {};
    defer IoDir.cwd().deleteFile(io, corrupt_path) catch {};
    {
        var manifest = try Manifest.init(allocator, corrupt_path, io);
        defer manifest.deinit();
        try manifest.add_sstable(.{
            .id = 1,
            .level = 0,
            .min_key = "a",
            .max_key = "z",
            .num_entries = 1,
            .min_seq = 1,
            .max_seq = 1,
            .file_size = 100,
        });
    }
    {
        const file = try IoDir.cwd().openFile(io, corrupt_path, .{ .mode = .read_write });
        defer file.close(io);
        var byte: [1]u8 = undefined;
        var iovecs = [1][]u8{&byte};
        _ = try file.readPositional(io, &iovecs, 4);
        byte[0] ^= 1;
        try file.writePositionalAll(io, &byte, 4);
        try file.sync(io);
    }
    try std.testing.expectError(
        error.ManifestChecksumMismatch,
        Manifest.init(allocator, corrupt_path, io),
    );

    const torn_path = "data/test_manifest_torn.manifest";
    IoDir.cwd().deleteFile(io, torn_path) catch {};
    defer IoDir.cwd().deleteFile(io, torn_path) catch {};
    var committed_size: u64 = 0;
    {
        var manifest = try Manifest.init(allocator, torn_path, io);
        defer manifest.deinit();
        try manifest.add_sstable(.{
            .id = 1,
            .level = 0,
            .min_key = "a",
            .max_key = "m",
            .num_entries = 1,
            .min_seq = 1,
            .max_seq = 1,
            .file_size = 100,
        });
        try manifest.add_sstable(.{
            .id = 2,
            .level = 0,
            .min_key = "n",
            .max_key = "z",
            .num_entries = 1,
            .min_seq = 2,
            .max_seq = 2,
            .file_size = 100,
        });
        committed_size = manifest.write_pos;
    }
    {
        const file = try IoDir.cwd().openFile(io, torn_path, .{ .mode = .read_write });
        defer file.close(io);
        try file.writePositionalAll(io, "COMPACT 3 2 1", committed_size);
        try file.sync(io);
    }
    {
        var manifest = try Manifest.init(allocator, torn_path, io);
        defer manifest.deinit();
        try std.testing.expectEqual(@as(u32, 2), manifest.live.count);
        try std.testing.expectEqual(@as(u64, 2), manifest.version);
        try std.testing.expectEqual(committed_size, (try manifest.file.?.stat(io)).size);
    }
}

test "manifest publication faults expose only the old or complete new generation" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const points = [_]FaultPoint{
        .before_copy,
        .after_copy,
        .after_record_write,
        .after_file_sync,
        .after_replace,
        .after_directory_sync,
    };
    for (points, 0..) |point, point_index| {
        var path_buffer: [128]u8 = undefined;
        const path = try std.fmt.bufPrint(
            &path_buffer,
            "data/test_manifest_fault_{d}.manifest",
            .{point_index},
        );
        IoDir.cwd().deleteFile(io, path) catch {};
        defer IoDir.cwd().deleteFile(io, path) catch {};
        {
            var manifest = try Manifest.init(allocator, path, io);
            defer manifest.deinit();
            try manifest.add_sstable(.{
                .id = 1,
                .level = 0,
                .min_key = "a",
                .max_key = "m",
                .num_entries = 1,
                .min_seq = 1,
                .max_seq = 1,
                .file_size = 100,
            });
            var injector: FaultInjector = .{ .fail_at = point };
            manifest.set_fault_injector(&injector);
            try std.testing.expectError(error.InjectedManifestFault, manifest.add_sstable(.{
                .id = 2,
                .level = 0,
                .min_key = "n",
                .max_key = "z",
                .num_entries = 1,
                .min_seq = 2,
                .max_seq = 2,
                .file_size = 100,
            }));
            try std.testing.expect(injector.triggered);
        }
        var recovered = try Manifest.init(allocator, path, io);
        defer recovered.deinit();
        const replaced = point == .after_replace or point == .after_directory_sync;
        try std.testing.expectEqual(
            @as(limits.SSTableCount, if (replaced) 2 else 1),
            recovered.live.count,
        );
        try std.testing.expectEqual(
            @as(u64, if (replaced) 2 else 1),
            recovered.version,
        );
    }
}
