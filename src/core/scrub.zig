const std = @import("std");
const limits = @import("limits.zig");
const store_mod = @import("store.zig");

const Allocator = std.mem.Allocator;
const EventStore = store_mod.EventStore;
const Io = std.Io;
const IoDir = Io.Dir;
const orphan_candidates_max: u16 = 4096;

pub const Summary = struct {
    live_sstables: u16,
    candidates: u16,
    quarantined: u16,
};

const TableName = struct {
    level: u32,
    id: u64,
    temporary: bool = false,
};

/// Scrub must only run on a verified store; callers state who verified it.
pub const Verified = enum { already_verified, verify_now };

pub fn run(
    allocator: Allocator,
    io: Io,
    wal_path: []const u8,
    store: *EventStore,
    verified: Verified,
) !Summary {
    if (verified == .verify_now) _ = try store.verify_integrity();
    const base = try derive_base(allocator, wal_path);
    defer allocator.free(base);
    const parent_path = std.fs.path.dirname(base) orelse ".";
    const base_name = std.fs.path.basename(base);
    const quarantine_path = try std.fmt.allocPrint(allocator, "{s}.quarantine", .{base});
    defer allocator.free(quarantine_path);

    var parent = try IoDir.cwd().openDir(io, parent_path, .{ .iterate = true });
    defer parent.close(io);
    var quarantine: ?IoDir = null;
    defer if (quarantine) |dir| dir.close(io);
    var summary: Summary = .{
        .live_sstables = store.lsm.sstable_count(),
        .candidates = 0,
        .quarantined = 0,
    };
    var iterator = parent.iterate();
    while (try iterator.next(io)) |entry| {
        if (entry.kind == .directory) continue;
        const table = parse_table_name(base_name, entry.name) orelse continue;
        summary.candidates += 1;
        if (summary.candidates > orphan_candidates_max) return error.TooManyOrphanCandidates;
        if (!table.temporary and is_live(store, table)) continue;

        if (quarantine == null) {
            IoDir.cwd().createDir(io, quarantine_path, .default_dir) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
            quarantine = try IoDir.cwd().openDir(io, quarantine_path, .{});
        }
        try parent.renamePreserve(entry.name, quarantine.?, entry.name, io);
        summary.quarantined += 1;
    }
    if (summary.quarantined > 0) {
        try sync_directory(parent_path, io);
        try sync_directory(quarantine_path, io);
    }
    return summary;
}

fn is_live(store: *const EventStore, table: TableName) bool {
    for (store.lsm.manifest.live.items_const()) |live| {
        if (live.level == table.level and live.id == table.id) return true;
    }
    return false;
}

fn parse_table_name(base_name: []const u8, name: []const u8) ?TableName {
    if (!std.mem.startsWith(u8, name, base_name)) return null;
    const temporary = std.mem.endsWith(u8, name, ".sst.tmp");
    const canonical_name = if (temporary) name[0 .. name.len - ".tmp".len] else name;
    const suffix = canonical_name[base_name.len..];
    if (!std.mem.startsWith(u8, suffix, "_L") or
        !std.mem.endsWith(u8, suffix, ".sst"))
        return null;
    const body = suffix[2 .. suffix.len - ".sst".len];
    const separator = std.mem.indexOfScalar(u8, body, '_') orelse return null;
    if (separator == 0 or separator + 1 == body.len) return null;
    const level = std.fmt.parseInt(u32, body[0..separator], 10) catch return null;
    const id = std.fmt.parseInt(u64, body[separator + 1 ..], 10) catch return null;
    if (id == 0) return null;
    var canonical_buffer: [IoDir.max_name_bytes]u8 = undefined;
    const canonical = std.fmt.bufPrint(
        &canonical_buffer,
        "{s}_L{d}_{d:0>8}.sst",
        .{ base_name, level, id },
    ) catch return null;
    if (!std.mem.eql(u8, canonical, canonical_name)) return null;
    return .{ .level = level, .id = id, .temporary = temporary };
}

fn derive_base(allocator: Allocator, wal_path: []const u8) ![]u8 {
    const base = if (std.mem.endsWith(u8, wal_path, ".wal"))
        wal_path[0 .. wal_path.len - 4]
    else
        wal_path;
    return allocator.dupe(u8, base);
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

test "table name parsing is strict" {
    try std.testing.expectEqual(
        TableName{ .level = 2, .id = 42 },
        parse_table_name("events", "events_L2_00000042.sst").?,
    );
    try std.testing.expect(parse_table_name("events", "events_L2_42.sst") == null);
    try std.testing.expect(parse_table_name("events", "other_L2_00000042.sst") == null);
    try std.testing.expect(parse_table_name("events", "events_Lx_00000042.sst") == null);
}

fn cleanup_test(io: Io) void {
    inline for (.{
        "data/test_scrub.wal",
        "data/test_scrub.crumb",
        "data/test_scrub.manifest",
        "data/test_scrub_L0_00000001.sst",
        "data/test_scrub_L0_00000099.sst",
        "data/test_scrub_L0_00000100.sst.tmp",
    }) |path| IoDir.cwd().deleteFile(io, path) catch {};
    IoDir.cwd().deleteTree(io, "data/test_scrub.quarantine") catch {};
}

test "scrub quarantines unreferenced tables and preserves live tables" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    cleanup_test(io);
    defer cleanup_test(io);

    var store = try EventStore.init(allocator, "data/test_scrub.wal", io, .{});
    defer store.deinit();
    try store.put("key", .{
        .sequence = 1,
        .event_type = "type",
        .payload = "payload",
        .timestamp = 1,
        .origin_service = "service",
    });
    try store.lsm.flush();
    const orphan = try IoDir.cwd().createFile(io, "data/test_scrub_L0_00000099.sst", .{
        .exclusive = true,
    });
    try orphan.writePositionalAll(io, "orphan", 0);
    try orphan.sync(io);
    orphan.close(io);
    const temporary = try IoDir.cwd().createFile(
        io,
        "data/test_scrub_L0_00000100.sst.tmp",
        .{ .exclusive = true },
    );
    try temporary.writePositionalAll(io, "temporary", 0);
    try temporary.sync(io);
    temporary.close(io);

    const summary = try run(allocator, io, "data/test_scrub.wal", &store, .verify_now);
    try std.testing.expectEqual(@as(u16, 1), summary.live_sstables);
    try std.testing.expectEqual(@as(u16, 3), summary.candidates);
    try std.testing.expectEqual(@as(u16, 2), summary.quarantined);
    const moved = try IoDir.cwd().openFile(
        io,
        "data/test_scrub.quarantine/test_scrub_L0_00000099.sst",
        .{ .mode = .read_only },
    );
    moved.close(io);
    const moved_temporary = try IoDir.cwd().openFile(
        io,
        "data/test_scrub.quarantine/test_scrub_L0_00000100.sst.tmp",
        .{ .mode = .read_only },
    );
    moved_temporary.close(io);
    const live = try IoDir.cwd().openFile(
        io,
        "data/test_scrub_L0_00000001.sst",
        .{ .mode = .read_only },
    );
    live.close(io);
    try std.testing.expectError(
        error.FileNotFound,
        IoDir.cwd().openFile(io, "data/test_scrub_L0_00000099.sst", .{ .mode = .read_only }),
    );
}
