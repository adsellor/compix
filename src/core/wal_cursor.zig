const std = @import("std");
const Io = std.Io;
const IoDir = Io.Dir;
const IoFile = Io.File;

pub const WalCursor = struct {
    file_offset: u64,
    last_sequence: u64,

    pub fn serialize(self: WalCursor) [16]u8 {
        var bytes: [16]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], self.file_offset, .little);
        std.mem.writeInt(u64, bytes[8..16], self.last_sequence, .little);
        return bytes;
    }

    pub fn deserialize(bytes: [16]u8) WalCursor {
        return .{
            .file_offset = std.mem.readInt(u64, bytes[0..8], .little),
            .last_sequence = std.mem.readInt(u64, bytes[8..16], .little),
        };
    }
};

pub fn derive_path(allocator: std.mem.Allocator, wal_path: []const u8) ![]u8 {
    if (std.mem.endsWith(u8, wal_path, ".wal")) {
        const base = wal_path[0 .. wal_path.len - 4];
        return try std.fmt.allocPrint(allocator, "{s}.crumb", .{base});
    }
    return try std.fmt.allocPrint(allocator, "{s}.crumb", .{wal_path});
}

pub fn derive_path_into(buffer: []u8, wal_path: []const u8) ![]const u8 {
    const base = if (std.mem.endsWith(u8, wal_path, ".wal"))
        wal_path[0 .. wal_path.len - 4]
    else
        wal_path;
    return std.fmt.bufPrint(buffer, "{s}.crumb", .{base}) catch
        return error.PathTooLong;
}

pub fn save_path(path: []const u8, cursor: WalCursor, io: Io) !void {
    const dir = IoDir.cwd();
    const file = try dir.createFile(io, path, .{ .truncate = true });
    defer file.close(io);

    const bytes = cursor.serialize();
    try file.writePositionalAll(io, &bytes, 0);
}

pub fn save(
    allocator: std.mem.Allocator,
    wal_path: []const u8,
    cursor: WalCursor,
    io: Io,
) !void {
    const path = try derive_path(allocator, wal_path);
    defer allocator.free(path);

    try save_path(path, cursor, io);
}

pub fn load_path(path: []const u8, io: Io) !?WalCursor {
    const dir = IoDir.cwd();
    const file = dir.openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);

    var bytes: [16]u8 = undefined;
    const n = try file.readPositionalAll(io, &bytes, 0);
    if (n != bytes.len) return error.InvalidFormat;

    return WalCursor.deserialize(bytes);
}

pub fn load(allocator: std.mem.Allocator, wal_path: []const u8, io: Io) !?WalCursor {
    const path = try derive_path(allocator, wal_path);
    defer allocator.free(path);

    return try load_path(path, io);
}

test "serialize/deserialize round-trip" {
    const cursor = WalCursor{ .file_offset = 12345, .last_sequence = 67890 };
    const bytes = cursor.serialize();
    const restored = WalCursor.deserialize(bytes);
    try std.testing.expectEqual(@as(u64, 12345), restored.file_offset);
    try std.testing.expectEqual(@as(u64, 67890), restored.last_sequence);
}

test "save/load round-trip" {
    const allocator = std.testing.allocator;
    const io = Io.Threaded.global_single_threaded.io();

    IoDir.cwd().deleteFile(io, "data/test_wal_cursor_roundtrip.crumb") catch {};

    const cursor = WalCursor{ .file_offset = 42, .last_sequence = 100 };
    try save(allocator, "data/test_wal_cursor_roundtrip.wal", cursor, io);
    defer IoDir.cwd().deleteFile(io, "data/test_wal_cursor_roundtrip.crumb") catch {};

    const loaded = try load(allocator, "data/test_wal_cursor_roundtrip.wal", io);
    try std.testing.expect(loaded != null);
    try std.testing.expectEqual(@as(u64, 42), loaded.?.file_offset);
    try std.testing.expectEqual(@as(u64, 100), loaded.?.last_sequence);
}

test "derive_path replaces .wal with .crumb" {
    const allocator = std.testing.allocator;

    const path = try derive_path(allocator, "data/events.wal");
    defer allocator.free(path);
    try std.testing.expectEqualStrings("data/events.crumb", path);

    const path2 = try derive_path(allocator, "data/events");
    defer allocator.free(path2);
    try std.testing.expectEqualStrings("data/events.crumb", path2);
}
