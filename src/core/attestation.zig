const std = @import("std");
const Io = std.Io;
const IoDir = Io.Dir;

pub const suffix = ".verified";

const magic: u32 = 0x43505856;
const body_size: usize = 24;
const checksum_size: usize = 16;
pub const encoded_size: usize = body_size + checksum_size;

pub const Attestation = struct {
    manifest_version: u64,
    committed_log_position: u64,
    streams: u32,

    pub fn serialize(self: Attestation) [encoded_size]u8 {
        var bytes: [encoded_size]u8 = undefined;
        std.mem.writeInt(u32, bytes[0..4], magic, .little);
        std.mem.writeInt(u64, bytes[4..12], self.manifest_version, .little);
        std.mem.writeInt(u64, bytes[12..20], self.committed_log_position, .little);
        std.mem.writeInt(u32, bytes[20..24], self.streams, .little);
        const digest = checksum(bytes[0..body_size]);
        @memcpy(bytes[body_size..encoded_size], &digest);
        return bytes;
    }

    pub fn deserialize(bytes: [encoded_size]u8) !Attestation {
        if (std.mem.readInt(u32, bytes[0..4], .little) != magic) return error.InvalidFormat;
        const expected = checksum(bytes[0..body_size]);
        const stored: [checksum_size]u8 = bytes[body_size..encoded_size].*;
        if (!std.crypto.timing_safe.eql([checksum_size]u8, expected, stored))
            return error.InvalidFormat;
        return .{
            .manifest_version = std.mem.readInt(u64, bytes[4..12], .little),
            .committed_log_position = std.mem.readInt(u64, bytes[12..20], .little),
            .streams = std.mem.readInt(u32, bytes[20..24], .little),
        };
    }

    pub fn covers(self: Attestation, manifest_version: u64, committed_log_position: u64) bool {
        return self.manifest_version == manifest_version and
            self.committed_log_position == committed_log_position;
    }
};

fn checksum(content: []const u8) [checksum_size]u8 {
    var digest: [32]u8 = undefined;
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("compix/attestation/v1");
    hasher.update(content);
    hasher.final(&digest);
    return digest[0..checksum_size].*;
}

pub fn derive_path_into(buffer: []u8, wal_path: []const u8) ![]const u8 {
    const base = if (std.mem.endsWith(u8, wal_path, ".wal"))
        wal_path[0 .. wal_path.len - 4]
    else
        wal_path;
    return std.fmt.bufPrint(buffer, "{s}{s}", .{ base, suffix }) catch
        return error.PathTooLong;
}

pub fn save_path(path: []const u8, value: Attestation, io: Io) !void {
    const file = try IoDir.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    const bytes = value.serialize();
    try file.writePositionalAll(io, &bytes, 0);
    try file.sync(io);
}

pub fn load_path(path: []const u8, io: Io) !?Attestation {
    const file = IoDir.cwd().openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer file.close(io);
    var bytes: [encoded_size]u8 = undefined;
    const read = file.readPositionalAll(io, &bytes, 0) catch return null;
    if (read != bytes.len) return null;
    return Attestation.deserialize(bytes) catch null;
}

pub fn remove_path(path: []const u8, io: Io) void {
    IoDir.cwd().deleteFile(io, path) catch {};
}

const testing = std.testing;

test "an attestation round-trips through its encoding" {
    const value: Attestation = .{
        .manifest_version = 4242,
        .committed_log_position = 5485656,
        .streams = 273243,
    };
    const restored = try Attestation.deserialize(value.serialize());
    try testing.expectEqual(value.manifest_version, restored.manifest_version);
    try testing.expectEqual(value.committed_log_position, restored.committed_log_position);
    try testing.expectEqual(value.streams, restored.streams);
}

test "a flipped bit anywhere in the record is refused" {
    const value: Attestation = .{
        .manifest_version = 7,
        .committed_log_position = 9,
        .streams = 3,
    };
    for (0..encoded_size) |index| {
        var bytes = value.serialize();
        bytes[index] ^= 0x40;
        try testing.expectError(error.InvalidFormat, Attestation.deserialize(bytes));
    }
}

test "coverage requires both the table set and the log position to match" {
    const value: Attestation = .{
        .manifest_version = 12,
        .committed_log_position = 900,
        .streams = 5,
    };
    try testing.expect(value.covers(12, 900));
    try testing.expect(!value.covers(13, 900));
    try testing.expect(!value.covers(12, 901));
    try testing.expect(!value.covers(11, 899));
}

test "the path sits beside the wal rather than inside it" {
    var buffer: [64]u8 = undefined;
    try testing.expectEqualStrings(
        "data/events.verified",
        try derive_path_into(&buffer, "data/events.wal"),
    );
    try testing.expectEqualStrings(
        "data/events.verified",
        try derive_path_into(&buffer, "data/events"),
    );
    var tiny: [4]u8 = undefined;
    try testing.expectError(error.PathTooLong, derive_path_into(&tiny, "data/events.wal"));
}

test "a missing or truncated file reads as no attestation rather than an error" {
    const io = Io.Threaded.global_single_threaded.io();
    const path = "data/test_attestation_partial.verified";
    IoDir.cwd().deleteFile(io, path) catch {};
    try testing.expectEqual(@as(?Attestation, null), try load_path(path, io));

    const value: Attestation = .{ .manifest_version = 1, .committed_log_position = 2, .streams = 3 };
    try save_path(path, value, io);
    defer IoDir.cwd().deleteFile(io, path) catch {};
    try testing.expect((try load_path(path, io)) != null);

    const file = try IoDir.cwd().createFile(io, path, .{ .truncate = true });
    try file.writePositionalAll(io, value.serialize()[0..8], 0);
    file.close(io);
    try testing.expectEqual(@as(?Attestation, null), try load_path(path, io));
}
