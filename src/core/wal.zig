const std = @import("std");
const radix = @import("radix.zig");
const event = @import("event.zig");
const EventValue = event.EventValue;
const RadixTree = radix.RadixTree;

pub const WriteAheadLog = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, file_path: []const u8) !WriteAheadLog {
        std.fs.cwd().makeDir("data") catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try std.fs.cwd().createFile(file_path, .{ .truncate = false });
        return WriteAheadLog{
            .file = file,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *WriteAheadLog) void {
        self.file.close();
    }

    pub fn append_event(self: *WriteAheadLog, key: []const u8, value: EventValue) !void {
        const serialized_value = try value.serialize(self.allocator);
        defer self.allocator.free(serialized_value);

        var key_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &key_len_bytes, @intCast(key.len), .little);
        try self.file.writeAll(&key_len_bytes);
        try self.file.writeAll(key);

        var value_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &value_len_bytes, @intCast(serialized_value.len), .little);
        try self.file.writeAll(&value_len_bytes);
        try self.file.writeAll(serialized_value);
        try self.file.writeAll("\n");
        try self.file.sync();
    }

    // TODO: this is not optimal, the replay can be better,
    // I should take into account that replay should be done with crumbs
    // crumbs can help to resolve other service state
    // so the replay should be pausable, resumable, and should pick up at the point left off
    pub fn replay(self: *WriteAheadLog, radix_tree: *RadixTree) !void {
        try self.file.seekTo(0);

        while (true) {
            var key_len_bytes: [4]u8 = undefined;
            _ = self.file.readAll(&key_len_bytes) catch break;
            const key_len = std.mem.readInt(u32, &key_len_bytes, .little);

            const key_buf = try self.allocator.alloc(u8, key_len);
            defer self.allocator.free(key_buf);
            _ = self.file.readAll(key_buf) catch break;

            var value_len_bytes: [4]u8 = undefined;
            _ = self.file.readAll(&value_len_bytes) catch break;
            const value_len = std.mem.readInt(u32, &value_len_bytes, .little);

            const value_buf = try self.allocator.alloc(u8, value_len);
            defer self.allocator.free(value_buf);
            _ = self.file.readAll(value_buf) catch break;

            var newline: [1]u8 = undefined;
            _ = self.file.readAll(&newline) catch break;

            const value = try EventValue.deserialize(self.allocator, value_buf);
            try radix_tree.insert(key_buf, value);
        }
    }
};
