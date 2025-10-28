const std = @import("std");

pub const EventValue = struct {
    sequence: u64,
    event_type: []const u8,
    payload: []const u8,
    timestamp: i64,

    pub fn init(
        allocator: std.mem.Allocator,
        sequence: u64,
        event_type: []const u8,
        payload: []const u8,
        timestamp: i64,
    ) !EventValue {
        return EventValue{
            .sequence = sequence,
            .event_type = try allocator.dupe(u8, event_type),
            .payload = try allocator.dupe(u8, payload),
            .timestamp = timestamp,
        };
    }

    pub fn serialize(self: *const EventValue, allocator: std.mem.Allocator) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{}|{}|{}|{s}|{s}", .{ self.sequence, self.timestamp, self.payload.len, self.event_type, self.payload });
    }

    pub fn deserialize(allocator: std.mem.Allocator, data: []const u8) !EventValue {
        var iter = std.mem.splitSequence(u8, data, "|");
        const sequence = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
        const timestamp = try std.fmt.parseInt(i64, iter.next() orelse return error.InvalidFormat, 10);
        const payload_len = try std.fmt.parseInt(usize, iter.next() orelse return error.InvalidFormat, 10);
        _ = payload_len;
        const event_type = iter.next() orelse return error.InvalidFormat;
        const payload = iter.next() orelse return error.InvalidFormat;

        return EventValue{
            .sequence = sequence,
            .event_type = try allocator.dupe(u8, event_type),
            .payload = try allocator.dupe(u8, payload),
            .timestamp = timestamp,
        };
    }

    pub fn deinit(self: *EventValue, allocator: std.mem.Allocator) void {
        allocator.free(self.event_type);
        allocator.free(self.payload);
    }
};

pub const KeyValuePair = struct {
    key: []const u8,
    value: EventValue,

    pub fn deinit(self: *KeyValuePair, allocator: std.mem.Allocator) void {
        allocator.free(self.key);
        self.value.deinit(allocator);
    }
};
