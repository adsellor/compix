const std = @import("std");

pub const EventValue = struct {
    sequence: u64,
    event_type: []const u8,
    payload: []const u8,
    timestamp: i64,
    origin_service: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        sequence: u64,
        origin_service: []const u8,
        event_type: []const u8,
        payload: []const u8,
        timestamp: i64,
    ) !EventValue {
        return EventValue{
            .sequence = sequence,
            .event_type = try allocator.dupe(u8, event_type),
            .payload = try allocator.dupe(u8, payload),
            .timestamp = timestamp,
            .origin_service = try allocator.dupe(u8, origin_service),
        };
    }

    pub fn serialize(self: *const EventValue, allocator: std.mem.Allocator) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{}|{}|{}|{s}|{s}|{s}", .{
            self.sequence,
            self.timestamp,
            self.payload.len,
            self.origin_service,
            self.event_type,
            self.payload,
        });
    }

    pub fn deserialize(allocator: std.mem.Allocator, data: []const u8) !EventValue {
        var iter = std.mem.splitSequence(u8, data, "|");
        const sequence = try std.fmt.parseInt(u64, iter.next() orelse return error.InvalidFormat, 10);
        const timestamp = try std.fmt.parseInt(i64, iter.next() orelse return error.InvalidFormat, 10);
        const payload_len = try std.fmt.parseInt(usize, iter.next() orelse return error.InvalidFormat, 10);
        const origin_service = iter.next() orelse return error.InvalidFormat;
        const event_type = iter.next() orelse return error.InvalidFormat;
        const payload_raw = iter.rest();
        const payload = if (payload_raw.len > 0) payload_raw else "";

        if (payload.len != payload_len) return error.InvalidFormat;

        return EventValue{
            .sequence = sequence,
            .event_type = try allocator.dupe(u8, event_type),
            .payload = try allocator.dupe(u8, payload),
            .timestamp = timestamp,
            .origin_service = try allocator.dupe(u8, origin_service),
        };
    }

    pub fn deinit(self: *EventValue, allocator: std.mem.Allocator) void {
        allocator.free(self.event_type);
        allocator.free(self.payload);
        allocator.free(self.origin_service);
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

test "serialize/deserialize round-trip with origin_service" {
    const allocator = std.testing.allocator;

    var ev = try EventValue.init(allocator, 42, "order-service", "order.created", "{\"id\":123}", 1700000000);
    defer ev.deinit(allocator);

    const serialized = try ev.serialize(allocator);
    defer allocator.free(serialized);

    var deserialized = try EventValue.deserialize(allocator, serialized);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 42), deserialized.sequence);
    try std.testing.expectEqual(@as(i64, 1700000000), deserialized.timestamp);
    try std.testing.expectEqualStrings("order-service", deserialized.origin_service);
    try std.testing.expectEqualStrings("order.created", deserialized.event_type);
    try std.testing.expectEqualStrings("{\"id\":123}", deserialized.payload);
}

test "payload containing pipe characters deserializes correctly" {
    const allocator = std.testing.allocator;

    var ev = try EventValue.init(allocator, 1, "test-service", "test.event", "a|b|c", 100);
    defer ev.deinit(allocator);

    const serialized = try ev.serialize(allocator);
    defer allocator.free(serialized);

    var deserialized = try EventValue.deserialize(allocator, serialized);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqualStrings("a|b|c", deserialized.payload);
    try std.testing.expectEqualStrings("test.event", deserialized.event_type);
    try std.testing.expectEqualStrings("test-service", deserialized.origin_service);
}

test "empty payload round-trip" {
    const allocator = std.testing.allocator;

    var ev = try EventValue.init(allocator, 5, "svc", "evt", "", 999);
    defer ev.deinit(allocator);

    const serialized = try ev.serialize(allocator);
    defer allocator.free(serialized);

    var deserialized = try EventValue.deserialize(allocator, serialized);
    defer deserialized.deinit(allocator);

    try std.testing.expectEqualStrings("", deserialized.payload);
    try std.testing.expectEqualStrings("evt", deserialized.event_type);
    try std.testing.expectEqualStrings("svc", deserialized.origin_service);
}
