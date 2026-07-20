const std = @import("std");
const assert = std.debug.assert;
const limits = @import("limits.zig");

pub const EventValue = struct {
    sequence: limits.Sequence,
    event_type: []const u8,
    payload: []const u8,
    timestamp: limits.Timestamp,
    origin_service: []const u8,

    pub const InitOptions = struct {
        sequence: limits.Sequence,
        origin_service: []const u8,
        event_type: []const u8,
        payload: []const u8,
        timestamp: limits.Timestamp,
    };

    pub fn init(options: InitOptions) !EventValue {
        if (options.origin_service.len > limits.service_name_length_max)
            return error.ServiceNameTooLong;
        if (options.event_type.len > limits.event_type_length_max)
            return error.EventTypeTooLong;
        if (options.payload.len > limits.payload_size_max) return error.PayloadTooLarge;

        return .{
            .sequence = options.sequence,
            .event_type = options.event_type,
            .payload = options.payload,
            .timestamp = options.timestamp,
            .origin_service = options.origin_service,
        };
    }

    pub fn serialize_into(self: *const EventValue, buffer: []u8) ![]const u8 {
        assert(std.mem.indexOfScalar(u8, self.origin_service, '|') == null);
        assert(std.mem.indexOfScalar(u8, self.event_type, '|') == null);
        return std.fmt.bufPrint(buffer, "{}|{}|{}|{s}|{s}|{s}", .{
            self.sequence,
            self.timestamp,
            self.payload.len,
            self.origin_service,
            self.event_type,
            self.payload,
        }) catch return error.BufferTooSmall;
    }

    pub fn serialized_size(self: *const EventValue) limits.ByteCount {
        const size = std.fmt.count("{}|{}|{}|{s}|{s}|{s}", .{
            self.sequence,
            self.timestamp,
            self.payload.len,
            self.origin_service,
            self.event_type,
            self.payload,
        });
        return limits.byte_count(size) catch unreachable;
    }

    /// Decode an event as views into `data`. The result owns no memory and is
    /// valid only while `data` remains unchanged.
    pub fn deserialize_borrowed(data: []const u8) !EventValue {
        var iter = std.mem.splitSequence(u8, data, "|");
        const sequence_str = iter.next() orelse return error.InvalidFormat;
        const sequence = try std.fmt.parseInt(u64, sequence_str, 10);
        const timestamp_str = iter.next() orelse return error.InvalidFormat;
        const timestamp = try std.fmt.parseInt(i64, timestamp_str, 10);
        const payload_len_str = iter.next() orelse return error.InvalidFormat;
        const payload_len = try std.fmt.parseInt(limits.ByteCount, payload_len_str, 10);
        const origin_service = iter.next() orelse return error.InvalidFormat;
        const event_type = iter.next() orelse return error.InvalidFormat;
        const payload_raw = iter.rest();
        const payload = if (payload_raw.len > 0) payload_raw else "";

        if (payload.len != limits.usize_from(payload_len)) return error.InvalidFormat;

        if (origin_service.len > limits.service_name_length_max)
            return error.ServiceNameTooLong;
        if (event_type.len > limits.event_type_length_max)
            return error.EventTypeTooLong;
        if (payload.len > limits.payload_size_max) return error.PayloadTooLarge;

        return .{
            .sequence = sequence,
            .origin_service = origin_service,
            .event_type = event_type,
            .payload = payload,
            .timestamp = timestamp,
        };
    }
};

pub const VisitControl = enum {
    continue_scan,
    stop,
};

pub const KeyValuePair = struct {
    key: []const u8,
    value: EventValue,
};

fn test_event_value(
    sequence: limits.Sequence,
    origin_service: []const u8,
    event_type: []const u8,
    payload: []const u8,
    timestamp: limits.Timestamp,
) !EventValue {
    return try EventValue.init(.{
        .sequence = sequence,
        .origin_service = origin_service,
        .event_type = event_type,
        .payload = payload,
        .timestamp = timestamp,
    });
}

test "serialize/deserialize round-trip with origin_service" {
    const ev = try test_event_value(
        42,
        "order-service",
        "order.created",
        "{\"id\":123}",
        1700000000,
    );
    var buffer: [1024]u8 = undefined;
    const serialized = try ev.serialize_into(&buffer);
    const deserialized = try EventValue.deserialize_borrowed(serialized);

    try std.testing.expectEqual(@as(u64, 42), deserialized.sequence);
    try std.testing.expectEqual(@as(i64, 1700000000), deserialized.timestamp);
    try std.testing.expectEqualStrings("order-service", deserialized.origin_service);
    try std.testing.expectEqualStrings("order.created", deserialized.event_type);
    try std.testing.expectEqualStrings("{\"id\":123}", deserialized.payload);
}

test "payload containing pipe characters deserializes correctly" {
    const ev = try test_event_value(1, "test-service", "test.event", "a|b|c", 100);
    var buffer: [1024]u8 = undefined;
    const serialized = try ev.serialize_into(&buffer);
    const deserialized = try EventValue.deserialize_borrowed(serialized);

    try std.testing.expectEqualStrings("a|b|c", deserialized.payload);
    try std.testing.expectEqualStrings("test.event", deserialized.event_type);
    try std.testing.expectEqualStrings("test-service", deserialized.origin_service);
}

test "empty payload round-trip" {
    const ev = try test_event_value(5, "svc", "evt", "", 999);
    var buffer: [1024]u8 = undefined;
    const serialized = try ev.serialize_into(&buffer);
    const deserialized = try EventValue.deserialize_borrowed(serialized);

    try std.testing.expectEqualStrings("", deserialized.payload);
    try std.testing.expectEqualStrings("evt", deserialized.event_type);
    try std.testing.expectEqualStrings("svc", deserialized.origin_service);
}
