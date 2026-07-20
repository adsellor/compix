const std = @import("std");
const assert = std.debug.assert;
const event = @import("event.zig");
const EventValue = event.EventValue;

pub const key_prefix = "_bc:";
pub const breadcrumb_event_type = "_breadcrumb";

pub const Breadcrumb = struct {
    source_service: []const u8,
    event_type: []const u8,
    last_sequence: u64,
    peer_address: []const u8,
    updated_at: i64,

    pub fn make_key_into(
        buffer: []u8,
        source_service: []const u8,
        event_type: []const u8,
    ) ![]const u8 {
        assert(source_service.len > 0);
        assert(std.mem.indexOfScalar(u8, source_service, ':') == null);
        return std.fmt.bufPrint(buffer, "{s}{s}:{s}", .{
            key_prefix,
            source_service,
            event_type,
        }) catch return error.BufferTooSmall;
    }

    pub fn is_system_key(key: []const u8) bool {
        return std.mem.startsWith(u8, key, key_prefix);
    }

    pub fn serialize_payload_into(self: *const Breadcrumb, buffer: []u8) ![]const u8 {
        return std.fmt.bufPrint(buffer, "{}|{s}", .{
            self.last_sequence,
            self.peer_address,
        }) catch return error.BufferTooSmall;
    }

    const ParsedPayload = struct { last_sequence: u64, peer_address: []const u8 };

    pub fn deserialize_payload_borrowed(payload: []const u8) !ParsedPayload {
        var iter = std.mem.splitSequence(u8, payload, "|");
        const sequence_string = iter.next() orelse return error.InvalidFormat;
        return .{
            .last_sequence = try std.fmt.parseInt(u64, sequence_string, 10),
            .peer_address = iter.rest(),
        };
    }

    pub fn from_wal_entry_borrowed(
        key: []const u8,
        value: EventValue,
    ) !?Breadcrumb {
        if (!is_system_key(key)) return null;

        const after_prefix = key[key_prefix.len..];
        const colon_pos = std.mem.indexOfScalar(u8, after_prefix, ':') orelse
            return error.InvalidFormat;
        const source_service = after_prefix[0..colon_pos];
        const event_type_from_key = after_prefix[colon_pos + 1 ..];

        const parsed = try deserialize_payload_borrowed(value.payload);
        return .{
            .source_service = source_service,
            .event_type = event_type_from_key,
            .last_sequence = parsed.last_sequence,
            .peer_address = parsed.peer_address,
            .updated_at = value.timestamp,
        };
    }
};

test "make_key produces correct format" {
    var buffer: [128]u8 = undefined;
    const key = try Breadcrumb.make_key_into(&buffer, "order-service", "order.created");
    try std.testing.expectEqualStrings("_bc:order-service:order.created", key);
}

test "is_system_key correctly identifies breadcrumb keys" {
    try std.testing.expect(Breadcrumb.is_system_key("_bc:order-service:order.created"));
    try std.testing.expect(Breadcrumb.is_system_key("_bc:"));
    try std.testing.expect(!Breadcrumb.is_system_key("user-service:u123:profile"));
    try std.testing.expect(!Breadcrumb.is_system_key(""));
    try std.testing.expect(!Breadcrumb.is_system_key("_b"));
}

test "to_event_value and from_wal_entry round-trip" {
    const bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 42,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 1700000000,
    };

    var key_buffer: [128]u8 = undefined;
    const key = try Breadcrumb.make_key_into(&key_buffer, bc.source_service, bc.event_type);
    var payload_buffer: [128]u8 = undefined;
    const payload = try bc.serialize_payload_into(&payload_buffer);
    const ev = EventValue{
        .sequence = 100,
        .event_type = breadcrumb_event_type,
        .payload = payload,
        .timestamp = 1700000000,
        .origin_service = "my-service",
    };

    try std.testing.expectEqualStrings(breadcrumb_event_type, ev.event_type);
    try std.testing.expectEqualStrings("my-service", ev.origin_service);

    const maybe_bc = try Breadcrumb.from_wal_entry_borrowed(key, ev);
    try std.testing.expect(maybe_bc != null);

    const restored = maybe_bc.?;

    try std.testing.expectEqualStrings("order-service", restored.source_service);
    try std.testing.expectEqualStrings("order.created", restored.event_type);
    try std.testing.expectEqual(@as(u64, 42), restored.last_sequence);
    try std.testing.expectEqualStrings("10.0.0.5:4200", restored.peer_address);
    try std.testing.expectEqual(@as(i64, 1700000000), restored.updated_at);
}

test "from_wal_entry returns null for non-breadcrumb keys" {
    const ev = EventValue{
        .sequence = 1,
        .event_type = "user.registered",
        .payload = "{}",
        .timestamp = 100,
        .origin_service = "test-service",
    };
    const result = try Breadcrumb.from_wal_entry_borrowed("user-service:u123:profile", ev);
    try std.testing.expect(result == null);
}

test "serialize_payload/deserialize_payload round-trip" {
    const bc = Breadcrumb{
        .source_service = "svc",
        .event_type = "evt",
        .last_sequence = 99,
        .peer_address = "192.168.1.1:5000",
        .updated_at = 0,
    };

    var buffer: [128]u8 = undefined;
    const payload = try bc.serialize_payload_into(&buffer);
    const parsed = try Breadcrumb.deserialize_payload_borrowed(payload);

    try std.testing.expectEqual(@as(u64, 99), parsed.last_sequence);
    try std.testing.expectEqualStrings("192.168.1.1:5000", parsed.peer_address);
}
