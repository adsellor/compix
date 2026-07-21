const std = @import("std");
const assert = std.debug.assert;
const event = @import("event.zig");
const EventValue = event.EventValue;

pub const key_prefix = "_bc:";
pub const breadcrumb_event_type = "_breadcrumb";

/// A breadcrumb is a per-(source_service, event_type, peer_address) high-water
/// mark: the last sequence this node has pulled for one remote stream from one
/// peer. Keying on the peer address lets a node track catch-up progress against
/// several peers that can each serve the same logical stream, without any of
/// them being authoritative here — the payload stays at its owner.
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
        peer_address: []const u8,
    ) ![]const u8 {
        assert(source_service.len > 0);
        assert(peer_address.len > 0);
        assert(std.mem.indexOfScalar(u8, source_service, ':') == null);
        assert(std.mem.indexOfScalar(u8, event_type, ':') == null);
        // peer_address is the trailing component and may itself contain ':'
        // (host:port), so it is parsed back as "the rest" of the key.
        return std.fmt.bufPrint(buffer, "{s}{s}:{s}:{s}", .{
            key_prefix,
            source_service,
            event_type,
            peer_address,
        }) catch return error.BufferTooSmall;
    }

    pub fn is_system_key(key: []const u8) bool {
        return std.mem.startsWith(u8, key, key_prefix);
    }

    pub fn serialize_payload_into(self: *const Breadcrumb, buffer: []u8) ![]const u8 {
        return std.fmt.bufPrint(buffer, "{}", .{self.last_sequence}) catch
            return error.BufferTooSmall;
    }

    pub fn deserialize_sequence(payload: []const u8) !u64 {
        return std.fmt.parseInt(u64, payload, 10) catch return error.InvalidFormat;
    }

    pub fn from_wal_entry_borrowed(
        key: []const u8,
        value: EventValue,
    ) !?Breadcrumb {
        if (!is_system_key(key)) return null;

        const after_prefix = key[key_prefix.len..];
        const service_end = std.mem.indexOfScalar(u8, after_prefix, ':') orelse
            return error.InvalidFormat;
        const source_service = after_prefix[0..service_end];
        const after_service = after_prefix[service_end + 1 ..];
        const type_end = std.mem.indexOfScalar(u8, after_service, ':') orelse
            return error.InvalidFormat;
        const event_type_from_key = after_service[0..type_end];
        const peer_address = after_service[type_end + 1 ..];
        if (peer_address.len == 0) return error.InvalidFormat;

        return .{
            .source_service = source_service,
            .event_type = event_type_from_key,
            .last_sequence = try deserialize_sequence(value.payload),
            .peer_address = peer_address,
            .updated_at = value.timestamp,
        };
    }
};

test "make_key produces correct format" {
    var buffer: [128]u8 = undefined;
    const key = try Breadcrumb.make_key_into(
        &buffer,
        "order-service",
        "order.created",
        "10.0.0.5:4200",
    );
    try std.testing.expectEqualStrings("_bc:order-service:order.created:10.0.0.5:4200", key);
}

test "is_system_key correctly identifies breadcrumb keys" {
    try std.testing.expect(Breadcrumb.is_system_key("_bc:order-service:order.created:10.0.0.5:4200"));
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
    const key = try Breadcrumb.make_key_into(
        &key_buffer,
        bc.source_service,
        bc.event_type,
        bc.peer_address,
    );
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

test "serialize_payload/deserialize_sequence round-trip" {
    const bc = Breadcrumb{
        .source_service = "svc",
        .event_type = "evt",
        .last_sequence = 99,
        .peer_address = "192.168.1.1:5000",
        .updated_at = 0,
    };

    var buffer: [128]u8 = undefined;
    const payload = try bc.serialize_payload_into(&buffer);
    try std.testing.expectEqual(@as(u64, 99), try Breadcrumb.deserialize_sequence(payload));
}

test "distinct peers keep independent high-water marks for one stream" {
    var buffer_a: [128]u8 = undefined;
    var buffer_b: [128]u8 = undefined;
    const key_a = try Breadcrumb.make_key_into(&buffer_a, "orders", "created", "10.0.0.1:4200");
    const key_b = try Breadcrumb.make_key_into(&buffer_b, "orders", "created", "10.0.0.2:4200");
    try std.testing.expect(!std.mem.eql(u8, key_a, key_b));
}
