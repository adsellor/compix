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

    pub fn init(
        allocator: std.mem.Allocator,
        source_service: []const u8,
        event_type: []const u8,
        last_sequence: u64,
        peer_address: []const u8,
        updated_at: i64,
    ) !Breadcrumb {
        return Breadcrumb{
            .source_service = try allocator.dupe(u8, source_service),
            .event_type = try allocator.dupe(u8, event_type),
            .last_sequence = last_sequence,
            .peer_address = try allocator.dupe(u8, peer_address),
            .updated_at = updated_at,
        };
    }

    pub fn deinit(self: *Breadcrumb, allocator: std.mem.Allocator) void {
        allocator.free(self.source_service);
        allocator.free(self.event_type);
        allocator.free(self.peer_address);
    }

    pub fn make_key(
        allocator: std.mem.Allocator,
        source_service: []const u8,
        event_type: []const u8,
    ) ![]u8 {
        assert(source_service.len > 0);
        // The key is parsed back by splitting on the first ':' after the
        // prefix, so the service name itself must not contain one.
        assert(std.mem.indexOfScalar(u8, source_service, ':') == null);
        const key = try std.fmt.allocPrint(
            allocator,
            "{s}{s}:{s}",
            .{ key_prefix, source_service, event_type },
        );
        assert(is_system_key(key));
        return key;
    }

    pub fn is_system_key(key: []const u8) bool {
        return std.mem.startsWith(u8, key, key_prefix);
    }

    pub fn serialize_payload(self: *const Breadcrumb, allocator: std.mem.Allocator) ![]u8 {
        return try std.fmt.allocPrint(
            allocator,
            "{}|{s}",
            .{ self.last_sequence, self.peer_address },
        );
    }

    const ParsedPayload = struct { last_sequence: u64, peer_address: []const u8 };

    pub fn deserialize_payload(allocator: std.mem.Allocator, payload: []const u8) !ParsedPayload {
        var iter = std.mem.splitSequence(u8, payload, "|");
        const seq_str = iter.next() orelse return error.InvalidFormat;
        const last_sequence = try std.fmt.parseInt(u64, seq_str, 10);
        const peer_address = iter.rest();
        return .{
            .last_sequence = last_sequence,
            .peer_address = try allocator.dupe(u8, peer_address),
        };
    }

    pub fn to_event_value(
        self: *const Breadcrumb,
        allocator: std.mem.Allocator,
        local_sequence: u64,
        self_service: []const u8,
        timestamp: i64,
    ) !EventValue {
        const payload = try self.serialize_payload(allocator);
        defer allocator.free(payload);
        return try EventValue.init(
            allocator,
            local_sequence,
            self_service,
            breadcrumb_event_type,
            payload,
            timestamp,
        );
    }

    pub fn from_wal_entry(
        allocator: std.mem.Allocator,
        key: []const u8,
        value: EventValue,
    ) !?Breadcrumb {
        if (!is_system_key(key)) return null;

        const after_prefix = key[key_prefix.len..];
        const colon_pos = std.mem.indexOfScalar(u8, after_prefix, ':') orelse
            return error.InvalidFormat;
        const source_service = after_prefix[0..colon_pos];
        const event_type_from_key = after_prefix[colon_pos + 1 ..];

        const parsed = try deserialize_payload(allocator, value.payload);

        return Breadcrumb{
            .source_service = try allocator.dupe(u8, source_service),
            .event_type = try allocator.dupe(u8, event_type_from_key),
            .last_sequence = parsed.last_sequence,
            .peer_address = parsed.peer_address,
            .updated_at = value.timestamp,
        };
    }
};

test "make_key produces correct format" {
    const allocator = std.testing.allocator;

    const key = try Breadcrumb.make_key(allocator, "order-service", "order.created");
    defer allocator.free(key);

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
    const allocator = std.testing.allocator;

    var bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 42,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 1700000000,
    };

    const key = try Breadcrumb.make_key(allocator, bc.source_service, bc.event_type);
    defer allocator.free(key);

    var ev = try bc.to_event_value(allocator, 100, "my-service", 1700000000);
    defer ev.deinit(allocator);

    try std.testing.expectEqualStrings(breadcrumb_event_type, ev.event_type);
    try std.testing.expectEqualStrings("my-service", ev.origin_service);

    const maybe_bc = try Breadcrumb.from_wal_entry(allocator, key, ev);
    try std.testing.expect(maybe_bc != null);

    var restored = maybe_bc.?;
    defer restored.deinit(allocator);

    try std.testing.expectEqualStrings("order-service", restored.source_service);
    try std.testing.expectEqualStrings("order.created", restored.event_type);
    try std.testing.expectEqual(@as(u64, 42), restored.last_sequence);
    try std.testing.expectEqualStrings("10.0.0.5:4200", restored.peer_address);
    try std.testing.expectEqual(@as(i64, 1700000000), restored.updated_at);
}

test "from_wal_entry returns null for non-breadcrumb keys" {
    const allocator = std.testing.allocator;

    var ev = try EventValue.init(allocator, 1, "test-service", "user.registered", "{}", 100);
    defer ev.deinit(allocator);

    const result = try Breadcrumb.from_wal_entry(allocator, "user-service:u123:profile", ev);
    try std.testing.expect(result == null);
}

test "serialize_payload/deserialize_payload round-trip" {
    const allocator = std.testing.allocator;

    const bc = Breadcrumb{
        .source_service = "svc",
        .event_type = "evt",
        .last_sequence = 99,
        .peer_address = "192.168.1.1:5000",
        .updated_at = 0,
    };

    const payload = try bc.serialize_payload(allocator);
    defer allocator.free(payload);

    const parsed = try Breadcrumb.deserialize_payload(allocator, payload);
    defer allocator.free(parsed.peer_address);

    try std.testing.expectEqual(@as(u64, 99), parsed.last_sequence);
    try std.testing.expectEqualStrings("192.168.1.1:5000", parsed.peer_address);
}
