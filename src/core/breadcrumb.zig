const std = @import("std");
const event = @import("event.zig");
const EventValue = event.EventValue;

pub const KEY_PREFIX = "_bc:";
pub const EVENT_TYPE = "_breadcrumb";

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

    pub fn makeKey(allocator: std.mem.Allocator, source_service: []const u8, event_type: []const u8) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{s}{s}:{s}", .{ KEY_PREFIX, source_service, event_type });
    }

    pub fn isSystemKey(key: []const u8) bool {
        return std.mem.startsWith(u8, key, KEY_PREFIX);
    }

    pub fn serializePayload(self: *const Breadcrumb, allocator: std.mem.Allocator) ![]u8 {
        return try std.fmt.allocPrint(allocator, "{}|{s}", .{ self.last_sequence, self.peer_address });
    }

    pub fn deserializePayload(allocator: std.mem.Allocator, payload: []const u8) !struct { last_sequence: u64, peer_address: []const u8 } {
        var iter = std.mem.splitSequence(u8, payload, "|");
        const seq_str = iter.next() orelse return error.InvalidFormat;
        const last_sequence = try std.fmt.parseInt(u64, seq_str, 10);
        const peer_address = iter.rest();
        return .{
            .last_sequence = last_sequence,
            .peer_address = try allocator.dupe(u8, peer_address),
        };
    }

    pub fn toEventValue(
        self: *const Breadcrumb,
        allocator: std.mem.Allocator,
        local_sequence: u64,
        self_service: []const u8,
        timestamp: i64,
    ) !EventValue {
        const payload = try self.serializePayload(allocator);
        defer allocator.free(payload);
        return try EventValue.init(allocator, local_sequence, self_service, EVENT_TYPE, payload, timestamp);
    }

    pub fn fromWalEntry(allocator: std.mem.Allocator, key: []const u8, value: EventValue) !?Breadcrumb {
        if (!isSystemKey(key)) return null;

        const after_prefix = key[KEY_PREFIX.len..];
        const colon_pos = std.mem.indexOfScalar(u8, after_prefix, ':') orelse return error.InvalidFormat;
        const source_service = after_prefix[0..colon_pos];
        const event_type_from_key = after_prefix[colon_pos + 1 ..];

        const parsed = try deserializePayload(allocator, value.payload);

        return Breadcrumb{
            .source_service = try allocator.dupe(u8, source_service),
            .event_type = try allocator.dupe(u8, event_type_from_key),
            .last_sequence = parsed.last_sequence,
            .peer_address = parsed.peer_address,
            .updated_at = value.timestamp,
        };
    }
};

test "makeKey produces correct format" {
    const allocator = std.testing.allocator;

    const key = try Breadcrumb.makeKey(allocator, "order-service", "order.created");
    defer allocator.free(key);

    try std.testing.expectEqualStrings("_bc:order-service:order.created", key);
}

test "isSystemKey correctly identifies breadcrumb keys" {
    try std.testing.expect(Breadcrumb.isSystemKey("_bc:order-service:order.created"));
    try std.testing.expect(Breadcrumb.isSystemKey("_bc:"));
    try std.testing.expect(!Breadcrumb.isSystemKey("user-service:u123:profile"));
    try std.testing.expect(!Breadcrumb.isSystemKey(""));
    try std.testing.expect(!Breadcrumb.isSystemKey("_b"));
}

test "toEventValue and fromWalEntry round-trip" {
    const allocator = std.testing.allocator;

    var bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 42,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 1700000000,
    };

    const key = try Breadcrumb.makeKey(allocator, bc.source_service, bc.event_type);
    defer allocator.free(key);

    var ev = try bc.toEventValue(allocator, 100, "my-service", 1700000000);
    defer ev.deinit(allocator);

    try std.testing.expectEqualStrings(EVENT_TYPE, ev.event_type);
    try std.testing.expectEqualStrings("my-service", ev.origin_service);

    const maybe_bc = try Breadcrumb.fromWalEntry(allocator, key, ev);
    try std.testing.expect(maybe_bc != null);

    var restored = maybe_bc.?;
    defer restored.deinit(allocator);

    try std.testing.expectEqualStrings("order-service", restored.source_service);
    try std.testing.expectEqualStrings("order.created", restored.event_type);
    try std.testing.expectEqual(@as(u64, 42), restored.last_sequence);
    try std.testing.expectEqualStrings("10.0.0.5:4200", restored.peer_address);
    try std.testing.expectEqual(@as(i64, 1700000000), restored.updated_at);
}

test "fromWalEntry returns null for non-breadcrumb keys" {
    const allocator = std.testing.allocator;

    var ev = try EventValue.init(allocator, 1, "test-service", "user.registered", "{}", 100);
    defer ev.deinit(allocator);

    const result = try Breadcrumb.fromWalEntry(allocator, "user-service:u123:profile", ev);
    try std.testing.expect(result == null);
}

test "serializePayload/deserializePayload round-trip" {
    const allocator = std.testing.allocator;

    const bc = Breadcrumb{
        .source_service = "svc",
        .event_type = "evt",
        .last_sequence = 99,
        .peer_address = "192.168.1.1:5000",
        .updated_at = 0,
    };

    const payload = try bc.serializePayload(allocator);
    defer allocator.free(payload);

    const parsed = try Breadcrumb.deserializePayload(allocator, payload);
    defer allocator.free(parsed.peer_address);

    try std.testing.expectEqual(@as(u64, 99), parsed.last_sequence);
    try std.testing.expectEqualStrings("192.168.1.1:5000", parsed.peer_address);
}

