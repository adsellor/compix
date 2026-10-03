const std = @import("std");
const assert = std.debug.assert;
const limits = @import("limits.zig");
const signing = @import("signing.zig");
const vc_mod = @import("vc.zig");

// NOTE: keeps an event signature from being reused as any other Ed25519 signature.
const signing_domain = "compix-event-v1";

/// Tombstone marker: read paths filter it, so applications must not author it.
pub const tombstone_origin = "compix";
pub const tombstone_event_type = "compix.tombstone";

pub fn makeTombstone(timestamp: limits.Timestamp) EventValue {
    return .{
        .sequence = 0,
        .event_type = tombstone_event_type,
        .payload = "",
        .timestamp = timestamp,
        .origin_service = tombstone_origin,
    };
}

pub fn isTombstone(value: EventValue) bool {
    return value.sequence == 0 and
        value.payload.len == 0 and
        std.mem.eql(u8, value.origin_service, tombstone_origin) and
        std.mem.eql(u8, value.event_type, tombstone_event_type);
}

pub const EventValue = struct {
    sequence: limits.Sequence,
    event_type: []const u8,
    payload: []const u8,
    timestamp: limits.Timestamp,
    origin_service: []const u8,
    /// Hex; empty means unsigned.
    signature: []const u8 = "",
    vector_clock: []const u8 = "",

    pub const InitOptions = struct {
        sequence: limits.Sequence,
        origin_service: []const u8,
        event_type: []const u8,
        payload: []const u8,
        timestamp: limits.Timestamp,
        signature: []const u8 = "",
        vector_clock: []const u8 = "",
    };

    pub fn init(options: InitOptions) !EventValue {
        if (options.origin_service.len > limits.service_name_length_max)
            return error.ServiceNameTooLong;
        if (options.event_type.len > limits.event_type_length_max)
            return error.EventTypeTooLong;
        if (options.payload.len > limits.payload_size_max) return error.PayloadTooLarge;
        if (options.vector_clock.len != 0 and
            !vc_mod.Clock.validateText(options.vector_clock))
            return error.InvalidVectorClock;

        return .{
            .sequence = options.sequence,
            .event_type = options.event_type,
            .payload = options.payload,
            .timestamp = options.timestamp,
            .origin_service = options.origin_service,
            .signature = options.signature,
            .vector_clock = options.vector_clock,
        };
    }

    pub fn is_signed(self: *const EventValue) bool {
        return self.signature.len != 0;
    }

    fn feed_signing_message(self: *const EventValue, ctx: anytype) void {
        assert(std.mem.indexOfScalar(u8, self.origin_service, '|') == null);
        assert(std.mem.indexOfScalar(u8, self.event_type, '|') == null);
        var number_buffer: [20]u8 = undefined;
        ctx.update(signing_domain);
        ctx.update("|");
        ctx.update(std.fmt.bufPrint(&number_buffer, "{}", .{self.sequence}) catch unreachable);
        ctx.update("|");
        ctx.update(std.fmt.bufPrint(&number_buffer, "{}", .{self.timestamp}) catch unreachable);
        ctx.update("|");
        ctx.update(self.origin_service);
        ctx.update("|");
        ctx.update(self.event_type);
        ctx.update("|");
        ctx.update(self.vector_clock);
        ctx.update("|");
        ctx.update(std.fmt.bufPrint(&number_buffer, "{}", .{self.payload.len}) catch unreachable);
        ctx.update("|");
        ctx.update(self.payload);
    }

    pub fn signing_message_into(self: *const EventValue, buffer: []u8) ![]const u8 {
        assert(std.mem.indexOfScalar(u8, self.origin_service, '|') == null);
        assert(std.mem.indexOfScalar(u8, self.event_type, '|') == null);
        return std.fmt.bufPrint(buffer, "{s}|{}|{}|{s}|{s}|{s}|{}|{s}", .{
            signing_domain,
            self.sequence,
            self.timestamp,
            self.origin_service,
            self.event_type,
            self.vector_clock,
            self.payload.len,
            self.payload,
        }) catch return error.BufferTooSmall;
    }

    /// `signature_hex_out` backs `self.signature` and must outlive any use of `self`.
    pub fn sign(
        self: *EventValue,
        key: *const signing.SigningKey,
        message_buffer: []u8,
        signature_hex_out: *[signing.signature_hex_length]u8,
    ) !void {
        const message = try self.signing_message_into(message_buffer);
        const raw = try key.sign(message);
        signature_hex_out.* = std.fmt.bytesToHex(raw, .lower);
        self.signature = signature_hex_out;
    }

    pub fn verify_provenance(
        self: *const EventValue,
        public_key: [signing.public_key_length]u8,
    ) bool {
        if (self.signature.len != signing.signature_hex_length) return false;
        var raw: [signing.signature_length]u8 = undefined;
        _ = std.fmt.hexToBytes(&raw, self.signature) catch return false;
        var verifier = signing.Verifier.init(raw, public_key) catch return false;
        self.feed_signing_message(&verifier);
        return verifier.verify();
    }

    /// Wire: `seq|ts|plen|origin|type|sig|vc|payload`; `vc` is optional.
    /// The payload is always the final plen bytes.
    pub fn serialize_into(self: *const EventValue, buffer: []u8) ![]const u8 {
        assert(std.mem.indexOfScalar(u8, self.origin_service, '|') == null);
        assert(std.mem.indexOfScalar(u8, self.event_type, '|') == null);
        if (self.vector_clock.len != 0) {
            return std.fmt.bufPrint(buffer, "{}|{}|{}|{s}|{s}|{s}|{s}|{s}", .{
                self.sequence,
                self.timestamp,
                self.payload.len,
                self.origin_service,
                self.event_type,
                self.signature,
                self.vector_clock,
                self.payload,
            }) catch return error.BufferTooSmall;
        }
        return std.fmt.bufPrint(buffer, "{}|{}|{}|{s}|{s}|{s}|{s}", .{
            self.sequence,
            self.timestamp,
            self.payload.len,
            self.origin_service,
            self.event_type,
            self.signature,
            self.payload,
        }) catch return error.BufferTooSmall;
    }

    pub fn serialized_size(self: *const EventValue) limits.ByteCount {
        const size = if (self.vector_clock.len != 0)
            std.fmt.count("{}|{}|{}|{s}|{s}|{s}|{s}|{s}", .{
                self.sequence,
                self.timestamp,
                self.payload.len,
                self.origin_service,
                self.event_type,
                self.signature,
                self.vector_clock,
                self.payload,
            })
        else
            std.fmt.count("{}|{}|{}|{s}|{s}|{s}|{s}", .{
                self.sequence,
                self.timestamp,
                self.payload.len,
                self.origin_service,
                self.event_type,
                self.signature,
                self.payload,
            });
        return limits.byte_count(size) catch unreachable;
    }

    /// Result borrows `data`.
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
        const signature = iter.next() orelse return error.InvalidFormat;
        if (signature.len != 0 and signature.len != signing.signature_hex_length)
            return error.InvalidFormat;
        const rest = iter.rest();
        if (rest.len < limits.usize_from(payload_len)) return error.InvalidFormat;
        const payload = rest[rest.len - limits.usize_from(payload_len) ..];
        var vector_clock: []const u8 = "";
        const prefix_len = rest.len - limits.usize_from(payload_len);
        if (prefix_len > 0) {
            if (prefix_len < 2 or rest[prefix_len - 1] != '|') return error.InvalidFormat;
            vector_clock = rest[0 .. prefix_len - 1];
            if (!vc_mod.Clock.validateText(vector_clock)) return error.InvalidVectorClock;
        }

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
            .signature = signature,
            .vector_clock = vector_clock,
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
    try std.testing.expectEqualStrings("", deserialized.vector_clock);
}

test "vector clock rides through serialization and legacy events still parse" {
    const ev = try EventValue.init(.{
        .sequence = 7,
        .origin_service = "order-service",
        .event_type = "order.created",
        .payload = "a|b|c",
        .timestamp = 100,
        .vector_clock = "101=4,202=9",
    });
    var buffer: [1024]u8 = undefined;
    const serialized = try ev.serialize_into(&buffer);
    try std.testing.expect(std.mem.endsWith(u8, serialized, "a|b|c"));
    const deserialized = try EventValue.deserialize_borrowed(serialized);
    try std.testing.expectEqualStrings("101=4,202=9", deserialized.vector_clock);
    try std.testing.expectEqualStrings("a|b|c", deserialized.payload);
    try std.testing.expectEqual(@as(u64, 7), deserialized.sequence);

    const legacy = try EventValue.deserialize_borrowed("3|55|2|os|t||{}");
    try std.testing.expectEqualStrings("", legacy.vector_clock);
    try std.testing.expectEqualStrings("{}", legacy.payload);
    try std.testing.expectEqual(@as(i64, 55), legacy.timestamp);

    try std.testing.expectError(
        error.InvalidFormat,
        EventValue.deserialize_borrowed("3|55|5|os|t||{}"),
    );
}

test "invalid vector clock text is rejected at init and parse" {
    try std.testing.expectError(error.InvalidVectorClock, EventValue.init(.{
        .sequence = 1,
        .origin_service = "s",
        .event_type = "t",
        .payload = "",
        .timestamp = 0,
        .vector_clock = "1=x",
    }));
    try std.testing.expectError(error.InvalidVectorClock, EventValue.init(.{
        .sequence = 1,
        .origin_service = "s",
        .event_type = "t",
        .payload = "",
        .timestamp = 0,
        .vector_clock = "1|2",
    }));
    try std.testing.expectError(
        error.InvalidVectorClock,
        EventValue.deserialize_borrowed("1|2|0|s|t||bad=vc|"),
    );
}

test "signature covers the vector clock" {
    const key = try signing.SigningKey.from_seed(@splat(7));
    var message_buffer: [limits.event_serialized_size_max]u8 = undefined;
    var signature_hex: [signing.signature_hex_length]u8 = undefined;

    var ev = try EventValue.init(.{
        .sequence = 1,
        .origin_service = "s",
        .event_type = "t",
        .payload = "p",
        .timestamp = 5,
        .vector_clock = "9=1",
    });
    try ev.sign(&key, &message_buffer, &signature_hex);
    try std.testing.expect(ev.verify_provenance(key.public_key()));

    const message = try ev.signing_message_into(&message_buffer);
    try std.testing.expect(std.mem.indexOf(u8, message, "|t|9=1|1|p") != null);

    var round_trip_buffer: [1024]u8 = undefined;
    const serialized = try ev.serialize_into(&round_trip_buffer);
    const decoded = try EventValue.deserialize_borrowed(serialized);
    try std.testing.expect(decoded.verify_provenance(key.public_key()));

    var altered = decoded;
    altered.vector_clock = "9=2";
    try std.testing.expect(!altered.verify_provenance(key.public_key()));
}
