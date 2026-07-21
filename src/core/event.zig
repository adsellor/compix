const std = @import("std");
const assert = std.debug.assert;
const limits = @import("limits.zig");
const signing = @import("signing.zig");

/// Domain separator so an event signature can never be reused as any other
/// kind of Ed25519 signature over the same bytes.
const signing_domain = "compix-event-v1";

pub const EventValue = struct {
    sequence: limits.Sequence,
    event_type: []const u8,
    payload: []const u8,
    timestamp: limits.Timestamp,
    origin_service: []const u8,
    /// The origin's Ed25519 signature over `signing_message_into`, hex-encoded
    /// (`signature_hex_length` bytes). Stored as a borrowed slice — like the
    /// other byte fields — rather than an inline array, to keep `EventValue`
    /// small on the by-value hot paths. Empty means unsigned (breadcrumbs,
    /// index entries, and legacy events).
    signature: []const u8 = "",

    pub const InitOptions = struct {
        sequence: limits.Sequence,
        origin_service: []const u8,
        event_type: []const u8,
        payload: []const u8,
        timestamp: limits.Timestamp,
        signature: []const u8 = "",
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
            .signature = options.signature,
        };
    }

    pub fn is_signed(self: *const EventValue) bool {
        return self.signature.len != 0;
    }

    /// Feed the canonical signing message — the immutable event identity, with
    /// a domain separator, excluding the signature — to a streaming signer or
    /// verifier. Streaming avoids a contiguous buffer for a large payload.
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
        ctx.update(std.fmt.bufPrint(&number_buffer, "{}", .{self.payload.len}) catch unreachable);
        ctx.update("|");
        ctx.update(self.payload);
    }

    /// Canonical signing message built into a caller buffer (one-shot sign
    /// path). Must produce the identical bytes `feed_signing_message` streams.
    pub fn signing_message_into(self: *const EventValue, buffer: []u8) ![]const u8 {
        assert(std.mem.indexOfScalar(u8, self.origin_service, '|') == null);
        assert(std.mem.indexOfScalar(u8, self.event_type, '|') == null);
        return std.fmt.bufPrint(buffer, "{s}|{}|{}|{s}|{s}|{}|{s}", .{
            signing_domain,
            self.sequence,
            self.timestamp,
            self.origin_service,
            self.event_type,
            self.payload.len,
            self.payload,
        }) catch return error.BufferTooSmall;
    }

    /// Sign this event in place with the origin's key. `message_buffer` holds
    /// the canonical message; `signature_hex_out` receives the hex signature and
    /// must outlive any use of `self` (it is what `self.signature` points at).
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

    /// Returns true if `signature` is a valid origin signature over this event.
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

    pub fn serialize_into(self: *const EventValue, buffer: []u8) ![]const u8 {
        assert(std.mem.indexOfScalar(u8, self.origin_service, '|') == null);
        assert(std.mem.indexOfScalar(u8, self.event_type, '|') == null);
        // signature is already hex (or empty); write it through, no re-encoding.
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
        const size = std.fmt.count("{}|{}|{}|{s}|{s}|{s}|{s}", .{
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
        const signature = iter.next() orelse return error.InvalidFormat;
        if (signature.len != 0 and signature.len != signing.signature_hex_length)
            return error.InvalidFormat;
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
            .signature = signature,
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
