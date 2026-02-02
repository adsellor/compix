const std = @import("std");
const Allocator = std.mem.Allocator;

pub const PROTOCOL_VERSION: u8 = 0x01;
pub const MAX_FRAME_SIZE: u32 = 4 * 1024 * 1024;

pub const MessageType = enum(u8) {
    handshake = 0x01,
    pull_request = 0x02,
    pull_response = 0x03,
    err = 0x04,
    put_event = 0x05,
    put_event_response = 0x06,
};

pub const Handshake = struct {
    service_name: []const u8,
    instance_id: []const u8,
};

pub const PullRequest = struct {
    event_type: []const u8,
    from_sequence: u64,
    max_count: u32,
};

pub const PullResponse = struct {
    events: []const EventEntry,

    pub const EventEntry = struct {
        key: []const u8,
        value: []const u8,
    };
};

pub const ErrorCode = enum(u16) {
    unknown_service = 1,
    unauthorized_event_type = 2,
    invalid_message = 3,
    internal_error = 4,
};

pub const Error = struct {
    code: ErrorCode,
    message: []const u8,
};

pub const PutEvent = struct {
    event_type: []const u8,
    payload: []const u8,
};

pub const PutEventResponse = struct {
    sequence: u64,
    timestamp: i64,
};

pub const Message = union(MessageType) {
    handshake: Handshake,
    pull_request: PullRequest,
    pull_response: PullResponse,
    err: Error,
    put_event: PutEvent,
    put_event_response: PutEventResponse,
};

/// Encode a Message into a framed byte buffer (4B length prefix + payload).
pub fn encode(allocator: Allocator, msg: Message) ![]u8 {
    // Calculate payload size
    const payload_size = payloadSize(msg);

    if (payload_size > MAX_FRAME_SIZE) return error.FrameTooLarge;

    const total_size = 4 + payload_size;
    const buf = try allocator.alloc(u8, total_size);
    errdefer allocator.free(buf);

    // Write payload length (4B big-endian)
    std.mem.writeInt(u32, buf[0..4], @intCast(payload_size), .big);

    // Write version
    buf[4] = PROTOCOL_VERSION;

    // Write message type
    buf[5] = @intFromEnum(std.meta.activeTag(msg));

    // Write type-specific fields
    var offset: usize = 6;
    switch (msg) {
        .handshake => |h| {
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(h.service_name.len), .big);
            offset += 2;
            @memcpy(buf[offset..][0..h.service_name.len], h.service_name);
            offset += h.service_name.len;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(h.instance_id.len), .big);
            offset += 2;
            @memcpy(buf[offset..][0..h.instance_id.len], h.instance_id);
        },
        .pull_request => |pr| {
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(pr.event_type.len), .big);
            offset += 2;
            @memcpy(buf[offset..][0..pr.event_type.len], pr.event_type);
            offset += pr.event_type.len;
            std.mem.writeInt(u64, buf[offset..][0..8], pr.from_sequence, .big);
            offset += 8;
            std.mem.writeInt(u32, buf[offset..][0..4], pr.max_count, .big);
        },
        .pull_response => |pr| {
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(pr.events.len), .big);
            offset += 4;
            for (pr.events) |entry| {
                std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entry.key.len), .big);
                offset += 4;
                @memcpy(buf[offset..][0..entry.key.len], entry.key);
                offset += entry.key.len;
                std.mem.writeInt(u32, buf[offset..][0..4], @intCast(entry.value.len), .big);
                offset += 4;
                @memcpy(buf[offset..][0..entry.value.len], entry.value);
                offset += entry.value.len;
            }
        },
        .err => |e| {
            std.mem.writeInt(u16, buf[offset..][0..2], @intFromEnum(e.code), .big);
            offset += 2;
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(e.message.len), .big);
            offset += 2;
            @memcpy(buf[offset..][0..e.message.len], e.message);
        },
        .put_event => |pe| {
            std.mem.writeInt(u16, buf[offset..][0..2], @intCast(pe.event_type.len), .big);
            offset += 2;
            @memcpy(buf[offset..][0..pe.event_type.len], pe.event_type);
            offset += pe.event_type.len;
            std.mem.writeInt(u32, buf[offset..][0..4], @intCast(pe.payload.len), .big);
            offset += 4;
            @memcpy(buf[offset..][0..pe.payload.len], pe.payload);
        },
        .put_event_response => |pr| {
            std.mem.writeInt(u64, buf[offset..][0..8], pr.sequence, .big);
            offset += 8;
            std.mem.writeInt(i64, buf[offset..][0..8], pr.timestamp, .big);
        },
    }

    return buf;
}

/// Decode a framed byte buffer into a Message. Input must include the 4B length prefix.
/// Caller owns all returned slices (allocated with allocator).
pub fn decode(allocator: Allocator, frame: []const u8) !Message {
    if (frame.len < 6) return error.TruncatedFrame;

    const payload_length = std.mem.readInt(u32, frame[0..4], .big);

    if (payload_length > MAX_FRAME_SIZE) return error.FrameTooLarge;
    if (payload_length < 2) return error.TruncatedFrame;
    if (frame.len < 4 + payload_length) return error.TruncatedFrame;

    const version = frame[4];
    if (version != PROTOCOL_VERSION) return error.UnsupportedVersion;

    const msg_type_byte = frame[5];
    const msg_type: MessageType = switch (msg_type_byte) {
        0x01 => .handshake,
        0x02 => .pull_request,
        0x03 => .pull_response,
        0x04 => .err,
        0x05 => .put_event,
        0x06 => .put_event_response,
        else => return error.UnknownMessageType,
    };

    const payload = frame[6 .. 4 + payload_length];

    switch (msg_type) {
        .handshake => {
            if (payload.len < 4) return error.TruncatedFrame;
            var off: usize = 0;

            const sn_len = std.mem.readInt(u16, payload[off..][0..2], .big);
            off += 2;
            if (off + sn_len + 2 > payload.len) return error.TruncatedFrame;
            const service_name = try allocator.dupe(u8, payload[off..][0..sn_len]);
            errdefer allocator.free(service_name);
            off += sn_len;

            const ii_len = std.mem.readInt(u16, payload[off..][0..2], .big);
            off += 2;
            if (off + ii_len > payload.len) return error.TruncatedFrame;
            const instance_id = try allocator.dupe(u8, payload[off..][0..ii_len]);

            return Message{ .handshake = .{
                .service_name = service_name,
                .instance_id = instance_id,
            } };
        },
        .pull_request => {
            if (payload.len < 14) return error.TruncatedFrame; // 2 + 0 + 8 + 4
            var off: usize = 0;

            const et_len = std.mem.readInt(u16, payload[off..][0..2], .big);
            off += 2;
            if (off + et_len + 12 > payload.len) return error.TruncatedFrame;
            const event_type = try allocator.dupe(u8, payload[off..][0..et_len]);
            off += et_len;

            const from_sequence = std.mem.readInt(u64, payload[off..][0..8], .big);
            off += 8;
            const max_count = std.mem.readInt(u32, payload[off..][0..4], .big);

            return Message{ .pull_request = .{
                .event_type = event_type,
                .from_sequence = from_sequence,
                .max_count = max_count,
            } };
        },
        .pull_response => {
            if (payload.len < 4) return error.TruncatedFrame;
            var off: usize = 0;

            const event_count = std.mem.readInt(u32, payload[off..][0..4], .big);
            off += 4;

            var events = try allocator.alloc(PullResponse.EventEntry, event_count);
            var parsed: usize = 0;
            errdefer {
                for (events[0..parsed]) |entry| {
                    allocator.free(entry.key);
                    allocator.free(entry.value);
                }
                allocator.free(events);
            }

            for (0..event_count) |i| {
                if (off + 4 > payload.len) return error.TruncatedFrame;
                const key_len = std.mem.readInt(u32, payload[off..][0..4], .big);
                off += 4;
                if (off + key_len > payload.len) return error.TruncatedFrame;
                const key = try allocator.dupe(u8, payload[off..][0..key_len]);
                errdefer allocator.free(key);
                off += key_len;

                if (off + 4 > payload.len) {
                    allocator.free(key);
                    return error.TruncatedFrame;
                }
                const val_len = std.mem.readInt(u32, payload[off..][0..4], .big);
                off += 4;
                if (off + val_len > payload.len) {
                    allocator.free(key);
                    return error.TruncatedFrame;
                }
                const value = try allocator.dupe(u8, payload[off..][0..val_len]);
                off += val_len;

                events[i] = .{ .key = key, .value = value };
                parsed += 1;
            }

            return Message{ .pull_response = .{ .events = events } };
        },
        .err => {
            if (payload.len < 4) return error.TruncatedFrame;
            var off: usize = 0;

            const code_raw = std.mem.readInt(u16, payload[off..][0..2], .big);
            const code: ErrorCode = switch (code_raw) {
                1 => .unknown_service,
                2 => .unauthorized_event_type,
                3 => .invalid_message,
                4 => .internal_error,
                else => return error.InvalidMessage,
            };
            off += 2;

            const msg_len = std.mem.readInt(u16, payload[off..][0..2], .big);
            off += 2;
            if (off + msg_len > payload.len) return error.TruncatedFrame;
            const message = try allocator.dupe(u8, payload[off..][0..msg_len]);

            return Message{ .err = .{
                .code = code,
                .message = message,
            } };
        },
        .put_event => {
            if (payload.len < 6) return error.TruncatedFrame;
            var off: usize = 0;

            const et_len = std.mem.readInt(u16, payload[off..][0..2], .big);
            off += 2;
            if (off + et_len + 4 > payload.len) return error.TruncatedFrame;
            const event_type = try allocator.dupe(u8, payload[off..][0..et_len]);
            errdefer allocator.free(event_type);
            off += et_len;

            const pl_len = std.mem.readInt(u32, payload[off..][0..4], .big);
            off += 4;
            if (off + pl_len > payload.len) return error.TruncatedFrame;
            const pay = try allocator.dupe(u8, payload[off..][0..pl_len]);

            return Message{ .put_event = .{
                .event_type = event_type,
                .payload = pay,
            } };
        },
        .put_event_response => {
            if (payload.len < 16) return error.TruncatedFrame;

            const sequence = std.mem.readInt(u64, payload[0..8], .big);
            const timestamp = std.mem.readInt(i64, payload[8..16], .big);

            return Message{ .put_event_response = .{
                .sequence = sequence,
                .timestamp = timestamp,
            } };
        },
    }
}

/// Free all allocations inside a decoded Message.
pub fn deinit(msg: *Message, allocator: Allocator) void {
    switch (msg.*) {
        .handshake => |h| {
            allocator.free(h.service_name);
            allocator.free(h.instance_id);
        },
        .pull_request => |pr| {
            allocator.free(pr.event_type);
        },
        .pull_response => |pr| {
            for (pr.events) |entry| {
                allocator.free(entry.key);
                allocator.free(entry.value);
            }
            allocator.free(pr.events);
        },
        .err => |e| {
            allocator.free(e.message);
        },
        .put_event => |pe| {
            allocator.free(pe.event_type);
            allocator.free(pe.payload);
        },
        .put_event_response => {},
    }
}

fn payloadSize(msg: Message) usize {
    // version(1) + type(1) + type-specific
    var size: usize = 2;
    switch (msg) {
        .handshake => |h| {
            size += 2 + h.service_name.len + 2 + h.instance_id.len;
        },
        .pull_request => |pr| {
            size += 2 + pr.event_type.len + 8 + 4;
        },
        .pull_response => |pr| {
            size += 4; // event_count
            for (pr.events) |entry| {
                size += 4 + entry.key.len + 4 + entry.value.len;
            }
        },
        .err => |e| {
            size += 2 + 2 + e.message.len;
        },
        .put_event => |pe| {
            size += 2 + pe.event_type.len + 4 + pe.payload.len;
        },
        .put_event_response => {
            size += 8 + 8;
        },
    }
    return size;
}

test "round-trip handshake" {
    const allocator = std.testing.allocator;

    const msg = Message{ .handshake = .{
        .service_name = "order-service",
        .instance_id = "node-1",
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqualStrings("order-service", decoded.handshake.service_name);
    try std.testing.expectEqualStrings("node-1", decoded.handshake.instance_id);
}

test "round-trip handshake with long service name" {
    const allocator = std.testing.allocator;

    const long_name = "a]" ** 200;
    const msg = Message{ .handshake = .{
        .service_name = long_name,
        .instance_id = "i",
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqualStrings(long_name, decoded.handshake.service_name);
}

test "round-trip handshake with empty names" {
    const allocator = std.testing.allocator;

    const msg = Message{ .handshake = .{
        .service_name = "",
        .instance_id = "",
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqualStrings("", decoded.handshake.service_name);
    try std.testing.expectEqualStrings("", decoded.handshake.instance_id);
}

test "round-trip pull_request" {
    const allocator = std.testing.allocator;

    const msg = Message{ .pull_request = .{
        .event_type = "order.created",
        .from_sequence = 42,
        .max_count = 100,
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqualStrings("order.created", decoded.pull_request.event_type);
    try std.testing.expectEqual(@as(u64, 42), decoded.pull_request.from_sequence);
    try std.testing.expectEqual(@as(u32, 100), decoded.pull_request.max_count);
}

test "round-trip pull_request with from_sequence=0 and max_count=0" {
    const allocator = std.testing.allocator;

    const msg = Message{ .pull_request = .{
        .event_type = "user.registered",
        .from_sequence = 0,
        .max_count = 0,
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqual(@as(u64, 0), decoded.pull_request.from_sequence);
    try std.testing.expectEqual(@as(u32, 0), decoded.pull_request.max_count);
}

test "round-trip pull_response with 0 events" {
    const allocator = std.testing.allocator;

    const msg = Message{ .pull_response = .{
        .events = &[_]PullResponse.EventEntry{},
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqual(@as(usize, 0), decoded.pull_response.events.len);
}

test "round-trip pull_response with multiple events" {
    const allocator = std.testing.allocator;

    const entries = [_]PullResponse.EventEntry{
        .{ .key = "evt:order-service:order.created:0000000000000001", .value = "1|100|2|os|order.created|{}" },
        .{ .key = "evt:order-service:order.created:0000000000000002", .value = "2|200|2|os|order.created|{}" },
        .{ .key = "evt:order-service:order.created:0000000000000003", .value = "3|300|2|os|order.created|{}" },
    };
    const msg = Message{ .pull_response = .{ .events = &entries } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqual(@as(usize, 3), decoded.pull_response.events.len);
    try std.testing.expectEqualStrings("evt:order-service:order.created:0000000000000001", decoded.pull_response.events[0].key);
    try std.testing.expectEqualStrings("1|100|2|os|order.created|{}", decoded.pull_response.events[0].value);
    try std.testing.expectEqualStrings("evt:order-service:order.created:0000000000000003", decoded.pull_response.events[2].key);
}

test "round-trip error with each error code" {
    const allocator = std.testing.allocator;

    const codes = [_]ErrorCode{ .unknown_service, .unauthorized_event_type, .invalid_message, .internal_error };
    for (codes) |code| {
        const msg = Message{ .err = .{
            .code = code,
            .message = "something went wrong",
        } };
        const frame = try encode(allocator, msg);
        defer allocator.free(frame);

        var decoded = try decode(allocator, frame);
        defer deinit(&decoded, allocator);

        try std.testing.expectEqual(code, decoded.err.code);
        try std.testing.expectEqualStrings("something went wrong", decoded.err.message);
    }
}

test "reject frames exceeding MAX_FRAME_SIZE" {
    const allocator = std.testing.allocator;

    // Construct a frame header claiming a payload larger than MAX_FRAME_SIZE
    var frame: [6]u8 = undefined;
    std.mem.writeInt(u32, frame[0..4], MAX_FRAME_SIZE + 1, .big);
    frame[4] = PROTOCOL_VERSION;
    frame[5] = @intFromEnum(MessageType.handshake);

    const result = decode(allocator, &frame);
    try std.testing.expectError(error.FrameTooLarge, result);
}

test "reject unknown message types" {
    const allocator = std.testing.allocator;

    var frame: [10]u8 = undefined;
    std.mem.writeInt(u32, frame[0..4], 6, .big);
    frame[4] = PROTOCOL_VERSION;
    frame[5] = 0xFF; // unknown type
    @memset(frame[6..], 0);

    const result = decode(allocator, &frame);
    try std.testing.expectError(error.UnknownMessageType, result);
}

test "reject truncated frames" {
    const allocator = std.testing.allocator;

    // Frame too short for even the header
    const result1 = decode(allocator, &[_]u8{ 0, 0, 0 });
    try std.testing.expectError(error.TruncatedFrame, result1);

    // Payload length says 10 but frame only has 8 bytes total
    var frame2: [8]u8 = undefined;
    std.mem.writeInt(u32, frame2[0..4], 10, .big);
    frame2[4] = PROTOCOL_VERSION;
    frame2[5] = @intFromEnum(MessageType.handshake);
    frame2[6] = 0;
    frame2[7] = 0;
    const result2 = decode(allocator, &frame2);
    try std.testing.expectError(error.TruncatedFrame, result2);

    // Valid header claiming handshake but truncated handshake body
    var frame3: [8]u8 = undefined;
    std.mem.writeInt(u32, frame3[0..4], 4, .big);
    frame3[4] = PROTOCOL_VERSION;
    frame3[5] = @intFromEnum(MessageType.handshake);
    // Only 2 bytes of payload after version+type, need at least 4 (two u16 lengths)
    frame3[6] = 0;
    frame3[7] = 5; // service_name_len = 5, but no data follows
    const result3 = decode(allocator, &frame3);
    try std.testing.expectError(error.TruncatedFrame, result3);
}

test "round-trip put_event" {
    const allocator = std.testing.allocator;

    const msg = Message{ .put_event = .{
        .event_type = "order.created",
        .payload = "{\"id\":123,\"amount\":99.99}",
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqualStrings("order.created", decoded.put_event.event_type);
    try std.testing.expectEqualStrings("{\"id\":123,\"amount\":99.99}", decoded.put_event.payload);
}

test "round-trip put_event with empty payload" {
    const allocator = std.testing.allocator;

    const msg = Message{ .put_event = .{
        .event_type = "heartbeat",
        .payload = "",
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqualStrings("heartbeat", decoded.put_event.event_type);
    try std.testing.expectEqualStrings("", decoded.put_event.payload);
}

test "round-trip put_event_response" {
    const allocator = std.testing.allocator;

    const msg = Message{ .put_event_response = .{
        .sequence = 42,
        .timestamp = 1700000000,
    } };
    const frame = try encode(allocator, msg);
    defer allocator.free(frame);

    var decoded = try decode(allocator, frame);
    defer deinit(&decoded, allocator);

    try std.testing.expectEqual(@as(u64, 42), decoded.put_event_response.sequence);
    try std.testing.expectEqual(@as(i64, 1700000000), decoded.put_event_response.timestamp);
}
