const std = @import("std");
const assert = std.debug.assert;
const auth = @import("auth.zig");
const limits = @import("limits.zig");
const vc_mod = @import("vc.zig");

pub const protocol_version: u8 = 0x05;
pub const frame_size_max: limits.ByteCount = 1024 * 1024;
pub const frame_prefix_size: limits.ByteCount = 4;
pub const frame_payload_size_max: limits.ByteCount = frame_size_max - frame_prefix_size;

comptime {
    assert(protocol_version > 0);
    assert(frame_size_max > frame_prefix_size + 2);
    assert(frame_payload_size_max + frame_prefix_size == frame_size_max);
}

pub const MessageType = enum(u8) {
    handshake = 0x01,
    pull_request = 0x02,
    pull_response = 0x03,
    err = 0x04,
    append_stream = 0x05,
    append_stream_response = 0x06,
    read_stream = 0x07,
    read_stream_response = 0x08,
    read_log = 0x09,
    read_log_response = 0x0A,
    health_request = 0x0B,
    health_response = 0x0C,
    vc_summary_request = 0x0D,
    vc_summary_response = 0x0E,
    bootstrap_request = 0x0F,
    bootstrap_response = 0x10,
};

pub const EventId = u128;
pub const StreamVersion = u64;
pub const LogPosition = u64;
pub const RequestCount = limits.MessageCount;
pub const SchemaVersion = u32;

pub const stream_id_length_max: limits.ByteCount = 1024;
pub const command_id_length_max: limits.ByteCount = 128;
pub const address_length_max: limits.ByteCount = limits.peer_address_length_max;
pub const instance_id_length_max: limits.ByteCount = limits.instance_id_length_max;
pub const service_name_length_max: limits.ByteCount = limits.service_name_length_max;
pub const append_events_max = limits.append_events_max;
pub const read_events_max = limits.read_events_max;
pub const requests_per_connection_max: RequestCount = limits.messages_per_connection_max;
pub const append_batch_header_size: limits.ByteCount = 112;
pub const append_event_storage_overhead: limits.ByteCount = 26;
pub const append_batch_size_max: limits.ByteCount = limits.payload_size_max;

comptime {
    assert(stream_id_length_max > 0);
    assert(command_id_length_max > 0);
    assert(address_length_max > 0);
    assert(address_length_max <= std.math.maxInt(u16));
    assert(command_id_length_max <= std.math.maxInt(u16));
    assert(instance_id_length_max > 0);
    assert(instance_id_length_max <= std.math.maxInt(u16));
    assert(service_name_length_max > 0);
    assert(service_name_length_max <= std.math.maxInt(u16));
    assert(stream_id_length_max <= std.math.maxInt(u16));
    assert(append_events_max <= std.math.maxInt(u16));
    assert(append_batch_header_size < append_batch_size_max);
    assert(read_events_max <= std.math.maxInt(u16));
    assert(requests_per_connection_max > 0);
}

pub const Handshake = struct {
    kind: Kind = .client_hello,
    service_name: []const u8,
    instance_id: []const u8,
    nonce: [auth.nonce_length]u8 = @splat(0),
    proof: [auth.proof_length]u8 = @splat(0),

    pub const Kind = enum(u8) {
        client_hello = 1,
        server_hello = 2,
    };
};

pub const PullRequest = struct {
    event_type: []const u8,
    from_sequence: limits.Sequence,
    max_count: limits.EventCount,
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
    unauthenticated = 5,
    permission_denied = 6,
};

pub const Error = struct {
    code: ErrorCode,
    message: []const u8,
};

pub const ExpectedVersion = union(enum(u8)) {
    empty = 0,
    exact: StreamVersion = 1,
};

pub const AppendEvent = struct {
    event_type: []const u8,
    schema_version: SchemaVersion,
    payload: []const u8,
};

pub const AppendStream = struct {
    stream_id: []const u8,
    expected_version: ExpectedVersion,
    command_id: []const u8,
    events: []const AppendEvent,
};

pub const AppendResult = struct {
    stream_version_first: StreamVersion,
    stream_version_last: StreamVersion,
    log_position_first: LogPosition,
    log_position_last: LogPosition,
};

pub const AppendStreamResponse = union(enum(u8)) {
    committed: AppendResult = 0,
    replayed: AppendResult = 1,
    version_conflict: StreamVersion = 2,
    command_conflict = 3,
};

pub const ReadStream = struct {
    stream_id: []const u8,
    from_version: StreamVersion,
    max_count: limits.EventCount,
};

pub const ReadLog = struct {
    from_position: LogPosition,
    max_count: limits.EventCount,
};

pub const ReadEvent = struct {
    event_id: EventId,
    stream_id: []const u8,
    stream_version: StreamVersion,
    log_position: LogPosition,
    command_id: []const u8,
    event_type: []const u8,
    schema_version: SchemaVersion,
    payload: []const u8,
};

pub const ReadStreamResponse = struct {
    next_version: StreamVersion,
    events: []const ReadEvent,
};

pub const ReadLogResponse = struct {
    next_position: LogPosition,
    events: []const ReadEvent,
};

pub const HealthRequest = struct {};

pub const VcSummaryRequest = struct {};

pub const VcEntry = vc_mod.Entry;

pub const VcSummaryResponse = struct {
    entries: []const VcEntry,
};

pub const vc_entries_max = vc_mod.entry_max;

/// `cursor` is the last key consumed (empty = start); max_count 0 = server default.
pub const BootstrapRequest = struct {
    cursor: []const u8,
    max_count: limits.EventCount,
};

pub const BootstrapEvent = struct {
    value: []const u8,
};

pub const BootstrapResponse = struct {
    events: []const BootstrapEvent,
    next_cursor: []const u8,
    complete: bool,
    vc: []const VcEntry,
};

pub const HealthResponse = struct {
    integrity_verified: bool,
    store_poisoned: bool,
    ready: bool,
    production_ready: bool,
    degraded: bool,
    compaction_active: bool,
    replication_quorum: bool,
    backup_in_progress: bool,
    streams_used: limits.StreamCount,
    streams_max: limits.StreamCount,
    stream_id_bytes_used: limits.ByteCount,
    stream_id_bytes_max: limits.ByteCount,
    connections_active: limits.ConnectionCount,
    connections_max: limits.ConnectionCount,
    memtable_bytes: limits.ByteCount,
    memtable_entries: limits.MemtableEntryCount,
    sstables: limits.SSTableCount,
    wal_bytes: u64,
    committed_log_position: LogPosition,
    auth_failures: u64,
    permission_denials: u64,
    admission_saturation: u64,
    corruption_findings: u64,
    compaction_failures: u64,
    replication_failures: u64,
    backups_completed: u64,
    last_backup_unix_seconds: i64,
    backup_age_seconds: u64,
    replication_lag: u64,
};

pub const DecodeBuffers = struct {
    pull_events: []PullResponse.EventEntry = &.{},
    append_events: []AppendEvent = &.{},
    read_events: []ReadEvent = &.{},
    vc_entries: []VcEntry = &.{},
    bootstrap_events: []BootstrapEvent = &.{},
};

pub const Message = union(MessageType) {
    handshake: Handshake,
    pull_request: PullRequest,
    pull_response: PullResponse,
    err: Error,
    append_stream: AppendStream,
    append_stream_response: AppendStreamResponse,
    read_stream: ReadStream,
    read_stream_response: ReadStreamResponse,
    read_log: ReadLog,
    read_log_response: ReadLogResponse,
    health_request: HealthRequest,
    health_response: HealthResponse,
    vc_summary_request: VcSummaryRequest,
    vc_summary_response: VcSummaryResponse,
    bootstrap_request: BootstrapRequest,
    bootstrap_response: BootstrapResponse,
};

pub fn encode_into(buffer: []u8, msg: Message) ![]const u8 {
    try validate_message(msg);
    const size = try payload_size(msg);
    if (size > frame_payload_size_max) return error.FrameTooLarge;

    const total_size = frame_prefix_size + size;
    if (buffer.len < total_size) return error.BufferTooSmall;
    const frame = buffer[0..total_size];

    std.mem.writeInt(u32, frame[0..4], @intCast(size), .big);
    frame[4] = protocol_version;
    frame[5] = @intFromEnum(std.meta.activeTag(msg));

    var offset: limits.ByteCount = 6;
    switch (msg) {
        .handshake => |h| {
            frame[offset] = @intFromEnum(h.kind);
            offset += 1;
            offset = write_bytes_u16(frame, offset, h.service_name);
            offset = write_bytes_u16(frame, offset, h.instance_id);
            @memcpy(frame[offset..][0..auth.nonce_length], &h.nonce);
            offset += auth.nonce_length;
            @memcpy(frame[offset..][0..auth.proof_length], &h.proof);
            offset += auth.proof_length;
        },
        .pull_request => |pr| {
            offset = write_bytes_u16(frame, offset, pr.event_type);
            std.mem.writeInt(u64, frame[offset..][0..8], pr.from_sequence, .big);
            offset += 8;
            std.mem.writeInt(u32, frame[offset..][0..4], pr.max_count, .big);
            offset += 4;
        },
        .pull_response => |pr| {
            std.mem.writeInt(u32, frame[offset..][0..4], @intCast(pr.events.len), .big);
            offset += 4;
            for (pr.events) |entry| {
                offset = write_bytes_u32(frame, offset, entry.key);
                offset = write_bytes_u32(frame, offset, entry.value);
            }
        },
        .err => |e| {
            std.mem.writeInt(u16, frame[offset..][0..2], @intFromEnum(e.code), .big);
            offset += 2;
            offset = write_bytes_u16(frame, offset, e.message);
        },
        .append_stream => |request| offset = write_append_stream(frame, offset, request),
        .append_stream_response => |response| {
            offset = write_append_stream_response(frame, offset, response);
        },
        .read_stream => |request| offset = write_read_stream(frame, offset, request),
        .read_stream_response => |response| {
            offset = write_read_response(frame, offset, response.next_version, response.events);
        },
        .read_log => |request| {
            std.mem.writeInt(u64, frame[offset..][0..8], request.from_position, .big);
            offset += 8;
            std.mem.writeInt(u16, frame[offset..][0..2], request.max_count, .big);
            offset += 2;
        },
        .read_log_response => |response| {
            offset = write_read_response(frame, offset, response.next_position, response.events);
        },
        .health_request => {},
        .health_response => |response| offset = write_health_response(frame, offset, response),
        .vc_summary_request => {},
        .vc_summary_response => |response| {
            std.mem.writeInt(u32, frame[offset..][0..4], @intCast(response.entries.len), .big);
            offset += 4;
            for (response.entries) |entry| {
                std.mem.writeInt(u64, frame[offset..][0..8], entry.node, .big);
                offset += 8;
                std.mem.writeInt(u64, frame[offset..][0..8], entry.counter, .big);
                offset += 8;
            }
        },
        .bootstrap_request => |request| {
            offset = write_bytes_u16(frame, offset, request.cursor);
            std.mem.writeInt(u32, frame[offset..][0..4], request.max_count, .big);
            offset += 4;
        },
        .bootstrap_response => |response| {
            std.mem.writeInt(u32, frame[offset..][0..4], @intCast(response.events.len), .big);
            offset += 4;
            for (response.events) |event| {
                offset = write_bytes_u32(frame, offset, event.value);
            }
            offset = write_bytes_u16(frame, offset, response.next_cursor);
            frame[offset] = if (response.complete) 1 else 0;
            offset += 1;
            std.mem.writeInt(u32, frame[offset..][0..4], @intCast(response.vc.len), .big);
            offset += 4;
            for (response.vc) |entry| {
                std.mem.writeInt(u64, frame[offset..][0..8], entry.node, .big);
                offset += 8;
                std.mem.writeInt(u64, frame[offset..][0..8], entry.counter, .big);
                offset += 8;
            }
        },
    }

    assert(offset == total_size);
    return frame;
}

fn write_health_response(
    frame: []u8,
    offset_start: limits.ByteCount,
    response: HealthResponse,
) limits.ByteCount {
    var offset = offset_start;
    var flags: u16 = 0;
    if (response.integrity_verified) flags |= 1 << 0;
    if (response.store_poisoned) flags |= 1 << 1;
    if (response.ready) flags |= 1 << 2;
    if (response.production_ready) flags |= 1 << 3;
    if (response.degraded) flags |= 1 << 4;
    if (response.compaction_active) flags |= 1 << 5;
    if (response.replication_quorum) flags |= 1 << 6;
    if (response.backup_in_progress) flags |= 1 << 7;
    std.mem.writeInt(u16, frame[offset..][0..2], flags, .big);
    offset += 2;
    inline for (.{
        response.streams_used,
        response.streams_max,
        response.stream_id_bytes_used,
        response.stream_id_bytes_max,
    }) |value| {
        std.mem.writeInt(u32, frame[offset..][0..4], value, .big);
        offset += 4;
    }
    std.mem.writeInt(u16, frame[offset..][0..2], response.connections_active, .big);
    offset += 2;
    std.mem.writeInt(u16, frame[offset..][0..2], response.connections_max, .big);
    offset += 2;
    std.mem.writeInt(u32, frame[offset..][0..4], response.memtable_bytes, .big);
    offset += 4;
    std.mem.writeInt(u32, frame[offset..][0..4], response.memtable_entries, .big);
    offset += 4;
    std.mem.writeInt(u16, frame[offset..][0..2], response.sstables, .big);
    offset += 2;
    inline for (.{
        response.wal_bytes,
        response.committed_log_position,
        response.auth_failures,
        response.permission_denials,
        response.admission_saturation,
        response.corruption_findings,
        response.compaction_failures,
        response.replication_failures,
        response.backups_completed,
    }) |value| {
        std.mem.writeInt(u64, frame[offset..][0..8], value, .big);
        offset += 8;
    }
    std.mem.writeInt(i64, frame[offset..][0..8], response.last_backup_unix_seconds, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], response.backup_age_seconds, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], response.replication_lag, .big);
    return offset + 8;
}

fn write_append_stream(
    frame: []u8,
    offset_start: limits.ByteCount,
    request: AppendStream,
) limits.ByteCount {
    var offset = write_bytes_u16(frame, offset_start, request.stream_id);
    frame[offset] = @intFromEnum(request.expected_version);
    offset += 1;
    if (request.expected_version == .exact) {
        std.mem.writeInt(u64, frame[offset..][0..8], request.expected_version.exact, .big);
        offset += 8;
    }
    offset = write_bytes_u16(frame, offset, request.command_id);
    std.mem.writeInt(u16, frame[offset..][0..2], @intCast(request.events.len), .big);
    offset += 2;
    for (request.events) |event| offset = write_append_event(frame, offset, event);
    return offset;
}

fn write_append_event(
    frame: []u8,
    offset_start: limits.ByteCount,
    event: AppendEvent,
) limits.ByteCount {
    var offset = offset_start;
    offset = write_bytes_u16(frame, offset, event.event_type);
    std.mem.writeInt(u32, frame[offset..][0..4], event.schema_version, .big);
    offset += 4;
    return write_bytes_u32(frame, offset, event.payload);
}

fn write_append_stream_response(
    frame: []u8,
    offset_start: limits.ByteCount,
    response: AppendStreamResponse,
) limits.ByteCount {
    var offset = offset_start;
    frame[offset] = @intFromEnum(response);
    offset += 1;
    switch (response) {
        .committed => |result| offset = write_append_result(frame, offset, result),
        .replayed => |result| offset = write_append_result(frame, offset, result),
        .version_conflict => |actual| {
            std.mem.writeInt(u64, frame[offset..][0..8], actual, .big);
            offset += 8;
        },
        .command_conflict => {},
    }
    return offset;
}

fn write_append_result(
    frame: []u8,
    offset_start: limits.ByteCount,
    result: AppendResult,
) limits.ByteCount {
    var offset = offset_start;
    std.mem.writeInt(u64, frame[offset..][0..8], result.stream_version_first, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], result.stream_version_last, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], result.log_position_first, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], result.log_position_last, .big);
    return offset + 8;
}

fn write_read_stream(
    frame: []u8,
    offset_start: limits.ByteCount,
    request: ReadStream,
) limits.ByteCount {
    var offset = write_bytes_u16(frame, offset_start, request.stream_id);
    std.mem.writeInt(u64, frame[offset..][0..8], request.from_version, .big);
    offset += 8;
    std.mem.writeInt(u16, frame[offset..][0..2], request.max_count, .big);
    return offset + 2;
}

fn write_read_response(
    frame: []u8,
    offset_start: limits.ByteCount,
    next_cursor: u64,
    events: []const ReadEvent,
) limits.ByteCount {
    var offset = offset_start;
    std.mem.writeInt(u64, frame[offset..][0..8], next_cursor, .big);
    offset += 8;
    std.mem.writeInt(u16, frame[offset..][0..2], @intCast(events.len), .big);
    offset += 2;
    for (events) |event| offset = write_read_event(frame, offset, event);
    return offset;
}

fn write_read_event(
    frame: []u8,
    offset_start: limits.ByteCount,
    event: ReadEvent,
) limits.ByteCount {
    var offset = offset_start;
    std.mem.writeInt(u128, frame[offset..][0..16], event.event_id, .big);
    offset += 16;
    offset = write_bytes_u16(frame, offset, event.stream_id);
    std.mem.writeInt(u64, frame[offset..][0..8], event.stream_version, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], event.log_position, .big);
    offset += 8;
    offset = write_bytes_u16(frame, offset, event.command_id);
    offset = write_bytes_u16(frame, offset, event.event_type);
    std.mem.writeInt(u32, frame[offset..][0..4], event.schema_version, .big);
    offset += 4;
    return write_bytes_u32(frame, offset, event.payload);
}

fn write_bytes_u16(buf: []u8, offset: limits.ByteCount, bytes: []const u8) limits.ByteCount {
    assert(bytes.len <= std.math.maxInt(u16));
    std.mem.writeInt(u16, buf[offset..][0..2], @intCast(bytes.len), .big);
    @memcpy(buf[offset + 2 ..][0..bytes.len], bytes);
    return offset + 2 + @as(limits.ByteCount, @intCast(bytes.len));
}

fn write_bytes_u32(buf: []u8, offset: limits.ByteCount, bytes: []const u8) limits.ByteCount {
    assert(bytes.len <= std.math.maxInt(u32));
    std.mem.writeInt(u32, buf[offset..][0..4], @intCast(bytes.len), .big);
    @memcpy(buf[offset + 4 ..][0..bytes.len], bytes);
    return offset + 4 + @as(limits.ByteCount, @intCast(bytes.len));
}

/// Returned slices borrow `frame` and the caller's scratch buffers.
pub fn decode(frame: []const u8, buffers: *DecodeBuffers) !Message {
    if (frame.len < 6) return error.TruncatedFrame;
    const payload_length = std.mem.readInt(u32, frame[0..4], .big);
    if (payload_length > frame_payload_size_max) return error.FrameTooLarge;
    if (payload_length < 2) return error.TruncatedFrame;

    const frame_size: limits.ByteCount = frame_prefix_size + payload_length;
    if (frame.len < frame_size) return error.TruncatedFrame;
    if (frame.len > frame_size) return error.TrailingData;
    if (frame[4] != protocol_version) return error.UnsupportedVersion;
    assert(frame_size >= frame_prefix_size + 2);
    assert(frame_size <= frame_size_max);

    const payload = frame[6..frame_size];
    return switch (frame[5]) {
        0x01 => decode_handshake_borrowed(payload),
        0x02 => decode_pull_request_borrowed(payload),
        0x03 => decode_pull_response_borrowed(payload, buffers.pull_events),
        0x04 => decode_error_borrowed(payload),
        0x05 => decode_append_stream_borrowed(payload, buffers.append_events),
        0x06 => decode_append_stream_response(payload),
        0x07 => decode_read_stream_borrowed(payload),
        0x08 => decode_read_stream_response_borrowed(payload, buffers.read_events),
        0x09 => decode_read_log_borrowed(payload),
        0x0A => decode_read_log_response_borrowed(payload, buffers.read_events),
        0x0B => if (payload.len == 0)
            .{ .health_request = .{} }
        else
            error.TrailingData,
        0x0C => decode_health_response(payload),
        0x0D => if (payload.len == 0)
            .{ .vc_summary_request = .{} }
        else
            error.TrailingData,
        0x0E => decode_vc_summary_response(payload, buffers.vc_entries),
        0x0F => decode_bootstrap_request(payload),
        0x10 => decode_bootstrap_response(
            payload,
            buffers.bootstrap_events,
            buffers.vc_entries,
        ),
        else => error.UnknownMessageType,
    };
}

fn decode_bootstrap_request(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const cursor = try decoder.read_bytes_u16();
    if (cursor.len > limits.key_length_max) return error.CursorTooLong;
    const max_count = try decoder.read_int(u32);
    if (max_count > limits.pull_events_max) return error.TooManyEvents;
    try decoder.finish();
    return .{ .bootstrap_request = .{
        .cursor = cursor,
        .max_count = @intCast(max_count),
    } };
}

fn decode_bootstrap_response(
    payload: []const u8,
    event_entries: []BootstrapEvent,
    vc_entries: []VcEntry,
) !Message {
    if (payload.len < 4) return error.TruncatedFrame;
    const count = std.mem.readInt(u32, payload[0..4], .big);
    if (count > limits.pull_events_max) return error.TooManyEvents;
    if (count > event_entries.len) return error.EventBufferTooSmall;

    var offset: limits.ByteCount = 4;
    for (event_entries[0..count]) |*entry| {
        if (offset + 4 > payload.len) return error.TruncatedFrame;
        const value_length = std.mem.readInt(u32, payload[offset..][0..4], .big);
        offset += 4;
        if (value_length > payload.len - offset) return error.TruncatedFrame;
        entry.* = .{ .value = payload[offset..][0..value_length] };
        offset += value_length;
    }
    if (offset + 2 > payload.len) return error.TruncatedFrame;
    const cursor_length = std.mem.readInt(u16, payload[offset..][0..2], .big);
    offset += 2;
    if (cursor_length > limits.key_length_max) return error.CursorTooLong;
    if (cursor_length > payload.len - offset) return error.TruncatedFrame;
    const next_cursor = payload[offset..][0..cursor_length];
    offset += cursor_length;
    if (offset + 1 > payload.len) return error.TruncatedFrame;
    const complete_byte = payload[offset];
    if (complete_byte > 1) return error.InvalidMessage;
    offset += 1;
    if (offset + 4 > payload.len) return error.TruncatedFrame;
    const vc_count = std.mem.readInt(u32, payload[offset..][0..4], .big);
    offset += 4;
    if (vc_count > vc_entries_max) return error.TooManyEntries;
    if (vc_count > vc_entries.len) return error.EventBufferTooSmall;
    const expected_vc_size: usize = @as(usize, vc_count) * 16;
    if (payload.len - offset != expected_vc_size) return error.InvalidMessage;
    var previous_node: ?u64 = null;
    for (vc_entries[0..vc_count], 0..) |*entry, index| {
        const base = offset + index * 16;
        entry.node = std.mem.readInt(u64, payload[base..][0..8], .big);
        entry.counter = std.mem.readInt(u64, payload[base + 8 ..][0..8], .big);
        if (previous_node) |previous| {
            if (entry.node <= previous) return error.InvalidMessage;
        }
        previous_node = entry.node;
    }
    return .{ .bootstrap_response = .{
        .events = event_entries[0..count],
        .next_cursor = next_cursor,
        .complete = complete_byte == 1,
        .vc = vc_entries[0..vc_count],
    } };
}

fn decode_vc_summary_response(payload: []const u8, entries: []VcEntry) !Message {
    if (payload.len < 4) return error.TruncatedFrame;
    const count = std.mem.readInt(u32, payload[0..4], .big);
    if (count > vc_entries_max) return error.TooManyEntries;
    if (count > entries.len) return error.EventBufferTooSmall;
    const expected_size: limits.ByteCount = @intCast(4 + @as(usize, count) * 16);
    if (payload.len != expected_size) return error.InvalidMessage;

    var previous_node: ?u64 = null;
    for (entries[0..count], 0..) |*entry, index| {
        const base = 4 + index * 16;
        entry.node = std.mem.readInt(u64, payload[base..][0..8], .big);
        entry.counter = std.mem.readInt(u64, payload[base + 8 ..][0..8], .big);
        if (previous_node) |previous| {
            if (entry.node <= previous) return error.InvalidMessage;
        }
        previous_node = entry.node;
    }
    return .{ .vc_summary_response = .{ .entries = entries[0..count] } };
}

fn decode_health_response(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const flags = try decoder.read_int(u16);
    if (flags & 0xff00 != 0) return error.InvalidHealthFlags;
    const response: HealthResponse = .{
        .integrity_verified = flags & (1 << 0) != 0,
        .store_poisoned = flags & (1 << 1) != 0,
        .ready = flags & (1 << 2) != 0,
        .production_ready = flags & (1 << 3) != 0,
        .degraded = flags & (1 << 4) != 0,
        .compaction_active = flags & (1 << 5) != 0,
        .replication_quorum = flags & (1 << 6) != 0,
        .backup_in_progress = flags & (1 << 7) != 0,
        .streams_used = try decoder.read_int(u32),
        .streams_max = try decoder.read_int(u32),
        .stream_id_bytes_used = try decoder.read_int(u32),
        .stream_id_bytes_max = try decoder.read_int(u32),
        .connections_active = try decoder.read_int(u16),
        .connections_max = try decoder.read_int(u16),
        .memtable_bytes = try decoder.read_int(u32),
        .memtable_entries = try decoder.read_int(u32),
        .sstables = try decoder.read_int(u16),
        .wal_bytes = try decoder.read_int(u64),
        .committed_log_position = try decoder.read_int(u64),
        .auth_failures = try decoder.read_int(u64),
        .permission_denials = try decoder.read_int(u64),
        .admission_saturation = try decoder.read_int(u64),
        .corruption_findings = try decoder.read_int(u64),
        .compaction_failures = try decoder.read_int(u64),
        .replication_failures = try decoder.read_int(u64),
        .backups_completed = try decoder.read_int(u64),
        .last_backup_unix_seconds = try decoder.read_int(i64),
        .backup_age_seconds = try decoder.read_int(u64),
        .replication_lag = try decoder.read_int(u64),
    };
    try decoder.finish();
    if (response.streams_used > response.streams_max or
        response.stream_id_bytes_used > response.stream_id_bytes_max or
        response.connections_active > response.connections_max)
        return error.InvalidHealthResponse;
    return .{ .health_response = response };
}

fn decode_handshake_borrowed(payload: []const u8) !Message {
    const minimum_size = 1 + 2 + 2 + auth.nonce_length + auth.proof_length;
    if (payload.len < minimum_size) return error.TruncatedFrame;
    var decoder: PayloadDecoder = .{ .payload = payload };
    const kind: Handshake.Kind = switch (try decoder.read_int(u8)) {
        1 => .client_hello,
        2 => .server_hello,
        else => return error.InvalidHandshakeKind,
    };
    const service_name = try decoder.read_bytes_u16();
    if (service_name.len > limits.service_name_length_max) return error.ServiceNameTooLong;
    const instance_id = try decoder.read_bytes_u16();
    if (instance_id.len > limits.instance_id_length_max) return error.InstanceIdTooLong;
    const nonce_bytes = try decoder.read_bytes(auth.nonce_length);
    const proof_bytes = try decoder.read_bytes(auth.proof_length);
    try decoder.finish();
    var nonce: [auth.nonce_length]u8 = undefined;
    @memcpy(&nonce, nonce_bytes);
    var proof: [auth.proof_length]u8 = undefined;
    @memcpy(&proof, proof_bytes);

    return .{ .handshake = .{
        .kind = kind,
        .service_name = service_name,
        .instance_id = instance_id,
        .nonce = nonce,
        .proof = proof,
    } };
}

fn decode_pull_request_borrowed(payload: []const u8) !Message {
    if (payload.len < 14) return error.TruncatedFrame;
    const event_type_length = std.mem.readInt(u16, payload[0..2], .big);
    if (event_type_length > limits.event_type_length_max) return error.EventTypeTooLong;
    const sequence_offset: limits.ByteCount = 2 + event_type_length;
    if (sequence_offset + 12 > payload.len) return error.TruncatedFrame;
    if (sequence_offset + 12 < payload.len) return error.TrailingData;

    const max_count_wire = std.mem.readInt(u32, payload[sequence_offset + 8 ..][0..4], .big);
    if (max_count_wire > limits.pull_events_max) return error.TooManyEvents;

    return .{ .pull_request = .{
        .event_type = payload[2..sequence_offset],
        .from_sequence = std.mem.readInt(u64, payload[sequence_offset..][0..8], .big),
        .max_count = @intCast(max_count_wire),
    } };
}

fn decode_pull_response_borrowed(
    payload: []const u8,
    event_entries: []PullResponse.EventEntry,
) !Message {
    if (payload.len < 4) return error.TruncatedFrame;
    const event_count = std.mem.readInt(u32, payload[0..4], .big);
    if (event_count > limits.pull_events_max) return error.TooManyEvents;
    if (event_count > event_entries.len) return error.EventBufferTooSmall;

    var offset: limits.ByteCount = 4;
    for (event_entries[0..event_count]) |*entry| {
        if (offset + 4 > payload.len) return error.TruncatedFrame;
        const key_length = std.mem.readInt(u32, payload[offset..][0..4], .big);
        offset += 4;
        if (key_length > payload.len - offset) return error.TruncatedFrame;
        const key = payload[offset..][0..key_length];
        offset += key_length;

        if (offset + 4 > payload.len) return error.TruncatedFrame;
        const value_length = std.mem.readInt(u32, payload[offset..][0..4], .big);
        offset += 4;
        if (value_length > payload.len - offset) return error.TruncatedFrame;
        const value = payload[offset..][0..value_length];
        offset += value_length;
        entry.* = .{ .key = key, .value = value };
    }
    if (offset < payload.len) return error.TrailingData;
    assert(offset == payload.len);

    return .{ .pull_response = .{ .events = event_entries[0..event_count] } };
}

fn decode_error_borrowed(payload: []const u8) !Message {
    if (payload.len < 4) return error.TruncatedFrame;
    const code: ErrorCode = switch (std.mem.readInt(u16, payload[0..2], .big)) {
        1 => .unknown_service,
        2 => .unauthorized_event_type,
        3 => .invalid_message,
        4 => .internal_error,
        5 => .unauthenticated,
        6 => .permission_denied,
        else => return error.InvalidMessage,
    };
    const message_length = std.mem.readInt(u16, payload[2..4], .big);
    const payload_end: limits.ByteCount = 4 + message_length;
    if (payload_end > payload.len) return error.TruncatedFrame;
    if (payload_end < payload.len) return error.TrailingData;
    return .{ .err = .{ .code = code, .message = payload[4..payload_end] } };
}

const PayloadDecoder = struct {
    payload: []const u8,
    offset: limits.ByteCount = 0,

    fn read_int(self: *PayloadDecoder, comptime T: type) !T {
        const width: limits.ByteCount = @sizeOf(T);
        if (width > self.payload.len - self.offset) return error.TruncatedFrame;
        const result = std.mem.readInt(T, self.payload[self.offset..][0..width], .big);
        self.offset += width;
        return result;
    }

    fn read_bytes_u16(self: *PayloadDecoder) ![]const u8 {
        const length = try self.read_int(u16);
        return self.read_bytes(length);
    }

    fn read_bytes_u32(self: *PayloadDecoder) ![]const u8 {
        const length = try self.read_int(u32);
        return self.read_bytes(length);
    }

    fn read_bytes(self: *PayloadDecoder, length: limits.ByteCount) ![]const u8 {
        if (length > self.payload.len - self.offset) return error.TruncatedFrame;
        const bytes = self.payload[self.offset..][0..length];
        self.offset += length;
        return bytes;
    }

    fn finish(self: *const PayloadDecoder) !void {
        if (self.offset < self.payload.len) return error.TrailingData;
        assert(self.offset == self.payload.len);
    }
};

fn decode_append_stream_borrowed(
    payload: []const u8,
    event_buffer: []AppendEvent,
) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const stream_id = try decoder.read_bytes_u16();
    const expected_version: ExpectedVersion = switch (try decoder.read_int(u8)) {
        0 => .empty,
        1 => .{ .exact = try decoder.read_int(u64) },
        else => return error.InvalidExpectedVersion,
    };
    const command_id = try decoder.read_bytes_u16();
    const event_count = try decoder.read_int(u16);
    if (event_count > append_events_max) return error.TooManyEvents;
    if (event_count > event_buffer.len) return error.EventBufferTooSmall;

    for (event_buffer[0..event_count]) |*event| {
        event.* = try decode_append_event(&decoder);
    }
    try decoder.finish();

    const request: AppendStream = .{
        .stream_id = stream_id,
        .expected_version = expected_version,
        .command_id = command_id,
        .events = event_buffer[0..event_count],
    };
    try validate_append_stream(request);
    return .{ .append_stream = request };
}

fn decode_append_event(decoder: *PayloadDecoder) !AppendEvent {
    return .{
        .event_type = try decoder.read_bytes_u16(),
        .schema_version = try decoder.read_int(u32),
        .payload = try decoder.read_bytes_u32(),
    };
}

fn decode_append_stream_response(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const response: AppendStreamResponse = switch (try decoder.read_int(u8)) {
        0 => .{ .committed = try decode_append_result(&decoder) },
        1 => .{ .replayed = try decode_append_result(&decoder) },
        2 => .{ .version_conflict = try decoder.read_int(u64) },
        3 => .command_conflict,
        else => return error.InvalidAppendStatus,
    };
    try decoder.finish();
    try validate_append_response(response);
    return .{ .append_stream_response = response };
}

fn decode_append_result(decoder: *PayloadDecoder) !AppendResult {
    return .{
        .stream_version_first = try decoder.read_int(u64),
        .stream_version_last = try decoder.read_int(u64),
        .log_position_first = try decoder.read_int(u64),
        .log_position_last = try decoder.read_int(u64),
    };
}

fn decode_read_stream_borrowed(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const request: ReadStream = .{
        .stream_id = try decoder.read_bytes_u16(),
        .from_version = try decoder.read_int(u64),
        .max_count = try decoder.read_int(u16),
    };
    try decoder.finish();
    try validate_read_stream(request);
    return .{ .read_stream = request };
}

fn decode_read_log_borrowed(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const request: ReadLog = .{
        .from_position = try decoder.read_int(u64),
        .max_count = try decoder.read_int(u16),
    };
    try decoder.finish();
    try validate_read_count(request.max_count);
    return .{ .read_log = request };
}

fn decode_read_stream_response_borrowed(
    payload: []const u8,
    event_buffer: []ReadEvent,
) !Message {
    const decoded = try decode_read_response(payload, event_buffer);
    return .{ .read_stream_response = .{
        .next_version = decoded.next_cursor,
        .events = decoded.events,
    } };
}

fn decode_read_log_response_borrowed(
    payload: []const u8,
    event_buffer: []ReadEvent,
) !Message {
    const decoded = try decode_read_response(payload, event_buffer);
    return .{ .read_log_response = .{
        .next_position = decoded.next_cursor,
        .events = decoded.events,
    } };
}

const DecodedReadResponse = struct {
    next_cursor: u64,
    events: []const ReadEvent,
};

fn decode_read_response(
    payload: []const u8,
    event_buffer: []ReadEvent,
) !DecodedReadResponse {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const next_cursor = try decoder.read_int(u64);
    const event_count = try decoder.read_int(u16);
    if (event_count > read_events_max) return error.TooManyEvents;
    if (event_count > event_buffer.len) return error.EventBufferTooSmall;
    for (event_buffer[0..event_count]) |*event| {
        event.* = try decode_read_event(&decoder);
        try validate_read_event(event.*);
    }
    try decoder.finish();
    return .{ .next_cursor = next_cursor, .events = event_buffer[0..event_count] };
}

fn decode_read_event(decoder: *PayloadDecoder) !ReadEvent {
    return .{
        .event_id = try decoder.read_int(u128),
        .stream_id = try decoder.read_bytes_u16(),
        .stream_version = try decoder.read_int(u64),
        .log_position = try decoder.read_int(u64),
        .command_id = try decoder.read_bytes_u16(),
        .event_type = try decoder.read_bytes_u16(),
        .schema_version = try decoder.read_int(u32),
        .payload = try decoder.read_bytes_u32(),
    };
}

fn payload_size(msg: Message) !limits.ByteCount {
    var size: limits.ByteCount = 2;
    switch (msg) {
        .handshake => |h| {
            try payload_size_add(&size, 1);
            try payload_size_add(&size, 2);
            try payload_size_add(&size, h.service_name.len);
            try payload_size_add(&size, 2);
            try payload_size_add(&size, h.instance_id.len);
            try payload_size_add(&size, auth.nonce_length + auth.proof_length);
        },
        .pull_request => |pr| {
            try payload_size_add(&size, 2);
            try payload_size_add(&size, pr.event_type.len);
            try payload_size_add(&size, 8 + 4);
        },
        .pull_response => |pr| {
            try payload_size_add(&size, 4);
            for (pr.events) |entry| {
                try payload_size_add(&size, 4);
                try payload_size_add(&size, entry.key.len);
                try payload_size_add(&size, 4);
                try payload_size_add(&size, entry.value.len);
            }
        },
        .err => |e| {
            try payload_size_add(&size, 2 + 2);
            try payload_size_add(&size, e.message.len);
        },
        .append_stream => |request| {
            try payload_size_add(&size, 2 + request.stream_id.len);
            try payload_size_add(&size, expected_version_size(request.expected_version));
            try payload_size_add(&size, 2 + request.command_id.len + 2);
            for (request.events) |event| try append_event_size(&size, event);
        },
        .append_stream_response => |response| {
            try payload_size_add(&size, append_response_size(response));
        },
        .read_stream => |request| {
            try payload_size_add(&size, 2 + request.stream_id.len + 8 + 2);
        },
        .read_log => try payload_size_add(&size, 8 + 2),
        .read_stream_response => |response| {
            try read_response_size(&size, response.events);
        },
        .read_log_response => |response| {
            try read_response_size(&size, response.events);
        },
        .health_request => {},
        .health_response => try payload_size_add(&size, 128),
        .vc_summary_request => {},
        .vc_summary_response => |response| {
            try payload_size_add(&size, 4);
            try payload_size_add(&size, response.entries.len * 16);
        },
        .bootstrap_request => |request| {
            try payload_size_add(&size, 2 + request.cursor.len);
            try payload_size_add(&size, 4);
        },
        .bootstrap_response => |response| {
            try payload_size_add(&size, 4);
            for (response.events) |event| {
                try payload_size_add(&size, 4 + event.value.len);
            }
            try payload_size_add(&size, 2 + response.next_cursor.len);
            try payload_size_add(&size, 1);
            try payload_size_add(&size, 4);
            try payload_size_add(&size, response.vc.len * 16);
        },
    }
    assert(size >= 2);
    assert(size <= frame_payload_size_max);
    return size;
}

fn expected_version_size(expected_version: ExpectedVersion) limits.ByteCount {
    return switch (expected_version) {
        .empty => 1,
        .exact => 1 + 8,
    };
}

fn append_event_size(size: *limits.ByteCount, event: AppendEvent) !void {
    try payload_size_add(size, 2 + event.event_type.len);
    try payload_size_add(size, 4);
    try payload_size_add(size, 4 + event.payload.len);
}

fn append_response_size(response: AppendStreamResponse) limits.ByteCount {
    return switch (response) {
        .committed, .replayed => 1 + 4 * 8,
        .version_conflict => 1 + 8,
        .command_conflict => 1,
    };
}

fn read_response_size(size: *limits.ByteCount, events: []const ReadEvent) !void {
    try payload_size_add(size, 8 + 2);
    for (events) |event| {
        try payload_size_add(size, 16);
        try payload_size_add(size, 2 + event.stream_id.len);
        try payload_size_add(size, 8 + 8);
        try payload_size_add(size, 2 + event.command_id.len);
        try payload_size_add(size, 2 + event.event_type.len);
        try payload_size_add(size, 4);
        try payload_size_add(size, 4 + event.payload.len);
    }
}

fn payload_size_add(size: *limits.ByteCount, increment: usize) !void {
    const increment_count = limits.byte_count(increment) catch return error.FrameTooLarge;
    size.* = std.math.add(limits.ByteCount, size.*, increment_count) catch
        return error.FrameTooLarge;
    if (size.* > frame_payload_size_max) return error.FrameTooLarge;
}

fn validate_message(msg: Message) !void {
    switch (msg) {
        .handshake => |handshake| {
            if (@intFromEnum(handshake.kind) == 0) return error.InvalidHandshakeKind;
            if (handshake.service_name.len > limits.service_name_length_max)
                return error.ServiceNameTooLong;
            if (handshake.instance_id.len > limits.instance_id_length_max)
                return error.InstanceIdTooLong;
        },
        .pull_request => |request| {
            if (request.event_type.len > limits.event_type_length_max)
                return error.EventTypeTooLong;
            if (request.max_count > limits.pull_events_max) return error.TooManyEvents;
        },
        .pull_response => |response| {
            if (response.events.len > limits.pull_events_max) return error.TooManyEvents;
        },
        .err => |protocol_error| {
            if (protocol_error.message.len > std.math.maxInt(u16))
                return error.ErrorMessageTooLong;
        },
        .append_stream => |request| try validate_append_stream(request),
        .append_stream_response => |response| try validate_append_response(response),
        .read_stream => |request| try validate_read_stream(request),
        .read_stream_response => |response| {
            if (response.events.len > read_events_max) return error.TooManyEvents;
            for (response.events) |event| try validate_read_event(event);
        },
        .read_log => |request| try validate_read_count(request.max_count),
        .read_log_response => |response| {
            if (response.events.len > read_events_max) return error.TooManyEvents;
            for (response.events) |event| try validate_read_event(event);
        },
        .health_request => {},
        .health_response => |response| {
            if (response.streams_used > response.streams_max or
                response.stream_id_bytes_used > response.stream_id_bytes_max or
                response.connections_active > response.connections_max)
                return error.InvalidHealthResponse;
        },
        .vc_summary_request => {},
        .vc_summary_response => |response| {
            if (response.entries.len > vc_entries_max) return error.TooManyEntries;
            for (response.entries, 0..) |entry, index| {
                if (index > 0 and entry.node <= response.entries[index - 1].node)
                    return error.InvalidMessage;
            }
        },
        .bootstrap_request => |request| {
            if (request.cursor.len > limits.key_length_max) return error.CursorTooLong;
            if (request.max_count > limits.pull_events_max) return error.TooManyEvents;
        },
        .bootstrap_response => |response| {
            if (response.events.len > limits.pull_events_max) return error.TooManyEvents;
            if (response.next_cursor.len > limits.key_length_max) return error.CursorTooLong;
            if (response.complete and response.next_cursor.len != 0 and response.events.len == 0)
                return error.InvalidMessage;
            if (response.vc.len > vc_entries_max) return error.TooManyEntries;
        },
    }
}

pub fn validate_append(request: AppendStream) !void {
    if (request.stream_id.len == 0) return error.StreamIdEmpty;
    if (request.stream_id.len > stream_id_length_max) return error.StreamIdTooLong;
    if (request.command_id.len == 0) return error.CommandIdEmpty;
    if (request.command_id.len > command_id_length_max) return error.CommandIdTooLong;
    if (request.events.len == 0) return error.EmptyAppend;
    if (request.events.len > append_events_max) return error.TooManyEvents;
    var storage_size: u64 = append_batch_header_size;
    storage_size += request.stream_id.len;
    storage_size += request.command_id.len;
    for (request.events) |event| {
        try validate_append_event(event);
        storage_size += append_event_storage_overhead;
        storage_size += event.event_type.len;
        storage_size += event.payload.len;
    }
    if (storage_size > append_batch_size_max) return error.AppendBatchTooLarge;
}

fn validate_append_stream(request: AppendStream) !void {
    try validate_append(request);
}

fn validate_append_event(event: AppendEvent) !void {
    if (event.event_type.len == 0) return error.EventTypeEmpty;
    if (event.event_type.len > limits.event_type_length_max) return error.EventTypeTooLong;
    if (event.payload.len > limits.payload_size_max) return error.PayloadTooLarge;
}

fn validate_append_response(response: AppendStreamResponse) !void {
    switch (response) {
        .committed => |result| try validate_append_result(result),
        .replayed => |result| try validate_append_result(result),
        .version_conflict, .command_conflict => {},
    }
}

fn validate_append_result(result: AppendResult) !void {
    if (result.stream_version_last < result.stream_version_first)
        return error.InvalidAppendResult;
    if (result.log_position_last < result.log_position_first)
        return error.InvalidAppendResult;
    const stream_span = result.stream_version_last - result.stream_version_first;
    const log_span = result.log_position_last - result.log_position_first;
    if (stream_span != log_span) return error.InvalidAppendResult;
}

fn validate_read_stream(request: ReadStream) !void {
    if (request.stream_id.len == 0) return error.StreamIdEmpty;
    if (request.stream_id.len > stream_id_length_max) return error.StreamIdTooLong;
    try validate_read_count(request.max_count);
}

fn validate_read_count(count: limits.EventCount) !void {
    if (count > read_events_max) return error.TooManyEvents;
}

fn validate_read_event(event: ReadEvent) !void {
    if (event.stream_id.len == 0) return error.StreamIdEmpty;
    if (event.stream_id.len > stream_id_length_max) return error.StreamIdTooLong;
    if (event.command_id.len == 0) return error.CommandIdEmpty;
    if (event.command_id.len > command_id_length_max) return error.CommandIdTooLong;
    if (event.event_type.len == 0) return error.EventTypeEmpty;
    if (event.event_type.len > limits.event_type_length_max) return error.EventTypeTooLong;
    if (event.payload.len > limits.payload_size_max) return error.PayloadTooLarge;
}

fn test_round_trip(
    frame_buffer: []u8,
    message: Message,
    decode_buffers: *DecodeBuffers,
) !Message {
    const frame = try encode_into(frame_buffer, message);
    return decode(frame, decode_buffers);
}

test "protocol surface requires caller-owned bounded memory" {
    try std.testing.expect(!@hasDecl(@This(), "encode"));
    try std.testing.expect(!@hasDecl(@This(), "deinit"));
    try std.testing.expect(@hasDecl(@This(), "encode_into"));
    try std.testing.expect(@hasDecl(@This(), "decode"));
}

test "round-trip handshake" {
    const msg = Message{ .handshake = .{
        .service_name = "order-service",
        .instance_id = "node-1",
    } };
    var frame_buffer: [128]u8 = undefined;
    var decode_buffers: DecodeBuffers = .{};
    const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

    try std.testing.expectEqualStrings("order-service", decoded.handshake.service_name);
    try std.testing.expectEqualStrings("node-1", decoded.handshake.instance_id);
}

test "round-trip handshake with long service name" {
    const long_name_array: [400]u8 = @splat('a');
    const long_name: []const u8 = &long_name_array;
    const msg = Message{ .handshake = .{
        .service_name = long_name,
        .instance_id = "i",
    } };
    var frame_buffer: [512]u8 = undefined;
    var decode_buffers: DecodeBuffers = .{};
    const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

    try std.testing.expectEqualStrings(long_name, decoded.handshake.service_name);
}

test "round-trip handshake with empty names" {
    const msg = Message{ .handshake = .{
        .service_name = "",
        .instance_id = "",
    } };
    var frame_buffer: [80]u8 = undefined;
    var decode_buffers: DecodeBuffers = .{};
    const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

    try std.testing.expectEqualStrings("", decoded.handshake.service_name);
    try std.testing.expectEqualStrings("", decoded.handshake.instance_id);
}

test "round-trip pull_request" {
    const msg = Message{ .pull_request = .{
        .event_type = "order.created",
        .from_sequence = 42,
        .max_count = 100,
    } };
    var frame_buffer: [64]u8 = undefined;
    var decode_buffers: DecodeBuffers = .{};
    const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

    try std.testing.expectEqualStrings("order.created", decoded.pull_request.event_type);
    try std.testing.expectEqual(@as(u64, 42), decoded.pull_request.from_sequence);
    try std.testing.expectEqual(@as(u32, 100), decoded.pull_request.max_count);
}

test "round-trip pull_request with from_sequence=0 and max_count=0" {
    const msg = Message{ .pull_request = .{
        .event_type = "user.registered",
        .from_sequence = 0,
        .max_count = 0,
    } };
    var frame_buffer: [64]u8 = undefined;
    var decode_buffers: DecodeBuffers = .{};
    const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

    try std.testing.expectEqual(@as(u64, 0), decoded.pull_request.from_sequence);
    try std.testing.expectEqual(@as(u32, 0), decoded.pull_request.max_count);
}

test "round-trip pull_response with 0 events" {
    const msg = Message{ .pull_response = .{
        .events = &[_]PullResponse.EventEntry{},
    } };
    var frame_buffer: [16]u8 = undefined;
    var decode_buffers: DecodeBuffers = .{};
    const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

    try std.testing.expectEqual(@as(usize, 0), decoded.pull_response.events.len);
}

test "round-trip pull_response with multiple events" {
    const entries = [_]PullResponse.EventEntry{
        .{
            .key = "evt:order-service:order.created:0000000000000001",
            .value = "1|100|2|os|order.created|{}",
        },
        .{
            .key = "evt:order-service:order.created:0000000000000002",
            .value = "2|200|2|os|order.created|{}",
        },
        .{
            .key = "evt:order-service:order.created:0000000000000003",
            .value = "3|300|2|os|order.created|{}",
        },
    };
    const msg = Message{ .pull_response = .{ .events = &entries } };
    var frame_buffer: [512]u8 = undefined;
    var event_buffer: [3]PullResponse.EventEntry = undefined;
    var decode_buffers: DecodeBuffers = .{ .pull_events = &event_buffer };
    const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

    try std.testing.expectEqual(@as(usize, 3), decoded.pull_response.events.len);
    try std.testing.expectEqualStrings(
        "evt:order-service:order.created:0000000000000001",
        decoded.pull_response.events[0].key,
    );
    try std.testing.expectEqualStrings(
        "1|100|2|os|order.created|{}",
        decoded.pull_response.events[0].value,
    );
    try std.testing.expectEqualStrings(
        "evt:order-service:order.created:0000000000000003",
        decoded.pull_response.events[2].key,
    );
}

test "round-trip error with each error code" {
    const codes = [_]ErrorCode{
        .unknown_service,
        .unauthorized_event_type,
        .invalid_message,
        .internal_error,
        .unauthenticated,
        .permission_denied,
    };
    for (codes) |code| {
        const msg = Message{ .err = .{
            .code = code,
            .message = "something went wrong",
        } };
        var frame_buffer: [64]u8 = undefined;
        var decode_buffers: DecodeBuffers = .{};
        const decoded = try test_round_trip(&frame_buffer, msg, &decode_buffers);

        try std.testing.expectEqual(code, decoded.err.code);
        try std.testing.expectEqualStrings("something went wrong", decoded.err.message);
    }
}

test "reject frames exceeding frame_size_max" {
    var frame: [6]u8 = undefined;
    std.mem.writeInt(u32, frame[0..4], frame_payload_size_max + 1, .big);
    frame[4] = protocol_version;
    frame[5] = @intFromEnum(MessageType.handshake);

    var decode_buffers: DecodeBuffers = .{};
    const result = decode(&frame, &decode_buffers);
    try std.testing.expectError(error.FrameTooLarge, result);
}

test "reject unknown message types" {
    var frame: [10]u8 = undefined;
    std.mem.writeInt(u32, frame[0..4], 6, .big);
    frame[4] = protocol_version;
    frame[5] = 0xFF;
    @memset(frame[6..], 0);

    var decode_buffers: DecodeBuffers = .{};
    const result = decode(&frame, &decode_buffers);
    try std.testing.expectError(error.UnknownMessageType, result);
}

test "reject truncated frames" {
    var decode_buffers: DecodeBuffers = .{};
    const result1 = decode(&[_]u8{ 0, 0, 0 }, &decode_buffers);
    try std.testing.expectError(error.TruncatedFrame, result1);

    var frame2: [8]u8 = undefined;
    std.mem.writeInt(u32, frame2[0..4], 10, .big);
    frame2[4] = protocol_version;
    frame2[5] = @intFromEnum(MessageType.handshake);
    frame2[6] = 0;
    frame2[7] = 0;
    const result2 = decode(&frame2, &decode_buffers);
    try std.testing.expectError(error.TruncatedFrame, result2);

    var frame3: [8]u8 = undefined;
    std.mem.writeInt(u32, frame3[0..4], 4, .big);
    frame3[4] = protocol_version;
    frame3[5] = @intFromEnum(MessageType.handshake);
    frame3[6] = 0;
    frame3[7] = 5;
    const result3 = decode(&frame3, &decode_buffers);
    try std.testing.expectError(error.TruncatedFrame, result3);
}

test "round-trip atomic append drafts without client event ids" {
    const events = [_]AppendEvent{
        .{ .event_type = "object.created", .schema_version = 1, .payload = "one" },
        .{ .event_type = "object.named", .schema_version = 2, .payload = "two" },
    };
    const message: Message = .{ .append_stream = .{
        .stream_id = "ontology-object/42",
        .expected_version = .{ .exact = 7 },
        .command_id = "command-123",
        .events = &events,
    } };
    var frame_buffer: [512]u8 = undefined;
    const frame = try encode_into(&frame_buffer, message);
    var event_buffer: [2]AppendEvent = undefined;
    var buffers: DecodeBuffers = .{ .append_events = &event_buffer };
    const decoded = try decode(frame, &buffers);

    try std.testing.expectEqualStrings("ontology-object/42", decoded.append_stream.stream_id);
    switch (decoded.append_stream.expected_version) {
        .exact => |version| try std.testing.expectEqual(@as(u64, 7), version),
        .empty => return error.UnexpectedExpectedVersion,
    }
    try std.testing.expectEqualStrings("command-123", decoded.append_stream.command_id);
    try std.testing.expectEqual(@as(usize, 2), decoded.append_stream.events.len);
    try std.testing.expectEqualStrings(
        "object.created",
        decoded.append_stream.events[0].event_type,
    );
    try std.testing.expectEqualStrings("two", decoded.append_stream.events[1].payload);
}

test "round-trip append empty-stream expectation" {
    const events = [_]AppendEvent{
        .{ .event_type = "watch.created", .schema_version = 1, .payload = "{}" },
    };
    const message: Message = .{ .append_stream = .{
        .stream_id = "watch/1",
        .expected_version = .empty,
        .command_id = "command-empty",
        .events = &events,
    } };
    var frame_buffer: [256]u8 = undefined;
    const frame = try encode_into(&frame_buffer, message);
    var event_buffer: [1]AppendEvent = undefined;
    var buffers: DecodeBuffers = .{ .append_events = &event_buffer };
    const decoded = try decode(frame, &buffers);

    try std.testing.expect(decoded.append_stream.expected_version == .empty);
    try std.testing.expectEqual(@as(u8, 0), frame[6 + 2 + "watch/1".len]);
}

test "round-trip append response statuses" {
    const result: AppendResult = .{
        .stream_version_first = 8,
        .stream_version_last = 9,
        .log_position_first = 100,
        .log_position_last = 101,
    };
    const responses = [_]AppendStreamResponse{
        .{ .committed = result },
        .{ .replayed = result },
        .{ .version_conflict = 12 },
        .command_conflict,
    };
    var frame_buffer: [128]u8 = undefined;
    for (responses) |response| {
        const frame = try encode_into(
            &frame_buffer,
            .{ .append_stream_response = response },
        );
        var buffers: DecodeBuffers = .{};
        const decoded = try decode(frame, &buffers);
        try expect_append_response_equal(response, decoded.append_stream_response);
    }
}

fn expect_append_response_equal(
    expected: AppendStreamResponse,
    actual: AppendStreamResponse,
) !void {
    try std.testing.expectEqual(std.meta.activeTag(expected), std.meta.activeTag(actual));
    switch (expected) {
        .committed => |result| try std.testing.expectEqualDeep(result, actual.committed),
        .replayed => |result| try std.testing.expectEqualDeep(result, actual.replayed),
        .version_conflict => |version| {
            try std.testing.expectEqual(version, actual.version_conflict);
        },
        .command_conflict => {},
    }
}

test "round-trip stream and log reads with assigned event ids" {
    const event: ReadEvent = .{
        .event_id = 0x0102030405060708090A0B0C0D0E0F10,
        .stream_id = "ontology-object/42",
        .stream_version = 8,
        .log_position = 100,
        .command_id = "command-123",
        .event_type = "object.created",
        .schema_version = 3,
        .payload = "payload",
    };
    var frame_buffer: [512]u8 = undefined;
    const frame = try encode_into(&frame_buffer, .{ .read_log_response = .{
        .next_position = 101,
        .events = &.{event},
    } });
    var event_buffer: [1]ReadEvent = undefined;
    var buffers: DecodeBuffers = .{ .read_events = &event_buffer };
    const decoded = try decode(frame, &buffers);

    try std.testing.expectEqual(@as(u64, 101), decoded.read_log_response.next_position);
    try std.testing.expectEqual(@as(usize, 1), decoded.read_log_response.events.len);
    const decoded_event = decoded.read_log_response.events[0];
    try std.testing.expectEqual(event.event_id, decoded_event.event_id);
    try std.testing.expectEqual(event.stream_version, decoded_event.stream_version);
    try std.testing.expectEqual(event.log_position, decoded_event.log_position);
    try std.testing.expectEqualStrings(event.stream_id, decoded_event.stream_id);
    try std.testing.expectEqualStrings(event.payload, decoded_event.payload);
}

test "round-trip bounded stream and log read requests" {
    var frame_buffer: [128]u8 = undefined;
    const stream_frame = try encode_into(&frame_buffer, .{ .read_stream = .{
        .stream_id = "watch/1",
        .from_version = 9,
        .max_count = read_events_max,
    } });
    var buffers: DecodeBuffers = .{};
    const stream = try decode(stream_frame, &buffers);
    try std.testing.expectEqual(@as(u64, 9), stream.read_stream.from_version);
    try std.testing.expectEqual(read_events_max, stream.read_stream.max_count);

    const log_frame = try encode_into(&frame_buffer, .{ .read_log = .{
        .from_position = 99,
        .max_count = 12,
    } });
    const log = try decode(log_frame, &buffers);
    try std.testing.expectEqual(@as(u64, 99), log.read_log.from_position);
    try std.testing.expectEqual(@as(limits.EventCount, 12), log.read_log.max_count);
}

test "round-trip bounded health metrics" {
    const expected: HealthResponse = .{
        .integrity_verified = true,
        .store_poisoned = false,
        .ready = true,
        .production_ready = false,
        .degraded = true,
        .compaction_active = false,
        .replication_quorum = false,
        .backup_in_progress = false,
        .streams_used = 12,
        .streams_max = 100,
        .stream_id_bytes_used = 120,
        .stream_id_bytes_max = 1000,
        .connections_active = 2,
        .connections_max = 32,
        .memtable_bytes = 4096,
        .memtable_entries = 20,
        .sstables = 3,
        .wal_bytes = 8192,
        .committed_log_position = 44,
        .auth_failures = 5,
        .permission_denials = 6,
        .admission_saturation = 7,
        .corruption_findings = 8,
        .compaction_failures = 9,
        .replication_failures = 13,
        .backups_completed = 10,
        .last_backup_unix_seconds = 1_700_000_000,
        .backup_age_seconds = 11,
        .replication_lag = 12,
    };
    var frame_buffer: [256]u8 = undefined;
    const frame = try encode_into(&frame_buffer, .{ .health_response = expected });
    var buffers: DecodeBuffers = .{};
    const decoded = try decode(frame, &buffers);
    try std.testing.expectEqualDeep(expected, decoded.health_response);
    const request_frame = try encode_into(&frame_buffer, .{ .health_request = .{} });
    try std.testing.expect((try decode(request_frame, &buffers)) == .health_request);
}

test "round-trip vector-clock summary" {
    const entries = [_]VcEntry{
        .{ .node = 1, .counter = 44 },
        .{ .node = 999, .counter = 7 },
    };
    var frame_buffer: [128]u8 = undefined;
    const frame = try encode_into(&frame_buffer, .{ .vc_summary_response = .{
        .entries = &entries,
    } });
    var entry_buffer: [vc_entries_max]VcEntry = undefined;
    var buffers: DecodeBuffers = .{ .vc_entries = &entry_buffer };
    const decoded = try decode(frame, &buffers);
    try std.testing.expectEqual(entries.len, decoded.vc_summary_response.entries.len);
    try std.testing.expectEqualSlices(
        VcEntry,
        &entries,
        decoded.vc_summary_response.entries,
    );

    const request_frame = try encode_into(&frame_buffer, .{ .vc_summary_request = .{} });
    try std.testing.expect((try decode(request_frame, &buffers)) == .vc_summary_request);

    const unordered = [_]VcEntry{
        .{ .node = 9, .counter = 1 },
        .{ .node = 2, .counter = 1 },
    };
    try std.testing.expectError(
        error.InvalidMessage,
        encode_into(&frame_buffer, .{ .vc_summary_response = .{ .entries = &unordered } }),
    );

    var oversized: [vc_entries_max + 1]VcEntry = @splat(.{ .node = 1, .counter = 0 });
    for (&oversized, 0..) |*entry, index| entry.node = index + 1;
    try std.testing.expectError(
        error.TooManyEntries,
        encode_into(&frame_buffer, .{ .vc_summary_response = .{ .entries = &oversized } }),
    );
}

test "round-trip bootstrap pagination" {
    const request: Message = .{ .bootstrap_request = .{
        .cursor = "evt:fugue-a:type:0000000000000004",
        .max_count = 16,
    } };
    var frame_buffer: [512]u8 = undefined;
    var buffers: DecodeBuffers = .{};
    const decoded_request = try test_round_trip(&frame_buffer, request, &buffers);
    try std.testing.expectEqualStrings(
        request.bootstrap_request.cursor,
        decoded_request.bootstrap_request.cursor,
    );
    try std.testing.expectEqual(@as(u32, 16), decoded_request.bootstrap_request.max_count);

    const empty_cursor_frame = try encode_into(&frame_buffer, .{ .bootstrap_request = .{
        .cursor = "",
        .max_count = 8,
    } });
    const decoded_empty = try decode(empty_cursor_frame, &buffers);
    try std.testing.expectEqual(@as(usize, 0), decoded_empty.bootstrap_request.cursor.len);
    try std.testing.expectError(
        error.TooManyEvents,
        encode_into(&frame_buffer, .{ .bootstrap_request = .{
            .cursor = "",
            .max_count = limits.pull_events_max + 1,
        } }),
    );

    const vc_entries = [_]VcEntry{.{ .node = 5, .counter = 12 }};
    const events = [_]BootstrapEvent{
        .{ .value = "1|10|2|os|t||{}" },
        .{ .value = "2|20|3|os|t||9=1|xyz" },
    };
    const response: Message = .{ .bootstrap_response = .{
        .events = &events,
        .next_cursor = "evt:fugue-a:type2:0000000000000007",
        .complete = false,
        .vc = &vc_entries,
    } };
    var bootstrap_buffer: [limits.pull_events_max]BootstrapEvent = undefined;
    var vc_buffer: [vc_entries_max]VcEntry = undefined;
    var response_buffers: DecodeBuffers = .{
        .bootstrap_events = &bootstrap_buffer,
        .vc_entries = &vc_buffer,
    };
    var response_frame: [4096]u8 = undefined;
    const encoded_response = try encode_into(&response_frame, response);
    const decoded_response = try decode(encoded_response, &response_buffers);
    try std.testing.expectEqual(events.len, decoded_response.bootstrap_response.events.len);
    try std.testing.expectEqualStrings(
        events[0].value,
        decoded_response.bootstrap_response.events[0].value,
    );
    try std.testing.expectEqualStrings(
        events[1].value,
        decoded_response.bootstrap_response.events[1].value,
    );
    try std.testing.expectEqualStrings(
        response.bootstrap_response.next_cursor,
        decoded_response.bootstrap_response.next_cursor,
    );
    try std.testing.expect(!decoded_response.bootstrap_response.complete);
    try std.testing.expectEqualSlices(
        VcEntry,
        &vc_entries,
        decoded_response.bootstrap_response.vc,
    );

    const final: Message = .{ .bootstrap_response = .{
        .events = &.{},
        .next_cursor = "",
        .complete = true,
        .vc = &.{},
    } };
    const encoded_final = try encode_into(&frame_buffer, final);
    const decoded_final = try decode(encoded_final, &response_buffers);
    try std.testing.expect(decoded_final.bootstrap_response.complete);
    try std.testing.expectEqual(@as(usize, 0), decoded_final.bootstrap_response.events.len);

    var too_many: [limits.pull_events_max + 1]BootstrapEvent = undefined;
    for (&too_many) |*entry| entry.* = .{ .value = "x" };
    try std.testing.expectError(error.TooManyEvents, encode_into(&frame_buffer, .{
        .bootstrap_response = .{
            .events = &too_many,
            .next_cursor = "",
            .complete = true,
            .vc = &.{},
        },
    }));
    const long_cursor: [limits.key_length_max + 1]u8 = @splat('k');
    try std.testing.expectError(error.CursorTooLong, encode_into(&frame_buffer, .{
        .bootstrap_request = .{ .cursor = &long_cursor, .max_count = 1 },
    }));
}

test "append and read contracts enforce shared bounds" {
    const event: AppendEvent = .{
        .event_type = "event",
        .schema_version = 1,
        .payload = "payload",
    };
    var frame_buffer: [512]u8 = undefined;
    const empty_append: Message = .{ .append_stream = .{
        .stream_id = "stream/1",
        .expected_version = .empty,
        .command_id = "command-1",
        .events = &.{},
    } };
    try std.testing.expectError(error.EmptyAppend, encode_into(&frame_buffer, empty_append));

    var events: [append_events_max + 1]AppendEvent = @splat(event);
    const large_append: Message = .{ .append_stream = .{
        .stream_id = "stream/1",
        .expected_version = .empty,
        .command_id = "command-1",
        .events = &events,
    } };
    try std.testing.expectError(error.TooManyEvents, encode_into(&frame_buffer, large_append));
    try std.testing.expectError(error.TooManyEvents, encode_into(&frame_buffer, .{ .read_log = .{
        .from_position = 0,
        .max_count = read_events_max + 1,
    } }));
}

test "append contract rejects oversized identifiers and event data" {
    const stream_id: [stream_id_length_max + 1]u8 = @splat('s');
    const command_id: [command_id_length_max + 1]u8 = @splat('c');
    const event_type: [limits.event_type_length_max + 1]u8 = @splat('e');
    const payload: [limits.payload_size_max + 1]u8 = @splat('p');
    var frame_buffer: [512]u8 = undefined;

    try expect_invalid_append(
        &frame_buffer,
        &stream_id,
        "command",
        "event",
        "",
        error.StreamIdTooLong,
    );
    try expect_invalid_append(
        &frame_buffer,
        "stream",
        &command_id,
        "event",
        "",
        error.CommandIdTooLong,
    );
    try expect_invalid_append(
        &frame_buffer,
        "stream",
        "command",
        &event_type,
        "",
        error.EventTypeTooLong,
    );
    try expect_invalid_append(
        &frame_buffer,
        "stream",
        "command",
        "event",
        &payload,
        error.PayloadTooLarge,
    );
}

fn expect_invalid_append(
    frame_buffer: []u8,
    stream_id: []const u8,
    command_id: []const u8,
    event_type: []const u8,
    payload: []const u8,
    expected_error: anyerror,
) !void {
    const events = [_]AppendEvent{
        .{ .event_type = event_type, .schema_version = 1, .payload = payload },
    };
    const message: Message = .{ .append_stream = .{
        .stream_id = stream_id,
        .expected_version = .empty,
        .command_id = command_id,
        .events = &events,
    } };
    try std.testing.expectError(expected_error, encode_into(frame_buffer, message));
}
