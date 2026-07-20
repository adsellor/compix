const std = @import("std");
const assert = std.debug.assert;
const auth = @import("auth.zig");
const limits = @import("limits.zig");
const raft = @import("raft.zig");

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
    raft_request_vote = 0x0D,
    raft_vote_result = 0x0E,
    raft_append_entries = 0x0F,
    raft_append_result = 0x10,
    raft_install_snapshot = 0x11,
    raft_snapshot_result = 0x12,
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
pub const replicated_command_size_max: limits.ByteCount = append_batch_size_max + 8;

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
    not_leader = 7,
    quorum_unavailable = 8,
    replication_unavailable = 9,
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

pub const RaftAppendEntries = struct {
    value: raft.AppendEntries,
    command: []const u8 = "",
};

pub const RaftInstallSnapshot = struct {
    value: raft.InstallSnapshot,
    offset: u64,
    done: bool,
    chunk: []const u8,
};

pub const RaftSnapshotResult = struct {
    value: raft.SnapshotResult,
    next_offset: u64,
    accepted: bool,
};

pub const DecodeBuffers = struct {
    pull_events: []PullResponse.EventEntry = &.{},
    append_events: []AppendEvent = &.{},
    read_events: []ReadEvent = &.{},
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
    raft_request_vote: raft.RequestVote,
    raft_vote_result: raft.VoteResult,
    raft_append_entries: RaftAppendEntries,
    raft_append_result: raft.AppendResult,
    raft_install_snapshot: RaftInstallSnapshot,
    raft_snapshot_result: RaftSnapshotResult,
};

/// Encodes into caller-owned memory.
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
        .raft_request_vote => |request| {
            std.mem.writeInt(u64, frame[offset..][0..8], request.term, .big);
            offset += 8;
            frame[offset] = request.candidate;
            offset += 1;
            std.mem.writeInt(u64, frame[offset..][0..8], request.last_log_index, .big);
            offset += 8;
            std.mem.writeInt(u64, frame[offset..][0..8], request.last_log_term, .big);
            offset += 8;
        },
        .raft_vote_result => |result| {
            std.mem.writeInt(u64, frame[offset..][0..8], result.term, .big);
            offset += 8;
            frame[offset] = result.voter;
            offset += 1;
            frame[offset] = @intFromBool(result.granted);
            offset += 1;
        },
        .raft_append_entries => |append| {
            offset = write_raft_append(frame, offset, append);
        },
        .raft_append_result => |result| {
            std.mem.writeInt(u64, frame[offset..][0..8], result.term, .big);
            offset += 8;
            frame[offset] = result.follower;
            offset += 1;
            frame[offset] = @intFromBool(result.success);
            offset += 1;
            std.mem.writeInt(u64, frame[offset..][0..8], result.match_index, .big);
            offset += 8;
            std.mem.writeInt(u64, frame[offset..][0..8], result.conflict_index, .big);
            offset += 8;
        },
        .raft_install_snapshot => |install| {
            std.mem.writeInt(u64, frame[offset..][0..8], install.value.term, .big);
            offset += 8;
            frame[offset] = install.value.leader;
            offset += 1;
            offset = write_raft_snapshot(frame, offset, install.value.snapshot);
            std.mem.writeInt(u64, frame[offset..][0..8], install.offset, .big);
            offset += 8;
            frame[offset] = @intFromBool(install.done);
            offset += 1;
            offset = write_bytes_u32(frame, offset, install.chunk);
        },
        .raft_snapshot_result => |result| {
            std.mem.writeInt(u64, frame[offset..][0..8], result.value.term, .big);
            offset += 8;
            frame[offset] = result.value.follower;
            offset += 1;
            std.mem.writeInt(u64, frame[offset..][0..8], result.value.installed_index, .big);
            offset += 8;
            std.mem.writeInt(u64, frame[offset..][0..8], result.next_offset, .big);
            offset += 8;
            frame[offset] = @intFromBool(result.accepted);
            offset += 1;
        },
    }

    assert(offset == total_size);
    return frame;
}

fn write_raft_append(
    frame: []u8,
    offset_start: limits.ByteCount,
    append: RaftAppendEntries,
) limits.ByteCount {
    var offset = offset_start;
    std.mem.writeInt(u64, frame[offset..][0..8], append.value.term, .big);
    offset += 8;
    frame[offset] = append.value.leader;
    offset += 1;
    std.mem.writeInt(u64, frame[offset..][0..8], append.value.previous_index, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], append.value.previous_term, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], append.value.leader_commit, .big);
    offset += 8;
    frame[offset] = @intFromBool(append.value.entry != null);
    offset += 1;
    if (append.value.entry) |entry| {
        std.mem.writeInt(u64, frame[offset..][0..8], entry.term, .big);
        offset += 8;
        std.mem.writeInt(u64, frame[offset..][0..8], entry.index, .big);
        offset += 8;
        @memcpy(frame[offset..][0..32], &entry.command_checksum);
        offset += 32;
        offset = write_bytes_u32(frame, offset, append.command);
    }
    return offset;
}

fn write_raft_snapshot(
    frame: []u8,
    offset_start: limits.ByteCount,
    snapshot: raft.Snapshot,
) limits.ByteCount {
    var offset = offset_start;
    std.mem.writeInt(u64, frame[offset..][0..8], snapshot.last_index, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], snapshot.last_term, .big);
    offset += 8;
    std.mem.writeInt(u64, frame[offset..][0..8], snapshot.state_size, .big);
    offset += 8;
    @memcpy(frame[offset..][0..32], &snapshot.state_checksum);
    return offset + 32;
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

/// Decodes without allocating. Returned slices borrow `frame`; decoded event
/// entries use the corresponding caller-owned scratch buffers.
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
        0x0D => decode_raft_request_vote(payload),
        0x0E => decode_raft_vote_result(payload),
        0x0F => decode_raft_append(payload),
        0x10 => decode_raft_append_result(payload),
        0x11 => decode_raft_install_snapshot(payload),
        0x12 => decode_raft_snapshot_result(payload),
        else => error.UnknownMessageType,
    };
}

fn decode_raft_request_vote(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const value: raft.RequestVote = .{
        .term = try decoder.read_int(u64),
        .candidate = try decoder.read_int(u8),
        .last_log_index = try decoder.read_int(u64),
        .last_log_term = try decoder.read_int(u64),
    };
    try decoder.finish();
    return .{ .raft_request_vote = value };
}

fn decode_raft_vote_result(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const term = try decoder.read_int(u64);
    const voter = try decoder.read_int(u8);
    const granted = switch (try decoder.read_int(u8)) {
        0 => false,
        1 => true,
        else => return error.InvalidBoolean,
    };
    try decoder.finish();
    return .{ .raft_vote_result = .{ .term = term, .voter = voter, .granted = granted } };
}

fn decode_raft_append(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const term = try decoder.read_int(u64);
    const leader = try decoder.read_int(u8);
    const previous_index = try decoder.read_int(u64);
    const previous_term = try decoder.read_int(u64);
    const leader_commit = try decoder.read_int(u64);
    const has_entry = switch (try decoder.read_int(u8)) {
        0 => false,
        1 => true,
        else => return error.InvalidBoolean,
    };
    var entry: ?raft.Entry = null;
    var command: []const u8 = "";
    if (has_entry) {
        const entry_term = try decoder.read_int(u64);
        const entry_index = try decoder.read_int(u64);
        const checksum_bytes = try decoder.read_bytes(32);
        command = try decoder.read_bytes_u32();
        if (command.len == 0 or command.len > replicated_command_size_max)
            return error.InvalidReplicatedCommand;
        entry = .{
            .term = entry_term,
            .index = entry_index,
            .command_checksum = checksum_bytes[0..32].*,
        };
    }
    try decoder.finish();
    return .{ .raft_append_entries = .{
        .value = .{
            .term = term,
            .leader = leader,
            .previous_index = previous_index,
            .previous_term = previous_term,
            .leader_commit = leader_commit,
            .entry = entry,
        },
        .command = command,
    } };
}

fn decode_raft_append_result(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const term = try decoder.read_int(u64);
    const follower = try decoder.read_int(u8);
    const success = switch (try decoder.read_int(u8)) {
        0 => false,
        1 => true,
        else => return error.InvalidBoolean,
    };
    const match_index = try decoder.read_int(u64);
    const conflict_index = try decoder.read_int(u64);
    try decoder.finish();
    return .{ .raft_append_result = .{
        .term = term,
        .follower = follower,
        .success = success,
        .match_index = match_index,
        .conflict_index = conflict_index,
    } };
}

fn decode_raft_install_snapshot(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const term = try decoder.read_int(u64);
    const leader = try decoder.read_int(u8);
    const snapshot = try decode_raft_snapshot(&decoder);
    const offset = try decoder.read_int(u64);
    const done_wire = try decoder.read_int(u8);
    if (done_wire > 1) return error.InvalidBoolean;
    const chunk = try decoder.read_bytes_u32();
    try decoder.finish();
    return .{ .raft_install_snapshot = .{
        .value = .{ .term = term, .leader = leader, .snapshot = snapshot },
        .offset = offset,
        .done = done_wire == 1,
        .chunk = chunk,
    } };
}

fn decode_raft_snapshot_result(payload: []const u8) !Message {
    var decoder: PayloadDecoder = .{ .payload = payload };
    const value: RaftSnapshotResult = .{
        .value = .{
            .term = try decoder.read_int(u64),
            .follower = try decoder.read_int(u8),
            .installed_index = try decoder.read_int(u64),
        },
        .next_offset = try decoder.read_int(u64),
        .accepted = switch (try decoder.read_int(u8)) {
            0 => false,
            1 => true,
            else => return error.InvalidBoolean,
        },
    };
    try decoder.finish();
    return .{ .raft_snapshot_result = value };
}

fn decode_raft_snapshot(decoder: *PayloadDecoder) !raft.Snapshot {
    return .{
        .last_index = try decoder.read_int(u64),
        .last_term = try decoder.read_int(u64),
        .state_size = try decoder.read_int(u64),
        .state_checksum = (try decoder.read_bytes(32))[0..32].*,
    };
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
        7 => .not_leader,
        8 => .quorum_unavailable,
        9 => .replication_unavailable,
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
    // version(1) + type(1) + type-specific
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
            try payload_size_add(&size, 4); // event_count
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
        .raft_request_vote => try payload_size_add(&size, 25),
        .raft_vote_result => try payload_size_add(&size, 10),
        .raft_append_entries => |append| {
            try payload_size_add(&size, 34);
            if (append.value.entry != null) {
                try payload_size_add(&size, 8 + 8 + 32 + 4 + append.command.len);
            }
        },
        .raft_append_result => try payload_size_add(&size, 26),
        .raft_install_snapshot => |install| {
            try payload_size_add(&size, 78 + install.chunk.len);
        },
        .raft_snapshot_result => try payload_size_add(&size, 26),
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
        .raft_request_vote => {},
        .raft_vote_result => {},
        .raft_append_entries => |append| {
            if (append.value.entry == null) {
                if (append.command.len != 0) return error.UnexpectedReplicatedCommand;
            } else if (append.command.len == 0 or
                append.command.len > replicated_command_size_max)
                return error.InvalidReplicatedCommand;
        },
        .raft_append_result => {},
        .raft_install_snapshot => |install| {
            if (install.chunk.len == 0 or
                install.chunk.len > limits.snapshot_chunk_size_max or
                install.offset > install.value.snapshot.state_size or
                install.chunk.len > install.value.snapshot.state_size - install.offset or
                install.done !=
                    (install.offset + install.chunk.len == install.value.snapshot.state_size))
                return error.InvalidSnapshotChunk;
        },
        .raft_snapshot_result => |result| {
            if (result.next_offset > limits.snapshot_size_hard_max)
                return error.InvalidSnapshotOffset;
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
        .not_leader,
        .quorum_unavailable,
        .replication_unavailable,
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
    // The length prefix excludes itself, but the complete frame is bounded.
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
    frame[5] = 0xFF; // unknown type
    @memset(frame[6..], 0);

    var decode_buffers: DecodeBuffers = .{};
    const result = decode(&frame, &decode_buffers);
    try std.testing.expectError(error.UnknownMessageType, result);
}

test "reject truncated frames" {
    // Frame too short for even the header
    var decode_buffers: DecodeBuffers = .{};
    const result1 = decode(&[_]u8{ 0, 0, 0 }, &decode_buffers);
    try std.testing.expectError(error.TruncatedFrame, result1);

    // Payload length says 10 but frame only has 8 bytes total
    var frame2: [8]u8 = undefined;
    std.mem.writeInt(u32, frame2[0..4], 10, .big);
    frame2[4] = protocol_version;
    frame2[5] = @intFromEnum(MessageType.handshake);
    frame2[6] = 0;
    frame2[7] = 0;
    const result2 = decode(&frame2, &decode_buffers);
    try std.testing.expectError(error.TruncatedFrame, result2);

    // Valid header claiming handshake but truncated handshake body
    var frame3: [8]u8 = undefined;
    std.mem.writeInt(u32, frame3[0..4], 4, .big);
    frame3[4] = protocol_version;
    frame3[5] = @intFromEnum(MessageType.handshake);
    // Only 2 bytes of payload after version+type, need at least 4 (two u16 lengths)
    frame3[6] = 0;
    frame3[7] = 5; // service_name_len = 5, but no data follows
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

test "round-trip Raft messages and replicated command" {
    const checksum: [32]u8 = @splat(0xa5);
    const snapshot_checksum: [32]u8 = @splat(0x5a);
    const messages = [_]Message{
        .{ .raft_request_vote = .{
            .term = 7,
            .candidate = 2,
            .last_log_index = 41,
            .last_log_term = 6,
        } },
        .{ .raft_vote_result = .{ .term = 7, .voter = 3, .granted = true } },
        .{ .raft_append_entries = .{ .value = .{
            .term = 7,
            .leader = 2,
            .previous_index = 41,
            .previous_term = 6,
            .leader_commit = 40,
            .entry = .{
                .term = 7,
                .index = 42,
                .command_checksum = checksum,
            },
        }, .command = "durable-command" } },
        .{ .raft_append_entries = .{ .value = .{
            .term = 7,
            .leader = 2,
            .previous_index = 42,
            .previous_term = 7,
            .leader_commit = 42,
            .entry = null,
        } } },
        .{ .raft_append_result = .{
            .term = 7,
            .follower = 3,
            .success = false,
            .match_index = 40,
            .conflict_index = 38,
        } },
        .{ .raft_install_snapshot = .{
            .value = .{
                .term = 8,
                .leader = 2,
                .snapshot = .{
                    .last_index = 42,
                    .last_term = 7,
                    .state_size = 4,
                    .state_checksum = snapshot_checksum,
                },
            },
            .offset = 0,
            .done = true,
            .chunk = "snap",
        } },
        .{ .raft_snapshot_result = .{
            .value = .{
                .term = 8,
                .follower = 3,
                .installed_index = 42,
            },
            .next_offset = 4,
            .accepted = true,
        } },
    };
    var frame_buffer: [1024]u8 = undefined;
    var buffers: DecodeBuffers = .{};
    for (messages) |expected| {
        const actual = try test_round_trip(&frame_buffer, expected, &buffers);
        switch (expected) {
            .raft_request_vote => |value| {
                try std.testing.expectEqualDeep(value, actual.raft_request_vote);
            },
            .raft_vote_result => |value| {
                try std.testing.expectEqualDeep(value, actual.raft_vote_result);
            },
            .raft_append_entries => |value| {
                try std.testing.expectEqualDeep(value.value, actual.raft_append_entries.value);
                try std.testing.expectEqualStrings(
                    value.command,
                    actual.raft_append_entries.command,
                );
            },
            .raft_append_result => |value| {
                try std.testing.expectEqualDeep(value, actual.raft_append_result);
            },
            .raft_install_snapshot => |value| {
                try std.testing.expectEqualDeep(value, actual.raft_install_snapshot);
            },
            .raft_snapshot_result => |value| {
                try std.testing.expectEqualDeep(value, actual.raft_snapshot_result);
            },
            else => unreachable,
        }
    }
}

test "Raft append wire rejects command and boolean mismatches" {
    const checksum: [32]u8 = @splat(0x31);
    const entry: raft.Entry = .{
        .term = 3,
        .index = 9,
        .command_checksum = checksum,
    };
    const heartbeat: raft.AppendEntries = .{
        .term = 3,
        .leader = 1,
        .previous_index = 8,
        .previous_term = 3,
        .leader_commit = 8,
        .entry = null,
    };
    var frame_buffer: [256]u8 = undefined;
    try std.testing.expectError(
        error.UnexpectedReplicatedCommand,
        encode_into(&frame_buffer, .{ .raft_append_entries = .{
            .value = heartbeat,
            .command = "unexpected",
        } }),
    );
    var with_entry = heartbeat;
    with_entry.entry = entry;
    try std.testing.expectError(
        error.InvalidReplicatedCommand,
        encode_into(&frame_buffer, .{ .raft_append_entries = .{ .value = with_entry } }),
    );

    const valid = try encode_into(&frame_buffer, .{ .raft_append_entries = .{
        .value = with_entry,
        .command = "x",
    } });
    var malformed: [256]u8 = undefined;
    @memcpy(malformed[0..valid.len], valid);
    const has_entry_offset = 6 + 8 + 1 + 8 + 8 + 8;
    malformed[has_entry_offset] = 2;
    var buffers: DecodeBuffers = .{};
    try std.testing.expectError(
        error.InvalidBoolean,
        decode(malformed[0..valid.len], &buffers),
    );

    @memcpy(malformed[0..valid.len], valid);
    const command_length_offset = has_entry_offset + 1 + 8 + 8 + 32;
    std.mem.writeInt(u32, malformed[command_length_offset..][0..4], 0, .big);
    try std.testing.expectError(
        error.InvalidReplicatedCommand,
        decode(malformed[0..valid.len], &buffers),
    );
}

test "snapshot chunks reject invalid bounds completion and booleans" {
    const install: raft.InstallSnapshot = .{
        .term = 4,
        .leader = 1,
        .snapshot = .{
            .last_index = 8,
            .last_term = 4,
            .state_size = 4,
            .state_checksum = @splat(0x44),
        },
    };
    var frame_buffer: [256]u8 = undefined;
    try std.testing.expectError(error.InvalidSnapshotChunk, encode_into(
        &frame_buffer,
        .{ .raft_install_snapshot = .{
            .value = install,
            .offset = 0,
            .done = true,
            .chunk = "",
        } },
    ));
    try std.testing.expectError(error.InvalidSnapshotChunk, encode_into(
        &frame_buffer,
        .{ .raft_install_snapshot = .{
            .value = install,
            .offset = 0,
            .done = false,
            .chunk = "full",
        } },
    ));
    try std.testing.expectError(error.InvalidSnapshotChunk, encode_into(
        &frame_buffer,
        .{ .raft_install_snapshot = .{
            .value = install,
            .offset = 3,
            .done = true,
            .chunk = "xx",
        } },
    ));
    const frame = try encode_into(&frame_buffer, .{ .raft_install_snapshot = .{
        .value = install,
        .offset = 0,
        .done = true,
        .chunk = "full",
    } });
    frame_buffer[79] = 2;
    var buffers: DecodeBuffers = .{};
    try std.testing.expectError(error.InvalidBoolean, decode(frame, &buffers));
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
