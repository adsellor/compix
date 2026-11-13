const std = @import("std");
const assert = std.debug.assert;

const event = @import("event.zig");
const fixed = @import("fixed.zig");
const limits = @import("limits.zig");
const lsm_mod = @import("lsm.zig");
const protocol = @import("protocol.zig");

const EventValue = event.EventValue;
const LSMEngine = lsm_mod.LSMEngine;

const batch_format_version: u8 = 2;
const batch_key_prefix = "v2:batch:";
const command_key_prefix = "v2:command:";
const log_key_prefix = "v2:log:";
const stream_key_prefix = "v2:stream:";
const batch_event_type = "compix.append-batch";
const command_index_event_type = "compix.command-index";
const event_index_event_type = "compix.event-index";
const origin_service = "compix";
const request_hash_size: limits.ByteCount = 32;
const batch_checksum_size: limits.ByteCount = 32;
const index_payload_size: limits.ByteCount = 10;
const command_index_payload_size: limits.ByteCount = 8 + request_hash_size;

comptime {
    assert(protocol.append_batch_header_size == 112);
    assert(protocol.append_event_storage_overhead == 26);
    assert(protocol.append_batch_size_max <= limits.event_serialized_size_max);
    assert(index_payload_size == @sizeOf(u64) + @sizeOf(u16));
    assert(command_index_payload_size == @sizeOf(u64) + request_hash_size);
}

const Writer = struct {
    bytes: []u8,
    offset: limits.ByteCount = 0,

    fn write_int(self: *Writer, comptime T: type, value: T) !void {
        const width: limits.ByteCount = @sizeOf(T);
        if (width > self.bytes.len - self.offset) return error.BufferTooSmall;
        std.mem.writeInt(T, self.bytes[self.offset..][0..width], value, .big);
        self.offset += width;
    }

    fn write_bytes(self: *Writer, source: []const u8) !void {
        const count = try limits.byte_count(source.len);
        if (count > self.bytes.len - self.offset) return error.BufferTooSmall;
        @memcpy(self.bytes[self.offset..][0..count], source);
        self.offset += count;
    }

    fn write_bytes_u16(self: *Writer, source: []const u8) !void {
        assert(source.len <= std.math.maxInt(u16));
        try self.write_int(u16, @intCast(source.len));
        try self.write_bytes(source);
    }
};

const Reader = struct {
    bytes: []const u8,
    offset: limits.ByteCount = 0,

    fn read_int(self: *Reader, comptime T: type) !T {
        const width: limits.ByteCount = @sizeOf(T);
        if (width > self.bytes.len - self.offset) return error.CorruptStreamData;
        const value = std.mem.readInt(T, self.bytes[self.offset..][0..width], .big);
        self.offset += width;
        return value;
    }

    fn read_bytes(self: *Reader, count: limits.ByteCount) ![]const u8 {
        if (count > self.bytes.len - self.offset) return error.CorruptStreamData;
        const result = self.bytes[self.offset..][0..count];
        self.offset += count;
        return result;
    }

    fn read_bytes_u16(self: *Reader) ![]const u8 {
        return self.read_bytes(try self.read_int(u16));
    }

    fn finish(self: *const Reader) !void {
        if (self.offset != self.bytes.len) return error.CorruptStreamData;
    }
};

const BatchEvent = struct {
    event_id: protocol.EventId,
    event_type: []const u8,
    schema_version: protocol.SchemaVersion,
    payload: []const u8,
};

const BatchDecoder = struct {
    reader: Reader,
    stream_id: []const u8,
    command_id: []const u8,
    expected_version: protocol.ExpectedVersion,
    request_hash: [request_hash_size]u8,
    result: protocol.AppendResult,
    event_count: limits.EventCount,
    event_index: limits.EventCount,

    fn init(bytes: []const u8) !BatchDecoder {
        if (bytes.len < batch_checksum_size) return error.CorruptStreamData;
        const content = bytes[0 .. bytes.len - batch_checksum_size];
        const stored_checksum = bytes[content.len..];
        const calculated_checksum = batch_checksum(content);
        if (!std.crypto.timing_safe.eql(
            [batch_checksum_size]u8,
            calculated_checksum,
            stored_checksum[0..batch_checksum_size].*,
        )) return error.CorruptStreamData;
        var reader: Reader = .{ .bytes = content };
        if (try reader.read_int(u8) != batch_format_version) return error.CorruptStreamData;
        const event_count = try reader.read_int(u16);
        const stream_id = try reader.read_bytes_u16();
        const command_id = try reader.read_bytes_u16();
        const expected_tag = try reader.read_int(u8);
        const expected_value = try reader.read_int(u64);
        const expected_version: protocol.ExpectedVersion = switch (expected_tag) {
            0 => empty: {
                if (expected_value != 0) return error.CorruptStreamData;
                break :empty .empty;
            },
            1 => .{ .exact = expected_value },
            else => return error.CorruptStreamData,
        };
        const hash_bytes = try reader.read_bytes(request_hash_size);
        var request_digest: [request_hash_size]u8 = undefined;
        @memcpy(&request_digest, hash_bytes);
        const result: protocol.AppendResult = .{
            .stream_version_first = try reader.read_int(u64),
            .stream_version_last = try reader.read_int(u64),
            .log_position_first = try reader.read_int(u64),
            .log_position_last = try reader.read_int(u64),
        };
        return .{
            .reader = reader,
            .stream_id = stream_id,
            .command_id = command_id,
            .expected_version = expected_version,
            .request_hash = request_digest,
            .result = result,
            .event_count = event_count,
            .event_index = 0,
        };
    }

    fn next(self: *BatchDecoder) !?BatchEvent {
        if (self.event_index == self.event_count) {
            try self.reader.finish();
            return null;
        }
        const result: BatchEvent = .{
            .event_id = try self.reader.read_int(u128),
            .event_type = try self.reader.read_bytes_u16(),
            .schema_version = try self.reader.read_int(u32),
            .payload = try self.reader.read_bytes(try self.reader.read_int(u32)),
        };
        self.event_index += 1;
        return result;
    }
};

fn hash_int(hasher: *std.crypto.hash.Blake3, comptime T: type, value: T) void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .big);
    hasher.update(&bytes);
}

fn hash_bytes_u16(hasher: *std.crypto.hash.Blake3, bytes: []const u8) void {
    assert(bytes.len <= std.math.maxInt(u16));
    hash_int(hasher, u16, @intCast(bytes.len));
    hasher.update(bytes);
}

fn request_hash(request: protocol.AppendStream) [request_hash_size]u8 {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hash_bytes_u16(&hasher, request.stream_id);
    hash_bytes_u16(&hasher, request.command_id);
    hash_int(&hasher, u8, @backingInt(request.expected_version));
    const expected_value: u64 = switch (request.expected_version) {
        .empty => 0,
        .exact => |version| version,
    };
    hash_int(&hasher, u64, expected_value);
    hash_int(&hasher, u16, @intCast(request.events.len));
    for (request.events) |append_event| {
        hash_bytes_u16(&hasher, append_event.event_type);
        hash_int(&hasher, u32, append_event.schema_version);
        hash_int(&hasher, u32, @intCast(append_event.payload.len));
        hasher.update(append_event.payload);
    }
    var digest: [request_hash_size]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn batch_checksum(content: []const u8) [batch_checksum_size]u8 {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("compix/append-batch/v2");
    hasher.update(content);
    var digest: [batch_checksum_size]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn hash_batch_request(decoder_start: BatchDecoder) ![request_hash_size]u8 {
    var decoder = decoder_start;
    var hasher = std.crypto.hash.Blake3.init(.{});
    hash_bytes_u16(&hasher, decoder.stream_id);
    hash_bytes_u16(&hasher, decoder.command_id);
    hash_int(&hasher, u8, @backingInt(decoder.expected_version));
    const expected_value: u64 = switch (decoder.expected_version) {
        .empty => 0,
        .exact => |version| version,
    };
    hash_int(&hasher, u64, expected_value);
    hash_int(&hasher, u16, decoder.event_count);
    while (try decoder.next()) |batch_event| {
        const event_index = decoder.event_index - 1;
        const log_position = decoder.result.log_position_first + event_index;
        if (batch_event.event_id != log_position) return error.CorruptStreamData;
        if (batch_event.event_type.len == 0) return error.CorruptStreamData;
        if (batch_event.event_type.len > limits.event_type_length_max)
            return error.CorruptStreamData;
        if (batch_event.payload.len > limits.payload_size_max)
            return error.CorruptStreamData;
        hash_bytes_u16(&hasher, batch_event.event_type);
        hash_int(&hasher, u32, batch_event.schema_version);
        hash_int(&hasher, u32, @intCast(batch_event.payload.len));
        hasher.update(batch_event.payload);
    }
    var digest: [request_hash_size]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn batch_size(request: protocol.AppendStream) !limits.ByteCount {
    var size: u64 = protocol.append_batch_header_size;
    size += request.stream_id.len;
    size += request.command_id.len;
    for (request.events) |append_event| {
        size += protocol.append_event_storage_overhead;
        size += append_event.event_type.len;
        size += append_event.payload.len;
    }
    if (size > protocol.append_batch_size_max) return error.AppendBatchTooLarge;
    return @intCast(size);
}

fn encode_batch(
    target: []u8,
    request: protocol.AppendStream,
    result: protocol.AppendResult,
) ![]const u8 {
    const size = try batch_size(request);
    if (target.len < size) return error.BufferTooSmall;
    const digest = request_hash(request);
    var writer: Writer = .{ .bytes = target[0..size] };
    try writer.write_int(u8, batch_format_version);
    try writer.write_int(u16, @intCast(request.events.len));
    try writer.write_bytes_u16(request.stream_id);
    try writer.write_bytes_u16(request.command_id);
    try writer.write_int(u8, @backingInt(request.expected_version));
    const expected_value: u64 = switch (request.expected_version) {
        .empty => 0,
        .exact => |version| version,
    };
    try writer.write_int(u64, expected_value);
    try writer.write_bytes(&digest);
    try writer.write_int(u64, result.stream_version_first);
    try writer.write_int(u64, result.stream_version_last);
    try writer.write_int(u64, result.log_position_first);
    try writer.write_int(u64, result.log_position_last);
    for (request.events, 0..) |append_event, index| {
        const log_position = result.log_position_first + index;
        try writer.write_int(u128, log_position);
        try writer.write_bytes_u16(append_event.event_type);
        try writer.write_int(u32, append_event.schema_version);
        try writer.write_int(u32, @intCast(append_event.payload.len));
        try writer.write_bytes(append_event.payload);
    }
    const checksum = batch_checksum(writer.bytes[0..writer.offset]);
    try writer.write_bytes(&checksum);
    assert(writer.offset == size);
    return writer.bytes;
}

fn validate_batch(bytes: []const u8) !BatchDecoder {
    const decoder = try BatchDecoder.init(bytes);
    if (decoder.event_count == 0) return error.CorruptStreamData;
    if (decoder.event_count > protocol.append_events_max) return error.CorruptStreamData;
    if (decoder.stream_id.len == 0) return error.CorruptStreamData;
    if (decoder.stream_id.len > protocol.stream_id_length_max)
        return error.CorruptStreamData;
    if (decoder.command_id.len == 0) return error.CorruptStreamData;
    if (decoder.command_id.len > protocol.command_id_length_max)
        return error.CorruptStreamData;
    if (decoder.result.stream_version_first == 0) return error.CorruptStreamData;
    if (decoder.result.log_position_first == 0) return error.CorruptStreamData;
    if (decoder.result.stream_version_last < decoder.result.stream_version_first)
        return error.CorruptStreamData;
    if (decoder.result.log_position_last < decoder.result.log_position_first)
        return error.CorruptStreamData;
    const span: u64 = decoder.event_count - 1;
    if (decoder.result.stream_version_last -| decoder.result.stream_version_first != span)
        return error.CorruptStreamData;
    if (decoder.result.log_position_last -| decoder.result.log_position_first != span)
        return error.CorruptStreamData;
    const expected_first = switch (decoder.expected_version) {
        .empty => @as(u64, 1),
        .exact => |version| std.math.add(u64, version, 1) catch
            return error.CorruptStreamData,
    };
    if (decoder.result.stream_version_first != expected_first)
        return error.CorruptStreamData;
    const calculated_hash = try hash_batch_request(decoder);
    if (!std.mem.eql(u8, &calculated_hash, &decoder.request_hash))
        return error.CorruptStreamData;
    return decoder;
}

const StreamHeadSlot = struct {
    stream_id_offset: limits.ByteCount,
    stream_id_length: u16,
    hash: u64,
    version: protocol.StreamVersion,
    occupied: bool,
};

const SlotLookup = struct {
    index: limits.StreamSlotIndex,
    found: bool,
};

pub const CommitFaultInjector = struct {
    fail_after_member: ?u16 = null,
    fail_before_sync: bool = false,
    fail_after_sync: bool = false,
    members_written: u16 = 0,
    triggered: bool = false,

    fn reset(self: *CommitFaultInjector) void {
        self.members_written = 0;
        self.triggered = false;
    }

    fn after_member(self: *CommitFaultInjector) !void {
        self.members_written += 1;
        if (self.fail_after_member == self.members_written) return self.fail();
    }

    fn boundary(self: *CommitFaultInjector, before_sync: bool) !void {
        if ((before_sync and self.fail_before_sync) or
            (!before_sync and self.fail_after_sync)) return self.fail();
    }

    fn fail(self: *CommitFaultInjector) error{InjectedAppendFault} {
        self.triggered = true;
        return error.InjectedAppendFault;
    }
};

fn stream_hash(stream_id: []const u8) u64 {
    var hash: u64 = 14695981039346656037;
    for (stream_id) |byte| {
        hash ^= byte;
        hash *%= 1099511628211;
    }
    return hash;
}

fn stream_slot_capacity(stream_capacity: limits.StreamCount) !limits.StreamCount {
    const required = std.math.mul(limits.StreamCount, stream_capacity, 2) catch
        return error.InvalidStreamCapacity;
    var slots: limits.StreamCount = 1;
    while (slots < required) {
        slots = std.math.mul(limits.StreamCount, slots, 2) catch
            return error.InvalidStreamCapacity;
    }
    assert(std.math.isPowerOfTwo(slots));
    assert(slots > stream_capacity);
    return slots;
}

pub const StreamStore = struct {
    allocator: std.mem.Allocator,
    lsm: *LSMEngine,
    stream_heads: []StreamHeadSlot,
    stream_ids: fixed.ByteArena,
    stream_id_storage: []u8,
    stream_capacity: limits.StreamCount,
    stream_count: limits.StreamCount,
    log_position_committed: protocol.LogPosition,
    log_position_durable: protocol.LogPosition,
    append_scratch: []u8,
    batch_scratch: []u8,
    lookup_scratch: []u8,
    poisoned: bool,
    fault_injector: ?*CommitFaultInjector,

    pub const InitOptions = struct {
        stream_capacity: limits.StreamCount,
        stream_id_bytes_capacity: limits.ByteCount,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        lsm: *LSMEngine,
        options: InitOptions,
    ) !StreamStore {
        if (options.stream_capacity == 0 or options.stream_capacity > limits.streams_max)
            return error.InvalidStreamCapacity;
        if (options.stream_id_bytes_capacity < options.stream_capacity or
            options.stream_id_bytes_capacity > limits.stream_id_bytes_capacity_max)
            return error.InvalidStreamIdByteCapacity;
        const slot_capacity = try stream_slot_capacity(options.stream_capacity);
        const stream_heads = try allocator.alloc(StreamHeadSlot, slot_capacity);
        errdefer allocator.free(stream_heads);
        for (stream_heads) |*slot| slot.occupied = false;
        const stream_id_storage = try allocator.alloc(u8, options.stream_id_bytes_capacity);
        errdefer allocator.free(stream_id_storage);
        const append_scratch = try allocator.alloc(u8, protocol.append_batch_size_max);
        errdefer allocator.free(append_scratch);
        const batch_scratch = try allocator.alloc(u8, limits.event_serialized_size_max);
        errdefer allocator.free(batch_scratch);
        const lookup_scratch = try allocator.alloc(u8, limits.event_serialized_size_max);
        errdefer allocator.free(lookup_scratch);
        var target: StreamStore = .{
            .allocator = allocator,
            .lsm = lsm,
            .stream_heads = stream_heads,
            .stream_ids = try fixed.ByteArena.init(stream_id_storage),
            .stream_id_storage = stream_id_storage,
            .stream_capacity = options.stream_capacity,
            .stream_count = 0,
            .log_position_committed = 0,
            .log_position_durable = 0,
            .append_scratch = append_scratch,
            .batch_scratch = batch_scratch,
            .lookup_scratch = lookup_scratch,
            .poisoned = false,
            .fault_injector = null,
        };
        try target.rebuild_heads();
        target.log_position_durable = target.log_position_committed;
        return target;
    }

    pub fn deinit(self: *StreamStore) void {
        self.allocator.free(self.lookup_scratch);
        self.allocator.free(self.batch_scratch);
        self.allocator.free(self.append_scratch);
        self.allocator.free(self.stream_id_storage);
        self.allocator.free(self.stream_heads);
    }

    pub fn set_fault_injector(self: *StreamStore, injector: ?*CommitFaultInjector) void {
        self.fault_injector = injector;
    }

    fn stored_stream_id(self: *const StreamStore, slot: *const StreamHeadSlot) []const u8 {
        assert(slot.occupied);
        const start = limits.usize_from(slot.stream_id_offset);
        return self.stream_id_storage[start..][0..slot.stream_id_length];
    }

    fn find_slot(self: *const StreamStore, stream_id: []const u8) !SlotLookup {
        const hash = stream_hash(stream_id);
        const mask: u64 = self.stream_heads.len - 1;
        const start = hash & mask;
        for (0..self.stream_heads.len) |probe| {
            const index: limits.StreamSlotIndex = @intCast((start + probe) & mask);
            const slot = &self.stream_heads[index];
            if (!slot.occupied) return .{ .index = index, .found = false };
            if (slot.hash != hash) continue;
            const stored = self.stored_stream_id(slot);
            if (std.mem.eql(u8, stored, stream_id)) return .{ .index = index, .found = true };
        }
        return error.StreamTableFull;
    }

    fn stream_version(self: *const StreamStore, stream_id: []const u8) !u64 {
        const lookup = try self.find_slot(stream_id);
        if (!lookup.found) return 0;
        return self.stream_heads[lookup.index].version;
    }

    fn set_stream_version(self: *StreamStore, stream_id: []const u8, version: u64) !void {
        const lookup = try self.find_slot(stream_id);
        const slot = &self.stream_heads[lookup.index];
        if (lookup.found) {
            if (version > slot.version) slot.version = version;
            return;
        }
        if (self.stream_count == self.stream_capacity) return error.StreamLimitExceeded;
        const stream_id_length = try limits.byte_count(stream_id.len);
        if (stream_id_length > self.stream_ids.remaining())
            return error.StreamIdByteCapacityExceeded;
        const stream_id_offset = self.stream_ids.used;
        _ = self.stream_ids.write(stream_id) catch unreachable;
        slot.stream_id_offset = stream_id_offset;
        slot.stream_id_length = @intCast(stream_id.len);
        slot.hash = stream_hash(stream_id);
        slot.version = version;
        slot.occupied = true;
        self.stream_count += 1;
        assert(self.stream_count <= self.stream_capacity);
    }

    const RebuildContext = struct {
        target: *StreamStore,

        fn visit(self: *RebuildContext, key: []const u8, value: EventValue) !event.VisitControl {
            if (!std.mem.eql(u8, value.origin_service, origin_service))
                return error.CorruptStreamData;
            if (!std.mem.eql(u8, value.event_type, batch_event_type))
                return error.CorruptStreamData;
            const decoder = try validate_batch(value.payload);
            var key_buffer: [limits.key_length_max]u8 = undefined;
            const expected_key = try batch_key(&key_buffer, decoder.result.log_position_first);
            if (!std.mem.eql(u8, key, expected_key)) return error.CorruptStreamData;
            if (value.sequence != decoder.result.log_position_last)
                return error.CorruptStreamData;
            try self.target.set_stream_version(
                decoder.stream_id,
                decoder.result.stream_version_last,
            );
            self.target.log_position_committed = @max(
                self.target.log_position_committed,
                decoder.result.log_position_last,
            );
            return .continue_scan;
        }
    };

    fn rebuild_heads(self: *StreamStore) !void {
        var context: RebuildContext = .{ .target = self };
        _ = try self.lsm.visit_prefix(batch_key_prefix, &context, RebuildContext.visit);
        assert(self.stream_count <= self.stream_capacity);
    }

    pub const Verification = struct {
        batches: u64,
        events: u64,
        streams: limits.StreamCount,
        committed_log_position: protocol.LogPosition,
    };

    pub const Metrics = struct {
        streams_used: limits.StreamCount,
        streams_max: limits.StreamCount,
        stream_id_bytes_used: limits.ByteCount,
        stream_id_bytes_max: limits.ByteCount,
        committed_log_position: protocol.LogPosition,
        poisoned: bool,
    };

    pub fn metrics(self: *const StreamStore) Metrics {
        return .{
            .streams_used = self.stream_count,
            .streams_max = self.stream_capacity,
            .stream_id_bytes_used = self.stream_ids.used,
            .stream_id_bytes_max = self.stream_ids.capacity(),
            .committed_log_position = self.log_position_durable,
            .poisoned = self.poisoned,
        };
    }

    const VerifyContext = struct {
        target: *StreamStore,
        next_log_position: protocol.LogPosition = 1,
        batches: u64 = 0,
        events: u64 = 0,

        fn visit(self: *VerifyContext, key: []const u8, value: EventValue) !event.VisitControl {
            if (!std.mem.eql(u8, value.origin_service, origin_service))
                return error.CorruptStreamData;
            if (!std.mem.eql(u8, value.event_type, batch_event_type))
                return error.CorruptStreamData;
            var decoder = try validate_batch(value.payload);
            if (decoder.result.log_position_first != self.next_log_position)
                return error.CorruptStreamData;
            var key_buffer: [limits.key_length_max]u8 = undefined;
            const expected_batch_key = try batch_key(
                &key_buffer,
                decoder.result.log_position_first,
            );
            if (!std.mem.eql(u8, key, expected_batch_key))
                return error.CorruptStreamData;
            if (value.sequence != decoder.result.log_position_last)
                return error.CorruptStreamData;

            var event_index: limits.EventCount = 0;
            while (try decoder.next()) |_| : (event_index += 1) {
                const log_position = decoder.result.log_position_first + event_index;
                const event_stream_version = decoder.result.stream_version_first + event_index;
                const log_ref = try self.target.lookup_event(null, log_position) orelse
                    return error.CorruptStreamData;
                if (log_ref.batch_first != decoder.result.log_position_first or
                    log_ref.event_index != event_index) return error.CorruptStreamData;
                const stream_ref = try self.target.lookup_event(
                    decoder.stream_id,
                    event_stream_version,
                ) orelse return error.CorruptStreamData;
                if (stream_ref.batch_first != decoder.result.log_position_first or
                    stream_ref.event_index != event_index) return error.CorruptStreamData;
            }
            if (event_index != decoder.event_count) return error.CorruptStreamData;

            const command_index_key = try command_key(&key_buffer, decoder.command_id);
            const command = try self.target.lsm.get_bounded(
                command_index_key,
                self.target.lookup_scratch,
            ) orelse return error.CorruptStreamData;
            if (!std.mem.eql(u8, command.origin_service, origin_service) or
                !std.mem.eql(u8, command.event_type, command_index_event_type) or
                command.sequence != decoder.result.log_position_last or
                command.payload.len != command_index_payload_size)
                return error.CorruptStreamData;
            if (std.mem.readInt(u64, command.payload[0..8], .big) !=
                decoder.result.log_position_first) return error.CorruptStreamData;
            if (!std.mem.eql(
                u8,
                command.payload[8..command_index_payload_size],
                &decoder.request_hash,
            )) return error.CorruptStreamData;

            self.next_log_position = decoder.result.log_position_last + 1;
            self.batches += 1;
            self.events += decoder.event_count;
            return .continue_scan;
        }
    };

    pub fn verify_integrity(self: *StreamStore) !Verification {
        var context: VerifyContext = .{ .target = self };
        _ = try self.lsm.visit_prefix(batch_key_prefix, &context, VerifyContext.visit);
        const verified_position = context.next_log_position - 1;
        if (verified_position != self.log_position_committed) return error.CorruptStreamData;
        return .{
            .batches = context.batches,
            .events = context.events,
            .streams = self.stream_count,
            .committed_log_position = verified_position,
        };
    }

    pub fn append(
        self: *StreamStore,
        request: protocol.AppendStream,
        timestamp: limits.Timestamp,
    ) !protocol.AppendStreamResponse {
        return self.append_internal(request, timestamp, true);
    }

    pub fn stage_append(
        self: *StreamStore,
        request: protocol.AppendStream,
        timestamp: limits.Timestamp,
    ) !protocol.AppendStreamResponse {
        return self.append_internal(request, timestamp, false);
    }

    fn append_internal(
        self: *StreamStore,
        request: protocol.AppendStream,
        timestamp: limits.Timestamp,
        sync_now: bool,
    ) !protocol.AppendStreamResponse {
        if (self.poisoned) return error.StorePoisoned;
        try protocol.validate_append(request);
        const digest = request_hash(request);
        if (try self.command_result(request.command_id, digest)) |response| return response;
        const version_actual = try self.stream_version(request.stream_id);
        if (!expected_matches(request.expected_version, version_actual)) {
            return .{ .version_conflict = version_actual };
        }
        const slot_lookup = try self.find_slot(request.stream_id);
        if (slot_lookup.found) {
            assert(self.stream_count > 0);
        } else {
            if (self.stream_count == self.stream_capacity) return error.StreamLimitExceeded;
        }
        const event_count: u64 = @intCast(request.events.len);
        const stream_first = std.math.add(u64, version_actual, 1) catch
            return error.StreamVersionOverflow;
        const stream_last = std.math.add(u64, version_actual, event_count) catch
            return error.StreamVersionOverflow;
        const log_first = std.math.add(u64, self.log_position_committed, 1) catch
            return error.LogPositionOverflow;
        const log_last = std.math.add(u64, self.log_position_committed, event_count) catch
            return error.LogPositionOverflow;
        const result: protocol.AppendResult = .{
            .stream_version_first = stream_first,
            .stream_version_last = stream_last,
            .log_position_first = log_first,
            .log_position_last = log_last,
        };
        const encoded = try encode_batch(self.append_scratch, request, result);
        self.commit(request, digest, result, encoded, timestamp) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.set_stream_version(request.stream_id, stream_last) catch unreachable;
        self.log_position_committed = log_last;
        assert(try self.stream_version(request.stream_id) == stream_last);
        assert(self.log_position_committed == log_last);
        if (sync_now) try self.durable_sync();
        return .{ .committed = result };
    }

    pub fn durable_sync(self: *StreamStore) !void {
        if (self.fault_injector) |injector| injector.boundary(true) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.lsm.sync() catch |err| {
            self.poisoned = true;
            return err;
        };
        if (self.fault_injector) |injector| injector.boundary(false) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.log_position_durable = self.log_position_committed;
    }

    fn commit(
        self: *StreamStore,
        request: protocol.AppendStream,
        digest: [request_hash_size]u8,
        result: protocol.AppendResult,
        encoded: []const u8,
        timestamp: limits.Timestamp,
    ) !void {
        if (self.fault_injector) |injector| injector.reset();
        for (request.events, 0..) |_, index| {
            const event_index: limits.EventCount = @intCast(index);
            const log_position = result.log_position_first + index;
            const event_stream_version = result.stream_version_first + index;
            var payload: [index_payload_size]u8 = undefined;
            write_index_payload(&payload, result.log_position_first, event_index);
            var key_buffer: [limits.key_length_max]u8 = undefined;
            const log_index_key = try log_key(&key_buffer, log_position);
            try self.put_meta(
                log_index_key,
                &payload,
                log_position,
                timestamp,
                event_index_event_type,
            );
            const stream_index_key = try stream_key(
                &key_buffer,
                request.stream_id,
                event_stream_version,
            );
            try self.put_meta(
                stream_index_key,
                &payload,
                log_position,
                timestamp,
                event_index_event_type,
            );
        }
        var command_payload: [command_index_payload_size]u8 = undefined;
        std.mem.writeInt(u64, command_payload[0..8], result.log_position_first, .big);
        @memcpy(command_payload[8..], &digest);
        var command_key_buffer: [limits.key_length_max]u8 = undefined;
        const command_index_key = try command_key(&command_key_buffer, request.command_id);
        try self.put_meta(
            command_index_key,
            &command_payload,
            result.log_position_last,
            timestamp,
            command_index_event_type,
        );
        var batch_key_buffer: [limits.key_length_max]u8 = undefined;
        const commit_key = try batch_key(&batch_key_buffer, result.log_position_first);
        try self.put_meta(
            commit_key,
            encoded,
            result.log_position_last,
            timestamp,
            batch_event_type,
        );
    }

    fn put_meta(
        self: *StreamStore,
        key: []const u8,
        payload: []const u8,
        sequence: u64,
        timestamp: limits.Timestamp,
        event_type_name: []const u8,
    ) !void {
        const value = try EventValue.init(.{
            .sequence = sequence,
            .origin_service = origin_service,
            .event_type = event_type_name,
            .payload = payload,
            .timestamp = timestamp,
        });
        try self.lsm.put(key, value);
        if (self.fault_injector) |injector| try injector.after_member();
    }

    fn command_result(
        self: *StreamStore,
        command_id: []const u8,
        digest: [request_hash_size]u8,
    ) !?protocol.AppendStreamResponse {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        const key = try command_key(&key_buffer, command_id);
        const value = try self.lsm.get_bounded(key, self.lookup_scratch) orelse return null;
        if (!std.mem.eql(u8, value.event_type, command_index_event_type))
            return error.CorruptStreamData;
        if (value.payload.len != command_index_payload_size)
            return error.CorruptStreamData;
        const batch_first = std.mem.readInt(u64, value.payload[0..8], .big);
        const stored_hash = value.payload[8..command_index_payload_size];
        const batch = try self.load_batch(batch_first) orelse return null;
        const decoder = try validate_batch(batch);
        if (!std.mem.eql(u8, decoder.command_id, command_id))
            return error.CorruptStreamData;
        if (!std.mem.eql(u8, stored_hash, &decoder.request_hash))
            return error.CorruptStreamData;
        if (!std.mem.eql(u8, stored_hash, &digest)) return .command_conflict;
        return .{ .replayed = decoder.result };
    }

    fn load_batch(self: *StreamStore, log_position_first: u64) !?[]const u8 {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        const key = try batch_key(&key_buffer, log_position_first);
        const value = try self.lsm.get_bounded(key, self.batch_scratch) orelse return null;
        if (!std.mem.eql(u8, value.origin_service, origin_service))
            return error.CorruptStreamData;
        if (!std.mem.eql(u8, value.event_type, batch_event_type))
            return error.CorruptStreamData;
        return value.payload;
    }

    pub fn read_stream(
        self: *StreamStore,
        request: protocol.ReadStream,
        output: []protocol.ReadEvent,
        output_bytes: []u8,
    ) !protocol.ReadStreamResponse {
        if (self.poisoned) return error.StorePoisoned;
        const head = try self.stream_version(request.stream_id);
        const page = try self.read_page(
            request.stream_id,
            request.from_version,
            request.max_count,
            head,
            self.log_position_durable,
            .{},
            output,
            output_bytes,
        );
        return .{ .next_version = page.next_cursor, .events = page.events };
    }

    pub fn read_log(
        self: *StreamStore,
        request: protocol.ReadLog,
        output: []protocol.ReadEvent,
        output_bytes: []u8,
    ) !protocol.ReadLogResponse {
        if (self.poisoned) return error.StorePoisoned;
        const page = try self.read_page(
            null,
            request.from_position,
            request.max_count,
            self.log_position_durable,
            self.log_position_durable,
            .{},
            output,
            output_bytes,
        );
        return .{ .next_position = page.next_cursor, .events = page.events };
    }

    pub const SubscriptionQuery = struct {
        /// Empty reads the whole ordered log; otherwise one stream.
        stream_id: []const u8,
        /// A log position for the whole log, a stream version for one stream.
        from: u64,
        /// Empty keeps every event type.
        event_types: []const []const u8,
    };

    pub const SubscriptionPage = struct {
        /// Resume cursor; past every examined event, kept or skipped.
        next: u64,
        /// The durable log position the page was read at.
        committed: protocol.LogPosition,
        events: []protocol.ReadEvent,
    };

    /// One durable page for a subscription, sized for a subscription batch.
    ///
    /// At most `subscription_scan_batches_max` batches are loaded, so a filter that matches
    /// nothing still costs the store's owner bounded work per page.
    pub fn read_subscription_page(
        self: *StreamStore,
        query: SubscriptionQuery,
        output: []protocol.ReadEvent,
        output_bytes: []u8,
    ) !SubscriptionPage {
        if (self.poisoned) return error.StorePoisoned;
        assert(query.event_types.len <= limits.subscription_event_types_max);
        assert(query.stream_id.len <= protocol.stream_id_length_max);
        const whole_log = query.stream_id.len == 0;
        const head = if (whole_log)
            self.log_position_durable
        else
            try self.stream_version(query.stream_id);
        const page = try self.read_page(
            if (whole_log) null else query.stream_id,
            query.from,
            0,
            head,
            self.log_position_durable,
            .{
                .event_types = query.event_types,
                .batches_max = limits.subscription_scan_batches_max,
                .header_size = protocol.subscription_batch_header_size,
            },
            output,
            output_bytes,
        );
        assert(page.next_cursor >= query.from);
        return .{
            .next = page.next_cursor,
            .committed = self.log_position_durable,
            .events = page.events,
        };
    }

    const ReadPage = struct {
        next_cursor: u64,
        events: []protocol.ReadEvent,
    };

    const PageFilter = struct {
        /// Empty keeps every event.
        event_types: []const []const u8 = &.{},
        /// Batches loaded before the page ends; each costs one LSM lookup.
        batches_max: u32 = std.math.maxInt(u32),
        /// Response bytes ahead of the events; the page stops before the frame would overflow.
        header_size: limits.ByteCount = 2 + 8 + 2,

        fn keeps(filter: PageFilter, event_type: []const u8) bool {
            if (filter.event_types.len == 0) return true;
            for (filter.event_types) |wanted| {
                if (std.mem.eql(u8, wanted, event_type)) return true;
            }
            return false;
        }
    };

    fn read_page(
        self: *StreamStore,
        stream_id: ?[]const u8,
        from_cursor: u64,
        count_max_request: limits.EventCount,
        head: u64,
        durable_max: u64,
        filter: PageFilter,
        output: []protocol.ReadEvent,
        output_bytes: []u8,
    ) !ReadPage {
        assert(filter.batches_max > 0);
        const count_max = if (count_max_request == 0)
            protocol.read_events_max
        else
            count_max_request;
        if (count_max > output.len) return error.EventBufferTooSmall;
        var arena = try fixed.ByteArena.init(output_bytes);
        const cursor_start: u64 = if (from_cursor == 0) 1 else from_cursor;
        var cursor: u64 = cursor_start;
        var count: limits.EventCount = 0;
        var wire_size: limits.ByteCount = filter.header_size;
        // NOTE: log positions are contiguous across batches, so once a batch is consumed whole the
        // cursor is the next batch's key and the ordered-log index lookup can be skipped.
        var batch_aligned = false;
        var batches: u32 = 0;
        while (count < count_max and batches < filter.batches_max and cursor <= head) {
            assert(cursor > 0);
            const index_ref: IndexRef = if (batch_aligned)
                .{ .batch_first = cursor, .event_index = 0 }
            else
                try self.lookup_event(stream_id, cursor) orelse return error.CorruptStreamData;
            const batch_bytes = try self.load_batch(index_ref.batch_first) orelse
                return error.CorruptStreamData;
            batches += 1;
            var decoder = try validate_batch(batch_bytes);
            const batch_consumed = try append_batch_events(
                &decoder,
                index_ref.event_index,
                stream_id,
                filter,
                &cursor,
                count_max,
                durable_max,
                output,
                &count,
                &arena,
                &wire_size,
            );
            if (!batch_consumed) break;
            batch_aligned = stream_id == null;
        }
        assert(cursor >= cursor_start);
        assert(count <= cursor - cursor_start);
        const next_cursor = if (cursor == cursor_start) from_cursor else cursor;
        return .{ .next_cursor = next_cursor, .events = output[0..count] };
    }

    const IndexRef = struct {
        batch_first: u64,
        event_index: limits.EventCount,
    };

    fn lookup_event(
        self: *StreamStore,
        stream_id: ?[]const u8,
        cursor: u64,
    ) !?IndexRef {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        const key = if (stream_id) |id|
            try stream_key(&key_buffer, id, cursor)
        else
            try log_key(&key_buffer, cursor);
        const value = try self.lsm.get_bounded(key, self.lookup_scratch) orelse return null;
        if (!std.mem.eql(u8, value.event_type, event_index_event_type))
            return error.CorruptStreamData;
        if (value.payload.len != index_payload_size) return error.CorruptStreamData;
        return .{
            .batch_first = std.mem.readInt(u64, value.payload[0..8], .big),
            .event_index = std.mem.readInt(u16, value.payload[8..10], .big),
        };
    }
};

fn append_batch_events(
    decoder: *BatchDecoder,
    event_index_start: limits.EventCount,
    stream_filter: ?[]const u8,
    filter: StreamStore.PageFilter,
    cursor: *u64,
    count_max: limits.EventCount,
    durable_max: u64,
    output: []protocol.ReadEvent,
    count: *limits.EventCount,
    arena: *fixed.ByteArena,
    wire_size: *limits.ByteCount,
) !bool {
    if (event_index_start >= decoder.event_count) return error.CorruptStreamData;
    if (stream_filter) |stream_id| {
        if (!std.mem.eql(u8, stream_id, decoder.stream_id))
            return error.CorruptStreamData;
    }
    while (try decoder.next()) |batch_event| {
        const event_index = decoder.event_index - 1;
        if (event_index < event_index_start) continue;
        if (count.* == count_max) return false;
        const stream_version = decoder.result.stream_version_first + event_index;
        const log_position = decoder.result.log_position_first + event_index;
        if (log_position > durable_max) return false;
        const expected_cursor = if (stream_filter) |_| stream_version else log_position;
        if (cursor.* != expected_cursor) return error.CorruptStreamData;
        if (!filter.keeps(batch_event.event_type)) {
            cursor.* += 1;
            continue;
        }
        const event_wire_size = try read_event_wire_size(decoder.*, batch_event);
        if (event_wire_size > protocol.frame_payload_size_max - wire_size.*) {
            if (count.* == 0) return error.ReadEventTooLarge;
            return false;
        }
        const target = &output[count.*];
        target.* = .{
            .event_id = batch_event.event_id,
            .stream_id = try arena.write(decoder.stream_id),
            .stream_version = stream_version,
            .log_position = log_position,
            .command_id = try arena.write(decoder.command_id),
            .event_type = try arena.write(batch_event.event_type),
            .schema_version = batch_event.schema_version,
            .payload = try arena.write(batch_event.payload),
        };
        wire_size.* += event_wire_size;
        count.* += 1;
        cursor.* += 1;
    }
    return true;
}

fn read_event_wire_size(decoder: BatchDecoder, batch_event: BatchEvent) !limits.ByteCount {
    const fixed_size: u64 = 16 + 2 + 8 + 8 + 2 + 2 + 4 + 4;
    const total = fixed_size +
        decoder.stream_id.len +
        decoder.command_id.len +
        batch_event.event_type.len +
        batch_event.payload.len;
    if (total > std.math.maxInt(limits.ByteCount)) return error.ReadEventTooLarge;
    return @intCast(total);
}

fn expected_matches(expected: protocol.ExpectedVersion, actual: u64) bool {
    return switch (expected) {
        .empty => actual == 0,
        .exact => |version| actual == version,
    };
}

fn write_index_payload(target: *[index_payload_size]u8, batch_first: u64, index: u16) void {
    std.mem.writeInt(u64, target[0..8], batch_first, .big);
    std.mem.writeInt(u16, target[8..10], index, .big);
}

fn write_hex_key(buffer: []u8, prefix: []const u8, bytes: []const u8) ![]const u8 {
    const size = prefix.len + bytes.len * 2;
    if (size > buffer.len) return error.BufferTooSmall;
    @memcpy(buffer[0..prefix.len], prefix);
    const alphabet = "0123456789abcdef";
    for (bytes, 0..) |byte, index| {
        buffer[prefix.len + index * 2] = alphabet[byte >> 4];
        buffer[prefix.len + index * 2 + 1] = alphabet[byte & 0x0f];
    }
    return buffer[0..size];
}

fn batch_key(buffer: []u8, log_position_first: u64) ![]const u8 {
    return std.fmt.bufPrint(
        buffer,
        batch_key_prefix ++ "{x:0>16}",
        .{log_position_first},
    ) catch error.BufferTooSmall;
}

fn log_key(buffer: []u8, log_position: u64) ![]const u8 {
    return std.fmt.bufPrint(
        buffer,
        log_key_prefix ++ "{x:0>16}",
        .{log_position},
    ) catch error.BufferTooSmall;
}

fn command_key(buffer: []u8, command_id: []const u8) ![]const u8 {
    return write_hex_key(buffer, command_key_prefix, command_id);
}

fn stream_key(buffer: []u8, stream_id: []const u8, stream_version: u64) ![]const u8 {
    const prefix = try write_hex_key(buffer, stream_key_prefix, stream_id);
    if (prefix.len + 1 + 16 > buffer.len) return error.BufferTooSmall;
    buffer[prefix.len] = ':';
    const suffix = std.fmt.bufPrint(
        buffer[prefix.len + 1 ..],
        "{x:0>16}",
        .{stream_version},
    ) catch return error.BufferTooSmall;
    return buffer[0 .. prefix.len + 1 + suffix.len];
}

test "batch encoding validates its positive and negative space" {
    const events = [_]protocol.AppendEvent{
        .{ .event_type = "source.generation.requested", .schema_version = 1, .payload = "one" },
        .{ .event_type = "source.discovery.requested", .schema_version = 2, .payload = "two" },
    };
    const request: protocol.AppendStream = .{
        .stream_id = "source-generation/42",
        .expected_version = .empty,
        .command_id = "command-42",
        .events = &events,
    };
    const result: protocol.AppendResult = .{
        .stream_version_first = 1,
        .stream_version_last = 2,
        .log_position_first = 10,
        .log_position_last = 11,
    };
    var buffer: [1024]u8 = undefined;
    const encoded = try encode_batch(&buffer, request, result);
    var decoder = try validate_batch(encoded);
    try std.testing.expectEqualStrings(request.stream_id, decoder.stream_id);
    try std.testing.expectEqualStrings(request.command_id, decoder.command_id);
    try std.testing.expectEqual(@as(u128, 10), (try decoder.next()).?.event_id);
    try std.testing.expectEqual(@as(u128, 11), (try decoder.next()).?.event_id);
    try std.testing.expect((try decoder.next()) == null);

    buffer[encoded.len - 1] ^= 1;
    try std.testing.expectError(error.CorruptStreamData, validate_batch(encoded));
}

test "stream capacity is configurable beyond the former 4096 lifetime ceiling" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const stem = "test_stream_capacity_large";
    delete_test_files(io, stem);
    defer delete_test_files(io, stem);
    const capacity: limits.StreamCount = 4_100;
    const wal_path = "data/test_stream_capacity_large.wal";
    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        var stream_id_buffer: [32]u8 = undefined;
        var command_id_buffer: [32]u8 = undefined;
        var encoded_buffer: [1024]u8 = undefined;
        const events = [_]protocol.AppendEvent{.{
            .event_type = "capacity.created",
            .schema_version = 1,
            .payload = "value",
        }};
        for (0..capacity) |index| {
            const position: u64 = index + 1;
            const stream_id = try std.fmt.bufPrint(
                &stream_id_buffer,
                "generation/{d}",
                .{index},
            );
            const command_id = try std.fmt.bufPrint(
                &command_id_buffer,
                "capacity-{d}",
                .{index},
            );
            const request: protocol.AppendStream = .{
                .stream_id = stream_id,
                .expected_version = .empty,
                .command_id = command_id,
                .events = &events,
            };
            const encoded = try encode_batch(&encoded_buffer, request, .{
                .stream_version_first = 1,
                .stream_version_last = 1,
                .log_position_first = position,
                .log_position_last = position,
            });
            var key_buffer: [limits.key_length_max]u8 = undefined;
            const key = try batch_key(&key_buffer, position);
            try lsm.put(key, try EventValue.init(.{
                .sequence = position,
                .origin_service = origin_service,
                .event_type = batch_event_type,
                .payload = encoded,
                .timestamp = 1,
            }));
        }
        try lsm.sync();
    }
    var lsm = try LSMEngine.init(allocator, wal_path, io);
    defer lsm.deinit();
    var streams = try StreamStore.init(allocator, &lsm, .{
        .stream_capacity = capacity,
        .stream_id_bytes_capacity = 128 * 1024,
    });
    defer streams.deinit();
    try std.testing.expectEqual(capacity, streams.stream_count);
    try std.testing.expectEqual(@as(u64, capacity), streams.log_position_committed);
    try std.testing.expectEqual(@as(u64, 1), try streams.stream_version("generation/4099"));
    try std.testing.expectError(
        error.StreamLimitExceeded,
        streams.set_stream_version("generation/4100", 1),
    );
    try std.testing.expect(streams.stream_count > 4096);
}

test "stream count and ID byte capacities fail without partial insertion" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const stem = "test_stream_capacity_bounds";
    delete_test_files(io, stem);
    defer delete_test_files(io, stem);
    var lsm = try LSMEngine.init(allocator, "data/test_stream_capacity_bounds.wal", io);
    defer lsm.deinit();
    var streams = try StreamStore.init(allocator, &lsm, .{
        .stream_capacity = 2,
        .stream_id_bytes_capacity = 2,
    });
    defer streams.deinit();

    try streams.set_stream_version("a", 1);
    try streams.set_stream_version("b", 1);
    try std.testing.expectError(error.StreamLimitExceeded, streams.set_stream_version("c", 1));
    try std.testing.expectEqual(@as(limits.StreamCount, 2), streams.stream_count);
    try std.testing.expectEqual(@as(limits.ByteCount, 2), streams.stream_ids.used);
}

test "stream head memory is compact and default capacity is production scale" {
    try std.testing.expect(@sizeOf(StreamHeadSlot) <= 32);
    try std.testing.expect(limits.stream_capacity_default >= 1_000_000);
    try std.testing.expectEqual(
        @as(limits.StreamCount, 2_097_152),
        try stream_slot_capacity(limits.stream_capacity_default),
    );
}

fn delete_test_files(io: std.Io, stem: []const u8) void {
    var path_buffer: [256]u8 = undefined;
    const suffixes = [_][]const u8{ ".wal", ".crumb", ".manifest" };
    for (suffixes) |suffix| {
        const path = std.fmt.bufPrint(&path_buffer, "data/{s}{s}", .{ stem, suffix }) catch
            unreachable;
        std.Io.Dir.cwd().deleteFile(io, path) catch {};
    }
}

fn expect_committed(response: protocol.AppendStreamResponse) !protocol.AppendResult {
    return switch (response) {
        .committed => |result| result,
        else => error.ExpectedCommitted,
    };
}

fn expect_replayed(response: protocol.AppendStreamResponse) !protocol.AppendResult {
    return switch (response) {
        .replayed => |result| result,
        else => error.ExpectedReplayed,
    };
}

test "atomic append enforces versions and command idempotency" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const stem = "test_stream_atomic";
    delete_test_files(io, stem);
    defer delete_test_files(io, stem);

    var lsm = try LSMEngine.init(allocator, "data/test_stream_atomic.wal", io);
    defer lsm.deinit();
    var streams = try StreamStore.init(allocator, &lsm, .{
        .stream_capacity = limits.stream_capacity_test_default,
        .stream_id_bytes_capacity = limits.stream_id_bytes_capacity_test_default,
    });
    defer streams.deinit();
    const events = [_]protocol.AppendEvent{
        .{ .event_type = "source.generation.requested", .schema_version = 1, .payload = "a" },
        .{ .event_type = "source.discovery.requested", .schema_version = 1, .payload = "b" },
    };
    const request: protocol.AppendStream = .{
        .stream_id = "source-generation/1",
        .expected_version = .empty,
        .command_id = "command-1",
        .events = &events,
    };
    const committed = try expect_committed(try streams.append(request, 100));
    try std.testing.expectEqual(@as(u64, 1), committed.stream_version_first);
    try std.testing.expectEqual(@as(u64, 2), committed.log_position_last);
    const replayed = try expect_replayed(try streams.append(request, 200));
    try std.testing.expectEqual(committed, replayed);

    const changed_events = [_]protocol.AppendEvent{.{
        .event_type = "source.generation.requested",
        .schema_version = 1,
        .payload = "changed",
    }};
    const changed = try streams.append(.{
        .stream_id = request.stream_id,
        .expected_version = .{ .exact = 2 },
        .command_id = request.command_id,
        .events = &changed_events,
    }, 300);
    try std.testing.expect(changed == .command_conflict);
    const conflict = try streams.append(.{
        .stream_id = request.stream_id,
        .expected_version = .empty,
        .command_id = "command-2",
        .events = &changed_events,
    }, 300);
    try std.testing.expectEqual(@as(u64, 2), conflict.version_conflict);
    const verification = try streams.verify_integrity();
    try std.testing.expectEqual(@as(u64, 1), verification.batches);
    try std.testing.expectEqual(@as(u64, 2), verification.events);
    try std.testing.expectEqual(@as(limits.StreamCount, 1), verification.streams);
    try std.testing.expectEqual(@as(u64, 2), verification.committed_log_position);
}

test "append crash injection at every batch member and sync boundary is all or none" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    for (1..7) |member| {
        try run_append_fault_case(allocator, io, .{
            .fail_after_member = @intCast(member),
        }, member == 6);
    }
    try run_append_fault_case(allocator, io, .{ .fail_before_sync = true }, true);
    try run_append_fault_case(allocator, io, .{ .fail_after_sync = true }, true);
}

fn run_append_fault_case(
    allocator: std.mem.Allocator,
    io: std.Io,
    injector_start: CommitFaultInjector,
    visible_after_restart: bool,
) !void {
    const stem = "test_stream_fault_boundary";
    const wal_path = "data/test_stream_fault_boundary.wal";
    delete_test_files(io, stem);
    defer delete_test_files(io, stem);
    const events = [_]protocol.AppendEvent{
        .{ .event_type = "object.created", .schema_version = 1, .payload = "one" },
        .{ .event_type = "object.named", .schema_version = 1, .payload = "two" },
    };
    const request: protocol.AppendStream = .{
        .stream_id = "object/fault-test",
        .expected_version = .empty,
        .command_id = "fault-command",
        .events = &events,
    };
    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        var streams = try StreamStore.init(allocator, &lsm, .{
            .stream_capacity = 8,
            .stream_id_bytes_capacity = 1024,
        });
        defer streams.deinit();
        var injector = injector_start;
        streams.set_fault_injector(&injector);
        try std.testing.expectError(error.InjectedAppendFault, streams.append(request, 100));
        try std.testing.expect(injector.triggered);
    }
    var lsm = try LSMEngine.init(allocator, wal_path, io);
    defer lsm.deinit();
    var streams = try StreamStore.init(allocator, &lsm, .{
        .stream_capacity = 8,
        .stream_id_bytes_capacity = 1024,
    });
    defer streams.deinit();
    const recovered = try streams.verify_integrity();
    try std.testing.expectEqual(
        @as(u64, if (visible_after_restart) 2 else 0),
        recovered.events,
    );
    const response = try streams.append(request, 200);
    if (visible_after_restart) {
        _ = try expect_replayed(response);
    } else {
        _ = try expect_committed(response);
    }
    const final = try streams.verify_integrity();
    try std.testing.expectEqual(@as(u64, 1), final.batches);
    try std.testing.expectEqual(@as(u64, 2), final.events);
    try std.testing.expectEqual(@as(u64, 2), final.committed_log_position);
}

test "bounded stream and ordered log reads survive restart" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const stem = "test_stream_restart";
    delete_test_files(io, stem);
    defer delete_test_files(io, stem);
    const wal_path = "data/test_stream_restart.wal";
    const first_event = [_]protocol.AppendEvent{.{
        .event_type = "source.generation.requested",
        .schema_version = 1,
        .payload = "request",
    }};
    {
        var lsm = try LSMEngine.init(allocator, wal_path, io);
        defer lsm.deinit();
        var streams = try StreamStore.init(allocator, &lsm, .{
            .stream_capacity = limits.stream_capacity_test_default,
            .stream_id_bytes_capacity = limits.stream_id_bytes_capacity_test_default,
        });
        defer streams.deinit();
        _ = try expect_committed(try streams.append(.{
            .stream_id = "source-generation/restart",
            .expected_version = .empty,
            .command_id = "restart-1",
            .events = &first_event,
        }, 100));
    }
    var lsm = try LSMEngine.init(allocator, wal_path, io);
    defer lsm.deinit();
    var streams = try StreamStore.init(allocator, &lsm, .{
        .stream_capacity = limits.stream_capacity_test_default,
        .stream_id_bytes_capacity = limits.stream_id_bytes_capacity_test_default,
    });
    defer streams.deinit();
    const second_event = [_]protocol.AppendEvent{.{
        .event_type = "source.discovery.requested",
        .schema_version = 1,
        .payload = "plan",
    }};
    _ = try expect_committed(try streams.append(.{
        .stream_id = "source-generation/restart",
        .expected_version = .{ .exact = 1 },
        .command_id = "restart-2",
        .events = &second_event,
    }, 200));
    var output: [protocol.read_events_max]protocol.ReadEvent = undefined;
    var output_bytes: [4096]u8 = undefined;
    const stream_page = try streams.read_stream(.{
        .stream_id = "source-generation/restart",
        .from_version = 0,
        .max_count = 1,
    }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 1), stream_page.events.len);
    try std.testing.expectEqual(@as(u64, 2), stream_page.next_version);
    const log_page = try streams.read_log(.{
        .from_position = 1,
        .max_count = 0,
    }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 2), log_page.events.len);
    try std.testing.expectEqualStrings("request", log_page.events[0].payload);
    try std.testing.expectEqualStrings("plan", log_page.events[1].payload);
    try std.testing.expectEqual(@as(u64, 3), log_page.next_position);
}

test "staged appends stay invisible to reads until durable_sync" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const stem = "test_stream_staged_durability";
    delete_test_files(io, stem);
    defer delete_test_files(io, stem);
    var lsm = try LSMEngine.init(allocator, "data/test_stream_staged_durability.wal", io);
    defer lsm.deinit();
    var streams = try StreamStore.init(allocator, &lsm, .{
        .stream_capacity = limits.stream_capacity_test_default,
        .stream_id_bytes_capacity = limits.stream_id_bytes_capacity_test_default,
    });
    defer streams.deinit();

    const events = [_]protocol.AppendEvent{.{
        .event_type = "source.generation.requested",
        .schema_version = 1,
        .payload = "staged",
    }};
    const request: protocol.AppendStream = .{
        .stream_id = "source-generation/staged",
        .expected_version = .empty,
        .command_id = "staged-1",
        .events = &events,
    };
    _ = try expect_committed(try streams.stage_append(request, 100));

    var output: [protocol.read_events_max]protocol.ReadEvent = undefined;
    var output_bytes: [4096]u8 = undefined;

    const staged_log = try streams.read_log(.{ .from_position = 1, .max_count = 0 }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 0), staged_log.events.len);
    const staged_stream = try streams.read_stream(.{
        .stream_id = "source-generation/staged",
        .from_version = 0,
        .max_count = 0,
    }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 0), staged_stream.events.len);
    try std.testing.expectEqual(@as(u64, 0), streams.metrics().committed_log_position);

    try streams.durable_sync();

    const durable_log = try streams.read_log(.{ .from_position = 1, .max_count = 0 }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 1), durable_log.events.len);
    try std.testing.expectEqualStrings("staged", durable_log.events[0].payload);
    const durable_stream = try streams.read_stream(.{
        .stream_id = "source-generation/staged",
        .from_version = 0,
        .max_count = 0,
    }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 1), durable_stream.events.len);
    try std.testing.expectEqual(@as(u64, 1), streams.metrics().committed_log_position);
}

test "subscription pages filter the log and one stream within a bounded scan" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const stem = "test_stream_filtered_log";
    delete_test_files(io, stem);
    defer delete_test_files(io, stem);
    var lsm = try LSMEngine.init(allocator, "data/test_stream_filtered_log.wal", io);
    defer lsm.deinit();
    var streams = try StreamStore.init(allocator, &lsm, .{
        .stream_capacity = limits.stream_capacity_test_default,
        .stream_id_bytes_capacity = limits.stream_id_bytes_capacity_test_default,
    });
    defer streams.deinit();

    // More unwanted batches than one scan may load, then the ones a subscriber wants.
    const noise_appends: u32 = limits.subscription_scan_batches_max + 1;
    const noise = [_]protocol.AppendEvent{
        .{ .event_type = "noise", .schema_version = 1, .payload = "" },
    };
    for (0..noise_appends) |index| {
        var command_buffer: [32]u8 = undefined;
        const command_id = try std.fmt.bufPrint(&command_buffer, "noise-{d}", .{index});
        _ = try expect_committed(try streams.append(.{
            .stream_id = "noise",
            .expected_version = if (index == 0) .empty else .{ .exact = index },
            .command_id = command_id,
            .events = &noise,
        }, 100));
    }
    const wanted = [_]protocol.AppendEvent{
        .{ .event_type = "wanted", .schema_version = 1, .payload = "first" },
        .{ .event_type = "noise", .schema_version = 1, .payload = "" },
        .{ .event_type = "wanted", .schema_version = 1, .payload = "second" },
    };
    const result = try expect_committed(try streams.append(.{
        .stream_id = "wanted",
        .expected_version = .empty,
        .command_id = "wanted-1",
        .events = &wanted,
    }, 100));

    var output: [protocol.read_events_max]protocol.ReadEvent = undefined;
    var output_bytes: [64 * 1024]u8 = undefined;
    const types = [_][]const u8{"wanted"};

    const whole_log: StreamStore.SubscriptionQuery = .{
        .stream_id = "",
        .from = 0,
        .event_types = &types,
    };
    const skipped = try streams.read_subscription_page(whole_log, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 0), skipped.events.len);
    try std.testing.expectEqual(@as(u64, limits.subscription_scan_batches_max + 1), skipped.next);
    try std.testing.expectEqual(result.log_position_last, skipped.committed);

    var query = whole_log;
    query.from = skipped.next;
    const expected_payloads = [_][]const u8{ "first", "second" };
    var found_count: usize = 0;
    for (0..noise_appends) |_| {
        if (query.from > result.log_position_last) break;
        const page = try streams.read_subscription_page(query, &output, &output_bytes);
        try std.testing.expect(page.next > query.from);
        // NOTE: payloads borrow `output_bytes`, which the next read reuses.
        for (page.events) |read_event| {
            try std.testing.expectEqualStrings("wanted", read_event.event_type);
            try std.testing.expectEqualStrings(expected_payloads[found_count], read_event.payload);
            found_count += 1;
        }
        query.from = page.next;
    }
    try std.testing.expectEqual(@as(usize, 2), found_count);
    try std.testing.expectEqual(result.log_position_last + 1, query.from);

    const unfiltered = try streams.read_subscription_page(.{
        .stream_id = "",
        .from = result.log_position_first,
        .event_types = &.{},
    }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 3), unfiltered.events.len);

    const stream_page = try streams.read_subscription_page(.{
        .stream_id = "wanted",
        .from = 0,
        .event_types = &types,
    }, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 2), stream_page.events.len);
    try std.testing.expectEqual(@as(u64, 3), stream_page.events[1].stream_version);
    try std.testing.expectEqual(@as(u64, 4), stream_page.next);

    // Mid-batch starts use the index; later batches are read by their first position alone.
    const mid_batch = try streams.read_log(
        .{ .from_position = result.log_position_first + 1, .max_count = 0 },
        &output,
        &output_bytes,
    );
    try std.testing.expectEqual(@as(usize, 2), mid_batch.events.len);
    try std.testing.expectEqual(result.log_position_first + 1, mid_batch.events[0].log_position);
    const across_batches = try streams.read_log(
        .{ .from_position = result.log_position_first - 2, .max_count = 0 },
        &output,
        &output_bytes,
    );
    try std.testing.expectEqual(@as(usize, 5), across_batches.events.len);
    for (across_batches.events, 0..) |read_event, index| {
        try std.testing.expectEqual(result.log_position_first - 2 + index, read_event.log_position);
    }
    try std.testing.expectEqualStrings("second", across_batches.events[4].payload);

    const caught_up = try streams.read_subscription_page(query, &output, &output_bytes);
    try std.testing.expectEqual(@as(usize, 0), caught_up.events.len);
    try std.testing.expectEqual(query.from, caught_up.next);
}
