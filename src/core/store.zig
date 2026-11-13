const std = @import("std");
const assert = std.debug.assert;

const lsm_mod = @import("lsm.zig");
const event = @import("event.zig");
const fixed = @import("fixed.zig");
const protocol = @import("protocol.zig");
const stream_store_mod = @import("stream_store.zig");
const attestation_mod = @import("attestation.zig");
const breadcrumb_mod = @import("breadcrumb.zig");
const contract_mod = @import("contract.zig");
const signing = @import("signing.zig");
const limits = @import("limits.zig");
const time_mod = @import("time.zig");
const vc_mod = @import("vc.zig");

const DependencyContract = contract_mod.DependencyContract;

const Io = std.Io;
const LSMEngine = lsm_mod.LSMEngine;
const EventValue = event.EventValue;
const StreamStore = stream_store_mod.StreamStore;
const Breadcrumb = breadcrumb_mod.Breadcrumb;

pub const event_prefix = "evt:";

pub const MakeEventKeyOptions = struct {
    origin: []const u8,
    event_type: []const u8,
    sequence: u64,
};

// NOTE: fixed-width hex, so key order matches sequence order.
const event_key_format = "evt:{s}:{s}:{x:0>16}";

pub fn make_event_key_into(buffer: []u8, options: MakeEventKeyOptions) ![]const u8 {
    assert(options.origin.len > 0);
    const key = std.fmt.bufPrint(buffer, event_key_format, .{
        options.origin,
        options.event_type,
        options.sequence,
    }) catch return error.BufferTooSmall;
    assert(std.mem.startsWith(u8, key, event_prefix));
    return key;
}

pub const Config = struct {
    identity: ?[]const u8 = null,
    /// Zero disables vector-clock stamping.
    vc_node_key: u64 = 0,
    contracts: []const DependencyContract = &.{},
    /// Empty disables reclaim. The most specific rule wins; no match keeps the event forever.
    retention: []const RetentionRule = &.{},
    stream_capacity: limits.StreamCount = limits.stream_capacity_test_default,
    stream_id_bytes_capacity: limits.ByteCount = limits.stream_id_bytes_capacity_test_default,
    signing_key: ?signing.SigningKey = null,
};

pub const RetentionRule = struct {
    origin: []const u8 = "*",
    event_type: []const u8 = "*",
    ttl_seconds: u64,
};

pub const QueryResult = struct {
    events: []EventValue,
    complete: bool,
};

const VcRebuildContext = struct {
    clock: *vc_mod.Clock,

    fn visit(self: *VcRebuildContext, _: []const u8, value: EventValue) !event.VisitControl {
        if (value.vector_clock.len == 0) return .continue_scan;
        const remote = vc_mod.Clock.decode(value.vector_clock) catch return .continue_scan;
        self.clock.observe(&remote) catch {};
        return .continue_scan;
    }
};

const pull_response_wire_size_empty: limits.ByteCount = 4;

fn pull_entry_wire_size(key: []const u8, value: *const EventValue) !limits.ByteCount {
    return limits.byte_count(4 + key.len + 4 + @as(usize, value.serialized_size()));
}

// NOTE: a page ends early when the next event would overflow; resume from the last sequence.
const QueryContext = struct {
    output: []EventValue,
    arena: fixed.ByteArena,
    from_sequence: limits.Sequence,
    count: limits.EventCount,
    complete: bool,
    wire_size: limits.ByteCount = pull_response_wire_size_empty,

    fn visit(self: *QueryContext, key: []const u8, value: EventValue) !event.VisitControl {
        if (value.sequence < self.from_sequence) return .continue_scan;
        if (self.count == self.output.len) {
            self.complete = false;
            return .stop;
        }

        const bytes_required = try limits.byte_count(
            value.origin_service.len + value.event_type.len + value.payload.len +
                value.signature.len + value.vector_clock.len,
        );
        assert(self.wire_size <= protocol.frame_payload_size_max);
        const entry_wire_size = try pull_entry_wire_size(key, &value);
        const fits_frame = entry_wire_size <= protocol.frame_payload_size_max - self.wire_size;
        if (!fits_frame or bytes_required > self.arena.remaining()) {
            if (self.count == 0) return error.QueryBufferTooSmall;
            self.complete = false;
            return .stop;
        }
        self.wire_size += entry_wire_size;
        const origin = self.arena.write(value.origin_service) catch unreachable;
        const event_type = self.arena.write(value.event_type) catch unreachable;
        const payload = self.arena.write(value.payload) catch unreachable;
        const signature = self.arena.write(value.signature) catch unreachable;
        const vector_clock = self.arena.write(value.vector_clock) catch unreachable;
        self.output[self.count] = .{
            .sequence = value.sequence,
            .event_type = event_type,
            .payload = payload,
            .timestamp = value.timestamp,
            .origin_service = origin,
            .signature = signature,
            .vector_clock = vector_clock,
        };
        self.count += 1;
        return .continue_scan;
    }
};

pub const EventStore = struct {
    allocator: std.mem.Allocator,
    lsm: *LSMEngine,
    streams: StreamStore,
    identity: ?[]const u8,
    vc_node_key: u64,
    contracts: []const DependencyContract,
    retention: []const RetentionRule,
    signing_key: ?signing.SigningKey,
    sign_scratch: []u8,
    /// Rebuilt at init from `evt:` entries, so it has no durable record of its own.
    vc_clock: vc_mod.Clock,

    pub const Metrics = struct {
        streams_used: limits.StreamCount,
        streams_max: limits.StreamCount,
        stream_id_bytes_used: limits.ByteCount,
        stream_id_bytes_max: limits.ByteCount,
        memtable_bytes: limits.ByteCount,
        memtable_entries: limits.MemtableEntryCount,
        sstables: limits.SSTableCount,
        wal_bytes: u64,
        committed_log_position: protocol.LogPosition,
        poisoned: bool,
        compaction_active: bool,
        compaction_failures: u64,
        startup_quarantined_files: u16,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        wal_path: []const u8,
        io: Io,
        config: Config,
    ) !EventStore {
        const lsm_ptr = try allocator.create(LSMEngine);
        errdefer allocator.destroy(lsm_ptr);

        lsm_ptr.* = try LSMEngine.init(allocator, wal_path, io);
        errdefer lsm_ptr.deinit();

        _ = try lsm_ptr.compact_until_stable();

        var streams = try StreamStore.init(allocator, lsm_ptr, .{
            .stream_capacity = config.stream_capacity,
            .stream_id_bytes_capacity = config.stream_id_bytes_capacity,
        });
        errdefer streams.deinit();

        const sign_scratch: []u8 = if (config.signing_key != null)
            try allocator.alloc(u8, limits.event_serialized_size_max)
        else
            &.{};
        errdefer allocator.free(sign_scratch);

        var vc_clock: vc_mod.Clock = .{};
        if (config.vc_node_key != 0) {
            // NOTE: reserve this node's slot first so later bumps cannot fail on capacity.
            try vc_clock.bump(config.vc_node_key, 0);
            var rebuild: VcRebuildContext = .{ .clock = &vc_clock };
            _ = try lsm_ptr.visit_prefix(event_prefix, &rebuild, VcRebuildContext.visit);
        }

        return EventStore{
            .allocator = allocator,
            .lsm = lsm_ptr,
            .streams = streams,
            .identity = config.identity,
            .vc_node_key = config.vc_node_key,
            .contracts = config.contracts,
            .retention = config.retention,
            .signing_key = config.signing_key,
            .sign_scratch = sign_scratch,
            .vc_clock = vc_clock,
        };
    }

    pub fn deinit(self: *EventStore) void {
        self.streams.deinit();
        self.lsm.deinit();
        self.allocator.destroy(self.lsm);
        self.allocator.free(self.sign_scratch);
    }

    pub fn put(self: *EventStore, key: []const u8, value: EventValue) !void {
        try self.put_buffered(key, value);
        try self.sync();
    }

    pub fn put_buffered(self: *EventStore, key: []const u8, value: EventValue) !void {
        try self.lsm.put(key, value);
    }

    pub fn sync(self: *EventStore) !void {
        try self.lsm.sync();
    }

    pub fn append_stream(
        self: *EventStore,
        request: protocol.AppendStream,
        timestamp: limits.Timestamp,
    ) !protocol.AppendStreamResponse {
        // NOTE: no fsync here; durable_sync commits the batch and `evt:` events together.
        const response = try self.streams.stage_append(request, timestamp);
        switch (response) {
            .committed => |result| {
                try self.index_owned_events(request, result, timestamp);
                try self.streams.durable_sync();
            },
            else => {},
        }
        return response;
    }

    pub fn stage_append_stream(
        self: *EventStore,
        request: protocol.AppendStream,
        timestamp: limits.Timestamp,
    ) !protocol.AppendStreamResponse {
        const response = try self.streams.stage_append(request, timestamp);
        switch (response) {
            .committed => |result| try self.index_owned_events(request, result, timestamp),
            else => {},
        }
        return response;
    }

    fn index_owned_events(
        self: *EventStore,
        request: protocol.AppendStream,
        result: protocol.AppendResult,
        timestamp: limits.Timestamp,
    ) !void {
        const origin = self.identity orelse return;
        var clock_buffer: [vc_mod.encoded_length_max]u8 = undefined;
        var stamp: []const u8 = "";
        if (self.vc_node_key != 0) {
            try self.vc_clock.bump(self.vc_node_key, result.log_position_last);
            stamp = try self.vc_clock.encodeInto(&clock_buffer);
        }
        for (request.events, 0..) |append_event, index| {
            const log_position = result.log_position_first + index;
            var value = try EventValue.init(.{
                .sequence = log_position,
                .origin_service = origin,
                .event_type = append_event.event_type,
                .payload = append_event.payload,
                .timestamp = timestamp,
                .vector_clock = stamp,
            });
            // WARN: `value.signature` points here; it must outlive put_buffered.
            var signature_hex: [signing.signature_hex_length]u8 = undefined;
            if (self.signing_key) |*key| {
                try value.sign(key, self.sign_scratch, &signature_hex);
            }

            var key_buffer: [limits.key_length_max]u8 = undefined;
            const key = try make_event_key_into(&key_buffer, .{
                .origin = origin,
                .event_type = append_event.event_type,
                .sequence = log_position,
            });
            try self.put_buffered(key, value);
        }
    }

    pub fn durable_sync(self: *EventStore) !void {
        return self.streams.durable_sync();
    }

    pub fn read_stream_into(
        self: *EventStore,
        request: protocol.ReadStream,
        output: []protocol.ReadEvent,
        output_bytes: []u8,
    ) !protocol.ReadStreamResponse {
        return self.streams.read_stream(request, output, output_bytes);
    }

    pub fn read_log_into(
        self: *EventStore,
        request: protocol.ReadLog,
        output: []protocol.ReadEvent,
        output_bytes: []u8,
    ) !protocol.ReadLogResponse {
        return self.streams.read_log(request, output, output_bytes);
    }

    pub fn read_subscription_page(
        self: *EventStore,
        query: StreamStore.SubscriptionQuery,
        output: []protocol.ReadEvent,
        output_bytes: []u8,
    ) !StreamStore.SubscriptionPage {
        return self.streams.read_subscription_page(query, output, output_bytes);
    }

    pub fn log_position_durable(self: *const EventStore) protocol.LogPosition {
        return self.streams.log_position_durable;
    }

    pub fn verify_integrity(self: *EventStore) !StreamStore.Verification {
        return self.streams.verify_integrity();
    }

    pub const TrustedState = struct {
        streams: limits.StreamCount,
        committed_log_position: protocol.LogPosition,
    };

    fn attestation_path_into(self: *const EventStore, buffer: []u8) ![]const u8 {
        return attestation_mod.derive_path_into(buffer, self.lsm.wal_path);
    }

    pub fn trusted_state(self: *EventStore) !?TrustedState {
        var buffer: [limits.wal_path_length_max]u8 = undefined;
        const path = self.attestation_path_into(&buffer) catch return null;
        const found = (try attestation_mod.load_path(path, self.lsm.io)) orelse return null;
        if (!found.covers(self.lsm.manifest.version, self.streams.log_position_committed))
            return null;
        if (found.streams != self.streams.stream_count) return null;
        return .{
            .streams = self.streams.stream_count,
            .committed_log_position = self.streams.log_position_committed,
        };
    }

    pub fn revoke_attestation(self: *EventStore) void {
        var buffer: [limits.wal_path_length_max]u8 = undefined;
        const path = self.attestation_path_into(&buffer) catch return;
        attestation_mod.remove_path(path, self.lsm.io);
    }

    pub fn attest_verified(self: *EventStore) !void {
        if (self.streams.poisoned) return error.StorePoisoned;
        var buffer: [limits.wal_path_length_max]u8 = undefined;
        const path = try self.attestation_path_into(&buffer);
        try attestation_mod.save_path(path, .{
            .manifest_version = self.lsm.manifest.version,
            .committed_log_position = self.streams.log_position_durable,
            .streams = self.streams.stream_count,
        }, self.lsm.io);
    }

    pub fn metrics(self: *const EventStore) Metrics {
        const streams = self.streams.metrics();
        return .{
            .streams_used = streams.streams_used,
            .streams_max = streams.streams_max,
            .stream_id_bytes_used = streams.stream_id_bytes_used,
            .stream_id_bytes_max = streams.stream_id_bytes_max,
            .memtable_bytes = self.lsm.memtable_bytes(),
            .memtable_entries = self.lsm.memtable_count(),
            .sstables = self.lsm.sstable_count(),
            .wal_bytes = self.lsm.wal_bytes(),
            .committed_log_position = streams.committed_log_position,
            .poisoned = streams.poisoned,
            .compaction_active = self.lsm.compaction_is_active(),
            .compaction_failures = self.lsm.compaction_failure_count(),
            .startup_quarantined_files = self.lsm.startup_quarantine_count(),
        };
    }

    pub fn get_bounded(self: *EventStore, key: []const u8, byte_buffer: []u8) !?EventValue {
        return try self.lsm.get_bounded(key, byte_buffer);
    }

    pub fn put_breadcrumb(
        self: *EventStore,
        bc: Breadcrumb,
        local_sequence: u64,
        self_service: []const u8,
        timestamp: i64,
    ) !void {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        const key = try Breadcrumb.make_key_into(
            &key_buffer,
            bc.source_service,
            bc.event_type,
            bc.peer_address,
        );
        var payload_buffer: [32]u8 = undefined;
        const payload = try bc.serialize_payload_into(&payload_buffer);
        const ev = EventValue{
            .sequence = local_sequence,
            .event_type = breadcrumb_mod.breadcrumb_event_type,
            .payload = payload,
            .timestamp = timestamp,
            .origin_service = self_service,
        };
        try self.put(key, ev);
    }

    pub fn get_breadcrumb_sequence(
        self: *EventStore,
        source_service: []const u8,
        event_type: []const u8,
        peer_address: []const u8,
        byte_buffer: []u8,
    ) !?limits.Sequence {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        const key = try Breadcrumb.make_key_into(
            &key_buffer,
            source_service,
            event_type,
            peer_address,
        );
        const value = try self.get_bounded(key, byte_buffer) orelse return null;
        return try Breadcrumb.deserialize_sequence(value.payload);
    }

    pub fn ingest_event(self: *EventStore, value: EventValue) !void {
        if (self.identity) |identity| {
            if (!std.mem.eql(u8, value.origin_service, identity)) {
                const contract = self.find_contract(value.origin_service, value.event_type) orelse
                    return error.UnauthorizedOrigin;
                // NOTE: a valid signature makes a pulled event trustworthy, whoever relayed it.
                if (contract.origin_public_key) |public_key| {
                    if (!value.verify_provenance(public_key))
                        return error.InvalidProvenance;
                }
            }
        }

        var key_buffer: [limits.key_length_max]u8 = undefined;
        const key = try make_event_key_into(&key_buffer, .{
            .origin = value.origin_service,
            .event_type = value.event_type,
            .sequence = value.sequence,
        });
        if (!try self.lsm.contains(key)) {
            try self.put(key, value);
        }
        // NOTE: merge even for duplicates, which can carry a clock this store has not seen.
        if (value.vector_clock.len != 0) {
            const remote = vc_mod.Clock.decode(value.vector_clock) catch return;
            self.vc_clock.observe(&remote) catch {};
        }
    }

    /// View into store state; copy under `store_mutex`.
    pub fn vc_summary(self: *const EventStore) []const vc_mod.Entry {
        return self.vc_clock.entries[0..self.vc_clock.len];
    }

    pub fn retentionTtl(
        self: *const EventStore,
        origin: []const u8,
        event_type: []const u8,
    ) ?u64 {
        var best: ?u64 = null;
        var best_score: u8 = 0;
        for (self.retention) |rule| {
            const origin_match = std.mem.eql(u8, rule.origin, "*") or
                std.mem.eql(u8, rule.origin, origin);
            if (!origin_match) continue;
            const type_match = std.mem.eql(u8, rule.event_type, "*") or
                std.mem.eql(u8, rule.event_type, event_type);
            if (!type_match) continue;
            const score: u8 =
                @as(u8, if (std.mem.eql(u8, rule.origin, "*")) 0 else 2) +
                @as(u8, if (std.mem.eql(u8, rule.event_type, "*")) 0 else 1);
            if (best == null or score > best_score) {
                best = rule.ttl_seconds;
                best_score = score;
            }
        }
        return best;
    }

    pub const ExpiredKey = struct {
        bytes: [limits.key_length_max]u8,
        len: u16,
    };

    pub const ReclaimScan = struct {
        keys: []ExpiredKey,
        complete: bool,
    };

    pub fn collect_expired(
        self: *EventStore,
        now: limits.Timestamp,
        out: []ExpiredKey,
    ) !ReclaimScan {
        var context = ReclaimContext{
            .store = self,
            .now = now,
            .out = out,
        };
        _ = try self.lsm.visit_prefix(event_prefix, &context, ReclaimContext.visit);
        return .{
            .keys = context.out[0..context.count],
            .complete = context.complete,
        };
    }

    const ReclaimContext = struct {
        store: *const EventStore,
        now: limits.Timestamp,
        out: []ExpiredKey,
        count: usize = 0,
        complete: bool = true,

        fn visit(self: *ReclaimContext, key: []const u8, value: EventValue) !event.VisitControl {
            const ttl = self.store.retentionTtl(value.origin_service, value.event_type) orelse
                return .continue_scan;
            const age_seconds = self.now - value.timestamp;
            if (age_seconds < 0) return .continue_scan;
            if (@as(u64, @intCast(age_seconds)) < ttl) return .continue_scan;
            if (self.count == self.out.len) {
                self.complete = false;
                return .stop;
            }
            self.out[self.count] = .{ .bytes = undefined, .len = @intCast(key.len) };
            @memcpy(self.out[self.count].bytes[0..key.len], key);
            self.count += 1;
            return .continue_scan;
        }
    };

    pub const reclaim_batch_max = 64;

    pub fn reclaim_expired(self: *EventStore, now: limits.Timestamp) !usize {
        if (self.retention.len == 0) return 0;
        const keys = try self.allocator.alloc(ExpiredKey, reclaim_batch_max);
        defer self.allocator.free(keys);

        var reclaimed_total: usize = 0;
        var batches: u32 = 0;
        while (batches < reclaim_batches_max) : (batches += 1) {
            const scan = try self.collect_expired(now, keys);
            for (scan.keys) |expired| {
                try self.lsm.delete(expired.bytes[0..expired.len], now);
                reclaimed_total += 1;
            }
            if (scan.complete and scan.keys.len < keys.len) return reclaimed_total;
            if (scan.keys.len == 0) return reclaimed_total;
        }
        return reclaimed_total;
    }

    const reclaim_batches_max: u32 = 4_096;

    pub const BootstrapScanResult = struct {
        events: []EventValue,
        next_cursor_len: usize,
        complete: bool,
    };

    /// One bounded page in key order. Empty `cursor` starts over; slices borrow caller buffers.
    pub fn bootstrap_into(
        self: *EventStore,
        cursor: []const u8,
        output: []EventValue,
        byte_buffer: []u8,
        next_cursor_out: *[limits.key_length_max]u8,
    ) !BootstrapScanResult {
        const origin = self.identity orelse return error.NoIdentity;
        var prefix_buffer: [limits.key_length_max]u8 = undefined;
        const prefix = try std.fmt.bufPrint(
            &prefix_buffer,
            "evt:{s}:",
            .{origin},
        );
        if (cursor.len != 0 and !std.mem.startsWith(u8, cursor, prefix))
            return error.InvalidCursor;

        var context = BootstrapContext{
            .arena = try fixed.ByteArena.init(byte_buffer),
            .output = output,
            .cursor = cursor,
            .next_cursor = next_cursor_out,
        };
        _ = try self.lsm.visit_prefix(prefix, &context, BootstrapContext.visit);
        return .{
            .events = context.output[0..context.count],
            .next_cursor_len = context.next_cursor_len,
            .complete = context.complete,
        };
    }

    const BootstrapContext = struct {
        arena: fixed.ByteArena,
        output: []EventValue,
        cursor: []const u8,
        next_cursor: *[limits.key_length_max]u8,
        count: usize = 0,
        next_cursor_len: usize = 0,
        complete: bool = true,

        fn visit(self: *BootstrapContext, key: []const u8, value: EventValue) !event.VisitControl {
            if (self.cursor.len != 0 and std.mem.order(u8, key, self.cursor) != .gt)
                return .continue_scan;
            if (self.count == self.output.len) {
                self.complete = false;
                return .stop;
            }
            const bytes_required = try limits.byte_count(
                value.origin_service.len + value.event_type.len + value.payload.len +
                    value.signature.len + value.vector_clock.len,
            );
            if (bytes_required > self.arena.remaining()) {
                self.complete = false;
                return .stop;
            }
            const origin = self.arena.write(value.origin_service) catch unreachable;
            const event_type = self.arena.write(value.event_type) catch unreachable;
            const payload = self.arena.write(value.payload) catch unreachable;
            const signature = self.arena.write(value.signature) catch unreachable;
            const vector_clock = self.arena.write(value.vector_clock) catch unreachable;
            self.output[self.count] = .{
                .sequence = value.sequence,
                .event_type = event_type,
                .payload = payload,
                .timestamp = value.timestamp,
                .origin_service = origin,
                .signature = signature,
                .vector_clock = vector_clock,
            };
            @memcpy(self.next_cursor[0..key.len], key);
            self.next_cursor_len = key.len;
            self.count += 1;
            return .continue_scan;
        }
    };

    fn find_contract(
        self: *const EventStore,
        origin: []const u8,
        event_type: []const u8,
    ) ?*const DependencyContract {
        for (self.contracts) |*contract| {
            if (contract.matches_event(origin, event_type)) return contract;
        }
        return null;
    }

    pub fn query_events_into(
        self: *EventStore,
        origin: []const u8,
        event_type: []const u8,
        from_sequence: limits.Sequence,
        output: []EventValue,
        byte_buffer: []u8,
    ) !QueryResult {
        var prefix_buffer: [limits.key_length_max]u8 = undefined;
        const prefix = std.fmt.bufPrint(&prefix_buffer, "evt:{s}:{s}:", .{
            origin,
            event_type,
        }) catch return error.BufferTooSmall;
        var start_buffer: [limits.key_length_max]u8 = undefined;
        const start_key = std.fmt.bufPrint(&start_buffer, event_key_format, .{
            origin,
            event_type,
            from_sequence,
        }) catch return error.BufferTooSmall;
        assert(std.mem.startsWith(u8, start_key, prefix));
        var context = QueryContext{
            .output = output,
            .arena = try fixed.ByteArena.init(byte_buffer),
            .from_sequence = from_sequence,
            .count = 0,
            .complete = true,
        };
        _ = try self.lsm.visit_prefix_from(prefix, start_key, &context, QueryContext.visit);
        const events = output[0..context.count];
        std.mem.sort(EventValue, events, {}, struct {
            fn less_than(_: void, left: EventValue, right: EventValue) bool {
                return left.sequence < right.sequence;
            }
        }.less_than);
        return .{ .events = events, .complete = context.complete };
    }
};

test {
    _ = @import("fixed.zig");
    _ = @import("limits.zig");
    _ = @import("radix.zig");
    _ = @import("wal.zig");
    _ = @import("wal_cursor.zig");
    _ = @import("breadcrumb.zig");
    _ = @import("event.zig");
    _ = @import("contract.zig");
    _ = @import("recovery.zig");
    _ = @import("identity.zig");
    _ = @import("protocol.zig");
    _ = @import("stream_store.zig");
    _ = @import("transport.zig");
    _ = @import("config.zig");
    _ = @import("server.zig");
    _ = @import("memtable.zig");
    _ = @import("sstable.zig");
    _ = @import("manifest.zig");
    _ = @import("merge_iterator.zig");
    _ = @import("lsm.zig");
    _ = @import("owner.zig");
}

test "storage surface exposes only bounded operational APIs" {
    const Memtable = @import("memtable.zig").Memtable;
    const SSTableReader = @import("sstable.zig").SSTableReader;
    const BreadcrumbType = @import("breadcrumb.zig").Breadcrumb;
    const RadixTree = @import("radix.zig").RadixTree;
    const merge = @import("merge_iterator.zig");
    const recovery = @import("recovery.zig");
    const sstable = @import("sstable.zig");

    try std.testing.expect(!@hasDecl(EventValue, "serialize"));
    try std.testing.expect(!@hasDecl(EventValue, "deserialize"));
    try std.testing.expect(!@hasDecl(EventValue, "deinit"));
    try std.testing.expect(!@hasDecl(RadixTree, "insert_borrowed"));
    try std.testing.expect(!@hasDecl(Memtable, "insert_borrowed"));
    try std.testing.expect(!@hasDecl(Memtable, "get"));
    try std.testing.expect(!@hasDecl(Memtable, "scan_prefix"));
    try std.testing.expect(!@hasDecl(LSMEngine, "put_borrowed"));
    try std.testing.expect(!@hasDecl(LSMEngine, "get"));
    try std.testing.expect(!@hasDecl(LSMEngine, "scan_prefix"));
    try std.testing.expect(!@hasDecl(EventStore, "put_borrowed"));
    try std.testing.expect(!@hasDecl(EventStore, "get"));
    try std.testing.expect(!@hasDecl(EventStore, "scan_range"));
    try std.testing.expect(!@hasDecl(EventStore, "query_events"));
    try std.testing.expect(!@hasDecl(EventStore, "get_breadcrumb"));
    try std.testing.expect(!@hasDecl(EventStore, "get_breadcrumbs"));
    try std.testing.expect(!@hasDecl(EventStore, "put_event"));
    try std.testing.expect(!@hasDecl(EventStore, "put_event_buffered"));
    try std.testing.expect(!@hasDecl(EventStore, "put_event_local"));
    try std.testing.expect(!@hasDecl(EventStore, "put_event_local_buffered"));
    try std.testing.expect(!@hasDecl(BreadcrumbType, "serialize_payload"));
    try std.testing.expect(!@hasDecl(BreadcrumbType, "deserialize_payload"));
    try std.testing.expect(!@hasDecl(SSTableReader, "open"));
    try std.testing.expect(!@hasDecl(SSTableReader, "get"));
    try std.testing.expect(!@hasDecl(SSTableReader, "scan_prefix"));
    try std.testing.expect(!@hasDecl(sstable, "write_sstable"));
    try std.testing.expect(!@hasDecl(merge, "MergeIterator"));
    try std.testing.expect(!@hasDecl(merge, "collect_merged"));
    try std.testing.expect(!@hasDecl(recovery, "compute_recovery_plan"));
}

const KeyValuePair = event.KeyValuePair;

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

const TestGetBuffer = struct {
    bytes: [limits.event_serialized_size_max]u8,

    fn get(self: *TestGetBuffer, store: *EventStore, key: []const u8) !?EventValue {
        return try store.get_bounded(key, &self.bytes);
    }

    fn breadcrumb(
        self: *TestGetBuffer,
        store: *EventStore,
        source_service: []const u8,
        event_type: []const u8,
        peer_address: []const u8,
    ) !?Breadcrumb {
        var key_buffer: [limits.key_length_max]u8 = undefined;
        const key = try Breadcrumb.make_key_into(
            &key_buffer,
            source_service,
            event_type,
            peer_address,
        );
        const value = try self.get(store, key) orelse return null;
        return try Breadcrumb.from_wal_entry_borrowed(key, value);
    }
};

const TestQueryBuffer = struct {
    events: [limits.pull_events_max]EventValue,
    bytes: [1024 * 1024]u8,

    fn query(
        self: *TestQueryBuffer,
        store: *EventStore,
        origin: []const u8,
        event_type: []const u8,
        from_sequence: limits.Sequence,
    ) !QueryResult {
        return try store.query_events_into(
            origin,
            event_type,
            from_sequence,
            &self.events,
            &self.bytes,
        );
    }
};

const TestScanBuffer = struct {
    entries: [512]KeyValuePair,
    bytes: [1024 * 1024]u8,

    const Context = struct {
        entries: []KeyValuePair,
        arena: fixed.ByteArena,
        count: limits.CollectionCount,

        fn visit(
            self: *Context,
            key: []const u8,
            value: EventValue,
        ) !event.VisitControl {
            if (self.count == self.entries.len) return error.TestScanCapacityExceeded;
            const key_copy = try self.arena.write(key);
            const origin = try self.arena.write(value.origin_service);
            const event_type = try self.arena.write(value.event_type);
            const payload = try self.arena.write(value.payload);
            self.entries[self.count] = .{
                .key = key_copy,
                .value = .{
                    .sequence = value.sequence,
                    .event_type = event_type,
                    .payload = payload,
                    .timestamp = value.timestamp,
                    .origin_service = origin,
                },
            };
            self.count += 1;
            return .continue_scan;
        }
    };

    fn scan(self: *TestScanBuffer, store: *EventStore, prefix: []const u8) ![]KeyValuePair {
        var context = Context{
            .entries = &self.entries,
            .arena = try fixed.ByteArena.init(&self.bytes),
            .count = 0,
        };
        _ = try store.lsm.visit_prefix(prefix, &context, Context.visit);
        return self.entries[0..context.count];
    }
};

test "basic operations" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_basic_operations.wal", io, .{});
    defer event_store.deinit();

    const sample_events = [_]struct {
        key: []const u8,
        event_type: []const u8,
        payload: []const u8,
    }{
        .{
            .key = "user-service:u123:profile",
            .event_type = "user.registered",
            .payload = "{\"email\":\"john@example.com\"}",
        },
        .{
            .key = "user-service:u124:profile",
            .event_type = "user.registered",
            .payload = "{\"email\":\"jane@example.com\"}",
        },
        .{
            .key = "order-service:o456:order",
            .event_type = "order.created",
            .payload = "{\"amount\":99.99,\"user\":\"u123\"}",
        },
        .{
            .key = "user-service:u123:settings",
            .event_type = "user.updated",
            .payload = "{\"theme\":\"dark\"}",
        },
    };

    for (sample_events, 0..) |sample, i| {
        const value = try test_event_value(
            i + 1,
            "test-service",
            sample.event_type,
            sample.payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try event_store.put(sample.key, value);
    }

    var get_buffer: TestGetBuffer = undefined;
    if (try get_buffer.get(&event_store, "user-service:u123:profile")) |found_value| {
        try std.testing.expectEqualStrings("user.registered", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1);
    } else {
        return error.ExpectedValueNotFound;
    }

    try std.testing.expect(try get_buffer.get(&event_store, "nonexistent") == null);
}

test "bulk inserts" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_bulk_inserts.wal", io, .{});
    defer event_store.deinit();

    for (0..150) |i| {
        const key = try std.fmt.allocPrint(allocator, "bulk-service:b{}:event", .{i});
        defer allocator.free(key);

        const payload = try std.fmt.allocPrint(allocator, "{{\"id\":{}}}", .{i});
        defer allocator.free(payload);

        const value = try test_event_value(
            i + 1000,
            "test-service",
            "bulk.insert",
            payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try event_store.put(key, value);
    }

    var get_buffer: TestGetBuffer = undefined;
    if (try get_buffer.get(&event_store, "bulk-service:b0:event")) |found_value| {
        try std.testing.expectEqualStrings("bulk.insert", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1000);
    } else {
        return error.ExpectedBulkValueNotFound;
    }

    if (try get_buffer.get(&event_store, "bulk-service:b149:event")) |found_value| {
        try std.testing.expectEqualStrings("bulk.insert", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1149);
    } else {
        return error.ExpectedLastBulkValueNotFound;
    }
}

test "range scans" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_range_scans.wal", io, .{});
    defer event_store.deinit();

    const sample_events = [_]struct {
        key: []const u8,
        event_type: []const u8,
        payload: []const u8,
    }{
        .{
            .key = "user-service:u123:profile",
            .event_type = "user.registered",
            .payload = "{\"email\":\"john@example.com\"}",
        },
        .{
            .key = "user-service:u124:profile",
            .event_type = "user.registered",
            .payload = "{\"email\":\"jane@example.com\"}",
        },
        .{
            .key = "order-service:o456:order",
            .event_type = "order.created",
            .payload = "{\"amount\":99.99,\"user\":\"u123\"}",
        },
        .{
            .key = "user-service:u123:settings",
            .event_type = "user.updated",
            .payload = "{\"theme\":\"dark\"}",
        },
    };

    for (sample_events, 0..) |sample, i| {
        const value = try test_event_value(
            i + 2000,
            "test-service",
            sample.event_type,
            sample.payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try event_store.put(sample.key, value);
    }

    var scan_buffer: TestScanBuffer = undefined;
    const user_events = try scan_buffer.scan(&event_store, "user-service:");

    try std.testing.expect(user_events.len == 3);

    var found_profile = false;
    var found_settings = false;
    for (user_events) |item| {
        if (std.mem.eql(u8, item.key, "user-service:u123:profile")) {
            found_profile = true;
            try std.testing.expectEqualStrings("user.registered", item.value.event_type);
        }
        if (std.mem.eql(u8, item.key, "user-service:u123:settings")) {
            found_settings = true;
            try std.testing.expectEqualStrings("user.updated", item.value.event_type);
        }
    }
    try std.testing.expect(found_profile);
    try std.testing.expect(found_settings);
}

test "concurrent simulation" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(
        allocator,
        "data/test_concurrent_simulation.wal",
        io,
        .{},
    );
    defer event_store.deinit();

    const operations = [_]struct {
        operation_type: []const u8,
        key: []const u8,
        event_type: []const u8,
        payload: []const u8,
    }{
        .{
            .operation_type = "write",
            .key = "session:s1:login",
            .event_type = "user.login",
            .payload = "{\"user\":\"alice\",\"ip\":\"192.168.1.1\"}",
        },
        .{
            .operation_type = "write",
            .key = "cart:c1:add",
            .event_type = "cart.item_added",
            .payload = "{\"item\":\"laptop\",\"qty\":1}",
        },
        .{ .operation_type = "read", .key = "session:s1:login", .event_type = "", .payload = "" },
        .{
            .operation_type = "write",
            .key = "session:s1:logout",
            .event_type = "user.logout",
            .payload = "{\"duration\":3600}",
        },
        .{
            .operation_type = "write",
            .key = "cart:c1:checkout",
            .event_type = "cart.checkout",
            .payload = "{\"total\":1299.99}",
        },
        .{ .operation_type = "read", .key = "cart:c1:add", .event_type = "", .payload = "" },
        .{
            .operation_type = "write",
            .key = "session:s2:login",
            .event_type = "user.login",
            .payload = "{\"user\":\"bob\",\"ip\":\"192.168.1.2\"}",
        },
    };

    var sequence: u64 = 3000;
    var writes_performed: u32 = 0;
    var reads_performed: u32 = 0;

    for (operations) |op| {
        if (std.mem.eql(u8, op.operation_type, "write")) {
            const value = try test_event_value(
                sequence,
                "test-service",
                op.event_type,
                op.payload,
                @truncate(time_mod.Instant.now(io).timestamp.nsec),
            );
            try event_store.put(op.key, value);
            writes_performed += 1;
            sequence += 1;
        } else if (std.mem.eql(u8, op.operation_type, "read")) {
            var get_buffer: TestGetBuffer = undefined;
            if (try get_buffer.get(&event_store, op.key) != null) reads_performed += 1;
        }
    }

    try std.testing.expect(writes_performed == 5);
    try std.testing.expect(reads_performed == 2);

    var scan_buffer: TestScanBuffer = undefined;
    const concurrent_scan = try scan_buffer.scan(&event_store, "session:");
    try std.testing.expect(concurrent_scan.len >= 2);
}

test "persistence recovery" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var store1 = try EventStore.init(allocator, "data/test_instance1.wal", io, .{});
    defer store1.deinit();

    var store2 = try EventStore.init(allocator, "data/test_instance2.wal", io, .{});
    defer store2.deinit();

    const instance1_events = [_]struct {
        key: []const u8,
        event_type: []const u8,
        payload: []const u8,
    }{
        .{
            .key = "user:alice",
            .event_type = "user.registered",
            .payload = "{\"email\":\"alice@example.com\",\"instance\":1}",
        },
        .{
            .key = "order:12345",
            .event_type = "order.created",
            .payload = "{\"amount\":99.99,\"user\":\"alice\"}",
        },
        .{
            .key = "session:alice-s1",
            .event_type = "user.login",
            .payload = "{\"user\":\"alice\",\"ip\":\"192.168.1.1\"}",
        },
    };

    for (instance1_events, 0..) |test_event, i| {
        const value = try test_event_value(
            i + 4000,
            "test-service",
            test_event.event_type,
            test_event.payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try store1.put(test_event.key, value);
    }

    const instance2_events = [_]struct {
        key: []const u8,
        event_type: []const u8,
        payload: []const u8,
    }{
        .{
            .key = "user:bob",
            .event_type = "user.registered",
            .payload = "{\"email\":\"bob@example.com\",\"instance\":2}",
        },
        .{
            .key = "order:67890",
            .event_type = "order.created",
            .payload = "{\"amount\":149.99,\"user\":\"bob\"}",
        },
        .{
            .key = "session:bob-s1",
            .event_type = "user.login",
            .payload = "{\"user\":\"bob\",\"ip\":\"192.168.1.2\"}",
        },
    };

    for (instance2_events, 0..) |test_event, i| {
        const value = try test_event_value(
            i + 4100,
            "test-service",
            test_event.event_type,
            test_event.payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try store2.put(test_event.key, value);
    }

    const instance1_keys = [_][]const u8{ "user:alice", "order:12345", "session:alice-s1" };
    var get_buffer_1: TestGetBuffer = undefined;
    for (instance1_keys) |key| {
        if (try get_buffer_1.get(&store1, key) == null) {
            return error.Instance1DataNotFound;
        }
    }

    const instance2_keys = [_][]const u8{ "user:bob", "order:67890", "session:bob-s1" };
    for (instance2_keys) |key| {
        try std.testing.expect(try get_buffer_1.get(&store1, key) == null);
    }

    var get_buffer_2: TestGetBuffer = undefined;
    for (instance2_keys) |key| {
        if (try get_buffer_2.get(&store2, key) == null) {
            return error.Instance2DataNotFound;
        }
    }

    for (instance1_keys) |key| {
        try std.testing.expect(try get_buffer_2.get(&store2, key) == null);
    }
}

test "edge cases" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_edge_cases.wal", io, .{});
    defer event_store.deinit();

    const empty_value = try test_event_value(
        6000,
        "test-service",
        "",
        "",
        @truncate(time_mod.Instant.now(io).timestamp.nsec),
    );
    try event_store.put("edge:empty", empty_value);

    var get_buffer: TestGetBuffer = undefined;
    if (try get_buffer.get(&event_store, "edge:empty")) |found_value| {
        try std.testing.expect(found_value.event_type.len == 0);
        try std.testing.expect(found_value.payload.len == 0);
    } else {
        return error.EmptyValueNotFound;
    }

    const long_key = try std.fmt.allocPrint(
        allocator,
        "edge:very:long:key:with:many:segments:and:even:more:" ++
            "segments:to:make:it:really:long:{}",
        .{time_mod.Instant.now(io).timestamp.nsec},
    );
    defer allocator.free(long_key);

    const long_payload = try std.fmt.allocPrint(
        allocator,
        "{{\"description\":\"This is a very long payload designed to test how the system " ++
            "handles large amounts of data in a single event. It contains multiple " ++
            "sentences and should stress test the serialization and storage " ++
            "mechanisms.\",\"data\":[{}]}}",
        .{time_mod.Instant.now(io).timestamp.nsec},
    );
    defer allocator.free(long_payload);

    const long_value = try test_event_value(
        6001,
        "test-service",
        "test.long_data",
        long_payload,
        @truncate(time_mod.Instant.now(io).timestamp.nsec),
    );
    try event_store.put(long_key, long_value);

    if (try get_buffer.get(&event_store, long_key)) |found_value| {
        try std.testing.expect(long_key.len > 90);
        try std.testing.expect(found_value.payload.len > 200);
        try std.testing.expectEqualStrings("test.long_data", found_value.event_type);
    } else {
        return error.LongValueNotFound;
    }

    const special_keys = [_][]const u8{
        "edge:special:key-with-dashes",
        "edge:special:key_with_underscores",
        "edge:special:key.with.dots",
        "edge:special:key@with@symbols",
    };

    for (special_keys, 0..) |key, i| {
        const value = try test_event_value(
            6100 + i,
            "test-service",
            "test.special",
            "special payload",
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try event_store.put(key, value);
    }

    var scan_buffer: TestScanBuffer = undefined;
    const special_scan = try scan_buffer.scan(&event_store, "edge:special:");
    try std.testing.expect(special_scan.len == 4);

    const overwrite_key = "edge:overwrite:test";

    const value1 = try test_event_value(
        6200,
        "test-service",
        "test.first",
        "first value",
        time_mod.Instant.now(io).timestamp.nsec,
    );
    try event_store.put(overwrite_key, value1);

    const value2 = try test_event_value(
        6201,
        "test-service",
        "test.second",
        "second value",
        time_mod.Instant.now(io).timestamp.nsec,
    );
    try event_store.put(overwrite_key, value2);

    if (try get_buffer.get(&event_store, overwrite_key)) |found_value| {
        try std.testing.expectEqualStrings("test.second", found_value.event_type);
        try std.testing.expect(found_value.sequence == 6201);
    } else {
        return error.OverwriteValueNotFound;
    }
}

test "put_breadcrumb and get_breadcrumb round-trip" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_bc_roundtrip.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_bc_roundtrip.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_bc_roundtrip.wal", io, .{});
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_bc_roundtrip.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_bc_roundtrip.crumb") catch {};
    }

    const bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 42,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 1700000000,
    };

    try event_store.put_breadcrumb(bc, 1, "my-service", 1700000000);

    var get_buffer: TestGetBuffer = undefined;
    const maybe_bc = try get_buffer.breadcrumb(
        &event_store,
        "order-service",
        "order.created",
        "10.0.0.5:4200",
    );
    try std.testing.expect(maybe_bc != null);

    const restored = maybe_bc.?;

    try std.testing.expectEqualStrings("order-service", restored.source_service);
    try std.testing.expectEqualStrings("order.created", restored.event_type);
    try std.testing.expectEqual(@as(u64, 42), restored.last_sequence);
    try std.testing.expectEqualStrings("10.0.0.5:4200", restored.peer_address);
}

test "get_breadcrumbs returns all breadcrumbs" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_bc_list.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_bc_list.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_bc_list.wal", io, .{});
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_bc_list.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_bc_list.crumb") catch {};
    }

    const bc1 = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 42,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 100,
    };
    try event_store.put_breadcrumb(bc1, 1, "my-service", 100);

    const bc2 = Breadcrumb{
        .source_service = "user-service",
        .event_type = "user.registered",
        .last_sequence = 10,
        .peer_address = "10.0.0.6:4200",
        .updated_at = 200,
    };
    try event_store.put_breadcrumb(bc2, 2, "my-service", 200);

    var scan_buffer: TestScanBuffer = undefined;
    const breadcrumbs = try scan_buffer.scan(&event_store, breadcrumb_mod.key_prefix);
    try std.testing.expectEqual(@as(usize, 2), breadcrumbs.len);
}

test "breadcrumbs and domain events coexist" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_bc_coexist.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_bc_coexist.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_bc_coexist.wal", io, .{});
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_bc_coexist.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_bc_coexist.crumb") catch {};
    }

    const domain_ev = try test_event_value(
        1,
        "my-service",
        "user.registered",
        "{\"email\":\"test@test.com\"}",
        100,
    );
    try event_store.put("user-service:u1:profile", domain_ev);

    const bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 5,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 200,
    };
    try event_store.put_breadcrumb(bc, 2, "my-service", 200);

    var get_buffer: TestGetBuffer = undefined;
    if (try get_buffer.get(&event_store, "user-service:u1:profile")) |found| {
        try std.testing.expectEqualStrings("user.registered", found.event_type);
    } else {
        return error.DomainEventNotFound;
    }

    const maybe_bc = try get_buffer.breadcrumb(
        &event_store,
        "order-service",
        "order.created",
        "10.0.0.5:4200",
    );
    try std.testing.expect(maybe_bc != null);
    const restored = maybe_bc.?;
    try std.testing.expectEqual(@as(u64, 5), restored.last_sequence);

    var scan_buffer: TestScanBuffer = undefined;
    const user_scan = try scan_buffer.scan(&event_store, "user-service:");
    try std.testing.expectEqual(@as(usize, 1), user_scan.len);
}

test "breadcrumb survives WAL replay" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_bc_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_bc_replay.crumb") catch {};

    {
        var event_store = try EventStore.init(allocator, "data/test_bc_replay.wal", io, .{});
        defer event_store.deinit();

        const bc = Breadcrumb{
            .source_service = "order-service",
            .event_type = "order.created",
            .last_sequence = 99,
            .peer_address = "10.0.0.5:4200",
            .updated_at = 500,
        };
        try event_store.put_breadcrumb(bc, 1, "my-service", 500);
    }

    {
        var event_store = try EventStore.init(allocator, "data/test_bc_replay.wal", io, .{});
        defer event_store.deinit();

        var get_buffer: TestGetBuffer = undefined;
        const maybe_bc = try get_buffer.breadcrumb(
            &event_store,
            "order-service",
            "order.created",
            "10.0.0.5:4200",
        );
        try std.testing.expect(maybe_bc != null);

        const restored = maybe_bc.?;

        try std.testing.expectEqualStrings("order-service", restored.source_service);
        try std.testing.expectEqualStrings("order.created", restored.event_type);
        try std.testing.expectEqual(@as(u64, 99), restored.last_sequence);
        try std.testing.expectEqualStrings("10.0.0.5:4200", restored.peer_address);
    }

    Io.Dir.cwd().deleteFile(io, "data/test_bc_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_bc_replay.crumb") catch {};
}

test "ingest_event and query_events basic" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_event_query.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_query.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_query.wal", io, .{});
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_event_query.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_event_query.crumb") catch {};
    }

    for (1..6) |i| {
        const payload = try std.fmt.allocPrint(allocator, "{{\"id\":{}}}", .{i});
        defer allocator.free(payload);
        const ev = try test_event_value(
            i,
            "order-service",
            "order.created",
            payload,
            @intCast(i * 100),
        );
        try store.ingest_event(ev);
    }

    var query_buffer: TestQueryBuffer = undefined;
    const all = try query_buffer.query(&store, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 5), all.events.len);

    for (all.events, 0..) |item, i| {
        try std.testing.expectEqual(@as(u64, i + 1), item.sequence);
    }
}

test "query_events filters by from_sequence" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_event_filter.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_filter.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_filter.wal", io, .{});
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_event_filter.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_event_filter.crumb") catch {};
    }

    for (1..11) |i| {
        const ev = try test_event_value(
            i,
            "order-service",
            "order.created",
            "{}",
            @intCast(i * 100),
        );
        try store.ingest_event(ev);
    }

    var query_buffer: TestQueryBuffer = undefined;
    const partial = try query_buffer.query(&store, "order-service", "order.created", 7);
    try std.testing.expectEqual(@as(usize, 4), partial.events.len);
    try std.testing.expectEqual(@as(u64, 7), partial.events[0].sequence);
    try std.testing.expectEqual(@as(u64, 10), partial.events[3].sequence);
}

test "query_events isolates by origin and event_type" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_event_isolate.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_isolate.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_isolate.wal", io, .{});
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_event_isolate.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_event_isolate.crumb") catch {};
    }

    for (1..4) |i| {
        const ev = try test_event_value(
            i,
            "order-service",
            "order.created",
            "{}",
            @intCast(i),
        );
        try store.ingest_event(ev);
    }
    for (1..3) |i| {
        const ev = try test_event_value(
            i,
            "user-service",
            "user.registered",
            "{}",
            @intCast(i),
        );
        try store.ingest_event(ev);
    }
    for (1..3) |i| {
        const ev = try test_event_value(
            i,
            "order-service",
            "order.updated",
            "{}",
            @intCast(i),
        );
        try store.ingest_event(ev);
    }

    var query_buffer: TestQueryBuffer = undefined;
    const orders = try query_buffer.query(&store, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 3), orders.events.len);
    const users = try query_buffer.query(&store, "user-service", "user.registered", 0);
    try std.testing.expectEqual(@as(usize, 2), users.events.len);
    const updates = try query_buffer.query(&store, "order-service", "order.updated", 0);
    try std.testing.expectEqual(@as(usize, 2), updates.events.len);
    const empty = try query_buffer.query(&store, "ghost-service", "ghost.event", 0);
    try std.testing.expectEqual(@as(usize, 0), empty.events.len);
}

test "query_events does not leak into breadcrumbs or arbitrary keys" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_event_noleak.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_noleak.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_noleak.wal", io, .{});
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_event_noleak.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_event_noleak.crumb") catch {};
    }

    const ev = try test_event_value(1, "order-service", "order.created", "{}", 100);
    try store.ingest_event(ev);

    const arb = try test_event_value(2, "order-service", "order.created", "{}", 200);
    try store.put("order-service:o1:order", arb);

    const bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 5,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 300,
    };
    try store.put_breadcrumb(bc, 3, "my-service", 300);

    var query_buffer: TestQueryBuffer = undefined;
    const results = try query_buffer.query(&store, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 1), results.events.len);
    try std.testing.expectEqual(@as(u64, 1), results.events[0].sequence);
}

test "ingested event survives WAL replay" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.crumb") catch {};

    {
        var store = try EventStore.init(allocator, "data/test_event_replay.wal", io, .{});
        defer store.deinit();

        for (1..4) |i| {
            const ev = try test_event_value(
                i,
                "order-service",
                "order.created",
                "{}",
                @intCast(i),
            );
            try store.ingest_event(ev);
        }
    }

    {
        var store = try EventStore.init(allocator, "data/test_event_replay.wal", io, .{});
        defer store.deinit();

        var query_buffer: TestQueryBuffer = undefined;
        const results = try query_buffer.query(&store, "order-service", "order.created", 0);
        try std.testing.expectEqual(@as(usize, 3), results.events.len);
        try std.testing.expectEqual(@as(u64, 1), results.events[0].sequence);
        try std.testing.expectEqual(@as(u64, 3), results.events[2].sequence);
    }

    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.crumb") catch {};
}

test "ingest_event accepts events matching a declared contract" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_ingest_accept.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_accept.crumb") catch {};

    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{ "order.created", "order.updated" },
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_accept.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_accept.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_accept.crumb") catch {};
    }

    const ev = try test_event_value(1, "order-service", "order.created", "{}", 100);
    try store.ingest_event(ev);

    var query_buffer: TestQueryBuffer = undefined;
    const results = try query_buffer.query(&store, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 1), results.events.len);
}

fn signed_test_event(
    key: *const signing.SigningKey,
    sequence: limits.Sequence,
    origin: []const u8,
    event_type: []const u8,
    payload: []const u8,
    timestamp: limits.Timestamp,
    signature_hex_out: *[signing.signature_hex_length]u8,
) !EventValue {
    var ev = try test_event_value(sequence, origin, event_type, payload, timestamp);
    var message_buffer: [256]u8 = undefined;
    try ev.sign(key, &message_buffer, signature_hex_out);
    return ev;
}

test "ingest_event accepts a validly signed event when the contract declares a public key" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_signed.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_signed.crumb") catch {};

    const key = try signing.SigningKey.from_seed(@splat(11));
    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
        .origin_public_key = key.public_key(),
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_signed.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_signed.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_signed.crumb") catch {};
    }

    var signature_hex: [signing.signature_hex_length]u8 = undefined;
    const ev = try signed_test_event(&key, 1, "order-service", "order.created", "{}", 100, &signature_hex);
    try store.ingest_event(ev);

    var query_buffer: TestQueryBuffer = undefined;
    const results = try query_buffer.query(&store, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 1), results.events.len);
    try std.testing.expect(results.events[0].is_signed());
    try std.testing.expect(results.events[0].verify_provenance(key.public_key()));
}

test "ingest_event rejects an unsigned event when the contract requires provenance" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_unsigned.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_unsigned.crumb") catch {};

    const key = try signing.SigningKey.from_seed(@splat(12));
    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
        .origin_public_key = key.public_key(),
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_unsigned.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_unsigned.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_unsigned.crumb") catch {};
    }

    const ev = try test_event_value(1, "order-service", "order.created", "{}", 100);
    try std.testing.expectError(error.InvalidProvenance, store.ingest_event(ev));
}

test "ingest_event rejects a forged event signed by the wrong key" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_forged.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_forged.crumb") catch {};

    const owner_key = try signing.SigningKey.from_seed(@splat(13));
    const attacker_key = try signing.SigningKey.from_seed(@splat(99));
    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
        .origin_public_key = owner_key.public_key(),
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_forged.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_forged.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_forged.crumb") catch {};
    }

    var signature_hex: [signing.signature_hex_length]u8 = undefined;
    const forged = try signed_test_event(&attacker_key, 1, "order-service", "order.created", "{}", 100, &signature_hex);
    try std.testing.expectError(error.InvalidProvenance, store.ingest_event(forged));
}

test "ingest_event accepts unsigned events when the contract declares no public key" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_nokey.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_nokey.crumb") catch {};

    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_nokey.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_nokey.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_nokey.crumb") catch {};
    }

    const ev = try test_event_value(1, "order-service", "order.created", "{}", 100);
    try store.ingest_event(ev);
    var query_buffer: TestQueryBuffer = undefined;
    const results = try query_buffer.query(&store, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 1), results.events.len);
}

test "owned appended events are signed, indexed for pull, and ingest-verifiable by a consumer" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    inline for (.{
        "data/test_owned_origin",
        "data/test_owned_consumer",
    }) |base| {
        Io.Dir.cwd().deleteFile(io, base ++ ".wal") catch {};
        Io.Dir.cwd().deleteFile(io, base ++ ".crumb") catch {};
    }

    const key = try signing.SigningKey.from_seed(@splat(21));

    var origin = try EventStore.init(allocator, "data/test_owned_origin.wal", io, .{
        .identity = "order-service",
        .signing_key = key,
    });
    defer {
        origin.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_owned_origin.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_owned_origin.crumb") catch {};
    }
    const append_events = [_]protocol.AppendEvent{
        .{ .event_type = "order.created", .schema_version = 1, .payload = "first" },
        .{ .event_type = "order.created", .schema_version = 1, .payload = "second" },
    };
    const append_response = try origin.append_stream(.{
        .stream_id = "orders/1",
        .expected_version = .empty,
        .command_id = "owned-command",
        .events = &append_events,
    }, 100);
    try std.testing.expect(append_response == .committed);

    var origin_query: TestQueryBuffer = undefined;
    const served = try origin_query.query(&origin, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 2), served.events.len);
    for (served.events) |ev| {
        try std.testing.expect(ev.is_signed());
        try std.testing.expect(ev.verify_provenance(key.public_key()));
    }

    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
        .origin_public_key = key.public_key(),
    }};
    var consumer = try EventStore.init(allocator, "data/test_owned_consumer.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        consumer.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_owned_consumer.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_owned_consumer.crumb") catch {};
    }
    for (served.events) |ev| try consumer.ingest_event(ev);

    var consumer_query: TestQueryBuffer = undefined;
    const ingested = try consumer_query.query(&consumer, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 2), ingested.events.len);
}

test "ingest_event rejects events from undeclared origins" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_origin.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_origin.crumb") catch {};

    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_reject_origin.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_origin.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_origin.crumb") catch {};
    }

    const ev = try test_event_value(1, "unknown-service", "order.created", "{}", 100);

    try std.testing.expectError(error.UnauthorizedOrigin, store.ingest_event(ev));
}

test "ingest_event rejects events with undeclared event types" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_type.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_type.crumb") catch {};

    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_reject_type.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_type.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_reject_type.crumb") catch {};
    }

    const ev = try test_event_value(1, "order-service", "order.deleted", "{}", 100);

    try std.testing.expectError(error.UnauthorizedOrigin, store.ingest_event(ev));
}

test "ingest_event silently deduplicates same (origin, type, sequence)" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_ingest_dedup.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_dedup.crumb") catch {};

    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    }};

    var store = try EventStore.init(allocator, "data/test_ingest_dedup.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_dedup.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_dedup.crumb") catch {};
    }

    const ev1 = try test_event_value(
        1,
        "order-service",
        "order.created",
        "{\"first\":true}",
        100,
    );
    try store.ingest_event(ev1);

    const ev2 = try test_event_value(
        1,
        "order-service",
        "order.created",
        "{\"second\":true}",
        200,
    );
    try store.ingest_event(ev2);

    var query_buffer: TestQueryBuffer = undefined;
    const results = try query_buffer.query(&store, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 1), results.events.len);
    try std.testing.expectEqualStrings("{\"first\":true}", results.events[0].payload);
}

test "ingest_event allows local origin events (origin == identity)" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_ingest_local.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_local.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_ingest_local.wal", io, .{
        .identity = "my-service",
        .contracts = &.{},
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_local.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_local.crumb") catch {};
    }

    const ev = try test_event_value(1, "my-service", "user.registered", "{}", 100);
    try store.ingest_event(ev);

    var query_buffer: TestQueryBuffer = undefined;
    const results = try query_buffer.query(&store, "my-service", "user.registered", 0);
    try std.testing.expectEqual(@as(usize, 1), results.events.len);
}

test "ingest_event with no identity set accepts everything" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_ingest_noident.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_ingest_noident.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_ingest_noident.wal", io, .{});
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_noident.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_ingest_noident.crumb") catch {};
    }

    const ev = try test_event_value(1, "any-service", "any.event", "{}", 100);
    try store.ingest_event(ev);

    var query_buffer: TestQueryBuffer = undefined;
    const results = try query_buffer.query(&store, "any-service", "any.event", 0);
    try std.testing.expectEqual(@as(usize, 1), results.events.len);
}

test "event store append and bounded query allocate nothing after initialization" {
    const io = Io.Threaded.global_single_threaded.io();
    const wal_path = "data/test_store_no_post_init_alloc.wal";
    Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_store_no_post_init_alloc.crumb") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_store_no_post_init_alloc.manifest") catch {};
    defer Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_store_no_post_init_alloc.crumb") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_store_no_post_init_alloc.manifest") catch {};

    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = failing.allocator();
    var event_store = try EventStore.init(allocator, wal_path, io, .{
        .identity = "fixed-service",
    });
    defer event_store.deinit();

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;

    try event_store.ingest_event(.{
        .sequence = 1,
        .origin_service = "fixed-service",
        .event_type = "fixed.event",
        .payload = "{\"id\":1}",
        .timestamp = 1,
    });
    try event_store.ingest_event(.{
        .sequence = 2,
        .origin_service = "fixed-service",
        .event_type = "fixed.event",
        .payload = "{\"id\":2}",
        .timestamp = 2,
    });

    var events: [4]EventValue = undefined;
    var bytes: [4096]u8 = undefined;
    const query = try event_store.query_events_into(
        "fixed-service",
        "fixed.event",
        0,
        &events,
        &bytes,
    );
    try std.testing.expectEqual(@as(usize, 2), query.events.len);
    try std.testing.expectEqual(@as(u64, 1), query.events[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), query.events[1].sequence);

    const append_events = [_]protocol.AppendEvent{.{
        .event_type = "source.generation.requested",
        .schema_version = 1,
        .payload = "{}",
    }};
    const append_response = try event_store.append_stream(.{
        .stream_id = "source-generation/no-allocation",
        .expected_version = .empty,
        .command_id = "no-allocation-1",
        .events = &append_events,
    }, 1);
    try std.testing.expect(append_response == .committed);
    var read_events: [4]protocol.ReadEvent = undefined;
    var read_bytes: [4096]u8 = undefined;
    const read_response = try event_store.read_log_into(.{
        .from_position = 0,
        .max_count = 4,
    }, &read_events, &read_bytes);
    try std.testing.expectEqual(@as(usize, 1), read_response.events.len);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(failing.fail_index, failing.alloc_index);
    try std.testing.expectEqual(failing.resize_fail_index, failing.resize_index);
}

const vc_mod_test = @import("vc.zig");

test "append stamps vector clocks and the summary exposes the high-water mark" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_vc_append.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_vc_append.wal") catch {};

    var event_store = try EventStore.init(allocator, "data/test_vc_append.wal", io, .{
        .identity = "order-service",
        .vc_node_key = vc_mod_test.nodeKey("node-a"),
    });
    defer event_store.deinit();

    const append_events = [_]protocol.AppendEvent{
        .{ .event_type = "order.created", .schema_version = 1, .payload = "{}" },
    };
    _ = try event_store.append_stream(.{
        .stream_id = "orders/1",
        .expected_version = .empty,
        .command_id = "vc-1",
        .events = &append_events,
    }, 100);

    var get_buffer: TestGetBuffer = undefined;
    const indexed = (try get_buffer.get(
        &event_store,
        "evt:order-service:order.created:0000000000000001",
    )).?;
    try std.testing.expect(indexed.vector_clock.len > 0);
    const stamped = try vc_mod_test.Clock.decode(indexed.vector_clock);
    try std.testing.expectEqual(
        @as(u64, 1),
        stamped.get(vc_mod_test.nodeKey("node-a")).?,
    );

    const summary = event_store.vc_summary();
    try std.testing.expectEqual(@as(usize, 1), summary.len);
    try std.testing.expectEqual(vc_mod_test.nodeKey("node-a"), summary[0].node);
    try std.testing.expectEqual(@as(u64, 1), summary[0].counter);

    _ = try event_store.append_stream(.{
        .stream_id = "orders/2",
        .expected_version = .empty,
        .command_id = "vc-2",
        .events = &append_events,
    }, 200);
    try std.testing.expectEqual(@as(u64, 2), event_store.vc_summary()[0].counter);
}

test "ingest merges remote vector clocks into the high-water mark" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_vc_ingest.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_vc_ingest.wal") catch {};

    var event_store = try EventStore.init(allocator, "data/test_vc_ingest.wal", io, .{
        .identity = "my-service",
        .vc_node_key = vc_mod_test.nodeKey("self"),
        .contracts = &.{
            .{
                .source_service = "peer-service",
                .event_types = &[_][]const u8{ "peer.event", "peer.other" },
                .peer_address = "127.0.0.1:1",
                .retention_hint = null,
            },
        },
    });
    defer event_store.deinit();

    const remote_clock_text = "111=5,222=9";
    const remote = try EventValue.init(.{
        .sequence = 3,
        .origin_service = "peer-service",
        .event_type = "peer.event",
        .payload = "{}",
        .timestamp = 1,
        .vector_clock = remote_clock_text,
    });
    try event_store.ingest_event(remote);

    const summary = event_store.vc_summary();
    try std.testing.expectEqual(@as(usize, 3), summary.len);
    try std.testing.expectEqual(@as(u64, 5), event_store.vc_clock.get(111).?);
    try std.testing.expectEqual(@as(u64, 9), event_store.vc_clock.get(222).?);
    try std.testing.expectEqual(@as(u64, 0), event_store.vc_clock.get(vc_mod_test.nodeKey("self")).?);

    try event_store.ingest_event(remote);
    try std.testing.expectEqual(@as(u64, 9), event_store.vc_clock.get(222).?);
    var stamped_again = remote;
    stamped_again.vector_clock = "222=99";
    try event_store.ingest_event(stamped_again);
    try std.testing.expectEqual(@as(u64, 99), event_store.vc_clock.get(222).?);

    const corrupt = try EventValue.init(.{
        .sequence = 4,
        .origin_service = "peer-service",
        .event_type = "peer.other",
        .payload = "{}",
        .timestamp = 2,
    });
    var corrupted = corrupt;
    corrupted.vector_clock = "garbage";
    try std.testing.expectError(error.InvalidVectorClock, EventValue.init(.{
        .sequence = 5,
        .origin_service = "peer-service",
        .event_type = "peer.bad",
        .payload = "{}",
        .timestamp = 3,
        .vector_clock = "garbage",
    }));
    try event_store.ingest_event(corrupted);
}

test "vector-clock knowledge survives restart by folding stored events" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_vc_restart.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_vc_restart.wal") catch {};

    {
        var event_store = try EventStore.init(allocator, "data/test_vc_restart.wal", io, .{
            .identity = "svc",
            .vc_node_key = vc_mod_test.nodeKey("n1"),
            .contracts = &.{
                .{
                    .source_service = "other",
                    .event_types = &[_][]const u8{"e"},
                    .peer_address = "127.0.0.1:1",
                    .retention_hint = null,
                },
            },
        });
        defer event_store.deinit();
        const append_events = [_]protocol.AppendEvent{
            .{ .event_type = "e", .schema_version = 1, .payload = "x" },
            .{ .event_type = "e", .schema_version = 1, .payload = "y" },
        };
        _ = try event_store.append_stream(.{
            .stream_id = "s/1",
            .expected_version = .empty,
            .command_id = "cmd-1",
            .events = &append_events,
        }, 1);
        const remote = try EventValue.init(.{
            .sequence = 1,
            .origin_service = "other",
            .event_type = "e",
            .payload = "z",
            .timestamp = 1,
            .vector_clock = "777=12",
        });
        try event_store.ingest_event(remote);
    }

    {
        var event_store = try EventStore.init(allocator, "data/test_vc_restart.wal", io, .{
            .identity = "svc",
            .vc_node_key = vc_mod_test.nodeKey("n1"),
            .contracts = &.{
                .{
                    .source_service = "other",
                    .event_types = &[_][]const u8{"e"},
                    .peer_address = "127.0.0.1:1",
                    .retention_hint = null,
                },
            },
        });
        defer event_store.deinit();
        const summary = event_store.vc_summary();
        try std.testing.expectEqual(@as(usize, 2), summary.len);
        try std.testing.expectEqual(@as(u64, 2), event_store.vc_clock.get(vc_mod_test.nodeKey("n1")).?);
        try std.testing.expectEqual(@as(u64, 12), event_store.vc_clock.get(777).?);

        const append_events = [_]protocol.AppendEvent{
            .{ .event_type = "e", .schema_version = 1, .payload = "w" },
        };
        _ = try event_store.append_stream(.{
            .stream_id = "s/2",
            .expected_version = .empty,
            .command_id = "cmd-2",
            .events = &append_events,
        }, 2);
        try std.testing.expectEqual(@as(u64, 3), event_store.vc_clock.get(vc_mod_test.nodeKey("n1")).?);
    }
}

test "bootstrap pagination streams every own event exactly once" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_bootstrap_scan.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_bootstrap_scan.wal") catch {};

    var event_store = try EventStore.init(allocator, "data/test_bootstrap_scan.wal", io, .{
        .identity = "my-service",
        .vc_node_key = 12345,
        .contracts = &.{
            .{
                .source_service = "peer-service",
                .event_types = &[_][]const u8{"peer.event"},
                .peer_address = "127.0.0.1:1",
                .retention_hint = null,
            },
        },
    });
    defer event_store.deinit();

    const own_events = [_]protocol.AppendEvent{
        .{ .event_type = "alpha.event", .schema_version = 1, .payload = "a1" },
        .{ .event_type = "beta.event", .schema_version = 1, .payload = "b1" },
        .{ .event_type = "alpha.event", .schema_version = 1, .payload = "a2" },
    };
    _ = try event_store.append_stream(.{
        .stream_id = "s/1",
        .expected_version = .empty,
        .command_id = "boot-1",
        .events = own_events[0..2],
    }, 1);
    _ = try event_store.append_stream(.{
        .stream_id = "s/2",
        .expected_version = .empty,
        .command_id = "boot-2",
        .events = own_events[2..3],
    }, 2);
    const foreign = try EventValue.init(.{
        .sequence = 9,
        .origin_service = "peer-service",
        .event_type = "peer.event",
        .payload = "nope",
        .timestamp = 3,
        .vector_clock = "999=4",
    });
    try event_store.ingest_event(foreign);

    var seen: std.ArrayList([]const u8) = .empty;
    defer {
        for (seen.items) |item| allocator.free(item);
        seen.deinit(allocator);
    }
    var cursor: [limits.key_length_max]u8 = undefined;
    var cursor_len: usize = 0;
    var undefined_cursor: [limits.key_length_max]u8 = undefined;
    var pages: usize = 0;
    var complete = false;
    while (!complete) {
        pages += 1;
        try std.testing.expect(pages < 32);
        var output: [2]EventValue = undefined;
        var bytes: [8192]u8 = undefined;
        var next_cursor: [limits.key_length_max]u8 = undefined;
        const scan = try event_store.bootstrap_into(
            cursor[0..cursor_len],
            &output,
            &bytes,
            &next_cursor,
        );
        complete = scan.complete;
        if (scan.next_cursor_len > 0) {
            @memcpy(cursor[0..scan.next_cursor_len], next_cursor[0..scan.next_cursor_len]);
            cursor_len = scan.next_cursor_len;
        }
        for (scan.events) |value| {
            try seen.append(allocator, try allocator.dupe(u8, value.payload));
            try std.testing.expectEqualStrings("my-service", value.origin_service);
            try std.testing.expect(value.vector_clock.len > 0);
        }
    }

    try std.testing.expectEqual(@as(usize, 3), seen.items.len);
    try std.testing.expectEqualStrings("a1", seen.items[0]);
    try std.testing.expectEqualStrings("a2", seen.items[1]);
    try std.testing.expectEqualStrings("b1", seen.items[2]);
    try std.testing.expect(pages >= 2);

    try std.testing.expectError(
        error.InvalidCursor,
        event_store.bootstrap_into("evt:other-service:", &.{}, &.{}, &undefined_cursor),
    );

    {
        Io.Dir.cwd().deleteFile(io, "data/test_bootstrap_anon.wal") catch {};
        var anonymous = try EventStore.init(
            allocator,
            "data/test_bootstrap_anon.wal",
            io,
            .{},
        );
        defer anonymous.deinit();
        try std.testing.expectError(
            error.NoIdentity,
            anonymous.bootstrap_into("", &.{}, &.{}, &undefined_cursor),
        );
    }
}

test "reclaim_expired tombstones only rule-matched expired events" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_reclaim.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_reclaim.wal") catch {};

    const now: limits.Timestamp = 10_000;
    {
        var event_store = try EventStore.init(allocator, "data/test_reclaim.wal", io, .{
            .identity = "my-service",
            .vc_node_key = vc_mod_test.nodeKey("n1"),
            .contracts = &.{
                .{
                    .source_service = "peer-service",
                    .event_types = &[_][]const u8{"whatever.event"},
                    .peer_address = "127.0.0.1:1",
                    .retention_hint = null,
                },
            },
            .retention = &.{
                .{ .event_type = "ephemeral.event", .ttl_seconds = 100 },
                .{ .origin = "peer-service", .ttl_seconds = 50 },
                .{ .ttl_seconds = 10_000 },
            },
        });
        defer event_store.deinit();

        const own = [_]protocol.AppendEvent{
            .{ .event_type = "ephemeral.event", .schema_version = 1, .payload = "old" },
            .{ .event_type = "kept.event", .schema_version = 1, .payload = "young" },
            .{ .event_type = "decision_recorded", .schema_version = 1, .payload = "durable" },
        };
        _ = try event_store.append_stream(.{
            .stream_id = "s/1",
            .expected_version = .empty,
            .command_id = "reclaim-1",
            .events = own[0..2],
        }, now - 500);
        _ = try event_store.append_stream(.{
            .stream_id = "s/2",
            .expected_version = .empty,
            .command_id = "reclaim-2",
            .events = own[2..3],
        }, now - 500);

        try event_store.ingest_event(try EventValue.init(.{
            .sequence = 9,
            .origin_service = "peer-service",
            .event_type = "whatever.event",
            .payload = "stale",
            .timestamp = now - 200,
            .vector_clock = "42=7",
        }));

        try std.testing.expectEqual(
            @as(u64, 100),
            event_store.retentionTtl("my-service", "ephemeral.event").?,
        );
        try std.testing.expectEqual(
            @as(u64, 10_000),
            event_store.retentionTtl("my-service", "kept.event").?,
        );
        try std.testing.expectEqual(
            @as(u64, 50),
            event_store.retentionTtl("peer-service", "whatever.event").?,
        );

        const reclaimed = try event_store.reclaim_expired(now);
        try std.testing.expectEqual(@as(usize, 2), reclaimed);

        var get_buffer: TestGetBuffer = undefined;
        try std.testing.expectEqual(@as(?EventValue, null), try get_buffer.get(
            &event_store,
            "evt:my-service:ephemeral.event:0000000000000001",
        ));
        try std.testing.expectEqual(@as(?EventValue, null), try get_buffer.get(
            &event_store,
            "evt:peer-service:whatever.event:0000000000000009",
        ));
        const durable = (try get_buffer.get(
            &event_store,
            "evt:my-service:decision_recorded:0000000000000003",
        )).?;
        try std.testing.expectEqualStrings("durable", durable.payload);

        try std.testing.expectEqual(@as(usize, 0), try event_store.reclaim_expired(now));

        var query_buffer: TestQueryBuffer = undefined;
        const pull = try query_buffer.query(&event_store, "peer-service", "whatever.event", 0);
        try std.testing.expectEqual(@as(usize, 0), pull.events.len);
    }
    {
        var reopened = try EventStore.init(allocator, "data/test_reclaim.wal", io, .{
            .identity = "my-service",
            .vc_node_key = vc_mod_test.nodeKey("n1"),
        });
        defer reopened.deinit();
        var reopened_buffer: TestGetBuffer = undefined;
        try std.testing.expectEqual(@as(?EventValue, null), try reopened_buffer.get(
            &reopened,
            "evt:my-service:ephemeral.event:0000000000000001",
        ));
        try std.testing.expect((try reopened_buffer.get(
            &reopened,
            "evt:my-service:decision_recorded:0000000000000003",
        )).?.payload.len > 0);
    }
}

test "empty retention config disables reclamation entirely" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    Io.Dir.cwd().deleteFile(io, "data/test_reclaim_off.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_reclaim_off.wal") catch {};

    var event_store = try EventStore.init(allocator, "data/test_reclaim_off.wal", io, .{
        .identity = "svc",
    });
    defer event_store.deinit();
    const ev = try test_event_value(1, "svc", "t", "{}", 0);
    try event_store.ingest_event(ev);
    try std.testing.expectEqual(@as(usize, 0), try event_store.reclaim_expired(1_000_000));
    var get_buffer: TestGetBuffer = undefined;
    try std.testing.expect((try get_buffer.get(&event_store, "evt:svc:t:0000000000000001")) != null);
}

test "query pages stop at the pull frame budget and resume from the last sequence" {
    const allocator = std.testing.allocator;

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_event_frame_budget.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_frame_budget.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_frame_budget.wal", io, .{});
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_event_frame_budget.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_event_frame_budget.crumb") catch {};
    }

    const payload: [6000]u8 = @splat('x');
    const event_count: u64 = 300;
    for (1..event_count + 1) |i| {
        try store.ingest_event(try test_event_value(i, "big-service", "note", &payload, 1));
        if (i == 150) try store.lsm.flush();
    }

    const query_buffer = try allocator.create(TestQueryBuffer);
    defer allocator.destroy(query_buffer);

    var from: u64 = 1;
    var pages: u32 = 0;
    while (pages < 16) : (pages += 1) {
        const page = try query_buffer.query(&store, "big-service", "note", from);
        if (page.events.len == 0) break;

        var wire_size: limits.ByteCount = pull_response_wire_size_empty;
        for (page.events, 0..) |*value, index| {
            try std.testing.expectEqual(from + index, value.sequence);
            var key_buffer: [limits.key_length_max]u8 = undefined;
            const key = try make_event_key_into(&key_buffer, .{
                .origin = value.origin_service,
                .event_type = value.event_type,
                .sequence = value.sequence,
            });
            wire_size += try pull_entry_wire_size(key, value);
        }
        try std.testing.expect(wire_size <= protocol.frame_payload_size_max);
        const last = page.events[page.events.len - 1].sequence;
        try std.testing.expectEqual(last == event_count, page.complete);
        from = last + 1;
    }
    try std.testing.expectEqual(event_count + 1, from);
    try std.testing.expect(pages >= 2);
}
