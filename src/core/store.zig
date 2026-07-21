const std = @import("std");
const assert = std.debug.assert;

const lsm_mod = @import("lsm.zig");
const event = @import("event.zig");
const fixed = @import("fixed.zig");
const protocol = @import("protocol.zig");
const stream_store_mod = @import("stream_store.zig");
const breadcrumb_mod = @import("breadcrumb.zig");
const contract_mod = @import("contract.zig");
const signing = @import("signing.zig");
const limits = @import("limits.zig");
const time_mod = @import("time.zig");

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

pub fn make_event_key_into(buffer: []u8, options: MakeEventKeyOptions) ![]const u8 {
    assert(options.origin.len > 0);
    const key = std.fmt.bufPrint(buffer, "evt:{s}:{s}:{x:0>16}", .{
        options.origin,
        options.event_type,
        options.sequence,
    }) catch return error.BufferTooSmall;
    assert(std.mem.startsWith(u8, key, event_prefix));
    return key;
}

pub const Config = struct {
    identity: ?[]const u8 = null,
    contracts: []const DependencyContract = &.{},
    stream_capacity: limits.StreamCount = limits.stream_capacity_test_default,
    stream_id_bytes_capacity: limits.ByteCount = limits.stream_id_bytes_capacity_test_default,
    /// When set, the store signs each of its own appended events as it indexes
    /// them under `evt:` keys for pull serving.
    signing_key: ?signing.SigningKey = null,
};

pub const QueryResult = struct {
    events: []EventValue,
    complete: bool,
};

const QueryContext = struct {
    output: []EventValue,
    arena: fixed.ByteArena,
    from_sequence: limits.Sequence,
    count: limits.EventCount,
    complete: bool,

    fn visit(self: *QueryContext, _: []const u8, value: EventValue) !event.VisitControl {
        if (value.sequence < self.from_sequence) return .continue_scan;
        if (self.count == self.output.len) {
            self.complete = false;
            return .stop;
        }

        const bytes_required = try limits.byte_count(
            value.origin_service.len + value.event_type.len + value.payload.len +
                value.signature.len,
        );
        if (bytes_required > self.arena.remaining()) return error.QueryBufferTooSmall;
        const origin = self.arena.write(value.origin_service) catch unreachable;
        const event_type = self.arena.write(value.event_type) catch unreachable;
        const payload = self.arena.write(value.payload) catch unreachable;
        const signature = self.arena.write(value.signature) catch unreachable;
        self.output[self.count] = .{
            .sequence = value.sequence,
            .event_type = event_type,
            .payload = payload,
            .timestamp = value.timestamp,
            .origin_service = origin,
            .signature = signature,
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
    contracts: []const DependencyContract,
    signing_key: ?signing.SigningKey,
    /// Buffer for the one-shot signing message; allocated only when signing is
    /// enabled. Sized for a full serialized event (payload plus header).
    sign_scratch: []u8,

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

        return EventStore{
            .allocator = allocator,
            .lsm = lsm_ptr,
            .streams = streams,
            .identity = config.identity,
            .contracts = config.contracts,
            .signing_key = config.signing_key,
            .sign_scratch = sign_scratch,
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
        // Stage (no fsync) so the canonical batch and the pull-servable evt:
        // events are made durable together by one durable_sync below.
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
            // The caller (server) fsyncs the whole staged set via group commit.
            .committed => |result| try self.index_owned_events(request, result, timestamp),
            else => {},
        }
        return response;
    }

    /// Index each of this node's own appended events under an `evt:` key so
    /// dependents can pull them, signing with the configured key so consumers
    /// can verify authorship. Skipped without an identity (dev mode).
    fn index_owned_events(
        self: *EventStore,
        request: protocol.AppendStream,
        result: protocol.AppendResult,
        timestamp: limits.Timestamp,
    ) !void {
        const origin = self.identity orelse return;
        for (request.events, 0..) |append_event, index| {
            const log_position = result.log_position_first + index;
            var value = try EventValue.init(.{
                .sequence = log_position,
                .origin_service = origin,
                .event_type = append_event.event_type,
                .payload = append_event.payload,
                .timestamp = timestamp,
            });
            // The hex signature buffer must outlive put_buffered below, which
            // serializes `value` (whose `signature` slice points into it).
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

    pub fn verify_integrity(self: *EventStore) !StreamStore.Verification {
        return self.streams.verify_integrity();
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
        // A configured identity authorizes cross-service intake by contract. A
        // node's own events and the no-identity dev mode bypass the check.
        if (self.identity) |identity| {
            if (!std.mem.eql(u8, value.origin_service, identity)) {
                const contract = self.find_contract(value.origin_service, value.event_type) orelse
                    return error.UnauthorizedOrigin;
                // Provenance: when the contract declares the owner's public key,
                // the event must carry a valid origin signature — this is what
                // makes a pulled event trustworthy regardless of which peer
                // relayed it. Unsigned or forged events are rejected.
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
        if (try self.lsm.contains(key)) return;
        try self.put(key, value);
    }

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
        var context = QueryContext{
            .output = output,
            .arena = try fixed.ByteArena.init(byte_buffer),
            .from_sequence = from_sequence,
            .count = 0,
            .complete = true,
        };
        _ = try self.lsm.visit_prefix(prefix, &context, QueryContext.visit);
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
    // Caller-owned; backs the returned event's borrowed signature slice.
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
    // The signature survives storage and is served back verbatim.
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

    // Attacker signs an event attributed to order-service with its own key.
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

    // Origin service appends its own events; the store auto-indexes them under
    // evt: keys, signed with its key, for pull serving.
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

    // The owned events are now servable via the pull query, signed.
    var origin_query: TestQueryBuffer = undefined;
    const served = try origin_query.query(&origin, "order-service", "order.created", 0);
    try std.testing.expectEqual(@as(usize, 2), served.events.len);
    for (served.events) |ev| {
        try std.testing.expect(ev.is_signed());
        try std.testing.expect(ev.verify_provenance(key.public_key()));
    }

    // A consumer that declares the owner's public key ingests them, verifying
    // authorship — the full producer -> signed pull -> verified ingest loop.
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
    // First write wins
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
