const std = @import("std");
const ArrayList = std.ArrayList;

const lsm_mod = @import("lsm.zig");
const event = @import("event.zig");
const breadcrumb_mod = @import("breadcrumb.zig");
const contract_mod = @import("contract.zig");
const time_mod = @import("time.zig");

const DependencyContract = contract_mod.DependencyContract;

const Io = std.Io;
const LSMEngine = lsm_mod.LSMEngine;
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;
const Breadcrumb = breadcrumb_mod.Breadcrumb;

pub const EVENT_PREFIX = "evt:";

pub fn makeEventKey(allocator: std.mem.Allocator, origin: []const u8, event_type: []const u8, sequence: u64) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "evt:{s}:{s}:{x:0>16}", .{ origin, event_type, sequence });
}

pub fn makeEventPrefix(allocator: std.mem.Allocator, origin: []const u8, event_type: []const u8) ![]const u8 {
    return try std.fmt.allocPrint(allocator, "evt:{s}:{s}:", .{ origin, event_type });
}

pub const Config = struct {
    identity: ?[]const u8 = null,
    contracts: []const DependencyContract = &.{},
};

pub const PutResult = struct {
    sequence: u64,
    timestamp: i64,
};

pub const EventStore = struct {
    allocator: std.mem.Allocator,
    lsm: *LSMEngine,
    identity: ?[]const u8,
    contracts: []const DependencyContract,
    next_sequence: u64,
    io: Io,

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8, io: Io, config: Config) !EventStore {
        const lsm_ptr = try allocator.create(LSMEngine);
        errdefer allocator.destroy(lsm_ptr);

        lsm_ptr.* = try LSMEngine.init(allocator, wal_path, io);
        errdefer lsm_ptr.deinit();

        var next_seq: u64 = 0;
        if (config.identity) |id| {
            const prefix = try std.fmt.allocPrint(allocator, "evt:{s}:", .{id});
            defer allocator.free(prefix);

            var local_events = ArrayList(KeyValuePair){};
            try lsm_ptr.scan_prefix(prefix, &local_events);
            defer {
                for (local_events.items) |*item| item.deinit(allocator);
                local_events.deinit(allocator);
            }

            for (local_events.items) |item| {
                if (item.value.sequence > next_seq) next_seq = item.value.sequence;
            }
        }

        return EventStore{
            .allocator = allocator,
            .lsm = lsm_ptr,
            .identity = config.identity,
            .contracts = config.contracts,
            .next_sequence = next_seq,
            .io = io,
        };
    }

    pub fn deinit(self: *EventStore) void {
        self.lsm.deinit();
        self.allocator.destroy(self.lsm);
    }

    pub fn put(self: *EventStore, key: []const u8, value: EventValue) !void {
        try self.lsm.put(key, value);
        try self.lsm.sync();
    }

    pub fn get(self: *EventStore, key: []const u8) !?EventValue {
        return try self.lsm.get(self.allocator, key);
    }

    pub fn scan_range(self: *EventStore, prefix: []const u8) !ArrayList(KeyValuePair) {
        var results = ArrayList(KeyValuePair){};
        try self.lsm.scan_prefix(prefix, &results);
        return results;
    }

    pub fn get_stats(self: *const EventStore) void {
        std.debug.print("Store Statistics:\n", .{});
        std.debug.print("Memtable entries: {}\n", .{self.lsm.memtableCount()});
        std.debug.print("SSTable count: {}\n", .{self.lsm.sstableCount()});
    }

    pub fn put_breadcrumb(self: *EventStore, bc: Breadcrumb, local_sequence: u64, self_service: []const u8, timestamp: i64) !void {
        const key = try Breadcrumb.makeKey(self.allocator, bc.source_service, bc.event_type);
        defer self.allocator.free(key);

        const ev = try bc.toEventValue(self.allocator, local_sequence, self_service, timestamp);
        try self.put(key, ev);
    }

    pub fn get_breadcrumb(self: *EventStore, source_service: []const u8, event_type: []const u8) !?Breadcrumb {
        const key = try Breadcrumb.makeKey(self.allocator, source_service, event_type);
        defer self.allocator.free(key);

        if (try self.get(key)) |ev_c| {
            var ev = ev_c;
            defer ev.deinit(self.allocator);
            return try Breadcrumb.fromWalEntry(self.allocator, key, ev);
        }

        return null;
    }

    pub fn put_event(self: *EventStore, value: EventValue) !void {
        const key = try makeEventKey(self.allocator, value.origin_service, value.event_type, value.sequence);
        defer self.allocator.free(key);
        try self.put(key, value);
    }

    pub fn put_event_local(self: *EventStore, event_type: []const u8, payload: []const u8) !PutResult {
        const id = self.identity orelse return error.NoIdentity;

        self.next_sequence += 1;
        const seq = self.next_sequence;
        const now: i64 = @intCast(time_mod.Instant.now(self.io).timestamp.sec);

        var ev = try EventValue.init(self.allocator, seq, id, event_type, payload, now);
        errdefer ev.deinit(self.allocator);

        try self.put_event(ev);
        return .{ .sequence = seq, .timestamp = now };
    }

    pub fn ingest_event(self: *EventStore, value: EventValue) !void {
        if (!self.isAllowedByContract(value.origin_service, value.event_type))
            return error.UnauthorizedOrigin;

        const key = try makeEventKey(self.allocator, value.origin_service, value.event_type, value.sequence);
        defer self.allocator.free(key);
        if (try self.lsm.contains(key)) {
            var v = value;
            v.deinit(self.allocator);
            return;
        }

        try self.put(key, value);
    }

    fn isAllowedByContract(self: *const EventStore, origin: []const u8, event_type: []const u8) bool {
        if (self.identity == null) return true;
        if (std.mem.eql(u8, origin, self.identity.?)) return true;
        for (self.contracts) |contract| {
            if (contract.matchesEvent(origin, event_type)) return true;
        }
        return false;
    }

    pub fn query_events(self: *EventStore, origin: []const u8, event_type: []const u8, from_sequence: u64) !ArrayList(EventValue) {
        const prefix = try makeEventPrefix(self.allocator, origin, event_type);
        defer self.allocator.free(prefix);

        var scan_results = try self.scan_range(prefix);
        defer scan_results.deinit(self.allocator);

        var results = ArrayList(EventValue){};
        errdefer {
            for (results.items) |*v| v.deinit(self.allocator);
            results.deinit(self.allocator);
        }

        for (scan_results.items) |*item| {
            if (item.value.sequence >= from_sequence) {
                try results.append(self.allocator, item.value);
                self.allocator.free(item.key);
            } else {
                item.deinit(self.allocator);
            }
        }

        std.mem.sort(EventValue, results.items, {}, struct {
            fn lessThan(_: void, a: EventValue, b: EventValue) bool {
                return a.sequence < b.sequence;
            }
        }.lessThan);

        return results;
    }

    pub fn get_breadcrumbs(self: *EventStore) !ArrayList(Breadcrumb) {
        var scan_results = try self.scan_range(breadcrumb_mod.KEY_PREFIX);
        defer {
            for (scan_results.items) |*item| {
                item.deinit(self.allocator);
            }
            scan_results.deinit(self.allocator);
        }

        var breadcrumbs = ArrayList(Breadcrumb){};
        errdefer {
            for (breadcrumbs.items) |*bc| bc.deinit(self.allocator);
            breadcrumbs.deinit(self.allocator);
        }

        for (scan_results.items) |item| {
            if (try Breadcrumb.fromWalEntry(self.allocator, item.key, item.value)) |bc| {
                try breadcrumbs.append(self.allocator, bc);
            }
        }

        return breadcrumbs;
    }
};

test {
    _ = @import("wal.zig");
    _ = @import("wal_cursor.zig");
    _ = @import("breadcrumb.zig");
    _ = @import("event.zig");
    _ = @import("contract.zig");
    _ = @import("recovery.zig");
    _ = @import("identity.zig");
    _ = @import("protocol.zig");
    _ = @import("transport.zig");
    _ = @import("config.zig");
    _ = @import("server.zig");
    _ = @import("memtable.zig");
    _ = @import("sstable.zig");
    _ = @import("manifest.zig");
    _ = @import("merge_iterator.zig");
    _ = @import("lsm.zig");
}

test "basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
        .{ .key = "user-service:u123:profile", .event_type = "user.registered", .payload = "{\"email\":\"john@example.com\"}" },
        .{ .key = "user-service:u124:profile", .event_type = "user.registered", .payload = "{\"email\":\"jane@example.com\"}" },
        .{ .key = "order-service:o456:order", .event_type = "order.created", .payload = "{\"amount\":99.99,\"user\":\"u123\"}" },
        .{ .key = "user-service:u123:settings", .event_type = "user.updated", .payload = "{\"theme\":\"dark\"}" },
    };

    for (sample_events, 0..) |sample, i| {
        const value = try EventValue.init(
            allocator,
            i + 1,
            "test-service",
            sample.event_type,
            sample.payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try event_store.put(sample.key, value);
    }

    if (try event_store.get("user-service:u123:profile")) |fv_c| {
        var found_value = fv_c;
        defer found_value.deinit(allocator);
        try std.testing.expectEqualStrings("user.registered", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1);
    } else {
        return error.ExpectedValueNotFound;
    }

    const nonexistent = try event_store.get("nonexistent");
    try std.testing.expect(nonexistent == null);
}

test "bulk inserts" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

        const value = try EventValue.init(
            allocator,
            i + 1000,
            "test-service",
            "bulk.insert",
            payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try event_store.put(key, value);
    }

    if (try event_store.get("bulk-service:b0:event")) |fv_c| {
        var found_value = fv_c;
        defer found_value.deinit(allocator);
        try std.testing.expectEqualStrings("bulk.insert", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1000);
    } else {
        return error.ExpectedBulkValueNotFound;
    }

    if (try event_store.get("bulk-service:b149:event")) |fv_c| {
        var found_value = fv_c;
        defer found_value.deinit(allocator);
        try std.testing.expectEqualStrings("bulk.insert", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1149);
    } else {
        return error.ExpectedLastBulkValueNotFound;
    }
}

test "range scans" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
        .{ .key = "user-service:u123:profile", .event_type = "user.registered", .payload = "{\"email\":\"john@example.com\"}" },
        .{ .key = "user-service:u124:profile", .event_type = "user.registered", .payload = "{\"email\":\"jane@example.com\"}" },
        .{ .key = "order-service:o456:order", .event_type = "order.created", .payload = "{\"amount\":99.99,\"user\":\"u123\"}" },
        .{ .key = "user-service:u123:settings", .event_type = "user.updated", .payload = "{\"theme\":\"dark\"}" },
    };

    for (sample_events, 0..) |sample, i| {
        const value = try EventValue.init(allocator, i + 2000, "test-service", sample.event_type, sample.payload, @truncate(time_mod.Instant.now(io).timestamp.nsec));
        try event_store.put(sample.key, value);
    }

    var user_events = try event_store.scan_range("user-service:");
    defer {
        for (user_events.items) |*item| {
            item.deinit(event_store.allocator);
        }
        user_events.deinit(event_store.allocator);
    }

    try std.testing.expect(user_events.items.len == 3);

    var found_profile = false;
    var found_settings = false;
    for (user_events.items) |item| {
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
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_concurrent_simulation.wal", io, .{});
    defer event_store.deinit();

    const operations = [_]struct {
        operation_type: []const u8,
        key: []const u8,
        event_type: []const u8,
        payload: []const u8,
    }{
        .{ .operation_type = "write", .key = "session:s1:login", .event_type = "user.login", .payload = "{\"user\":\"alice\",\"ip\":\"192.168.1.1\"}" },
        .{ .operation_type = "write", .key = "cart:c1:add", .event_type = "cart.item_added", .payload = "{\"item\":\"laptop\",\"qty\":1}" },
        .{ .operation_type = "read", .key = "session:s1:login", .event_type = "", .payload = "" },
        .{ .operation_type = "write", .key = "session:s1:logout", .event_type = "user.logout", .payload = "{\"duration\":3600}" },
        .{ .operation_type = "write", .key = "cart:c1:checkout", .event_type = "cart.checkout", .payload = "{\"total\":1299.99}" },
        .{ .operation_type = "read", .key = "cart:c1:add", .event_type = "", .payload = "" },
        .{ .operation_type = "write", .key = "session:s2:login", .event_type = "user.login", .payload = "{\"user\":\"bob\",\"ip\":\"192.168.1.2\"}" },
    };

    var sequence: u64 = 3000;
    var writes_performed: u32 = 0;
    var reads_performed: u32 = 0;

    for (operations) |op| {
        if (std.mem.eql(u8, op.operation_type, "write")) {
            const value = try EventValue.init(allocator, sequence, "test-service", op.event_type, op.payload, @truncate(time_mod.Instant.now(io).timestamp.nsec));
            try event_store.put(op.key, value);
            writes_performed += 1;
            sequence += 1;
        } else if (std.mem.eql(u8, op.operation_type, "read")) {
            if (try event_store.get(op.key)) |fv_c| {
                var fv = fv_c;
                fv.deinit(allocator);
                reads_performed += 1;
            }
        }
    }

    try std.testing.expect(writes_performed == 5);
    try std.testing.expect(reads_performed == 2);

    var concurrent_scan = try event_store.scan_range("session:");
    defer {
        for (concurrent_scan.items) |*item| {
            item.deinit(event_store.allocator);
        }
        concurrent_scan.deinit(event_store.allocator);
    }
    try std.testing.expect(concurrent_scan.items.len >= 2);
}

test "persistence recovery" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
        .{ .key = "user:alice", .event_type = "user.registered", .payload = "{\"email\":\"alice@example.com\",\"instance\":1}" },
        .{ .key = "order:12345", .event_type = "order.created", .payload = "{\"amount\":99.99,\"user\":\"alice\"}" },
        .{ .key = "session:alice-s1", .event_type = "user.login", .payload = "{\"user\":\"alice\",\"ip\":\"192.168.1.1\"}" },
    };

    for (instance1_events, 0..) |test_event, i| {
        const value = try EventValue.init(
            allocator,
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
        .{ .key = "user:bob", .event_type = "user.registered", .payload = "{\"email\":\"bob@example.com\",\"instance\":2}" },
        .{ .key = "order:67890", .event_type = "order.created", .payload = "{\"amount\":149.99,\"user\":\"bob\"}" },
        .{ .key = "session:bob-s1", .event_type = "user.login", .payload = "{\"user\":\"bob\",\"ip\":\"192.168.1.2\"}" },
    };

    for (instance2_events, 0..) |test_event, i| {
        const value = try EventValue.init(
            allocator,
            i + 4100,
            "test-service",
            test_event.event_type,
            test_event.payload,
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try store2.put(test_event.key, value);
    }

    const instance1_keys = [_][]const u8{ "user:alice", "order:12345", "session:alice-s1" };
    for (instance1_keys) |key| {
        if (try store1.get(key)) |fv_c| {
            var fv = fv_c;
            fv.deinit(allocator);
        } else {
            return error.Instance1DataNotFound;
        }
    }

    const instance2_keys = [_][]const u8{ "user:bob", "order:67890", "session:bob-s1" };
    for (instance2_keys) |key| {
        const found_in_store1 = try store1.get(key);
        try std.testing.expect(found_in_store1 == null);
    }

    for (instance2_keys) |key| {
        if (try store2.get(key)) |fv_c| {
            var fv = fv_c;
            fv.deinit(allocator);
        } else {
            return error.Instance2DataNotFound;
        }
    }

    for (instance1_keys) |key| {
        const found_in_store2 = try store2.get(key);
        try std.testing.expect(found_in_store2 == null);
    }
}

test "edge cases" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_edge_cases.wal", io, .{});
    defer event_store.deinit();

    const empty_value = try EventValue.init(
        allocator,
        6000,
        "test-service",
        "",
        "",
        @truncate(time_mod.Instant.now(io).timestamp.nsec),
    );
    try event_store.put("edge:empty", empty_value);

    if (try event_store.get("edge:empty")) |fv_c| {
        var found_value = fv_c;
        defer found_value.deinit(allocator);
        try std.testing.expect(found_value.event_type.len == 0);
        try std.testing.expect(found_value.payload.len == 0);
    } else {
        return error.EmptyValueNotFound;
    }

    const long_key = try std.fmt.allocPrint(allocator, "edge:very:long:key:with:many:segments:and:even:more:segments:to:make:it:really:long:{}", .{time_mod.Instant.now(io).timestamp.nsec});
    defer allocator.free(long_key);

    const long_payload = try std.fmt.allocPrint(
        allocator,
        "{{\"description\":\"This is a very long payload designed to test how the system handles large amounts of data in a single event. It contains multiple sentences and should stress test the serialization and storage mechanisms.\",\"data\":[{}]}}",
        .{time_mod.Instant.now(io).timestamp.nsec},
    );
    defer allocator.free(long_payload);

    const long_value = try EventValue.init(allocator, 6001, "test-service", "test.long_data", long_payload, @truncate(time_mod.Instant.now(io).timestamp.nsec));
    try event_store.put(long_key, long_value);

    if (try event_store.get(long_key)) |fv_c| {
        var found_value = fv_c;
        defer found_value.deinit(allocator);
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
        const value = try EventValue.init(
            allocator,
            6100 + i,
            "test-service",
            "test.special",
            "special payload",
            @truncate(time_mod.Instant.now(io).timestamp.nsec),
        );
        try event_store.put(key, value);
    }

    var special_scan = try event_store.scan_range("edge:special:");
    defer {
        for (special_scan.items) |*item| {
            item.deinit(event_store.allocator);
        }
        special_scan.deinit(event_store.allocator);
    }
    try std.testing.expect(special_scan.items.len == 4);

    const overwrite_key = "edge:overwrite:test";

    const value1 = try EventValue.init(
        allocator,
        6200,
        "test-service",
        "test.first",
        "first value",
        time_mod.Instant.now(io).timestamp.nsec,
    );
    try event_store.put(overwrite_key, value1);

    const value2 = try EventValue.init(
        allocator,
        6201,
        "test-service",
        "test.second",
        "second value",
        time_mod.Instant.now(io).timestamp.nsec,
    );
    try event_store.put(overwrite_key, value2);

    if (try event_store.get(overwrite_key)) |fv_c| {
        var found_value = fv_c;
        defer found_value.deinit(allocator);
        try std.testing.expectEqualStrings("test.second", found_value.event_type);
        try std.testing.expect(found_value.sequence == 6201);
    } else {
        return error.OverwriteValueNotFound;
    }
}

test "put_breadcrumb and get_breadcrumb round-trip" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    const maybe_bc = try event_store.get_breadcrumb("order-service", "order.created");
    try std.testing.expect(maybe_bc != null);

    var restored = maybe_bc.?;
    defer restored.deinit(allocator);

    try std.testing.expectEqualStrings("order-service", restored.source_service);
    try std.testing.expectEqualStrings("order.created", restored.event_type);
    try std.testing.expectEqual(@as(u64, 42), restored.last_sequence);
    try std.testing.expectEqualStrings("10.0.0.5:4200", restored.peer_address);
}

test "get_breadcrumbs returns all breadcrumbs" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    var breadcrumbs = try event_store.get_breadcrumbs();
    defer {
        for (breadcrumbs.items) |*item| item.deinit(allocator);
        breadcrumbs.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), breadcrumbs.items.len);
}

test "breadcrumbs and domain events coexist" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    const domain_ev = try EventValue.init(allocator, 1, "my-service", "user.registered", "{\"email\":\"test@test.com\"}", 100);
    try event_store.put("user-service:u1:profile", domain_ev);

    const bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 5,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 200,
    };
    try event_store.put_breadcrumb(bc, 2, "my-service", 200);

    if (try event_store.get("user-service:u1:profile")) |fv_c| {
        var found = fv_c;
        defer found.deinit(allocator);
        try std.testing.expectEqualStrings("user.registered", found.event_type);
    } else {
        return error.DomainEventNotFound;
    }

    const maybe_bc = try event_store.get_breadcrumb("order-service", "order.created");
    try std.testing.expect(maybe_bc != null);
    var restored = maybe_bc.?;
    defer restored.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 5), restored.last_sequence);

    var user_scan = try event_store.scan_range("user-service:");
    defer {
        for (user_scan.items) |*item| item.deinit(allocator);
        user_scan.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), user_scan.items.len);
}

test "breadcrumb survives WAL replay" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

        const maybe_bc = try event_store.get_breadcrumb("order-service", "order.created");
        try std.testing.expect(maybe_bc != null);

        var restored = maybe_bc.?;
        defer restored.deinit(allocator);

        try std.testing.expectEqualStrings("order-service", restored.source_service);
        try std.testing.expectEqualStrings("order.created", restored.event_type);
        try std.testing.expectEqual(@as(u64, 99), restored.last_sequence);
        try std.testing.expectEqualStrings("10.0.0.5:4200", restored.peer_address);
    }

    Io.Dir.cwd().deleteFile(io, "data/test_bc_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_bc_replay.crumb") catch {};
}

test "put_event and query_events basic" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
        const ev = try EventValue.init(allocator, i, "order-service", "order.created", payload, @intCast(i * 100));
        try store.put_event(ev);
    }

    var all = try store.query_events("order-service", "order.created", 0);
    defer {
        for (all.items) |*v| v.deinit(allocator);
        all.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 5), all.items.len);

    for (all.items, 0..) |item, i| {
        try std.testing.expectEqual(@as(u64, i + 1), item.sequence);
    }
}

test "query_events filters by from_sequence" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
        const ev = try EventValue.init(allocator, i, "order-service", "order.created", "{}", @intCast(i * 100));
        try store.put_event(ev);
    }

    var partial = try store.query_events("order-service", "order.created", 7);
    defer {
        for (partial.items) |*v| v.deinit(allocator);
        partial.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 4), partial.items.len);
    try std.testing.expectEqual(@as(u64, 7), partial.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 10), partial.items[3].sequence);
}

test "query_events isolates by origin and event_type" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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
        const ev = try EventValue.init(allocator, i, "order-service", "order.created", "{}", @intCast(i));
        try store.put_event(ev);
    }
    for (1..3) |i| {
        const ev = try EventValue.init(allocator, i, "user-service", "user.registered", "{}", @intCast(i));
        try store.put_event(ev);
    }
    for (1..3) |i| {
        const ev = try EventValue.init(allocator, i, "order-service", "order.updated", "{}", @intCast(i));
        try store.put_event(ev);
    }

    var orders = try store.query_events("order-service", "order.created", 0);
    defer {
        for (orders.items) |*v| v.deinit(allocator);
        orders.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 3), orders.items.len);

    var users = try store.query_events("user-service", "user.registered", 0);
    defer {
        for (users.items) |*v| v.deinit(allocator);
        users.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 2), users.items.len);

    var updates = try store.query_events("order-service", "order.updated", 0);
    defer {
        for (updates.items) |*v| v.deinit(allocator);
        updates.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 2), updates.items.len);

    var empty = try store.query_events("ghost-service", "ghost.event", 0);
    defer empty.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), empty.items.len);
}

test "query_events does not leak into breadcrumbs or arbitrary keys" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    const ev = try EventValue.init(allocator, 1, "order-service", "order.created", "{}", 100);
    try store.put_event(ev);

    const arb = try EventValue.init(allocator, 2, "order-service", "order.created", "{}", 200);
    try store.put("order-service:o1:order", arb);

    const bc = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 5,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 300,
    };
    try store.put_breadcrumb(bc, 3, "my-service", 300);

    var results = try store.query_events("order-service", "order.created", 0);
    defer {
        for (results.items) |*v| v.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), results.items.len);
    try std.testing.expectEqual(@as(u64, 1), results.items[0].sequence);
}

test "put_event survives WAL replay" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.crumb") catch {};

    {
        var store = try EventStore.init(allocator, "data/test_event_replay.wal", io, .{});
        defer store.deinit();

        for (1..4) |i| {
            const ev = try EventValue.init(allocator, i, "order-service", "order.created", "{}", @intCast(i));
            try store.put_event(ev);
        }
    }

    {
        var store = try EventStore.init(allocator, "data/test_event_replay.wal", io, .{});
        defer store.deinit();

        var results = try store.query_events("order-service", "order.created", 0);
        defer {
            for (results.items) |*v| v.deinit(allocator);
            results.deinit(allocator);
        }
        try std.testing.expectEqual(@as(usize, 3), results.items.len);
        try std.testing.expectEqual(@as(u64, 1), results.items[0].sequence);
        try std.testing.expectEqual(@as(u64, 3), results.items[2].sequence);
    }

    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_event_replay.crumb") catch {};
}

test "ingest_event accepts events matching a declared contract" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    const ev = try EventValue.init(allocator, 1, "order-service", "order.created", "{}", 100);
    try store.ingest_event(ev);

    var results = try store.query_events("order-service", "order.created", 0);
    defer {
        for (results.items) |*v| v.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), results.items.len);
}

test "ingest_event rejects events from undeclared origins" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    var ev = try EventValue.init(allocator, 1, "unknown-service", "order.created", "{}", 100);
    defer ev.deinit(allocator);

    try std.testing.expectError(error.UnauthorizedOrigin, store.ingest_event(ev));
}

test "ingest_event rejects events with undeclared event types" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    var ev = try EventValue.init(allocator, 1, "order-service", "order.deleted", "{}", 100);
    defer ev.deinit(allocator);

    try std.testing.expectError(error.UnauthorizedOrigin, store.ingest_event(ev));
}

test "ingest_event silently deduplicates same (origin, type, sequence)" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    const ev1 = try EventValue.init(allocator, 1, "order-service", "order.created", "{\"first\":true}", 100);
    try store.ingest_event(ev1);

    const ev2 = try EventValue.init(allocator, 1, "order-service", "order.created", "{\"second\":true}", 200);
    try store.ingest_event(ev2);

    var results = try store.query_events("order-service", "order.created", 0);
    defer {
        for (results.items) |*v| v.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), results.items.len);
    // First write wins
    try std.testing.expectEqualStrings("{\"first\":true}", results.items[0].payload);
}

test "ingest_event allows local origin events (origin == identity)" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    const ev = try EventValue.init(allocator, 1, "my-service", "user.registered", "{}", 100);
    try store.ingest_event(ev);

    var results = try store.query_events("my-service", "user.registered", 0);
    defer {
        for (results.items) |*v| v.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), results.items.len);
}

test "ingest_event with no identity set accepts everything" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

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

    const ev = try EventValue.init(allocator, 1, "any-service", "any.event", "{}", 100);
    try store.ingest_event(ev);

    var results = try store.query_events("any-service", "any.event", 0);
    defer {
        for (results.items) |*v| v.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 1), results.items.len);
}

test "put_event_local assigns monotonic sequences" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_put_local.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_put_local.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_put_local.wal", io, .{
        .identity = "my-service",
    });
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_put_local.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_put_local.crumb") catch {};
    }

    const r1 = try store.put_event_local("order.created", "{\"id\":1}");
    const r2 = try store.put_event_local("order.created", "{\"id\":2}");
    const r3 = try store.put_event_local("user.registered", "{\"name\":\"alice\"}");

    try std.testing.expectEqual(@as(u64, 1), r1.sequence);
    try std.testing.expectEqual(@as(u64, 2), r2.sequence);
    try std.testing.expectEqual(@as(u64, 3), r3.sequence);
    try std.testing.expect(r1.timestamp > 0);

    var orders = try store.query_events("my-service", "order.created", 0);
    defer {
        for (orders.items) |*v| v.deinit(allocator);
        orders.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 2), orders.items.len);
    try std.testing.expectEqualStrings("{\"id\":1}", orders.items[0].payload);
    try std.testing.expectEqualStrings("{\"id\":2}", orders.items[1].payload);
    try std.testing.expectEqualStrings("my-service", orders.items[0].origin_service);
}

test "put_event_local rejects when no identity" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_put_local_noident.wal") catch {};

    var store = try EventStore.init(allocator, "data/test_put_local_noident.wal", io, .{});
    defer {
        store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_put_local_noident.wal") catch {};
    }

    const result = store.put_event_local("order.created", "{}");
    try std.testing.expectError(error.NoIdentity, result);
}

test "put_event_local sequence survives WAL replay" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_put_local_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_put_local_replay.crumb") catch {};

    {
        var store = try EventStore.init(allocator, "data/test_put_local_replay.wal", io, .{
            .identity = "my-service",
        });
        defer store.deinit();

        _ = try store.put_event_local("order.created", "{\"id\":1}");
        _ = try store.put_event_local("order.created", "{\"id\":2}");
        _ = try store.put_event_local("order.created", "{\"id\":3}");
    }

    {
        var store = try EventStore.init(allocator, "data/test_put_local_replay.wal", io, .{
            .identity = "my-service",
        });
        defer store.deinit();

        const r = try store.put_event_local("order.created", "{\"id\":4}");
        try std.testing.expectEqual(@as(u64, 4), r.sequence);

        var results = try store.query_events("my-service", "order.created", 0);
        defer {
            for (results.items) |*v| v.deinit(allocator);
            results.deinit(allocator);
        }
        try std.testing.expectEqual(@as(usize, 4), results.items.len);
    }

    Io.Dir.cwd().deleteFile(io, "data/test_put_local_replay.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_put_local_replay.crumb") catch {};
}
