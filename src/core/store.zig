const std = @import("std");
const ArrayList = std.ArrayList;

const radix = @import("radix.zig");
const wal = @import("wal.zig");
const event = @import("event.zig");
const breadcrumb_mod = @import("breadcrumb.zig");

const Io = std.Io;
const RadixTree = radix.RadixTree;
const WriteAheadLog = wal.WriteAheadLog;
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

pub const EventStore = struct {
    allocator: std.mem.Allocator,

    memtable: RadixTree,

    wal: *WriteAheadLog,

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8, io: Io) !EventStore {
        var memtable = try RadixTree.init(allocator);
        errdefer memtable.deinit();

        const wal_ptr = try allocator.create(WriteAheadLog);
        errdefer allocator.destroy(wal_ptr);

        wal_ptr.* = try WriteAheadLog.init(allocator, wal_path, io);
        errdefer wal_ptr.deinit();

        _ = try wal_ptr.replay(&memtable);

        return EventStore{
            .allocator = allocator,
            .memtable = memtable,
            .wal = wal_ptr,
        };
    }

    pub fn deinit(self: *EventStore) void {
        self.memtable.deinit();
        self.wal.deinit();
        self.allocator.destroy(self.wal);
    }

    pub fn put(self: *EventStore, key: []const u8, value: EventValue) !void {
        try self.wal.append_event(key, value);
        try self.memtable.insert(key, value);
    }

    pub fn get(self: *const EventStore, key: []const u8) ?*const EventValue {
        return self.memtable.get(key);
    }

    pub fn scan_range(self: *const EventStore, prefix: []const u8) !ArrayList(KeyValuePair) {
        var results = ArrayList(KeyValuePair){};
        try self.memtable.scan_prefix(prefix, &results);
        return results;
    }

    pub fn get_stats(self: *const EventStore) void {
        std.debug.print("Store Statistics:\n", .{});
        std.debug.print("Memtable entries: {}\n", .{self.memtable.size});
    }

    pub fn put_breadcrumb(self: *EventStore, bc: Breadcrumb, local_sequence: u64, self_service: []const u8, timestamp: i64) !void {
        const key = try Breadcrumb.makeKey(self.allocator, bc.source_service, bc.event_type);
        defer self.allocator.free(key);

        const ev = try bc.toEventValue(self.allocator, local_sequence, self_service, timestamp);
        try self.put(key, ev);
    }

    pub fn get_breadcrumb(self: *const EventStore, source_service: []const u8, event_type: []const u8) !?Breadcrumb {
        const key = try Breadcrumb.makeKey(self.allocator, source_service, event_type);
        defer self.allocator.free(key);

        if (self.get(key)) |ev| {
            return try Breadcrumb.fromWalEntry(self.allocator, key, ev.*);
        }

        return null;
    }

    pub fn put_event(self: *EventStore, value: EventValue) !void {
        const key = try makeEventKey(self.allocator, value.origin_service, value.event_type, value.sequence);
        defer self.allocator.free(key);
        try self.put(key, value);
    }

    pub fn query_events(self: *const EventStore, origin: []const u8, event_type: []const u8, from_sequence: u64) !ArrayList(EventValue) {
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

    pub fn get_breadcrumbs(self: *const EventStore) !ArrayList(Breadcrumb) {
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
}

test "basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_basic_operations.wal", io);
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
            @truncate((try std.time.Instant.now()).timestamp.nsec),
        );
        try event_store.put(sample.key, value);
    }

    if (event_store.get("user-service:u123:profile")) |found_value| {
        try std.testing.expectEqualStrings("user.registered", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1);
    } else {
        return error.ExpectedValueNotFound;
    }

    const nonexistent = event_store.get("nonexistent");
    try std.testing.expect(nonexistent == null);
}

test "bulk inserts" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_bulk_inserts.wal", io);
    defer event_store.deinit();

    for (0..150) |i| {
        const key = try std.fmt.allocPrint(allocator, "bulk-service:b{}:event", .{i});
        defer allocator.free(key);

        const payload = try std.fmt.allocPrint(allocator, "{{\"id\":{}}}", .{i});
        defer allocator.free(payload);

        const value = try EventValue.init(allocator, i + 1000, "test-service", "bulk.insert", payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
        try event_store.put(key, value);
    }

    if (event_store.get("bulk-service:b0:event")) |found_value| {
        try std.testing.expectEqualStrings("bulk.insert", found_value.event_type);
        try std.testing.expect(found_value.sequence == 1000);
    } else {
        return error.ExpectedBulkValueNotFound;
    }

    if (event_store.get("bulk-service:b149:event")) |found_value| {
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_range_scans.wal", io);
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
        const value = try EventValue.init(allocator, i + 2000, "test-service", sample.event_type, sample.payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_concurrent_simulation.wal", io);
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
            const value = try EventValue.init(allocator, sequence, "test-service", op.event_type, op.payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
            try event_store.put(op.key, value);
            writes_performed += 1;
            sequence += 1;
        } else if (std.mem.eql(u8, op.operation_type, "read")) {
            if (event_store.get(op.key)) |_| {
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var store1 = try EventStore.init(allocator, "data/test_instance1.wal", io);
    defer store1.deinit();

    var store2 = try EventStore.init(allocator, "data/test_instance2.wal", io);
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
            @truncate((try std.time.Instant.now()).timestamp.nsec),
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
            @truncate((try std.time.Instant.now()).timestamp.nsec),
        );
        try store2.put(test_event.key, value);
    }

    const instance1_keys = [_][]const u8{ "user:alice", "order:12345", "session:alice-s1" };
    for (instance1_keys) |key| {
        if (store1.get(key)) |_| {} else {
            return error.Instance1DataNotFound;
        }
    }

    const instance2_keys = [_][]const u8{ "user:bob", "order:67890", "session:bob-s1" };
    for (instance2_keys) |key| {
        const found_in_store1 = store1.get(key);
        try std.testing.expect(found_in_store1 == null);
    }

    for (instance2_keys) |key| {
        if (store2.get(key)) |_| {} else {
            return error.Instance2DataNotFound;
        }
    }

    for (instance1_keys) |key| {
        const found_in_store2 = store2.get(key);
        try std.testing.expect(found_in_store2 == null);
    }
}

test "edge cases" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, "data/test_edge_cases.wal", io);
    defer event_store.deinit();

    const empty_value = try EventValue.init(
        allocator,
        6000,
        "test-service",
        "",
        "",
        @truncate((try std.time.Instant.now()).timestamp.nsec),
    );
    try event_store.put("edge:empty", empty_value);

    if (event_store.get("edge:empty")) |found_value| {
        try std.testing.expect(found_value.event_type.len == 0);
        try std.testing.expect(found_value.payload.len == 0);
    } else {
        return error.EmptyValueNotFound;
    }

    const long_key = try std.fmt.allocPrint(allocator, "edge:very:long:key:with:many:segments:and:even:more:segments:to:make:it:really:long:{}", .{(try std.time.Instant.now()).timestamp.nsec});
    defer allocator.free(long_key);

    const long_payload = try std.fmt.allocPrint(
        allocator,
        "{{\"description\":\"This is a very long payload designed to test how the system handles large amounts of data in a single event. It contains multiple sentences and should stress test the serialization and storage mechanisms.\",\"data\":[{}]}}",
        .{(try std.time.Instant.now()).timestamp.nsec},
    );
    defer allocator.free(long_payload);

    const long_value = try EventValue.init(allocator, 6001, "test-service", "test.long_data", long_payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
    try event_store.put(long_key, long_value);

    if (event_store.get(long_key)) |found_value| {
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
            @truncate((try std.time.Instant.now()).timestamp.nsec),
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
        (try std.time.Instant.now()).timestamp.nsec,
    );
    try event_store.put(overwrite_key, value1);

    const value2 = try EventValue.init(
        allocator,
        6201,
        "test-service",
        "test.second",
        "second value",
        (try std.time.Instant.now()).timestamp.nsec,
    );
    try event_store.put(overwrite_key, value2);

    if (event_store.get(overwrite_key)) |found_value| {
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_bc_roundtrip.wal") catch {};
    std.fs.cwd().deleteFile("data/test_bc_roundtrip.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_bc_roundtrip.wal", io);
    defer {
        event_store.deinit();
        std.fs.cwd().deleteFile("data/test_bc_roundtrip.wal") catch {};
        std.fs.cwd().deleteFile("data/test_bc_roundtrip.crumb") catch {};
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_bc_list.wal") catch {};
    std.fs.cwd().deleteFile("data/test_bc_list.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_bc_list.wal", io);
    defer {
        event_store.deinit();
        std.fs.cwd().deleteFile("data/test_bc_list.wal") catch {};
        std.fs.cwd().deleteFile("data/test_bc_list.crumb") catch {};
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_bc_coexist.wal") catch {};
    std.fs.cwd().deleteFile("data/test_bc_coexist.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_bc_coexist.wal", io);
    defer {
        event_store.deinit();
        std.fs.cwd().deleteFile("data/test_bc_coexist.wal") catch {};
        std.fs.cwd().deleteFile("data/test_bc_coexist.crumb") catch {};
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

    if (event_store.get("user-service:u1:profile")) |found| {
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_bc_replay.wal") catch {};
    std.fs.cwd().deleteFile("data/test_bc_replay.crumb") catch {};

    {
        var event_store = try EventStore.init(allocator, "data/test_bc_replay.wal", io);
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
        var event_store = try EventStore.init(allocator, "data/test_bc_replay.wal", io);
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

    std.fs.cwd().deleteFile("data/test_bc_replay.wal") catch {};
    std.fs.cwd().deleteFile("data/test_bc_replay.crumb") catch {};
}

test "put_event and query_events basic" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_event_query.wal") catch {};
    std.fs.cwd().deleteFile("data/test_event_query.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_query.wal", io);
    defer {
        store.deinit();
        std.fs.cwd().deleteFile("data/test_event_query.wal") catch {};
        std.fs.cwd().deleteFile("data/test_event_query.crumb") catch {};
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_event_filter.wal") catch {};
    std.fs.cwd().deleteFile("data/test_event_filter.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_filter.wal", io);
    defer {
        store.deinit();
        std.fs.cwd().deleteFile("data/test_event_filter.wal") catch {};
        std.fs.cwd().deleteFile("data/test_event_filter.crumb") catch {};
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_event_isolate.wal") catch {};
    std.fs.cwd().deleteFile("data/test_event_isolate.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_isolate.wal", io);
    defer {
        store.deinit();
        std.fs.cwd().deleteFile("data/test_event_isolate.wal") catch {};
        std.fs.cwd().deleteFile("data/test_event_isolate.crumb") catch {};
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_event_noleak.wal") catch {};
    std.fs.cwd().deleteFile("data/test_event_noleak.crumb") catch {};

    var store = try EventStore.init(allocator, "data/test_event_noleak.wal", io);
    defer {
        store.deinit();
        std.fs.cwd().deleteFile("data/test_event_noleak.wal") catch {};
        std.fs.cwd().deleteFile("data/test_event_noleak.crumb") catch {};
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

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.fs.cwd().deleteFile("data/test_event_replay.wal") catch {};
    std.fs.cwd().deleteFile("data/test_event_replay.crumb") catch {};

    {
        var store = try EventStore.init(allocator, "data/test_event_replay.wal", io);
        defer store.deinit();

        for (1..4) |i| {
            const ev = try EventValue.init(allocator, i, "order-service", "order.created", "{}", @intCast(i));
            try store.put_event(ev);
        }
    }

    {
        var store = try EventStore.init(allocator, "data/test_event_replay.wal", io);
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

    std.fs.cwd().deleteFile("data/test_event_replay.wal") catch {};
    std.fs.cwd().deleteFile("data/test_event_replay.crumb") catch {};
}
