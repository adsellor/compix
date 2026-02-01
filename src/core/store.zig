const std = @import("std");
const ArrayList = std.ArrayList;

const radix = @import("radix.zig");
const lsm = @import("lsm.zig");
const wal = @import("wal.zig");
const event = @import("event.zig");

const RadixTree = radix.RadixTree;
const SSTable = lsm.SSTable;
const WriteAheadLog = wal.WriteAheadLog;
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

pub const HybridEventStore = struct {
    allocator: std.mem.Allocator,

    memtable: RadixTree,
    memtable_size: usize,
    max_memtable_size: usize,

    sstables: ArrayList(SSTable),

    wal: WriteAheadLog,

    compaction_generation: u32,

    pub fn init(allocator: std.mem.Allocator, wal_path: []const u8) !HybridEventStore {
        var memtable = try RadixTree.init(allocator);
        errdefer memtable.deinit();

        var wal_file = try WriteAheadLog.init(allocator, wal_path);
        errdefer wal_file.deinit();

        try wal_file.replay(&memtable);

        return HybridEventStore{
            .allocator = allocator,
            .memtable = memtable,
            .memtable_size = 0,
            .max_memtable_size = 64 * 1024,
            .sstables = ArrayList(SSTable){},
            .wal = wal_file,
            .compaction_generation = 0,
        };
    }

    pub fn deinit(self: *HybridEventStore) void {
        self.memtable.deinit();
        for (self.sstables.items) |*sstable| {
            sstable.deinit();
        }
        self.sstables.deinit(self.allocator);
        self.wal.deinit();
    }

    pub fn put(self: *HybridEventStore, key: []const u8, value: EventValue) !void {
        try self.wal.append_event(key, value);
        try self.memtable.insert(key, value);
        self.memtable_size += key.len + 100;

        if (self.memtable_size > self.max_memtable_size) {
            try self.flush_memtable();
        }

    }

    pub fn get(self: *const HybridEventStore, key: []const u8) ?EventValue {
        if (self.memtable.get(key)) |value| {
            return value;
        }

        var i = self.sstables.items.len;
        while (i > 0) {
            i -= 1;
            if (self.sstables.items[i].get(key)) |value| {
                return value;
            }
        }

        return null;
    }

    pub fn scan_range(self: *const HybridEventStore, prefix: []const u8) !ArrayList(KeyValuePair) {
        var results = ArrayList(KeyValuePair){};
        try self.memtable.scan_prefix(prefix, &results);
        return results;
    }

    fn flush_memtable(self: *HybridEventStore) !void {

        const sstable_path = try std.fmt.allocPrint(self.allocator, "data/sstable_{}.dat", .{self.compaction_generation});
        defer self.allocator.free(sstable_path);

        var sstable = SSTable.init(self.allocator, sstable_path);
        try sstable.write_from_radix(&self.memtable);
        try self.sstables.append(self.allocator, sstable);

        self.memtable.deinit();
        self.memtable = try RadixTree.init(self.allocator);
        self.memtable_size = 0;
        self.compaction_generation += 1;

    }

    pub fn get_stats(self: *const HybridEventStore) void {
        std.debug.print("Store Statistics:\n", .{});
        std.debug.print("Memtable entries: {}\n", .{self.memtable.size});
        std.debug.print("SSTables: {}\n", .{self.sstables.items.len});
        std.debug.print("Generation: {}\n", .{self.compaction_generation});
        std.debug.print("Estimated memtable size: {} bytes\n", .{self.memtable_size});
    }
};

test "basic operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var event_store = try HybridEventStore.init(allocator, "data/test_basic_operations.wal");
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
        const value = try EventValue.init(allocator, i + 1, sample.event_type, sample.payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
        try event_store.put(sample.key, value);
    }

    if (event_store.get("user-service:u123:profile")) |found_value| {
        var value = found_value;
        defer value.deinit(allocator);
        try std.testing.expectEqualStrings("user.registered", value.event_type);
        try std.testing.expect(value.sequence == 1);
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

    var event_store = try HybridEventStore.init(allocator, "data/test_bulk_inserts.wal");
    defer event_store.deinit();

    for (0..150) |i| {
        const key = try std.fmt.allocPrint(allocator, "bulk-service:b{}:event", .{i});
        defer allocator.free(key);

        const payload = try std.fmt.allocPrint(allocator, "{{\"id\":{}}}", .{i});
        defer allocator.free(payload);

        const value = try EventValue.init(allocator, i + 1000, "bulk.insert", payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
        try event_store.put(key, value);
    }

    if (event_store.get("bulk-service:b0:event")) |found_value| {
        var value = found_value;
        defer value.deinit(allocator);
        try std.testing.expectEqualStrings("bulk.insert", value.event_type);
        try std.testing.expect(value.sequence == 1000);
    } else {
        return error.ExpectedBulkValueNotFound;
    }

    if (event_store.get("bulk-service:b149:event")) |found_value| {
        var value = found_value;
        defer value.deinit(allocator);
        try std.testing.expectEqualStrings("bulk.insert", value.event_type);
        try std.testing.expect(value.sequence == 1149);
    } else {
        return error.ExpectedLastBulkValueNotFound;
    }
}

test "range scans" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var event_store = try HybridEventStore.init(allocator, "data/test_range_scans.wal");
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
        const value = try EventValue.init(allocator, i + 2000, sample.event_type, sample.payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
        try event_store.put(sample.key, value);
    }

    var user_events = try event_store.scan_range("user-service:");
    defer {
        for (user_events.items) |item| {
            event_store.allocator.free(item.key);
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

    var event_store = try HybridEventStore.init(allocator, "data/test_concurrent_simulation.wal");
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
            const value = try EventValue.init(allocator, sequence, op.event_type, op.payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
            try event_store.put(op.key, value);
            writes_performed += 1;
            sequence += 1;
        } else if (std.mem.eql(u8, op.operation_type, "read")) {
            if (event_store.get(op.key)) |found_value| {
                var value = found_value;
                defer value.deinit(allocator);
                reads_performed += 1;
            }
        }
    }

    try std.testing.expect(writes_performed == 5);
    try std.testing.expect(reads_performed == 2);

    var concurrent_scan = try event_store.scan_range("session:");
    defer {
        for (concurrent_scan.items) |item| {
            event_store.allocator.free(item.key);
        }
        concurrent_scan.deinit(event_store.allocator);
    }
    try std.testing.expect(concurrent_scan.items.len >= 2);
}

test "persistence recovery" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var store1 = try HybridEventStore.init(allocator, "data/test_instance1.wal");
    defer store1.deinit();

    var store2 = try HybridEventStore.init(allocator, "data/test_instance2.wal");
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
        const value = try EventValue.init(allocator, i + 4000, test_event.event_type, test_event.payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
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
        const value = try EventValue.init(allocator, i + 4100, test_event.event_type, test_event.payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
        try store2.put(test_event.key, value);
    }

    const instance1_keys = [_][]const u8{ "user:alice", "order:12345", "session:alice-s1" };
    for (instance1_keys) |key| {
        if (store1.get(key)) |found_value| {
            var value = found_value;
            defer value.deinit(allocator);
        } else {
            return error.Instance1DataNotFound;
        }
    }

    const instance2_keys = [_][]const u8{ "user:bob", "order:67890", "session:bob-s1" };
    for (instance2_keys) |key| {
        const found_in_store1 = store1.get(key);
        try std.testing.expect(found_in_store1 == null);
    }

    for (instance2_keys) |key| {
        if (store2.get(key)) |found_value| {
            var value = found_value;
            defer value.deinit(allocator);
        } else {
            return error.Instance2DataNotFound;
        }
    }

    for (instance1_keys) |key| {
        const found_in_store2 = store2.get(key);
        try std.testing.expect(found_in_store2 == null);
    }
}

test "stress test" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var event_store = try HybridEventStore.init(allocator, "data/test_stress_test.wal");
    defer event_store.deinit();

    const total_operations = 10000;
    const services = [_][]const u8{ "auth", "payment", "inventory", "shipping", "analytics" };
    const event_types = [_][]const u8{ "created", "updated", "deleted", "processed", "failed" };

    var operations_completed: u32 = 0;

    for (0..total_operations) |i| {
        const service = services[i % services.len];
        const event_type_name = event_types[i % event_types.len];
        const resource_id = i;

        const key = try std.fmt.allocPrint(allocator, "{s}:r{}:operation", .{ service, resource_id });
        defer allocator.free(key);

        const full_event_type = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ service, event_type_name });
        defer allocator.free(full_event_type);

        const payload = try std.fmt.allocPrint(allocator, "{{\"operation_id\":{},\"service\":\"{s}\",\"timestamp\":{}}}", .{ i, service, (try std.time.Instant.now()).timestamp.nsec });
        defer allocator.free(payload);

        const value = try EventValue.init(allocator, i + 5000, full_event_type, payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
        try event_store.put(key, value);

        operations_completed += 1;
    }

    try std.testing.expect(operations_completed == total_operations);

    std.debug.print("\n=== Operation Statistics ===\n", .{});
    std.debug.print("Total Operations: {}\n", .{total_operations});
    std.debug.print("Services: {}\n", .{services.len});
    std.debug.print("Operations per service: {}\n", .{total_operations / services.len});

    std.debug.print("\n=== Operations by Service ===\n", .{});
    for (services) |service| {
        std.debug.print("{s}: {}\n", .{ service, total_operations / services.len });
    }

    var found_count: u32 = 0;
    for (0..50) |i| {
        const random_id = i * (total_operations / 50);
        const service = services[random_id % services.len];

        const test_key = try std.fmt.allocPrint(allocator, "{s}:r{}:operation", .{ service, random_id });
        defer allocator.free(test_key);

        if (event_store.get(test_key)) |found_value| {
            var value = found_value;
            defer value.deinit(allocator);
            found_count += 1;
        }
    }
    try std.testing.expect(found_count > 0);
}

test "edge cases" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var event_store = try HybridEventStore.init(allocator, "data/test_edge_cases.wal");
    defer event_store.deinit();

    const empty_value = try EventValue.init(allocator, 6000, "", "", @truncate((try std.time.Instant.now()).timestamp.nsec));
    try event_store.put("edge:empty", empty_value);

    if (event_store.get("edge:empty")) |found_value| {
        var value = found_value;
        defer value.deinit(allocator);
        try std.testing.expect(value.event_type.len == 0);
        try std.testing.expect(value.payload.len == 0);
    } else {
        return error.EmptyValueNotFound;
    }

    const long_key = try std.fmt.allocPrint(allocator, "edge:very:long:key:with:many:segments:and:even:more:segments:to:make:it:really:long:{}", .{(try std.time.Instant.now()).timestamp.nsec});
    defer allocator.free(long_key);

    const long_payload = try std.fmt.allocPrint(allocator, "{{\"description\":\"This is a very long payload designed to test how the system handles large amounts of data in a single event. It contains multiple sentences and should stress test the serialization and storage mechanisms.\",\"data\":[{}]}}", .{(try std.time.Instant.now()).timestamp.nsec});
    defer allocator.free(long_payload);

    const long_value = try EventValue.init(allocator, 6001, "test.long_data", long_payload, @truncate((try std.time.Instant.now()).timestamp.nsec));
    try event_store.put(long_key, long_value);

    if (event_store.get(long_key)) |found_value| {
        var value = found_value;
        defer value.deinit(allocator);
        try std.testing.expect(long_key.len > 90);
        try std.testing.expect(value.payload.len > 200);
        try std.testing.expectEqualStrings("test.long_data", value.event_type);
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
        const value = try EventValue.init(allocator, 6100 + i, "test.special", "special payload", @truncate((try std.time.Instant.now()).timestamp.nsec));
        try event_store.put(key, value);
    }

    var special_scan = try event_store.scan_range("edge:special:");
    defer {
        for (special_scan.items) |item| {
            event_store.allocator.free(item.key);
        }
        special_scan.deinit(event_store.allocator);
    }
    try std.testing.expect(special_scan.items.len == 4);

    const overwrite_key = "edge:overwrite:test";

    const value1 = try EventValue.init(allocator, 6200, "test.first", "first value", (try std.time.Instant.now()).timestamp.nsec);
    try event_store.put(overwrite_key, value1);

    const value2 = try EventValue.init(allocator, 6201, "test.second", "second value", (try std.time.Instant.now()).timestamp.nsec);
    try event_store.put(overwrite_key, value2);

    if (event_store.get(overwrite_key)) |found_value| {
        var final_value = found_value;
        defer final_value.deinit(allocator);
        try std.testing.expectEqualStrings("test.second", final_value.event_type);
        try std.testing.expect(final_value.sequence == 6201);
    } else {
        return error.OverwriteValueNotFound;
    }
}
