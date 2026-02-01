const std = @import("std");
const store = @import("core/store.zig");
const radix = @import("core/radix.zig");
const wal = @import("core/wal.zig");
const event = @import("core/event.zig");

const Io = std.Io;
const EventValue = event.EventValue;
const EventStore = store.EventStore;
const RadixTree = radix.RadixTree;
const WriteAheadLog = wal.WriteAheadLog;
const ArrayList = std.ArrayList;
const KeyValuePair = event.KeyValuePair;

fn timestamp_ns() i64 {
    return (std.time.Instant.now() catch return 0).timestamp.nsec;
}

fn elapsed_us(start: std.time.Instant, end: std.time.Instant) u64 {
    const ns = end.since(start);
    return @intCast(ns / std.time.ns_per_us);
}

fn print_result(label: []const u8, count: usize, us: u64) void {
    const ops_per_sec: u64 = if (us > 0) @intCast((@as(u128, count) * 1_000_000) / us) else 0;
    std.debug.print("  {s:<40} {d:>8} ops in {d:>8} us  ({d:>10} ops/s)\n", .{ label, count, us, ops_per_sec });
}
fn bench_wal_append(allocator: std.mem.Allocator) !void {
    const path = "data/bench_wal_append.wal";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("data/bench_wal_append.crumb") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("data/bench_wal_append.crumb") catch {};
    }

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var wal_log = try WriteAheadLog.init(allocator, path, io);
    defer wal_log.deinit();

    const count: usize = 5_000;
    const start = try std.time.Instant.now();

    for (0..count) |i| {
        const key = try std.fmt.allocPrint(allocator, "bench:wal:{}", .{i});
        defer allocator.free(key);
        const val = EventValue{
            .sequence = i + 1,
            .event_type = "bench.append",
            .payload = "{\"data\":\"some payload for benchmarking\"}",
            .timestamp = 0,
            .origin_service = "bench-service",
        };
        try wal_log.append_event(key, val);
    }

    try wal_log.flush();
    const end = try std.time.Instant.now();
    print_result("wal.append_event (5k writes + async sync)", count, elapsed_us(start, end));
}

fn bench_wal_replay(allocator: std.mem.Allocator) !void {
    const path = "data/bench_wal_replay.wal";
    std.fs.cwd().deleteFile(path) catch {};
    std.fs.cwd().deleteFile("data/bench_wal_replay.crumb") catch {};
    defer {
        std.fs.cwd().deleteFile(path) catch {};
        std.fs.cwd().deleteFile("data/bench_wal_replay.crumb") catch {};
    }

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    {
        var wal_log = try WriteAheadLog.init(allocator, path, io);
        defer wal_log.deinit();

        for (0..10_000) |i| {
            const key = try std.fmt.allocPrint(allocator, "bench:replay:{}", .{i});
            defer allocator.free(key);
            const val = EventValue{
                .sequence = i + 1,
                .event_type = "bench.replay",
                .payload = "{\"n\":1}",
                .timestamp = 0,
                .origin_service = "bench-service",
            };
            try wal_log.append_event(key, val);
        }
    }

    var wal_log = try WriteAheadLog.init(allocator, path, io);
    defer wal_log.deinit();

    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const start = try std.time.Instant.now();
    _ = try wal_log.replay(&tree);
    const end = try std.time.Instant.now();

    print_result("wal.replay (10k entries)", 10_000, elapsed_us(start, end));
}

fn bench_radix_insert(allocator: std.mem.Allocator) !void {
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const count: usize = 50_000;
    const services = [_][]const u8{ "auth", "payment", "inventory", "shipping", "analytics" };
    const event_types = [_][]const u8{ "created", "updated", "deleted", "processed", "failed" };

    var keys = try allocator.alloc([]u8, count);
    defer {
        for (keys) |k| allocator.free(k);
        allocator.free(keys);
    }
    var vals = try allocator.alloc(EventValue, count);
    defer {
        allocator.free(vals);
    }

    for (0..count) |i| {
        keys[i] = try std.fmt.allocPrint(allocator, "{s}:r{}:op", .{ services[i % services.len], i });
        const et = try allocator.dupe(u8, event_types[i % event_types.len]);
        const pl = try allocator.dupe(u8, "{\"i\":1}");
        const os = try allocator.dupe(u8, "bench-service");
        vals[i] = EventValue{
            .sequence = i,
            .event_type = et,
            .payload = pl,
            .timestamp = 0,
            .origin_service = os,
        };
    }

    const start = try std.time.Instant.now();
    for (0..count) |i| {
        try tree.insert(keys[i], vals[i]);
    }
    const end = try std.time.Instant.now();

    std.debug.assert(tree.size == count);
    print_result("radix.insert (50k unique keys)", count, elapsed_us(start, end));
}

fn bench_radix_overwrite(allocator: std.mem.Allocator) !void {
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const unique_keys: usize = 1_000;
    const overwrites_per_key: usize = 10;
    const count = unique_keys * overwrites_per_key;

    var keys = try allocator.alloc([]u8, unique_keys);
    defer {
        for (keys) |k| allocator.free(k);
        allocator.free(keys);
    }
    for (0..unique_keys) |i| {
        keys[i] = try std.fmt.allocPrint(allocator, "ow:k{}", .{i});
    }

    const start = try std.time.Instant.now();
    for (0..count) |i| {
        const val = try EventValue.init(allocator, i, "bench-service", "bench.overwrite", "{}", 0);
        try tree.insert(keys[i % unique_keys], val);
    }
    const end = try std.time.Instant.now();

    std.debug.assert(tree.size == unique_keys);
    print_result("radix.insert (1k keys x10 overwrites)", count, elapsed_us(start, end));
}

fn bench_radix_get(allocator: std.mem.Allocator) !void {
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const key_count: usize = 10_000;
    var keys = try allocator.alloc([]u8, key_count);
    defer {
        for (keys) |k| allocator.free(k);
        allocator.free(keys);
    }

    for (0..key_count) |i| {
        keys[i] = try std.fmt.allocPrint(allocator, "get:svc:r{}:data", .{i});
        const val = try EventValue.init(allocator, i, "bench-service", "bench.get", "{\"x\":1}", 0);
        try tree.insert(keys[i], val);
    }

    const lookups: usize = 100_000;
    var found: usize = 0;

    const start = try std.time.Instant.now();
    for (0..lookups) |i| {
        if (tree.get(keys[i % key_count])) |_| {
            found += 1;
        }
    }
    const end = try std.time.Instant.now();

    std.debug.assert(found == lookups);
    print_result("radix.get (100k lookups, 10k keys)", lookups, elapsed_us(start, end));
}

fn bench_radix_get_miss(allocator: std.mem.Allocator) !void {
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    for (0..1_000) |i| {
        const key = try std.fmt.allocPrint(allocator, "miss:k{}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "svc", "e", "{}", 0);
        try tree.insert(key, val);
    }

    const miss_key_count: usize = 10_000;
    var miss_keys = try allocator.alloc([]u8, miss_key_count);
    defer {
        for (miss_keys) |k| allocator.free(k);
        allocator.free(miss_keys);
    }
    for (0..miss_key_count) |i| {
        miss_keys[i] = try std.fmt.allocPrint(allocator, "nonexistent:k{}", .{i});
    }

    const lookups: usize = 100_000;
    var misses: usize = 0;

    const start = try std.time.Instant.now();
    for (0..lookups) |i| {
        if (tree.get(miss_keys[i % miss_key_count]) == null) {
            misses += 1;
        }
    }
    const end = try std.time.Instant.now();

    std.debug.assert(misses == lookups);
    print_result("radix.get miss (100k misses)", lookups, elapsed_us(start, end));
}

fn bench_scan_prefix(allocator: std.mem.Allocator) !void {
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const services = [_][]const u8{ "auth", "payment", "inventory", "shipping", "analytics" };

    for (0..10_000) |i| {
        const svc = services[i % services.len];
        const key = try std.fmt.allocPrint(allocator, "{s}:r{}:evt", .{ svc, i });
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "bench-service", "bench.scan", "{}", 0);
        try tree.insert(key, val);
    }

    const scans: usize = 200;
    var total_results: usize = 0;

    const start = try std.time.Instant.now();
    for (0..scans) |i| {
        const prefix = services[i % services.len];
        var results = ArrayList(KeyValuePair){};
        try tree.scan_prefix(prefix, &results);
        total_results += results.items.len;
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    const end = try std.time.Instant.now();

    print_result("radix.scan_prefix (200 scans over 10k)", scans, elapsed_us(start, end));
    std.debug.print("    total matched results across scans: {}\n", .{total_results});
}

fn bench_scan_prefix_selectivity(allocator: std.mem.Allocator) !void {
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    for (0..10_000) |i| {
        const key = try std.fmt.allocPrint(allocator, "sel:svc:r{x:0>8}:data", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "svc", "e", "{}", 0);
        try tree.insert(key, val);
    }

    {
        const start = try std.time.Instant.now();
        for (0..50) |_| {
            var results = ArrayList(KeyValuePair){};
            try tree.scan_prefix("sel:", &results);
            for (results.items) |*item| item.deinit(allocator);
            results.deinit(allocator);
        }
        const end = try std.time.Instant.now();
        print_result("scan_prefix wide  (50x, ~10k hits each)", 50, elapsed_us(start, end));
    }

    {
        const narrow_count: usize = 10_000;
        var prefixes = try allocator.alloc([]u8, narrow_count);
        defer {
            for (prefixes) |p| allocator.free(p);
            allocator.free(prefixes);
        }
        for (0..narrow_count) |i| {
            prefixes[i] = try std.fmt.allocPrint(allocator, "sel:svc:r{x:0>8}:", .{i});
        }

        const start = try std.time.Instant.now();
        for (0..narrow_count) |i| {
            var results = ArrayList(KeyValuePair){};
            try tree.scan_prefix(prefixes[i], &results);
            for (results.items) |*item| item.deinit(allocator);
            results.deinit(allocator);
        }
        const end = try std.time.Instant.now();
        print_result("scan_prefix narrow (10k x ~1 hit each)", narrow_count, elapsed_us(start, end));
    }
}

fn bench_store_stress(allocator: std.mem.Allocator) !void {
    const wal_path = "data/bench_stress.wal";
    const crumb_path = "data/bench_stress.crumb";
    std.fs.cwd().deleteFile(wal_path) catch {};
    std.fs.cwd().deleteFile(crumb_path) catch {};
    defer {
        std.fs.cwd().deleteFile(wal_path) catch {};
        std.fs.cwd().deleteFile(crumb_path) catch {};
    }

    var threaded = Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, wal_path, io);
    defer event_store.deinit();

    const total_operations: usize = 10_000;
    const services = [_][]const u8{ "auth", "payment", "inventory", "shipping", "analytics" };
    const event_type_names = [_][]const u8{ "created", "updated", "deleted", "processed", "failed" };

    const write_start = try std.time.Instant.now();
    for (0..total_operations) |i| {
        const svc = services[i % services.len];
        const etn = event_type_names[i % event_type_names.len];

        const key = try std.fmt.allocPrint(allocator, "{s}:r{}:operation", .{ svc, i });
        defer allocator.free(key);

        const full_event_type = try std.fmt.allocPrint(allocator, "{s}.{s}", .{ svc, etn });
        defer allocator.free(full_event_type);

        const payload = try std.fmt.allocPrint(
            allocator,
            "{{\"operation_id\":{},\"service\":\"{s}\"}}",
            .{ i, svc },
        );
        defer allocator.free(payload);

        const value = try EventValue.init(allocator, i + 5000, "test-service", full_event_type, payload, timestamp_ns());
        try event_store.put(key, value);
    }
    const write_end = try std.time.Instant.now();
    print_result("store.put (10k WAL+radix writes)", total_operations, elapsed_us(write_start, write_end));

    const read_count: usize = 1_000;
    var read_keys = try allocator.alloc([]u8, read_count);
    defer {
        for (read_keys) |k| allocator.free(k);
        allocator.free(read_keys);
    }
    for (0..read_count) |i| {
        const idx = i * (total_operations / read_count);
        const svc = services[idx % services.len];
        read_keys[i] = try std.fmt.allocPrint(allocator, "{s}:r{}:operation", .{ svc, idx });
    }

    var found: usize = 0;
    const read_start = try std.time.Instant.now();
    for (0..read_count) |i| {
        if (event_store.get(read_keys[i])) |_| found += 1;
    }
    const read_end = try std.time.Instant.now();
    print_result("store.get (1k point reads)", read_count, elapsed_us(read_start, read_end));
    std.debug.assert(found == read_count);

    const scan_start = try std.time.Instant.now();
    var total_scan_results: usize = 0;
    for (services) |svc| {
        var results = try event_store.scan_range(svc);
        total_scan_results += results.items.len;
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    const scan_end = try std.time.Instant.now();
    print_result("store.scan_range (5 prefix scans)", services.len, elapsed_us(scan_start, scan_end));
    std.debug.print("    total scan results: {}\n", .{total_scan_results});

    std.debug.print("\n  Store stats:\n", .{});
    std.debug.print("    memtable entries: {}\n", .{event_store.memtable.size});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("[WAL]\n", .{});
    try bench_wal_append(allocator);
    try bench_wal_replay(allocator);

    std.debug.print("\n[Radix insert]\n", .{});
    try bench_radix_insert(allocator);
    try bench_radix_overwrite(allocator);

    std.debug.print("\n[Radix get]\n", .{});
    try bench_radix_get(allocator);
    try bench_radix_get_miss(allocator);

    std.debug.print("\n[Radix scan_prefix]\n", .{});
    try bench_scan_prefix(allocator);
    try bench_scan_prefix_selectivity(allocator);

    std.debug.print("\n[EventStore end-to-end]\n", .{});
    try bench_store_stress(allocator);
}
