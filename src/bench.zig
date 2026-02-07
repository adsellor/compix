const std = @import("std");
const store = @import("core/store.zig");
const radix = @import("core/radix.zig");
const wal = @import("core/wal.zig");
const event = @import("core/event.zig");
const protocol = @import("core/protocol.zig");
const transport = @import("core/transport.zig");
const sstable = @import("core/sstable.zig");
const lsm_mod = @import("core/lsm.zig");
const memtable_mod = @import("core/memtable.zig");
const time_mod = @import("core/time.zig");

const Io = std.Io;
const EventValue = event.EventValue;
const EventStore = store.EventStore;
const RadixTree = radix.RadixTree;
const WriteAheadLog = wal.WriteAheadLog;
const LSMEngine = lsm_mod.LSMEngine;
const Memtable = memtable_mod.Memtable;
const ArrayList = std.ArrayList;
const KeyValuePair = event.KeyValuePair;
const Message = protocol.Message;

fn timestamp_ns() i64 {
    return time_mod.Instant.now(Io.Threaded.global_single_threaded.io()).timestamp.nsec;
}

fn elapsed_us(start: time_mod.Instant, end: time_mod.Instant) u64 {
    const ns = end.since(start);
    return @intCast(ns / std.time.ns_per_us);
}

const BenchLog = struct {
    buf: ArrayList(u8),

    fn init() BenchLog {
        return .{ .buf = ArrayList(u8){} };
    }

    fn appendFmt(self: *BenchLog, allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) void {
        const s = std.fmt.allocPrint(allocator, fmt, args) catch return;
        defer allocator.free(s);
        self.buf.appendSlice(allocator, s) catch {};
    }

    fn section(self: *BenchLog, allocator: std.mem.Allocator, name: []const u8) void {
        std.debug.print("\n[{s}]\n", .{name});
        self.appendFmt(allocator, "\n### {s}\n\n| Benchmark | Ops | Time (us) | Ops/s |\n|:----------|----:|----------:|------:|\n", .{name});
    }

    fn result(self: *BenchLog, allocator: std.mem.Allocator, label: []const u8, count: usize, us: u64) void {
        const ops_per_sec: u64 = if (us > 0) @intCast((@as(u128, count) * 1_000_000) / us) else 0;
        std.debug.print("  {s:<40} {d:>8} ops in {d:>8} us  ({d:>10} ops/s)\n", .{ label, count, us, ops_per_sec });
        self.appendFmt(allocator, "| {s} | {d} | {d} | {d} |\n", .{ label, count, us, ops_per_sec });
    }

    fn note(self: *BenchLog, allocator: std.mem.Allocator, comptime fmt: []const u8, args: anytype) void {
        std.debug.print(fmt, args);
        self.appendFmt(allocator, fmt, args);
    }

    fn write_file(self: *BenchLog, allocator: std.mem.Allocator) !void {
        const io = Io.Threaded.global_single_threaded.io();
        const dir = Io.Dir.cwd();

        dir.createDir(io, "bench", .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const instant = time_mod.Instant.now(io);
        const epoch_secs: u64 = @intCast(instant.timestamp.sec);
        const es = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
        const day = es.getEpochDay().calculateYearDay();
        const md = day.calculateMonthDay();
        const ds = es.getDaySeconds();

        const date_str = try std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
            day.year,
            @intFromEnum(md.month),
            md.day_index + 1,
            ds.getHoursIntoDay(),
            ds.getMinutesIntoHour(),
            ds.getSecondsIntoMinute(),
        });
        defer allocator.free(date_str);

        const file_name = try std.fmt.allocPrint(allocator, "bench/results-{d:0>4}{d:0>2}{d:0>2}-{d:0>2}{d:0>2}{d:0>2}.md", .{
            day.year,
            @intFromEnum(md.month),
            md.day_index + 1,
            ds.getHoursIntoDay(),
            ds.getMinutesIntoHour(),
            ds.getSecondsIntoMinute(),
        });
        defer allocator.free(file_name);

        const header = try std.fmt.allocPrint(allocator, "# Bench Results — {s} UTC\n", .{date_str});
        defer allocator.free(header);

        const file = try dir.createFile(io, file_name, .{ .truncate = true, .read = false });
        defer file.close(io);

        try file.writePositionalAll(io, header, 0);
        try file.writePositionalAll(io, self.buf.items, header.len);

        std.debug.print("\nResults written to {s}\n", .{file_name});
    }

    fn deinit(self: *BenchLog, allocator: std.mem.Allocator) void {
        self.buf.deinit(allocator);
    }
};

fn bench_wal_append(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const path = "data/bench_wal_append.wal";
    const sio = Io.Threaded.global_single_threaded.io();
    Io.Dir.cwd().deleteFile(sio, path) catch {};
    Io.Dir.cwd().deleteFile(sio, "data/bench_wal_append.crumb") catch {};
    defer {
        Io.Dir.cwd().deleteFile(sio, path) catch {};
        Io.Dir.cwd().deleteFile(sio, "data/bench_wal_append.crumb") catch {};
    }

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var wal_log = try WriteAheadLog.init(allocator, path, io);
    defer wal_log.deinit();

    const count: usize = 5_000;
    const start = time_mod.Instant.now(io);

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
    const end = time_mod.Instant.now(io);
    log.result(allocator, "wal.append_event (5k writes + async sync)", count, elapsed_us(start, end));
}

fn bench_wal_replay(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const path = "data/bench_wal_replay.wal";
    const sio = Io.Threaded.global_single_threaded.io();
    Io.Dir.cwd().deleteFile(sio, path) catch {};
    Io.Dir.cwd().deleteFile(sio, "data/bench_wal_replay.crumb") catch {};
    defer {
        Io.Dir.cwd().deleteFile(sio, path) catch {};
        Io.Dir.cwd().deleteFile(sio, "data/bench_wal_replay.crumb") catch {};
    }

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
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

    const start = time_mod.Instant.now(io);
    _ = try wal_log.replay(&tree);
    const end = time_mod.Instant.now(io);

    log.result(allocator, "wal.replay (10k entries)", 10_000, elapsed_us(start, end));
}

fn bench_radix_insert(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
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

    const start = time_mod.Instant.now(io);
    for (0..count) |i| {
        try tree.insert(keys[i], vals[i]);
    }
    const end = time_mod.Instant.now(io);

    std.debug.assert(tree.size == count);
    log.result(allocator, "radix.insert (50k unique keys)", count, elapsed_us(start, end));
}

fn bench_radix_overwrite(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
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

    const start = time_mod.Instant.now(io);
    for (0..count) |i| {
        const val = try EventValue.init(allocator, i, "bench-service", "bench.overwrite", "{}", 0);
        try tree.insert(keys[i % unique_keys], val);
    }
    const end = time_mod.Instant.now(io);

    std.debug.assert(tree.size == unique_keys);
    log.result(allocator, "radix.insert (1k keys x10 overwrites)", count, elapsed_us(start, end));
}

fn bench_radix_get(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
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

    const start = time_mod.Instant.now(io);
    for (0..lookups) |i| {
        if (tree.get(keys[i % key_count])) |_| {
            found += 1;
        }
    }
    const end = time_mod.Instant.now(io);

    std.debug.assert(found == lookups);
    log.result(allocator, "radix.get (100k lookups, 10k keys)", lookups, elapsed_us(start, end));
}

fn bench_radix_get_miss(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
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

    const start = time_mod.Instant.now(io);
    for (0..lookups) |i| {
        if (tree.get(miss_keys[i % miss_key_count]) == null) {
            misses += 1;
        }
    }
    const end = time_mod.Instant.now(io);

    std.debug.assert(misses == lookups);
    log.result(allocator, "radix.get miss (100k misses)", lookups, elapsed_us(start, end));
}

fn bench_scan_prefix(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
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

    const start = time_mod.Instant.now(io);
    for (0..scans) |i| {
        const prefix = services[i % services.len];
        var results = ArrayList(KeyValuePair){};
        try tree.scan_prefix(prefix, &results);
        total_results += results.items.len;
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    const end = time_mod.Instant.now(io);

    log.result(allocator, "radix.scan_prefix (200 scans over 10k)", scans, elapsed_us(start, end));
    log.note(allocator, "    total matched results across scans: {}\n", .{total_results});
}

fn bench_scan_prefix_selectivity(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    for (0..10_000) |i| {
        const key = try std.fmt.allocPrint(allocator, "sel:svc:r{x:0>8}:data", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i, "svc", "e", "{}", 0);
        try tree.insert(key, val);
    }

    {
        const start = time_mod.Instant.now(io);
        for (0..50) |_| {
            var results = ArrayList(KeyValuePair){};
            try tree.scan_prefix("sel:", &results);
            for (results.items) |*item| item.deinit(allocator);
            results.deinit(allocator);
        }
        const end = time_mod.Instant.now(io);
        log.result(allocator, "scan_prefix wide  (50x, ~10k hits each)", 50, elapsed_us(start, end));
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

        const start = time_mod.Instant.now(io);
        for (0..narrow_count) |i| {
            var results = ArrayList(KeyValuePair){};
            try tree.scan_prefix(prefixes[i], &results);
            for (results.items) |*item| item.deinit(allocator);
            results.deinit(allocator);
        }
        const end = time_mod.Instant.now(io);
        log.result(allocator, "scan_prefix narrow (10k x ~1 hit each)", narrow_count, elapsed_us(start, end));
    }
}

fn cleanupBenchLSM(allocator: std.mem.Allocator, io: Io, base: []const u8) void {
    const wal_path = std.fmt.allocPrint(allocator, "{s}.wal", .{base}) catch return;
    defer allocator.free(wal_path);
    const crumb = std.fmt.allocPrint(allocator, "{s}.crumb", .{base}) catch return;
    defer allocator.free(crumb);
    const manifest = std.fmt.allocPrint(allocator, "{s}.manifest", .{base}) catch return;
    defer allocator.free(manifest);

    IoDir.cwd().deleteFile(io, wal_path) catch {};
    IoDir.cwd().deleteFile(io, crumb) catch {};
    IoDir.cwd().deleteFile(io, manifest) catch {};

    for (0..20) |i| {
        for (0..3) |lvl| {
            const p = std.fmt.allocPrint(allocator, "{s}_L{d}_{d:0>8}.sst", .{ base, lvl, i + 1 }) catch continue;
            defer allocator.free(p);
            IoDir.cwd().deleteFile(io, p) catch {};
        }
    }
}

const IoDir = Io.Dir;

fn bench_store_stress(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const wal_path = "data/bench_stress.wal";
    const sio = Io.Threaded.global_single_threaded.io();
    cleanupBenchLSM(allocator, sio, "data/bench_stress");
    defer cleanupBenchLSM(allocator, sio, "data/bench_stress");

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();

    const total_operations: usize = 10_000;
    const services = [_][]const u8{ "auth", "payment", "inventory", "shipping", "analytics" };
    const event_type_names = [_][]const u8{ "created", "updated", "deleted", "processed", "failed" };

    const write_start = time_mod.Instant.now(io);
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
    const write_end = time_mod.Instant.now(io);
    log.result(allocator, "store.put durable (10k synced writes)", total_operations, elapsed_us(write_start, write_end));

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
    const read_start = time_mod.Instant.now(io);
    for (0..read_count) |i| {
        if (try event_store.get(read_keys[i])) |v| {
            var val = v;
            val.deinit(allocator);
            found += 1;
        }
    }
    const read_end = time_mod.Instant.now(io);
    log.result(allocator, "store.get (1k point reads)", read_count, elapsed_us(read_start, read_end));
    std.debug.assert(found == read_count);

    const scan_start = time_mod.Instant.now(io);
    var total_scan_results: usize = 0;
    for (services) |svc| {
        var results = try event_store.scan_range(svc);
        total_scan_results += results.items.len;
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    const scan_end = time_mod.Instant.now(io);
    log.result(allocator, "store.scan_range (5 prefix scans)", services.len, elapsed_us(scan_start, scan_end));
    log.note(allocator, "\n    total scan results: {}\n", .{total_scan_results});
    log.note(allocator, "    memtable entries: {}\n", .{event_store.lsm.memtableCount()});
}

fn bench_protocol_encode_decode(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
    {
        const count: usize = 50_000;
        const msg = Message{ .handshake = .{
            .service_name = "order-service",
            .instance_id = "node-1",
        } };

        const start = time_mod.Instant.now(io);
        for (0..count) |_| {
            const frame = try protocol.encode(allocator, msg);
            var decoded = try protocol.decode(allocator, frame);
            protocol.deinit(&decoded, allocator);
            allocator.free(frame);
        }
        const end = time_mod.Instant.now(io);
        log.result(allocator, "protocol handshake encode+decode (50k)", count, elapsed_us(start, end));
    }

    {
        const count: usize = 50_000;
        const msg = Message{ .pull_request = .{
            .event_type = "order.created",
            .from_sequence = 42,
            .max_count = 100,
        } };

        const start = time_mod.Instant.now(io);
        for (0..count) |_| {
            const frame = try protocol.encode(allocator, msg);
            var decoded = try protocol.decode(allocator, frame);
            protocol.deinit(&decoded, allocator);
            allocator.free(frame);
        }
        const end = time_mod.Instant.now(io);
        log.result(allocator, "protocol pull_request encode+decode (50k)", count, elapsed_us(start, end));
    }

    {
        const count: usize = 10_000;
        var entries: [10]protocol.PullResponse.EventEntry = undefined;
        for (&entries, 0..) |*e, i| {
            e.* = .{
                .key = try std.fmt.allocPrint(allocator, "evt:svc:type:{x:0>16}", .{i + 1}),
                .value = try std.fmt.allocPrint(allocator, "{}|100|2|svc|type|{{}}", .{i + 1}),
            };
        }
        defer for (&entries) |e| {
            allocator.free(e.key);
            allocator.free(e.value);
        };

        const msg = Message{ .pull_response = .{ .events = &entries } };

        const start = time_mod.Instant.now(io);
        for (0..count) |_| {
            const frame = try protocol.encode(allocator, msg);
            var decoded = try protocol.decode(allocator, frame);
            protocol.deinit(&decoded, allocator);
            allocator.free(frame);
        }
        const end = time_mod.Instant.now(io);
        log.result(allocator, "protocol pull_response(10 events) enc+dec (10k)", count, elapsed_us(start, end));
    }

    {
        const count: usize = 1_000;
        const entries_list = try allocator.alloc(protocol.PullResponse.EventEntry, 100);
        defer allocator.free(entries_list);

        for (entries_list, 0..) |*e, i| {
            e.* = .{
                .key = try std.fmt.allocPrint(allocator, "evt:svc:type:{x:0>16}", .{i + 1}),
                .value = try std.fmt.allocPrint(allocator, "{}|100|24|svc|type|{{\"data\":\"payload-{}\"}}", .{ i + 1, i + 1 }),
            };
        }
        defer for (entries_list) |e| {
            allocator.free(e.key);
            allocator.free(e.value);
        };

        const msg = Message{ .pull_response = .{ .events = entries_list } };

        const start = time_mod.Instant.now(io);
        for (0..count) |_| {
            const frame = try protocol.encode(allocator, msg);
            var decoded = try protocol.decode(allocator, frame);
            protocol.deinit(&decoded, allocator);
            allocator.free(frame);
        }
        const end = time_mod.Instant.now(io);
        log.result(allocator, "protocol pull_response(100 events) enc+dec (1k)", count, elapsed_us(start, end));
    }
}

fn bench_transport_handshake(allocator: std.mem.Allocator, log: *BenchLog) !void {
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try transport.Listener.init(allocator, "127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.getPort();

    const count: usize = 500;

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *transport.Listener, n: usize) void {
            for (0..n) |_| {
                const conn = l.accept() catch return;
                defer conn.destroy();
                var msg = conn.recvMessage() catch return;
                conn.sendMessage(msg) catch {};
                protocol.deinit(&msg, l.allocator);
            }
        }
    }.run, .{ &listener, count });

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const start = time_mod.Instant.now(io);
    for (0..count) |_| {
        const conn = try transport.dial(allocator, addr_str, io);
        try conn.sendMessage(.{ .handshake = .{
            .service_name = "bench-service",
            .instance_id = "bench-1",
        } });
        var resp = try conn.recvMessage();
        protocol.deinit(&resp, allocator);
        conn.destroy();
    }
    const end = time_mod.Instant.now(io);

    server_thread.join();
    log.result(allocator, "transport dial+handshake round-trip (500)", count, elapsed_us(start, end));
}

fn bench_transport_pull_throughput(allocator: std.mem.Allocator, log: *BenchLog) !void {
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try transport.Listener.init(allocator, "127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.getPort();

    const rounds: usize = 500;
    const events_per_response: usize = 20;

    var entries: [events_per_response]protocol.PullResponse.EventEntry = undefined;
    for (&entries, 0..) |*e, i| {
        e.* = .{
            .key = try std.fmt.allocPrint(allocator, "evt:order-svc:order.created:{x:0>16}", .{i + 1}),
            .value = try std.fmt.allocPrint(allocator, "{}|100|2|order-svc|order.created|{{}}", .{i + 1}),
        };
    }
    defer for (&entries) |e| {
        allocator.free(e.key);
        allocator.free(e.value);
    };

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *transport.Listener, n: usize, evts: []const protocol.PullResponse.EventEntry) void {
            const conn = l.accept() catch return;
            defer conn.destroy();

            // Receive handshake
            var hs = conn.recvMessage() catch return;
            protocol.deinit(&hs, l.allocator);

            for (0..n) |_| {
                var req = conn.recvMessage() catch return;
                protocol.deinit(&req, l.allocator);
                conn.sendMessage(.{ .pull_response = .{ .events = evts } }) catch return;
            }
        }
    }.run, .{ &listener, rounds, &entries });

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const conn = try transport.dial(allocator, addr_str, io);
    defer conn.destroy();

    try conn.sendMessage(.{ .handshake = .{
        .service_name = "bench-client",
        .instance_id = "bc-1",
    } });

    var total_events: usize = 0;
    const start = time_mod.Instant.now(io);
    for (0..rounds) |_| {
        try conn.sendMessage(.{ .pull_request = .{
            .event_type = "order.created",
            .from_sequence = 0,
            .max_count = 0,
        } });

        var resp = try conn.recvMessage();
        total_events += resp.pull_response.events.len;
        protocol.deinit(&resp, allocator);
    }
    const end = time_mod.Instant.now(io);

    server_thread.join();
    log.result(allocator, "transport pull req+resp (500 rounds, 20 events each)", rounds, elapsed_us(start, end));
    log.note(allocator, "    total events transferred: {}\n", .{total_events});
}

fn bench_transport_message_burst(allocator: std.mem.Allocator, log: *BenchLog) !void {
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try transport.Listener.init(allocator, "127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.getPort();

    const count: usize = 5_000;

    // Server thread: accept one connection, echo all messages
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *transport.Listener, n: usize) void {
            const conn = l.accept() catch return;
            defer conn.destroy();
            for (0..n) |_| {
                var msg = conn.recvMessage() catch return;
                conn.sendMessage(msg) catch {};
                protocol.deinit(&msg, l.allocator);
            }
        }
    }.run, .{ &listener, count });

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const conn = try transport.dial(allocator, addr_str, io);
    defer conn.destroy();

    const start = time_mod.Instant.now(io);
    for (0..count) |i| {
        try conn.sendMessage(.{ .pull_request = .{
            .event_type = "order.created",
            .from_sequence = i,
            .max_count = 100,
        } });
        var resp = try conn.recvMessage();
        protocol.deinit(&resp, allocator);
    }
    const end = time_mod.Instant.now(io);

    server_thread.join();
    log.result(allocator, "transport message echo on single conn (5k)", count, elapsed_us(start, end));
}

fn bench_memtable_insert(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const count: usize = 50_000;
    const start = time_mod.Instant.now(io);

    for (0..count) |i| {
        const key = try std.fmt.allocPrint(allocator, "mt:k{d:0>8}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i + 1, "bench-service", "bench.insert", "{}", 0);
        try mt.insert(key, val);
    }
    const end = time_mod.Instant.now(io);

    log.result(allocator, "memtable.insert (50k sorted)", count, elapsed_us(start, end));
    log.note(allocator, "    size_bytes: {}\n", .{mt.size_bytes});
}

fn bench_memtable_get(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const io = Io.Threaded.global_single_threaded.io();
    var mt = Memtable.init(allocator);
    defer mt.deinit();

    const key_count: usize = 10_000;
    var keys = try allocator.alloc([]u8, key_count);
    defer {
        for (keys) |k| allocator.free(k);
        allocator.free(keys);
    }

    for (0..key_count) |i| {
        keys[i] = try std.fmt.allocPrint(allocator, "mt:get:k{d:0>8}", .{i});
        const val = try EventValue.init(allocator, i + 1, "svc", "evt", "{}", 0);
        try mt.insert(keys[i], val);
    }

    const lookups: usize = 100_000;
    var found: usize = 0;

    const start = time_mod.Instant.now(io);
    for (0..lookups) |i| {
        if (try mt.get(allocator, keys[i % key_count])) |v| {
            var val = v;
            val.deinit(allocator);
            found += 1;
        }
    }
    const end = time_mod.Instant.now(io);

    std.debug.assert(found == lookups);
    log.result(allocator, "memtable.get (100k lookups, 10k keys)", lookups, elapsed_us(start, end));
}

fn bench_sstable_write(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const sio = Io.Threaded.global_single_threaded.io();
    const path = "data/bench_sst_write.sst";
    IoDir.cwd().deleteFile(sio, path) catch {};
    defer IoDir.cwd().deleteFile(sio, path) catch {};

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const count: usize = 10_000;
    var entries = try allocator.alloc(sstable.RawEntry, count);
    defer {
        for (entries) |e| {
            allocator.free(e.key);
            allocator.free(e.value);
        }
        allocator.free(entries);
    }

    for (0..count) |i| {
        entries[i] = .{
            .key = try std.fmt.allocPrint(allocator, "sst:k{d:0>8}", .{i}),
            .value = try std.fmt.allocPrint(allocator, "{}|0|2|bench-svc|bench.evt|{{}}", .{i + 1}),
        };
    }

    const start = time_mod.Instant.now(io);
    _ = try sstable.writeSSTable(allocator, path, io, entries);
    const end = time_mod.Instant.now(io);

    log.result(allocator, "sstable.write (10k entries)", count, elapsed_us(start, end));
}

fn bench_sstable_read(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const sio = Io.Threaded.global_single_threaded.io();
    const path = "data/bench_sst_read.sst";
    IoDir.cwd().deleteFile(sio, path) catch {};
    defer IoDir.cwd().deleteFile(sio, path) catch {};

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const count: usize = 10_000;
    var entries = try allocator.alloc(sstable.RawEntry, count);
    defer {
        for (entries) |e| {
            allocator.free(e.key);
            allocator.free(e.value);
        }
        allocator.free(entries);
    }

    var keys = try allocator.alloc([]u8, count);
    defer allocator.free(keys);

    for (0..count) |i| {
        const k = try std.fmt.allocPrint(allocator, "sst:k{d:0>8}", .{i});
        entries[i] = .{
            .key = k,
            .value = try std.fmt.allocPrint(allocator, "{}|0|2|bench-svc|bench.evt|{{}}", .{i + 1}),
        };
        keys[i] = k;
    }

    _ = try sstable.writeSSTable(allocator, path, io, entries);

    var reader = try sstable.SSTableReader.open(allocator, path, io);
    defer reader.deinit();

    const lookups: usize = 10_000;
    var found: usize = 0;

    const start = time_mod.Instant.now(io);
    for (0..lookups) |i| {
        if (try reader.get(allocator, keys[i % count])) |v| {
            var val = v;
            val.deinit(allocator);
            found += 1;
        }
    }
    const end = time_mod.Instant.now(io);

    std.debug.assert(found == lookups);
    log.result(allocator, "sstable.get (10k point reads)", lookups, elapsed_us(start, end));
}

fn bench_lsm_put_get(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const sio = Io.Threaded.global_single_threaded.io();
    cleanupBenchLSM(allocator, sio, "data/bench_lsm_pg");
    defer cleanupBenchLSM(allocator, sio, "data/bench_lsm_pg");

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var lsm = try LSMEngine.init(allocator, "data/bench_lsm_pg.wal", io);
    defer lsm.deinit();

    const write_count: usize = 10_000;
    const write_start = time_mod.Instant.now(io);
    for (0..write_count) |i| {
        const key = try std.fmt.allocPrint(allocator, "lsm:k{d:0>8}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i + 1, "bench-svc", "bench.put", "{}", 0);
        try lsm.put(key, val);
    }
    const write_end = time_mod.Instant.now(io);
    log.result(allocator, "lsm.put (10k writes)", write_count, elapsed_us(write_start, write_end));

    const read_count: usize = 5_000;
    var found: usize = 0;
    const read_start = time_mod.Instant.now(io);
    for (0..read_count) |i| {
        const key = try std.fmt.allocPrint(allocator, "lsm:k{d:0>8}", .{i * 2});
        defer allocator.free(key);
        if (try lsm.get(allocator, key)) |v| {
            var val = v;
            val.deinit(allocator);
            found += 1;
        }
    }
    const read_end = time_mod.Instant.now(io);

    std.debug.assert(found == read_count);
    log.result(allocator, "lsm.get (5k reads, in-memtable)", read_count, elapsed_us(read_start, read_end));
}

fn bench_lsm_flush(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const sio = Io.Threaded.global_single_threaded.io();
    cleanupBenchLSM(allocator, sio, "data/bench_lsm_flush");
    defer cleanupBenchLSM(allocator, sio, "data/bench_lsm_flush");

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var lsm = try LSMEngine.init(allocator, "data/bench_lsm_flush.wal", io);
    defer lsm.deinit();

    // Write entries
    const count: usize = 10_000;
    for (0..count) |i| {
        const key = try std.fmt.allocPrint(allocator, "flush:k{d:0>8}", .{i});
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i + 1, "svc", "evt", "{\"data\":\"some payload\"}", 0);
        try lsm.put(key, val);
    }

    const start = time_mod.Instant.now(io);
    try lsm.flush();
    const end = time_mod.Instant.now(io);

    log.result(allocator, "lsm.flush (10k entries -> SSTable)", 1, elapsed_us(start, end));
    log.note(allocator, "    SSTable count after flush: {}\n", .{lsm.sstableCount()});

    // Read after flush (from SSTable)
    var found: usize = 0;
    const read_start = time_mod.Instant.now(io);
    for (0..1_000) |i| {
        const key = try std.fmt.allocPrint(allocator, "flush:k{d:0>8}", .{i * 10});
        defer allocator.free(key);
        if (try lsm.get(allocator, key)) |v| {
            var val = v;
            val.deinit(allocator);
            found += 1;
        }
    }
    const read_end = time_mod.Instant.now(io);

    std.debug.assert(found == 1_000);
    log.result(allocator, "lsm.get after flush (1k reads from SSTable)", 1_000, elapsed_us(read_start, read_end));
}

fn bench_lsm_scan(allocator: std.mem.Allocator, log: *BenchLog) !void {
    const sio = Io.Threaded.global_single_threaded.io();
    cleanupBenchLSM(allocator, sio, "data/bench_lsm_scan");
    defer cleanupBenchLSM(allocator, sio, "data/bench_lsm_scan");

    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var lsm = try LSMEngine.init(allocator, "data/bench_lsm_scan.wal", io);
    defer lsm.deinit();

    const services = [_][]const u8{ "auth", "payment", "inventory", "shipping", "analytics" };

    for (0..5_000) |i| {
        const svc = services[i % services.len];
        const key = try std.fmt.allocPrint(allocator, "{s}:r{d:0>8}:evt", .{ svc, i });
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i + 1, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    // Flush half to SSTable, keep rest in memtable
    try lsm.flush();

    for (5_000..10_000) |i| {
        const svc = services[i % services.len];
        const key = try std.fmt.allocPrint(allocator, "{s}:r{d:0>8}:evt", .{ svc, i });
        defer allocator.free(key);
        const val = try EventValue.init(allocator, i + 1, "svc", "evt", "{}", 0);
        try lsm.put(key, val);
    }

    const scans: usize = 50;
    var total_results: usize = 0;

    const start = time_mod.Instant.now(io);
    for (0..scans) |i| {
        const prefix = services[i % services.len];
        var results = ArrayList(KeyValuePair){};
        try lsm.scan_prefix(prefix, &results);
        total_results += results.items.len;
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    const end = time_mod.Instant.now(io);

    log.result(allocator, "lsm.scan_prefix (50 scans, cross memtable+SST)", scans, elapsed_us(start, end));
    log.note(allocator, "    total matched results: {}\n", .{total_results});
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var log = BenchLog.init();
    defer log.deinit(allocator);

    log.section(allocator, "WAL");
    try bench_wal_append(allocator, &log);
    try bench_wal_replay(allocator, &log);

    log.section(allocator, "Radix insert");
    try bench_radix_insert(allocator, &log);
    try bench_radix_overwrite(allocator, &log);

    log.section(allocator, "Radix get");
    try bench_radix_get(allocator, &log);
    try bench_radix_get_miss(allocator, &log);

    log.section(allocator, "Radix scan_prefix");
    try bench_scan_prefix(allocator, &log);
    try bench_scan_prefix_selectivity(allocator, &log);

    log.section(allocator, "Memtable");
    try bench_memtable_insert(allocator, &log);
    try bench_memtable_get(allocator, &log);

    log.section(allocator, "SSTable");
    try bench_sstable_write(allocator, &log);
    try bench_sstable_read(allocator, &log);

    log.section(allocator, "LSM Engine");
    try bench_lsm_put_get(allocator, &log);
    try bench_lsm_flush(allocator, &log);
    try bench_lsm_scan(allocator, &log);

    log.section(allocator, "EventStore end-to-end");
    try bench_store_stress(allocator, &log);

    log.section(allocator, "Protocol encode/decode");
    try bench_protocol_encode_decode(allocator, &log);

    log.section(allocator, "Transport");
    try bench_transport_handshake(allocator, &log);
    try bench_transport_pull_throughput(allocator, &log);
    try bench_transport_message_burst(allocator, &log);

    log.write_file(allocator) catch |err| {
        std.debug.print("\nFailed to write bench/results.md: {}\n", .{err});
    };
}
