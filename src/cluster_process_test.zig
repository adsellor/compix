//! Multi-process Raft replacement and stopped-node restore drill.

const std = @import("std");
const driver = @import("driver.zig");
const protocol = @import("core/protocol.zig");
const transport = @import("core/transport.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const IoDir = Io.Dir;
const node_count = 3;
const cluster_secret = "0123456789abcdef0123456789abcdef";
const app_secret = "abcdef0123456789abcdef0123456789";
const archive_path = "data/process_cluster_archive";
const config_paths = [node_count][]const u8{
    "data/process_cluster_1.json",
    "data/process_cluster_2.json",
    "data/process_cluster_3.json",
};
const wal_paths = [node_count][]const u8{
    "data/process_cluster_1.wal",
    "data/process_cluster_2.wal",
    "data/process_cluster_3.wal",
};
const state_paths = [node_count][]const u8{
    "data/process_cluster_1.raft",
    "data/process_cluster_2.raft",
    "data/process_cluster_3.raft",
};
const instance_ids = [node_count][]const u8{
    "process-node-1",
    "process-node-2",
    "process-node-3",
};

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const compix_path = args.next() orelse return error.MissingCompixPath;
    if (args.next() != null) return error.TooManyArguments;
    try run(init.gpa, init.io, compix_path);
}

fn run(allocator: Allocator, io: Io, compix_path: []const u8) !void {
    cleanup(io);
    defer cleanup(io);
    var address_storage: [node_count][32]u8 = undefined;
    var addresses: [node_count][]const u8 = undefined;
    try reserve_addresses(io, &address_storage, &addresses);
    for (0..node_count) |index| {
        try write_config(
            allocator,
            io,
            config_paths[index],
            wal_paths[index],
            state_paths[index],
            index,
            &addresses,
        );
    }

    var children: [node_count]?std.process.Child = @splat(null);
    defer for (&children) |*child| kill_child(child, io);
    for (&children, 0..) |*child, index| {
        child.* = try spawn_node(io, compix_path, config_paths[index]);
    }
    const first_leader = try wait_for_leader(io, &addresses, &children);
    try append_commands(io, addresses[first_leader], 1, 6);
    try wait_for_position(io, addresses[first_leader], 5);

    const replacement = (first_leader + 1) % node_count;
    kill_child(&children[replacement], io);
    cleanup_node(io, replacement);
    children[replacement] = try spawn_node(io, compix_path, config_paths[replacement]);
    try wait_for_position(io, addresses[replacement], 5);

    kill_child(&children[first_leader], io);
    const second_leader = try wait_for_leader(io, &addresses, &children);
    if (second_leader == first_leader) return error.LeaderDidNotFailOver;
    try append_commands(io, addresses[second_leader], 6, 7);
    for (0..node_count) |index| {
        if (children[index] != null) try wait_for_position(io, addresses[index], 6);
    }
    for (&children) |*child| kill_child(child, io);

    const target_config = "data/process_cluster_restore.json";
    try write_config(
        allocator,
        io,
        target_config,
        "data/process_cluster_restore.wal",
        "data/process_cluster_restore.raft",
        second_leader,
        &addresses,
    );
    try run_compix(allocator, io, &.{
        compix_path,
        "backup",
        config_paths[second_leader],
        archive_path,
    });
    try run_compix(allocator, io, &.{
        compix_path,
        "restore",
        target_config,
        archive_path,
    });
    try run_compix(allocator, io, &.{ compix_path, "verify", target_config });
    try flip_first_byte(io, "data/process_cluster_restore.raft.snapshot");
    try run_compix_expect_failure(
        allocator,
        io,
        &.{ compix_path, "verify", target_config },
        "SnapshotChecksumMismatch",
    );
}

fn reserve_addresses(
    io: Io,
    storage: *[node_count][32]u8,
    addresses: *[node_count][]const u8,
) !void {
    var listeners: [node_count]transport.Listener = undefined;
    var initialized: u8 = 0;
    defer for (listeners[0..initialized]) |*listener| listener.deinit();
    for (&listeners) |*listener| {
        listener.* = try transport.Listener.init("127.0.0.1:0", io);
        initialized += 1;
    }
    for (&listeners, 0..) |*listener, index| {
        addresses[index] = try std.fmt.bufPrint(&storage[index], "127.0.0.1:{d}", .{
            listener.get_port(),
        });
    }
}

fn write_config(
    allocator: Allocator,
    io: Io,
    path: []const u8,
    wal_path: []const u8,
    state_path: []const u8,
    node_index: usize,
    addresses: *const [node_count][]const u8,
) !void {
    const json = try std.fmt.allocPrint(allocator,
        \\{{"service_name":"process-cluster","instance_id":"{s}",
        \\ "listen_address":"{s}","wal_path":"{s}",
        \\ "client_secret":"{s}","connection_capacity":8,
        \\ "stream_capacity":128,"stream_id_bytes_capacity":65536,
        \\ "credentials":[
        \\  {{"service_name":"process-cluster","secret":"{s}",
        \\   "permissions":["replicate"]}},
        \\  {{"service_name":"process-client","secret":"{s}",
        \\   "permissions":["append","read_stream","read_log","observe"]}}],
        \\ "contracts":[],"cluster":{{"node_id":{d},"state_path":"{s}",
        \\  "heartbeat_interval_ms":20,"election_timeout_min_ms":100,
        \\  "election_timeout_max_ms":180,"rpc_timeout_ms":80,"log_capacity":8,
        \\  "members":[
        \\   {{"node_id":1,"instance_id":"{s}","address":"{s}"}},
        \\   {{"node_id":2,"instance_id":"{s}","address":"{s}"}},
        \\   {{"node_id":3,"instance_id":"{s}","address":"{s}"}}]}}}}
    , .{
        instance_ids[node_index], addresses[node_index], wal_path,
        cluster_secret,           cluster_secret,        app_secret,
        node_index + 1,           state_path,            instance_ids[0],
        addresses[0],             instance_ids[1],       addresses[1],
        instance_ids[2],          addresses[2],
    });
    defer allocator.free(json);
    const file = try IoDir.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    try file.writePositionalAll(io, json, 0);
    try file.sync(io);
}

fn spawn_node(io: Io, compix_path: []const u8, config_path: []const u8) !std.process.Child {
    return try std.process.spawn(io, .{
        .argv = &.{ compix_path, config_path },
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
}

fn kill_child(child: *?std.process.Child, io: Io) void {
    if (child.*) |*process| process.kill(io);
    child.* = null;
}

fn wait_for_leader(
    io: Io,
    addresses: *const [node_count][]const u8,
    children: *const [node_count]?std.process.Child,
) !usize {
    for (0..500) |_| {
        for (0..node_count) |index| {
            if (children[index] == null) continue;
            const health = request_health(io, addresses[index]) catch continue;
            if (health.ready and health.replication_quorum) return index;
        }
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.ProcessClusterElectionTimeout;
}

fn wait_for_position(io: Io, address: []const u8, expected: u64) !void {
    for (0..500) |_| {
        const health = request_health(io, address) catch {
            try io.sleep(.fromMilliseconds(20), .awake);
            continue;
        };
        if (health.committed_log_position == expected) return;
        try io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.ProcessClusterApplyTimeout;
}

fn request_health(io: Io, address: []const u8) !protocol.HealthResponse {
    var client: driver.Client = undefined;
    try driver.Client.connect(&client, .{
        .address = address,
        .service_name = "process-client",
        .instance_id = "process-probe",
        .secret = app_secret,
        .io = io,
    });
    defer client.deinit();
    const reply = try client.health();
    return switch (reply) {
        .response => |response| response,
        .rejected => error.HealthRejected,
    };
}

fn append_commands(
    io: Io,
    address: []const u8,
    first: u64,
    end: u64,
) !void {
    var client: driver.Client = undefined;
    try driver.Client.connect(&client, .{
        .address = address,
        .service_name = "process-client",
        .instance_id = "process-writer",
        .secret = app_secret,
        .io = io,
    });
    defer client.deinit();
    const events = [_]protocol.AppendEvent{.{
        .event_type = "process.event",
        .schema_version = 1,
        .payload = "payload",
    }};
    var command = first;
    while (command < end) : (command += 1) {
        var id_buffer: [64]u8 = undefined;
        const command_id = try std.fmt.bufPrint(&id_buffer, "process-command-{d}", .{command});
        const reply = try client.append(.{
            .stream_id = "process/stream",
            .expected_version = if (command == 1) .empty else .{ .exact = command - 1 },
            .command_id = command_id,
            .events = &events,
        });
        switch (reply) {
            .response => |response| if (response != .committed) return error.AppendNotCommitted,
            .rejected => return error.AppendRejected,
        }
    }
}

fn run_compix(allocator: Allocator, io: Io, argv: []const []const u8) !void {
    const result = try std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(1024 * 1024),
        .stderr_limit = .limited(1024 * 1024),
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    if (!result.term.success()) return error.CompixCommandFailed;
}

fn run_compix_expect_failure(
    allocator: Allocator,
    io: Io,
    argv: []const []const u8,
    expected_error: []const u8,
) !void {
    const result = try std.process.run(allocator, io, .{
        .argv = argv,
        .stdout_limit = .limited(1024 * 1024),
        .stderr_limit = .limited(1024 * 1024),
    });
    defer allocator.free(result.stdout);
    defer allocator.free(result.stderr);
    if (result.term.success()) return error.CompixCommandUnexpectedlySucceeded;
    if (std.mem.indexOf(u8, result.stderr, expected_error) == null)
        return error.CompixCommandReturnedUnexpectedError;
}

fn flip_first_byte(io: Io, path: []const u8) !void {
    const file = try IoDir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    var byte: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &byte, 0) != 1) return error.EmptyTestPayload;
    byte[0] ^= 1;
    try file.writePositionalAll(io, &byte, 0);
    try file.sync(io);
}

fn cleanup(io: Io) void {
    IoDir.cwd().deleteTree(io, archive_path) catch {};
    for (0..node_count) |index| {
        cleanup_node(io, index);
        IoDir.cwd().deleteFile(io, config_paths[index]) catch {};
    }
    inline for (.{
        "data/process_cluster_restore.json",
        "data/process_cluster_restore.wal",
        "data/process_cluster_restore.crumb",
        "data/process_cluster_restore.manifest",
        "data/process_cluster_restore.raft",
        "data/process_cluster_restore.raft.snapshot",
        "data/process_cluster_restore.raft.snapshot.incoming",
        "data/process_cluster_restore.raft.snapshot.quarantine",
        "data/process_cluster_restore.backup-state",
    }) |path| IoDir.cwd().deleteFile(io, path) catch {};
}

fn cleanup_node(io: Io, index: usize) void {
    IoDir.cwd().deleteFile(io, wal_paths[index]) catch {};
    IoDir.cwd().deleteFile(io, state_paths[index]) catch {};
    var path_buffer: [160]u8 = undefined;
    inline for (.{ ".crumb", ".manifest", ".backup-state" }) |suffix| {
        const path = std.fmt.bufPrint(&path_buffer, "data/process_cluster_{d}{s}", .{
            index + 1,
            suffix,
        }) catch unreachable;
        IoDir.cwd().deleteFile(io, path) catch {};
    }
    inline for (.{ ".snapshot", ".snapshot.incoming", ".snapshot.quarantine" }) |suffix| {
        const path = std.fmt.bufPrint(&path_buffer, "{s}{s}", .{
            state_paths[index],
            suffix,
        }) catch unreachable;
        IoDir.cwd().deleteFile(io, path) catch {};
    }
}
