const std = @import("std");
const backup = @import("core/backup.zig");
const config = @import("core/config.zig");
const scrub = @import("core/scrub.zig");
const store = @import("core/store.zig");
const server = @import("core/server.zig");

const Command = enum { serve, verify, scrub, backup, restore };

fn print_usage() void {
    std.debug.print(
        "Usage: compix <config-file>\n" ++
            "       compix verify <config-file>\n" ++
            "       compix scrub <config-file>\n" ++
            "       compix backup <config-file> <new-backup-directory>\n" ++
            "       compix restore <config-file> <backup-directory>\n",
        .{},
    );
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();

    const first_argument = args.next() orelse {
        print_usage();
        return error.MissingArgument;
    };
    const command: Command = if (std.mem.eql(u8, first_argument, "verify"))
        .verify
    else if (std.mem.eql(u8, first_argument, "scrub"))
        .scrub
    else if (std.mem.eql(u8, first_argument, "backup"))
        .backup
    else if (std.mem.eql(u8, first_argument, "restore"))
        .restore
    else
        .serve;
    const config_path = if (command == .serve)
        first_argument
    else
        args.next() orelse return error.MissingConfigPath;
    const archive_path = if (command == .backup or command == .restore)
        args.next() orelse return error.MissingBackupPath
    else
        null;
    if (args.next() != null) return error.TooManyArguments;

    var cfg = config.load(allocator, config_path, io) catch |err| {
        std.debug.print("Failed to load config '{s}': {}\n", .{ config_path, err });
        return err;
    };
    defer cfg.deinit();

    if (command == .restore) {
        const restored = backup.restore(allocator, io, &cfg, archive_path.?) catch |err| {
            std.debug.print("Failed to restore backup '{s}': {}\n", .{ archive_path.?, err });
            return err;
        };
        var restored_store = store.EventStore.init(allocator, cfg.wal_path, io, .{
            .identity = cfg.identity.name,
            .contracts = cfg.contracts,
            .stream_capacity = cfg.stream_capacity,
            .stream_id_bytes_capacity = cfg.stream_id_bytes_capacity,
        }) catch |err| {
            std.debug.print("Restored files failed to open: {}\n", .{err});
            return err;
        };
        defer restored_store.deinit();
        const verification = restored_store.verify_integrity() catch |err| {
            std.debug.print("Restored store failed integrity verification: {}\n", .{err});
            return err;
        };
        if (cfg.cluster) |cluster| {
            try backup.verify_cluster_state(allocator, io, cluster);
        }
        if (verification.committed_log_position != restored.committed_log_position)
            return error.RestoredPositionMismatch;
        std.debug.print(
            "{{\"ok\":true,\"operation\":\"restore\",\"files\":{d}," ++
                "\"bytes\":{d},\"streams\":{d},\"batches\":{d}," ++
                "\"events\":{d},\"committed_log_position\":{d}}}\n",
            .{
                restored.files,
                restored.bytes,
                verification.streams,
                verification.batches,
                verification.events,
                verification.committed_log_position,
            },
        );
        return;
    }

    var event_store = store.EventStore.init(allocator, cfg.wal_path, io, .{
        .identity = cfg.identity.name,
        .contracts = cfg.contracts,
        .stream_capacity = cfg.stream_capacity,
        .stream_id_bytes_capacity = cfg.stream_id_bytes_capacity,
    }) catch |err| {
        std.debug.print("Failed to initialize store at '{s}': {}\n", .{ cfg.wal_path, err });
        return err;
    };
    defer event_store.deinit();

    if (command == .verify) {
        const verification = try event_store.verify_integrity();
        if (cfg.cluster) |cluster| {
            try backup.verify_cluster_state(allocator, io, cluster);
        }
        std.debug.print(
            "{{\"ok\":true,\"streams\":{d},\"batches\":{d}," ++
                "\"events\":{d},\"committed_log_position\":{d}}}\n",
            .{
                verification.streams,
                verification.batches,
                verification.events,
                verification.committed_log_position,
            },
        );
        return;
    }

    if (command == .scrub) {
        if (cfg.cluster) |cluster| {
            try backup.verify_cluster_state(allocator, io, cluster);
        }
        const result = scrub.run(allocator, io, cfg.wal_path, &event_store) catch |err| {
            std.debug.print("Scrub failed: {}\n", .{err});
            return err;
        };
        std.debug.print(
            "{{\"ok\":true,\"operation\":\"scrub\",\"live_sstables\":{d}," ++
                "\"candidates\":{d},\"quarantined\":{d}}}\n",
            .{ result.live_sstables, result.candidates, result.quarantined },
        );
        return;
    }

    if (command == .backup) {
        const saved = backup.create(allocator, io, &cfg, &event_store, archive_path.?) catch |err| {
            std.debug.print("Failed to create backup '{s}': {}\n", .{ archive_path.?, err });
            return err;
        };
        std.debug.print(
            "{{\"ok\":true,\"operation\":\"backup\",\"files\":{d}," ++
                "\"bytes\":{d},\"committed_log_position\":{d}}}\n",
            .{ saved.files, saved.bytes, saved.committed_log_position },
        );
        return;
    }

    const startup_verification = event_store.verify_integrity() catch |err| {
        std.debug.print("Startup integrity verification failed: {}\n", .{err});
        return err;
    };
    const startup_scrub = scrub.run(allocator, io, cfg.wal_path, &event_store) catch |err| {
        std.debug.print("Startup orphan scrub failed: {}\n", .{err});
        return err;
    };
    if (startup_scrub.quarantined > 0) {
        std.debug.print(
            "compix: quarantined {d} orphan storage files before serving\n",
            .{startup_scrub.quarantined},
        );
    }
    const backup_state = backup.read_state(allocator, io, cfg.wal_path) catch |err| {
        std.debug.print("Backup state verification failed: {}\n", .{err});
        return err;
    };

    var srv: server.Server = undefined;
    server.Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = cfg.identity,
        .contracts = cfg.contracts,
        .io = io,
        .connection_capacity = cfg.connection_capacity,
        .client_secret = cfg.client_secret,
        .credentials = cfg.credentials,
        .integrity_verified = true,
        .backups_completed = backup_state.completed,
        .last_backup_unix_seconds = backup_state.last_success_unix_seconds,
        .cluster = cfg.cluster,
    }) catch |err| {
        std.debug.print("Failed to start server: {}\n", .{err});
        return err;
    };
    defer srv.deinit();

    if (cfg.cluster == null) {
        srv.run_recovery() catch |err| {
            std.debug.print("Recovery warning: {}\n", .{err});
        };
    } else if (cfg.contracts.len > 0) {
        std.debug.print(
            "compix: peer recovery is disabled in clustered mode; ingest must use Raft\n",
            .{},
        );
    }

    server.register_signal_handlers();

    if (cfg.cluster) |cluster| {
        std.debug.print(
            "compix: {s}/{s} cluster node {d} starting election; members={d}; " ++
                "streams={d}/{d}; committed_position={d}; connections={d}; " ++
                "raft_log={d}; snapshot_max={d}\n",
            .{
                cfg.identity.name,
                cfg.identity.instance_id,
                cluster.node_id,
                cluster.members.len,
                startup_verification.streams,
                cfg.stream_capacity,
                startup_verification.committed_log_position,
                cfg.connection_capacity,
                cluster.log_capacity,
                cluster.snapshot_size_max,
            },
        );
    } else {
        std.debug.print(
            "compix: {s}/{s} ready in single-node degraded mode; streams={d}/{d}; " ++
                "committed_position={d}; stream_id_bytes={d}; connections={d}\n",
            .{
                cfg.identity.name,
                cfg.identity.instance_id,
                startup_verification.streams,
                cfg.stream_capacity,
                startup_verification.committed_log_position,
                cfg.stream_id_bytes_capacity,
                cfg.connection_capacity,
            },
        );
    }

    try srv.serve();

    std.debug.print("compix: shutting down\n", .{});
}
