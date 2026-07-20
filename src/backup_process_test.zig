const std = @import("std");
const config_mod = @import("core/config.zig");
const protocol = @import("core/protocol.zig");
const store_mod = @import("core/store.zig");

const Io = std.Io;
const IoDir = Io.Dir;

const source_config_path = "test/backup_source.json";
const target_config_path = "test/backup_target.json";
const archive_path = "data/process_backup_archive";

pub fn main(init: std.process.Init) !void {
    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();
    const command = args.next() orelse return error.MissingCommand;
    if (args.next() != null) return error.TooManyArguments;
    if (std.mem.eql(u8, command, "prepare")) return prepare(init.gpa, init.io);
    if (std.mem.eql(u8, command, "check")) return check(init.gpa, init.io);
    return error.UnknownCommand;
}

fn prepare(allocator: std.mem.Allocator, io: Io) !void {
    cleanup(io);
    var config = try config_mod.load(allocator, source_config_path, io);
    defer config.deinit();
    var store = try store_mod.EventStore.init(allocator, config.wal_path, io, .{
        .identity = config.identity.name,
        .contracts = config.contracts,
        .stream_capacity = config.stream_capacity,
        .stream_id_bytes_capacity = config.stream_id_bytes_capacity,
    });
    defer store.deinit();
    const events = [_]protocol.AppendEvent{
        .{ .event_type = "process.created", .schema_version = 1, .payload = "first" },
        .{ .event_type = "process.updated", .schema_version = 1, .payload = "second" },
    };
    const response = try store.append_stream(.{
        .stream_id = "backup-process/1",
        .expected_version = .empty,
        .command_id = "backup-process-command",
        .events = &events,
    }, 1);
    if (response != .committed) return error.ExpectedCommitted;
    try store.lsm.flush();
}

fn check(allocator: std.mem.Allocator, io: Io) !void {
    defer cleanup(io);
    var config = try config_mod.load(allocator, target_config_path, io);
    defer config.deinit();
    var store = try store_mod.EventStore.init(allocator, config.wal_path, io, .{
        .identity = config.identity.name,
        .contracts = config.contracts,
        .stream_capacity = config.stream_capacity,
        .stream_id_bytes_capacity = config.stream_id_bytes_capacity,
    });
    defer store.deinit();
    const verification = try store.verify_integrity();
    if (verification.streams != 1 or verification.batches != 1 or
        verification.events != 2 or verification.committed_log_position != 2)
        return error.RestoredContentMismatch;
    const events = [_]protocol.AppendEvent{
        .{ .event_type = "process.created", .schema_version = 1, .payload = "first" },
        .{ .event_type = "process.updated", .schema_version = 1, .payload = "second" },
    };
    const response = try store.append_stream(.{
        .stream_id = "backup-process/1",
        .expected_version = .empty,
        .command_id = "backup-process-command",
        .events = &events,
    }, 2);
    if (response != .replayed) return error.ExpectedReplayed;
}

fn cleanup(io: Io) void {
    inline for (.{ "source", "target" }) |kind| {
        var path_buffer: [256]u8 = undefined;
        inline for (.{ ".wal", ".crumb", ".manifest", ".backup-state" }) |suffix| {
            const path = std.fmt.bufPrint(
                &path_buffer,
                "data/process_backup_{s}{s}",
                .{ kind, suffix },
            ) catch unreachable;
            IoDir.cwd().deleteFile(io, path) catch {};
        }
        for (0..3) |level| {
            for (1..9) |id| {
                const path = std.fmt.bufPrint(
                    &path_buffer,
                    "data/process_backup_{s}_L{d}_{d:0>8}.sst",
                    .{ kind, level, id },
                ) catch unreachable;
                IoDir.cwd().deleteFile(io, path) catch {};
            }
        }
    }
    IoDir.cwd().deleteTree(io, archive_path) catch {};
}
