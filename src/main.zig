const std = @import("std");
const config = @import("core/config.zig");
const store = @import("core/store.zig");
const server = @import("core/server.zig");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.skip();

    const config_path = args.next() orelse {
        std.debug.print("Usage: compix <config-file>\n", .{});
        return error.MissingArgument;
    };

    var cfg = config.load(allocator, config_path, io) catch |err| {
        std.debug.print("Failed to load config '{s}': {}\n", .{ config_path, err });
        return err;
    };
    defer cfg.deinit();

    var event_store = store.EventStore.init(allocator, cfg.wal_path, io, .{
        .identity = cfg.identity.name,
        .contracts = cfg.contracts,
    }) catch |err| {
        std.debug.print("Failed to initialize store at '{s}': {}\n", .{ cfg.wal_path, err });
        return err;
    };
    defer event_store.deinit();

    var srv = server.Server.init(allocator, &event_store, cfg.identity, cfg.contracts, io) catch |err| {
        std.debug.print("Failed to start server: {}\n", .{err});
        return err;
    };
    defer srv.deinit();

    srv.runRecovery() catch |err| {
        std.debug.print("Recovery warning: {}\n", .{err});
    };

    std.debug.print("compix: {s}/{s} ready\n", .{ cfg.identity.name, cfg.identity.instance_id });

    try srv.serve();
}
