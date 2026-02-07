const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const Io = std.Io;

const transport = @import("transport.zig");
const protocol = @import("protocol.zig");
const store_mod = @import("store.zig");
const event_mod = @import("event.zig");
const recovery_mod = @import("recovery.zig");
const breadcrumb_mod = @import("breadcrumb.zig");
const contract_mod = @import("contract.zig");
const identity_mod = @import("identity.zig");
const time_mod = @import("time.zig");

const EventStore = store_mod.EventStore;
const EventValue = event_mod.EventValue;
const DependencyContract = contract_mod.DependencyContract;
const ServiceIdentity = identity_mod.ServiceIdentity;
const Breadcrumb = breadcrumb_mod.Breadcrumb;

const log = std.log.scoped(.server);

var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

fn handleSignal(_: std.posix.SIG) callconv(.c) void {
    shutdown_requested.store(true, .release);
}

pub inline fn registerSignalHandlers() void {
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.TERM, &act, null);
    std.posix.sigaction(.INT, &act, null);
}

pub const Server = struct {
    allocator: Allocator,
    store: *EventStore,
    identity: ServiceIdentity,
    contracts: []const DependencyContract,
    listener: transport.Listener,
    io: Io,
    running: bool,

    pub fn init(
        allocator: Allocator,
        store: *EventStore,
        identity: ServiceIdentity,
        contracts: []const DependencyContract,
        io: Io,
    ) !Server {
        const listener = try transport.Listener.init(allocator, identity.listen_address, io);
        return .{
            .allocator = allocator,
            .store = store,
            .identity = identity,
            .contracts = contracts,
            .listener = listener,
            .io = io,
            .running = true,
        };
    }

    pub fn deinit(self: *Server) void {
        self.listener.deinit();
    }

    /// Run startup recovery: compute recovery plan from breadcrumbs,
    /// connect to each peer, send PullRequest, ingest responses,
    /// update breadcrumbs.
    pub fn runRecovery(self: *Server) !void {
        var breadcrumbs = try self.store.get_breadcrumbs();
        defer {
            for (breadcrumbs.items) |*bc| bc.deinit(self.allocator);
            breadcrumbs.deinit(self.allocator);
        }

        var tasks = try recovery_mod.computeRecoveryPlan(self.allocator, self.contracts, breadcrumbs.items);
        defer {
            for (tasks.items) |*t| t.deinit(self.allocator);
            tasks.deinit(self.allocator);
        }

        if (tasks.items.len == 0) {
            log.info("recovery: no tasks to run", .{});
            return;
        }

        var total_ingested: usize = 0;
        var errors_skipped: usize = 0;

        for (tasks.items) |task| {
            self.executeRecoveryTask(task, &total_ingested) catch {
                errors_skipped += 1;
                continue;
            };
        }

        log.info("recovery complete: {d} events ingested, {d} tasks skipped", .{ total_ingested, errors_skipped });
    }

    fn executeRecoveryTask(self: *Server, task: recovery_mod.RecoveryTask, total_ingested: *usize) !void {
        const conn = try transport.dial(self.allocator, task.peer_address, self.io);
        defer conn.destroy();

        try conn.sendMessage(.{ .handshake = .{
            .service_name = self.identity.name,
            .instance_id = self.identity.instance_id,
        } });

        try conn.sendMessage(.{ .pull_request = .{
            .event_type = task.event_type,
            .from_sequence = task.from_sequence,
            .max_count = 0,
        } });

        var response = try conn.recvMessage();
        defer protocol.deinit(&response, self.allocator);

        switch (response) {
            .pull_response => |pr| {
                var highest_seq: u64 = if (task.from_sequence > 0) task.from_sequence - 1 else 0;

                for (pr.events) |entry| {
                    var ev = EventValue.deserialize(self.allocator, entry.value) catch continue;

                    if (ev.sequence > highest_seq) highest_seq = ev.sequence;

                    self.store.ingest_event(ev) catch {
                        ev.deinit(self.allocator);
                        continue;
                    };
                    total_ingested.* += 1;
                }

                if (pr.events.len > 0) {
                    const now: i64 = @intCast((time_mod.Instant.now() catch
                        return).timestamp.sec);

                    const bc = Breadcrumb{
                        .source_service = task.source_service,
                        .event_type = task.event_type,
                        .last_sequence = highest_seq,
                        .peer_address = task.peer_address,
                        .updated_at = now,
                    };
                    self.store.put_breadcrumb(bc, highest_seq, self.identity.name, now) catch {};
                }
            },
            .err => |e| {
                log.warn("recovery error from {s}: {s}", .{ task.peer_address, e.message });
                return error.RecoveryTaskFailed;
            },
            else => return error.UnexpectedMessage,
        }
    }

    pub fn handleConnection(self: *Server, conn: *transport.Connection) void {
        var hs_msg = conn.recvMessage() catch return;

        switch (hs_msg) {
            .handshake => |h| {
                log.info("peer connected: {s}/{s}", .{ h.service_name, h.instance_id });
                protocol.deinit(&hs_msg, self.allocator);
            },
            else => {
                protocol.deinit(&hs_msg, self.allocator);
                conn.sendMessage(.{ .err = .{
                    .code = .invalid_message,
                    .message = "expected handshake",
                } }) catch {};
                return;
            },
        }

        while (true) {
            var msg = conn.recvMessage() catch return;

            switch (msg) {
                .pull_request => |req| {
                    self.servePullRequest(conn, req) catch {
                        protocol.deinit(&msg, self.allocator);
                        return;
                    };
                    protocol.deinit(&msg, self.allocator);
                },
                .put_event => |pe| {
                    const result = self.store.put_event_local(pe.event_type, pe.payload) catch {
                        protocol.deinit(&msg, self.allocator);
                        conn.sendMessage(.{ .err = .{
                            .code = .internal_error,
                            .message = "failed to store event",
                        } }) catch return;
                        continue;
                    };
                    protocol.deinit(&msg, self.allocator);
                    conn.sendMessage(.{ .put_event_response = .{
                        .sequence = result.sequence,
                        .timestamp = result.timestamp,
                    } }) catch return;
                },
                else => {
                    protocol.deinit(&msg, self.allocator);
                    conn.sendMessage(.{ .err = .{
                        .code = .invalid_message,
                        .message = "unexpected message type",
                    } }) catch {};
                },
            }
        }
    }

    fn servePullRequest(self: *Server, conn: *transport.Connection, req: protocol.PullRequest) !void {
        var events = try self.store.query_events(self.identity.name, req.event_type, req.from_sequence);
        defer {
            for (events.items) |*v| v.deinit(self.allocator);
            events.deinit(self.allocator);
        }

        var entries = try self.allocator.alloc(protocol.PullResponse.EventEntry, events.items.len);
        var built: usize = 0;
        defer {
            for (entries[0..built]) |entry| {
                self.allocator.free(entry.key);
                self.allocator.free(entry.value);
            }
            self.allocator.free(entries);
        }

        for (events.items) |*ev| {
            const key = try store_mod.makeEventKey(self.allocator, ev.origin_service, ev.event_type, ev.sequence);
            errdefer self.allocator.free(key);
            const value = try ev.serialize(self.allocator);

            entries[built] = .{ .key = key, .value = value };
            built += 1;
        }

        try conn.sendMessage(.{ .pull_response = .{ .events = entries[0..built] } });
    }

    pub fn serve(self: *Server) !void {
        log.info("listening on {s}", .{self.identity.listen_address});

        while (self.running and !shutdown_requested.load(.acquire)) {
            const conn = self.listener.accept() catch |err| {
                if (!self.running or shutdown_requested.load(.acquire)) return;
                log.warn("accept error: {}", .{err});
                continue;
            };
            self.handleConnection(conn);
            conn.destroy();
        }
    }

    pub fn shutdown(self: *Server) void {
        self.running = false;
    }
};

test "recovery ingests events from peer" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // Clean up test files
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.crumb") catch {};

    // Start a mock peer that serves events
    var peer_listener = try transport.Listener.init(allocator, "127.0.0.1:0", io);
    defer peer_listener.deinit();
    const peer_port = peer_listener.getPort();

    const peer_addr = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{peer_port});
    defer allocator.free(peer_addr);

    // Peer thread: respond to handshake + pull request with 2 events
    const peer_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *transport.Listener) void {
            const conn = l.accept() catch return;
            defer conn.destroy();

            // Receive handshake
            var hs = conn.recvMessage() catch return;
            protocol.deinit(&hs, l.allocator);

            // Receive pull request
            var req = conn.recvMessage() catch return;
            protocol.deinit(&req, l.allocator);

            // Send 2 events
            const entries = [_]protocol.PullResponse.EventEntry{
                .{ .key = "evt:order-service:order.created:0000000000000001", .value = "1|100|2|order-service|order.created|{}" },
                .{ .key = "evt:order-service:order.created:0000000000000002", .value = "2|200|2|order-service|order.created|{}" },
            };
            conn.sendMessage(.{ .pull_response = .{ .events = &entries } }) catch {};
        }
    }.run, .{&peer_listener});

    // Contract pointing to mock peer
    const event_types = [_][]const u8{"order.created"};
    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &event_types,
        .peer_address = peer_addr,
        .retention_hint = null,
    }};

    var event_store = try EventStore.init(allocator, "data/test_recovery_server.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.crumb") catch {};
    }

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };

    var srv = try Server.init(allocator, &event_store, identity, &contracts, io);
    defer srv.deinit();

    try srv.runRecovery();

    peer_thread.join();

    // Verify events were ingested
    var results = try event_store.query_events("order-service", "order.created", 0);
    defer {
        for (results.items) |*v| v.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 2), results.items.len);
    try std.testing.expectEqual(@as(u64, 1), results.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), results.items[1].sequence);
}

test "handleConnection serves PullRequest" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // Clean up
    Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.crumb") catch {};

    // Create store with events
    var event_store = try EventStore.init(allocator, "data/test_serve_pull.wal", io, .{
        .identity = "my-service",
    });
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.crumb") catch {};
    }

    // Put 3 events
    for (1..4) |i| {
        const payload = try std.fmt.allocPrint(allocator, "{{\"id\":{}}}", .{i});
        defer allocator.free(payload);
        const ev = try EventValue.init(allocator, i, "my-service", "order.created", payload, @intCast(i * 100));
        try event_store.put_event(ev);
    }

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };

    var srv = try Server.init(allocator, &event_store, identity, &.{}, io);
    defer srv.deinit();

    const port = srv.listener.getPort();

    // Server thread: accept one connection and handle it
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            const conn = s.listener.accept() catch return;
            defer conn.destroy();
            s.handleConnection(conn);
        }
    }.run, .{&srv});

    // Client: connect, handshake, pull request, receive response
    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const client = try transport.dial(allocator, addr_str, io);

    try client.sendMessage(.{ .handshake = .{
        .service_name = "test-client",
        .instance_id = "c-1",
    } });

    try client.sendMessage(.{ .pull_request = .{
        .event_type = "order.created",
        .from_sequence = 0,
        .max_count = 0,
    } });

    var response = try client.recvMessage();
    defer protocol.deinit(&response, allocator);

    try std.testing.expectEqual(@as(usize, 3), response.pull_response.events.len);

    // Verify events are deserializable and in sequence order
    var ev0 = try EventValue.deserialize(allocator, response.pull_response.events[0].value);
    defer ev0.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 1), ev0.sequence);
    try std.testing.expectEqualStrings("my-service", ev0.origin_service);

    // Close client to unblock server thread
    client.destroy();
    server_thread.join();
}

test "recovery skips unreachable peers" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // Clean up
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_skip.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_skip.crumb") catch {};

    // Contract pointing to an unreachable peer (port 1 — nobody listening)
    const event_types = [_][]const u8{"order.created"};
    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &event_types,
        .peer_address = "127.0.0.1:1",
        .retention_hint = null,
    }};

    var event_store = try EventStore.init(allocator, "data/test_recovery_skip.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
    });
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_recovery_skip.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_recovery_skip.crumb") catch {};
    }

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };

    var srv = try Server.init(allocator, &event_store, identity, &contracts, io);
    defer srv.deinit();

    // Should not error — unreachable peers are skipped
    try srv.runRecovery();

    // Store should have no events (nothing was ingested)
    var results = try event_store.query_events("order-service", "order.created", 0);
    defer results.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 0), results.items.len);
}

test "handleConnection accepts PutEvent and returns sequence" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_put_event.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_put_event.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_put_event.wal", io, .{
        .identity = "my-service",
    });
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_put_event.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_put_event.crumb") catch {};
    }

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };

    var srv = try Server.init(allocator, &event_store, identity, &.{}, io);
    defer srv.deinit();

    const port = srv.listener.getPort();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            const conn = s.listener.accept() catch return;
            defer conn.destroy();
            s.handleConnection(conn);
        }
    }.run, .{&srv});

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const client = try transport.dial(allocator, addr_str, io);

    try client.sendMessage(.{ .handshake = .{
        .service_name = "test-client",
        .instance_id = "c-1",
    } });

    for (0..3) |_| {
        try client.sendMessage(.{ .put_event = .{
            .event_type = "order.created",
            .payload = "{\"item\":\"widget\"}",
        } });

        var response = try client.recvMessage();
        defer protocol.deinit(&response, allocator);

        try std.testing.expect(response.put_event_response.sequence > 0);
        try std.testing.expect(response.put_event_response.timestamp > 0);
    }

    client.destroy();
    server_thread.join();

    var results = try event_store.query_events("my-service", "order.created", 0);
    defer {
        for (results.items) |*v| v.deinit(allocator);
        results.deinit(allocator);
    }
    try std.testing.expectEqual(@as(usize, 3), results.items.len);
    try std.testing.expectEqual(@as(u64, 1), results.items[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), results.items[1].sequence);
    try std.testing.expectEqual(@as(u64, 3), results.items[2].sequence);
    try std.testing.expectEqualStrings("{\"item\":\"widget\"}", results.items[0].payload);
}
