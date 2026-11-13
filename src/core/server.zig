const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const transport = @import("transport.zig");
const protocol = @import("protocol.zig");
const auth = @import("auth.zig");
const store_mod = @import("store.zig");
const event_mod = @import("event.zig");
const recovery_mod = @import("recovery.zig");
const breadcrumb_mod = @import("breadcrumb.zig");
const contract_mod = @import("contract.zig");
const identity_mod = @import("identity.zig");
const time_mod = @import("time.zig");
const limits = @import("limits.zig");
const owner_mod = @import("owner.zig");
const subscription_mod = @import("subscription.zig");

const EventStore = store_mod.EventStore;
const StreamStore = @import("stream_store.zig").StreamStore;
const EventValue = event_mod.EventValue;
const DependencyContract = contract_mod.DependencyContract;
const ServiceIdentity = identity_mod.ServiceIdentity;
const Breadcrumb = breadcrumb_mod.Breadcrumb;
const Subscriber = subscription_mod.Subscriber;

const log = std.log.scoped(.server);
// NOTE: a leak bound, not a liveness mechanism; keep it above any client stall.
const connection_idle_deadline_ms: u32 = 60 * 60 * 1000;
const handshake_deadline_ms: u32 = if (builtin.is_test) 50 else 3_000;
const bootstrap_pages_max: u32 = 4_096;
const gossip_dial_timeout_ms: u32 = if (builtin.is_test) 50 else 5_000;
const gossip_interval_ms: u64 = if (builtin.is_test) 20 else 60_000;
const gossip_retry_first_ms: u64 = if (builtin.is_test) 20 else 1_000;
const gossip_tick_ms: u64 = 100;
/// A quiet subscription still gets an empty batch this often, so the subscriber can tell a
/// quiet log from a dead connection well inside the driver's receive deadline.
const subscription_heartbeat_ms: u32 = if (builtin.is_test) 50 else 15_000;
/// A subscriber that has not taken a frame for this long is closed instead of stalling its pusher.
const subscription_send_deadline_ms: u32 = if (builtin.is_test) 1_000 else 30_000;

/// After SIGTERM or SIGINT the process exits within this bound even if teardown hangs.
pub const shutdown_deadline_ms: u32 = 10_000;

var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
var shutdown_deadline_armed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

comptime {
    std.debug.assert(connection_idle_deadline_ms > 0);
    std.debug.assert(connection_idle_deadline_ms < std.math.maxInt(u32));
    std.debug.assert(subscription_heartbeat_ms > 0);
    std.debug.assert(subscription_heartbeat_ms < connection_idle_deadline_ms);
    std.debug.assert(subscription_send_deadline_ms > 0);
}

fn handle_signal(_: std.posix.SIG) callconv(.c) void {
    shutdown_requested.store(true, .release);
}

pub inline fn register_signal_handlers() void {
    shutdown_requested.store(false, .release);
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handle_signal },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(.TERM, &act, null);
    std.posix.sigaction(.INT, &act, null);
}

fn arm_shutdown_deadline(io: Io) void {
    if (shutdown_deadline_armed.swap(true, .acq_rel)) return;
    const thread = std.Thread.spawn(.{ .stack_size = 64 * 1024 }, enforce_shutdown_deadline, .{io}) catch |err| {
        log.warn("shutdown deadline not armed: {}", .{err});
        return;
    };
    thread.detach();
}

fn enforce_shutdown_deadline(io: Io) void {
    io.sleep(.fromMilliseconds(shutdown_deadline_ms), .awake) catch {};
    std.debug.print("compix: shutdown exceeded {d} ms; exiting\n", .{shutdown_deadline_ms});
    std.process.exit(1);
}

pub const Server = struct {
    allocator: Allocator,
    store: *EventStore,
    identity: ServiceIdentity,
    contracts: []const DependencyContract,
    client_secret: []const u8,
    credentials: []const auth.Credential,
    listener: transport.Listener,
    io: Io,
    running: std.atomic.Value(bool),
    gossip_started: std.atomic.Value(bool) = .init(false),
    connection_group: Io.Group,
    connection_pool: []ConnectionSlot,
    connection_pool_mutex: Io.Mutex,
    connection_slots: Io.Semaphore,
    group_commit_condition: Io.Condition,
    group_commit_count: u64,
    group_commit_failed: bool,
    group_commit_generation: u64,
    group_commit_leader: bool,
    group_commit_mutex: Io.Mutex,
    store_mutex: Io.Mutex,
    flush_mutex: Io.Mutex,
    flush_needed: Io.Condition,
    flush_done: Io.Condition,
    flush_active_bg: bool,
    flush_failed: bool,
    replay_entries: []auth.ReplayEntry,
    replay_mutex: Io.Mutex,
    recovery_connection: *transport.Connection,
    recovery_tasks: []recovery_mod.RecoveryTask,
    recovery_scratch: []u8,
    integrity_verified: bool,
    connections_active: std.atomic.Value(u16),
    store_owner: *owner_mod.Owner,
    auth_failures: std.atomic.Value(u64),
    permission_denials: std.atomic.Value(u64),
    admission_saturation: std.atomic.Value(u64),
    corruption_findings: std.atomic.Value(u64),
    backups_completed: u64,
    last_backup_unix_seconds: i64,
    /// Lets group commit skip walking the connection pool while nobody subscribes.
    subscriptions_active: std.atomic.Value(u16),

    const ConnectionSlot = struct {
        connection: transport.Connection,
        scratch: ConnectionScratch,
        in_use: bool,
    };

    const ConnectionScratch = struct {
        query_events: [limits.pull_events_max]EventValue,
        read_events: [limits.read_events_max]protocol.ReadEvent,
        vc_entries: [protocol.vc_entries_max]protocol.VcEntry,
        bootstrap_cursor: [limits.key_length_max]u8,
        query_bytes: [protocol.frame_size_max]u8,
        receive_completions: [2]ReceiveCompletion,
        send_completions: [2]SendCompletion,
        subscriber: Subscriber,
    };

    const SendCompletion = union(enum) {
        sent: anyerror!void,
        deadline: Io.Cancelable!void,
    };

    const ReceiveCompletion = union(enum) {
        message: anyerror!protocol.Message,
        deadline: Io.Cancelable!void,
    };

    const AuthenticatedSession = struct {
        permissions: ?auth.Permissions,
    };

    pub const InitOptions = struct {
        store: *EventStore,
        identity: ServiceIdentity,
        contracts: []const DependencyContract,
        io: Io,
        connection_capacity: limits.ConnectionCount = limits.connection_capacity_test_default,
        client_secret: []const u8 = "",
        credentials: []const auth.Credential = &.{},
        integrity_verified: bool = true,
        backups_completed: u64 = 0,
        last_backup_unix_seconds: i64 = 0,
    };

    /// In place, like `init_in_place`: the store owner's thread points back into the server.
    pub fn init(
        target: *Server,
        allocator: Allocator,
        store: *EventStore,
        identity: ServiceIdentity,
        contracts: []const DependencyContract,
        io: Io,
    ) !void {
        try init_in_place(target, allocator, .{
            .store = store,
            .identity = identity,
            .contracts = contracts,
            .io = io,
        });
    }

    pub fn init_in_place(
        target: *Server,
        allocator: Allocator,
        options: InitOptions,
    ) !void {
        var listener = try transport.Listener.init(
            options.identity.listen_address,
            options.io,
        );
        errdefer listener.deinit();

        if (options.connection_capacity == 0 or
            options.connection_capacity > limits.connections_max)
            return error.InvalidConnectionCapacity;
        const connection_pool = try allocator.alloc(ConnectionSlot, options.connection_capacity);
        errdefer allocator.free(connection_pool);
        for (connection_pool) |*slot| {
            slot.in_use = false;
            slot.scratch.subscriber = .idle;
        }

        var recovery_task_capacity: u32 = 0;
        for (options.contracts) |contract| {
            recovery_task_capacity += @intCast(contract.event_types.len);
        }
        if (recovery_task_capacity > limits.recovery_tasks_max)
            return error.TooManyRecoveryTasks;
        const recovery_tasks = try allocator.alloc(
            recovery_mod.RecoveryTask,
            recovery_task_capacity,
        );
        errdefer allocator.free(recovery_tasks);
        const recovery_connection = try allocator.create(transport.Connection);
        errdefer allocator.destroy(recovery_connection);
        const recovery_scratch = try allocator.alloc(u8, limits.event_serialized_size_max);
        errdefer allocator.free(recovery_scratch);
        const replay_capacity: usize = if (options.credentials.len == 0)
            0
        else
            auth.replay_capacity_default;
        const replay_entries = try allocator.alloc(auth.ReplayEntry, replay_capacity);
        errdefer allocator.free(replay_entries);
        for (replay_entries) |*entry| entry.* = .{};

        const store_owner = try allocator.create(owner_mod.Owner);
        errdefer allocator.destroy(store_owner);
        store_owner.* = try owner_mod.Owner.init(allocator, options.store, options.io, .{
            .capacity = std.math.ceilPowerOfTwoAssert(u32, options.connection_capacity),
            .legacy_store_lock = &target.store_mutex,
            .durable_listener = .{ .context = target, .on_durable = on_durable },
        });
        errdefer store_owner.deinit();

        target.* = .{
            .allocator = allocator,
            .store = options.store,
            .identity = options.identity,
            .contracts = options.contracts,
            .client_secret = options.client_secret,
            .credentials = options.credentials,
            .listener = listener,
            .io = options.io,
            .running = .init(true),
            .connection_group = .init,
            .connection_pool = connection_pool,
            .connection_pool_mutex = .init,
            .connection_slots = .{ .permits = options.connection_capacity },
            .group_commit_condition = .init,
            .group_commit_count = 0,
            .group_commit_failed = false,
            .group_commit_generation = 0,
            .group_commit_leader = false,
            .group_commit_mutex = .init,
            .store_mutex = .init,
            .flush_mutex = .init,
            .flush_needed = .init,
            .flush_done = .init,
            .flush_active_bg = false,
            .flush_failed = false,
            .replay_entries = replay_entries,
            .replay_mutex = .init,
            .recovery_connection = recovery_connection,
            .recovery_tasks = recovery_tasks,
            .recovery_scratch = recovery_scratch,
            .integrity_verified = options.integrity_verified,
            .store_owner = store_owner,
            .connections_active = .init(0),
            .auth_failures = .init(0),
            .permission_denials = .init(0),
            .admission_saturation = .init(0),
            .corruption_findings = .init(@as(u64, @intCast(
                options.store.metrics().startup_quarantined_files,
            ))),
            .backups_completed = options.backups_completed,
            .last_backup_unix_seconds = options.last_backup_unix_seconds,
            .subscriptions_active = .init(0),
        };
        // NOTE: only now, since the owner calls back into the finished server.
        try store_owner.start();

        options.store.lsm.enable_background_flush();
    }

    pub fn deinit(self: *Server) void {
        self.connection_group.cancel(self.io);
        self.store_owner.stop();
        self.store_owner.deinit();
        self.allocator.destroy(self.store_owner);
        for (self.connection_pool) |slot| {
            std.debug.assert(!slot.in_use);
        }
        self.allocator.free(self.connection_pool);
        self.allocator.destroy(self.recovery_connection);
        self.allocator.free(self.recovery_tasks);
        self.allocator.free(self.recovery_scratch);
        self.allocator.free(self.replay_entries);
        self.listener.deinit();
    }

    pub fn run_recovery(self: *Server) !void {
        return self.recoveryPass(true);
    }

    pub fn gossipOnce(self: *Server) void {
        self.recoveryPass(false) catch |err| {
            log.warn("gossip pass failed: {}", .{err});
        };
    }

    fn recoveryPass(self: *Server, startup: bool) !void {
        var task_count: u32 = 0;
        for (self.contracts) |contract| {
            for (contract.event_types) |event_type| {
                std.debug.assert(task_count < self.recovery_tasks.len);
                self.store_mutex.lockUncancelable(self.io);
                const last_sequence = self.store.get_breadcrumb_sequence(
                    contract.source_service,
                    event_type,
                    contract.peer_address,
                    self.recovery_scratch,
                ) catch |err| {
                    self.store_mutex.unlock(self.io);
                    return err;
                };
                self.store_mutex.unlock(self.io);
                const from_sequence = if (last_sequence) |sequence| blk: {
                    const addition = @addWithOverflow(sequence, 1);
                    if (addition[1] != 0) return error.SequenceOverflow;
                    break :blk addition[0];
                } else 0;
                self.recovery_tasks[task_count] = .{
                    .peer_address = contract.peer_address,
                    .source_service = contract.source_service,
                    .event_type = event_type,
                    .from_sequence = from_sequence,
                    .had_breadcrumb = last_sequence != null,
                };
                task_count += 1;
            }
        }

        if (task_count == 0) {
            if (startup) log.info("recovery: no tasks to run", .{});
            return;
        }

        var total_ingested: u32 = 0;
        var errors_skipped: u32 = 0;

        var processed: u32 = 0;
        while (processed < task_count) : (processed += 1) {
            if (self.recovery_tasks[processed].bootstrap_done) continue;
            const peer_address = self.recovery_tasks[processed].peer_address;
            const source_service = self.recovery_tasks[processed].source_service;

            var peer_fresh = true;
            var span_end = processed;
            while (span_end < task_count) : (span_end += 1) {
                const task = self.recovery_tasks[span_end];
                if (!std.mem.eql(u8, task.peer_address, peer_address)) continue;
                if (!std.mem.eql(u8, task.source_service, source_service)) continue;
                if (task.had_breadcrumb) peer_fresh = false;
            }
            var inner = processed;
            while (inner < task_count) : (inner += 1) {
                const task = self.recovery_tasks[inner];
                if (!std.mem.eql(u8, task.peer_address, peer_address)) continue;
                if (!std.mem.eql(u8, task.source_service, source_service)) continue;
                if (peer_fresh) {
                    self.recovery_tasks[inner].bootstrap_done = true;
                }
            }

            if (peer_fresh) {
                const contract_types = self.contractTypesFor(
                    source_service,
                    peer_address,
                ) orelse &.{};
                self.execute_bootstrap_drain(
                    peer_address,
                    source_service,
                    contract_types,
                    &total_ingested,
                ) catch |err| {
                    log.warn("bootstrap from {s} failed: {}", .{ peer_address, err });
                    errors_skipped += 1;
                };
                continue;
            }

            var inner_index = processed;
            while (inner_index < task_count) : (inner_index += 1) {
                const task = self.recovery_tasks[inner_index];
                if (!std.mem.eql(u8, task.peer_address, peer_address)) continue;
                if (!std.mem.eql(u8, task.source_service, source_service)) continue;
                if (task.bootstrap_done) continue;
                self.execute_recovery_task(task, &total_ingested) catch {
                    errors_skipped += 1;
                    continue;
                };
            }
        }

        if (startup or total_ingested > 0 or errors_skipped > 0) {
            log.info("recovery complete: {d} events ingested, {d} tasks skipped", .{
                total_ingested,
                errors_skipped,
            });
        }
    }

    fn contractTypesFor(
        self: *Server,
        source_service: []const u8,
        peer_address: []const u8,
    ) ?[]const []const u8 {
        for (self.contracts) |*contract| {
            if (std.mem.eql(u8, contract.source_service, source_service) and
                std.mem.eql(u8, contract.peer_address, peer_address))
                return contract.event_types;
        }
        return null;
    }

    fn execute_bootstrap_drain(
        self: *Server,
        peer_address: []const u8,
        source_service: []const u8,
        contract_types: []const []const u8,
        total_ingested: *u32,
    ) !void {
        try transport.dial_into_timeout(
            self.recovery_connection,
            peer_address,
            self.io,
            gossip_dial_timeout_ms,
        );
        const conn = self.recovery_connection;
        defer conn.close();

        if (self.client_secret.len == 0) {
            try conn.send_message(.{ .handshake = .{
                .service_name = self.identity.name,
                .instance_id = self.identity.instance_id,
            } });
        } else {
            try self.authenticate_as_client(conn);
        }

        const maxima = try self.allocator.alloc(limits.Sequence, contract_types.len);
        defer self.allocator.free(maxima);
        @memset(maxima, 0);

        var cursor: [limits.key_length_max]u8 = undefined;
        var cursor_len: usize = 0;
        var complete = false;
        var pages: u32 = 0;
        while (!complete) {
            pages += 1;
            if (pages > bootstrap_pages_max) return error.BootstrapPageLimitExceeded;
            try conn.send_message(.{ .bootstrap_request = .{
                .cursor = cursor[0..cursor_len],
                .max_count = 0,
            } });
            const response = try conn.recv_message();
            switch (response) {
                .bootstrap_response => |page| {
                    for (page.events) |entry| {
                        const ev = EventValue.deserialize_borrowed(entry.value) catch |err| {
                            log.warn("bootstrap: corrupt event from {s}: {}", .{
                                peer_address,
                                err,
                            });
                            continue;
                        };
                        for (contract_types, 0..) |contract_type, index| {
                            if (std.mem.eql(u8, ev.event_type, contract_type) and
                                ev.sequence > maxima[index])
                                maxima[index] = ev.sequence;
                        }
                        // WARN: runs beside request handlers and syncs the WAL; hold store_mutex.
                        self.store_mutex.lockUncancelable(self.io);
                        const ingested = self.store.ingest_event(ev);
                        self.store_mutex.unlock(self.io);
                        ingested catch |err| {
                            log.debug("bootstrap: skipped event from {s}: {}", .{
                                peer_address,
                                err,
                            });
                            continue;
                        };
                        total_ingested.* += 1;
                    }
                    if (page.next_cursor.len > cursor.len) return error.InvalidCursor;
                    @memcpy(cursor[0..page.next_cursor.len], page.next_cursor);
                    cursor_len = page.next_cursor.len;
                    complete = page.complete;
                },
                .err => |e| {
                    log.warn("bootstrap error from {s}: {s}", .{ peer_address, e.message });
                    return error.RecoveryTaskFailed;
                },
                else => return error.UnexpectedMessage,
            }
        }

        const now: limits.Timestamp = @intCast(time_mod.Instant.now(self.io).timestamp.sec);
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        for (contract_types, 0..) |contract_type, index| {
            if (maxima[index] == 0) continue;
            const breadcrumb = Breadcrumb{
                .source_service = source_service,
                .event_type = contract_type,
                .last_sequence = maxima[index],
                .peer_address = peer_address,
                .updated_at = now,
            };
            try self.store.put_breadcrumb(
                breadcrumb,
                maxima[index],
                self.identity.name,
                now,
            );
        }
    }

    fn execute_recovery_task(
        self: *Server,
        task: recovery_mod.RecoveryTask,
        total_ingested: *u32,
    ) !void {
        try transport.dial_into_timeout(
            self.recovery_connection,
            task.peer_address,
            self.io,
            gossip_dial_timeout_ms,
        );
        const conn = self.recovery_connection;
        defer conn.close();

        if (self.client_secret.len == 0) {
            try conn.send_message(.{ .handshake = .{
                .service_name = self.identity.name,
                .instance_id = self.identity.instance_id,
            } });
        } else {
            try self.authenticate_as_client(conn);
        }

        try conn.send_message(.{ .pull_request = .{
            .event_type = task.event_type,
            .from_sequence = task.from_sequence,
            .max_count = 0,
        } });

        const response = try conn.recv_message();

        switch (response) {
            .pull_response => |pr| {
                var highest_seq: u64 = if (task.from_sequence > 0) task.from_sequence - 1 else 0;

                for (pr.events) |entry| {
                    const ev = EventValue.deserialize_borrowed(entry.value) catch |err| {
                        log.warn("recovery: corrupt event from {s}: {}", .{
                            task.peer_address,
                            err,
                        });
                        continue;
                    };

                    if (ev.sequence > highest_seq) highest_seq = ev.sequence;

                    // WARN: runs beside request handlers and syncs the WAL; hold store_mutex.
                    self.store_mutex.lockUncancelable(self.io);
                    const ingested = self.store.ingest_event(ev);
                    self.store_mutex.unlock(self.io);
                    ingested catch |err| {
                        log.warn("recovery: rejected event from {s}: {}", .{
                            task.peer_address,
                            err,
                        });
                        continue;
                    };
                    total_ingested.* += 1;
                }

                if (pr.events.len > 0) {
                    try self.record_recovery_progress(task, highest_seq);
                }
            },
            .err => |e| {
                log.warn("recovery error from {s}: {s}", .{ task.peer_address, e.message });
                return error.RecoveryTaskFailed;
            },
            else => return error.UnexpectedMessage,
        }
    }

    fn record_recovery_progress(
        self: *Server,
        task: recovery_mod.RecoveryTask,
        sequence: limits.Sequence,
    ) !void {
        const now: limits.Timestamp = @intCast(time_mod.Instant.now(self.io).timestamp.sec);
        const breadcrumb = Breadcrumb{
            .source_service = task.source_service,
            .event_type = task.event_type,
            .last_sequence = sequence,
            .peer_address = task.peer_address,
            .updated_at = now,
        };
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        try self.store.put_breadcrumb(breadcrumb, sequence, self.identity.name, now);
    }

    pub fn handle_connection(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
    ) !void {
        const session = try self.authenticate_connection(conn, scratch);

        for (0..limits.messages_per_connection_max) |_| {
            const msg = try self.recv_message_bounded(conn, scratch);
            if (session.permissions) |granted| {
                if (!message_permitted(granted, msg)) {
                    _ = self.permission_denials.fetchAdd(1, .monotonic);
                    try conn.send_message(.{ .err = .{
                        .code = .permission_denied,
                        .message = "operation not permitted",
                    } });
                    continue;
                }
            }
            switch (msg) {
                .pull_request => |req| {
                    self.serve_pull_request(conn, scratch, req) catch |err| {
                        return err;
                    };
                },
                .append_stream => |request| {
                    try self.serve_append_stream(conn, request);
                },
                .read_stream => |request| {
                    try self.serve_read_stream(conn, scratch, request);
                },
                .read_log => |request| {
                    try self.serve_read_log(conn, scratch, request);
                },
                .health_request => {
                    try conn.send_message(.{ .health_response = self.health() });
                },
                .vc_summary_request => {
                    var summary_len: usize = 0;
                    self.submit_to_owner(.{ .vc_summary = .{
                        .out = &scratch.vc_entries,
                        .out_len = &summary_len,
                    } }) catch |err| {
                        try send_store_error(conn, err);
                        continue;
                    };
                    assert(summary_len <= scratch.vc_entries.len);
                    try conn.send_message(.{ .vc_summary_response = .{
                        .entries = scratch.vc_entries[0..summary_len],
                    } });
                },
                .bootstrap_request => |request| {
                    try self.serve_bootstrap_request(conn, scratch, request);
                },
                // NOTE: a subscription owns the connection until either side ends it.
                .subscribe => |request| {
                    return self.serve_subscription(conn, scratch, request);
                },
                else => {
                    conn.send_message(.{ .err = .{
                        .code = .invalid_message,
                        .message = "unexpected message type",
                    } }) catch |err| {
                        log.warn("failed to send error reply: {}", .{err});
                        return err;
                    };
                },
            }
        }

        return error.MessageLimitExceeded;
    }

    fn authenticate_connection(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
    ) !AuthenticatedSession {
        const message = try self.recv_message_with_deadline(
            conn,
            scratch,
            handshake_deadline_ms,
        );
        const hello = switch (message) {
            .handshake => |handshake| handshake,
            else => return self.reject_authentication(conn, error.InvalidHandshake),
        };
        if (self.credentials.len == 0) {
            log.info("peer connected without configured authentication: {s}/{s}", .{
                hello.service_name,
                hello.instance_id,
            });
            return .{ .permissions = null };
        }
        if (hello.kind != .client_hello)
            return self.reject_authentication(conn, error.InvalidHandshake);
        const credential = auth.find(self.credentials, hello.service_name) orelse
            return self.reject_authentication(conn, error.UnknownService);
        if (!auth.verify_client_proof(
            credential.secret,
            hello.service_name,
            hello.instance_id,
            hello.nonce,
            hello.proof,
        )) return self.reject_authentication(conn, error.AuthenticationFailed);
        self.replay_mutex.lockUncancelable(self.io);
        auth.claim_client_hello(
            self.replay_entries,
            hello.service_name,
            hello.nonce,
            time_mod.Instant.now(self.io).timestamp.sec,
        ) catch |err| {
            self.replay_mutex.unlock(self.io);
            return self.reject_authentication(conn, err);
        };
        self.replay_mutex.unlock(self.io);
        var server_nonce: [auth.nonce_length]u8 = undefined;
        try self.io.randomSecure(&server_nonce);
        try conn.send_message(.{ .handshake = .{
            .kind = .server_hello,
            .service_name = self.identity.name,
            .instance_id = self.identity.instance_id,
            .nonce = server_nonce,
            .proof = auth.make_server_proof(
                credential.secret,
                hello.nonce,
                server_nonce,
                self.identity.name,
                self.identity.instance_id,
            ),
        } });
        const keys = auth.derive_session_keys(
            credential.secret,
            hello.nonce,
            server_nonce,
            hello.service_name,
            hello.instance_id,
            self.identity.name,
            self.identity.instance_id,
        );
        try conn.enable_security(keys, .server);
        log.info("authenticated peer connected: {s}/{s}", .{
            hello.service_name,
            hello.instance_id,
        });
        return .{ .permissions = credential.permissions };
    }

    fn reject_authentication(
        self: *Server,
        conn: *transport.Connection,
        failure: anyerror,
    ) anyerror {
        _ = self.auth_failures.fetchAdd(1, .monotonic);
        conn.send_message(.{ .err = .{
            .code = .unauthenticated,
            .message = "authentication failed",
        } }) catch {};
        return failure;
    }

    fn authenticate_as_client(self: *Server, conn: *transport.Connection) !void {
        try auth.validate_secret(self.client_secret);
        var client_nonce: [auth.nonce_length]u8 = undefined;
        try self.io.randomSecure(&client_nonce);
        try conn.send_message(.{ .handshake = .{
            .kind = .client_hello,
            .service_name = self.identity.name,
            .instance_id = self.identity.instance_id,
            .nonce = client_nonce,
            .proof = auth.make_client_proof(
                self.client_secret,
                self.identity.name,
                self.identity.instance_id,
                client_nonce,
            ),
        } });
        const response = try conn.recv_message();
        const server = switch (response) {
            .handshake => |handshake| handshake,
            else => return error.AuthenticationFailed,
        };
        if (server.kind != .server_hello) return error.InvalidHandshake;
        if (!auth.verify_server_proof(
            self.client_secret,
            client_nonce,
            server.nonce,
            server.service_name,
            server.instance_id,
            server.proof,
        )) return error.AuthenticationFailed;
        const keys = auth.derive_session_keys(
            self.client_secret,
            client_nonce,
            server.nonce,
            self.identity.name,
            self.identity.instance_id,
            server.service_name,
            server.instance_id,
        );
        try conn.enable_security(keys, .client);
    }

    fn recv_message_bounded(
        self: *Server,
        connection: *transport.Connection,
        scratch: *ConnectionScratch,
    ) !protocol.Message {
        return self.recv_message_with_deadline(
            connection,
            scratch,
            connection_idle_deadline_ms,
        );
    }

    fn recv_message_with_deadline(
        self: *Server,
        connection: *transport.Connection,
        scratch: *ConnectionScratch,
        deadline_ms: u32,
    ) !protocol.Message {
        var select = Io.Select(ReceiveCompletion).init(
            self.io,
            &scratch.receive_completions,
        );
        defer select.cancelDiscard();
        try select.concurrent(.message, recv_message_task, .{connection});
        try select.concurrent(.deadline, connection_deadline, .{ self.io, deadline_ms });
        const completion = try select.await();
        switch (completion) {
            .message => |result| return try result,
            .deadline => |result| {
                try result;
                return error.ConnectionIdleDeadlineExceeded;
            },
        }
    }

    fn serve_pull_request(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        req: protocol.PullRequest,
    ) !void {
        const requested_count = if (req.max_count == 0)
            limits.pull_events_max
        else
            @min(req.max_count, limits.pull_events_max);
        self.store_mutex.lockUncancelable(self.io);
        const query = self.store.query_events_into(
            self.identity.name,
            req.event_type,
            req.from_sequence,
            scratch.query_events[0..requested_count],
            &scratch.query_bytes,
        ) catch |err| {
            self.store_mutex.unlock(self.io);
            return err;
        };
        self.store_mutex.unlock(self.io);
        const response_count = query.events.len;
        const entries = conn.response_entries()[0..response_count];
        const response_scratch = conn.response_scratch();
        var scratch_used: u32 = 0;
        var built: u16 = 0;

        for (query.events) |*ev| {
            const key = try store_mod.make_event_key_into(response_scratch[scratch_used..], .{
                .origin = ev.origin_service,
                .event_type = ev.event_type,
                .sequence = ev.sequence,
            });
            scratch_used += try limits.byte_count(key.len);
            const value = try ev.serialize_into(response_scratch[scratch_used..]);
            scratch_used += try limits.byte_count(value.len);

            entries[built] = .{ .key = key, .value = value };
            built += 1;
        }

        try conn.send_message(.{ .pull_response = .{ .events = entries[0..built] } });
    }

    fn serve_append_stream(
        self: *Server,
        conn: *transport.Connection,
        request: protocol.AppendStream,
    ) !void {
        const timestamp: limits.Timestamp = @intCast(time_mod.Instant.now(self.io).timestamp.sec);
        if (!try self.reserve_flush_room(conn)) return;
        const response = self.store.stage_append_stream(request, timestamp) catch |err| {
            self.record_store_error(err);
            self.store_mutex.unlock(self.io);
            try send_store_error(conn, err);
            return;
        };
        self.store_mutex.unlock(self.io);
        self.group_commit() catch |err| {
            try send_store_error(conn, err);
            return;
        };
        try conn.send_message(.{ .append_stream_response = response });
    }

    // NOTE: true holds `store_mutex` with room for a transaction; false has sent an error frame.
    fn reserve_flush_room(self: *Server, conn: *transport.Connection) !bool {
        while (true) {
            self.store_mutex.lockUncancelable(self.io);
            if (self.store.lsm.active_has_room()) return true;
            if (self.store.lsm.can_freeze()) {
                self.store.lsm.freeze() catch |err| {
                    self.store.streams.poisoned = true;
                    self.store_mutex.unlock(self.io);
                    try send_store_error(conn, err);
                    return false;
                };
                self.store_mutex.unlock(self.io);
                self.flush_mutex.lockUncancelable(self.io);
                self.flush_active_bg = true;
                self.flush_needed.broadcast(self.io);
                self.flush_mutex.unlock(self.io);
                continue;
            }
            self.store_mutex.unlock(self.io);
            self.flush_mutex.lockUncancelable(self.io);
            while (self.flush_active_bg and self.running.load(.acquire)) {
                self.flush_done.waitUncancelable(self.io, &self.flush_mutex);
            }
            const failed = self.flush_failed;
            self.flush_mutex.unlock(self.io);
            if (failed) {
                try send_store_error(conn, error.BackgroundFlushFailed);
                return false;
            }
        }
    }

    fn background_flush_loop(self: *Server) Io.Cancelable!void {
        while (self.running.load(.acquire)) {
            self.flush_mutex.lockUncancelable(self.io);
            while (!self.flush_active_bg and self.running.load(.acquire)) {
                self.flush_needed.waitUncancelable(self.io, &self.flush_mutex);
            }
            const active = self.flush_active_bg;
            self.flush_mutex.unlock(self.io);
            if (!self.running.load(.acquire)) return;
            if (!active) continue;

            self.run_background_flush() catch |err| {
                if (err == error.Canceled) return error.Canceled;
                log.err("background flush failed: {}", .{err});
                self.store_mutex.lockUncancelable(self.io);
                self.store.streams.poisoned = true;
                self.store_mutex.unlock(self.io);
                self.flush_mutex.lockUncancelable(self.io);
                self.flush_failed = true;
                self.flush_active_bg = false;
                self.flush_done.broadcast(self.io);
                self.flush_mutex.unlock(self.io);
                self.shutdown();
                return;
            };

            self.flush_mutex.lockUncancelable(self.io);
            self.flush_active_bg = false;
            self.flush_done.broadcast(self.io);
            self.flush_mutex.unlock(self.io);

            if (self.store.retention.len != 0) {
                self.store_mutex.lockUncancelable(self.io);
                _ = self.store.reclaim_expired(
                    @intCast(time_mod.Instant.now(self.io).timestamp.sec),
                ) catch |err| blk: {
                    log.warn("retention reclaim failed: {}", .{err});
                    break :blk @as(usize, 0);
                };
                self.store_mutex.unlock(self.io);
            }
        }
    }

    fn run_background_flush(self: *Server) !void {
        self.store_mutex.lockUncancelable(self.io);
        const pending = self.store.lsm.flush_pending_now();
        self.store_mutex.unlock(self.io);
        if (pending) {
            try self.store.lsm.flush_frozen_write();
            self.store_mutex.lockUncancelable(self.io);
            defer self.store_mutex.unlock(self.io);
            try self.store.lsm.flush_frozen_publish();
        }
        while (true) {
            self.store_mutex.lockUncancelable(self.io);
            const plan = self.store.lsm.compaction_prepare(0);
            self.store_mutex.unlock(self.io);
            const chosen = plan orelse break;
            const result = self.store.lsm.compaction_write(chosen) catch |err| {
                self.store_mutex.lockUncancelable(self.io);
                self.store.lsm.compaction_abandon();
                self.store_mutex.unlock(self.io);
                return err;
            };
            self.store_mutex.lockUncancelable(self.io);
            defer self.store_mutex.unlock(self.io);
            if (result) |value| {
                try self.store.lsm.compaction_publish(chosen, value);
            } else {
                self.store.lsm.compaction_abandon();
            }
        }
    }

    fn serve_bootstrap_request(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        request: protocol.BootstrapRequest,
    ) !void {
        const cap: usize = if (request.max_count == 0)
            limits.pull_events_max
        else
            @min(request.max_count, limits.pull_events_max);

        self.store_mutex.lockUncancelable(self.io);
        const scan = self.store.bootstrap_into(
            request.cursor,
            scratch.query_events[0..cap],
            &scratch.query_bytes,
            &scratch.bootstrap_cursor,
        ) catch |err| {
            self.store_mutex.unlock(self.io);
            self.record_store_error(err);
            switch (err) {
                error.InvalidCursor, error.NoIdentity => try conn.send_message(.{ .err = .{
                    .code = .invalid_message,
                    .message = "invalid bootstrap cursor",
                } }),
                else => try send_store_error(conn, err),
            }
            return;
        };
        const vc_entries = self.store.vc_summary();
        const vc_count = @min(vc_entries.len, scratch.vc_entries.len);
        @memcpy(scratch.vc_entries[0..vc_count], vc_entries[0..vc_count]);
        self.store_mutex.unlock(self.io);

        const wire = conn.response_scratch();
        var used: u32 = 0;
        var built: usize = 0;
        const budget: u32 = protocol.frame_payload_size_max - 512;
        while (built < scan.events.len) : (built += 1) {
            const size = scan.events[built].serialized_size();
            if (used + 4 + size > budget) break;
            std.mem.writeInt(u32, wire[used..][0..4], @intCast(size), .big);
            _ = scan.events[built].serialize_into(wire[used + 4 ..]) catch unreachable;
            used += 4 + @as(u32, @intCast(size));
        }

        var cursor_len = scan.next_cursor_len;
        var complete = scan.complete;
        if (built < scan.events.len) {
            if (built == 0) {
                try send_store_error(conn, error.ReadEventTooLarge);
                return;
            }
            complete = false;
            const resumed_key = store_mod.make_event_key_into(
                &scratch.bootstrap_cursor,
                .{
                    .origin = scan.events[built - 1].origin_service,
                    .event_type = scan.events[built - 1].event_type,
                    .sequence = scan.events[built - 1].sequence,
                },
            ) catch unreachable;
            cursor_len = resumed_key.len;
        }

        var entries: [limits.pull_events_max]protocol.BootstrapEvent = undefined;
        var offset: u32 = 0;
        for (entries[0..built]) |*entry| {
            const value_len = std.mem.readInt(u32, wire[offset..][0..4], .big);
            entry.* = .{ .value = wire[offset + 4 ..][0..value_len] };
            offset += 4 + value_len;
        }
        try conn.send_message(.{ .bootstrap_response = .{
            .events = entries[0..built],
            .next_cursor = scratch.bootstrap_cursor[0..cursor_len],
            .complete = complete,
            .vc = scratch.vc_entries[0..vc_count],
        } });
    }

    fn serve_read_stream(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        request: protocol.ReadStream,
    ) !void {
        self.store_mutex.lockUncancelable(self.io);
        const response = self.store.read_stream_into(
            request,
            &scratch.read_events,
            &scratch.query_bytes,
        ) catch |err| {
            self.record_store_error(err);
            self.store_mutex.unlock(self.io);
            try send_store_error(conn, err);
            return;
        };
        self.store_mutex.unlock(self.io);
        try conn.send_message(.{ .read_stream_response = response });
    }

    fn serve_read_log(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        request: protocol.ReadLog,
    ) !void {
        self.store_mutex.lockUncancelable(self.io);
        const response = self.store.read_log_into(
            request,
            &scratch.read_events,
            &scratch.query_bytes,
        ) catch |err| {
            self.record_store_error(err);
            self.store_mutex.unlock(self.io);
            try send_store_error(conn, err);
            return;
        };
        self.store_mutex.unlock(self.io);
        try conn.send_message(.{ .read_log_response = response });
    }

    /// Pushes the durable log, or one stream, from `request.from` until either side ends it.
    ///
    /// This task is the connection's only owner from here on: it never reads from `conn`
    /// again and is the only sender. Every page is read on demand by the store's owner into
    /// this connection's scratch, so nothing is queued per subscriber. A subscriber that reads
    /// slowly stalls only this task in `send`, and one that stops reading hits the send deadline.
    fn serve_subscription(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        request: protocol.Subscribe,
    ) !void {
        const subscriber = &scratch.subscriber;
        // NOTE: copies out of the receive frame, which every encrypted send overwrites.
        try subscriber.open(request.stream_id, request.event_types);
        defer subscriber.shut();
        _ = self.subscriptions_active.fetchAdd(1, .seq_cst);
        defer _ = self.subscriptions_active.fetchSub(1, .seq_cst);

        var cursor: u64 = request.from;
        // The durable position at which the last read found nothing past `cursor`. Until the
        // durable position moves beyond it, reading again could only find nothing again.
        var caught_up_at: ?protocol.LogPosition = null;
        var heartbeat: Heartbeat = .start(self.io);
        for (0..limits.subscription_rounds_max) |_| {
            if (!self.running.load(.acquire)) return;
            // NOTE: read before checking for work, so a notification after the check is kept.
            const epoch_seen = subscriber.epoch();
            const durable = self.store_owner.durable_position.load(.seq_cst);
            if (caught_up_at == null or durable > caught_up_at.?) {
                const page = try self.read_subscription_page(conn, scratch, cursor);
                assert(page.next >= cursor);
                if (page.events.len > 0) {
                    try self.send_with_deadline(conn, scratch, .{ .subscription_batch = .{
                        .next = page.next,
                        .committed_position = page.committed,
                        .events = page.events,
                    } });
                    heartbeat.sent = true;
                }
                // NOTE: skipped events move the cursor without a frame; the next batch or
                // heartbeat carries it.
                const idle = page.events.len == 0 and page.next == cursor;
                caught_up_at = if (idle) page.committed else null;
                cursor = page.next;
                continue;
            }
            if (heartbeat.due(self.io)) {
                if (!heartbeat.sent) try self.send_with_deadline(conn, scratch, .{
                    .subscription_batch = .{
                        .next = cursor,
                        .committed_position = durable,
                        .events = &.{},
                    },
                });
                heartbeat = .start(self.io);
                continue;
            }
            try subscriber.wait(self.io, epoch_seen, heartbeat.deadline);
        }
        return error.SubscriptionLimitExceeded;
    }

    fn read_subscription_page(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        cursor: u64,
    ) !StreamStore.SubscriptionPage {
        const target = &scratch.subscriber.target;
        var page: StreamStore.SubscriptionPage = undefined;
        self.submit_to_owner(.{ .subscription_page = .{
            .query = .{
                .stream_id = target.stream_id(),
                .from = cursor,
                .event_types = target.filter.event_types(),
            },
            .output = &scratch.read_events,
            .output_bytes = &scratch.query_bytes,
            .page = &page,
        } }) catch |err| {
            self.record_store_error(err);
            log.err("subscription read failed: {}", .{err});
            try self.send_with_deadline(conn, scratch, .{ .err = .{
                .code = .internal_error,
                .message = "stream operation failed",
            } });
            return err;
        };
        assert(page.events.len <= scratch.read_events.len);
        return page;
    }

    /// Sends one frame, or fails once `subscription_send_deadline_ms` passes first. A canceled
    /// send leaves the stream mid-frame, so the caller must close the connection on error.
    fn send_with_deadline(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        message: protocol.Message,
    ) !void {
        var select = Io.Select(SendCompletion).init(self.io, &scratch.send_completions);
        defer select.cancelDiscard();
        try select.concurrent(.sent, send_message_task, .{ conn, message });
        try select.concurrent(.deadline, connection_deadline, .{
            self.io,
            subscription_send_deadline_ms,
        });
        switch (try select.await()) {
            .sent => |result| return result,
            .deadline => |result| {
                try result;
                return error.SubscriberSendDeadlineExceeded;
            },
        }
    }

    fn send_message_task(connection: *transport.Connection, message: protocol.Message) anyerror!void {
        return connection.send_message(message);
    }

    fn submit_to_owner(self: *Server, payload: owner_mod.Command.Payload) !void {
        var outcome: owner_mod.Outcome = .{ .failed = error.Unset };
        var done: Io.Semaphore = .{ .permits = 0 };
        // NOTE: the queue holds one command per connection, so it cannot be full.
        self.store_owner.submit(.{ .payload = payload, .outcome = &outcome, .done = &done }) catch
            unreachable;
        switch (outcome) {
            .done => {},
            .failed => |err| return err,
        }
    }

    fn on_durable(context: *anyopaque, position: protocol.LogPosition) void {
        const self: *Server = @ptrCast(@alignCast(context));
        assert(position <= self.store_owner.durable_position.load(.seq_cst));
        self.notify_subscribers();
    }

    /// Wakes every live subscriber after a commit or on shutdown. A no-op without subscribers.
    ///
    /// NOTE: an awake subscriber is notified too. It may have checked the durable position
    /// already without being asleep yet; only the epoch bump keeps it from sleeping through
    /// this commit, and `notify` itself skips the syscall when nobody sleeps.
    fn notify_subscribers(self: *Server) void {
        if (self.subscriptions_active.load(.seq_cst) == 0) return;
        for (self.connection_pool) |*slot| {
            const subscriber = &slot.scratch.subscriber;
            if (subscriber.active.load(.acquire)) subscriber.notify(self.io);
        }
    }

    const Heartbeat = struct {
        deadline: Io.Clock.Timestamp,
        /// Whether a batch went out this period; if so the period's heartbeat is skipped, so
        /// the longest silence is two periods.
        sent: bool,

        fn start(io: Io) Heartbeat {
            return .{
                .deadline = .fromNow(io, .{
                    .raw = .fromMilliseconds(subscription_heartbeat_ms),
                    .clock = .awake,
                }),
                .sent = false,
            };
        }

        fn due(heartbeat: *const Heartbeat, io: Io) bool {
            const now = Io.Clock.Timestamp.now(io, heartbeat.deadline.clock);
            return now.raw.nanoseconds >= heartbeat.deadline.raw.nanoseconds;
        }
    };

    pub fn serve(self: *Server) !void {
        log.info("listening on {s}", .{self.identity.listen_address});

        try self.connection_group.concurrent(
            self.io,
            monitor_shutdown_signal,
            .{self},
        );
        try self.connection_group.concurrent(
            self.io,
            background_flush_loop,
            .{self},
        );
        // NOTE: never run this before accept: two peers booting together would deadlock.
        // Spawn at most once, because serve() can re-enter.
        if (!self.gossip_started.swap(true, .acq_rel)) {
            try self.connection_group.concurrent(
                self.io,
                gossip_loop,
                .{self},
            );
        }

        while (self.running.load(.acquire)) {
            if (shutdown_requested.load(.acquire)) break;
            if (self.connections_active.load(.acquire) == self.connection_pool.len) {
                _ = self.admission_saturation.fetchAdd(1, .monotonic);
            }
            try self.connection_slots.wait(self.io);
            const slot_index = self.acquire_connection_slot();
            const slot = &self.connection_pool[slot_index];

            self.listener.accept_into(&slot.connection) catch |err| {
                self.release_connection_slot(slot_index);
                if (self.running.load(.acquire)) {
                    if (shutdown_requested.load(.acquire)) break;
                } else {
                    break;
                }
                log.warn("accept error: {}", .{err});
                continue;
            };

            self.connection_group.concurrent(
                self.io,
                serve_connection_concurrent,
                .{ self, slot_index },
            ) catch |err| switch (err) {
                error.ConcurrencyUnavailable => serve_connection_concurrent(self, slot_index),
            };
        }

        if (shutdown_requested.load(.acquire)) arm_shutdown_deadline(self.io);
        self.running.store(false, .release);
        self.flush_mutex.lockUncancelable(self.io);
        self.flush_needed.broadcast(self.io);
        self.flush_mutex.unlock(self.io);
        self.connection_group.cancel(self.io);
    }

    pub fn shutdown(self: *Server) void {
        self.running.store(false, .release);
        self.notify_subscribers();
        self.listener.stop_accepting() catch |err| {
            log.err("failed to stop listener: {}", .{err});
        };
    }

    fn monitor_shutdown_signal(self: *Server) Io.Cancelable!void {
        while (self.running.load(.acquire)) {
            if (shutdown_requested.load(.acquire)) {
                arm_shutdown_deadline(self.io);
                self.shutdown();
                return;
            }
            try self.io.sleep(.fromMilliseconds(10), .awake);
        }
    }

    fn gossip_loop(self: *Server) Io.Cancelable!void {
        var first_pass = true;
        while (self.running.load(.acquire)) {
            self.gossipOnce();
            const delay_ms: u64 = if (first_pass)
                gossip_retry_first_ms
            else
                gossip_interval_ms;
            first_pass = false;
            var waited_ms: u64 = 0;
            while (waited_ms < delay_ms and self.running.load(.acquire)) {
                try self.io.sleep(.fromMilliseconds(gossip_tick_ms), .awake);
                waited_ms += gossip_tick_ms;
            }
        }
    }

    fn recv_message_task(connection: *transport.Connection) anyerror!protocol.Message {
        return connection.recv_message();
    }

    fn connection_deadline(io: Io, deadline_ms: u32) Io.Cancelable!void {
        try io.sleep(.fromMilliseconds(deadline_ms), .awake);
    }

    fn acquire_connection_slot(self: *Server) u16 {
        self.connection_pool_mutex.lockUncancelable(self.io);
        defer self.connection_pool_mutex.unlock(self.io);

        for (self.connection_pool, 0..) |*slot, index| {
            if (!slot.in_use) {
                slot.in_use = true;
                const previous = self.connections_active.fetchAdd(1, .release);
                std.debug.assert(previous < self.connection_pool.len);
                return @intCast(index);
            }
        }
        unreachable;
    }

    fn release_connection_slot(self: *Server, slot_index: u16) void {
        std.debug.assert(slot_index < self.connection_pool.len);
        self.connection_pool_mutex.lockUncancelable(self.io);
        std.debug.assert(self.connection_pool[slot_index].in_use);
        self.connection_pool[slot_index].in_use = false;
        const previous = self.connections_active.fetchSub(1, .release);
        std.debug.assert(previous > 0);
        self.connection_pool_mutex.unlock(self.io);
        self.connection_slots.post(self.io);
    }

    fn serve_connection_concurrent(self: *Server, slot_index: u16) void {
        const slot = &self.connection_pool[slot_index];
        const connection = &slot.connection;
        defer {
            connection.close();
            self.release_connection_slot(slot_index);
        }

        self.handle_connection(connection, &slot.scratch) catch |err| switch (err) {
            error.Canceled => std.debug.assert(!self.running.load(.acquire)),
            error.EndOfStream,
            error.MessageLimitExceeded,
            error.SubscriptionLimitExceeded,
            => {},
            else => log.warn("connection closed: {}", .{err}),
        };
    }

    fn group_commit(self: *Server) !void {
        self.group_commit_mutex.lockUncancelable(self.io);
        const generation = self.group_commit_generation;
        if (self.group_commit_leader) {
            while (generation == self.group_commit_generation) {
                self.group_commit_condition.waitUncancelable(
                    self.io,
                    &self.group_commit_mutex,
                );
            }
            const failed = self.group_commit_failed;
            self.group_commit_mutex.unlock(self.io);
            if (failed) return error.GroupCommitFailed;
            return;
        }

        self.group_commit_leader = true;
        self.group_commit_mutex.unlock(self.io);
        self.io.sleep(.fromMilliseconds(1), .awake) catch |err| switch (err) {
            error.Canceled => {},
        };

        self.group_commit_mutex.lockUncancelable(self.io);
        const failed = sync: {
            self.submit_to_owner(.durable_sync) catch break :sync true;
            break :sync false;
        };

        self.group_commit_failed = failed;
        self.group_commit_count += 1;
        self.group_commit_generation += 1;
        self.group_commit_leader = false;
        self.group_commit_condition.broadcast(self.io);
        self.group_commit_mutex.unlock(self.io);
        if (failed) return error.GroupCommitFailed;
    }

    pub fn health(self: *Server) protocol.HealthResponse {
        // NOTE: single node, so no quorum; these fields stay for wire compatibility.
        const clustered = false;
        const replication_quorum = false;
        const replication_poisoned = false;
        const replication_lag: u64 = 0;
        self.store_mutex.lockUncancelable(self.io);
        defer self.store_mutex.unlock(self.io);
        const metrics = self.store.metrics();
        const now = time_mod.Instant.now(self.io).timestamp.sec;
        const backup_age: u64 = if (self.last_backup_unix_seconds > 0 and
            now >= self.last_backup_unix_seconds)
            @intCast(now - self.last_backup_unix_seconds)
        else
            std.math.maxInt(u64);
        const locally_ready = self.integrity_verified and
            !metrics.poisoned and !replication_poisoned and
            metrics.streams_used < metrics.streams_max;
        const ready = locally_ready and (!clustered or replication_quorum);
        const production_ready = clustered and ready;
        return .{
            .integrity_verified = self.integrity_verified,
            .store_poisoned = metrics.poisoned or replication_poisoned,
            .ready = ready,
            .production_ready = production_ready,
            .degraded = !production_ready,
            .compaction_active = metrics.compaction_active,
            .replication_quorum = replication_quorum,
            .backup_in_progress = false,
            .streams_used = metrics.streams_used,
            .streams_max = metrics.streams_max,
            .stream_id_bytes_used = metrics.stream_id_bytes_used,
            .stream_id_bytes_max = metrics.stream_id_bytes_max,
            .connections_active = self.connections_active.load(.acquire),
            .connections_max = @intCast(self.connection_pool.len),
            .memtable_bytes = metrics.memtable_bytes,
            .memtable_entries = metrics.memtable_entries,
            .sstables = metrics.sstables,
            .wal_bytes = metrics.wal_bytes,
            .committed_log_position = metrics.committed_log_position,
            .auth_failures = self.auth_failures.load(.acquire),
            .permission_denials = self.permission_denials.load(.acquire),
            .admission_saturation = self.admission_saturation.load(.acquire),
            .corruption_findings = self.corruption_findings.load(.acquire),
            .compaction_failures = metrics.compaction_failures,
            .replication_failures = 0,
            .backups_completed = self.backups_completed,
            .last_backup_unix_seconds = self.last_backup_unix_seconds,
            .backup_age_seconds = backup_age,
            .replication_lag = replication_lag,
        };
    }

    // WARN: hold store_mutex so the finding and poison bit publish as one state change.
    fn record_store_error(self: *Server, store_error: anyerror) void {
        switch (store_error) {
            error.CorruptStreamData,
            error.WalChecksumMismatch,
            error.SSTableDataChecksumMismatch,
            error.SSTableIndexChecksumMismatch,
            error.SSTableFooterChecksumMismatch,
            error.ManifestChecksumMismatch,
            => {
                self.store.streams.poisoned = true;
                _ = self.corruption_findings.fetchAdd(1, .monotonic);
            },
            else => {},
        }
    }
};

fn message_permitted(permissions: auth.Permissions, message: protocol.Message) bool {
    return switch (message) {
        .append_stream => permissions.append,
        .read_stream => permissions.read_stream,
        .read_log => permissions.read_log,
        .subscribe => |request| if (request.stream_id.len == 0)
            permissions.read_log
        else
            permissions.read_stream,
        .pull_request, .bootstrap_request => permissions.recover,
        .health_request, .vc_summary_request => permissions.observe,
        .handshake,
        .pull_response,
        .err,
        .append_stream_response,
        .read_stream_response,
        .read_log_response,
        .health_response,
        .vc_summary_response,
        .bootstrap_response,
        .subscription_batch,
        => false,
    };
}

test "operation permissions are least privilege" {
    const append: protocol.Message = .{ .append_stream = .{
        .stream_id = "stream",
        .expected_version = .empty,
        .command_id = "command",
        .events = &.{},
    } };
    const read_stream: protocol.Message = .{ .read_stream = .{
        .stream_id = "stream",
        .from_version = 0,
        .max_count = 1,
    } };
    const read_log: protocol.Message = .{ .read_log = .{
        .from_position = 0,
        .max_count = 1,
    } };
    const recover: protocol.Message = .{ .pull_request = .{
        .event_type = "event",
        .from_sequence = 0,
        .max_count = 1,
    } };
    const observe: protocol.Message = .{ .health_request = .{} };
    const observe_vc: protocol.Message = .{ .vc_summary_request = .{} };
    const recover_bootstrap: protocol.Message = .{ .bootstrap_request = .{
        .cursor = "",
        .max_count = 1,
    } };
    const messages = [_]protocol.Message{
        append,
        read_stream,
        read_log,
        recover,
        observe,
        observe_vc,
        recover_bootstrap,
    };
    const grants = [_]auth.Permissions{
        .{ .append = true },
        .{ .read_stream = true },
        .{ .read_log = true },
        .{ .recover = true },
        .{ .observe = true },
        .{ .observe = true },
        .{ .recover = true },
    };
    for (messages, 0..) |message, operation| {
        for (grants) |grant| {
            const expected = switch (operation) {
                0 => grant.append,
                1 => grant.read_stream,
                2 => grant.read_log,
                3, 6 => grant.recover,
                4, 5 => grant.observe,
                else => unreachable,
            };
            try std.testing.expectEqual(expected, message_permitted(grant, message));
        }
    }
    try std.testing.expect(!message_permitted(.{
        .append = true,
        .read_stream = true,
        .read_log = true,
        .recover = true,
        .observe = true,
        .replicate = true,
    }, .{ .err = .{ .code = .internal_error, .message = "response" } }));
}

fn send_store_error(conn: *transport.Connection, store_error: anyerror) !void {
    log.err("stream operation failed: {}", .{store_error});
    try conn.send_message(.{ .err = .{
        .code = .internal_error,
        .message = "stream operation failed",
    } });
}

fn test_event_value(
    sequence: limits.Sequence,
    origin_service: []const u8,
    event_type: []const u8,
    payload: []const u8,
    timestamp: limits.Timestamp,
) !EventValue {
    return try EventValue.init(.{
        .sequence = sequence,
        .origin_service = origin_service,
        .event_type = event_type,
        .payload = payload,
        .timestamp = timestamp,
    });
}

test "group commit batches concurrent durability waits" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_group_commit.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_group_commit.crumb") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_group_commit.wal") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_group_commit.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_group_commit.wal", io, .{});
    defer event_store.deinit();
    const identity = ServiceIdentity{
        .name = "group-commit-test",
        .instance_id = "node-1",
        .listen_address = "127.0.0.1:0",
    };
    var server: Server = undefined;
    try Server.init(&server, allocator, &event_store, identity, &.{}, io);
    defer server.deinit();

    const Worker = struct {
        fn run(target: *Server) Io.Cancelable!void {
            target.group_commit() catch @panic("group commit failed");
        }
    };
    var group: Io.Group = .init;
    defer group.cancel(io);
    group.async(io, Worker.run, .{&server});
    group.async(io, Worker.run, .{&server});
    try group.await(io);

    try std.testing.expectEqual(@as(u64, 1), server.group_commit_count);
}

test "shutdown interrupts a blocked accept" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_server_shutdown.wal";
    Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer Io.Dir.cwd().deleteFile(io, wal_path) catch {};

    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();
    const identity: ServiceIdentity = .{
        .name = "shutdown-test",
        .instance_id = "node-1",
        .listen_address = "127.0.0.1:0",
    };
    var server: Server = undefined;
    try Server.init(&server, allocator, &event_store, identity, &.{}, io);
    defer server.deinit();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(target: *Server) void {
            target.serve() catch @panic("server failed during shutdown");
        }
    }.run, .{&server});
    try io.sleep(.fromMilliseconds(10), .awake);
    server.shutdown();
    server_thread.join();

    try std.testing.expect(!server.running.load(.acquire));
}

test "shutdown cancels an idle accepted connection" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_server_idle_shutdown.wal";
    Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer Io.Dir.cwd().deleteFile(io, wal_path) catch {};

    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();
    const identity: ServiceIdentity = .{
        .name = "idle-shutdown-test",
        .instance_id = "node-1",
        .listen_address = "127.0.0.1:0",
    };
    var server: Server = undefined;
    try Server.init(&server, allocator, &event_store, identity, &.{}, io);
    defer server.deinit();
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(target: *Server) void {
            target.serve() catch @panic("server failed during idle shutdown");
        }
    }.run, .{&server});

    var address_buffer: [32]u8 = undefined;
    const server_address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{server.listener.get_port()},
    );
    var client: transport.Connection = undefined;
    try transport.dial_into(&client, server_address, io);
    defer client.close();
    try client.send_message(.{ .handshake = .{
        .service_name = "idle-client",
        .instance_id = "client-1",
    } });
    try io.sleep(.fromMilliseconds(10), .awake);
    server.shutdown();
    server_thread.join();

    try std.testing.expect(!server.running.load(.acquire));
}

test "unauthenticated connection receives only the handshake deadline" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_handshake_deadline.wal";
    inline for (.{
        wal_path,
        "data/test_handshake_deadline.crumb",
        "data/test_handshake_deadline.manifest",
    }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    defer {
        inline for (.{
            wal_path,
            "data/test_handshake_deadline.crumb",
            "data/test_handshake_deadline.manifest",
        }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    }
    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();
    var srv: Server = undefined;
    try Server.init(&srv, allocator, &event_store, .{
        .name = "deadline-test",
        .instance_id = "node-1",
        .listen_address = "127.0.0.1:0",
    }, &.{}, io);
    defer srv.deinit();
    const Worker = struct {
        fn run(target: *Server) void {
            var connection: transport.Connection = undefined;
            target.listener.accept_into(&connection) catch @panic("accept failed");
            defer connection.close();
            var scratch: Server.ConnectionScratch = undefined;
            target.handle_connection(&connection, &scratch) catch |err| switch (err) {
                error.ConnectionIdleDeadlineExceeded => return,
                else => @panic(@errorName(err)),
            };
            @panic("idle unauthenticated connection did not time out");
        }
    };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&srv});
    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{srv.listener.get_port()},
    );
    var client: transport.Connection = undefined;
    try transport.dial_into(&client, address, io);
    defer client.close();
    thread.join();
}

test "connection saturation is bounded and operator visible" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_admission_saturation.wal";
    inline for (.{
        wal_path,
        "data/test_admission_saturation.crumb",
        "data/test_admission_saturation.manifest",
    }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    defer {
        inline for (.{
            wal_path,
            "data/test_admission_saturation.crumb",
            "data/test_admission_saturation.manifest",
        }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    }
    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();
    var srv: Server = undefined;
    try Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = .{
            .name = "admission-test",
            .instance_id = "node-1",
            .listen_address = "127.0.0.1:0",
        },
        .contracts = &.{},
        .io = io,
        .connection_capacity = 1,
    });
    defer srv.deinit();
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(target: *Server) void {
            target.serve() catch @panic("admission server failed");
        }
    }.run, .{&srv});
    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{srv.listener.get_port()},
    );
    var client: transport.Connection = undefined;
    try transport.dial_into(&client, address, io);
    var observed = false;
    for (0..100) |_| {
        const health_state = srv.health();
        if (health_state.connections_active == 1 and
            health_state.admission_saturation > 0)
        {
            observed = true;
            break;
        }
        try io.sleep(.fromMilliseconds(2), .awake);
    }
    try std.testing.expect(observed);
    client.close();
    srv.shutdown();
    server_thread.join();
    try std.testing.expectEqual(@as(u16, 0), srv.health().connections_active);
}

test "recovery ingests events from peer" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.crumb") catch {};

    var peer_listener = try transport.Listener.init("127.0.0.1:0", io);
    defer peer_listener.deinit();
    const peer_port = peer_listener.get_port();

    var peer_address_buffer: [32]u8 = undefined;
    const peer_address = try std.fmt.bufPrint(
        &peer_address_buffer,
        "127.0.0.1:{d}",
        .{peer_port},
    );
    var peer_connection: transport.Connection = undefined;

    const peer_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *transport.Listener, conn: *transport.Connection) void {
            l.accept_into(conn) catch return;
            defer conn.close();

            _ = conn.recv_message() catch return;

            _ = conn.recv_message() catch return;

            const entries = [_]protocol.PullResponse.EventEntry{
                .{
                    .key = "evt:order-service:order.created:0000000000000001",
                    .value = "1|100|2|order-service|order.created||{}",
                },
                .{
                    .key = "evt:order-service:order.created:0000000000000002",
                    .value = "2|200|2|order-service|order.created||{}",
                },
            };
            conn.send_message(.{ .pull_response = .{ .events = &entries } }) catch {};
        }
    }.run, .{ &peer_listener, &peer_connection });

    const event_types = [_][]const u8{"order.created"};
    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &event_types,
        .peer_address = peer_address,
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
    try event_store.put_breadcrumb(.{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 0,
        .peer_address = peer_address,
        .updated_at = 1,
    }, 0, "my-service", 1);

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };

    var srv: Server = undefined;
    try Server.init(&srv, allocator, &event_store, identity, &contracts, io);
    defer srv.deinit();

    try srv.run_recovery();

    peer_thread.join();

    var events: [limits.pull_events_max]EventValue = undefined;
    var bytes: [1024 * 1024]u8 = undefined;
    const results = try event_store.query_events_into(
        "order-service",
        "order.created",
        0,
        &events,
        &bytes,
    );
    try std.testing.expectEqual(@as(usize, 2), results.events.len);
    try std.testing.expectEqual(@as(u64, 1), results.events[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), results.events[1].sequence);
}

test "handle_connection serves PullRequest" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.crumb") catch {};

    var event_store = try EventStore.init(allocator, "data/test_serve_pull.wal", io, .{
        .identity = "my-service",
    });
    defer {
        event_store.deinit();
        Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.wal") catch {};
        Io.Dir.cwd().deleteFile(io, "data/test_serve_pull.crumb") catch {};
    }

    for (1..4) |i| {
        const payload = try std.fmt.allocPrint(allocator, "{{\"id\":{}}}", .{i});
        defer allocator.free(payload);
        const ev = try test_event_value(
            i,
            "my-service",
            "order.created",
            payload,
            @intCast(i * 100),
        );
        try event_store.ingest_event(ev);
    }

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };

    var srv: Server = undefined;
    try Server.init(&srv, allocator, &event_store, identity, &.{}, io);
    defer srv.deinit();

    const port = srv.listener.get_port();
    var accepted_connection: transport.Connection = undefined;
    var connection_scratch: Server.ConnectionScratch = undefined;

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(
            s: *Server,
            conn: *transport.Connection,
            scratch: *Server.ConnectionScratch,
        ) void {
            s.listener.accept_into(conn) catch return;
            defer conn.close();
            s.handle_connection(conn, scratch) catch |err| switch (err) {
                error.EndOfStream => {},
                else => @panic(@errorName(err)),
            };
        }
    }.run, .{ &srv, &accepted_connection, &connection_scratch });

    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(&address_buffer, "127.0.0.1:{d}", .{port});
    var client: transport.Connection = undefined;
    try transport.dial_into(&client, address, io);

    try client.send_message(.{ .handshake = .{
        .service_name = "test-client",
        .instance_id = "c-1",
    } });

    try client.send_message(.{ .pull_request = .{
        .event_type = "order.created",
        .from_sequence = 0,
        .max_count = 0,
    } });

    const response = try client.recv_message();

    try std.testing.expectEqual(@as(usize, 3), response.pull_response.events.len);

    const ev0 = try EventValue.deserialize_borrowed(response.pull_response.events[0].value);
    try std.testing.expectEqual(@as(u64, 1), ev0.sequence);
    try std.testing.expectEqualStrings("my-service", ev0.origin_service);

    client.close();
    server_thread.join();
}

fn serve_test_connections(
    server: *Server,
    connection: *transport.Connection,
    scratch: *Server.ConnectionScratch,
    connection_count: u8,
) void {
    std.debug.assert(connection_count > 0);
    for (0..connection_count) |_| {
        server.listener.accept_into(connection) catch return;
        server.handle_connection(connection, scratch) catch |err| switch (err) {
            error.EndOfStream, error.MessageLimitExceeded => {},
            else => @panic(@errorName(err)),
        };
        connection.close();
    }
}

fn serve_auth_test_connections(
    server: *Server,
    connection: *transport.Connection,
    scratch: *Server.ConnectionScratch,
    connection_count: u8,
) void {
    for (0..connection_count) |_| {
        server.listener.accept_into(connection) catch return;
        server.handle_connection(connection, scratch) catch |err| switch (err) {
            error.EndOfStream,
            error.UnknownService,
            error.AuthenticationFailed,
            error.ReplayedHandshake,
            => {},
            else => @panic(@errorName(err)),
        };
        connection.close();
    }
}

const AuthTestResponse = union(enum) {
    rejected: protocol.ErrorCode,
    accepted: struct {
        nonce: [auth.nonce_length]u8,
        proof: [auth.proof_length]u8,
    },
};

fn send_auth_test_hello(
    address: []const u8,
    io: Io,
    hello: protocol.Handshake,
) !AuthTestResponse {
    var connection: transport.Connection = undefined;
    try transport.dial_into(&connection, address, io);
    defer connection.close();
    try connection.send_message(.{ .handshake = hello });
    const response = try connection.recv_message();
    return switch (response) {
        .err => |server_error| .{ .rejected = server_error.code },
        .handshake => |server_hello| accepted: {
            if (!std.mem.eql(u8, server_hello.service_name, "compix-auth-test") or
                !std.mem.eql(u8, server_hello.instance_id, "node-1"))
                return error.InvalidHandshake;
            break :accepted .{ .accepted = .{
                .nonce = server_hello.nonce,
                .proof = server_hello.proof,
            } };
        },
        else => error.UnexpectedMessage,
    };
}

test "server rejects unknown wrong tampered and replayed authenticated hellos" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_server_auth.wal";
    Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_server_auth.crumb") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_server_auth.manifest") catch {};
    defer Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_server_auth.crumb") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_server_auth.manifest") catch {};
    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();
    const secret = "0123456789abcdef0123456789abcdef";
    const credentials = [_]auth.Credential{.{
        .service_name = "known-service",
        .secret = secret,
        .permissions = .{ .read_stream = true },
    }};
    var server: Server = undefined;
    try Server.init_in_place(&server, allocator, .{
        .store = &event_store,
        .identity = .{
            .name = "compix-auth-test",
            .instance_id = "node-1",
            .listen_address = "127.0.0.1:0",
        },
        .contracts = &.{},
        .io = io,
        .credentials = &credentials,
    });
    defer server.deinit();
    var accepted: transport.Connection = undefined;
    var scratch: Server.ConnectionScratch = undefined;
    const server_thread = try std.Thread.spawn(
        .{},
        serve_auth_test_connections,
        .{ &server, &accepted, &scratch, 5 },
    );
    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{server.listener.get_port()},
    );

    const unknown_nonce: [auth.nonce_length]u8 = @splat(1);
    const unknown = try send_auth_test_hello(address, io, .{
        .service_name = "unknown-service",
        .instance_id = "unknown-1",
        .nonce = unknown_nonce,
        .proof = auth.make_client_proof(
            secret,
            "unknown-service",
            "unknown-1",
            unknown_nonce,
        ),
    });
    try std.testing.expectEqual(protocol.ErrorCode.unauthenticated, unknown.rejected);

    const wrong_nonce: [auth.nonce_length]u8 = @splat(2);
    const wrong = try send_auth_test_hello(address, io, .{
        .service_name = "known-service",
        .instance_id = "wrong-1",
        .nonce = wrong_nonce,
        .proof = auth.make_client_proof(
            "abcdef0123456789abcdef0123456789",
            "known-service",
            "wrong-1",
            wrong_nonce,
        ),
    });
    try std.testing.expectEqual(protocol.ErrorCode.unauthenticated, wrong.rejected);

    const tampered_nonce: [auth.nonce_length]u8 = @splat(3);
    var tampered_proof = auth.make_client_proof(
        secret,
        "known-service",
        "tampered-1",
        tampered_nonce,
    );
    tampered_proof[0] ^= 1;
    const tampered = try send_auth_test_hello(address, io, .{
        .service_name = "known-service",
        .instance_id = "tampered-1",
        .nonce = tampered_nonce,
        .proof = tampered_proof,
    });
    try std.testing.expectEqual(protocol.ErrorCode.unauthenticated, tampered.rejected);

    const replay_nonce: [auth.nonce_length]u8 = @splat(4);
    const replay_hello: protocol.Handshake = .{
        .service_name = "known-service",
        .instance_id = "replay-1",
        .nonce = replay_nonce,
        .proof = auth.make_client_proof(
            secret,
            "known-service",
            "replay-1",
            replay_nonce,
        ),
    };
    const accepted_hello = try send_auth_test_hello(address, io, replay_hello);
    try std.testing.expect(auth.verify_server_proof(
        secret,
        replay_nonce,
        accepted_hello.accepted.nonce,
        "compix-auth-test",
        "node-1",
        accepted_hello.accepted.proof,
    ));
    const replayed = try send_auth_test_hello(address, io, replay_hello);
    try std.testing.expectEqual(protocol.ErrorCode.unauthenticated, replayed.rejected);
    server_thread.join();
}

const DriverTest = struct {
    const driver = @import("../driver.zig");

    fn exhaust_connection(client: *driver.Client, stream_id: []const u8) !void {
        var requests_sent: protocol.RequestCount = 3;
        while (requests_sent <= protocol.requests_per_connection_max) : (requests_sent += 1) {
            const reply = try client.read_stream(.{
                .stream_id = stream_id,
                .from_version = 0,
                .max_count = 1,
            });
            try std.testing.expectEqual(@as(usize, 1), reply.response.events.len);
        }
        try std.testing.expectEqual(@as(protocol.RequestCount, 1), client.request_count);
    }
};

test "network driver appends and reads committed streams" {
    const driver = DriverTest.driver;
    const client_secret = "0123456789abcdef0123456789abcdef";
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_driver_streams.wal";
    Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_driver_streams.crumb") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_driver_streams.manifest") catch {};
    defer Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_driver_streams.crumb") catch {};
    defer Io.Dir.cwd().deleteFile(io, "data/test_driver_streams.manifest") catch {};
    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();
    const identity: ServiceIdentity = .{
        .name = "compix-test",
        .instance_id = "node-1",
        .listen_address = "127.0.0.1:0",
    };
    const credentials = [_]auth.Credential{ .{
        .service_name = "meridian-watch",
        .secret = client_secret,
        .permissions = .{
            .append = true,
            .read_stream = true,
        },
    }, .{
        .service_name = "meridian-read-only",
        .secret = client_secret,
        .permissions = .{ .read_stream = true },
    }, .{
        .service_name = "compix-operator",
        .secret = client_secret,
        .permissions = .{ .observe = true },
    } };
    var srv: Server = undefined;
    try Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = identity,
        .contracts = &.{},
        .io = io,
        .credentials = &credentials,
    });
    defer srv.deinit();
    var accepted_connection: transport.Connection = undefined;
    var connection_scratch: Server.ConnectionScratch = undefined;
    const server_thread = try std.Thread.spawn(
        .{},
        serve_test_connections,
        .{ &srv, &accepted_connection, &connection_scratch, 4 },
    );
    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{srv.listener.get_port()},
    );
    var client: driver.Client = undefined;
    try driver.Client.connect(&client, .{
        .address = address,
        .service_name = "meridian-watch",
        .instance_id = "watch-1",
        .secret = client_secret,
        .io = io,
    });
    const events = [_]driver.AppendEvent{.{
        .event_type = "source.generation.requested",
        .schema_version = 1,
        .payload = "request",
    }};
    const append_request: driver.AppendRequest = .{
        .stream_id = "source-generation/network",
        .expected_version = .empty,
        .command_id = "network-command-1",
        .events = &events,
    };
    const append_reply = try client.append(append_request);
    try std.testing.expect(append_reply.response == .committed);
    const replay_reply = try client.append(append_request);
    try std.testing.expect(replay_reply.response == .replayed);
    const stream_reply = try client.read_stream(.{
        .stream_id = append_request.stream_id,
        .from_version = 0,
        .max_count = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), stream_reply.response.events.len);
    try std.testing.expectEqualStrings("request", stream_reply.response.events[0].payload);
    try DriverTest.exhaust_connection(&client, append_request.stream_id);
    client.deinit();

    var read_only_client: driver.Client = undefined;
    try driver.Client.connect(&read_only_client, .{
        .address = address,
        .service_name = "meridian-read-only",
        .instance_id = "reader-1",
        .secret = client_secret,
        .io = io,
    });
    const denied = try read_only_client.append(.{
        .stream_id = "source-generation/denied",
        .expected_version = .empty,
        .command_id = "denied-command",
        .events = &events,
    });
    try std.testing.expectEqual(protocol.ErrorCode.permission_denied, denied.rejected.code);
    const allowed = try read_only_client.read_stream(.{
        .stream_id = append_request.stream_id,
        .from_version = 0,
        .max_count = 1,
    });
    try std.testing.expectEqual(@as(usize, 1), allowed.response.events.len);
    const health_denied = try read_only_client.health();
    try std.testing.expectEqual(
        protocol.ErrorCode.permission_denied,
        health_denied.rejected.code,
    );
    read_only_client.deinit();

    var operator: driver.Client = undefined;
    try driver.Client.connect(&operator, .{
        .address = address,
        .service_name = "compix-operator",
        .instance_id = "operator-1",
        .secret = client_secret,
        .io = io,
    });
    const health_reply = try operator.health();
    try std.testing.expect(health_reply.response.integrity_verified);
    try std.testing.expect(health_reply.response.ready);
    try std.testing.expect(health_reply.response.degraded);
    try std.testing.expect(!health_reply.response.production_ready);
    try std.testing.expect(!health_reply.response.replication_quorum);
    try std.testing.expectEqual(@as(u64, 1), health_reply.response.committed_log_position);
    try std.testing.expectEqual(@as(u64, 2), health_reply.response.permission_denials);
    operator.deinit();
    server_thread.join();
}

test "vector-clock summary serves high-water marks over the wire" {
    const vc = @import("vc.zig");
    const driver = @import("../driver.zig");
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_vc_summary_wire.wal";
    Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    var event_store = try EventStore.init(allocator, wal_path, io, .{
        .identity = "compix-test",
        .vc_node_key = vc.nodeKey("node-1"),
        .contracts = &.{
            .{
                .source_service = "peer-service",
                .event_types = &[_][]const u8{"peer.event"},
                .peer_address = "127.0.0.1:1",
                .retention_hint = null,
            },
        },
    });
    defer event_store.deinit();
    const identity: ServiceIdentity = .{
        .name = "compix-test",
        .instance_id = "node-1",
        .listen_address = "127.0.0.1:0",
    };
    const client_secret = "0123456789abcdef0123456789abcdef";
    const credentials = [_]auth.Credential{ .{
        .service_name = "writer-observer",
        .secret = client_secret,
        .permissions = .{ .append = true, .observe = true },
    }, .{
        .service_name = "plain-reader",
        .secret = client_secret,
        .permissions = .{ .read_stream = true },
    } };
    var srv: Server = undefined;
    try Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = identity,
        .contracts = event_store.contracts,
        .io = io,
        .credentials = &credentials,
    });
    defer srv.deinit();
    var accepted_connection: transport.Connection = undefined;
    var connection_scratch: Server.ConnectionScratch = undefined;
    const server_thread = try std.Thread.spawn(
        .{},
        serve_test_connections,
        .{ &srv, &accepted_connection, &connection_scratch, 3 },
    );
    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{srv.listener.get_port()},
    );

    var client: driver.Client = undefined;
    try driver.Client.connect(&client, .{
        .address = address,
        .service_name = "writer-observer",
        .instance_id = "watch-1",
        .secret = client_secret,
        .io = io,
    });
    const events = [_]driver.AppendEvent{.{
        .event_type = "order.created",
        .schema_version = 1,
        .payload = "{}",
    }};
    const append_reply = try client.append(.{
        .stream_id = "orders/wire",
        .expected_version = .empty,
        .command_id = "wire-command-1",
        .events = &events,
    });
    try std.testing.expect(append_reply.response == .committed);

    const summary_reply = try client.vc_summary();
    try std.testing.expect(summary_reply == .response);
    try std.testing.expectEqual(@as(usize, 1), summary_reply.response.entries.len);
    try std.testing.expectEqual(vc.nodeKey("node-1"), summary_reply.response.entries[0].node);
    try std.testing.expectEqual(@as(u64, 1), summary_reply.response.entries[0].counter);
    client.deinit();

    var restricted: driver.Client = undefined;
    try driver.Client.connect(&restricted, .{
        .address = address,
        .service_name = "plain-reader",
        .instance_id = "reader-1",
        .secret = client_secret,
        .io = io,
    });
    const denied = try restricted.vc_summary();
    try std.testing.expectEqual(protocol.ErrorCode.permission_denied, denied.rejected.code);
    restricted.deinit();

    const remote = try EventValue.init(.{
        .sequence = 1,
        .origin_service = "peer-service",
        .event_type = "peer.event",
        .payload = "{}",
        .timestamp = 1,
        .vector_clock = "555=12",
    });
    try event_store.ingest_event(remote);

    var client_two: driver.Client = undefined;
    try driver.Client.connect(&client_two, .{
        .address = address,
        .service_name = "writer-observer",
        .instance_id = "watch-2",
        .secret = client_secret,
        .io = io,
    });
    const merged = try client_two.vc_summary();
    try std.testing.expect(merged == .response);
    try std.testing.expectEqual(@as(usize, 2), merged.response.entries.len);
    try std.testing.expectEqual(@as(u64, 12), merged.response.entries[0].counter);
    try std.testing.expectEqual(vc.nodeKey("node-1"), merged.response.entries[1].node);
    client_two.deinit();
    server_thread.join();
}

fn cleanup_bg_flush_e2e(io: Io) void {
    const singles = [_][]const u8{
        "data/test_bg_flush_e2e.wal",
        "data/test_bg_flush_e2e.wal.flushing",
        "data/test_bg_flush_e2e.crumb",
        "data/test_bg_flush_e2e.manifest",
    };
    for (singles) |p| Io.Dir.cwd().deleteFile(io, p) catch {};
    Io.Dir.cwd().deleteTree(io, "data/test_bg_flush_e2e.quarantine") catch {};
    for (0..4) |level| {
        for (1..128) |id| {
            var buffer: [64]u8 = undefined;
            const path = std.fmt.bufPrint(
                &buffer,
                "data/test_bg_flush_e2e_L{d}_{d:0>8}.sst",
                .{ level, id },
            ) catch continue;
            Io.Dir.cwd().deleteFile(io, path) catch {};
        }
    }
}

test "single-node appends drive background flushes and survive restart" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_bg_flush_e2e.wal";
    cleanup_bg_flush_e2e(io);
    defer cleanup_bg_flush_e2e(io);

    const append_count: u32 = 150;
    const events_per_append: u16 = 256;
    const total_events: u64 = @as(u64, append_count) * events_per_append;

    {
        var event_store = try EventStore.init(allocator, wal_path, io, .{});
        defer event_store.deinit();
        const identity: ServiceIdentity = .{
            .name = "bg-flush-test",
            .instance_id = "node-1",
            .listen_address = "127.0.0.1:0",
        };
        var server: Server = undefined;
        try Server.init(&server, allocator, &event_store, identity, &.{}, io);
        defer server.deinit();
        const server_thread = try std.Thread.spawn(.{}, struct {
            fn run(target: *Server) void {
                target.serve() catch @panic("server failed during background flush test");
            }
        }.run, .{&server});

        var address_buffer: [32]u8 = undefined;
        const address = try std.fmt.bufPrint(
            &address_buffer,
            "127.0.0.1:{d}",
            .{server.listener.get_port()},
        );
        var client: transport.Connection = undefined;
        try transport.dial_into(&client, address, io);
        try client.send_message(.{ .handshake = .{
            .service_name = "bg-flush-client",
            .instance_id = "client-1",
        } });

        var events: [events_per_append]protocol.AppendEvent = undefined;
        for (&events) |*slot| slot.* = .{ .event_type = "e", .schema_version = 1, .payload = "x" };

        var index: u32 = 0;
        while (index < append_count) : (index += 1) {
            var stream_buffer: [32]u8 = undefined;
            var command_buffer: [32]u8 = undefined;
            const stream_id = try std.fmt.bufPrint(&stream_buffer, "bg/{d}", .{index});
            const command_id = try std.fmt.bufPrint(&command_buffer, "cmd-{d}", .{index});
            try client.send_message(.{ .append_stream = .{
                .stream_id = stream_id,
                .expected_version = .empty,
                .command_id = command_id,
                .events = &events,
            } });
            const reply = try client.recv_message();
            try std.testing.expect(reply.append_stream_response == .committed);
        }

        try client.send_message(.{ .health_request = .{} });
        const health = try client.recv_message();
        try std.testing.expectEqual(total_events, health.health_response.committed_log_position);
        try std.testing.expect(health.health_response.sstables > 0);
        client.close();

        server.shutdown();
        server_thread.join();
    }

    var reopened = try EventStore.init(allocator, wal_path, io, .{});
    defer reopened.deinit();
    const verification = try reopened.verify_integrity();
    try std.testing.expectEqual(total_events, verification.events);
    try std.testing.expectEqual(@as(limits.StreamCount, append_count), verification.streams);
    try std.testing.expectEqual(total_events, verification.committed_log_position);
}

test "health readiness follows integrity poison and stream capacity" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_health_state.wal";
    inline for (.{
        wal_path,
        "data/test_health_state.crumb",
        "data/test_health_state.manifest",
    }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    defer {
        inline for (.{
            wal_path,
            "data/test_health_state.crumb",
            "data/test_health_state.manifest",
        }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    }

    var event_store = try EventStore.init(allocator, wal_path, io, .{
        .stream_capacity = 1,
        .stream_id_bytes_capacity = 128,
    });
    defer event_store.deinit();
    var srv: Server = undefined;
    try Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = .{
            .name = "health-test",
            .instance_id = "node-1",
            .listen_address = "127.0.0.1:0",
        },
        .contracts = &.{},
        .io = io,
    });
    defer srv.deinit();
    const initial = srv.health();
    try std.testing.expect(initial.ready);
    try std.testing.expectEqual(@as(limits.StreamCount, 0), initial.streams_used);
    const events = [_]protocol.AppendEvent{.{
        .event_type = "event",
        .schema_version = 1,
        .payload = "payload",
    }};
    _ = try event_store.append_stream(.{
        .stream_id = "only-stream",
        .expected_version = .empty,
        .command_id = "health-command",
        .events = &events,
    }, 1);
    const full = srv.health();
    try std.testing.expect(!full.ready);
    try std.testing.expectEqual(full.streams_max, full.streams_used);
    event_store.streams.poisoned = true;
    const poisoned = srv.health();
    try std.testing.expect(poisoned.store_poisoned);
    try std.testing.expect(!poisoned.ready);
}

test "recovery skips unreachable peers" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    Io.Dir.cwd().deleteFile(io, "data/test_recovery_skip.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_skip.crumb") catch {};

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

    var srv: Server = undefined;
    try Server.init(&srv, allocator, &event_store, identity, &contracts, io);
    defer srv.deinit();

    try srv.run_recovery();

    var events: [limits.pull_events_max]EventValue = undefined;
    var bytes: [1024 * 1024]u8 = undefined;
    const results = try event_store.query_events_into(
        "order-service",
        "order.created",
        0,
        &events,
        &bytes,
    );
    try std.testing.expectEqual(@as(usize, 0), results.events.len);
}

test "bootstrap streams every own event and clock over the wire" {
    const vc = @import("vc.zig");
    const driver = @import("../driver.zig");
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_bootstrap_wire.wal";
    Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    defer Io.Dir.cwd().deleteFile(io, wal_path) catch {};
    var event_store = try EventStore.init(allocator, wal_path, io, .{
        .identity = "compix-test",
        .vc_node_key = vc.nodeKey("node-1"),
        .contracts = &.{
            .{
                .source_service = "peer-service",
                .event_types = &[_][]const u8{"peer.event"},
                .peer_address = "127.0.0.1:1",
                .retention_hint = null,
            },
        },
    });
    defer event_store.deinit();

    const own_events = [_]protocol.AppendEvent{
        .{ .event_type = "alpha.event", .schema_version = 1, .payload = "one" },
        .{ .event_type = "beta.event", .schema_version = 1, .payload = "two" },
        .{ .event_type = "alpha.event", .schema_version = 1, .payload = "three" },
    };
    _ = try event_store.append_stream(.{
        .stream_id = "s/1",
        .expected_version = .empty,
        .command_id = "wire-boot-1",
        .events = own_events[0..2],
    }, 1);
    _ = try event_store.append_stream(.{
        .stream_id = "s/2",
        .expected_version = .empty,
        .command_id = "wire-boot-2",
        .events = own_events[2..3],
    }, 2);
    try event_store.ingest_event(try EventValue.init(.{
        .sequence = 9,
        .origin_service = "peer-service",
        .event_type = "peer.event",
        .payload = "foreign",
        .timestamp = 3,
        .vector_clock = "777=5",
    }));

    var srv: Server = undefined;
    const client_secret = "0123456789abcdef0123456789abcdef";
    const credentials = [_]auth.Credential{.{
        .service_name = "compix-test",
        .secret = client_secret,
        .permissions = .{ .append = true, .recover = true },
    }};
    try Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = .{
            .name = "compix-test",
            .instance_id = "node-1",
            .listen_address = "127.0.0.1:0",
        },
        .contracts = event_store.contracts,
        .io = io,
        .credentials = &credentials,
    });
    defer srv.deinit();
    var accepted_connection: transport.Connection = undefined;
    var connection_scratch: Server.ConnectionScratch = undefined;
    const server_thread = try std.Thread.spawn(
        .{},
        serve_test_connections,
        .{ &srv, &accepted_connection, &connection_scratch, 1 },
    );
    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{srv.listener.get_port()},
    );

    var client: driver.Client = undefined;
    try driver.Client.connect(&client, .{
        .address = address,
        .service_name = "compix-test",
        .instance_id = "node-1",
        .secret = "0123456789abcdef0123456789abcdef",
        .io = io,
    });

    var cursor: [limits.key_length_max]u8 = undefined;
    var cursor_len: usize = 0;
    var payloads: [8][]const u8 = undefined;
    var payload_count: usize = 0;
    var vc_saw_foreign = false;
    var pages: usize = 0;
    var complete = false;
    while (!complete) {
        pages += 1;
        try std.testing.expect(pages < 16);
        const reply = try client.bootstrap(.{
            .cursor = cursor[0..cursor_len],
            .max_count = 2,
        });
        const page = switch (reply) {
            .response => |value| value,
            .rejected => |server_error| {
                std.debug.print("bootstrap rejected: {}\n", .{server_error.code});
                return error.BootstrapRejected;
            },
        };
        for (page.events) |event| {
            const parsed = try EventValue.deserialize_borrowed(event.value);
            try std.testing.expectEqualStrings("compix-test", parsed.origin_service);
            try std.testing.expect(parsed.vector_clock.len > 0);
            payloads[payload_count] = try std.testing.allocator.dupe(
                u8,
                parsed.payload,
            );
            payload_count += 1;
        }
        if (page.vc.len > 0) {
            for (page.vc) |entry| {
                if (entry.node == 777) vc_saw_foreign = true;
            }
        }
        complete = page.complete;
        if (page.next_cursor.len > 0) {
            try std.testing.expect(page.next_cursor.len <= cursor.len);
            @memcpy(cursor[0..page.next_cursor.len], page.next_cursor);
            cursor_len = page.next_cursor.len;
        }
    }
    client.deinit();

    try std.testing.expectEqual(@as(usize, 3), payload_count);
    try std.testing.expectEqualStrings("one", payloads[0]);
    try std.testing.expectEqualStrings("three", payloads[1]);
    try std.testing.expectEqualStrings("two", payloads[2]);
    try std.testing.expect(vc_saw_foreign);
    for (payloads[0..payload_count]) |payload| std.testing.allocator.free(payload);
    try std.testing.expect(pages >= 2);

    server_thread.join();
}

test "fresh peer bootstraps full state and seeds breadcrumbs" {
    const vc = @import("vc.zig");
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    inline for (.{
        "data/test_recovery_bootstrap.wal",
        "data/test_recovery_bootstrap.crumb",
        "data/test_recovery_bootstrap.manifest",
    }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    defer {
        inline for (.{ "data/test_recovery_bootstrap.wal", "data/test_recovery_bootstrap.crumb", "data/test_recovery_bootstrap.manifest" }) |path| {
            Io.Dir.cwd().deleteFile(io, path) catch {};
        }
    }

    var peer_listener = try transport.Listener.init("127.0.0.1:0", io);
    defer peer_listener.deinit();
    var peer_address_buffer: [32]u8 = undefined;
    const peer_address = try std.fmt.bufPrint(
        &peer_address_buffer,
        "127.0.0.1:{d}",
        .{peer_listener.get_port()},
    );
    var peer_connection: transport.Connection = undefined;

    const peer_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *transport.Listener, conn: *transport.Connection) void {
            l.accept_into(conn) catch return;
            defer conn.close();
            _ = conn.recv_message() catch return;
            while (true) {
                const message = conn.recv_message() catch return;
                switch (message) {
                    .bootstrap_request => |request| {
                        if (request.cursor.len == 0) {
                            const entries = [_]protocol.BootstrapEvent{
                                .{ .value = "1|100|2|order-service|order.created||9=3|{}" },
                                .{ .value = "2|200|2|order-service|order.created||9=3|{}" },
                            };
                            conn.send_message(.{ .bootstrap_response = .{
                                .events = &entries,
                                .next_cursor = "evt:order-service:order.created:0000000000000002",
                                .complete = false,
                                .vc = &.{.{ .node = 9, .counter = 3 }},
                            } }) catch return;
                        } else {
                            conn.send_message(.{ .bootstrap_response = .{
                                .events = &.{},
                                .next_cursor = "",
                                .complete = true,
                                .vc = &.{},
                            } }) catch return;
                            return;
                        }
                    },
                    else => return,
                }
            }
        }
    }.run, .{ &peer_listener, &peer_connection });

    const event_types = [_][]const u8{"order.created"};
    const contracts = [_]DependencyContract{.{
        .source_service = "order-service",
        .event_types = &event_types,
        .peer_address = peer_address,
        .retention_hint = null,
    }};

    var event_store = try EventStore.init(allocator, "data/test_recovery_bootstrap.wal", io, .{
        .identity = "my-service",
        .contracts = &contracts,
        .vc_node_key = vc.nodeKey("self"),
    });
    defer event_store.deinit();

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };
    var srv: Server = undefined;
    try Server.init(&srv, allocator, &event_store, identity, &contracts, io);
    defer srv.deinit();

    try srv.run_recovery();
    peer_thread.join();

    var events: [limits.pull_events_max]EventValue = undefined;
    var bytes: [1024 * 1024]u8 = undefined;
    const results = try event_store.query_events_into(
        "order-service",
        "order.created",
        0,
        &events,
        &bytes,
    );
    try std.testing.expectEqual(@as(usize, 2), results.events.len);
    try std.testing.expectEqualStrings("9=3", results.events[0].vector_clock);
    try std.testing.expectEqual(@as(u64, 3), event_store.vc_clock.get(9).?);

    var crumb_buffer: [limits.key_length_max * 4]u8 = undefined;
    _ = &crumb_buffer;
    const tracked = try event_store.get_breadcrumb_sequence(
        "order-service",
        "order.created",
        peer_address,
        &crumb_buffer,
    );
    try std.testing.expectEqual(@as(u64, 2), tracked.?);
}

test "store-owner actor serves vector-clock summaries over the wire" {
    const vc = @import("vc.zig");
    const driver = @import("../driver.zig");
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_owner_wire.wal";
    inline for (.{
        wal_path,
        "data/test_owner_wire.crumb",
        "data/test_owner_wire.manifest",
    }) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    defer inline for (.{ wal_path, "data/test_owner_wire.crumb", "data/test_owner_wire.manifest" }) |path| {
        Io.Dir.cwd().deleteFile(io, path) catch {};
    };
    var event_store = try EventStore.init(allocator, wal_path, io, .{
        .identity = "compix-test",
        .vc_node_key = vc.nodeKey("node-1"),
    });
    defer event_store.deinit();

    const append_events = [_]protocol.AppendEvent{
        .{ .event_type = "order.created", .schema_version = 1, .payload = "{}" },
    };
    _ = try event_store.append_stream(.{
        .stream_id = "orders/owner",
        .expected_version = .empty,
        .command_id = "owner-wire-1",
        .events = &append_events,
    }, 100);

    var srv: Server = undefined;
    try Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = .{
            .name = "compix-test",
            .instance_id = "node-1",
            .listen_address = "127.0.0.1:0",
        },
        .contracts = &.{},
        .io = io,
    });
    defer srv.deinit();
    try std.testing.expect(srv.store_owner.thread != null);

    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{srv.listener.get_port()},
    );
    var accepted_connection: transport.Connection = undefined;
    var connection_scratch: Server.ConnectionScratch = undefined;
    const server_thread = try std.Thread.spawn(
        .{},
        serve_test_connections,
        .{ &srv, &accepted_connection, &connection_scratch, 1 },
    );

    var client: driver.Client = undefined;
    try driver.Client.connect(&client, .{
        .address = address,
        .service_name = "loopback",
        .instance_id = "probe-1",
        .secret = "0123456789abcdef0123456789abcdef",
        .io = io,
    });
    const summary_reply = try client.vc_summary();
    try std.testing.expect(summary_reply == .response);
    try std.testing.expectEqual(@as(usize, 1), summary_reply.response.entries.len);
    try std.testing.expectEqual(vc.nodeKey("node-1"), summary_reply.response.entries[0].node);
    try std.testing.expectEqual(@as(u64, 1), summary_reply.response.entries[0].counter);
    client.deinit();
    server_thread.join();
}

test "driver subscription pushes filtered log and stream batches and resumes" {
    const driver = DriverTest.driver;
    const client_secret = "0123456789abcdef0123456789abcdef";
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    const wal_path = "data/test_driver_subscription.wal";
    const paths = [_][]const u8{
        wal_path,
        "data/test_driver_subscription.crumb",
        "data/test_driver_subscription.manifest",
    };
    for (paths) |path| Io.Dir.cwd().deleteFile(io, path) catch {};
    defer for (paths) |path| Io.Dir.cwd().deleteFile(io, path) catch {};

    var event_store = try EventStore.init(allocator, wal_path, io, .{});
    defer event_store.deinit();
    const credentials = [_]auth.Credential{ .{
        .service_name = "meridian-writer",
        .secret = client_secret,
        .permissions = .{ .append = true },
    }, .{
        .service_name = "meridian-subscriber",
        .secret = client_secret,
        .permissions = .{ .read_log = true, .read_stream = true },
    } };
    var srv: Server = undefined;
    try Server.init_in_place(&srv, allocator, .{
        .store = &event_store,
        .identity = .{
            .name = "compix-test",
            .instance_id = "node-1",
            .listen_address = "127.0.0.1:0",
        },
        .contracts = &.{},
        .io = io,
        .credentials = &credentials,
    });
    defer srv.deinit();
    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(target: *Server) void {
            target.serve() catch @panic("server failed during subscription test");
        }
    }.run, .{&srv});
    defer server_thread.join();
    defer srv.shutdown();

    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{srv.listener.get_port()},
    );
    var writer: driver.Client = undefined;
    try driver.Client.connect(&writer, .{
        .address = address,
        .service_name = "meridian-writer",
        .instance_id = "writer-1",
        .secret = client_secret,
        .io = io,
    });
    defer writer.deinit();

    const first = [_]driver.AppendEvent{
        .{ .event_type = "object.created", .schema_version = 1, .payload = "one" },
        .{ .event_type = "noise", .schema_version = 1, .payload = "" },
        .{ .event_type = "object.created", .schema_version = 1, .payload = "three" },
    };
    const first_reply = try writer.append(.{
        .stream_id = "object/1",
        .expected_version = .empty,
        .command_id = "subscription-1",
        .events = &first,
    });
    try std.testing.expect(first_reply.response == .committed);

    var subscriber: driver.Client = undefined;
    try driver.Client.connect(&subscriber, .{
        .address = address,
        .service_name = "meridian-subscriber",
        .instance_id = "subscriber-1",
        .secret = client_secret,
        .io = io,
    });
    defer subscriber.deinit();
    var subscription: driver.Subscription = undefined;
    try subscription.start(&subscriber, .{
        .from = 0,
        .event_types = &.{"object.created"},
    });

    const backlog = (try subscription.next()).batch;
    try std.testing.expectEqual(@as(usize, 2), backlog.events.len);
    try std.testing.expectEqualStrings("one", backlog.events[0].payload);
    try std.testing.expectEqualStrings("three", backlog.events[1].payload);
    try std.testing.expectEqual(@as(u64, 3), backlog.events[1].log_position);
    try std.testing.expectEqual(@as(u64, 4), backlog.next);
    try std.testing.expectEqual(@as(u64, 3), backlog.committed_position);

    const quiet = (try subscription.next()).batch;
    try std.testing.expectEqual(@as(usize, 0), quiet.events.len);
    try std.testing.expectEqual(@as(u64, 4), quiet.next);
    try std.testing.expectEqual(@as(u64, 3), quiet.committed_position);

    const later = [_]driver.AppendEvent{
        .{ .event_type = "noise", .schema_version = 1, .payload = "" },
        .{ .event_type = "object.created", .schema_version = 1, .payload = "five" },
    };
    const later_reply = try writer.append(.{
        .stream_id = "object/1",
        .expected_version = .{ .exact = 3 },
        .command_id = "subscription-2",
        .events = &later,
    });
    try std.testing.expect(later_reply.response == .committed);
    const five = try next_subscription_events(&subscription);
    try std.testing.expectEqual(@as(usize, 1), five.events.len);
    try std.testing.expectEqualStrings("five", five.events[0].payload);
    try std.testing.expectEqual(@as(u64, 5), five.events[0].log_position);
    const resume_from = five.next;
    try std.testing.expectEqual(@as(u64, 6), resume_from);
    subscription.stop();

    const after = [_]driver.AppendEvent{
        .{ .event_type = "object.created", .schema_version = 1, .payload = "six" },
    };
    const after_reply = try writer.append(.{
        .stream_id = "object/1",
        .expected_version = .{ .exact = 5 },
        .command_id = "subscription-3",
        .events = &after,
    });
    try std.testing.expect(after_reply.response == .committed);

    try subscription.start(&subscriber, .{ .from = resume_from });
    const six = try next_subscription_events(&subscription);
    try std.testing.expectEqual(@as(usize, 1), six.events.len);
    try std.testing.expectEqualStrings("six", six.events[0].payload);
    try std.testing.expectEqual(@as(u64, 7), six.next);
    subscription.stop();

    try subscription.start(&subscriber, .{
        .stream_id = "object/1",
        .from = 4,
        .event_types = &.{"object.created"},
    });
    const stream = try next_subscription_events(&subscription);
    try std.testing.expectEqual(@as(usize, 2), stream.events.len);
    try std.testing.expectEqualStrings("five", stream.events[0].payload);
    try std.testing.expectEqual(@as(u64, 5), stream.events[0].stream_version);
    try std.testing.expectEqualStrings("six", stream.events[1].payload);
    try std.testing.expectEqual(@as(u64, 7), stream.next);
    subscription.stop();

    inline for (.{ "", "object/1" }) |stream_id| {
        var denied: driver.Subscription = undefined;
        try denied.start(&writer, .{ .stream_id = stream_id, .from = 0 });
        const rejected = try denied.next();
        try std.testing.expectEqual(protocol.ErrorCode.permission_denied, rejected.rejected.code);
    }
}

/// Skips heartbeats, which a slow test machine may interleave before any live batch.
fn next_subscription_events(
    subscription: *DriverTest.driver.Subscription,
) !DriverTest.driver.SubscriptionBatch {
    for (0..64) |_| {
        const batch = (try subscription.next()).batch;
        if (batch.events.len > 0) return batch;
    }
    return error.TestExpectedEvents;
}
