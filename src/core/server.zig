const std = @import("std");
const builtin = @import("builtin");
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
const config_mod = @import("config.zig");
const raft = @import("raft.zig");
const replication_mod = @import("replication.zig");
const snapshot_storage = @import("snapshot_storage.zig");

const EventStore = store_mod.EventStore;
const EventValue = event_mod.EventValue;
const DependencyContract = contract_mod.DependencyContract;
const ServiceIdentity = identity_mod.ServiceIdentity;
const Breadcrumb = breadcrumb_mod.Breadcrumb;

const log = std.log.scoped(.server);
const connection_idle_deadline_ms: u32 = 5 * 60 * 1000;
const handshake_deadline_ms: u32 = if (builtin.is_test) 50 else 3_000;
const raft_dispatch_capacity: u16 = 256;

var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);

comptime {
    std.debug.assert(connection_idle_deadline_ms > 0);
    std.debug.assert(connection_idle_deadline_ms < std.math.maxInt(u32));
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
    replay_entries: []auth.ReplayEntry,
    replay_mutex: Io.Mutex,
    recovery_connection: *transport.Connection,
    recovery_tasks: []recovery_mod.RecoveryTask,
    recovery_scratch: []u8,
    integrity_verified: bool,
    connections_active: std.atomic.Value(u16),
    auth_failures: std.atomic.Value(u64),
    permission_denials: std.atomic.Value(u64),
    admission_saturation: std.atomic.Value(u64),
    corruption_findings: std.atomic.Value(u64),
    backups_completed: u64,
    last_backup_unix_seconds: i64,
    replication: ?*replication_mod.Runtime,
    replication_mutex: Io.Mutex,
    replication_rpc_mutex: Io.Mutex,
    replication_connection: ?*transport.Connection,
    replication_command_scratch: []u8,
    replication_failures: std.atomic.Value(u64),

    const ConnectionSlot = struct {
        connection: transport.Connection,
        scratch: ConnectionScratch,
        in_use: bool,
    };

    const ConnectionScratch = struct {
        query_events: [limits.pull_events_max]EventValue,
        read_events: [limits.read_events_max]protocol.ReadEvent,
        query_bytes: [protocol.frame_size_max]u8,
        receive_completions: [2]ReceiveCompletion,
    };

    const ReceiveCompletion = union(enum) {
        message: anyerror!protocol.Message,
        deadline: Io.Cancelable!void,
    };

    const AuthenticatedSession = struct {
        permissions: ?auth.Permissions,
        raft_node: ?raft.NodeId,
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
        cluster: ?config_mod.Cluster = null,
    };

    pub fn init(
        allocator: Allocator,
        store: *EventStore,
        identity: ServiceIdentity,
        contracts: []const DependencyContract,
        io: Io,
    ) !Server {
        var target: Server = undefined;
        try init_in_place(&target, allocator, .{
            .store = store,
            .identity = identity,
            .contracts = contracts,
            .io = io,
        });
        return target;
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
        for (connection_pool) |*slot| slot.in_use = false;

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

        var replication_runtime: ?*replication_mod.Runtime = null;
        var replication_connection: ?*transport.Connection = null;
        var replication_command_scratch: []u8 = &.{};
        if (options.cluster) |cluster| {
            const runtime = try allocator.create(replication_mod.Runtime);
            errdefer allocator.destroy(runtime);
            var seed_bytes: [8]u8 = undefined;
            try options.io.randomSecure(&seed_bytes);
            runtime.* = try replication_mod.Runtime.init(
                allocator,
                options.io,
                cluster,
                options.store,
                std.mem.readInt(u64, &seed_bytes, .little),
            );
            errdefer runtime.deinit();
            const outbound = try allocator.create(transport.Connection);
            errdefer allocator.destroy(outbound);
            const command_scratch = try allocator.alloc(
                u8,
                @max(cluster.command_slot_size, limits.snapshot_chunk_size_max),
            );
            errdefer allocator.free(command_scratch);
            replication_runtime = runtime;
            replication_connection = outbound;
            replication_command_scratch = command_scratch;
        }

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
            .replay_entries = replay_entries,
            .replay_mutex = .init,
            .recovery_connection = recovery_connection,
            .recovery_tasks = recovery_tasks,
            .recovery_scratch = recovery_scratch,
            .integrity_verified = options.integrity_verified,
            .connections_active = .init(0),
            .auth_failures = .init(0),
            .permission_denials = .init(0),
            .admission_saturation = .init(0),
            .corruption_findings = .init(@as(u64, @intCast(
                options.store.metrics().startup_quarantined_files,
            )) + if (replication_runtime) |runtime| runtime.recovery_findings else 0),
            .backups_completed = options.backups_completed,
            .last_backup_unix_seconds = options.last_backup_unix_seconds,
            .replication = replication_runtime,
            .replication_mutex = .init,
            .replication_rpc_mutex = .init,
            .replication_connection = replication_connection,
            .replication_command_scratch = replication_command_scratch,
            .replication_failures = .init(0),
        };
    }

    pub fn deinit(self: *Server) void {
        self.connection_group.cancel(self.io);
        for (self.connection_pool) |slot| {
            std.debug.assert(!slot.in_use);
        }
        self.allocator.free(self.connection_pool);
        self.allocator.destroy(self.recovery_connection);
        self.allocator.free(self.recovery_tasks);
        self.allocator.free(self.recovery_scratch);
        self.allocator.free(self.replay_entries);
        if (self.replication) |runtime| {
            runtime.deinit();
            self.allocator.destroy(runtime);
            self.allocator.destroy(self.replication_connection.?);
            self.allocator.free(self.replication_command_scratch);
        }
        self.listener.deinit();
    }

    /// Run startup recovery: compute recovery plan from breadcrumbs,
    /// connect to each peer, send PullRequest, ingest responses,
    /// update breadcrumbs.
    pub fn run_recovery(self: *Server) !void {
        var task_count: u32 = 0;
        for (self.contracts) |contract| {
            for (contract.event_types) |event_type| {
                std.debug.assert(task_count < self.recovery_tasks.len);
                const last_sequence = try self.store.get_breadcrumb_sequence(
                    contract.source_service,
                    event_type,
                    self.recovery_scratch,
                );
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
                };
                task_count += 1;
            }
        }

        if (task_count == 0) {
            log.info("recovery: no tasks to run", .{});
            return;
        }

        var total_ingested: u32 = 0;
        var errors_skipped: u32 = 0;

        for (self.recovery_tasks[0..task_count]) |task| {
            self.execute_recovery_task(task, &total_ingested) catch {
                errors_skipped += 1;
                continue;
            };
        }

        log.info("recovery complete: {d} events ingested, {d} tasks skipped", .{
            total_ingested,
            errors_skipped,
        });
    }

    fn execute_recovery_task(
        self: *Server,
        task: recovery_mod.RecoveryTask,
        total_ingested: *u32,
    ) !void {
        try transport.dial_into(
            self.recovery_connection,
            task.peer_address,
            self.io,
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

                    self.store.ingest_event(ev) catch |err| {
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
            if (is_raft_request(msg) and session.raft_node == null) {
                _ = self.permission_denials.fetchAdd(1, .monotonic);
                try conn.send_message(.{ .err = .{
                    .code = .permission_denied,
                    .message = "authenticated cluster membership required",
                } });
                continue;
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
                .raft_request_vote, .raft_append_entries, .raft_install_snapshot => {
                    try self.serve_raft_request(conn, session.raft_node.?, msg);
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
            return .{ .permissions = null, .raft_node = null };
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
        const raft_node = if (self.replication) |runtime|
            runtime.node_for_authenticated_instance(
                hello.service_name,
                hello.instance_id,
                credential.permissions,
                self.identity.name,
            )
        else
            null;
        return .{ .permissions = credential.permissions, .raft_node = raft_node };
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
        if (self.replication != null) {
            try self.serve_replicated_append(conn, request);
            return;
        }
        const timestamp: limits.Timestamp = @intCast(time_mod.Instant.now(self.io).timestamp.sec);
        self.store_mutex.lockUncancelable(self.io);
        const response = self.store.append_stream(request, timestamp) catch |err| {
            self.record_store_error(err);
            self.store_mutex.unlock(self.io);
            try send_store_error(conn, err);
            return;
        };
        self.store_mutex.unlock(self.io);
        try conn.send_message(.{ .append_stream_response = response });
    }

    fn serve_replicated_append(
        self: *Server,
        conn: *transport.Connection,
        request: protocol.AppendStream,
    ) !void {
        var initial: [replication_mod.outbox_capacity]raft.Envelope = undefined;
        self.replication_mutex.lockUncancelable(self.io);
        const runtime = self.replication.?;
        runtime.outbox.reset();
        const timestamp: limits.Timestamp = @intCast(time_mod.Instant.now(self.io).timestamp.sec);
        const index = runtime.machine.propose_append(
            request,
            timestamp,
            &runtime.outbox,
        ) catch |err| {
            self.replication_mutex.unlock(self.io);
            switch (err) {
                error.NotLeader => try send_protocol_error(conn, .not_leader, "not leader"),
                error.RaftLogFull => try send_protocol_error(
                    conn,
                    .replication_unavailable,
                    "replication log capacity exhausted",
                ),
                else => {
                    self.record_replication_failure();
                    try send_protocol_error(
                        conn,
                        .replication_unavailable,
                        "replication proposal failed",
                    );
                },
            }
            return;
        };
        self.store_mutex.lockUncancelable(self.io);
        runtime.checkpoint_and_apply() catch |err| {
            self.store_mutex.unlock(self.io);
            self.replication_mutex.unlock(self.io);
            if (err == error.Canceled) return error.Canceled;
            runtime.machine.poisoned = true;
            self.record_replication_failure();
            log.err("failed to persist replicated proposal: {}", .{err});
            try send_protocol_error(
                conn,
                .replication_unavailable,
                "replication persistence failed",
            );
            return;
        };
        self.store_mutex.unlock(self.io);
        const initial_count = runtime.take_outbox(&initial) catch |err| {
            self.replication_mutex.unlock(self.io);
            return err;
        };
        self.replication_mutex.unlock(self.io);

        self.dispatch_raft(initial[0..initial_count]) catch |err| {
            self.record_replication_failure();
            log.warn("replication dispatch failed: {}", .{err});
            try send_protocol_error(
                conn,
                .replication_unavailable,
                "replication dispatch failed",
            );
            return;
        };

        self.replication_mutex.lockUncancelable(self.io);
        const response = runtime.machine.result(index);
        const role = runtime.machine.node.role;
        const poisoned = runtime.machine.poisoned;
        self.replication_mutex.unlock(self.io);
        if (response) |committed| {
            try conn.send_message(.{ .append_stream_response = committed });
        } else if (poisoned) {
            try send_protocol_error(
                conn,
                .replication_unavailable,
                "replication state is poisoned",
            );
        } else if (role != .leader) {
            try send_protocol_error(conn, .not_leader, "leadership changed before commit");
        } else {
            try send_protocol_error(conn, .quorum_unavailable, "quorum did not commit append");
        }
    }

    /// Returns true after sending a clustered-read rejection. Single-node reads
    /// return false immediately; clustered reads require a fresh leader quorum
    /// barrier before touching the canonical store.
    fn ensure_linearizable_read(
        self: *Server,
        conn: *transport.Connection,
    ) !bool {
        if (self.replication == null) return false;
        var initial: [replication_mod.outbox_capacity]raft.Envelope = undefined;
        self.replication_mutex.lockUncancelable(self.io);
        const runtime = self.replication.?;
        if (runtime.machine.poisoned) {
            self.replication_mutex.unlock(self.io);
            try send_protocol_error(
                conn,
                .replication_unavailable,
                "replication state is poisoned",
            );
            return true;
        }
        runtime.outbox.reset();
        runtime.machine.node.begin_linearizable_read(&runtime.outbox) catch |err| {
            self.replication_mutex.unlock(self.io);
            if (err == error.NotLeader) {
                try send_protocol_error(conn, .not_leader, "not leader");
                return true;
            }
            return err;
        };
        const count = try runtime.take_outbox(&initial);
        self.replication_mutex.unlock(self.io);
        self.dispatch_raft(initial[0..count]) catch |err| {
            self.record_replication_failure();
            log.warn("linearizable read barrier failed: {}", .{err});
            try send_protocol_error(
                conn,
                .replication_unavailable,
                "read barrier dispatch failed",
            );
            return true;
        };
        self.replication_mutex.lockUncancelable(self.io);
        const ready = runtime.machine.node.linearizable_read_ready() and
            !runtime.machine.poisoned;
        const role = runtime.machine.node.role;
        self.replication_mutex.unlock(self.io);
        if (ready) return false;
        if (role != .leader) {
            try send_protocol_error(conn, .not_leader, "leadership changed during read barrier");
        } else {
            try send_protocol_error(conn, .quorum_unavailable, "read quorum unavailable");
        }
        return true;
    }

    fn serve_raft_request(
        self: *Server,
        conn: *transport.Connection,
        authenticated_node: raft.NodeId,
        message: protocol.Message,
    ) !void {
        if (message == .raft_install_snapshot)
            return self.serve_raft_snapshot_chunk(
                conn,
                authenticated_node,
                message.raft_install_snapshot,
            );
        const incoming = try raft_request_from_protocol(
            message,
            authenticated_node,
            self.replication.?.config.node_id,
        );
        var responses: [replication_mod.outbox_capacity]raft.Envelope = undefined;
        self.replication_mutex.lockUncancelable(self.io);
        const runtime = self.replication.?;
        runtime.outbox.reset();
        runtime.machine.receive(
            incoming.envelope,
            incoming.command,
            &runtime.outbox,
        ) catch |err| {
            self.replication_mutex.unlock(self.io);
            self.record_replication_failure();
            log.warn("rejected Raft request from node {d}: {}", .{
                authenticated_node,
                err,
            });
            try send_protocol_error(conn, .replication_unavailable, "invalid Raft request");
            return;
        };
        self.store_mutex.lockUncancelable(self.io);
        runtime.checkpoint_and_apply() catch |err| {
            self.store_mutex.unlock(self.io);
            self.replication_mutex.unlock(self.io);
            if (err == error.Canceled) return error.Canceled;
            runtime.machine.poisoned = true;
            self.record_replication_failure();
            log.err("failed to persist Raft request from node {d}: {}", .{
                authenticated_node,
                err,
            });
            try send_protocol_error(
                conn,
                .replication_unavailable,
                "replication persistence failed",
            );
            return;
        };
        self.store_mutex.unlock(self.io);
        const response_count = runtime.take_outbox(&responses) catch |err| {
            self.replication_mutex.unlock(self.io);
            return err;
        };
        self.replication_mutex.unlock(self.io);
        if (response_count != 1) return error.InvalidRaftResponseCount;
        const response = responses[0];
        if (response.from != runtime.config.node_id or response.to != authenticated_node)
            return error.InvalidRaftEnvelope;
        try conn.send_message(try raft_response_to_protocol(response));
    }

    fn serve_raft_snapshot_chunk(
        self: *Server,
        conn: *transport.Connection,
        authenticated_node: raft.NodeId,
        install: protocol.RaftInstallSnapshot,
    ) !void {
        self.replication_mutex.lockUncancelable(self.io);
        const runtime = self.replication.?;
        self.store_mutex.lockUncancelable(self.io);
        const progress = runtime.receive_snapshot_chunk(
            authenticated_node,
            install,
        ) catch |err| {
            self.store_mutex.unlock(self.io);
            self.replication_mutex.unlock(self.io);
            if (err == error.Canceled) return error.Canceled;
            runtime.machine.poisoned = true;
            self.record_replication_failure();
            log.err("snapshot chunk from node {d} failed: {}", .{
                authenticated_node,
                err,
            });
            try send_protocol_error(
                conn,
                .replication_unavailable,
                "snapshot installation failed",
            );
            return;
        };
        self.store_mutex.unlock(self.io);
        self.replication_mutex.unlock(self.io);
        try conn.send_message(.{ .raft_snapshot_result = .{
            .value = progress.result,
            .next_offset = progress.next_offset,
            .accepted = progress.accepted,
        } });
    }

    fn dispatch_raft(self: *Server, initial: []const raft.Envelope) !void {
        if (initial.len == 0) return;
        if (initial.len > raft_dispatch_capacity) return error.RaftDispatchQueueFull;
        var pending: [raft_dispatch_capacity]raft.Envelope = undefined;
        @memcpy(pending[0..initial.len], initial);
        var pending_count: usize = initial.len;
        var cursor: usize = 0;

        self.replication_rpc_mutex.lockUncancelable(self.io);
        defer self.replication_rpc_mutex.unlock(self.io);
        while (cursor < pending_count) : (cursor += 1) {
            const request = pending[cursor];
            const response_message = self.send_raft_rpc(request) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    _ = self.replication_failures.fetchAdd(1, .monotonic);
                    log.warn("Raft RPC {d}->{d} failed: {}", .{
                        request.from,
                        request.to,
                        err,
                    });
                    continue;
                },
            };

            self.replication_mutex.lockUncancelable(self.io);
            const runtime = self.replication.?;
            runtime.outbox.reset();
            runtime.machine.receive(.{
                .from = request.to,
                .to = request.from,
                .message = response_message,
            }, null, &runtime.outbox) catch |err| {
                self.replication_mutex.unlock(self.io);
                return err;
            };
            self.store_mutex.lockUncancelable(self.io);
            runtime.checkpoint_and_apply() catch |err| {
                runtime.machine.poisoned = true;
                self.store_mutex.unlock(self.io);
                self.replication_mutex.unlock(self.io);
                return err;
            };
            self.store_mutex.unlock(self.io);
            const appended = runtime.take_outbox(pending[pending_count..]) catch |err| {
                self.replication_mutex.unlock(self.io);
                return err;
            };
            pending_count += appended;
            self.replication_mutex.unlock(self.io);
        }
    }

    fn send_raft_rpc(self: *Server, request: raft.Envelope) !raft.Message {
        const runtime = self.replication.?;
        if (request.from != runtime.config.node_id or request.to == runtime.config.node_id)
            return error.InvalidRaftEnvelope;
        const peer = runtime.member(request.to) orelse return error.UnknownRaftPeer;
        if (request.message == .install_snapshot)
            return self.send_snapshot_rpc(request, peer);
        var command: []const u8 = "";
        self.replication_mutex.lockUncancelable(self.io);
        if (runtime.machine.command_for(request)) |stored| {
            if (stored.len > self.replication_command_scratch.len) {
                self.replication_mutex.unlock(self.io);
                return error.ReplicatedCommandTooLarge;
            }
            @memcpy(self.replication_command_scratch[0..stored.len], stored);
            command = self.replication_command_scratch[0..stored.len];
        }
        self.replication_mutex.unlock(self.io);

        const conn = self.replication_connection.?;
        try transport.dial_into_timeout(
            conn,
            peer.address,
            self.io,
            runtime.config.rpc_timeout_ms,
        );
        defer conn.close();
        try self.authenticate_cluster_peer(conn, peer);
        try conn.send_message(try raft_request_to_protocol(request, command));
        const response = try self.recv_replication_message(
            conn,
            runtime.config.rpc_timeout_ms,
        );
        if (response == .err) return error.RaftPeerRejected;
        const raft_response = try raft_response_from_protocol(response);
        if (!raft_response_matches(request.message, raft_response))
            return error.UnexpectedRaftResponse;
        return raft_response;
    }

    fn send_snapshot_rpc(
        self: *Server,
        request: raft.Envelope,
        peer: config_mod.ClusterMember,
    ) !raft.Message {
        const install = request.message.install_snapshot;
        var offset: u64 = 0;
        var final_result: ?raft.SnapshotResult = null;
        while (offset < install.snapshot.state_size) {
            const conn = self.replication_connection.?;
            try transport.dial_into_timeout(
                conn,
                peer.address,
                self.io,
                self.replication.?.config.rpc_timeout_ms,
            );
            defer conn.close();
            try self.authenticate_cluster_peer(conn, peer);
            var message_count: u16 = 0;
            while (offset < install.snapshot.state_size and message_count < 512) {
                const chunk = try snapshot_storage.read_chunk(
                    self.io,
                    self.replication.?.snapshot_paths.archive,
                    install.snapshot,
                    offset,
                    self.replication_command_scratch[0..limits.snapshot_chunk_size_max],
                );
                const done = offset + chunk.len == install.snapshot.state_size;
                try conn.send_message(.{ .raft_install_snapshot = .{
                    .value = install,
                    .offset = offset,
                    .done = done,
                    .chunk = chunk,
                } });
                const response = try self.recv_replication_message(
                    conn,
                    self.replication.?.config.rpc_timeout_ms,
                );
                if (response == .err or response != .raft_snapshot_result)
                    return error.RaftPeerRejected;
                const progress = response.raft_snapshot_result;
                if (progress.value.follower != request.to) return error.InvalidRaftEnvelope;
                if (!progress.accepted) return .{ .snapshot_result = progress.value };
                const expected_offset = offset + chunk.len;
                if (progress.next_offset != expected_offset)
                    return error.InvalidSnapshotOffset;
                offset = progress.next_offset;
                final_result = progress.value;
                message_count += 1;
            }
        }
        const result = final_result orelse return error.InvalidSnapshot;
        if (result.installed_index < install.snapshot.last_index)
            return error.SnapshotInstallationRejected;
        return .{ .snapshot_result = result };
    }

    fn authenticate_cluster_peer(
        self: *Server,
        conn: *transport.Connection,
        peer: config_mod.ClusterMember,
    ) !void {
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
        const response = try self.recv_replication_message(
            conn,
            self.replication.?.config.rpc_timeout_ms,
        );
        const server_hello = switch (response) {
            .handshake => |handshake| handshake,
            else => return error.AuthenticationFailed,
        };
        if (server_hello.kind != .server_hello or
            !std.mem.eql(u8, server_hello.service_name, self.identity.name) or
            !std.mem.eql(u8, server_hello.instance_id, peer.instance_id))
            return error.ClusterIdentityMismatch;
        if (!auth.verify_server_proof(
            self.client_secret,
            client_nonce,
            server_hello.nonce,
            server_hello.service_name,
            server_hello.instance_id,
            server_hello.proof,
        )) return error.AuthenticationFailed;
        const keys = auth.derive_session_keys(
            self.client_secret,
            client_nonce,
            server_hello.nonce,
            self.identity.name,
            self.identity.instance_id,
            server_hello.service_name,
            server_hello.instance_id,
        );
        try conn.enable_security(keys, .client);
    }

    fn recv_replication_message(
        self: *Server,
        connection: *transport.Connection,
        deadline_ms: u32,
    ) !protocol.Message {
        var completions: [2]ReceiveCompletion = undefined;
        var select = Io.Select(ReceiveCompletion).init(self.io, &completions);
        defer select.cancelDiscard();
        try select.concurrent(.message, recv_message_task, .{connection});
        try select.concurrent(.deadline, connection_deadline, .{ self.io, deadline_ms });
        const completion = try select.await();
        return switch (completion) {
            .message => |result| try result,
            .deadline => |result| deadline: {
                try result;
                break :deadline error.ReplicationRpcDeadlineExceeded;
            },
        };
    }

    fn record_replication_failure(self: *Server) void {
        _ = self.replication_failures.fetchAdd(1, .monotonic);
    }

    fn serve_read_stream(
        self: *Server,
        conn: *transport.Connection,
        scratch: *ConnectionScratch,
        request: protocol.ReadStream,
    ) !void {
        if (try self.ensure_linearizable_read(conn)) return;
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
        if (try self.ensure_linearizable_read(conn)) return;
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

    pub fn serve(self: *Server) !void {
        log.info("listening on {s}", .{self.identity.listen_address});

        try self.connection_group.concurrent(
            self.io,
            monitor_shutdown_signal,
            .{self},
        );
        if (self.replication != null) {
            try self.connection_group.concurrent(
                self.io,
                replication_loop,
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

        self.connection_group.cancel(self.io);
    }

    pub fn shutdown(self: *Server) void {
        self.running.store(false, .release);
        self.listener.stop_accepting() catch |err| {
            log.err("failed to stop listener: {}", .{err});
        };
    }

    fn monitor_shutdown_signal(self: *Server) Io.Cancelable!void {
        while (self.running.load(.acquire)) {
            if (shutdown_requested.load(.acquire)) {
                self.shutdown();
                return;
            }
            try self.io.sleep(.fromMilliseconds(10), .awake);
        }
    }

    fn replication_loop(self: *Server) Io.Cancelable!void {
        const interval_ms = self.replication.?.config.heartbeat_interval_ms;
        while (self.running.load(.acquire)) {
            try self.io.sleep(.fromMilliseconds(interval_ms), .awake);
            self.replication_cycle() catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => {
                    self.record_replication_failure();
                    self.replication_mutex.lockUncancelable(self.io);
                    self.replication.?.machine.poisoned = true;
                    self.replication_mutex.unlock(self.io);
                    log.err("fatal replication loop failure: {}", .{err});
                    self.shutdown();
                    return;
                },
            };
        }
    }

    fn replication_cycle(self: *Server) !void {
        var initial: [replication_mod.outbox_capacity]raft.Envelope = undefined;
        self.replication_mutex.lockUncancelable(self.io);
        const runtime = self.replication.?;
        runtime.outbox.reset();
        runtime.machine.node.tick(&runtime.outbox) catch |err| {
            self.replication_mutex.unlock(self.io);
            return err;
        };
        self.store_mutex.lockUncancelable(self.io);
        runtime.checkpoint_and_apply() catch |err| {
            runtime.machine.poisoned = true;
            self.store_mutex.unlock(self.io);
            self.replication_mutex.unlock(self.io);
            return err;
        };
        self.store_mutex.unlock(self.io);
        const count = runtime.take_outbox(&initial) catch |err| {
            self.replication_mutex.unlock(self.io);
            return err;
        };
        self.replication_mutex.unlock(self.io);
        try self.dispatch_raft(initial[0..count]);
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
            error.EndOfStream, error.MessageLimitExceeded => {},
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
        self.store_mutex.lockUncancelable(self.io);
        const failed = sync: {
            self.store.sync() catch break :sync true;
            break :sync false;
        };
        self.store_mutex.unlock(self.io);

        self.group_commit_failed = failed;
        self.group_commit_count += 1;
        self.group_commit_generation += 1;
        self.group_commit_leader = false;
        self.group_commit_condition.broadcast(self.io);
        self.group_commit_mutex.unlock(self.io);
        if (failed) return error.GroupCommitFailed;
    }

    pub fn health(self: *Server) protocol.HealthResponse {
        var clustered = false;
        var replication_quorum = false;
        var replication_poisoned = false;
        var replication_lag: u64 = 0;
        if (self.replication) |runtime| {
            clustered = true;
            self.replication_mutex.lockUncancelable(self.io);
            replication_quorum = runtime.machine.node.linearizable_read_ready();
            replication_poisoned = runtime.machine.poisoned;
            replication_lag = runtime.replication_lag();
            self.replication_mutex.unlock(self.io);
        }
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
            .replication_failures = self.replication_failures.load(.acquire),
            .backups_completed = self.backups_completed,
            .last_backup_unix_seconds = self.last_backup_unix_seconds,
            .backup_age_seconds = backup_age,
            .replication_lag = replication_lag,
        };
    }

    // Called with store_mutex held so a corruption finding and the poison bit
    // become visible as one state transition to health readers.
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

const IncomingRaftRequest = struct {
    envelope: raft.Envelope,
    command: ?[]const u8,
};

fn raft_request_from_protocol(
    message: protocol.Message,
    from: raft.NodeId,
    to: raft.NodeId,
) !IncomingRaftRequest {
    return switch (message) {
        .raft_request_vote => |request| .{
            .envelope = .{ .from = from, .to = to, .message = .{
                .request_vote = request,
            } },
            .command = null,
        },
        .raft_append_entries => |append| .{
            .envelope = .{ .from = from, .to = to, .message = .{
                .append_entries = append.value,
            } },
            .command = if (append.value.entry == null) null else append.command,
        },
        .raft_install_snapshot => |install| .{
            .envelope = .{ .from = from, .to = to, .message = .{
                .install_snapshot = install.value,
            } },
            .command = null,
        },
        else => error.UnexpectedRaftRequest,
    };
}

fn raft_request_to_protocol(
    envelope: raft.Envelope,
    command: []const u8,
) !protocol.Message {
    return switch (envelope.message) {
        .request_vote => |request| .{ .raft_request_vote = request },
        .append_entries => |append| .{ .raft_append_entries = .{
            .value = append,
            .command = command,
        } },
        // Metadata-only installation is intentionally never sent: a follower
        // must not acknowledge a snapshot whose canonical bytes it lacks.
        .install_snapshot => error.SnapshotTransferUnsupported,
        else => error.UnexpectedRaftRequest,
    };
}

fn raft_response_from_protocol(message: protocol.Message) !raft.Message {
    return switch (message) {
        .raft_vote_result => |response| .{ .vote_result = response },
        .raft_append_result => |response| .{ .append_result = response },
        .raft_snapshot_result => |response| .{ .snapshot_result = response.value },
        else => error.UnexpectedRaftResponse,
    };
}

fn raft_response_to_protocol(envelope: raft.Envelope) !protocol.Message {
    return switch (envelope.message) {
        .vote_result => |response| .{ .raft_vote_result = response },
        .append_result => |response| .{ .raft_append_result = response },
        .snapshot_result => |response| .{ .raft_snapshot_result = .{
            .value = response,
            .next_offset = 0,
            .accepted = true,
        } },
        else => error.UnexpectedRaftResponse,
    };
}

fn raft_response_matches(request: raft.Message, response: raft.Message) bool {
    return switch (request) {
        .request_vote => response == .vote_result,
        .append_entries => response == .append_result,
        .install_snapshot => response == .snapshot_result,
        else => false,
    };
}

fn is_raft_request(message: protocol.Message) bool {
    return switch (message) {
        .raft_request_vote, .raft_append_entries, .raft_install_snapshot => true,
        else => false,
    };
}

fn send_protocol_error(
    conn: *transport.Connection,
    code: protocol.ErrorCode,
    message: []const u8,
) !void {
    try conn.send_message(.{ .err = .{ .code = code, .message = message } });
}

fn message_permitted(permissions: auth.Permissions, message: protocol.Message) bool {
    return switch (message) {
        .append_stream => permissions.append,
        .read_stream => permissions.read_stream,
        .read_log => permissions.read_log,
        .pull_request => permissions.recover,
        .health_request => permissions.observe,
        .raft_request_vote, .raft_append_entries, .raft_install_snapshot => permissions.replicate,
        .handshake,
        .pull_response,
        .err,
        .append_stream_response,
        .read_stream_response,
        .read_log_response,
        .health_response,
        .raft_vote_result,
        .raft_append_result,
        .raft_snapshot_result,
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
    const replicate: protocol.Message = .{ .raft_request_vote = .{
        .term = 1,
        .candidate = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    } };
    const messages = [_]protocol.Message{
        append,
        read_stream,
        read_log,
        recover,
        observe,
        replicate,
    };
    const grants = [_]auth.Permissions{
        .{ .append = true },
        .{ .read_stream = true },
        .{ .read_log = true },
        .{ .recover = true },
        .{ .observe = true },
        .{ .replicate = true },
    };
    for (grants, 0..) |grant, allowed| {
        for (messages, 0..) |message, operation| {
            try std.testing.expectEqual(allowed == operation, message_permitted(grant, message));
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
    var server = try Server.init(allocator, &event_store, identity, &.{}, io);
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
    var server = try Server.init(allocator, &event_store, identity, &.{}, io);
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
    var server = try Server.init(allocator, &event_store, identity, &.{}, io);
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
    var srv = try Server.init(allocator, &event_store, .{
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

    // Clean up test files
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.wal") catch {};
    Io.Dir.cwd().deleteFile(io, "data/test_recovery_server.crumb") catch {};

    // Start a mock peer that serves events
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

    // Peer thread: respond to handshake + pull request with 2 events
    const peer_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *transport.Listener, conn: *transport.Connection) void {
            l.accept_into(conn) catch return;
            defer conn.close();

            // Receive handshake
            _ = conn.recv_message() catch return;

            // Receive pull request
            _ = conn.recv_message() catch return;

            // Send 2 events
            const entries = [_]protocol.PullResponse.EventEntry{
                .{
                    .key = "evt:order-service:order.created:0000000000000001",
                    .value = "1|100|2|order-service|order.created|{}",
                },
                .{
                    .key = "evt:order-service:order.created:0000000000000002",
                    .value = "2|200|2|order-service|order.created|{}",
                },
            };
            conn.send_message(.{ .pull_response = .{ .events = &entries } }) catch {};
        }
    }.run, .{ &peer_listener, &peer_connection });

    // Contract pointing to mock peer
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

    const identity = ServiceIdentity{
        .name = "my-service",
        .instance_id = "test-1",
        .listen_address = "127.0.0.1:0",
    };

    var srv = try Server.init(allocator, &event_store, identity, &contracts, io);
    defer srv.deinit();

    try srv.run_recovery();

    peer_thread.join();

    // Verify events were ingested
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

    var srv = try Server.init(allocator, &event_store, identity, &.{}, io);
    defer srv.deinit();

    const port = srv.listener.get_port();
    var accepted_connection: transport.Connection = undefined;
    var connection_scratch: Server.ConnectionScratch = undefined;

    // Server thread: accept one connection and handle it
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

    // Client: connect, handshake, pull request, receive response
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

    // Verify events are deserializable and in sequence order
    const ev0 = try EventValue.deserialize_borrowed(response.pull_response.events[0].value);
    try std.testing.expectEqual(@as(u64, 1), ev0.sequence);
    try std.testing.expectEqualStrings("my-service", ev0.origin_service);

    // Close client to unblock server thread
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
    try srv.run_recovery();

    // Store should have no events (nothing was ingested)
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

const LiveClusterTest = struct {
    const node_count = 3;
    const cluster_secret = "0123456789abcdef0123456789abcdef";
    const app_secret = "abcdef0123456789abcdef0123456789";
    const instance_ids = [node_count][]const u8{ "cluster-1", "cluster-2", "cluster-3" };
    const wal_paths = [node_count][]const u8{
        "data/test_live_cluster_1.wal",
        "data/test_live_cluster_2.wal",
        "data/test_live_cluster_3.wal",
    };
    const state_paths = [node_count][]const u8{
        "data/test_live_cluster_1.raft",
        "data/test_live_cluster_2.raft",
        "data/test_live_cluster_3.raft",
    };

    const Node = struct {
        store: EventStore,
        server: Server,
    };

    allocator: Allocator,
    io: Io,
    address_storage: [node_count][32]u8 = undefined,
    addresses: [node_count][]const u8 = undefined,
    members: [node_count]config_mod.ClusterMember = undefined,
    credentials: [2]auth.Credential = .{
        .{
            .service_name = "compix-cluster-test",
            .secret = cluster_secret,
            .permissions = .{ .replicate = true },
        },
        .{
            .service_name = "cluster-app",
            .secret = app_secret,
            .permissions = .{ .append = true, .read_stream = true, .read_log = true },
        },
    },
    nodes: [node_count]Node = undefined,
    threads: [node_count]?std.Thread = @splat(null),
    live: [node_count]bool = @splat(false),
    initialized: [node_count]bool = @splat(false),
    built: u8 = 0,

    fn init(self: *LiveClusterTest, allocator: Allocator, io: Io) !void {
        self.* = .{ .allocator = allocator, .io = io };
        cleanup_files(io);

        var reservations: [node_count]transport.Listener = undefined;
        var reserved: u8 = 0;
        errdefer for (reservations[0..reserved]) |*listener| listener.deinit();
        for (&reservations) |*listener| {
            listener.* = try transport.Listener.init("127.0.0.1:0", io);
            reserved += 1;
        }
        for (&reservations, 0..) |*listener, index| {
            self.addresses[index] = try std.fmt.bufPrint(
                &self.address_storage[index],
                "127.0.0.1:{d}",
                .{listener.get_port()},
            );
        }
        for (&reservations) |*listener| listener.deinit();
        reserved = 0;

        for (&self.members, 0..) |*member, index| member.* = .{
            .node_id = @intCast(index + 1),
            .instance_id = instance_ids[index],
            .address = self.addresses[index],
        };
        for (0..node_count) |index| {
            try self.init_node(index);
            self.initialized[index] = true;
            self.built += 1;
        }
    }

    fn init_node(self: *LiveClusterTest, index: usize) !void {
        const node = &self.nodes[index];
        node.store = try EventStore.init(self.allocator, wal_paths[index], self.io, .{
            .identity = "compix-cluster-test",
            .stream_capacity = 32,
            .stream_id_bytes_capacity = 4096,
        });
        errdefer node.store.deinit();
        try Server.init_in_place(&node.server, self.allocator, .{
            .store = &node.store,
            .identity = .{
                .name = "compix-cluster-test",
                .instance_id = instance_ids[index],
                .listen_address = self.addresses[index],
            },
            .contracts = &.{},
            .io = self.io,
            .connection_capacity = 4,
            .client_secret = cluster_secret,
            .credentials = &self.credentials,
            .cluster = .{
                .node_id = @intCast(index + 1),
                .state_path = state_paths[index],
                .members = &self.members,
                .heartbeat_interval_ms = 20,
                .election_timeout_min_ticks = 25,
                .election_timeout_max_ticks = 40,
                .rpc_timeout_ms = 300,
                .log_capacity = 16,
                .command_slot_size = protocol.replicated_command_size_max,
            },
        });
    }

    fn start(self: *LiveClusterTest) !void {
        for (0..self.built) |index| {
            self.threads[index] = try std.Thread.spawn(
                .{},
                serve_node,
                .{&self.nodes[index].server},
            );
            self.live[index] = true;
        }
    }

    fn stop(self: *LiveClusterTest, index: usize) void {
        if (!self.live[index]) return;
        self.nodes[index].server.shutdown();
        self.threads[index].?.join();
        self.threads[index] = null;
        self.live[index] = false;
    }

    fn restart(self: *LiveClusterTest, index: usize) !void {
        self.stop(index);
        self.nodes[index].server.deinit();
        self.nodes[index].store.deinit();
        self.initialized[index] = false;
        try self.init_node(index);
        self.initialized[index] = true;
        self.threads[index] = try std.Thread.spawn(
            .{},
            serve_node,
            .{&self.nodes[index].server},
        );
        self.live[index] = true;
    }

    fn replace(self: *LiveClusterTest, index: usize) !void {
        self.stop(index);
        self.nodes[index].server.deinit();
        self.nodes[index].store.deinit();
        self.initialized[index] = false;
        cleanup_node_files(self.io, index);
        try self.init_node(index);
        self.initialized[index] = true;
        self.threads[index] = try std.Thread.spawn(
            .{},
            serve_node,
            .{&self.nodes[index].server},
        );
        self.live[index] = true;
    }

    fn deinit(self: *LiveClusterTest) void {
        for (0..self.built) |index| self.stop(index);
        var remaining = self.built;
        while (remaining > 0) {
            remaining -= 1;
            if (!self.initialized[remaining]) continue;
            self.nodes[remaining].server.deinit();
            self.nodes[remaining].store.deinit();
            self.initialized[remaining] = false;
        }
        self.built = 0;
        cleanup_files(self.io);
    }

    fn wait_for_leader(self: *LiveClusterTest) !usize {
        for (0..250) |_| {
            for (0..self.built) |index| {
                if (!self.live[index]) continue;
                const health_state = self.nodes[index].server.health();
                if (health_state.ready and health_state.replication_quorum) return index;
            }
            try self.io.sleep(.fromMilliseconds(20), .awake);
        }
        return error.ClusterElectionTimeout;
    }

    fn wait_for_position(self: *LiveClusterTest, expected: u64) !void {
        for (0..250) |_| {
            var complete = true;
            for (0..self.built) |index| {
                if (!self.live[index]) continue;
                self.nodes[index].server.store_mutex.lockUncancelable(self.io);
                const position = self.nodes[index].store.metrics().committed_log_position;
                self.nodes[index].server.store_mutex.unlock(self.io);
                if (position != expected) complete = false;
            }
            if (complete) return;
            try self.io.sleep(.fromMilliseconds(20), .awake);
        }
        return error.ClusterApplyTimeout;
    }

    fn wait_for_snapshot(
        self: *LiveClusterTest,
        node_index: usize,
        expected_minimum: raft.Index,
    ) !void {
        for (0..250) |_| {
            self.nodes[node_index].server.replication_mutex.lockUncancelable(self.io);
            const snapshot_index =
                self.nodes[node_index].server.replication.?.machine.node.snapshot.last_index;
            self.nodes[node_index].server.replication_mutex.unlock(self.io);
            if (snapshot_index >= expected_minimum) return;
            try self.io.sleep(.fromMilliseconds(20), .awake);
        }
        return error.ClusterSnapshotTimeout;
    }

    fn serve_node(server: *Server) void {
        server.serve() catch |err| {
            if (server.running.load(.acquire)) {
                std.debug.panic("live cluster server failed: {}", .{err});
            }
        };
    }

    fn cleanup_files(io: Io) void {
        for (0..node_count) |index| {
            cleanup_node_files(io, index);
        }
    }

    fn cleanup_node_files(io: Io, index: usize) void {
        Io.Dir.cwd().deleteFile(io, wal_paths[index]) catch {};
        Io.Dir.cwd().deleteFile(io, state_paths[index]) catch {};
        var state_buffer: [128]u8 = undefined;
        inline for (.{ ".snapshot", ".snapshot.incoming", ".snapshot.quarantine" }) |suffix| {
            const path = std.fmt.bufPrint(&state_buffer, "{s}{s}", .{
                state_paths[index],
                suffix,
            }) catch unreachable;
            Io.Dir.cwd().deleteFile(io, path) catch {};
        }
        var path_buffer: [128]u8 = undefined;
        inline for (.{ ".crumb", ".manifest" }) |suffix| {
            const path = std.fmt.bufPrint(
                &path_buffer,
                "data/test_live_cluster_{d}{s}",
                .{ index + 1, suffix },
            ) catch unreachable;
            Io.Dir.cwd().deleteFile(io, path) catch {};
        }
    }
};

test "live cluster snapshots, replaces a node, fails over, and fences minority writes" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    var cluster: LiveClusterTest = undefined;
    try cluster.init(allocator, io);
    defer cluster.deinit();
    try cluster.start();

    const first_leader = try cluster.wait_for_leader();
    var production_observed = false;
    for (0..200) |_| {
        const health = cluster.nodes[first_leader].server.health();
        if (health.production_ready and !health.degraded) {
            try std.testing.expect(health.ready);
            try std.testing.expect(health.replication_quorum);
            production_observed = true;
            break;
        }
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.testing.expect(production_observed);
    var client: DriverTest.driver.Client = undefined;
    try DriverTest.driver.Client.connect(&client, .{
        .address = cluster.addresses[first_leader],
        .service_name = "cluster-app",
        .instance_id = "client-1",
        .secret = LiveClusterTest.app_secret,
        .io = io,
    });
    const large_payload: [40 * 1024]u8 = @splat('x');
    const first_event = [_]protocol.AppendEvent{.{
        .event_type = "cluster.created",
        .schema_version = 1,
        .payload = &large_payload,
    }};
    const first = try client.append(.{
        .stream_id = "cluster/1",
        .expected_version = .empty,
        .command_id = "live-cluster-command-1",
        .events = &first_event,
    });
    try std.testing.expect(first.response == .committed);
    const read = try client.read_stream(.{
        .stream_id = "cluster/1",
        .from_version = 0,
        .max_count = 8,
    });
    switch (read) {
        .response => |response| try std.testing.expectEqual(
            @as(usize, 1),
            response.events.len,
        ),
        .rejected => |rejection| {
            std.debug.print("snapshot test read rejected: {t} {s}\n", .{
                rejection.code,
                rejection.message,
            });
            return error.UnexpectedReadRejection;
        },
    }
    for (2..20) |command_number| {
        var command_buffer: [64]u8 = undefined;
        const command_id = try std.fmt.bufPrint(
            &command_buffer,
            "live-cluster-command-{d}",
            .{command_number},
        );
        const appended = try client.append(.{
            .stream_id = "cluster/1",
            .expected_version = .{ .exact = command_number - 1 },
            .command_id = command_id,
            .events = &first_event,
        });
        switch (appended) {
            .response => |response| try std.testing.expect(response == .committed),
            .rejected => |rejection| {
                std.debug.print("snapshot test append {d} rejected: {t} {s}\n", .{
                    command_number,
                    rejection.code,
                    rejection.message,
                });
                return error.UnexpectedAppendRejection;
            },
        }
    }
    client.deinit();
    try cluster.wait_for_position(19);
    try cluster.wait_for_snapshot(first_leader, 16);

    const restarted_follower = (first_leader + 1) % LiveClusterTest.node_count;
    try cluster.replace(restarted_follower);
    try cluster.wait_for_position(19);
    cluster.nodes[restarted_follower].server.replication_mutex.lockUncancelable(io);
    const installed_snapshot = cluster.nodes[restarted_follower]
        .server.replication.?.machine.node.snapshot.last_index;
    cluster.nodes[restarted_follower].server.replication_mutex.unlock(io);
    try std.testing.expect(installed_snapshot >= 16);

    cluster.stop(first_leader);
    const second_leader = try cluster.wait_for_leader();
    try std.testing.expect(second_leader != first_leader);
    try DriverTest.driver.Client.connect(&client, .{
        .address = cluster.addresses[second_leader],
        .service_name = "cluster-app",
        .instance_id = "client-2",
        .secret = LiveClusterTest.app_secret,
        .io = io,
    });
    const second_event = [_]protocol.AppendEvent{.{
        .event_type = "cluster.updated",
        .schema_version = 1,
        .payload = "two",
    }};
    const second = try client.append(.{
        .stream_id = "cluster/1",
        .expected_version = .{ .exact = 19 },
        .command_id = "live-cluster-command-20",
        .events = &second_event,
    });
    try std.testing.expect(second.response == .committed);
    client.deinit();
    try cluster.wait_for_position(20);

    for (0..LiveClusterTest.node_count) |index| {
        if (cluster.live[index] and index != second_leader) cluster.stop(index);
    }
    try DriverTest.driver.Client.connect(&client, .{
        .address = cluster.addresses[second_leader],
        .service_name = "cluster-app",
        .instance_id = "client-3",
        .secret = LiveClusterTest.app_secret,
        .io = io,
    });
    const unavailable = try client.append(.{
        .stream_id = "cluster/1",
        .expected_version = .{ .exact = 20 },
        .command_id = "live-cluster-command-no-quorum",
        .events = &second_event,
    });
    try std.testing.expectEqual(
        protocol.ErrorCode.quorum_unavailable,
        unavailable.rejected.code,
    );
    client.deinit();
    var degraded_observed = false;
    for (0..200) |_| {
        const health = cluster.nodes[second_leader].server.health();
        if (!health.production_ready and health.degraded and !health.ready and
            !health.replication_quorum)
        {
            degraded_observed = true;
            break;
        }
        try io.sleep(.fromMilliseconds(10), .awake);
    }
    try std.testing.expect(degraded_observed);
}
