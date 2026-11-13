const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;

pub const protocol = @import("core/protocol.zig");
const auth = @import("core/auth.zig");
const transport = @import("core/transport.zig");
const limits = @import("core/limits.zig");
const subscription = @import("core/subscription.zig");

pub const endpoint_count_max: u8 = 5;
pub const endpoint_list_length_max: u16 =
    endpoint_count_max * protocol.address_length_max + endpoint_count_max - 1;
const request_rounds_max: u8 = 2;

/// Turns a dead socket into a reconnect, because reads have no deadline of their own.
/// It must never fire on a healthy-but-slow store.
pub const receive_deadline_default_ms: u32 = 120 * 1000;

comptime {
    assert(endpoint_count_max >= 3);
    assert(request_rounds_max > 1);
}

pub const AppendEvent = protocol.AppendEvent;
pub const AppendRequest = protocol.AppendStream;
pub const AppendResponse = protocol.AppendStreamResponse;
pub const ReadEvent = protocol.ReadEvent;
pub const ReadLogRequest = protocol.ReadLog;
pub const ReadLogResponse = protocol.ReadLogResponse;
pub const ReadStreamRequest = protocol.ReadStream;
pub const ReadStreamResponse = protocol.ReadStreamResponse;
pub const HealthResponse = protocol.HealthResponse;
pub const VcEntry = protocol.VcEntry;
pub const VcSummaryRequest = protocol.VcSummaryRequest;
pub const VcSummaryResponse = protocol.VcSummaryResponse;
pub const BootstrapRequest = protocol.BootstrapRequest;
pub const BootstrapEvent = protocol.BootstrapEvent;
pub const BootstrapResponse = protocol.BootstrapResponse;
pub const ServerError = protocol.Error;

pub const ConnectOptions = struct {
    /// One address or a comma-separated list; at most five endpoints are tried.
    address: []const u8,
    service_name: []const u8,
    instance_id: []const u8,
    secret: []const u8,
    io: Io,
    receive_deadline_ms: u32 = receive_deadline_default_ms,
};

pub const AppendReply = union(enum) {
    response: AppendResponse,
    rejected: ServerError,
};

pub const ReadStreamReply = union(enum) {
    response: ReadStreamResponse,
    rejected: ServerError,
};

pub const PullReply = union(enum) {
    response: protocol.PullResponse,
    rejected: ServerError,
};

pub const ReadLogReply = union(enum) {
    response: ReadLogResponse,
    rejected: ServerError,
};

pub const HealthReply = union(enum) {
    response: HealthResponse,
    rejected: ServerError,
};

pub const VcSummaryReply = union(enum) {
    response: VcSummaryResponse,
    rejected: ServerError,
};

pub const BootstrapReply = union(enum) {
    response: BootstrapResponse,
    rejected: ServerError,
};

pub const SubscriptionBatch = protocol.SubscriptionBatch;

pub const SubscriptionReply = union(enum) {
    batch: SubscriptionBatch,
    /// The server ended the subscription; `Subscription.start` again to retry.
    rejected: ServerError,
};

pub const SubscribeOptions = struct {
    /// Empty subscribes to the whole ordered log; otherwise to this one stream.
    stream_id: []const u8 = "",
    /// Where to start, typically the caller's checkpointed `next`: a log position for the
    /// whole log, a stream version for one stream. 0 means the start.
    from: u64,
    /// Only these event types are delivered; empty delivers every event.
    event_types: []const []const u8 = &.{},
};

/// A resumable push subscription to the ordered log or one stream, on a client used for
/// nothing else.
///
/// The server holds no state for it, so it survives disconnects and endpoint failover: the
/// subscription reconnects and resubscribes from the `next` of the last batch it returned.
/// Across process restarts, start again from the caller's own checkpoint.
///
/// There is no window to tune: the server reads each batch from the log on demand, so a
/// caller that is slow to call `next` only leaves the socket full and the server's pusher
/// waiting. A caller that stops calling `next` for the server's send deadline is disconnected,
/// and resumes on its next call.
///
/// WARN: Not thread-safe. Returned batches borrow the client's buffers until the next `next`.
/// FIXME: Check to see weather we can do better here, the subscription should be distributed
/// by ownership, subagents attached as fugue nodes may messed this up
pub const Subscription = struct {
    client: *Client,
    /// The cursor to resume from: a log position, or a stream version for one stream.
    next_cursor: u64,
    active: bool,
    target: subscription.Target,

    pub fn start(target: *Subscription, client: *Client, options: SubscribeOptions) !void {
        for (options.event_types) |event_type| {
            if (event_type.len == 0) return error.EventTypeEmpty;
            if (event_type.len > limits.event_type_length_max) return error.EventTypeTooLong;
        }
        target.* = .{
            .client = client,
            .next_cursor = options.from,
            .active = false,
            .target = undefined,
        };
        try target.target.set(options.stream_id, options.event_types);
        try target.subscribe_with_failover();
        assert(target.active);
    }

    pub fn stop(self: *Subscription) void {
        self.client.close();
        self.active = false;
    }

    /// The next batch, waiting for one if nothing is new. A batch without events is a
    /// heartbeat; its `next` may still have moved past events the filter skipped.
    pub fn next(self: *Subscription) !SubscriptionReply {
        if (!self.active) return error.SubscriptionInactive;
        const attempts_max: u8 = self.client.endpoint_count * request_rounds_max;
        assert(attempts_max > 0);
        var last_error: anyerror = error.ClusterUnavailable;
        for (0..attempts_max) |_| {
            if (self.receive_batch()) |reply| {
                return reply;
            } else |err| switch (err) {
                error.InvalidSubscriptionBatch, error.UnexpectedMessage => {
                    self.stop();
                    return err;
                },
                else => last_error = err,
            }
            // NOTE: a lost connection loses nothing: the server resumes at `next_cursor`.
            self.client.close();
            self.client.advance_endpoint();
            self.subscribe() catch |err| {
                last_error = err;
                self.client.close();
            };
        }
        self.stop();
        return last_error;
    }

    fn receive_batch(self: *Subscription) !SubscriptionReply {
        if (!self.active) return error.SubscriptionInactive;
        const message = try self.client.recv_bounded();
        const batch = switch (message) {
            .subscription_batch => |batch| batch,
            .err => |server_error| {
                self.stop();
                return .{ .rejected = server_error };
            },
            else => return error.UnexpectedMessage,
        };
        if (batch.next < self.next_cursor) return error.InvalidSubscriptionBatch;
        const whole_log = self.target.stream_id().len == 0;
        for (batch.events) |event| {
            const cursor = if (whole_log) event.log_position else event.stream_version;
            if (cursor < self.next_cursor or cursor >= batch.next)
                return error.InvalidSubscriptionBatch;
            if (!whole_log and !std.mem.eql(u8, event.stream_id, self.target.stream_id()))
                return error.InvalidSubscriptionBatch;
        }
        self.next_cursor = batch.next;
        return .{ .batch = batch };
    }

    fn subscribe_with_failover(self: *Subscription) !void {
        const attempts_max: u8 = self.client.endpoint_count * request_rounds_max;
        assert(attempts_max > 0);
        var last_error: anyerror = error.ClusterUnavailable;
        for (0..attempts_max) |_| {
            self.subscribe() catch |err| {
                last_error = err;
                self.client.close();
                self.client.advance_endpoint();
                continue;
            };
            return;
        }
        return last_error;
    }

    fn subscribe(self: *Subscription) !void {
        self.active = false;
        if (!self.client.connected) try self.client.connect_transport();
        try self.client.connection.send_message(.{ .subscribe = .{
            .stream_id = self.target.stream_id(),
            .from = self.next_cursor,
            .event_types = self.target.filter.event_types(),
        } });
        self.active = true;
    }
};

/// WARN: Not thread-safe. Returned slices borrow its buffers until the next request.
pub const Client = struct {
    addresses: [endpoint_count_max][protocol.address_length_max]u8,
    address_lengths: [endpoint_count_max]u16,
    connection: transport.Connection,
    connected: bool,
    endpoint_count: u8,
    endpoint_index: u8,
    instance_id: [protocol.instance_id_length_max]u8,
    instance_id_length: u16,
    io: Io,
    receive_deadline_ms: u32,
    request_count: protocol.RequestCount,
    service_name: [protocol.service_name_length_max]u8,
    service_name_length: u16,
    secret: [auth.secret_length_max]u8,
    secret_length: u8,
    vc_entries: [protocol.vc_entries_max]protocol.VcEntry,

    pub fn connect(target: *Client, options: ConnectOptions) !void {
        if (options.address.len == 0) return error.InvalidAddress;
        if (options.address.len > endpoint_list_length_max) return error.AddressListTooLong;
        if (options.service_name.len == 0) return error.InvalidServiceName;
        if (options.service_name.len > protocol.service_name_length_max)
            return error.ServiceNameTooLong;
        if (options.instance_id.len == 0) return error.InvalidInstanceId;
        if (options.instance_id.len > protocol.instance_id_length_max)
            return error.InstanceIdTooLong;
        try auth.validate_secret(options.secret);

        target.* = .{
            .addresses = undefined,
            .address_lengths = @splat(0),
            .connection = undefined,
            .connected = false,
            .endpoint_count = 0,
            .endpoint_index = 0,
            .instance_id = undefined,
            .instance_id_length = @intCast(options.instance_id.len),
            .io = options.io,
            .receive_deadline_ms = options.receive_deadline_ms,
            .request_count = 0,
            .service_name = undefined,
            .service_name_length = @intCast(options.service_name.len),
            .secret = undefined,
            .secret_length = @intCast(options.secret.len),
            .vc_entries = undefined,
        };
        target.endpoint_count = try copy_endpoints(
            options.address,
            &target.addresses,
            &target.address_lengths,
        );
        @memcpy(target.instance_id[0..target.instance_id_length], options.instance_id);
        @memcpy(target.service_name[0..target.service_name_length], options.service_name);
        @memcpy(target.secret[0..target.secret_length], options.secret);
        var last_error: anyerror = error.ClusterUnavailable;
        var attempt: u8 = 0;
        while (attempt < target.endpoint_count) : (attempt += 1) {
            target.connect_transport() catch |err| {
                last_error = err;
                target.advance_endpoint();
                continue;
            };
            break;
        } else {
            std.crypto.secureZero(u8, &target.secret);
            target.secret_length = 0;
            return last_error;
        }
        assert(target.connected);
        assert(target.request_count == 0);
    }

    pub fn close(self: *Client) void {
        if (!self.connected) return;
        self.connection.close();
        self.connected = false;
        self.request_count = 0;
        assert(!self.connected);
    }

    pub fn deinit(self: *Client) void {
        self.close();
        std.crypto.secureZero(u8, &self.secret);
        self.secret_length = 0;
    }

    pub fn append(self: *Client, request: AppendRequest) !AppendReply {
        const request_message: protocol.Message = .{ .append_stream = request };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .append_stream_response => |response| .{ .response = response },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    pub fn read_stream(self: *Client, request: ReadStreamRequest) !ReadStreamReply {
        const request_message: protocol.Message = .{ .read_stream = request };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .read_stream_response => |response| .{ .response = response },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    pub fn read_log(self: *Client, request: ReadLogRequest) !ReadLogReply {
        const request_message: protocol.Message = .{ .read_log = request };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .read_log_response => |response| .{ .response = response },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    pub fn pull(self: *Client, request: protocol.PullRequest) !PullReply {
        const request_message: protocol.Message = .{ .pull_request = request };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .pull_response => |response| .{ .response = response },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    pub fn health(self: *Client) !HealthReply {
        const request_message: protocol.Message = .{ .health_request = .{} };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .health_response => |response| .{ .response = response },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    pub fn vc_summary(self: *Client) !VcSummaryReply {
        const request_message: protocol.Message = .{ .vc_summary_request = .{} };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .vc_summary_response => |response| blk: {
                const count = @min(response.entries.len, self.vc_entries.len);
                @memcpy(self.vc_entries[0..count], response.entries[0..count]);
                break :blk .{ .response = .{ .entries = self.vc_entries[0..count] } };
            },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    pub fn bootstrap(self: *Client, request: BootstrapRequest) !BootstrapReply {
        const request_message: protocol.Message = .{ .bootstrap_request = request };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .bootstrap_response => |response| .{ .response = response },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    fn round_trip(self: *Client, message: *const protocol.Message) !protocol.Message {
        // NOTE: safe to retry: reads are replay-safe and command IDs make appends idempotent.
        const request_attempts_max = self.endpoint_count * request_rounds_max;
        var attempt: u8 = 0;
        while (attempt < request_attempts_max) : (attempt += 1) {
            self.ensure_connection() catch |err| {
                self.advance_endpoint();
                if (attempt + 1 == request_attempts_max) return err;
                continue;
            };
            self.connection.send_message(message.*) catch |err| {
                self.close();
                self.advance_endpoint();
                if (attempt + 1 == request_attempts_max) return err;
                continue;
            };
            self.request_count += 1;
            assert(self.request_count <= protocol.requests_per_connection_max);
            const response = self.recv_bounded() catch |err| {
                self.close();
                self.advance_endpoint();
                if (attempt + 1 == request_attempts_max) return err;
                continue;
            };
            if (self.endpoint_count > 1 and retryable_server_response(response) and
                attempt + 1 < request_attempts_max)
            {
                self.close();
                self.advance_endpoint();
                continue;
            }
            return response;
        }
        unreachable;
    }

    fn recv_bounded(self: *Client) !protocol.Message {
        var state: ReceiveState = .{ .client = self, .message = undefined };
        var completions: [2]ReceiveCompletion = undefined;
        var select = Io.Select(ReceiveCompletion).init(self.io, &completions);
        defer select.cancelDiscard();
        try select.concurrent(.received, receive_task, .{&state});
        try select.concurrent(.deadline, receive_deadline, .{ self.io, self.receive_deadline_ms });
        const completion = try select.await();
        switch (completion) {
            .received => |result| {
                try result;
                return state.message;
            },
            .deadline => |result| {
                try result;
                return error.ReceiveDeadlineExceeded;
            },
        }
    }

    fn ensure_connection(self: *Client) !void {
        if (!self.connected) {
            try self.connect_transport();
            return;
        }
        if (self.request_count < protocol.requests_per_connection_max) return;
        assert(self.request_count == protocol.requests_per_connection_max);
        self.close();
        try self.connect_transport();
    }

    fn connect_transport(self: *Client) !void {
        assert(!self.connected);
        assert(self.endpoint_index < self.endpoint_count);
        const address_length = self.address_lengths[self.endpoint_index];
        try transport.dial_into(
            &self.connection,
            self.addresses[self.endpoint_index][0..address_length],
            self.io,
        );
        errdefer self.connection.close();
        var client_nonce: [auth.nonce_length]u8 = undefined;
        try self.io.randomSecure(&client_nonce);
        const secret = self.secret[0..self.secret_length];
        const service_name = self.service_name[0..self.service_name_length];
        const instance_id = self.instance_id[0..self.instance_id_length];
        try self.connection.send_message(.{ .handshake = .{
            .kind = .client_hello,
            .service_name = service_name,
            .instance_id = instance_id,
            .nonce = client_nonce,
            .proof = auth.make_client_proof(
                secret,
                service_name,
                instance_id,
                client_nonce,
            ),
        } });
        // NOTE: connect runs before any tick, so no outer deadline can rescue a silent peer.
        const response = try self.recv_bounded();
        const server = switch (response) {
            .handshake => |handshake| handshake,
            .err => return error.AuthenticationFailed,
            else => return error.InvalidHandshake,
        };
        if (server.kind != .server_hello) return error.InvalidHandshake;
        if (!auth.verify_server_proof(
            secret,
            client_nonce,
            server.nonce,
            server.service_name,
            server.instance_id,
            server.proof,
        )) return error.AuthenticationFailed;
        const keys = auth.derive_session_keys(
            secret,
            client_nonce,
            server.nonce,
            service_name,
            instance_id,
            server.service_name,
            server.instance_id,
        );
        try self.connection.enable_security(keys, .client);
        self.connected = true;
        self.request_count = 0;
        assert(self.connected);
    }

    fn advance_endpoint(self: *Client) void {
        assert(!self.connected);
        assert(self.endpoint_count > 0);
        self.endpoint_index = (self.endpoint_index + 1) % self.endpoint_count;
    }
};

fn copy_endpoints(
    text: []const u8,
    addresses: *[endpoint_count_max][protocol.address_length_max]u8,
    lengths: *[endpoint_count_max]u16,
) !u8 {
    if (text.len == 0) return error.InvalidAddress;
    var parts = std.mem.splitScalar(u8, text, ',');
    var count: u8 = 0;
    while (parts.next()) |part| {
        if (part.len == 0) return error.InvalidAddress;
        if (part.len > protocol.address_length_max) return error.AddressTooLong;
        if (count == endpoint_count_max) return error.TooManyEndpoints;
        for (0..count) |previous| {
            if (std.mem.eql(u8, addresses[previous][0..lengths[previous]], part))
                return error.DuplicateEndpoint;
        }
        @memcpy(addresses[count][0..part.len], part);
        lengths[count] = @intCast(part.len);
        count += 1;
    }
    if (count == 0) return error.InvalidAddress;
    return count;
}

fn retryable_server_response(response: protocol.Message) bool {
    _ = response;
    return false;
}

test "bounded endpoint lists reject empty duplicate and excess entries" {
    var addresses: [endpoint_count_max][protocol.address_length_max]u8 = undefined;
    var lengths: [endpoint_count_max]u16 = @splat(0);
    try std.testing.expectEqual(
        @as(u8, 3),
        try copy_endpoints(
            "127.0.0.1:4101,127.0.0.1:4102,127.0.0.1:4103",
            &addresses,
            &lengths,
        ),
    );
    try std.testing.expectEqualStrings("127.0.0.1:4102", addresses[1][0..lengths[1]]);
    try std.testing.expectError(
        error.InvalidAddress,
        copy_endpoints("127.0.0.1:4101,", &addresses, &lengths),
    );
    try std.testing.expectError(
        error.DuplicateEndpoint,
        copy_endpoints("127.0.0.1:4101,127.0.0.1:4101", &addresses, &lengths),
    );
    try std.testing.expectError(
        error.TooManyEndpoints,
        copy_endpoints("1:1,2:2,3:3,4:4,5:5,6:6", &addresses, &lengths),
    );
}

comptime {
    assert(!@hasDecl(@This(), "EventStore"));
    assert(!@hasDecl(@This(), "LSMEngine"));
    assert(@hasDecl(Client, "connect"));
    assert(@hasDecl(Client, "append"));
    assert(@hasDecl(Client, "read_stream"));
    assert(@hasDecl(Client, "read_log"));
    assert(@hasDecl(Client, "health"));
    assert(@hasDecl(Client, "pull"));
    assert(@hasDecl(Client, "vc_summary"));
    assert(@hasDecl(Client, "bootstrap"));
    assert(@hasDecl(Subscription, "next"));
}

const ReceiveState = struct {
    client: *Client,
    message: protocol.Message,
};

const ReceiveCompletion = union(enum) {
    received: anyerror!void,
    deadline: Io.Cancelable!void,
};

fn receive_task(state: *ReceiveState) anyerror!void {
    state.message = try state.client.connection.recv_message();
}

fn receive_deadline(io: Io, timeout_ms: u32) Io.Cancelable!void {
    try io.sleep(.fromMilliseconds(timeout_ms), .awake);
}

test "a peer that accepts and never answers becomes an error, not a hang" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try transport.Listener.init("127.0.0.1:0", io);
    defer listener.deinit();

    var address_buffer: [32]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(
        &address_buffer,
        "127.0.0.1:{d}",
        .{listener.get_port()},
    );

    var client: Client = undefined;
    const connected = Client.connect(&client, .{
        .address = endpoint,
        .service_name = "silent-peer-test",
        .instance_id = "probe-1",
        .secret = "0123456789abcdef0123456789abcdef",
        .io = io,
        .receive_deadline_ms = 150,
    });
    if (connected) |_| {
        defer client.deinit();
        const reply = client.health();
        try std.testing.expect(std.meta.isError(reply));
    } else |err| {
        try std.testing.expect(err == error.ReceiveDeadlineExceeded or
            err == error.ConnectDeadlineExceeded or
            err == error.EndOfStream or
            err == error.ClusterUnavailable);
    }
}
