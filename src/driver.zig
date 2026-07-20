//! Network driver for a running Compix service.
//!
//! This module exposes no storage-engine implementation. Returned slices borrow
//! the client's fixed receive buffers and remain valid until the next request.

const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;

pub const protocol = @import("core/protocol.zig");
const auth = @import("core/auth.zig");
const transport = @import("core/transport.zig");

pub const endpoint_count_max: u8 = 5;
pub const endpoint_list_length_max: u16 =
    endpoint_count_max * protocol.address_length_max + endpoint_count_max - 1;
const request_rounds_max: u8 = 2;

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
pub const ServerError = protocol.Error;

pub const ConnectOptions = struct {
    /// One address or a comma-separated static cluster list. The client visits
    /// at most five endpoints in bounded rounds on transport or leader errors.
    address: []const u8,
    service_name: []const u8,
    instance_id: []const u8,
    secret: []const u8,
    io: Io,
};

pub const AppendReply = union(enum) {
    response: AppendResponse,
    rejected: ServerError,
};

pub const ReadStreamReply = union(enum) {
    response: ReadStreamResponse,
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
    request_count: protocol.RequestCount,
    service_name: [protocol.service_name_length_max]u8,
    service_name_length: u16,
    secret: [auth.secret_length_max]u8,
    secret_length: u8,

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
            .request_count = 0,
            .service_name = undefined,
            .service_name_length = @intCast(options.service_name.len),
            .secret = undefined,
            .secret_length = @intCast(options.secret.len),
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

    pub fn health(self: *Client) !HealthReply {
        const request_message: protocol.Message = .{ .health_request = .{} };
        const message = try self.round_trip(&request_message);
        return switch (message) {
            .health_response => |response| .{ .response = response },
            .err => |server_error| .{ .rejected = server_error },
            else => error.UnexpectedMessage,
        };
    }

    fn round_trip(self: *Client, message: *const protocol.Message) !protocol.Message {
        // Reads are replay-safe and append command IDs make retries idempotent. Two bounded
        // rounds can renew expired connections and visit every configured cluster endpoint
        // without duplicating committed work.
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
            const response = self.connection.recv_message() catch |err| {
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

    fn ensure_connection(self: *Client) !void {
        if (!self.connected) {
            try self.connect_transport();
            return;
        }
        if (self.request_count < protocol.requests_per_connection_max) return;
        assert(self.request_count == protocol.requests_per_connection_max);
        // The server bounds work per connection; renew before sending beyond that contract.
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
        const response = try self.connection.recv_message();
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
    return switch (response) {
        .err => |server_error| switch (server_error.code) {
            .not_leader, .quorum_unavailable, .replication_unavailable => true,
            else => false,
        },
        else => false,
    };
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
}
