const std = @import("std");
const Io = std.Io;
const net = Io.net;
const auth = @import("auth.zig");
const protocol = @import("protocol.zig");
const limits = @import("limits.zig");

const Message = protocol.Message;
const Aead = std.crypto.aead.chacha_poly.ChaCha20Poly1305;
const secure_header_size: limits.ByteCount = 8;
const secure_overhead: limits.ByteCount = secure_header_size + Aead.tag_length;
const wire_frame_size_max: limits.ByteCount = protocol.frame_size_max + secure_overhead;

pub const SecurityRole = enum {
    client,
    server,
};

const Security = struct {
    send_key: [Aead.key_length]u8,
    receive_key: [Aead.key_length]u8,
    send_sequence: u64 = 0,
    receive_sequence: u64 = 0,
};

comptime {
    std.debug.assert(limits.pull_events_max > 0);
    std.debug.assert(limits.append_events_max == protocol.append_events_max);
    std.debug.assert(limits.read_events_max == protocol.read_events_max);
}

pub fn parse_address(address_str: []const u8) !net.IpAddress {
    return net.IpAddress.parseLiteral(address_str) catch return error.InvalidAddress;
}

pub const Connection = struct {
    stream: net.Stream,
    io: Io,
    _read_buf: [8192]u8,
    _write_buf: [8192]u8,
    _reader: net.Stream.Reader,
    _writer: net.Stream.Writer,
    _frame_read_buf: [wire_frame_size_max]u8,
    _frame_write_buf: [wire_frame_size_max]u8,
    _event_entries: [@import("limits.zig").pull_events_max]protocol.PullResponse.EventEntry,
    _append_events: [limits.append_events_max]protocol.AppendEvent,
    _read_events: [limits.read_events_max]protocol.ReadEvent,
    _security: ?Security,

    pub fn init(
        target: *Connection,
        stream: net.Stream,
        io: Io,
    ) void {
        target.* = .{
            .stream = stream,
            .io = io,
            ._read_buf = undefined,
            ._write_buf = undefined,
            ._reader = undefined,
            ._writer = undefined,
            ._frame_read_buf = undefined,
            ._frame_write_buf = undefined,
            ._event_entries = undefined,
            ._append_events = undefined,
            ._read_events = undefined,
            ._security = null,
        };
        target._reader = stream.reader(io, &target._read_buf);
        target._writer = stream.writer(io, &target._write_buf);
        std.debug.assert(target._frame_read_buf.len == wire_frame_size_max);
        std.debug.assert(target._frame_write_buf.len == wire_frame_size_max);
        std.debug.assert(target._event_entries.len == limits.pull_events_max);
        std.debug.assert(target._append_events.len == limits.append_events_max);
        std.debug.assert(target._read_events.len == limits.read_events_max);
    }

    pub fn send_message(self: *Connection, msg: Message) !void {
        const frame = try protocol.encode_into(&self._frame_write_buf, msg);
        std.debug.assert(frame.len >= protocol.frame_prefix_size + 2);
        std.debug.assert(frame.len <= protocol.frame_size_max);
        const wire = if (self._security != null)
            try self.encrypt_frame(frame)
        else
            frame;
        try self._writer.interface.writeAll(wire);
        try self._writer.interface.flush();
    }

    /// Receives a frame without allocating. Returned slices remain valid until
    /// the next call to recv_message and must not be deinitialized.
    pub fn recv_message(self: *Connection) !Message {
        if (self._security != null) return self.recv_encrypted_message();
        return self.recv_plain_message();
    }

    fn recv_plain_message(self: *Connection) !Message {
        var len_buf: [4]u8 = undefined;
        try self._reader.interface.readSliceAll(&len_buf);

        const payload_len = std.mem.readInt(u32, &len_buf, .big);
        if (payload_len > protocol.frame_payload_size_max) return error.FrameTooLarge;
        if (payload_len < 2) return error.TruncatedFrame;

        const frame_size: limits.ByteCount = 4 + payload_len;
        std.debug.assert(frame_size >= protocol.frame_prefix_size + 2);
        std.debug.assert(frame_size <= protocol.frame_size_max);
        const frame = self._frame_read_buf[0..frame_size];

        @memcpy(frame[0..4], &len_buf);
        try self._reader.interface.readSliceAll(frame[4..]);

        var decode_buffers: protocol.DecodeBuffers = .{
            .pull_events = &self._event_entries,
            .append_events = &self._append_events,
            .read_events = &self._read_events,
        };
        return protocol.decode(frame, &decode_buffers);
    }

    fn encrypt_frame(self: *Connection, frame: []const u8) ![]const u8 {
        const security = &(self._security orelse return error.SecurityNotEnabled);
        if (security.send_sequence == std.math.maxInt(u64))
            return error.SecuritySequenceExhausted;
        const plaintext = frame[protocol.frame_prefix_size..];
        const payload_size: limits.ByteCount = secure_header_size +
            @as(limits.ByteCount, @intCast(plaintext.len)) + Aead.tag_length;
        const wire = self._frame_read_buf[0 .. protocol.frame_prefix_size + payload_size];
        std.mem.writeInt(u32, wire[0..4], payload_size, .big);
        std.mem.writeInt(u64, wire[4..12], security.send_sequence, .big);
        const ciphertext = wire[12..][0..plaintext.len];
        const tag_target = wire[12 + plaintext.len ..][0..Aead.tag_length];
        Aead.encrypt(
            ciphertext,
            tag_target,
            plaintext,
            wire[0..12],
            security_nonce(security.send_sequence),
            security.send_key,
        );
        security.send_sequence += 1;
        return wire;
    }

    fn recv_encrypted_message(self: *Connection) !Message {
        var length_bytes: [4]u8 = undefined;
        try self._reader.interface.readSliceAll(&length_bytes);
        const payload_size = std.mem.readInt(u32, &length_bytes, .big);
        if (payload_size < secure_overhead + 2) return error.TruncatedSecureFrame;
        if (payload_size > protocol.frame_payload_size_max + secure_overhead)
            return error.FrameTooLarge;
        const wire = self._frame_write_buf[0 .. protocol.frame_prefix_size + payload_size];
        @memcpy(wire[0..4], &length_bytes);
        try self._reader.interface.readSliceAll(wire[4..]);
        return self.decrypt_frame(wire);
    }

    fn decrypt_frame(self: *Connection, wire: []const u8) !Message {
        const security = &(self._security orelse return error.SecurityNotEnabled);
        if (wire.len < protocol.frame_prefix_size) return error.TruncatedSecureFrame;
        const payload_size = std.mem.readInt(u32, wire[0..4], .big);
        if (payload_size < secure_overhead + 2) return error.TruncatedSecureFrame;
        if (payload_size > protocol.frame_payload_size_max + secure_overhead)
            return error.FrameTooLarge;
        if (wire.len != protocol.frame_prefix_size + payload_size)
            return error.InvalidSecureFrameLength;
        const sequence = std.mem.readInt(u64, wire[4..12], .big);
        if (sequence != security.receive_sequence) return error.SecuritySequenceMismatch;
        if (security.receive_sequence == std.math.maxInt(u64))
            return error.SecuritySequenceExhausted;
        const ciphertext_length = payload_size - secure_overhead;
        const ciphertext = wire[12..][0..ciphertext_length];
        const tag: [Aead.tag_length]u8 = wire[12 + ciphertext_length ..][0..Aead.tag_length].*;
        const plaintext = self._frame_read_buf[4..][0..ciphertext_length];
        Aead.decrypt(
            plaintext,
            ciphertext,
            tag,
            wire[0..12],
            security_nonce(sequence),
            security.receive_key,
        ) catch return error.AuthenticationFailed;
        std.mem.writeInt(u32, self._frame_read_buf[0..4], ciphertext_length, .big);
        security.receive_sequence += 1;
        var decode_buffers: protocol.DecodeBuffers = .{
            .pull_events = &self._event_entries,
            .append_events = &self._append_events,
            .read_events = &self._read_events,
        };
        return protocol.decode(
            self._frame_read_buf[0 .. protocol.frame_prefix_size + ciphertext_length],
            &decode_buffers,
        );
    }

    pub fn enable_security(
        self: *Connection,
        keys: auth.SessionKeys,
        role: SecurityRole,
    ) !void {
        if (self._security != null) return error.SecurityAlreadyEnabled;
        self._security = switch (role) {
            .client => .{
                .send_key = keys.client_to_server,
                .receive_key = keys.server_to_client,
            },
            .server => .{
                .send_key = keys.server_to_client,
                .receive_key = keys.client_to_server,
            },
        };
    }

    pub fn secure(self: *const Connection) bool {
        return self._security != null;
    }

    /// Reuses the consumed read frame for response assembly. Any message
    /// slices returned by recv_message become invalid after this call.
    pub fn response_scratch(self: *Connection) []u8 {
        return &self._frame_read_buf;
    }

    pub fn response_entries(self: *Connection) []protocol.PullResponse.EventEntry {
        return &self._event_entries;
    }

    pub fn close(self: *Connection) void {
        if (self._security) |*security| {
            std.crypto.secureZero(u8, &security.send_key);
            std.crypto.secureZero(u8, &security.receive_key);
        }
        self._security = null;
        self.stream.close(self.io);
    }
};

fn security_nonce(sequence: u64) [Aead.nonce_length]u8 {
    var nonce: [Aead.nonce_length]u8 = @splat(0);
    std.mem.writeInt(u64, nonce[4..12], sequence, .big);
    return nonce;
}

pub const Listener = struct {
    server: net.Server,
    io: Io,

    pub fn init(address_str: []const u8, io: Io) !Listener {
        const addr = try parse_address(address_str);
        const server = try addr.listen(io, .{ .reuse_address = true });
        return .{ .server = server, .io = io };
    }

    pub fn accept_into(self: *Listener, target: *Connection) !void {
        const stream = try self.server.accept(self.io);
        Connection.init(target, stream, self.io);
    }

    pub fn get_port(self: *const Listener) u16 {
        return self.server.socket.address.getPort();
    }

    /// Stops a blocking accept without closing the socket. Keeping close in
    /// deinit avoids a descriptor-close race with the accepting thread.
    pub fn stop_accepting(self: *Listener) !void {
        const stream: net.Stream = .{ .socket = self.server.socket };
        try stream.shutdown(self.io, .both);
    }

    pub fn deinit(self: *Listener) void {
        self.server.deinit(self.io);
    }
};

pub fn dial_into(
    target: *Connection,
    address_str: []const u8,
    io: Io,
) !void {
    const addr = try parse_address(address_str);
    const stream = try addr.connect(io, .{ .mode = .stream });
    Connection.init(target, stream, io);
}

pub fn dial_into_timeout(
    target: *Connection,
    address_str: []const u8,
    io: Io,
    timeout_ms: u32,
) !void {
    if (timeout_ms == 0) return error.InvalidConnectTimeout;
    var state: DialState = .{
        .target = target,
        .address = address_str,
        .io = io,
        .connected = .init(false),
    };
    var completions: [2]DialCompletion = undefined;
    var select = Io.Select(DialCompletion).init(io, &completions);
    var keep_connection = false;
    defer {
        select.cancelDiscard();
        if (!keep_connection and state.connected.load(.acquire)) target.close();
    }
    try select.concurrent(.connected, dial_task, .{&state});
    try select.concurrent(.deadline, dial_deadline, .{ io, timeout_ms });
    const completion = try select.await();
    switch (completion) {
        .connected => |result| {
            try result;
            keep_connection = true;
        },
        .deadline => |result| {
            try result;
            return error.ConnectDeadlineExceeded;
        },
    }
}

const DialState = struct {
    target: *Connection,
    address: []const u8,
    io: Io,
    connected: std.atomic.Value(bool),
};

const DialCompletion = union(enum) {
    connected: anyerror!void,
    deadline: Io.Cancelable!void,
};

fn dial_task(state: *DialState) anyerror!void {
    try dial_into(state.target, state.address, state.io);
    state.connected.store(true, .release);
}

fn dial_deadline(io: Io, timeout_ms: u32) Io.Cancelable!void {
    try io.sleep(.fromMilliseconds(timeout_ms), .awake);
}

test "parse_address valid" {
    const addr = try parse_address("127.0.0.1:4200");
    try std.testing.expectEqual(@as(u16, 4200), addr.getPort());
}

test "parse_address port 0" {
    const addr = try parse_address("127.0.0.1:0");
    try std.testing.expectEqual(@as(u16, 0), addr.getPort());
}

test "parse_address no colon defaults to port 0" {
    const addr = try parse_address("127.0.0.1");
    try std.testing.expectEqual(@as(u16, 0), addr.getPort());
}

test "parse_address invalid host" {
    const result = parse_address("not-an-ip");
    try std.testing.expectError(error.InvalidAddress, result);
}

test "parse_address invalid port" {
    const result = parse_address("127.0.0.1:abc");
    try std.testing.expectError(error.InvalidAddress, result);
}

test "transport surface requires in-place connection ownership" {
    try std.testing.expect(!@hasField(Connection, "allocator"));
    try std.testing.expect(!@hasDecl(Connection, "create"));
    try std.testing.expect(!@hasDecl(Connection, "destroy"));
    try std.testing.expect(!@hasDecl(Listener, "accept"));
    try std.testing.expect(!@hasDecl(@This(), "dial"));
    try std.testing.expect(@hasDecl(Listener, "accept_into"));
    try std.testing.expect(@hasDecl(@This(), "dial_into"));
    try std.testing.expect(@hasDecl(@This(), "dial_into_timeout"));
}

test "secure frames encrypt, authenticate, and enforce sequence order" {
    const keys: auth.SessionKeys = .{
        .client_to_server = @splat(0x41),
        .server_to_client = @splat(0x52),
    };
    var sender: Connection = undefined;
    sender._security = null;
    try sender.enable_security(keys, .client);
    var receiver: Connection = undefined;
    receiver._security = null;
    try receiver.enable_security(keys, .server);

    const plaintext = try protocol.encode_into(&sender._frame_write_buf, .{
        .handshake = .{
            .service_name = "plaintext-must-not-appear-on-wire",
            .instance_id = "secure-test",
        },
    });
    const first_encrypted = try sender.encrypt_frame(plaintext);
    var first_storage: [wire_frame_size_max]u8 = undefined;
    @memcpy(first_storage[0..first_encrypted.len], first_encrypted);
    const first = first_storage[0..first_encrypted.len];
    try std.testing.expect(std.mem.indexOf(
        u8,
        first,
        "plaintext-must-not-appear-on-wire",
    ) == null);
    const decoded = try receiver.decrypt_frame(first);
    try std.testing.expectEqualStrings(
        "plaintext-must-not-appear-on-wire",
        decoded.handshake.service_name,
    );
    try std.testing.expectError(error.SecuritySequenceMismatch, receiver.decrypt_frame(first));

    const second_plaintext = try protocol.encode_into(&sender._frame_write_buf, .{
        .handshake = .{ .service_name = "second", .instance_id = "secure-test" },
    });
    const second_encrypted = try sender.encrypt_frame(second_plaintext);
    var second_storage: [wire_frame_size_max]u8 = undefined;
    @memcpy(second_storage[0..second_encrypted.len], second_encrypted);
    const second = second_storage[0..second_encrypted.len];

    var reordered: Connection = undefined;
    reordered._security = null;
    try reordered.enable_security(keys, .server);
    try std.testing.expectError(error.SecuritySequenceMismatch, reordered.decrypt_frame(second));

    var tampered_storage = first_storage;
    tampered_storage[12] ^= 1;
    var tampered: Connection = undefined;
    tampered._security = null;
    try tampered.enable_security(keys, .server);
    try std.testing.expectError(
        error.AuthenticationFailed,
        tampered.decrypt_frame(tampered_storage[0..first.len]),
    );

    sender._security.?.send_sequence = std.math.maxInt(u64);
    try std.testing.expectError(error.SecuritySequenceExhausted, sender.encrypt_frame(plaintext));
}

test "handshake round-trip over TCP" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try Listener.init("127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.get_port();
    var server_connection: Connection = undefined;

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener, conn: *Connection) void {
            l.accept_into(conn) catch return;
            defer conn.close();
            const msg = conn.recv_message() catch return;
            conn.send_message(msg) catch {};
        }
    }.run, .{ &listener, &server_connection });

    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(&address_buffer, "127.0.0.1:{d}", .{port});
    var client: Connection = undefined;
    try dial_into(&client, address, io);
    defer client.close();

    try client.send_message(.{ .handshake = .{
        .service_name = "order-service",
        .instance_id = "node-1",
    } });

    const response = try client.recv_message();

    try std.testing.expectEqualStrings("order-service", response.handshake.service_name);
    try std.testing.expectEqualStrings("node-1", response.handshake.instance_id);

    server_thread.join();
}

test "pull_request and pull_response over TCP" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try Listener.init("127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.get_port();
    var server_connection: Connection = undefined;

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener, conn: *Connection) void {
            l.accept_into(conn) catch return;
            defer conn.close();

            _ = conn.recv_message() catch return;

            const entries = [_]protocol.PullResponse.EventEntry{
                .{ .key = "evt:svc:type:0001", .value = "payload-1" },
                .{ .key = "evt:svc:type:0002", .value = "payload-2" },
            };
            conn.send_message(.{ .pull_response = .{ .events = &entries } }) catch {};
        }
    }.run, .{ &listener, &server_connection });

    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(&address_buffer, "127.0.0.1:{d}", .{port});
    var client: Connection = undefined;
    try dial_into(&client, address, io);
    defer client.close();

    try client.send_message(.{ .pull_request = .{
        .event_type = "order.created",
        .from_sequence = 5,
        .max_count = 100,
    } });

    const response = try client.recv_message();

    try std.testing.expectEqual(@as(usize, 2), response.pull_response.events.len);
    try std.testing.expectEqualStrings("evt:svc:type:0001", response.pull_response.events[0].key);
    try std.testing.expectEqualStrings("payload-2", response.pull_response.events[1].value);

    server_thread.join();
}

test "error message over TCP" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try Listener.init("127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.get_port();
    var server_connection: Connection = undefined;

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener, conn: *Connection) void {
            l.accept_into(conn) catch return;
            defer conn.close();

            _ = conn.recv_message() catch return;

            conn.send_message(.{ .err = .{
                .code = .unauthorized_event_type,
                .message = "contract violation",
            } }) catch {};
        }
    }.run, .{ &listener, &server_connection });

    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(&address_buffer, "127.0.0.1:{d}", .{port});
    var client: Connection = undefined;
    try dial_into(&client, address, io);
    defer client.close();

    try client.send_message(.{ .handshake = .{
        .service_name = "rogue",
        .instance_id = "x",
    } });

    const response = try client.recv_message();

    try std.testing.expectEqual(protocol.ErrorCode.unauthorized_event_type, response.err.code);
    try std.testing.expectEqualStrings("contract violation", response.err.message);

    server_thread.join();
}

test "multiple messages on same connection" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try Listener.init("127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.get_port();
    var server_connection: Connection = undefined;

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener, conn: *Connection) void {
            l.accept_into(conn) catch return;
            defer conn.close();
            for (0..3) |_| {
                const msg = conn.recv_message() catch return;
                conn.send_message(msg) catch {};
            }
        }
    }.run, .{ &listener, &server_connection });

    var address_buffer: [32]u8 = undefined;
    const address = try std.fmt.bufPrint(&address_buffer, "127.0.0.1:{d}", .{port});
    var client: Connection = undefined;
    try dial_into(&client, address, io);
    defer client.close();

    const names = [_][]const u8{ "svc-a", "svc-b", "svc-c" };
    for (names) |name| {
        try client.send_message(.{ .handshake = .{
            .service_name = name,
            .instance_id = "1",
        } });

        const resp = try client.recv_message();
        try std.testing.expectEqualStrings(name, resp.handshake.service_name);
    }

    server_thread.join();
}
