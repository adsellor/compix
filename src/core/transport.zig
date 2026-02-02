const std = @import("std");
const Io = std.Io;
const net = Io.net;
const protocol = @import("protocol.zig");
const Allocator = std.mem.Allocator;

const Message = protocol.Message;

pub fn parseAddress(address_str: []const u8) !net.IpAddress {
    return net.IpAddress.parseLiteral(address_str) catch return error.InvalidAddress;
}

pub const Connection = struct {
    stream: net.Stream,
    io: Io,
    allocator: Allocator,
    _read_buf: [8192]u8,
    _write_buf: [8192]u8,
    _reader: net.Stream.Reader,
    _writer: net.Stream.Writer,

    pub fn create(allocator: Allocator, stream: net.Stream, io: Io) !*Connection {
        const self = try allocator.create(Connection);
        self.* = .{
            .stream = stream,
            .io = io,
            .allocator = allocator,
            ._read_buf = undefined,
            ._write_buf = undefined,
            ._reader = undefined,
            ._writer = undefined,
        };
        self._reader = stream.reader(io, &self._read_buf);
        self._writer = stream.writer(io, &self._write_buf);
        return self;
    }

    pub fn sendMessage(self: *Connection, msg: Message) !void {
        const frame = try protocol.encode(self.allocator, msg);
        defer self.allocator.free(frame);
        try self._writer.interface.writeAll(frame);
        try self._writer.interface.flush();
    }

    /// Receive a length-prefixed frame and decode it.
    /// NOTE: Caller must call protocol.deinit on the returned message.
    pub fn recvMessage(self: *Connection) !Message {
        var len_buf: [4]u8 = undefined;
        try self._reader.interface.readSliceAll(&len_buf);

        const payload_len = std.mem.readInt(u32, &len_buf, .big);
        if (payload_len > protocol.MAX_FRAME_SIZE) return error.FrameTooLarge;
        if (payload_len < 2) return error.TruncatedFrame;

        const frame = try self.allocator.alloc(u8, 4 + payload_len);
        defer self.allocator.free(frame);

        @memcpy(frame[0..4], &len_buf);
        try self._reader.interface.readSliceAll(frame[4..]);

        return protocol.decode(self.allocator, frame);
    }

    pub fn destroy(self: *Connection) void {
        self.stream.close(self.io);
        self.allocator.destroy(self);
    }
};

pub const Listener = struct {
    server: net.Server,
    io: Io,
    allocator: Allocator,

    pub fn init(allocator: Allocator, address_str: []const u8, io: Io) !Listener {
        const addr = try parseAddress(address_str);
        const server = try addr.listen(io, .{ .reuse_address = true });
        return .{ .server = server, .io = io, .allocator = allocator };
    }

    pub fn accept(self: *Listener) !*Connection {
        const stream = try self.server.accept(self.io);
        return Connection.create(self.allocator, stream, self.io);
    }

    pub fn getPort(self: *const Listener) u16 {
        return self.server.socket.address.getPort();
    }

    pub fn deinit(self: *Listener) void {
        self.server.deinit(self.io);
    }
};

pub fn dial(allocator: Allocator, address_str: []const u8, io: Io) !*Connection {
    const addr = try parseAddress(address_str);
    const stream = try addr.connect(io, .{ .mode = .stream });
    return Connection.create(allocator, stream, io);
}

test "parseAddress valid" {
    const addr = try parseAddress("127.0.0.1:4200");
    try std.testing.expectEqual(@as(u16, 4200), addr.getPort());
}

test "parseAddress port 0" {
    const addr = try parseAddress("127.0.0.1:0");
    try std.testing.expectEqual(@as(u16, 0), addr.getPort());
}

test "parseAddress no colon defaults to port 0" {
    const addr = try parseAddress("127.0.0.1");
    try std.testing.expectEqual(@as(u16, 0), addr.getPort());
}

test "parseAddress invalid host" {
    const result = parseAddress("not-an-ip");
    try std.testing.expectError(error.InvalidAddress, result);
}

test "parseAddress invalid port" {
    const result = parseAddress("127.0.0.1:abc");
    try std.testing.expectError(error.InvalidAddress, result);
}

test "handshake round-trip over TCP" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try Listener.init(allocator, "127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.getPort();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener) void {
            const conn = l.accept() catch return;
            defer conn.destroy();
            var msg = conn.recvMessage() catch return;
            conn.sendMessage(msg) catch {};
            protocol.deinit(&msg, l.allocator);
        }
    }.run, .{&listener});

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const client = try dial(allocator, addr_str, io);
    defer client.destroy();

    try client.sendMessage(.{ .handshake = .{
        .service_name = "order-service",
        .instance_id = "node-1",
    } });

    var response = try client.recvMessage();
    defer protocol.deinit(&response, allocator);

    try std.testing.expectEqualStrings("order-service", response.handshake.service_name);
    try std.testing.expectEqualStrings("node-1", response.handshake.instance_id);

    server_thread.join();
}

test "pull_request and pull_response over TCP" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try Listener.init(allocator, "127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.getPort();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener) void {
            const conn = l.accept() catch return;
            defer conn.destroy();

            var req = conn.recvMessage() catch return;
            protocol.deinit(&req, l.allocator);

            const entries = [_]protocol.PullResponse.EventEntry{
                .{ .key = "evt:svc:type:0001", .value = "payload-1" },
                .{ .key = "evt:svc:type:0002", .value = "payload-2" },
            };
            conn.sendMessage(.{ .pull_response = .{ .events = &entries } }) catch {};
        }
    }.run, .{&listener});

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const client = try dial(allocator, addr_str, io);
    defer client.destroy();

    try client.sendMessage(.{ .pull_request = .{
        .event_type = "order.created",
        .from_sequence = 5,
        .max_count = 100,
    } });

    var response = try client.recvMessage();
    defer protocol.deinit(&response, allocator);

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

    var listener = try Listener.init(allocator, "127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.getPort();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener) void {
            const conn = l.accept() catch return;
            defer conn.destroy();

            var msg = conn.recvMessage() catch return;
            protocol.deinit(&msg, l.allocator);

            conn.sendMessage(.{ .err = .{
                .code = .unauthorized_event_type,
                .message = "contract violation",
            } }) catch {};
        }
    }.run, .{&listener});

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const client = try dial(allocator, addr_str, io);
    defer client.destroy();

    try client.sendMessage(.{ .handshake = .{
        .service_name = "rogue",
        .instance_id = "x",
    } });

    var response = try client.recvMessage();
    defer protocol.deinit(&response, allocator);

    try std.testing.expectEqual(protocol.ErrorCode.unauthorized_event_type, response.err.code);
    try std.testing.expectEqualStrings("contract violation", response.err.message);

    server_thread.join();
}

test "multiple messages on same connection" {
    const allocator = std.testing.allocator;
    var threaded = Io.Threaded.init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    var listener = try Listener.init(allocator, "127.0.0.1:0", io);
    defer listener.deinit();
    const port = listener.getPort();

    const server_thread = try std.Thread.spawn(.{}, struct {
        fn run(l: *Listener) void {
            const conn = l.accept() catch return;
            defer conn.destroy();
            for (0..3) |_| {
                var msg = conn.recvMessage() catch return;
                conn.sendMessage(msg) catch {};
                protocol.deinit(&msg, l.allocator);
            }
        }
    }.run, .{&listener});

    const addr_str = try std.fmt.allocPrint(allocator, "127.0.0.1:{d}", .{port});
    defer allocator.free(addr_str);

    const client = try dial(allocator, addr_str, io);
    defer client.destroy();

    const names = [_][]const u8{ "svc-a", "svc-b", "svc-c" };
    for (names) |name| {
        try client.sendMessage(.{ .handshake = .{
            .service_name = name,
            .instance_id = "1",
        } });

        var resp = try client.recvMessage();
        defer protocol.deinit(&resp, allocator);
        try std.testing.expectEqualStrings(name, resp.handshake.service_name);
    }

    server_thread.join();
}
