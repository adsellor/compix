const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;

const limits = @import("limits.zig");
const protocol = @import("protocol.zig");

comptime {
    assert(limits.subscription_event_types_max > 0);
    assert(limits.subscription_event_types_bytes_max >= limits.subscription_event_types_max);
    assert(protocol.stream_id_length_max <= std.math.maxInt(u16));
}

pub const Filter = struct {
    count: u8,
    types: [limits.subscription_event_types_max][]const u8,
    bytes: [limits.subscription_event_types_bytes_max]u8,

    pub fn set(filter: *Filter, types: []const []const u8) error{FilterTooLarge}!void {
        if (types.len > limits.subscription_event_types_max) return error.FilterTooLarge;
        var bytes_used: limits.ByteCount = 0;
        for (types, 0..) |event_type, index| {
            assert(event_type.len > 0);
            if (event_type.len > filter.bytes.len - bytes_used) return error.FilterTooLarge;
            const target = filter.bytes[bytes_used..][0..event_type.len];
            @memcpy(target, event_type);
            filter.types[index] = target;
            bytes_used += @intCast(event_type.len);
        }
        filter.count = @intCast(types.len);
        assert(filter.count == types.len);
        assert(bytes_used <= filter.bytes.len);
    }

    /// Borrows `filter`; empty means every event type.
    pub fn event_types(filter: *const Filter) []const []const u8 {
        assert(filter.count <= limits.subscription_event_types_max);
        return filter.types[0..filter.count];
    }
};

/// What a subscription reads: the whole ordered log or one stream, and which event types.
/// Copied out of the Subscribe frame, because every encrypted send reuses the receive buffer.
pub const Target = struct {
    stream_id_length: u16,
    stream_id_bytes: [protocol.stream_id_length_max]u8,
    filter: Filter,

    pub fn set(
        target: *Target,
        stream_id_source: []const u8,
        event_types: []const []const u8,
    ) error{ StreamIdTooLong, FilterTooLarge }!void {
        if (stream_id_source.len > target.stream_id_bytes.len) return error.StreamIdTooLong;
        @memcpy(target.stream_id_bytes[0..stream_id_source.len], stream_id_source);
        target.stream_id_length = @intCast(stream_id_source.len);
        try target.filter.set(event_types);
        assert(target.stream_id_length == stream_id_source.len);
    }

    /// Borrows `target`; empty means the whole ordered log.
    pub fn stream_id(target: *const Target) []const u8 {
        assert(target.stream_id_length <= target.stream_id_bytes.len);
        return target.stream_id_bytes[0..target.stream_id_length];
    }
};

/// The state one connection slot shares between its subscription pusher, which owns the
/// connection, and whoever produces work for it: the store's owner after a commit, shutdown.
///
/// It lives in the connection pool, so producers find subscribers by walking the pool without
/// a registry, and each subscriber has its own futex word, so a commit wakes only pushers that
/// sleep. Only the pusher writes `target` and `sleeping`; producers only call `notify`.
pub const Subscriber = struct {
    active: std.atomic.Value(bool),
    sleeping: std.atomic.Value(bool),
    /// Futex word: bumped on every notification, so a wait on a stale value returns at once.
    wake_epoch: std.atomic.Value(u32),
    target: Target,

    pub const idle: Subscriber = .{
        .active = .init(false),
        .sleeping = .init(false),
        .wake_epoch = .init(0),
        .target = undefined,
    };

    pub fn open(
        subscriber: *Subscriber,
        stream_id: []const u8,
        event_types: []const []const u8,
    ) error{ StreamIdTooLong, FilterTooLarge }!void {
        assert(!subscriber.active.load(.acquire));
        try subscriber.target.set(stream_id, event_types);
        subscriber.sleeping.store(false, .release);
        // NOTE: last, so a producer that sees `active` sees a fully opened subscriber.
        subscriber.active.store(true, .seq_cst);
    }

    pub fn shut(subscriber: *Subscriber) void {
        assert(subscriber.active.load(.acquire));
        assert(!subscriber.sleeping.load(.acquire));
        subscriber.active.store(false, .seq_cst);
    }

    pub fn notify(subscriber: *Subscriber, io: Io) void {
        // NOTE: seq_cst pairs with `wait`: either this load sees `sleeping`, or the pusher's
        // futex compare sees the bumped epoch and does not sleep. No wakeup is lost.
        _ = subscriber.wake_epoch.fetchAdd(1, .seq_cst);
        if (subscriber.sleeping.load(.seq_cst)) {
            io.futexWake(u32, &subscriber.wake_epoch.raw, 1);
        }
    }

    /// Read before checking for work; pass to `wait` so a notification in between is not lost.
    pub fn epoch(subscriber: *const Subscriber) u32 {
        return subscriber.wake_epoch.load(.seq_cst);
    }

    pub fn wait(
        subscriber: *Subscriber,
        io: Io,
        epoch_seen: u32,
        deadline: Io.Clock.Timestamp,
    ) Io.Cancelable!void {
        assert(subscriber.active.load(.acquire));
        subscriber.sleeping.store(true, .seq_cst);
        defer subscriber.sleeping.store(false, .seq_cst);
        try io.futexWaitTimeout(
            u32,
            &subscriber.wake_epoch.raw,
            epoch_seen,
            .{ .deadline = deadline },
        );
    }
};

test "filter copies event types and enforces its byte budget" {
    var filter: Filter = undefined;
    var source = [_]u8{ 'a', '.', 'b' };
    try filter.set(&.{ source[0..], "c.d" });
    source[0] = 'x';
    try std.testing.expectEqual(@as(usize, 2), filter.event_types().len);
    try std.testing.expectEqualStrings("a.b", filter.event_types()[0]);
    try std.testing.expectEqualStrings("c.d", filter.event_types()[1]);

    try filter.set(&.{});
    try std.testing.expectEqual(@as(usize, 0), filter.event_types().len);

    const half: [limits.subscription_event_types_bytes_max / 2 + 1]u8 = @splat('t');
    try std.testing.expectError(error.FilterTooLarge, filter.set(&.{ &half, &half }));
    const too_many: [limits.subscription_event_types_max + 1][]const u8 = @splat("t");
    try std.testing.expectError(error.FilterTooLarge, filter.set(&too_many));
}

test "target owns its stream id copy" {
    var target: Target = undefined;
    var source = [_]u8{ 'o', '/', '1' };
    try target.set(&source, &.{"object.created"});
    source[2] = '2';
    try std.testing.expectEqualStrings("o/1", target.stream_id());
    try std.testing.expectEqualStrings("object.created", target.filter.event_types()[0]);

    try target.set("", &.{});
    try std.testing.expectEqualStrings("", target.stream_id());
    const too_long: [protocol.stream_id_length_max + 1]u8 = @splat('s');
    try std.testing.expectError(error.StreamIdTooLong, target.set(&too_long, &.{}));
}

test "a notification before the wait is never lost" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();
    var subscriber: Subscriber = .idle;
    try subscriber.open("", &.{});
    defer subscriber.shut();

    const seen = subscriber.epoch();
    subscriber.notify(io);
    // Without the epoch the pusher would sleep the full hour here.
    const deadline: Io.Clock.Timestamp = .fromNow(io, .{
        .raw = .fromSeconds(3600),
        .clock = .awake,
    });
    try subscriber.wait(io, seen, deadline);
    try std.testing.expect(subscriber.epoch() != seen);
    try std.testing.expect(!subscriber.sleeping.load(.acquire));
}
