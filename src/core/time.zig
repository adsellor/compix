const std = @import("std");
const Io = std.Io;

pub const Instant = struct {
    timestamp: Timestamp,
    raw: Io.Timestamp,

    pub const Timestamp = struct {
        sec: i64,
        nsec: i64,
    };

    pub fn now(io: Io) Instant {
        const ts = Io.Clock.real.now(io);
        const total_ns: i128 = ts.nanoseconds;
        const sec: i64 = @intCast(@divFloor(total_ns, std.time.ns_per_s));
        const rem: i64 = @intCast(@mod(total_ns, std.time.ns_per_s));
        return .{
            .timestamp = .{
                .sec = sec,
                .nsec = rem,
            },
            .raw = ts,
        };
    }

    pub fn since(self: Instant, earlier: Instant) u64 {
        const duration = earlier.raw.durationTo(self.raw);
        return @intCast(@max(duration.nanoseconds, 0));
    }
};
