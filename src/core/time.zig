const std = @import("std");
const posix = std.posix;

pub const Instant = struct {
    timestamp: Timestamp,

    pub const Timestamp = struct {
        sec: i64,
        nsec: i64,
    };

    pub fn now() error{Unsupported}!Instant {
        var ts: posix.timespec = .{ .sec = 0, .nsec = 0 };
        const rc = posix.system.clock_gettime(.REALTIME, &ts);
        if (posix.errno(rc) != .SUCCESS) return error.Unsupported;
        return .{
            .timestamp = .{
                .sec = @intCast(ts.sec),
                .nsec = @intCast(ts.nsec),
            },
        };
    }

    pub fn since(self: Instant, earlier: Instant) u64 {
        const s: i64 = self.timestamp.sec - earlier.timestamp.sec;
        const ns: i64 = self.timestamp.nsec - earlier.timestamp.nsec;
        const total_ns: i64 = s * std.time.ns_per_s + ns;
        return @intCast(@max(total_ns, 0));
    }
};
