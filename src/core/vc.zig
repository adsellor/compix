const std = @import("std");
const assert = std.debug.assert;

pub const entry_max = 16;
pub const encoded_length_max = entry_max * (20 + 1 + 20 + 1);

pub fn nodeKey(name: []const u8) u64 {
    var hash: u64 = 14695981039346656037;
    for (name) |byte| {
        hash ^= byte;
        hash *%= 1099511628211;
    }
    return hash;
}

pub const Entry = struct {
    node: u64,
    counter: u64,
};

// NOTE: entries stay sorted so replicas encode identical text. The text never contains `|`
// because it rides inside an `EventValue` field.
pub const Clock = struct {
    entries: [entry_max]Entry = undefined,
    len: usize = 0,

    pub fn get(self: *const Clock, node: u64) ?u64 {
        const index = self.find(node) orelse return null;
        return self.entries[index].counter;
    }

    pub fn bump(self: *Clock, node: u64, counter: u64) error{ClockFull}!void {
        if (self.find(node)) |index| {
            if (counter > self.entries[index].counter) self.entries[index].counter = counter;
            return;
        }
        if (self.len == entry_max) return error.ClockFull;
        var index: usize = self.len;
        while (index > 0 and self.entries[index - 1].node > node) : (index -= 1) {
            self.entries[index] = self.entries[index - 1];
        }
        self.entries[index] = .{ .node = node, .counter = counter };
        self.len += 1;
    }

    /// Fails without mutating when the union would exceed capacity.
    pub fn observe(self: *Clock, other: *const Clock) error{ClockFull}!void {
        var fresh: usize = 0;
        for (other.entries[0..other.len]) |entry| {
            if (self.find(entry.node) == null) fresh += 1;
        }
        if (self.len + fresh > entry_max) return error.ClockFull;
        for (other.entries[0..other.len]) |entry| try self.bump(entry.node, entry.counter);
    }

    pub fn dominates(self: *const Clock, other: *const Clock) bool {
        for (other.entries[0..other.len]) |entry| {
            const known = self.get(entry.node) orelse return false;
            if (known < entry.counter) return false;
        }
        return true;
    }

    pub fn concurrent(a: *const Clock, b: *const Clock) bool {
        return !a.dominates(b) and !b.dominates(a);
    }

    pub fn equal(a: *const Clock, b: *const Clock) bool {
        if (a.len != b.len) return false;
        for (a.entries[0..a.len], b.entries[0..b.len]) |left, right| {
            if (left.node != right.node or left.counter != right.counter) return false;
        }
        return true;
    }

    pub fn encodeInto(self: *const Clock, buffer: []u8) error{BufferTooSmall}![]const u8 {
        var used: usize = 0;
        for (self.entries[0..self.len], 0..) |entry, index| {
            if (index > 0) {
                if (used == buffer.len) return error.BufferTooSmall;
                buffer[used] = ',';
                used += 1;
            }
            const written = std.fmt.bufPrint(
                buffer[used..],
                "{d}={d}",
                .{ entry.node, entry.counter },
            ) catch return error.BufferTooSmall;
            used += written.len;
        }
        assert(used <= encoded_length_max);
        return buffer[0..used];
    }

    pub const DecodeError = error{ InvalidFormat, ClockFull };

    pub fn decode(text: []const u8) DecodeError!Clock {
        var clock: Clock = .{};
        if (text.len == 0) return clock;
        var previous_node: ?u64 = null;
        var iter = std.mem.splitScalar(u8, text, ',');
        while (iter.next()) |pair| {
            const separator = std.mem.indexOfScalar(u8, pair, '=') orelse
                return error.InvalidFormat;
            if (separator == 0 or separator + 1 == pair.len) return error.InvalidFormat;
            const node = std.fmt.parseInt(u64, pair[0..separator], 10) catch
                return error.InvalidFormat;
            const counter = std.fmt.parseInt(u64, pair[separator + 1 ..], 10) catch
                return error.InvalidFormat;
            if (previous_node) |previous| {
                if (node <= previous) return error.InvalidFormat;
            }
            try clock.bump(node, counter);
            previous_node = node;
        }
        return clock;
    }

    pub fn validateText(text: []const u8) bool {
        _ = decode(text) catch return false;
        return true;
    }

    fn find(self: *const Clock, node: u64) ?usize {
        var low: usize = 0;
        var high: usize = self.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.math.order(self.entries[mid].node, node)) {
                .eq => return mid,
                .lt => low = mid + 1,
                .gt => high = mid,
            }
        }
        return null;
    }
};

test "bump inserts sorted and takes the max" {
    var clock: Clock = .{};
    try clock.bump(nodeKey("beta"), 4);
    try clock.bump(nodeKey("alpha"), 2);
    try std.testing.expectEqual(@as(usize, 2), clock.len);
    try std.testing.expect(clock.entries[0].node < clock.entries[1].node);
    try std.testing.expectEqual(
        @as(u64, 2),
        clock.get(nodeKey("alpha")).?,
    );
    try std.testing.expectEqual(@as(u64, 4), clock.get(nodeKey("beta")).?);

    try clock.bump(nodeKey("beta"), 2);
    try std.testing.expectEqual(@as(u64, 4), clock.get(nodeKey("beta")).?);
    try clock.bump(nodeKey("beta"), 9);
    try std.testing.expectEqual(@as(u64, 9), clock.get(nodeKey("beta")).?);
    try std.testing.expect(clock.get(nodeKey("gamma")) == null);
}

test "observe merges element-wise and rejects capacity overflow atomically" {
    var a: Clock = .{};
    try a.bump(1, 5);
    try a.bump(2, 1);
    var b: Clock = .{};
    try b.bump(2, 7);
    try b.bump(3, 3);
    try a.observe(&b);
    try std.testing.expectEqual(@as(u64, 5), a.get(1).?);
    try std.testing.expectEqual(@as(u64, 7), a.get(2).?);
    try std.testing.expectEqual(@as(u64, 3), a.get(3).?);

    var full: Clock = .{};
    for (0..entry_max) |node| try full.bump(@intCast(node), 1);
    var outsider: Clock = .{};
    try outsider.bump(1000, 1);
    try std.testing.expectError(error.ClockFull, full.observe(&outsider));
    try std.testing.expectEqual(@as(u64, 1), full.get(0).?);
    try std.testing.expect(full.get(1000) == null);
}

test "partial order: dominates and concurrent" {
    var a: Clock = .{};
    try a.bump(1, 3);
    var b: Clock = .{};
    try b.bump(1, 2);
    try std.testing.expect(a.dominates(&b));
    try std.testing.expect(!b.dominates(&a));

    var c: Clock = .{};
    try c.bump(2, 1);
    try std.testing.expect(Clock.concurrent(&a, &c));

    var d: Clock = .{};
    try std.testing.expect(a.dominates(&d));
    try std.testing.expect(d.dominates(&d));
    try std.testing.expect(!d.dominates(&a));
}

test "encode is deterministic and round-trips through decode" {
    var clock: Clock = .{};
    try clock.bump(nodeKey("beta"), 44);
    try clock.bump(nodeKey("alpha"), 7);

    var first_buffer: [encoded_length_max]u8 = undefined;
    var second_buffer: [encoded_length_max]u8 = undefined;
    const first = try clock.encodeInto(&first_buffer);
    const second = try clock.encodeInto(&second_buffer);
    try std.testing.expectEqualStrings(first, second);
    try std.testing.expect(std.mem.indexOfScalar(u8, first, '|') == null);

    const decoded = try Clock.decode(first);
    try std.testing.expect(Clock.equal(&clock, &decoded));

    const empty = try (Clock{}).encodeInto(&first_buffer);
    try std.testing.expectEqualStrings("", empty);
    try std.testing.expect(Clock.equal(&(try Clock.decode(empty)), &.{}));
}

test "decode rejects malformed and unordered text" {
    try std.testing.expectError(error.InvalidFormat, Clock.decode("1=2,"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode(",1=2"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("1=2=3"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("=5"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("1="));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("x=1"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("1=o"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("5=1,2=2"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("2=2,2=3"));
    try std.testing.expectError(error.InvalidFormat, Clock.decode("1|2"));
    try std.testing.expectError(error.ClockFull, blk: {
        var text: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer text.deinit();
        for (0..entry_max + 1) |node| {
            if (node > 0) text.writer.print(",", .{}) catch unreachable;
            text.writer.print("{d}=1", .{node}) catch unreachable;
        }
        break :blk Clock.decode(text.written());
    });
    try std.testing.expect(Clock.validateText("12=0,34=56"));
    try std.testing.expect(!Clock.validateText("12=a"));
}

test "capacity bound rejects unknown nodes but accepts known ones" {
    var clock: Clock = .{};
    for (0..entry_max) |node| try clock.bump(@intCast(node + 1), 0);
    try std.testing.expectError(error.ClockFull, clock.bump(entry_max + 1, 0));
    try clock.bump(1, 6);
    try std.testing.expectEqual(@as(u64, 6), clock.get(1).?);
}

test "nodeKey is deterministic and order sensitive" {
    try std.testing.expectEqual(nodeKey("fugue-alpha"), nodeKey("fugue-alpha"));
    try std.testing.expect(nodeKey("fugue-alpha") != nodeKey("fugue-beta"));
}
