const std = @import("std");
const print = std.debug.print;

const store = @import("core/store.zig");
const event = @import("core/event.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    var event_store = try store.EventStore.init(allocator, "data/events.wal", io);
    defer event_store.deinit();

    const now = try std.time.Instant.now();
    const sample_value = try event.EventValue.init(allocator, 1, "compix", "system.startup", "{\"timestamp\":\"now\"}", now.timestamp.nsec);
    try event_store.put("system:startup", sample_value);

    if (event_store.get("system:startup")) |_| {
        print("Event store is working correctly!\n", .{});
    } else {
        print("Event store test failed!\n", .{});
    }
}
