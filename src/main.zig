const std = @import("std");
const print = std.debug.print;

const store = @import("core/store.zig");
const event = @import("core/event.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var event_store = try store.HybridEventStore.init(allocator, "data/events.wal");
    defer event_store.deinit();

    const sample_value = try event.EventValue.init(allocator, 1, "system.startup", "{\"timestamp\":\"now\"}", std.time.timestamp());
    try event_store.put("system:startup", sample_value);

    if (event_store.get("system:startup")) |found_value| {
        var value = found_value;
        defer value.deinit(allocator);
        print("Event store is working correctly!\n", .{});
    } else {
        print("Event store test failed!\n", .{});
    }
}
