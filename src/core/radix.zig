const std = @import("std");
const assert = std.debug.assert;
const event = @import("event.zig");
const fixed = @import("fixed.zig");
const limits = @import("limits.zig");
const EventValue = event.EventValue;

const root_index: limits.NodeIndex = 0;

comptime {
    assert(limits.key_length_max > 0);
    assert(limits.radix_nodes_max >= 2 * limits.memtable_entries_max + 1);
}

pub const RadixNode = struct {
    key_fragment: []const u8,
    value: ?EventValue,
    first_child: ?limits.NodeIndex,
    next_sibling: ?limits.NodeIndex,
    is_terminal: bool,
};

const ChildMatch = struct {
    index: limits.NodeIndex,
    common_length: limits.ByteCount,
};

const ScanFrame = struct {
    next_child: ?limits.NodeIndex,
    key_length_before: limits.ByteCount,
};

pub const RadixTree = struct {
    allocator: std.mem.Allocator,
    nodes: fixed.List(RadixNode),
    node_storage: []RadixNode,
    bytes: fixed.ByteArena,
    byte_storage: []u8,
    size: limits.NodeCount,

    pub fn init(allocator: std.mem.Allocator) !RadixTree {
        const node_storage = try allocator.alloc(RadixNode, limits.radix_nodes_max);
        errdefer allocator.free(node_storage);
        const byte_storage = try allocator.alloc(u8, limits.memtable_bytes_max);
        errdefer allocator.free(byte_storage);
        var nodes = try fixed.List(RadixNode).init(node_storage);
        try nodes.append(.{
            .key_fragment = "",
            .value = null,
            .first_child = null,
            .next_sibling = null,
            .is_terminal = false,
        });
        return .{
            .allocator = allocator,
            .nodes = nodes,
            .node_storage = node_storage,
            .bytes = try fixed.ByteArena.init(byte_storage),
            .byte_storage = byte_storage,
            .size = 0,
        };
    }

    pub fn deinit(self: *RadixTree) void {
        self.allocator.free(self.byte_storage);
        self.allocator.free(self.node_storage);
    }

    pub fn insert(self: *RadixTree, key: []const u8, value: EventValue) !void {
        assert(key.len > 0);
        if (key.len > limits.key_length_max) return error.KeyTooLong;
        const existing_index = self.find_exact_node(key);
        const key_bytes: limits.ByteCount = if (existing_index == null)
            try limits.byte_count(key.len)
        else
            0;
        const value_bytes = try value_size(value);
        const bytes_required = std.math.add(
            limits.ByteCount,
            key_bytes,
            value_bytes,
        ) catch return error.RadixByteCapacityExceeded;
        if (bytes_required > self.bytes.remaining()) return error.RadixByteCapacityExceeded;

        if (existing_index) |node_index| {
            const node = &self.nodes.items()[node_index];
            if (!node.is_terminal) self.size += 1;
            node.value = self.store_value(value);
            node.is_terminal = true;
            return;
        }
        if (self.nodes.capacity() - self.nodes.count < 2)
            return error.RadixNodeCapacityExceeded;
        self.insert_new(key, value);
    }

    fn insert_new(self: *RadixTree, key: []const u8, value: EventValue) void {
        const size_before = self.size;
        var node_index = root_index;
        var remaining = key;
        var depth: limits.ByteCount = 0;
        while (remaining.len > 0) {
            assert(depth <= limits.key_length_max);
            depth += 1;
            if (self.find_child(node_index, remaining)) |match| {
                if (match.common_length < self.nodes.items()[match.index].key_fragment.len) {
                    self.split_child(match.index, match.common_length);
                }
                node_index = match.index;
                remaining = remaining[match.common_length..];
            } else {
                const fragment = self.bytes.write(remaining) catch unreachable;
                const stored = self.store_value(value);
                const parent = &self.nodes.items()[node_index];
                const child_index = self.nodes.count;
                self.nodes.append(.{
                    .key_fragment = fragment,
                    .value = stored,
                    .first_child = null,
                    .next_sibling = parent.first_child,
                    .is_terminal = true,
                }) catch unreachable;
                parent.first_child = child_index;
                self.size += 1;
                assert(self.size == size_before + 1);
                return;
            }
        }
        const node = &self.nodes.items()[node_index];
        assert(!node.is_terminal);
        node.value = self.store_value(value);
        node.is_terminal = true;
        self.size += 1;
        assert(self.size == size_before + 1);
    }

    fn split_child(
        self: *RadixTree,
        child_index: limits.NodeIndex,
        split_position: limits.ByteCount,
    ) void {
        const previous = self.nodes.items_const()[child_index];
        assert(split_position > 0);
        assert(split_position < previous.key_fragment.len);
        const suffix_index = self.nodes.count;
        self.nodes.append(.{
            .key_fragment = previous.key_fragment[split_position..],
            .value = previous.value,
            .first_child = previous.first_child,
            .next_sibling = null,
            .is_terminal = previous.is_terminal,
        }) catch unreachable;
        const child = &self.nodes.items()[child_index];
        child.key_fragment = previous.key_fragment[0..split_position];
        child.value = null;
        child.first_child = suffix_index;
        child.is_terminal = false;
        assert(child.next_sibling == previous.next_sibling);
    }

    pub fn get(self: *const RadixTree, key: []const u8) ?*const EventValue {
        assert(key.len > 0);
        if (key.len > limits.key_length_max) return null;
        const node_index = self.find_exact_node(key) orelse return null;
        const node = &self.nodes.items_const()[node_index];
        if (!node.is_terminal) return null;
        assert(node.value != null);
        return &node.value.?;
    }

    fn find_exact_node(self: *const RadixTree, key: []const u8) ?limits.NodeIndex {
        var node_index = root_index;
        var remaining = key;
        var depth: limits.ByteCount = 0;
        while (remaining.len > 0) {
            assert(depth <= limits.key_length_max);
            depth += 1;
            const match = self.find_child(node_index, remaining) orelse return null;
            const child = &self.nodes.items_const()[match.index];
            if (match.common_length != child.key_fragment.len) return null;
            node_index = match.index;
            remaining = remaining[match.common_length..];
        }
        return node_index;
    }

    fn find_child(
        self: *const RadixTree,
        node_index: limits.NodeIndex,
        remaining: []const u8,
    ) ?ChildMatch {
        var child_index = self.nodes.items_const()[node_index].first_child;
        while (child_index) |index| {
            const child = &self.nodes.items_const()[index];
            const common_length = common_prefix_length(remaining, child.key_fragment);
            if (common_length > 0) return .{
                .index = index,
                .common_length = common_length,
            };
            child_index = child.next_sibling;
        }
        return null;
    }

    pub fn visit_prefix(
        self: *const RadixTree,
        prefix: []const u8,
        context: anytype,
        comptime visit: anytype,
    ) !event.VisitControl {
        assert(prefix.len <= limits.key_length_max);
        var key_buffer: [limits.key_length_max]u8 = undefined;
        var key_length: limits.ByteCount = 0;
        var stack: [limits.key_length_max + 1]ScanFrame = undefined;
        var stack_count: limits.ByteCount = 0;
        const root_control = try self.scan_enter(
            root_index,
            prefix,
            &key_buffer,
            &key_length,
            &stack,
            &stack_count,
            context,
            visit,
        );
        if (root_control == .stop) return .stop;
        while (stack_count > 0) {
            const frame = &stack[stack_count - 1];
            if (frame.next_child) |child_index| {
                frame.next_child = self.nodes.items_const()[child_index].next_sibling;
                const control = try self.scan_enter(
                    child_index,
                    prefix,
                    &key_buffer,
                    &key_length,
                    &stack,
                    &stack_count,
                    context,
                    visit,
                );
                if (control == .stop) return .stop;
            } else {
                key_length = frame.key_length_before;
                stack_count -= 1;
            }
        }
        assert(key_length == 0);
        return .continue_scan;
    }

    fn scan_enter(
        self: *const RadixTree,
        node_index: limits.NodeIndex,
        prefix: []const u8,
        key_buffer: []u8,
        key_length: *limits.ByteCount,
        stack: []ScanFrame,
        stack_count: *limits.ByteCount,
        context: anytype,
        comptime visit: anytype,
    ) !event.VisitControl {
        const node = &self.nodes.items_const()[node_index];
        const key_length_before = key_length.*;
        const fragment_length = limits.byte_count(node.key_fragment.len) catch unreachable;
        assert(key_length.* + fragment_length <= key_buffer.len);
        @memcpy(key_buffer[key_length.*..][0..fragment_length], node.key_fragment);
        key_length.* += fragment_length;
        const key = key_buffer[0..key_length.*];
        const pruned = if (key.len >= prefix.len)
            !std.mem.startsWith(u8, key, prefix)
        else
            !std.mem.startsWith(u8, prefix, key);
        if (pruned) {
            key_length.* = key_length_before;
            return .continue_scan;
        }
        if (node.is_terminal and key.len >= prefix.len) {
            assert(node.value != null);
            if (try visit(context, key, node.value.?) == .stop) return .stop;
        }
        assert(stack_count.* < stack.len);
        stack[stack_count.*] = .{
            .next_child = node.first_child,
            .key_length_before = key_length_before,
        };
        stack_count.* += 1;
        return .continue_scan;
    }

    fn store_value(self: *RadixTree, value: EventValue) EventValue {
        return .{
            .sequence = value.sequence,
            .event_type = self.bytes.write(value.event_type) catch unreachable,
            .payload = self.bytes.write(value.payload) catch unreachable,
            .timestamp = value.timestamp,
            .origin_service = self.bytes.write(value.origin_service) catch unreachable,
            .signature = self.bytes.write(value.signature) catch unreachable,
        };
    }
};

fn value_size(value: EventValue) !limits.ByteCount {
    return try limits.byte_count(
        value.event_type.len + value.payload.len + value.origin_service.len +
            value.signature.len,
    );
}

fn common_prefix_length(a: []const u8, b: []const u8) limits.ByteCount {
    var index: limits.ByteCount = 0;
    while (index < a.len and index < b.len and a[index] == b[index]) {
        index += 1;
    }
    assert(index <= a.len);
    assert(index <= b.len);
    return index;
}

const CountContext = struct {
    count: limits.CollectionCount = 0,

    fn visit(self: *CountContext, _: []const u8, _: EventValue) !event.VisitControl {
        self.count += 1;
        return .continue_scan;
    }
};

test "radix insert and get" {
    var tree = try RadixTree.init(std.testing.allocator);
    defer tree.deinit();

    try tree.insert("hello", .{
        .sequence = 1,
        .event_type = "evt",
        .payload = "{}",
        .timestamp = 0,
        .origin_service = "svc",
    });
    try tree.insert("help", .{
        .sequence = 2,
        .event_type = "evt",
        .payload = "{}",
        .timestamp = 0,
        .origin_service = "svc",
    });

    try std.testing.expectEqual(@as(u64, 1), tree.get("hello").?.sequence);
    try std.testing.expectEqual(@as(u64, 2), tree.get("help").?.sequence);
    try std.testing.expect(tree.get("hel") == null);
    try std.testing.expect(tree.get("missing") == null);
    try std.testing.expectEqual(@as(limits.NodeCount, 2), tree.size);
}

test "radix overwrite does not grow size" {
    var tree = try RadixTree.init(std.testing.allocator);
    defer tree.deinit();
    const first = EventValue{
        .sequence = 1,
        .event_type = "first",
        .payload = "{}",
        .timestamp = 0,
        .origin_service = "svc",
    };
    var second = first;
    second.sequence = 2;
    second.event_type = "second";
    try tree.insert("key", first);
    try tree.insert("key", second);
    try std.testing.expectEqual(@as(limits.NodeCount, 1), tree.size);
    try std.testing.expectEqual(@as(u64, 2), tree.get("key").?.sequence);
}

test "radix prefix visitor is bounded" {
    var tree = try RadixTree.init(std.testing.allocator);
    defer tree.deinit();
    const keys = [_][]const u8{ "a:1", "a:2", "a:3", "b:1" };
    for (keys, 0..) |key, index| try tree.insert(key, .{
        .sequence = index + 1,
        .event_type = "evt",
        .payload = "{}",
        .timestamp = 0,
        .origin_service = "svc",
    });
    var context = CountContext{};
    _ = try tree.visit_prefix("a:", &context, CountContext.visit);
    try std.testing.expectEqual(@as(limits.CollectionCount, 3), context.count);
}

test "radix operations allocate nothing after initialization" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var tree = try RadixTree.init(failing.allocator());
    defer tree.deinit();
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    var key_buffer: [64]u8 = undefined;
    for (0..256) |index| {
        const key = try std.fmt.bufPrint(&key_buffer, "fixed:{d}", .{index});
        try tree.insert(key, .{
            .sequence = index + 1,
            .event_type = "fixed.event",
            .payload = "{}",
            .timestamp = 1,
            .origin_service = "fixed-service",
        });
    }
    try std.testing.expectEqual(@as(u64, 256), tree.get("fixed:255").?.sequence);
    var context = CountContext{};
    _ = try tree.visit_prefix("fixed:", &context, CountContext.visit);
    try std.testing.expectEqual(@as(limits.CollectionCount, 256), context.count);
    try std.testing.expect(!failing.has_induced_failure);
}
