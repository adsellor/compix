const std = @import("std");
const assert = std.debug.assert;
const ArrayList = std.ArrayList;
const event = @import("event.zig");
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

pub const key_length_max: u32 = 4096;

comptime {
    assert(key_length_max > 0);
    assert(key_length_max <= 64 * 1024);
}

pub const RadixNode = struct {
    key_fragment: []const u8,
    value: ?EventValue = null,
    children: ArrayList(*RadixNode),
    is_terminal: bool,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, key_fragment: []const u8) !*RadixNode {
        const node = try allocator.create(RadixNode);
        node.* = RadixNode{
            .key_fragment = try allocator.dupe(u8, key_fragment),
            .children = ArrayList(*RadixNode).empty,
            .is_terminal = false,
            .allocator = allocator,
        };
        return node;
    }

    /// Frees the subtree's fields and descendants; the node itself is not destroyed.
    /// Iterative (no recursion, no allocation): repeatedly detaches and frees a leaf.
    pub fn deinit(self: *RadixNode) void {
        const allocator = self.allocator;
        while (self.children.items.len > 0) {
            var leaf_parent = self;
            while (leaf_parent.children.items[leaf_parent.children.items.len - 1]
                .children.items.len > 0)
            {
                leaf_parent = leaf_parent.children.items[leaf_parent.children.items.len - 1];
            }
            const leaf = leaf_parent.children.pop().?;
            assert(leaf.children.items.len == 0);
            allocator.free(leaf.key_fragment);
            if (leaf.value) |*value| value.deinit(allocator);
            leaf.children.deinit(allocator);
            allocator.destroy(leaf);
        }
        allocator.free(self.key_fragment);
        if (self.value) |*value| value.deinit(allocator);
        self.children.deinit(allocator);
    }
};

pub const RadixTree = struct {
    root: *RadixNode,
    allocator: std.mem.Allocator,
    size: usize,

    pub fn init(allocator: std.mem.Allocator) !RadixTree {
        const root = try RadixNode.init(allocator, "");
        return RadixTree{
            .root = root,
            .allocator = allocator,
            .size = 0,
        };
    }

    pub fn deinit(self: *RadixTree) void {
        self.root.deinit();
        self.allocator.destroy(self.root);
    }

    pub fn insert(self: *RadixTree, key: []const u8, value: EventValue) !void {
        assert(key.len <= key_length_max);
        const size_before = self.size;

        var node = self.root;
        var remaining = key;
        var depth: u32 = 0;

        while (true) {
            assert(depth <= key_length_max + 1);
            depth += 1;

            if (remaining.len == 0) {
                const was_new = !node.is_terminal;
                if (node.value) |*existing| {
                    existing.deinit(self.allocator);
                }
                node.value = value;
                node.is_terminal = true;
                if (was_new) self.size += 1;
                assert(self.size >= size_before);
                return;
            }

            var matched: ?*RadixNode = null;
            var common_len: usize = 0;
            for (node.children.items) |child| {
                common_len = common_prefix_length(remaining, child.key_fragment);
                if (common_len > 0) {
                    matched = child;
                    break;
                }
            }

            if (matched) |child| {
                assert(common_len > 0);
                assert(common_len <= child.key_fragment.len);
                if (common_len < child.key_fragment.len) {
                    try self.split_child(child, common_len);
                    assert(child.key_fragment.len == common_len);
                }
                node = child;
                remaining = remaining[common_len..];
            } else {
                const new_child = try RadixNode.init(self.allocator, remaining);
                new_child.value = value;
                new_child.is_terminal = true;
                try node.children.append(self.allocator, new_child);
                self.size += 1;
                assert(self.size == size_before + 1);
                return;
            }
        }
    }

    fn split_child(self: *RadixTree, child: *RadixNode, split_pos: usize) !void {
        assert(split_pos > 0);
        assert(split_pos < child.key_fragment.len);

        const remaining_fragment = child.key_fragment[split_pos..];
        const new_child = try RadixNode.init(self.allocator, remaining_fragment);
        new_child.value = child.value;
        new_child.is_terminal = child.is_terminal;
        new_child.children = child.children;

        const prefix = try self.allocator.dupe(u8, child.key_fragment[0..split_pos]);
        self.allocator.free(child.key_fragment);
        child.key_fragment = prefix;
        child.value = null;
        child.is_terminal = false;
        child.children = ArrayList(*RadixNode).empty;
        try child.children.append(self.allocator, new_child);

        assert(child.children.items.len == 1);
        assert(!child.is_terminal);
    }

    pub fn get(self: *const RadixTree, key: []const u8) ?*const EventValue {
        assert(key.len <= key_length_max);

        var node: *const RadixNode = self.root;
        var remaining = key;
        var depth: u32 = 0;

        while (true) {
            assert(depth <= key_length_max + 1);
            depth += 1;

            if (remaining.len == 0) {
                if (node.is_terminal) {
                    assert(node.value != null);
                    if (node.value) |*v| return v;
                }
                return null;
            }

            var descended = false;
            for (node.children.items) |child| {
                const common_len = common_prefix_length(remaining, child.key_fragment);
                if (common_len == child.key_fragment.len and common_len > 0) {
                    node = child;
                    remaining = remaining[common_len..];
                    descended = true;
                    break;
                }
            }
            if (!descended) return null;
        }
    }

    const ScanFrame = struct {
        node: *const RadixNode,
        child_index: usize,
        prev_key_len: usize,
    };

    pub fn scan_prefix(
        self: *const RadixTree,
        prefix: []const u8,
        results: *ArrayList(KeyValuePair),
    ) !void {
        assert(prefix.len <= key_length_max);

        var key_buf = ArrayList(u8).empty;
        defer key_buf.deinit(self.allocator);

        var stack = ArrayList(ScanFrame).empty;
        defer stack.deinit(self.allocator);

        try self.scan_push(&stack, &key_buf, self.root, prefix, results);

        while (stack.items.len > 0) {
            assert(stack.items.len <= key_length_max + 1);
            const frame = &stack.items[stack.items.len - 1];

            if (frame.child_index < frame.node.children.items.len) {
                const child = frame.node.children.items[frame.child_index];
                frame.child_index += 1;
                try self.scan_push(&stack, &key_buf, child, prefix, results);
            } else {
                key_buf.items.len = frame.prev_key_len;
                stack.items.len -= 1;
            }
        }

        assert(key_buf.items.len == 0);
    }

    /// Appends the node's fragment to key_buf; if the accumulated key still matches
    /// the prefix, collects a terminal value and pushes a frame to visit children.
    /// Otherwise the subtree is pruned and key_buf is restored.
    fn scan_push(
        self: *const RadixTree,
        stack: *ArrayList(ScanFrame),
        key_buf: *ArrayList(u8),
        node: *const RadixNode,
        prefix: []const u8,
        results: *ArrayList(KeyValuePair),
    ) !void {
        const prev_len = key_buf.items.len;
        try key_buf.appendSlice(self.allocator, node.key_fragment);
        const full_key = key_buf.items;

        const pruned = if (full_key.len >= prefix.len)
            !std.mem.startsWith(u8, full_key, prefix)
        else
            !std.mem.startsWith(u8, prefix, full_key);
        if (pruned) {
            key_buf.items.len = prev_len;
            return;
        }

        if (node.is_terminal and full_key.len >= prefix.len) {
            assert(node.value != null);
            const original = node.value.?;
            try results.append(self.allocator, KeyValuePair{
                .key = try self.allocator.dupe(u8, full_key),
                .value = EventValue{
                    .sequence = original.sequence,
                    .event_type = try self.allocator.dupe(u8, original.event_type),
                    .payload = try self.allocator.dupe(u8, original.payload),
                    .timestamp = original.timestamp,
                    .origin_service = try self.allocator.dupe(u8, original.origin_service),
                },
            });
        }

        try stack.append(self.allocator, ScanFrame{
            .node = node,
            .child_index = 0,
            .prev_key_len = prev_len,
        });
    }
};

fn common_prefix_length(a: []const u8, b: []const u8) usize {
    var i: usize = 0;
    while (i < a.len and i < b.len and a[i] == b[i]) {
        i += 1;
    }
    assert(i <= a.len);
    assert(i <= b.len);
    return i;
}

test "radix insert and get" {
    const allocator = std.testing.allocator;
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const v1 = try EventValue.init(allocator, 1, "svc", "evt", "{}", 0);
    try tree.insert("hello", v1);
    const v2 = try EventValue.init(allocator, 2, "svc", "evt", "{}", 0);
    try tree.insert("help", v2);

    try std.testing.expectEqual(@as(u64, 1), tree.get("hello").?.sequence);
    try std.testing.expectEqual(@as(u64, 2), tree.get("help").?.sequence);
    try std.testing.expect(tree.get("hel") == null);
    try std.testing.expect(tree.get("missing") == null);
    try std.testing.expectEqual(@as(usize, 2), tree.size);
}

test "radix overwrite does not grow size" {
    const allocator = std.testing.allocator;
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const v1 = try EventValue.init(allocator, 1, "svc", "first", "{}", 0);
    try tree.insert("key", v1);
    const v2 = try EventValue.init(allocator, 2, "svc", "second", "{}", 0);
    try tree.insert("key", v2);

    try std.testing.expectEqual(@as(usize, 1), tree.size);
    try std.testing.expectEqual(@as(u64, 2), tree.get("key").?.sequence);
}

test "radix scan_prefix" {
    const allocator = std.testing.allocator;
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    const keys = [_][]const u8{ "a:1", "a:2", "a:3", "b:1" };
    for (keys, 0..) |k, i| {
        const v = try EventValue.init(allocator, i + 1, "svc", "evt", "{}", 0);
        try tree.insert(k, v);
    }

    var results = ArrayList(KeyValuePair).empty;
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    try tree.scan_prefix("a:", &results);
    try std.testing.expectEqual(@as(usize, 3), results.items.len);
}

test "radix deep keys stay bounded" {
    const allocator = std.testing.allocator;
    var tree = try RadixTree.init(allocator);
    defer tree.deinit();

    // Nested prefixes force maximum tree depth (one node per added character).
    var key_buf: [256]u8 = undefined;
    for (0..256) |len| {
        key_buf[len] = 'x';
        const v = try EventValue.init(allocator, len + 1, "svc", "evt", "{}", 0);
        try tree.insert(key_buf[0 .. len + 1], v);
    }
    try std.testing.expectEqual(@as(usize, 256), tree.size);
    try std.testing.expectEqual(@as(u64, 256), tree.get(&key_buf).?.sequence);

    var results = ArrayList(KeyValuePair).empty;
    defer {
        for (results.items) |*item| item.deinit(allocator);
        results.deinit(allocator);
    }
    try tree.scan_prefix("xxx", &results);
    try std.testing.expectEqual(@as(usize, 254), results.items.len);
}
