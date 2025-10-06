const std = @import("std");
const ArrayList = std.ArrayList;
const event = @import("event.zig");
const EventValue = event.EventValue;
const KeyValuePair = event.KeyValuePair;

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
            .children = ArrayList(*RadixNode){},
            .is_terminal = false,
            .allocator = allocator,
        };
        return node;
    }

    pub fn deinit(self: *RadixNode) void {
        self.allocator.free(self.key_fragment);
        if (self.value) |*value| {
            value.deinit(self.allocator);
        }
        for (self.children.items) |child| {
            child.deinit();
            self.allocator.destroy(child);
        }
        self.children.deinit(self.allocator);
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
        try self.insert_recursive(self.root, key, value);
        self.size += 1;
    }

    fn insert_recursive(self: *RadixTree, node: *RadixNode, key: []const u8, value: EventValue) !void {
        if (key.len == 0) {
            if (node.value) |*existing| {
                existing.deinit(self.allocator);
            }
            node.value = value;
            node.is_terminal = true;
            return;
        }

        for (node.children.items) |child| {
            const common_len = common_prefix_length(key, child.key_fragment);
            if (common_len > 0) {
                if (common_len == child.key_fragment.len) {
                    return self.insert_recursive(child, key[common_len..], value);
                } else {
                    try self.split_child(child, common_len);
                    return self.insert_recursive(child, key[common_len..], value);
                }
            }
        }

        const new_child = try RadixNode.init(self.allocator, key);
        new_child.value = value;
        new_child.is_terminal = true;
        try node.children.append(self.allocator, new_child);
    }

    fn split_child(self: *RadixTree, child: *RadixNode, split_pos: usize) !void {
        if (split_pos >= child.key_fragment.len) {
            return;
        }

        const remaining_fragment = child.key_fragment[split_pos..];
        const new_child = try RadixNode.init(self.allocator, remaining_fragment);
        new_child.value = child.value;
        new_child.is_terminal = child.is_terminal;
        new_child.children = child.children;

        const temp_prefix = try self.allocator.dupe(u8, child.key_fragment[0..split_pos]);
        defer self.allocator.free(temp_prefix);

        self.allocator.free(child.key_fragment);
        child.key_fragment = try self.allocator.dupe(u8, temp_prefix);
        child.value = null;
        child.is_terminal = false;
        child.children = ArrayList(*RadixNode){};
        try child.children.append(self.allocator, new_child);
    }

    pub fn get(self: *const RadixTree, key: []const u8) ?EventValue {
        return self.get_recursive(self.root, key);
    }

    fn get_recursive(self: *const RadixTree, node: *const RadixNode, key: []const u8) ?EventValue {
        if (key.len == 0) {
            if (node.is_terminal and node.value != null) {
                const original = node.value.?;
                return EventValue{
                    .sequence = original.sequence,
                    .event_type = self.allocator.dupe(u8, original.event_type) catch return null,
                    .payload = self.allocator.dupe(u8, original.payload) catch return null,
                    .timestamp = original.timestamp,
                };
            }
            return null;
        }

        for (node.children.items) |child| {
            const common_len = common_prefix_length(key, child.key_fragment);
            if (common_len == child.key_fragment.len and common_len > 0) {
                return self.get_recursive(child, key[common_len..]);
            }
        }
        return null;
    }

    pub fn scan_prefix(self: *const RadixTree, prefix: []const u8, results: *ArrayList(KeyValuePair)) !void {
        try self.scan_prefix_recursive(self.root, prefix, "", results);
    }

    fn scan_prefix_recursive(
        self: *const RadixTree,
        node: *const RadixNode,
        target_prefix: []const u8,
        current_key: []const u8,
        results: *ArrayList(KeyValuePair),
    ) !void {
        const full_key = if (current_key.len == 0)
            node.key_fragment
        else
            try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ current_key, node.key_fragment });
        defer if (current_key.len > 0) self.allocator.free(full_key);

        if (node.is_terminal and std.mem.startsWith(u8, full_key, target_prefix)) {
            try results.append(self.allocator, KeyValuePair{
                .key = try self.allocator.dupe(u8, full_key),
                .value = node.value.?,
            });
        }

        for (node.children.items) |child| {
            try self.scan_prefix_recursive(child, target_prefix, full_key, results);
        }
    }
};

fn common_prefix_length(a: []const u8, b: []const u8) usize {
    var i: usize = 0;
    while (i < a.len and i < b.len and a[i] == b[i]) {
        i += 1;
    }
    return i;
}
