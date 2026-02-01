const std = @import("std");
const ArrayList = std.ArrayList;
const breadcrumb_mod = @import("breadcrumb.zig");
const contract_mod = @import("contract.zig");
const Breadcrumb = breadcrumb_mod.Breadcrumb;
const DependencyContract = contract_mod.DependencyContract;

pub const RecoveryTask = struct {
    peer_address: []const u8,
    source_service: []const u8,
    event_type: []const u8,
    from_sequence: u64,

    pub fn deinit(self: *RecoveryTask, allocator: std.mem.Allocator) void {
        allocator.free(self.peer_address);
        allocator.free(self.source_service);
        allocator.free(self.event_type);
    }
};

/// Pure computation: given dependency contracts and known breadcrumbs, produce
/// a list of recovery tasks describing what data needs to be fetched from peers.
pub fn computeRecoveryPlan(
    allocator: std.mem.Allocator,
    contracts: []const DependencyContract,
    breadcrumbs: []const Breadcrumb,
) !ArrayList(RecoveryTask) {
    var tasks = ArrayList(RecoveryTask){};
    errdefer {
        for (tasks.items) |*t| t.deinit(allocator);
        tasks.deinit(allocator);
    }

    for (contracts) |contract| {
        for (contract.event_types) |et| {
            var found: ?Breadcrumb = null;
            for (breadcrumbs) |bc| {
                if (std.mem.eql(u8, bc.source_service, contract.source_service) and
                    std.mem.eql(u8, bc.event_type, et))
                {
                    found = bc;
                    break;
                }
            }

            const from_seq: u64 = if (found) |bc| bc.last_sequence + 1 else 0;

            try tasks.append(allocator, RecoveryTask{
                .peer_address = try allocator.dupe(u8, contract.peer_address),
                .source_service = try allocator.dupe(u8, contract.source_service),
                .event_type = try allocator.dupe(u8, et),
                .from_sequence = from_seq,
            });
        }
    }

    return tasks;
}

test "computeRecoveryPlan with no breadcrumbs creates tasks from seq 0" {
    const allocator = std.testing.allocator;

    const contracts = [_]DependencyContract{
        .{
            .source_service = "order-service",
            .event_types = &[_][]const u8{ "order.created", "order.updated" },
            .peer_address = "10.0.0.5:4200",
            .retention_hint = null,
        },
    };

    const breadcrumbs = [_]Breadcrumb{};

    var tasks = try computeRecoveryPlan(allocator, &contracts, &breadcrumbs);
    defer {
        for (tasks.items) |*t| t.deinit(allocator);
        tasks.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), tasks.items.len);
    try std.testing.expectEqualStrings("order.created", tasks.items[0].event_type);
    try std.testing.expectEqual(@as(u64, 0), tasks.items[0].from_sequence);
    try std.testing.expectEqualStrings("order.updated", tasks.items[1].event_type);
    try std.testing.expectEqual(@as(u64, 0), tasks.items[1].from_sequence);
}

test "computeRecoveryPlan with partial breadcrumbs fills gaps" {
    const allocator = std.testing.allocator;

    const contracts = [_]DependencyContract{
        .{
            .source_service = "order-service",
            .event_types = &[_][]const u8{ "order.created", "order.updated" },
            .peer_address = "10.0.0.5:4200",
            .retention_hint = null,
        },
    };

    const breadcrumbs = [_]Breadcrumb{
        .{
            .source_service = "order-service",
            .event_type = "order.created",
            .last_sequence = 42,
            .peer_address = "10.0.0.5:4200",
            .updated_at = 100,
        },
    };

    var tasks = try computeRecoveryPlan(allocator, &contracts, &breadcrumbs);
    defer {
        for (tasks.items) |*t| t.deinit(allocator);
        tasks.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), tasks.items.len);

    try std.testing.expectEqualStrings("order.created", tasks.items[0].event_type);
    try std.testing.expectEqual(@as(u64, 43), tasks.items[0].from_sequence);

    try std.testing.expectEqualStrings("order.updated", tasks.items[1].event_type);
    try std.testing.expectEqual(@as(u64, 0), tasks.items[1].from_sequence);
}

test "computeRecoveryPlan with multiple contracts" {
    const allocator = std.testing.allocator;

    const contracts = [_]DependencyContract{
        .{
            .source_service = "order-service",
            .event_types = &[_][]const u8{"order.created"},
            .peer_address = "10.0.0.5:4200",
            .retention_hint = null,
        },
        .{
            .source_service = "user-service",
            .event_types = &[_][]const u8{"user.registered"},
            .peer_address = "10.0.0.6:4200",
            .retention_hint = 1000,
        },
    };

    const breadcrumbs = [_]Breadcrumb{
        .{
            .source_service = "order-service",
            .event_type = "order.created",
            .last_sequence = 100,
            .peer_address = "10.0.0.5:4200",
            .updated_at = 200,
        },
        .{
            .source_service = "user-service",
            .event_type = "user.registered",
            .last_sequence = 50,
            .peer_address = "10.0.0.6:4200",
            .updated_at = 150,
        },
    };

    var tasks = try computeRecoveryPlan(allocator, &contracts, &breadcrumbs);
    defer {
        for (tasks.items) |*t| t.deinit(allocator);
        tasks.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), tasks.items.len);
    try std.testing.expectEqual(@as(u64, 101), tasks.items[0].from_sequence);
    try std.testing.expectEqualStrings("10.0.0.5:4200", tasks.items[0].peer_address);
    try std.testing.expectEqual(@as(u64, 51), tasks.items[1].from_sequence);
    try std.testing.expectEqualStrings("10.0.0.6:4200", tasks.items[1].peer_address);
}

test "computeRecoveryPlan with no contracts returns empty" {
    const allocator = std.testing.allocator;

    const contracts = [_]DependencyContract{};
    const breadcrumbs = [_]Breadcrumb{};

    var tasks = try computeRecoveryPlan(allocator, &contracts, &breadcrumbs);
    defer tasks.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 0), tasks.items.len);
}
