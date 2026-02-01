const std = @import("std");
const breadcrumb = @import("breadcrumb.zig");
const Breadcrumb = breadcrumb.Breadcrumb;

pub const DependencyContract = struct {
    source_service: []const u8,
    event_types: []const []const u8,
    peer_address: []const u8,
    retention_hint: ?u64,

    /// Returns true if this contract covers the given breadcrumb's source_service and event_type.
    pub fn matchesBreadcrumb(self: *const DependencyContract, bc: Breadcrumb) bool {
        if (!std.mem.eql(u8, self.source_service, bc.source_service)) return false;
        for (self.event_types) |et| {
            if (std.mem.eql(u8, et, bc.event_type)) return true;
        }
        return false;
    }
};

test "matchesBreadcrumb matches correct source and type" {
    const contract = DependencyContract{
        .source_service = "order-service",
        .event_types = &[_][]const u8{ "order.created", "order.updated" },
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    };

    const matching = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.created",
        .last_sequence = 42,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 100,
    };
    try std.testing.expect(contract.matchesBreadcrumb(matching));

    const matching2 = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.updated",
        .last_sequence = 10,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 200,
    };
    try std.testing.expect(contract.matchesBreadcrumb(matching2));
}

test "matchesBreadcrumb rejects wrong source" {
    const contract = DependencyContract{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    };

    const wrong_source = Breadcrumb{
        .source_service = "user-service",
        .event_type = "order.created",
        .last_sequence = 1,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 100,
    };
    try std.testing.expect(!contract.matchesBreadcrumb(wrong_source));
}

test "matchesBreadcrumb rejects wrong event type" {
    const contract = DependencyContract{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    };

    const wrong_type = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.deleted",
        .last_sequence = 1,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 100,
    };
    try std.testing.expect(!contract.matchesBreadcrumb(wrong_type));
}
