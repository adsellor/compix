const std = @import("std");
const breadcrumb = @import("breadcrumb.zig");
const signing = @import("signing.zig");
const Breadcrumb = breadcrumb.Breadcrumb;

pub const DependencyContract = struct {
    source_service: []const u8,
    event_types: []const []const u8,
    peer_address: []const u8,
    retention_hint: ?u64,
    /// The declared owner's Ed25519 public key. When set, pulled events from
    /// this origin must carry a valid origin signature or ingest rejects them.
    origin_public_key: ?[signing.public_key_length]u8 = null,

    /// Returns true if this contract covers the given breadcrumb's source_service and event_type.
    pub fn matches_breadcrumb(self: *const DependencyContract, bc: Breadcrumb) bool {
        return self.matches_event(bc.source_service, bc.event_type);
    }

    /// Returns true if this contract covers the given (origin, event_type) pair.
    pub fn matches_event(
        self: *const DependencyContract,
        origin: []const u8,
        event_type: []const u8,
    ) bool {
        if (!std.mem.eql(u8, self.source_service, origin)) return false;
        for (self.event_types) |et| {
            if (std.mem.eql(u8, et, event_type)) return true;
        }
        return false;
    }
};

test "matches_breadcrumb matches correct source and type" {
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
    try std.testing.expect(contract.matches_breadcrumb(matching));

    const matching2 = Breadcrumb{
        .source_service = "order-service",
        .event_type = "order.updated",
        .last_sequence = 10,
        .peer_address = "10.0.0.5:4200",
        .updated_at = 200,
    };
    try std.testing.expect(contract.matches_breadcrumb(matching2));
}

test "matches_breadcrumb rejects wrong source" {
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
    try std.testing.expect(!contract.matches_breadcrumb(wrong_source));
}

test "matches_breadcrumb rejects wrong event type" {
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
    try std.testing.expect(!contract.matches_breadcrumb(wrong_type));
}

test "matches_event matches correct origin and type" {
    const contract = DependencyContract{
        .source_service = "order-service",
        .event_types = &[_][]const u8{ "order.created", "order.updated" },
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    };

    try std.testing.expect(contract.matches_event("order-service", "order.created"));
    try std.testing.expect(contract.matches_event("order-service", "order.updated"));
}

test "matches_event rejects wrong origin" {
    const contract = DependencyContract{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    };

    try std.testing.expect(!contract.matches_event("user-service", "order.created"));
}

test "matches_event rejects wrong event type" {
    const contract = DependencyContract{
        .source_service = "order-service",
        .event_types = &[_][]const u8{"order.created"},
        .peer_address = "10.0.0.5:4200",
        .retention_hint = null,
    };

    try std.testing.expect(!contract.matches_event("order-service", "order.deleted"));
}
