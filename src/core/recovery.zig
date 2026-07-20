const limits = @import("limits.zig");

pub const RecoveryTask = struct {
    peer_address: []const u8,
    source_service: []const u8,
    event_type: []const u8,
    from_sequence: limits.Sequence,
};
