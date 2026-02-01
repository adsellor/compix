const std = @import("std");

pub const ServiceIdentity = struct {
    name: []const u8,
    instance_id: []const u8,
    listen_address: []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        instance_id: []const u8,
        listen_address: []const u8,
    ) !ServiceIdentity {
        return ServiceIdentity{
            .name = try allocator.dupe(u8, name),
            .instance_id = try allocator.dupe(u8, instance_id),
            .listen_address = try allocator.dupe(u8, listen_address),
        };
    }

    pub fn deinit(self: *ServiceIdentity, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.instance_id);
        allocator.free(self.listen_address);
    }
};

test "ServiceIdentity init and deinit" {
    const allocator = std.testing.allocator;

    var identity = try ServiceIdentity.init(allocator, "user-service", "host-abc-123", "127.0.0.1:4200");
    defer identity.deinit(allocator);

    try std.testing.expectEqualStrings("user-service", identity.name);
    try std.testing.expectEqualStrings("host-abc-123", identity.instance_id);
    try std.testing.expectEqualStrings("127.0.0.1:4200", identity.listen_address);
}
