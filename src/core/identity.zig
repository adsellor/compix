const std = @import("std");
const limits = @import("limits.zig");

pub const ServiceIdentity = struct {
    name: []const u8,
    instance_id: []const u8,
    listen_address: []const u8,

    pub const InitOptions = struct {
        name: []const u8,
        instance_id: []const u8,
        listen_address: []const u8,
    };

    pub fn init_in_place(
        target: *ServiceIdentity,
        allocator: std.mem.Allocator,
        options: InitOptions,
    ) !void {
        if (options.name.len > limits.service_name_length_max) return error.ServiceNameTooLong;
        if (options.instance_id.len > limits.instance_id_length_max)
            return error.InstanceIdTooLong;
        if (options.listen_address.len > limits.peer_address_length_max)
            return error.AddressTooLong;

        const owned_name = try allocator.dupe(u8, options.name);
        errdefer allocator.free(owned_name);
        const owned_instance_id = try allocator.dupe(u8, options.instance_id);
        errdefer allocator.free(owned_instance_id);
        const owned_listen_address = try allocator.dupe(u8, options.listen_address);

        target.* = .{
            .name = owned_name,
            .instance_id = owned_instance_id,
            .listen_address = owned_listen_address,
        };
    }

    pub fn init(
        allocator: std.mem.Allocator,
        name: []const u8,
        instance_id: []const u8,
        listen_address: []const u8,
    ) !ServiceIdentity {
        var target: ServiceIdentity = undefined;
        try init_in_place(&target, allocator, .{
            .name = name,
            .instance_id = instance_id,
            .listen_address = listen_address,
        });
        return target;
    }

    pub fn deinit(self: *ServiceIdentity, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.instance_id);
        allocator.free(self.listen_address);
    }
};

test "ServiceIdentity init and deinit" {
    const allocator = std.testing.allocator;

    var identity = try ServiceIdentity.init(
        allocator,
        "user-service",
        "host-abc-123",
        "127.0.0.1:4200",
    );
    defer identity.deinit(allocator);

    try std.testing.expectEqualStrings("user-service", identity.name);
    try std.testing.expectEqualStrings("host-abc-123", identity.instance_id);
    try std.testing.expectEqualStrings("127.0.0.1:4200", identity.listen_address);
}
