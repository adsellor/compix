const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const identity_mod = @import("identity.zig");
const contract_mod = @import("contract.zig");

const ServiceIdentity = identity_mod.ServiceIdentity;
const DependencyContract = contract_mod.DependencyContract;

pub const ContractEntry = struct {
    source_service: []const u8,
    event_types: []const []const u8,
    peer_address: []const u8,
};

pub const ServiceConfig = struct {
    service_name: []const u8,
    instance_id: []const u8,
    listen_address: []const u8,
    wal_path: []const u8,
    contracts: []const ContractEntry,
};

pub const LoadedConfig = struct {
    identity: ServiceIdentity,
    contracts: []const DependencyContract,
    wal_path: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *LoadedConfig) void {
        for (self.contracts) |contract| {
            for (contract.event_types) |et| {
                self.allocator.free(et);
            }
            self.allocator.free(contract.event_types);
            self.allocator.free(contract.source_service);
            self.allocator.free(contract.peer_address);
        }
        self.allocator.free(self.contracts);
        self.allocator.free(self.wal_path);
        var id = self.identity;
        id.deinit(self.allocator);
    }
};

pub fn load(allocator: Allocator, path: []const u8, io: Io) !LoadedConfig {
    const file = try Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const file_size: usize = @intCast((try file.stat(io)).size);
    if (file_size == 0) return error.EmptyConfigFile;

    const data = try allocator.alloc(u8, file_size);
    defer allocator.free(data);

    _ = try file.readPositionalAll(io, data, 0);

    return loadFromSlice(allocator, data);
}

pub fn loadFromSlice(allocator: Allocator, data: []const u8) !LoadedConfig {
    const parsed = try std.json.parseFromSlice(ServiceConfig, allocator, data, .{
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    const cfg = parsed.value;

    var identity = try ServiceIdentity.init(allocator, cfg.service_name, cfg.instance_id, cfg.listen_address);
    errdefer identity.deinit(allocator);

    const contracts = try allocator.alloc(DependencyContract, cfg.contracts.len);
    var built: usize = 0;
    errdefer {
        for (contracts[0..built]) |contract| {
            for (contract.event_types) |et| allocator.free(et);
            allocator.free(contract.event_types);
            allocator.free(contract.source_service);
            allocator.free(contract.peer_address);
        }
        allocator.free(contracts);
    }

    for (cfg.contracts) |entry| {
        const event_types = try allocator.alloc([]const u8, entry.event_types.len);
        var et_built: usize = 0;
        errdefer {
            for (event_types[0..et_built]) |et| allocator.free(et);
            allocator.free(event_types);
        }

        for (entry.event_types) |et| {
            event_types[et_built] = try allocator.dupe(u8, et);
            et_built += 1;
        }

        const source_service = try allocator.dupe(u8, entry.source_service);
        errdefer allocator.free(source_service);

        const peer_address = try allocator.dupe(u8, entry.peer_address);

        contracts[built] = .{
            .source_service = source_service,
            .event_types = event_types,
            .peer_address = peer_address,
            .retention_hint = null,
        };
        built += 1;
    }

    const wal_path = try allocator.dupe(u8, cfg.wal_path);

    return .{
        .identity = identity,
        .contracts = contracts,
        .wal_path = wal_path,
        .allocator = allocator,
    };
}

test "parse valid config" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "service_name": "order-service",
        \\  "instance_id": "node-1",
        \\  "listen_address": "0.0.0.0:4200",
        \\  "wal_path": "data/events.wal",
        \\  "contracts": [
        \\    {
        \\      "source_service": "user-service",
        \\      "event_types": ["user.registered", "user.updated"],
        \\      "peer_address": "10.0.0.6:4200"
        \\    }
        \\  ]
        \\}
    ;

    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqualStrings("order-service", cfg.identity.name);
    try std.testing.expectEqualStrings("node-1", cfg.identity.instance_id);
    try std.testing.expectEqualStrings("0.0.0.0:4200", cfg.identity.listen_address);
    try std.testing.expectEqualStrings("data/events.wal", cfg.wal_path);

    try std.testing.expectEqual(@as(usize, 1), cfg.contracts.len);
    try std.testing.expectEqualStrings("user-service", cfg.contracts[0].source_service);
    try std.testing.expectEqual(@as(usize, 2), cfg.contracts[0].event_types.len);
    try std.testing.expectEqualStrings("user.registered", cfg.contracts[0].event_types[0]);
    try std.testing.expectEqualStrings("user.updated", cfg.contracts[0].event_types[1]);
    try std.testing.expectEqualStrings("10.0.0.6:4200", cfg.contracts[0].peer_address);
    try std.testing.expect(cfg.contracts[0].retention_hint == null);
}

test "empty contracts array is valid" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "service_name": "standalone",
        \\  "instance_id": "node-1",
        \\  "listen_address": "0.0.0.0:4200",
        \\  "wal_path": "data/standalone.wal",
        \\  "contracts": []
        \\}
    ;

    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqualStrings("standalone", cfg.identity.name);
    try std.testing.expectEqual(@as(usize, 0), cfg.contracts.len);
}

test "missing required fields produce an error" {
    const allocator = std.testing.allocator;

    // Missing service_name
    const json =
        \\{
        \\  "instance_id": "node-1",
        \\  "listen_address": "0.0.0.0:4200",
        \\  "wal_path": "data/events.wal",
        \\  "contracts": []
        \\}
    ;

    const result = loadFromSlice(allocator, json);
    try std.testing.expect(std.meta.isError(result));
}

test "multiple contracts parsed correctly" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "service_name": "gateway",
        \\  "instance_id": "gw-1",
        \\  "listen_address": "0.0.0.0:4200",
        \\  "wal_path": "data/gw.wal",
        \\  "contracts": [
        \\    {
        \\      "source_service": "order-service",
        \\      "event_types": ["order.created"],
        \\      "peer_address": "10.0.0.5:4200"
        \\    },
        \\    {
        \\      "source_service": "user-service",
        \\      "event_types": ["user.registered", "user.updated"],
        \\      "peer_address": "10.0.0.6:4200"
        \\    }
        \\  ]
        \\}
    ;

    var cfg = try loadFromSlice(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 2), cfg.contracts.len);
    try std.testing.expectEqualStrings("order-service", cfg.contracts[0].source_service);
    try std.testing.expectEqualStrings("user-service", cfg.contracts[1].source_service);
    try std.testing.expectEqual(@as(usize, 1), cfg.contracts[0].event_types.len);
    try std.testing.expectEqual(@as(usize, 2), cfg.contracts[1].event_types.len);
}
