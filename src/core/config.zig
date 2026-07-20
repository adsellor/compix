const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const auth = @import("auth.zig");
const limits = @import("limits.zig");
const protocol = @import("protocol.zig");
const raft = @import("raft.zig");

const identity_mod = @import("identity.zig");
const contract_mod = @import("contract.zig");

const ServiceIdentity = identity_mod.ServiceIdentity;
const DependencyContract = contract_mod.DependencyContract;

pub const ContractEntry = struct {
    source_service: []const u8,
    event_types: []const []const u8,
    peer_address: []const u8,
};

pub const CredentialEntry = struct {
    service_name: []const u8,
    secret: []const u8,
    permissions: []const auth.Permission,
};

pub const ClusterMemberEntry = struct {
    node_id: raft.NodeId,
    instance_id: []const u8,
    address: []const u8,
};

pub const ClusterEntry = struct {
    node_id: raft.NodeId,
    state_path: []const u8,
    members: []const ClusterMemberEntry,
    heartbeat_interval_ms: u32 = 100,
    election_timeout_min_ms: u32 = 600,
    election_timeout_max_ms: u32 = 1_200,
    rpc_timeout_ms: u32 = 500,
    log_capacity: u16 = 64,
    command_slot_size: limits.ByteCount = protocol.replicated_command_size_max,
    snapshot_size_max: u64 = limits.snapshot_size_default_max,
};

pub const ServiceConfig = struct {
    service_name: []const u8,
    instance_id: []const u8,
    listen_address: []const u8,
    wal_path: []const u8,
    contracts: []const ContractEntry,
    client_secret: ?[]const u8 = null,
    credentials: []const CredentialEntry = &.{},
    connection_capacity: limits.ConnectionCount = limits.connection_capacity_default,
    stream_capacity: limits.StreamCount = limits.stream_capacity_default,
    stream_id_bytes_capacity: limits.ByteCount = limits.stream_id_bytes_capacity_default,
    cluster: ?ClusterEntry = null,
};

pub const ClusterMember = struct {
    node_id: raft.NodeId,
    instance_id: []const u8,
    address: []const u8,
};

pub const Cluster = struct {
    node_id: raft.NodeId,
    state_path: []const u8,
    members: []const ClusterMember,
    heartbeat_interval_ms: u32,
    election_timeout_min_ticks: u16,
    election_timeout_max_ticks: u16,
    rpc_timeout_ms: u32,
    log_capacity: u16,
    command_slot_size: limits.ByteCount,
    snapshot_size_max: u64 = limits.snapshot_size_default_max,
};

pub const LoadedConfig = struct {
    identity: ServiceIdentity,
    contracts: []const DependencyContract,
    client_secret: []const u8,
    credentials: []const auth.Credential,
    wal_path: []const u8,
    connection_capacity: limits.ConnectionCount,
    stream_capacity: limits.StreamCount,
    stream_id_bytes_capacity: limits.ByteCount,
    cluster: ?Cluster,
    allocator: Allocator,

    pub fn deinit(self: *LoadedConfig) void {
        std.crypto.secureZero(u8, @constCast(self.client_secret));
        self.allocator.free(self.client_secret);
        for (self.credentials) |credential| {
            self.allocator.free(credential.service_name);
            std.crypto.secureZero(u8, @constCast(credential.secret));
            self.allocator.free(credential.secret);
        }
        self.allocator.free(self.credentials);
        for (self.contracts) |contract| {
            for (contract.event_types) |et| {
                self.allocator.free(et);
            }
            self.allocator.free(contract.event_types);
            self.allocator.free(contract.source_service);
            self.allocator.free(contract.peer_address);
        }
        self.allocator.free(self.contracts);
        if (self.cluster) |cluster| {
            for (cluster.members) |member| {
                self.allocator.free(member.instance_id);
                self.allocator.free(member.address);
            }
            self.allocator.free(cluster.members);
            self.allocator.free(cluster.state_path);
        }
        self.allocator.free(self.wal_path);
        var id = self.identity;
        id.deinit(self.allocator);
    }
};

pub fn load(allocator: Allocator, path: []const u8, io: Io) !LoadedConfig {
    const file = try Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const file_size_u64 = (try file.stat(io)).size;
    if (file_size_u64 > limits.config_file_size_max) return error.ConfigFileTooLarge;
    const file_size: usize = @intCast(file_size_u64);
    if (file_size == 0) return error.EmptyConfigFile;

    const data = try allocator.alloc(u8, file_size);
    defer allocator.free(data);

    _ = try file.readPositionalAll(io, data, 0);

    return load_from_slice(allocator, data);
}

pub fn load_from_slice(allocator: Allocator, data: []const u8) !LoadedConfig {
    const parsed = try std.json.parseFromSlice(ServiceConfig, allocator, data, .{
        .allocate = .alloc_always,
    });
    defer parsed.deinit();

    const config = parsed.value;
    try validate_config(&config);

    var identity: ServiceIdentity = undefined;
    try ServiceIdentity.init_in_place(&identity, allocator, .{
        .name = config.service_name,
        .instance_id = config.instance_id,
        .listen_address = config.listen_address,
    });
    errdefer identity.deinit(allocator);

    const contracts = try allocator.alloc(DependencyContract, config.contracts.len);
    var contracts_built: u16 = 0;
    errdefer {
        for (contracts[0..contracts_built]) |contract| {
            for (contract.event_types) |event_type| allocator.free(event_type);
            allocator.free(contract.event_types);
            allocator.free(contract.source_service);
            allocator.free(contract.peer_address);
        }
        allocator.free(contracts);
    }

    for (config.contracts) |entry| {
        const event_types = try allocator.alloc([]const u8, entry.event_types.len);
        var event_types_built: u16 = 0;
        errdefer {
            for (event_types[0..event_types_built]) |event_type| allocator.free(event_type);
            allocator.free(event_types);
        }

        for (entry.event_types) |event_type| {
            event_types[event_types_built] = try allocator.dupe(u8, event_type);
            event_types_built += 1;
        }

        const source_service = try allocator.dupe(u8, entry.source_service);
        errdefer allocator.free(source_service);

        const peer_address = try allocator.dupe(u8, entry.peer_address);

        contracts[contracts_built] = .{
            .source_service = source_service,
            .event_types = event_types,
            .peer_address = peer_address,
            .retention_hint = null,
        };
        contracts_built += 1;
    }

    const credentials = try allocator.alloc(auth.Credential, config.credentials.len);
    var credentials_built: u16 = 0;
    errdefer {
        for (credentials[0..credentials_built]) |credential| {
            allocator.free(credential.service_name);
            std.crypto.secureZero(u8, @constCast(credential.secret));
            allocator.free(credential.secret);
        }
        allocator.free(credentials);
    }
    for (config.credentials) |entry| {
        const service_name = try allocator.dupe(u8, entry.service_name);
        errdefer allocator.free(service_name);
        const secret = try allocator.dupe(u8, entry.secret);
        errdefer {
            std.crypto.secureZero(u8, secret);
            allocator.free(secret);
        }
        credentials[credentials_built] = .{
            .service_name = service_name,
            .secret = secret,
            .permissions = try auth.Permissions.from_list(entry.permissions),
        };
        credentials_built += 1;
    }

    const client_secret = try allocator.dupe(u8, config.client_secret.?);
    errdefer {
        std.crypto.secureZero(u8, client_secret);
        allocator.free(client_secret);
    }

    const wal_path = try allocator.dupe(u8, config.wal_path);
    errdefer allocator.free(wal_path);
    const cluster = if (config.cluster) |entry|
        try clone_cluster(allocator, entry)
    else
        null;

    return .{
        .identity = identity,
        .contracts = contracts,
        .client_secret = client_secret,
        .credentials = credentials,
        .wal_path = wal_path,
        .connection_capacity = config.connection_capacity,
        .stream_capacity = config.stream_capacity,
        .stream_id_bytes_capacity = config.stream_id_bytes_capacity,
        .cluster = cluster,
        .allocator = allocator,
    };
}

fn clone_cluster(allocator: Allocator, entry: ClusterEntry) !Cluster {
    const state_path = try allocator.dupe(u8, entry.state_path);
    errdefer allocator.free(state_path);
    const members = try allocator.alloc(ClusterMember, entry.members.len);
    var built: u8 = 0;
    errdefer {
        for (members[0..built]) |member| {
            allocator.free(member.instance_id);
            allocator.free(member.address);
        }
        allocator.free(members);
    }
    for (entry.members) |member| {
        const instance_id = try allocator.dupe(u8, member.instance_id);
        errdefer allocator.free(instance_id);
        const address = try allocator.dupe(u8, member.address);
        members[built] = .{
            .node_id = member.node_id,
            .instance_id = instance_id,
            .address = address,
        };
        built += 1;
    }
    return .{
        .node_id = entry.node_id,
        .state_path = state_path,
        .members = members,
        .heartbeat_interval_ms = entry.heartbeat_interval_ms,
        .election_timeout_min_ticks = timeout_ticks(
            entry.election_timeout_min_ms,
            entry.heartbeat_interval_ms,
        ),
        .election_timeout_max_ticks = timeout_ticks(
            entry.election_timeout_max_ms,
            entry.heartbeat_interval_ms,
        ),
        .rpc_timeout_ms = entry.rpc_timeout_ms,
        .log_capacity = entry.log_capacity,
        .command_slot_size = entry.command_slot_size,
        .snapshot_size_max = entry.snapshot_size_max,
    };
}

fn timeout_ticks(timeout_ms: u32, heartbeat_ms: u32) u16 {
    return @intCast(std.math.divCeil(u32, timeout_ms, heartbeat_ms) catch unreachable);
}

fn validate_config(config: *const ServiceConfig) !void {
    if (config.wal_path.len > limits.wal_path_length_max) return error.WalPathTooLong;
    if (config.contracts.len > limits.contracts_max) return error.TooManyContracts;
    if (config.connection_capacity == 0 or
        config.connection_capacity > limits.connections_max)
        return error.InvalidConnectionCapacity;
    if (config.stream_capacity == 0 or config.stream_capacity > limits.streams_max)
        return error.InvalidStreamCapacity;
    if (config.stream_id_bytes_capacity < config.stream_capacity or
        config.stream_id_bytes_capacity > limits.stream_id_bytes_capacity_max)
        return error.InvalidStreamIdByteCapacity;
    const client_secret = config.client_secret orelse return error.MissingClientSecret;
    try auth.validate_secret(client_secret);
    if (config.credentials.len == 0) return error.MissingCredentials;
    if (config.credentials.len > auth.credentials_max) return error.TooManyCredentials;
    for (config.credentials, 0..) |entry, index| {
        if (entry.service_name.len == 0) return error.InvalidCredentialService;
        if (entry.service_name.len > limits.service_name_length_max)
            return error.ServiceNameTooLong;
        try auth.validate_secret(entry.secret);
        if (entry.permissions.len == 0) return error.MissingPermissions;
        _ = try auth.Permissions.from_list(entry.permissions);
        for (config.credentials[0..index]) |previous| {
            if (std.mem.eql(u8, previous.service_name, entry.service_name))
                return error.DuplicateCredentialService;
        }
    }

    if (config.cluster) |cluster| try validate_cluster(config, cluster, client_secret);

    for (config.contracts) |entry| {
        if (entry.source_service.len > limits.service_name_length_max)
            return error.ServiceNameTooLong;
        if (entry.peer_address.len > limits.peer_address_length_max)
            return error.AddressTooLong;
        if (entry.event_types.len > limits.event_types_per_contract_max)
            return error.TooManyEventTypes;
        for (entry.event_types) |event_type| {
            if (event_type.len > limits.event_type_length_max)
                return error.EventTypeTooLong;
        }
    }
}

fn validate_cluster(
    config: *const ServiceConfig,
    cluster: ClusterEntry,
    client_secret: []const u8,
) !void {
    if (cluster.members.len < raft.nodes_min or cluster.members.len > raft.nodes_max or
        cluster.members.len % 2 == 0)
        return error.InvalidClusterSize;
    if (cluster.state_path.len == 0 or cluster.state_path.len > limits.wal_path_length_max)
        return error.InvalidReplicatedStatePath;
    if (std.mem.eql(u8, cluster.state_path, config.wal_path))
        return error.ReplicatedStatePathConflictsWithWal;
    if (cluster.heartbeat_interval_ms == 0 or
        cluster.heartbeat_interval_ms > std.math.maxInt(u32) / 2 or
        cluster.election_timeout_min_ms < cluster.heartbeat_interval_ms * 2 or
        cluster.election_timeout_max_ms < cluster.election_timeout_min_ms or
        cluster.rpc_timeout_ms == 0 or
        cluster.rpc_timeout_ms >= cluster.election_timeout_min_ms)
        return error.InvalidRaftTimeout;
    const min_ticks = std.math.divCeil(
        u32,
        cluster.election_timeout_min_ms,
        cluster.heartbeat_interval_ms,
    ) catch return error.InvalidRaftTimeout;
    const max_ticks = std.math.divCeil(
        u32,
        cluster.election_timeout_max_ms,
        cluster.heartbeat_interval_ms,
    ) catch return error.InvalidRaftTimeout;
    if (min_ticks < 2 or max_ticks > std.math.maxInt(u16))
        return error.InvalidRaftTimeout;
    if (cluster.log_capacity < 8 or cluster.log_capacity > 1024)
        return error.InvalidRaftLogCapacity;
    if (cluster.command_slot_size < protocol.replicated_command_size_max or
        cluster.command_slot_size > protocol.frame_size_max + 8)
        return error.InvalidReplicationSlotSize;
    if (cluster.snapshot_size_max < protocol.replicated_command_size_max or
        cluster.snapshot_size_max > limits.snapshot_size_hard_max)
        return error.InvalidSnapshotCapacity;

    var own_count: u8 = 0;
    for (cluster.members, 0..) |member, index| {
        if (member.instance_id.len == 0 or
            member.instance_id.len > limits.instance_id_length_max)
            return error.InvalidClusterInstance;
        if (member.address.len == 0 or member.address.len > limits.peer_address_length_max)
            return error.InvalidClusterAddress;
        _ = std.Io.net.IpAddress.parseLiteral(member.address) catch
            return error.InvalidClusterAddress;
        if (member.node_id == cluster.node_id) {
            own_count += 1;
            if (!std.mem.eql(u8, member.instance_id, config.instance_id))
                return error.ClusterIdentityMismatch;
            if (!std.mem.eql(u8, member.address, config.listen_address))
                return error.ClusterAddressMismatch;
        }
        for (cluster.members[0..index]) |previous| {
            if (previous.node_id == member.node_id) return error.DuplicateClusterMember;
            if (std.mem.eql(u8, previous.instance_id, member.instance_id))
                return error.DuplicateClusterInstance;
            if (std.mem.eql(u8, previous.address, member.address))
                return error.DuplicateClusterAddress;
        }
    }
    if (own_count != 1) return error.NodeNotInCluster;

    const cluster_credential = for (config.credentials) |credential| {
        if (std.mem.eql(u8, credential.service_name, config.service_name))
            break credential;
    } else return error.MissingClusterCredential;
    const permissions = try auth.Permissions.from_list(cluster_credential.permissions);
    if (!permissions.replicate) return error.MissingClusterPermission;
    if (!std.mem.eql(u8, cluster_credential.secret, client_secret))
        return error.ClusterSecretMismatch;
}

test "parse valid config" {
    const allocator = std.testing.allocator;

    const json =
        \\{
        \\  "service_name": "order-service",
        \\  "instance_id": "node-1",
        \\  "listen_address": "0.0.0.0:4200",
        \\  "wal_path": "data/events.wal",
        \\  "client_secret": "0123456789abcdef0123456789abcdef",
        \\  "credentials": [{"service_name":"meridian-watch",
        \\    "secret":"abcdef0123456789abcdef0123456789",
        \\    "permissions":["append","read_stream","read_log"]}],
        \\  "contracts": [
        \\    {
        \\      "source_service": "user-service",
        \\      "event_types": ["user.registered", "user.updated"],
        \\      "peer_address": "10.0.0.6:4200"
        \\    }
        \\  ]
        \\}
    ;

    var cfg = try load_from_slice(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqualStrings("order-service", cfg.identity.name);
    try std.testing.expectEqualStrings("node-1", cfg.identity.instance_id);
    try std.testing.expectEqualStrings("0.0.0.0:4200", cfg.identity.listen_address);
    try std.testing.expectEqualStrings("data/events.wal", cfg.wal_path);
    try std.testing.expectEqualStrings(
        "0123456789abcdef0123456789abcdef",
        cfg.client_secret,
    );
    try std.testing.expectEqual(@as(usize, 1), cfg.credentials.len);
    try std.testing.expect(cfg.credentials[0].permissions.append);
    try std.testing.expectEqual(limits.connection_capacity_default, cfg.connection_capacity);
    try std.testing.expectEqual(limits.stream_capacity_default, cfg.stream_capacity);
    try std.testing.expectEqual(
        limits.stream_id_bytes_capacity_default,
        cfg.stream_id_bytes_capacity,
    );

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
        \\  "client_secret": "0123456789abcdef0123456789abcdef",
        \\  "credentials": [{"service_name":"admin",
        \\    "secret":"abcdef0123456789abcdef0123456789",
        \\    "permissions":["append","read_stream","read_log","recover"]}],
        \\  "contracts": []
        \\}
    ;

    var cfg = try load_from_slice(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqualStrings("standalone", cfg.identity.name);
    try std.testing.expectEqual(@as(usize, 0), cfg.contracts.len);
}

test "cluster config binds membership identity timeouts and replication credential" {
    const json =
        \\{
        \\  "service_name":"compix-orders",
        \\  "instance_id":"orders-2",
        \\  "listen_address":"127.0.0.1:4302",
        \\  "wal_path":"data/orders-2.wal",
        \\  "client_secret":"0123456789abcdef0123456789abcdef",
        \\  "credentials":[{"service_name":"compix-orders",
        \\    "secret":"0123456789abcdef0123456789abcdef",
        \\    "permissions":["replicate"]}],
        \\  "contracts":[],
        \\  "cluster":{
        \\    "node_id":2,
        \\    "state_path":"data/orders-2.raft",
        \\    "heartbeat_interval_ms":75,
        \\    "election_timeout_min_ms":500,
        \\    "election_timeout_max_ms":900,
        \\    "rpc_timeout_ms":250,
        \\    "log_capacity":32,
        \\    "members":[
        \\      {"node_id":1,"instance_id":"orders-1","address":"127.0.0.1:4301"},
        \\      {"node_id":2,"instance_id":"orders-2","address":"127.0.0.1:4302"},
        \\      {"node_id":3,"instance_id":"orders-3","address":"127.0.0.1:4303"}
        \\    ]
        \\  }
        \\}
    ;
    var cfg = try load_from_slice(std.testing.allocator, json);
    defer cfg.deinit();
    const cluster = cfg.cluster.?;
    try std.testing.expectEqual(@as(raft.NodeId, 2), cluster.node_id);
    try std.testing.expectEqual(@as(u16, 7), cluster.election_timeout_min_ticks);
    try std.testing.expectEqual(@as(u16, 12), cluster.election_timeout_max_ticks);
    try std.testing.expectEqual(@as(u16, 32), cluster.log_capacity);
    try std.testing.expectEqualStrings("orders-3", cluster.members[2].instance_id);
    try std.testing.expectEqualStrings("127.0.0.1:4301", cluster.members[0].address);
}

test "cluster config fails closed on identity and credential mismatch" {
    const wrong_identity =
        \\{"service_name":"c","instance_id":"node-9","listen_address":"127.0.0.1:1",
        \\ "wal_path":"data/c.wal","client_secret":"0123456789abcdef0123456789abcdef",
        \\ "credentials":[{"service_name":"c","secret":"0123456789abcdef0123456789abcdef",
        \\ "permissions":["replicate"]}],"contracts":[],"cluster":{"node_id":1,
        \\ "state_path":"data/c.raft","members":[
        \\ {"node_id":1,"instance_id":"node-1","address":"127.0.0.1:1"},
        \\ {"node_id":2,"instance_id":"node-2","address":"127.0.0.1:2"},
        \\ {"node_id":3,"instance_id":"node-3","address":"127.0.0.1:3"}]}}
    ;
    try std.testing.expectError(
        error.ClusterIdentityMismatch,
        load_from_slice(std.testing.allocator, wrong_identity),
    );

    const no_replicate =
        \\{"service_name":"c","instance_id":"node-1","listen_address":"127.0.0.1:1",
        \\ "wal_path":"data/c.wal","client_secret":"0123456789abcdef0123456789abcdef",
        \\ "credentials":[{"service_name":"c","secret":"0123456789abcdef0123456789abcdef",
        \\ "permissions":["observe"]}],"contracts":[],"cluster":{"node_id":1,
        \\ "state_path":"data/c.raft","members":[
        \\ {"node_id":1,"instance_id":"node-1","address":"127.0.0.1:1"},
        \\ {"node_id":2,"instance_id":"node-2","address":"127.0.0.1:2"},
        \\ {"node_id":3,"instance_id":"node-3","address":"127.0.0.1:3"}]}}
    ;
    try std.testing.expectError(
        error.MissingClusterPermission,
        load_from_slice(std.testing.allocator, no_replicate),
    );
}

test "capacity configuration rejects zero and compiled-limit overflow" {
    const allocator = std.testing.allocator;
    const zero_connections =
        \\{"service_name":"s","instance_id":"n","listen_address":"127.0.0.1:1",
        \\ "wal_path":"data/s.wal","contracts":[],"connection_capacity":0}
    ;
    try std.testing.expectError(
        error.InvalidConnectionCapacity,
        load_from_slice(allocator, zero_connections),
    );
    const stream_overflow =
        \\{"service_name":"s","instance_id":"n","listen_address":"127.0.0.1:1",
        \\ "wal_path":"data/s.wal","contracts":[],"stream_capacity":16777217}
    ;
    try std.testing.expectError(
        error.InvalidStreamCapacity,
        load_from_slice(allocator, stream_overflow),
    );
    const insufficient_id_bytes =
        \\{"service_name":"s","instance_id":"n","listen_address":"127.0.0.1:1",
        \\ "wal_path":"data/s.wal","contracts":[],"stream_capacity":100,
        \\ "stream_id_bytes_capacity":99}
    ;
    try std.testing.expectError(
        error.InvalidStreamIdByteCapacity,
        load_from_slice(allocator, insufficient_id_bytes),
    );
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

    const result = load_from_slice(allocator, json);
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
        \\  "client_secret": "0123456789abcdef0123456789abcdef",
        \\  "credentials": [{"service_name":"admin",
        \\    "secret":"abcdef0123456789abcdef0123456789",
        \\    "permissions":["append","read_stream","read_log","recover"]}],
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

    var cfg = try load_from_slice(allocator, json);
    defer cfg.deinit();

    try std.testing.expectEqual(@as(usize, 2), cfg.contracts.len);
    try std.testing.expectEqualStrings("order-service", cfg.contracts[0].source_service);
    try std.testing.expectEqualStrings("user-service", cfg.contracts[1].source_service);
    try std.testing.expectEqual(@as(usize, 1), cfg.contracts[0].event_types.len);
    try std.testing.expectEqual(@as(usize, 2), cfg.contracts[1].event_types.len);
}
