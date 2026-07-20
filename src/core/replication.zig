//! Live ownership of the bounded Raft machine and its atomic command checkpoint.
//!
//! Socket scheduling remains in `server.zig`; every method here is called while
//! the server's replication mutex is held. `checkpoint_and_apply` establishes
//! the required ordering: Raft state and command bytes are durable before any
//! protocol response, committed stream data is then fsynced, and the advanced
//! applied index is checkpointed last.

const std = @import("std");
const auth = @import("auth.zig");
const config_mod = @import("config.zig");
const limits = @import("limits.zig");
const protocol = @import("protocol.zig");
const raft = @import("raft.zig");
const replicated = @import("replicated_store.zig");
const storage = @import("replicated_storage.zig");
const snapshot_storage = @import("snapshot_storage.zig");
const store_mod = @import("store.zig");

const Allocator = std.mem.Allocator;
const EventStore = store_mod.EventStore;

pub const outbox_capacity: u16 = 64;

pub const SnapshotProgress = struct {
    result: raft.SnapshotResult,
    next_offset: u64,
    accepted: bool,
};

pub const Runtime = struct {
    allocator: Allocator,
    io: std.Io,
    config: config_mod.Cluster,
    member_ids: []raft.NodeId,
    entries: []raft.Entry,
    command_bytes: []u8,
    command_indices: []raft.Index,
    command_lengths: []limits.ByteCount,
    results: []replicated.ResultSlot,
    decode_events: []protocol.AppendEvent,
    state_scratch: []u8,
    snapshot_paths: snapshot_storage.Paths,
    snapshot_scratch: []u8,
    outbox_storage: []raft.Envelope,
    outbox: raft.Outbox,
    machine: replicated.Machine,
    recovery_findings: u16,

    pub fn init(
        allocator: Allocator,
        io: std.Io,
        cluster: config_mod.Cluster,
        event_store: *EventStore,
        election_seed: u64,
    ) !Runtime {
        const capacity: usize = cluster.log_capacity;
        const member_ids = try allocator.alloc(raft.NodeId, cluster.members.len);
        errdefer allocator.free(member_ids);
        for (cluster.members, 0..) |cluster_member, index| {
            member_ids[index] = cluster_member.node_id;
        }

        const entries = try allocator.alloc(raft.Entry, capacity);
        errdefer allocator.free(entries);
        const command_indices = try allocator.alloc(raft.Index, capacity);
        errdefer allocator.free(command_indices);
        const command_lengths = try allocator.alloc(limits.ByteCount, capacity);
        errdefer allocator.free(command_lengths);
        const results = try allocator.alloc(replicated.ResultSlot, capacity);
        errdefer allocator.free(results);
        const command_bytes_size = std.math.mul(
            usize,
            capacity,
            cluster.command_slot_size,
        ) catch return error.InvalidReplicationCapacity;
        const command_bytes = try allocator.alloc(u8, command_bytes_size);
        errdefer allocator.free(command_bytes);
        const decode_events = try allocator.alloc(
            protocol.AppendEvent,
            protocol.append_events_max,
        );
        errdefer allocator.free(decode_events);
        const state_scratch_size = try storage.state_size_max(
            capacity,
            cluster.command_slot_size,
        );
        const state_scratch = try allocator.alloc(u8, state_scratch_size);
        errdefer allocator.free(state_scratch);
        const snapshot_paths = try snapshot_storage.Paths.init(allocator, cluster.state_path);
        errdefer snapshot_paths.deinit(allocator);
        const recovery_findings: u16 = @intFromBool(
            try snapshot_storage.quarantine_incoming(io, snapshot_paths),
        );
        const snapshot_scratch_size = @max(
            @as(usize, snapshot_storage.chunk_size_max),
            @as(usize, cluster.command_slot_size),
        );
        const snapshot_scratch = try allocator.alloc(u8, snapshot_scratch_size);
        errdefer allocator.free(snapshot_scratch);
        const outbox_storage = try allocator.alloc(raft.Envelope, outbox_capacity);
        errdefer allocator.free(outbox_storage);

        const recovered = try storage.load(
            io,
            cluster.state_path,
            cluster.node_id,
            member_ids,
            entries,
            command_bytes,
            command_indices,
            command_lengths,
            cluster.command_slot_size,
            state_scratch,
        );
        if (recovered == null and event_store.metrics().committed_log_position != 0)
            return error.MissingReplicatedStateForNonEmptyStore;
        const restored = recovered orelse storage.Recovered{
            .hard_state = .{},
            .snapshot = .{
                .last_index = 0,
                .last_term = 0,
                .state_size = 0,
                .state_checksum = @splat(0),
            },
            .entry_count = 0,
            .commit_index = 0,
            .applied_index = 0,
        };
        if (restored.snapshot.state_size > cluster.snapshot_size_max)
            return error.SnapshotCapacityExceeded;
        try snapshot_storage.verify(
            io,
            snapshot_paths.archive,
            restored.snapshot,
            member_ids,
            snapshot_scratch,
        );
        // Reapply the retained committed log after every restart. Canonical
        // command ids make this idempotent and close the crash window where the
        // store fsync completed just before the applied-index checkpoint.
        const replay_from = restored.snapshot.last_index;
        var machine = try replicated.Machine.init(.{
            .raft_config = .{
                .id = cluster.node_id,
                .members = member_ids,
                .log_storage = entries,
                .hard_state = restored.hard_state,
                .snapshot = restored.snapshot,
                .election_timeout_ticks = cluster.election_timeout_min_ticks,
                .election_timeout_ticks_max = cluster.election_timeout_max_ticks,
                .election_seed = election_seed,
                .heartbeat_ticks = 1,
                .restored_entry_count = restored.entry_count,
                .restored_commit_index = restored.commit_index,
                .restored_applied_index = replay_from,
            },
            .store = event_store,
            .command_bytes = command_bytes,
            .command_indices = command_indices,
            .command_lengths = command_lengths,
            .results = results,
            .decode_events = decode_events,
            .slot_size = cluster.command_slot_size,
            .restored_commands = recovered != null,
        });
        try machine.apply_committed();
        try storage.save(io, cluster.state_path, &machine, state_scratch);
        var outbox = try raft.Outbox.init(outbox_storage);
        outbox.reset();
        return .{
            .allocator = allocator,
            .io = io,
            .config = cluster,
            .member_ids = member_ids,
            .entries = entries,
            .command_bytes = command_bytes,
            .command_indices = command_indices,
            .command_lengths = command_lengths,
            .results = results,
            .decode_events = decode_events,
            .state_scratch = state_scratch,
            .snapshot_paths = snapshot_paths,
            .snapshot_scratch = snapshot_scratch,
            .outbox_storage = outbox_storage,
            .outbox = outbox,
            .machine = machine,
            .recovery_findings = recovery_findings,
        };
    }

    pub fn deinit(self: *Runtime) void {
        self.allocator.free(self.outbox_storage);
        self.allocator.free(self.snapshot_scratch);
        self.snapshot_paths.deinit(self.allocator);
        self.allocator.free(self.state_scratch);
        self.allocator.free(self.decode_events);
        self.allocator.free(self.command_bytes);
        self.allocator.free(self.results);
        self.allocator.free(self.command_lengths);
        self.allocator.free(self.command_indices);
        self.allocator.free(self.entries);
        self.allocator.free(self.member_ids);
    }

    pub fn checkpoint_and_apply(self: *Runtime) !void {
        try storage.save(
            self.io,
            self.config.state_path,
            &self.machine,
            self.state_scratch,
        );
        const applied_before = self.machine.node.applied_index;
        try self.machine.apply_committed();
        if (self.machine.node.applied_index != applied_before) {
            try storage.save(
                self.io,
                self.config.state_path,
                &self.machine,
                self.state_scratch,
            );
        }
        try self.compact_applied_log();
    }

    fn compact_applied_log(self: *Runtime) !void {
        const threshold: u16 = @max(@as(u16, 1), self.config.log_capacity / 2);
        const applied_span = self.machine.node.applied_index -
            self.machine.node.snapshot.last_index;
        if (applied_span < threshold) return;
        const snapshot = try snapshot_storage.build(
            self.io,
            self.snapshot_paths.archive,
            &self.machine,
            self.machine.node.applied_index,
            self.config.snapshot_size_max,
            self.snapshot_scratch,
        );
        try self.machine.node.compact(snapshot);
        try storage.save(
            self.io,
            self.config.state_path,
            &self.machine,
            self.state_scratch,
        );
    }

    pub fn receive_snapshot_chunk(
        self: *Runtime,
        from: raft.NodeId,
        install: protocol.RaftInstallSnapshot,
    ) !SnapshotProgress {
        if (install.value.leader != from or install.value.snapshot.state_size == 0 or
            install.value.snapshot.state_size > self.config.snapshot_size_max)
            return error.InvalidSnapshot;
        const accepted = try self.machine.node.prepare_snapshot(install.value.term, from);
        if (!accepted) return .{
            .result = self.snapshot_result(),
            .next_offset = 0,
            .accepted = false,
        };
        try storage.save(
            self.io,
            self.config.state_path,
            &self.machine,
            self.state_scratch,
        );
        const next_offset = try snapshot_storage.stage_chunk(
            self.io,
            self.snapshot_paths.incoming,
            install.value.snapshot,
            install.offset,
            install.chunk,
            install.done,
        );
        if (!install.done) return .{
            .result = self.snapshot_result(),
            .next_offset = next_offset,
            .accepted = true,
        };
        try snapshot_storage.replay(
            self.io,
            self.snapshot_paths.incoming,
            install.value.snapshot,
            self.member_ids,
            &self.machine,
            self.snapshot_scratch,
        );
        try snapshot_storage.publish(
            self.io,
            self.snapshot_paths.incoming,
            self.snapshot_paths.archive,
        );
        self.outbox.reset();
        try self.machine.receive(.{
            .from = from,
            .to = self.config.node_id,
            .message = .{ .install_snapshot = install.value },
        }, null, &self.outbox);
        try storage.save(
            self.io,
            self.config.state_path,
            &self.machine,
            self.state_scratch,
        );
        if (self.outbox.count != 1 or self.outbox.items()[0].message != .snapshot_result)
            return error.InvalidRaftResponseCount;
        const result = self.outbox.items()[0].message.snapshot_result;
        self.outbox.reset();
        return .{ .result = result, .next_offset = next_offset, .accepted = true };
    }

    fn snapshot_result(self: *const Runtime) raft.SnapshotResult {
        return .{
            .term = self.machine.node.hard.term,
            .follower = self.config.node_id,
            .installed_index = self.machine.node.snapshot.last_index,
        };
    }

    pub fn take_outbox(self: *Runtime, target: []raft.Envelope) !usize {
        if (target.len < self.outbox.count) return error.RaftDispatchQueueFull;
        const count = self.outbox.count;
        @memcpy(target[0..count], self.outbox.items());
        self.outbox.reset();
        return count;
    }

    pub fn member(self: *const Runtime, id: raft.NodeId) ?config_mod.ClusterMember {
        for (self.config.members) |candidate| {
            if (candidate.node_id == id) return candidate;
        }
        return null;
    }

    pub fn node_for_authenticated_instance(
        self: *const Runtime,
        service_name: []const u8,
        instance_id: []const u8,
        permissions: auth.Permissions,
        expected_service: []const u8,
    ) ?raft.NodeId {
        if (!permissions.replicate or !std.mem.eql(u8, service_name, expected_service))
            return null;
        for (self.config.members) |candidate| {
            if (candidate.node_id != self.config.node_id and
                std.mem.eql(u8, candidate.instance_id, instance_id))
                return candidate.node_id;
        }
        return null;
    }

    pub fn replication_lag(self: *const Runtime) u64 {
        return self.machine.node.last_index() - self.machine.node.applied_index;
    }
};
