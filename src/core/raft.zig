//! Allocation-free Raft consensus state machine.
//!
//! Persistence and transport are deliberately injected by the caller: `hard_state`
//! must be durably recorded before emitted messages are sent, and committed entries
//! must be applied in index order. This module owns no threads, sockets, or files.

const std = @import("std");
const assert = std.debug.assert;

pub const NodeId = u8;
pub const Term = u64;
pub const Index = u64;
pub const nodes_max: u8 = 5;
pub const nodes_min: u8 = 3;

pub const Role = enum { follower, candidate, leader };

pub const Entry = struct {
    term: Term,
    index: Index,
    command_checksum: [32]u8,
};

pub const HardState = struct {
    term: Term = 0,
    voted_for: ?NodeId = null,
};

pub const Snapshot = struct {
    last_index: Index,
    last_term: Term,
    state_size: u64,
    state_checksum: [32]u8,
};

pub const RequestVote = struct {
    term: Term,
    candidate: NodeId,
    last_log_index: Index,
    last_log_term: Term,
};

pub const VoteResult = struct {
    term: Term,
    voter: NodeId,
    granted: bool,
};

pub const AppendEntries = struct {
    term: Term,
    leader: NodeId,
    previous_index: Index,
    previous_term: Term,
    leader_commit: Index,
    entry: ?Entry,
};

pub const AppendResult = struct {
    term: Term,
    follower: NodeId,
    success: bool,
    match_index: Index,
    conflict_index: Index,
};

pub const InstallSnapshot = struct {
    term: Term,
    leader: NodeId,
    snapshot: Snapshot,
};

pub const SnapshotResult = struct {
    term: Term,
    follower: NodeId,
    installed_index: Index,
};

pub const Message = union(enum) {
    request_vote: RequestVote,
    vote_result: VoteResult,
    append_entries: AppendEntries,
    append_result: AppendResult,
    install_snapshot: InstallSnapshot,
    snapshot_result: SnapshotResult,
};

pub const Envelope = struct {
    from: NodeId,
    to: NodeId,
    message: Message,
};

pub const Outbox = struct {
    storage: []Envelope,
    count: u16 = 0,

    pub fn init(storage: []Envelope) !Outbox {
        if (storage.len == 0 or storage.len > std.math.maxInt(u16))
            return error.InvalidOutboxCapacity;
        return .{ .storage = storage };
    }

    pub fn items(self: *const Outbox) []const Envelope {
        return self.storage[0..self.count];
    }

    pub fn reset(self: *Outbox) void {
        self.count = 0;
    }

    fn emit(self: *Outbox, envelope: Envelope) !void {
        if (self.count == self.storage.len) return error.OutboxFull;
        self.storage[self.count] = envelope;
        self.count += 1;
    }
};

pub const Config = struct {
    id: NodeId,
    members: []const NodeId,
    log_storage: []Entry,
    hard_state: HardState = .{},
    snapshot: Snapshot = .{
        .last_index = 0,
        .last_term = 0,
        .state_size = 0,
        .state_checksum = @splat(0),
    },
    election_timeout_ticks: u16,
    election_timeout_ticks_max: ?u16 = null,
    election_seed: u64 = 0,
    heartbeat_ticks: u16,
    restored_entry_count: u16 = 0,
    restored_commit_index: ?Index = null,
    restored_applied_index: ?Index = null,
};

pub const Node = struct {
    id: NodeId,
    members: []const NodeId,
    entries: []Entry,
    entry_count: u16,
    hard: HardState,
    hard_state_dirty: bool,
    snapshot: Snapshot,
    role: Role,
    leader_id: ?NodeId,
    commit_index: Index,
    applied_index: Index,
    election_elapsed: u16,
    election_timeout: u16,
    election_timeout_min: u16,
    election_timeout_max: u16,
    election_random_state: u64,
    heartbeat_elapsed: u16,
    heartbeat_interval: u16,
    votes: u8,
    quorum_acks: u8,
    read_barrier_ready: bool,
    next_index: [nodes_max]Index,
    match_index: [nodes_max]Index,

    pub fn init(config: Config) !Node {
        if (config.members.len < nodes_min or config.members.len > nodes_max or
            config.members.len % 2 == 0)
            return error.InvalidClusterSize;
        if (config.log_storage.len == 0 or
            config.log_storage.len > std.math.maxInt(u16))
            return error.InvalidLogCapacity;
        if (config.restored_entry_count > config.log_storage.len)
            return error.InvalidRestoredState;
        const election_timeout_max = config.election_timeout_ticks_max orelse
            config.election_timeout_ticks;
        if (config.election_timeout_ticks < 2 or
            election_timeout_max < config.election_timeout_ticks or
            config.heartbeat_ticks == 0 or
            config.heartbeat_ticks >= config.election_timeout_ticks)
            return error.InvalidRaftTimeout;
        var own_count: u8 = 0;
        for (config.members, 0..) |member, index| {
            if (member == config.id) own_count += 1;
            for (config.members[0..index]) |previous| {
                if (previous == member) return error.DuplicateClusterMember;
            }
        }
        if (own_count != 1) return error.NodeNotInCluster;
        if (config.snapshot.last_index == 0 and config.snapshot.last_term != 0)
            return error.InvalidSnapshot;
        if (config.snapshot.last_index == std.math.maxInt(Index))
            return error.InvalidSnapshot;
        if (config.hard_state.term == 0 and config.hard_state.voted_for != null)
            return error.InvalidHardState;
        if (config.hard_state.voted_for) |voted_for| {
            var found = false;
            for (config.members) |member| if (member == voted_for) {
                found = true;
            };
            if (!found) return error.InvalidHardState;
        }
        var expected_index = std.math.add(Index, config.snapshot.last_index, 1) catch
            return error.InvalidRestoredState;
        for (config.log_storage[0..config.restored_entry_count]) |entry| {
            if (entry.index != expected_index or entry.term == 0)
                return error.InvalidRestoredState;
            expected_index = std.math.add(Index, expected_index, 1) catch
                return error.InvalidRestoredState;
        }
        const restored_last_index = std.math.add(
            Index,
            config.snapshot.last_index,
            config.restored_entry_count,
        ) catch return error.InvalidRestoredState;
        const commit_index = config.restored_commit_index orelse config.snapshot.last_index;
        const applied_index = config.restored_applied_index orelse config.snapshot.last_index;
        if (commit_index < config.snapshot.last_index or
            commit_index > restored_last_index or
            applied_index < config.snapshot.last_index or applied_index > commit_index)
            return error.InvalidRestoredState;
        var election_random_state = config.election_seed ^
            (@as(u64, config.id) *% 0x9e3779b97f4a7c15);
        if (election_random_state == 0) election_random_state = 0xd1b54a32d192ed03;
        const election_timeout = next_election_timeout(
            config.election_timeout_ticks,
            election_timeout_max,
            &election_random_state,
        );
        return .{
            .id = config.id,
            .members = config.members,
            .entries = config.log_storage,
            .entry_count = config.restored_entry_count,
            .hard = config.hard_state,
            .hard_state_dirty = false,
            .snapshot = config.snapshot,
            .role = .follower,
            .leader_id = null,
            .commit_index = commit_index,
            .applied_index = applied_index,
            .election_elapsed = 0,
            .election_timeout = election_timeout,
            .election_timeout_min = config.election_timeout_ticks,
            .election_timeout_max = election_timeout_max,
            .election_random_state = election_random_state,
            .heartbeat_elapsed = 0,
            .heartbeat_interval = config.heartbeat_ticks,
            .votes = 0,
            .quorum_acks = 0,
            .read_barrier_ready = false,
            .next_index = @splat(config.snapshot.last_index + 1),
            .match_index = @splat(0),
        };
    }

    pub fn tick(self: *Node, outbox: *Outbox) !void {
        switch (self.role) {
            .leader => {
                self.heartbeat_elapsed += 1;
                if (self.heartbeat_elapsed >= self.heartbeat_interval) {
                    self.heartbeat_elapsed = 0;
                    try self.begin_linearizable_read(outbox);
                }
            },
            .follower, .candidate => {
                self.election_elapsed += 1;
                if (self.election_elapsed >= self.election_timeout)
                    try self.start_election(outbox);
            },
        }
    }

    pub fn hard_state_needs_persist(self: *const Node) bool {
        return self.hard_state_dirty;
    }

    pub fn mark_hard_state_persisted(self: *Node) void {
        self.hard_state_dirty = false;
    }

    pub fn receive(self: *Node, envelope: Envelope, outbox: *Outbox) !void {
        if (envelope.to != self.id or !self.is_member(envelope.from) or
            message_sender(envelope.message) != envelope.from)
            return error.InvalidRaftEnvelope;
        const term = message_term(envelope.message);
        if (term > self.hard.term) self.become_follower(term, null);
        switch (envelope.message) {
            .request_vote => |request| try self.receive_vote_request(request, outbox),
            .vote_result => |result| try self.receive_vote_result(result, outbox),
            .append_entries => |append| try self.receive_append(append, outbox),
            .append_result => |result| try self.receive_append_result(result, outbox),
            .install_snapshot => |install| try self.receive_snapshot(install, outbox),
            .snapshot_result => |result| try self.receive_snapshot_result(result, outbox),
        }
        assert(self.commit_index <= self.last_index());
        assert(self.applied_index <= self.commit_index);
    }

    pub fn prepare_snapshot(self: *Node, term: Term, leader: NodeId) !bool {
        if (!self.is_member(leader) or leader == self.id) return error.InvalidRaftEnvelope;
        if (term < self.hard.term) return false;
        self.become_follower(term, leader);
        self.election_elapsed = 0;
        return true;
    }

    pub fn propose(self: *Node, command_checksum: [32]u8, outbox: *Outbox) !Index {
        if (self.role != .leader) return error.NotLeader;
        if (self.entry_count == self.entries.len) return error.RaftLogFull;
        const index = std.math.add(Index, self.last_index(), 1) catch
            return error.RaftIndexOverflow;
        self.entries[self.entry_count] = .{
            .term = self.hard.term,
            .index = index,
            .command_checksum = command_checksum,
        };
        self.entry_count += 1;
        self.match_index[self.member_slot(self.id).?] = index;
        try self.broadcast_replication(outbox);
        return index;
    }

    pub fn begin_linearizable_read(self: *Node, outbox: *Outbox) !void {
        if (self.role != .leader) return error.NotLeader;
        self.quorum_acks = member_bit(self.member_slot(self.id).?);
        self.read_barrier_ready = false;
        try self.broadcast_replication(outbox);
    }

    pub fn linearizable_read_ready(self: *const Node) bool {
        return self.role == .leader and self.read_barrier_ready;
    }

    pub fn follower_stale_read_index(self: *const Node) Index {
        return self.applied_index;
    }

    pub fn next_committed(self: *const Node) ?Entry {
        if (self.applied_index == self.commit_index) return null;
        return self.entry_at(self.applied_index + 1);
    }

    pub fn mark_applied(self: *Node, index: Index) !void {
        if (index != self.applied_index + 1 or index > self.commit_index)
            return error.InvalidAppliedIndex;
        self.applied_index = index;
    }

    pub fn compact(self: *Node, snapshot: Snapshot) !void {
        if (snapshot.last_index <= self.snapshot.last_index or
            snapshot.last_index > self.applied_index)
            return error.InvalidSnapshot;
        const term = self.term_at(snapshot.last_index) orelse return error.InvalidSnapshot;
        if (term != snapshot.last_term) return error.InvalidSnapshot;
        self.discard_through(snapshot.last_index);
        self.snapshot = snapshot;
        assert(self.snapshot.last_index <= self.applied_index);
    }

    pub fn last_index(self: *const Node) Index {
        return std.math.add(Index, self.snapshot.last_index, self.entry_count) catch
            unreachable;
    }

    pub fn last_term(self: *const Node) Term {
        if (self.entry_count == 0) return self.snapshot.last_term;
        return self.entries[self.entry_count - 1].term;
    }

    fn start_election(self: *Node, outbox: *Outbox) !void {
        self.hard.term = std.math.add(Term, self.hard.term, 1) catch
            return error.TermOverflow;
        self.hard.voted_for = self.id;
        self.hard_state_dirty = true;
        self.role = .candidate;
        self.leader_id = null;
        self.election_elapsed = 0;
        self.reset_election_timeout();
        self.votes = member_bit(self.member_slot(self.id).?);
        self.read_barrier_ready = false;
        for (self.members) |member| {
            if (member == self.id) continue;
            try outbox.emit(.{
                .from = self.id,
                .to = member,
                .message = .{ .request_vote = .{
                    .term = self.hard.term,
                    .candidate = self.id,
                    .last_log_index = self.last_index(),
                    .last_log_term = self.last_term(),
                } },
            });
        }
    }

    fn receive_vote_request(self: *Node, request: RequestVote, outbox: *Outbox) !void {
        var granted = false;
        if (request.term == self.hard.term) {
            const can_vote = self.hard.voted_for == null or
                self.hard.voted_for.? == request.candidate;
            const up_to_date = request.last_log_term > self.last_term() or
                (request.last_log_term == self.last_term() and
                    request.last_log_index >= self.last_index());
            if (can_vote and up_to_date) {
                self.hard.voted_for = request.candidate;
                self.hard_state_dirty = true;
                self.election_elapsed = 0;
                self.reset_election_timeout();
                granted = true;
            }
        }
        try outbox.emit(.{
            .from = self.id,
            .to = request.candidate,
            .message = .{ .vote_result = .{
                .term = self.hard.term,
                .voter = self.id,
                .granted = granted,
            } },
        });
    }

    fn receive_vote_result(self: *Node, result: VoteResult, outbox: *Outbox) !void {
        if (result.term != self.hard.term or self.role != .candidate or !result.granted) return;
        const slot = self.member_slot(result.voter) orelse return error.InvalidRaftEnvelope;
        self.votes |= member_bit(slot);
        if (@popCount(self.votes) < self.quorum()) return;
        self.role = .leader;
        self.leader_id = self.id;
        self.heartbeat_elapsed = 0;
        self.read_barrier_ready = false;
        const next = std.math.add(Index, self.last_index(), 1) catch
            return error.RaftIndexOverflow;
        for (self.members, 0..) |member, index| {
            self.next_index[index] = next;
            self.match_index[index] = if (member == self.id) self.last_index() else 0;
        }
        try self.begin_linearizable_read(outbox);
    }

    fn receive_append(self: *Node, append: AppendEntries, outbox: *Outbox) !void {
        if (append.term < self.hard.term) {
            return self.reject_append(
                append.leader,
                std.math.add(Index, self.last_index(), 1) catch std.math.maxInt(Index),
                outbox,
            );
        }
        if (append.term == self.hard.term) self.become_follower(append.term, append.leader);
        self.election_elapsed = 0;
        const previous_term = self.term_at(append.previous_index) orelse {
            return self.reject_append(
                append.leader,
                std.math.add(Index, self.last_index(), 1) catch std.math.maxInt(Index),
                outbox,
            );
        };
        if (previous_term != append.previous_term) {
            var conflict = append.previous_index;
            while (conflict > self.snapshot.last_index + 1 and
                self.term_at(conflict - 1) == previous_term)
                conflict -= 1;
            return self.reject_append(append.leader, conflict, outbox);
        }
        if (append.entry) |entry| {
            if (entry.index != append.previous_index + 1 or entry.term == 0)
                return error.InvalidRaftEntry;
            if (self.entry_at(entry.index)) |existing| {
                if (existing.term != entry.term or
                    !std.mem.eql(
                        u8,
                        &existing.command_checksum,
                        &entry.command_checksum,
                    )) self.truncate_from(entry.index);
            }
            if (entry.index > self.last_index()) try self.append_received(entry);
        }
        self.commit_index = @max(
            self.commit_index,
            @min(append.leader_commit, self.last_index()),
        );
        try outbox.emit(.{
            .from = self.id,
            .to = append.leader,
            .message = .{ .append_result = .{
                .term = self.hard.term,
                .follower = self.id,
                .success = true,
                .match_index = self.last_index(),
                .conflict_index = 0,
            } },
        });
    }

    fn receive_append_result(self: *Node, result: AppendResult, outbox: *Outbox) !void {
        if (result.term != self.hard.term or self.role != .leader) return;
        const slot = self.member_slot(result.follower) orelse return error.InvalidRaftEnvelope;
        if (!result.success) {
            self.next_index[slot] = @max(
                1,
                @min(result.conflict_index, self.last_index() + 1),
            );
            return self.send_replication(result.follower, outbox);
        }
        self.match_index[slot] = @max(self.match_index[slot], result.match_index);
        self.next_index[slot] = std.math.add(Index, self.match_index[slot], 1) catch
            std.math.maxInt(Index);
        self.quorum_acks |= member_bit(slot);
        if (@popCount(self.quorum_acks) >= self.quorum()) self.read_barrier_ready = true;
        const before = self.commit_index;
        self.advance_commit();
        if (self.commit_index != before) try self.broadcast_replication(outbox);
        if (self.next_index[slot] <= self.last_index())
            try self.send_replication(result.follower, outbox);
    }

    fn receive_snapshot(self: *Node, install: InstallSnapshot, outbox: *Outbox) !void {
        if (install.term < self.hard.term) {
            return outbox.emit(.{
                .from = self.id,
                .to = install.leader,
                .message = .{ .snapshot_result = .{
                    .term = self.hard.term,
                    .follower = self.id,
                    .installed_index = self.snapshot.last_index,
                } },
            });
        }
        self.become_follower(install.term, install.leader);
        self.election_elapsed = 0;
        if (install.snapshot.last_index > self.snapshot.last_index) {
            if (self.term_at(install.snapshot.last_index)) |term| {
                if (term == install.snapshot.last_term) {
                    self.discard_through(install.snapshot.last_index);
                } else {
                    self.entry_count = 0;
                }
            } else {
                self.entry_count = 0;
            }
            self.snapshot = install.snapshot;
            self.commit_index = @max(self.commit_index, install.snapshot.last_index);
            self.applied_index = @max(self.applied_index, install.snapshot.last_index);
        }
        try outbox.emit(.{
            .from = self.id,
            .to = install.leader,
            .message = .{ .snapshot_result = .{
                .term = self.hard.term,
                .follower = self.id,
                .installed_index = self.snapshot.last_index,
            } },
        });
    }

    fn receive_snapshot_result(self: *Node, result: SnapshotResult, outbox: *Outbox) !void {
        if (result.term != self.hard.term or self.role != .leader) return;
        const slot = self.member_slot(result.follower) orelse return error.InvalidRaftEnvelope;
        self.match_index[slot] = @max(self.match_index[slot], result.installed_index);
        self.next_index[slot] = std.math.add(Index, self.match_index[slot], 1) catch
            std.math.maxInt(Index);
        self.quorum_acks |= member_bit(slot);
        try self.send_replication(result.follower, outbox);
    }

    fn reject_append(
        self: *Node,
        leader: NodeId,
        conflict_index: Index,
        outbox: *Outbox,
    ) !void {
        try outbox.emit(.{
            .from = self.id,
            .to = leader,
            .message = .{ .append_result = .{
                .term = self.hard.term,
                .follower = self.id,
                .success = false,
                .match_index = 0,
                .conflict_index = conflict_index,
            } },
        });
    }

    fn broadcast_replication(self: *Node, outbox: *Outbox) !void {
        for (self.members) |member| {
            if (member != self.id) try self.send_replication(member, outbox);
        }
    }

    fn send_replication(self: *Node, follower: NodeId, outbox: *Outbox) !void {
        const slot = self.member_slot(follower) orelse return error.InvalidRaftEnvelope;
        const next = self.next_index[slot];
        if (next <= self.snapshot.last_index) {
            return outbox.emit(.{
                .from = self.id,
                .to = follower,
                .message = .{ .install_snapshot = .{
                    .term = self.hard.term,
                    .leader = self.id,
                    .snapshot = self.snapshot,
                } },
            });
        }
        const previous_index = next - 1;
        const previous_term = self.term_at(previous_index) orelse
            return error.InvalidNextIndex;
        try outbox.emit(.{
            .from = self.id,
            .to = follower,
            .message = .{ .append_entries = .{
                .term = self.hard.term,
                .leader = self.id,
                .previous_index = previous_index,
                .previous_term = previous_term,
                .leader_commit = self.commit_index,
                .entry = self.entry_at(next),
            } },
        });
    }

    fn advance_commit(self: *Node) void {
        var candidate = self.last_index();
        while (candidate > self.commit_index) : (candidate -= 1) {
            if (self.term_at(candidate).? != self.hard.term) continue;
            var replicated: u8 = 0;
            for (self.members, 0..) |_, slot| {
                if (self.match_index[slot] >= candidate) replicated += 1;
            }
            if (replicated >= self.quorum()) {
                self.commit_index = candidate;
                return;
            }
        }
    }

    fn append_received(self: *Node, entry: Entry) !void {
        const expected = std.math.add(Index, self.last_index(), 1) catch
            return error.RaftIndexOverflow;
        if (entry.index != expected) return error.InvalidRaftEntry;
        if (self.entry_count == self.entries.len) return error.RaftLogFull;
        self.entries[self.entry_count] = entry;
        self.entry_count += 1;
    }

    fn truncate_from(self: *Node, index: Index) void {
        assert(index > self.snapshot.last_index);
        assert(index <= self.last_index());
        self.entry_count = @intCast(index - self.snapshot.last_index - 1);
        self.commit_index = @min(self.commit_index, self.last_index());
        self.applied_index = @min(self.applied_index, self.commit_index);
    }

    fn discard_through(self: *Node, index: Index) void {
        assert(index >= self.snapshot.last_index);
        assert(index <= self.last_index());
        const discard: u16 = @intCast(index - self.snapshot.last_index);
        const remaining = self.entry_count - discard;
        std.mem.copyForwards(Entry, self.entries[0..remaining], self.entries[discard..self.entry_count]);
        self.entry_count = remaining;
    }

    fn entry_at(self: *const Node, index: Index) ?Entry {
        if (index <= self.snapshot.last_index or index > self.last_index()) return null;
        const offset: u16 = @intCast(index - self.snapshot.last_index - 1);
        return self.entries[offset];
    }

    fn term_at(self: *const Node, index: Index) ?Term {
        if (index == 0) return if (self.snapshot.last_index == 0) 0 else null;
        if (index == self.snapshot.last_index) return self.snapshot.last_term;
        return if (self.entry_at(index)) |entry| entry.term else null;
    }

    fn become_follower(self: *Node, term: Term, leader: ?NodeId) void {
        assert(term >= self.hard.term);
        if (term > self.hard.term) {
            self.hard.term = term;
            self.hard.voted_for = null;
            self.hard_state_dirty = true;
        }
        self.role = .follower;
        self.leader_id = leader;
        self.votes = 0;
        self.quorum_acks = 0;
        self.read_barrier_ready = false;
        self.election_elapsed = 0;
        self.reset_election_timeout();
    }

    fn reset_election_timeout(self: *Node) void {
        self.election_timeout = next_election_timeout(
            self.election_timeout_min,
            self.election_timeout_max,
            &self.election_random_state,
        );
    }

    fn member_slot(self: *const Node, id: NodeId) ?u8 {
        for (self.members, 0..) |member, index| {
            if (member == id) return @intCast(index);
        }
        return null;
    }

    fn is_member(self: *const Node, id: NodeId) bool {
        return self.member_slot(id) != null;
    }

    fn quorum(self: *const Node) u8 {
        return @intCast(self.members.len / 2 + 1);
    }
};

fn next_election_timeout(minimum: u16, maximum: u16, state: *u64) u16 {
    assert(minimum >= 2);
    assert(maximum >= minimum);
    if (minimum == maximum) return minimum;
    var value = state.*;
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    state.* = value;
    const random = value *% 0x2545f4914f6cdd1d;
    const span: u64 = @as(u64, maximum) - minimum + 1;
    return minimum + @as(u16, @intCast(random % span));
}

fn member_bit(slot: u8) u8 {
    assert(slot < nodes_max);
    return @as(u8, 1) << @intCast(slot);
}

fn message_sender(message: Message) NodeId {
    return switch (message) {
        .request_vote => |request| request.candidate,
        .vote_result => |result| result.voter,
        .append_entries => |append| append.leader,
        .append_result => |result| result.follower,
        .install_snapshot => |install| install.leader,
        .snapshot_result => |result| result.follower,
    };
}

fn message_term(message: Message) Term {
    return switch (message) {
        .request_vote => |value| value.term,
        .vote_result => |value| value.term,
        .append_entries => |value| value.term,
        .append_result => |value| value.term,
        .install_snapshot => |value| value.term,
        .snapshot_result => |value| value.term,
    };
}

fn checksum(byte: u8) [32]u8 {
    return @splat(byte);
}

const TestCluster = struct {
    members: [5]NodeId = .{ 1, 2, 3, 4, 5 },
    logs: [5][32]Entry = undefined,
    nodes: [5]Node = undefined,
    node_count: u8,
    messages: [256]Envelope = undefined,
    outbox: Outbox = undefined,

    fn init(self: *TestCluster, count: u8) !void {
        assert(count == 3 or count == 5);
        self.node_count = count;
        self.outbox = try Outbox.init(&self.messages);
        const timeouts = [_]u16{ 3, 5, 7, 9, 11 };
        for (self.nodes[0..count], 0..) |*target, index| {
            target.* = try Node.init(.{
                .id = self.members[index],
                .members = self.members[0..count],
                .log_storage = &self.logs[index],
                .election_timeout_ticks = timeouts[index],
                .heartbeat_ticks = 1,
            });
        }
    }

    fn node(self: *TestCluster, id: NodeId) *Node {
        for (self.nodes[0..self.node_count]) |*candidate| {
            if (candidate.id == id) return candidate;
        }
        unreachable;
    }

    fn deliver_all(self: *TestCluster) !void {
        var cursor: u16 = 0;
        while (cursor < self.outbox.count) : (cursor += 1) {
            const envelope = self.outbox.storage[cursor];
            try self.node(envelope.to).receive(envelope, &self.outbox);
        }
        self.outbox.reset();
    }

    fn deliver_if(self: *TestCluster, allowed: *const fn (Envelope) bool) !void {
        var cursor: u16 = 0;
        while (cursor < self.outbox.count) : (cursor += 1) {
            const envelope = self.outbox.storage[cursor];
            if (allowed(envelope)) try self.node(envelope.to).receive(envelope, &self.outbox);
        }
        self.outbox.reset();
    }

    fn elect_first(self: *TestCluster) !void {
        for (0..3) |_| try self.nodes[0].tick(&self.outbox);
        try self.deliver_all();
        try std.testing.expectEqual(Role.leader, self.nodes[0].role);
    }
};

test "three-node election quorum commit duplicate delivery and read barrier" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(3);
    try cluster.elect_first();
    for (cluster.nodes[0..3]) |*node_state| {
        try std.testing.expect(node_state.hard_state_needs_persist());
        node_state.mark_hard_state_persisted();
        try std.testing.expect(!node_state.hard_state_needs_persist());
    }
    try std.testing.expect(cluster.nodes[0].linearizable_read_ready());
    const index = try cluster.nodes[0].propose(checksum(1), &cluster.outbox);
    try std.testing.expectEqual(@as(Index, 1), index);
    const duplicate = cluster.outbox.items()[0];
    try cluster.node(duplicate.to).receive(duplicate, &cluster.outbox);
    try cluster.node(duplicate.to).receive(duplicate, &cluster.outbox);
    try cluster.deliver_all();
    try std.testing.expectEqual(@as(Index, 1), cluster.nodes[0].commit_index);
    try std.testing.expectEqual(@as(u16, 1), cluster.nodes[1].entry_count);
    try cluster.nodes[0].begin_linearizable_read(&cluster.outbox);
    try std.testing.expect(!cluster.nodes[0].linearizable_read_ready());
    try cluster.deliver_all();
    try std.testing.expect(cluster.nodes[0].linearizable_read_ready());
}

test "Raft envelope sender must match the authenticated message principal" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(3);
    try std.testing.expectError(error.InvalidRaftEnvelope, cluster.nodes[0].receive(.{
        .from = 2,
        .to = 1,
        .message = .{ .request_vote = .{
            .term = 1,
            .candidate = 3,
            .last_log_index = 0,
            .last_log_term = 0,
        } },
    }, &cluster.outbox));
    try std.testing.expectEqual(@as(Term, 0), cluster.nodes[0].hard.term);
    try std.testing.expectEqual(@as(u16, 0), cluster.outbox.count);
}

test "stale leader commit cannot move a restarted follower backward" {
    const members = [_]NodeId{ 1, 2, 3 };
    var entries: [4]Entry = undefined;
    entries[0] = .{ .term = 2, .index = 1, .command_checksum = checksum(0x44) };
    var follower = try Node.init(.{
        .id = 2,
        .members = &members,
        .log_storage = &entries,
        .hard_state = .{ .term = 2 },
        .election_timeout_ticks = 5,
        .heartbeat_ticks = 1,
        .restored_entry_count = 1,
        .restored_commit_index = 1,
        .restored_applied_index = 1,
    });
    var messages: [4]Envelope = undefined;
    var outbox = try Outbox.init(&messages);
    try follower.receive(.{
        .from = 1,
        .to = 2,
        .message = .{ .append_entries = .{
            .term = 2,
            .leader = 1,
            .previous_index = 1,
            .previous_term = 2,
            .leader_commit = 0,
            .entry = null,
        } },
    }, &outbox);
    try std.testing.expectEqual(@as(Index, 1), follower.commit_index);
    try std.testing.expectEqual(@as(Index, 1), follower.applied_index);
}

test "new leader commits a prior-term entry persisted before the old leader crash" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(3);
    try cluster.elect_first();
    _ = try cluster.nodes[0].propose(checksum(0xa1), &cluster.outbox);
    const append_to_second = cluster.outbox.items()[0];
    try std.testing.expectEqual(@as(NodeId, 2), append_to_second.to);
    try cluster.nodes[1].receive(append_to_second, &cluster.outbox);
    cluster.outbox.reset(); // Old leader crashes before seeing the durable response.
    try std.testing.expectEqual(@as(Index, 0), cluster.nodes[0].commit_index);
    try std.testing.expectEqual(@as(Index, 1), cluster.nodes[1].last_index());

    for (0..5) |_| try cluster.nodes[1].tick(&cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.from != 1 and envelope.to != 1;
        }
    }.allow);
    try std.testing.expectEqual(Role.leader, cluster.nodes[1].role);
    _ = try cluster.nodes[1].propose(checksum(0xb2), &cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.from != 1 and envelope.to != 1;
        }
    }.allow);
    try std.testing.expectEqual(@as(Index, 2), cluster.nodes[1].commit_index);
    try std.testing.expectEqual(@as(Index, 2), cluster.nodes[2].last_index());
    try std.testing.expectEqual(checksum(0xa1), cluster.nodes[2].entries[0].command_checksum);
}

test "leader crash before follower persistence cannot commit its orphaned append" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(3);
    try cluster.elect_first();
    _ = try cluster.nodes[0].propose(checksum(0xc1), &cluster.outbox);
    cluster.outbox.reset(); // The leader crashes before either follower persists the entry.

    for (0..5) |_| try cluster.nodes[1].tick(&cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.from != 1 and envelope.to != 1;
        }
    }.allow);
    try std.testing.expectEqual(Role.leader, cluster.nodes[1].role);
    _ = try cluster.nodes[1].propose(checksum(0xc2), &cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.from != 1 and envelope.to != 1;
        }
    }.allow);
    try std.testing.expectEqual(@as(Index, 1), cluster.nodes[1].commit_index);
    try std.testing.expectEqual(checksum(0xc2), cluster.nodes[1].entries[0].command_checksum);
    try std.testing.expectEqual(checksum(0xc2), cluster.nodes[2].entries[0].command_checksum);
}

test "delayed prior-term append response cannot revive a fenced leader" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(3);
    try cluster.elect_first();
    _ = try cluster.nodes[0].propose(checksum(0xd1), &cluster.outbox);
    const append_to_second = cluster.outbox.items()[0];
    try cluster.nodes[1].receive(append_to_second, &cluster.outbox);
    const delayed_response = cluster.outbox.items()[2];
    try std.testing.expect(std.meta.activeTag(delayed_response.message) == .append_result);
    cluster.outbox.reset();

    for (0..5) |_| try cluster.nodes[1].tick(&cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.from != 1 and envelope.to != 1;
        }
    }.allow);
    try std.testing.expectEqual(Role.leader, cluster.nodes[1].role);
    try cluster.nodes[0].receive(.{
        .from = 2,
        .to = 1,
        .message = .{ .append_entries = .{
            .term = cluster.nodes[1].hard.term,
            .leader = 2,
            .previous_index = 1,
            .previous_term = cluster.nodes[1].entries[0].term,
            .leader_commit = 0,
            .entry = null,
        } },
    }, &cluster.outbox);
    try std.testing.expectEqual(Role.follower, cluster.nodes[0].role);
    const fenced_term = cluster.nodes[0].hard.term;
    const fenced_commit = cluster.nodes[0].commit_index;
    cluster.outbox.reset();

    try cluster.nodes[0].receive(delayed_response, &cluster.outbox);
    try std.testing.expectEqual(Role.follower, cluster.nodes[0].role);
    try std.testing.expectEqual(fenced_term, cluster.nodes[0].hard.term);
    try std.testing.expectEqual(fenced_commit, cluster.nodes[0].commit_index);
    try std.testing.expectEqual(@as(u16, 0), cluster.outbox.count);
}

test "partition prevents commit and newer-term leader fences the old leader" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(3);
    try cluster.elect_first();
    _ = try cluster.nodes[0].propose(checksum(2), &cluster.outbox);
    cluster.outbox.reset();
    try std.testing.expectEqual(@as(Index, 0), cluster.nodes[0].commit_index);

    for (0..5) |_| try cluster.nodes[1].tick(&cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.from != 1 and envelope.to != 1;
        }
    }.allow);
    try std.testing.expectEqual(Role.leader, cluster.nodes[1].role);
    try std.testing.expect(cluster.nodes[1].hard.term > cluster.nodes[0].hard.term);

    const stale_term = cluster.nodes[0].hard.term;
    cluster.outbox.reset();
    try cluster.nodes[0].tick(&cluster.outbox);
    try cluster.nodes[0].receive(.{
        .from = 2,
        .to = 1,
        .message = .{ .append_entries = .{
            .term = cluster.nodes[1].hard.term,
            .leader = 2,
            .previous_index = 0,
            .previous_term = 0,
            .leader_commit = 0,
            .entry = null,
        } },
    }, &cluster.outbox);
    try std.testing.expectEqual(Role.follower, cluster.nodes[0].role);
    try std.testing.expect(cluster.nodes[0].hard.term > stale_term);
}

test "five-node leader commits only after three durable replicas" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(5);
    try cluster.elect_first();
    _ = try cluster.nodes[0].propose(checksum(3), &cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.to <= 2 and envelope.from <= 2;
        }
    }.allow);
    try std.testing.expectEqual(@as(Index, 0), cluster.nodes[0].commit_index);
    try cluster.nodes[0].tick(&cluster.outbox);
    try cluster.deliver_if(struct {
        fn allow(envelope: Envelope) bool {
            return envelope.to <= 3 and envelope.from <= 3;
        }
    }.allow);
    try std.testing.expectEqual(@as(Index, 1), cluster.nodes[0].commit_index);
}

test "snapshot installation catches up a replaced follower" {
    var cluster: TestCluster = .{ .node_count = 0 };
    try cluster.init(3);
    try cluster.elect_first();
    _ = try cluster.nodes[0].propose(checksum(4), &cluster.outbox);
    try cluster.deliver_all();
    try cluster.nodes[0].mark_applied(1);
    try cluster.nodes[0].compact(.{
        .last_index = 1,
        .last_term = cluster.nodes[0].hard.term,
        .state_size = 123,
        .state_checksum = checksum(9),
    });
    cluster.nodes[2].entry_count = 0;
    cluster.nodes[2].commit_index = 0;
    cluster.nodes[2].applied_index = 0;
    cluster.outbox.reset();
    try cluster.nodes[0].tick(&cluster.outbox);
    try cluster.deliver_all();
    try std.testing.expectEqual(@as(Index, 1), cluster.nodes[2].snapshot.last_index);
    try std.testing.expectEqual(checksum(9), cluster.nodes[2].snapshot.state_checksum);
}
