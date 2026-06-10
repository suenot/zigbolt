const std = @import("std");
const raft_log = @import("raft_log.zig");

pub const NodeState = enum { follower, candidate, leader };

pub const RaftConfig = struct {
    node_id: u32,
    peer_count: u32,
    election_timeout_min_ms: u32 = 150,
    election_timeout_max_ms: u32 = 300,
    heartbeat_interval_ms: u32 = 50,
};

/// Messages exchanged between Raft nodes.
pub const RaftMessage = union(enum) {
    request_vote: RequestVote,
    request_vote_response: RequestVoteResponse,
    append_entries: AppendEntries,
    append_entries_response: AppendEntriesResponse,
};

pub const RequestVote = struct {
    term: u64,
    candidate_id: u32,
    last_log_index: u64,
    last_log_term: u64,
};

pub const RequestVoteResponse = struct {
    term: u64,
    vote_granted: bool,
};

pub const AppendEntries = struct {
    term: u64,
    leader_id: u32,
    prev_log_index: u64,
    prev_log_term: u64,
    entries: []const raft_log.RaftLog.StoredEntry,
    leader_commit: u64,
};

pub const AppendEntriesResponse = struct {
    term: u64,
    success: bool,
    match_index: u64,
};

/// A response to a Raft message: the target node and the message to send.
pub const MessageResponse = struct {
    to: u32,
    msg: RaftMessage,
};

pub const RaftNode = struct {
    config: RaftConfig,
    state: NodeState,
    current_term: u64,
    voted_for: ?u32,
    log: raft_log.RaftLog,
    commit_index: u64,
    last_applied: u64,

    // Peer-id -> slot mapping. `peers[slot]` is the node id of the peer that
    // owns slot `slot` in next_index/match_index/votes_granted. The node's own
    // id is deliberately EXCLUDED: indexing those arrays by raw sender id
    // would alias the self id with a peer slot and drop acks from peers whose
    // id falls outside [0, peer_count).
    peers: []u32,

    // Leader state (allocated arrays, one slot per peer; see `peers`)
    next_index: []u64,
    match_index: []u64,
    // Per-peer granted-vote set for the CURRENT election. A bare counter is
    // unsafe: a duplicated (retransmitted) grant from the same peer would be
    // counted twice and could produce a false majority.
    votes_granted: []bool,
    // Number of votes won in the current election (self + granted set bits).
    votes_received: u32,

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: RaftConfig) !RaftNode {
        const next_idx = try allocator.alloc(u64, config.peer_count);
        errdefer allocator.free(next_idx);
        @memset(next_idx, 1);
        const match_idx = try allocator.alloc(u64, config.peer_count);
        errdefer allocator.free(match_idx);
        @memset(match_idx, 0);

        // Default peer ids: the first `peer_count` ids from 0 upward, skipping
        // our own id (matches the convention of contiguous node ids 0..N-1).
        const peers = try allocator.alloc(u32, config.peer_count);
        errdefer allocator.free(peers);
        {
            var id: u32 = 0;
            var i: usize = 0;
            while (i < peers.len) : (id += 1) {
                if (id != config.node_id) {
                    peers[i] = id;
                    i += 1;
                }
            }
        }

        const votes = try allocator.alloc(bool, config.peer_count);
        errdefer allocator.free(votes);
        @memset(votes, false);

        return .{
            .config = config,
            .state = .follower,
            .current_term = 0,
            .voted_for = null,
            .log = raft_log.RaftLog.init(allocator),
            .commit_index = 0,
            .last_applied = 0,
            .peers = peers,
            .next_index = next_idx,
            .match_index = match_idx,
            .votes_granted = votes,
            .votes_received = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RaftNode) void {
        self.allocator.free(self.peers);
        self.allocator.free(self.next_index);
        self.allocator.free(self.match_index);
        self.allocator.free(self.votes_granted);
        self.log.deinit();
    }

    /// Handle an incoming Raft message from `from`. Returns an optional response.
    pub fn handleMessage(self: *RaftNode, from: u32, msg: RaftMessage) ?MessageResponse {
        switch (msg) {
            .request_vote => |rv| return self.handleRequestVote(from, rv),
            .request_vote_response => |rvr| {
                self.handleRequestVoteResponse(from, rvr);
                return null;
            },
            .append_entries => |ae| return self.handleAppendEntries(from, ae),
            .append_entries_response => |aer| {
                self.handleAppendEntriesResponse(from, aer);
                return null;
            },
        }
    }

    /// Called on election timeout. Transitions to candidate and returns a RequestVote message.
    pub fn startElection(self: *RaftNode) RaftMessage {
        self.becomeCandidate();
        // A single-node cluster (peer_count == 0, quorum of 1) wins the
        // election with just its own vote; no responses will ever arrive.
        if (self.hasVoteMajority()) {
            self.becomeLeader();
        }
        return .{ .request_vote = .{
            .term = self.current_term,
            .candidate_id = self.config.node_id,
            .last_log_index = self.log.lastIndex(),
            .last_log_term = self.log.lastTerm(),
        } };
    }

    /// Leader: propose a new log entry. Returns the log index.
    pub fn propose(self: *RaftNode, data: []const u8) !u64 {
        if (self.state != .leader) return error.NotLeader;
        const index = try self.log.append(self.current_term, data);
        // In a single-node cluster (quorum of 1) the entry commits at once;
        // with peers this recomputes from unchanged match_index (a no-op).
        self.updateCommitIndex();
        return index;
    }

    /// Leader: create an AppendEntries message for a specific peer.
    /// Returns null if `peer_id` is not a known peer (e.g. our own id).
    pub fn createAppendEntries(self: *RaftNode, peer_id: u32) ?AppendEntries {
        const slot = self.peerSlot(peer_id) orelse return null;
        return self.buildAppendEntries(slot, true);
    }

    /// Leader: create a heartbeat for a specific peer. A heartbeat is just an
    /// AppendEntries with no entries, built from that peer's next_index so a
    /// lagging follower still passes (or meaningfully fails) the consistency
    /// check. Returns null if `peer_id` is not a known peer.
    pub fn createHeartbeat(self: *RaftNode, peer_id: u32) ?AppendEntries {
        const slot = self.peerSlot(peer_id) orelse return null;
        return self.buildAppendEntries(slot, false);
    }

    fn buildAppendEntries(self: *RaftNode, slot: usize, include_entries: bool) AppendEntries {
        const ni = self.next_index[slot];
        const prev_log_index = if (ni > 0) ni - 1 else 0;
        const prev_log_term = if (prev_log_index > 0)
            if (self.log.getEntry(prev_log_index)) |e| e.term else 0
        else
            0;

        return .{
            .term = self.current_term,
            .leader_id = self.config.node_id,
            .prev_log_index = prev_log_index,
            .prev_log_term = prev_log_term,
            .entries = if (include_entries) self.log.entriesFrom(ni) else &.{},
            .leader_commit = self.commit_index,
        };
    }

    /// Return entries that are committed but not yet applied: exactly the
    /// range (last_applied, commit_index]. Never returns uncommitted entries
    /// (they may still be truncated by a future leader); applying them would
    /// be a phantom execution, and re-applying them later a double execution.
    pub fn getApplicableEntries(self: *RaftNode) []const raft_log.RaftLog.StoredEntry {
        if (self.commit_index <= self.last_applied) return &.{};
        const tail = self.log.entriesFrom(self.last_applied + 1);
        const want: u64 = self.commit_index - self.last_applied;
        const n: usize = @intCast(@min(want, @as(u64, tail.len)));
        return tail[0..n];
    }

    /// Mark entries as applied up to the given index.
    pub fn markApplied(self: *RaftNode, up_to: u64) void {
        if (up_to > self.last_applied) {
            self.last_applied = up_to;
        }
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Map a peer node id to its slot in next_index/match_index/votes_granted.
    /// Returns null for our own id or any unknown id.
    fn peerSlot(self: *const RaftNode, peer_id: u32) ?usize {
        for (self.peers, 0..) |p, i| {
            if (p == peer_id) return i;
        }
        return null;
    }

    /// True if the granted-vote set (plus our self vote) is a majority of the
    /// full cluster (peer_count peers + self).
    fn hasVoteMajority(self: *const RaftNode) bool {
        var count: u32 = 1; // self vote
        for (self.votes_granted) |granted| {
            if (granted) count += 1;
        }
        return count * 2 > self.config.peer_count + 1;
    }

    fn becomeFollower(self: *RaftNode, term: u64) void {
        self.state = .follower;
        self.current_term = term;
        self.voted_for = null;
        self.votes_received = 0;
        @memset(self.votes_granted, false);
    }

    fn becomeCandidate(self: *RaftNode) void {
        self.current_term += 1;
        self.state = .candidate;
        self.voted_for = self.config.node_id;
        self.votes_received = 1; // vote for self
        @memset(self.votes_granted, false);
    }

    fn becomeLeader(self: *RaftNode) void {
        self.state = .leader;
        self.votes_received = 0;
        @memset(self.votes_granted, false);
        // Reinitialize next_index and match_index
        const last = self.log.lastIndex() + 1;
        @memset(self.next_index, last);
        @memset(self.match_index, 0);
    }

    pub fn updateCommitIndex(self: *RaftNode) void {
        // Find the largest N such that a majority of match_index[i] >= N
        // and log[N].term == currentTerm.
        const last = self.log.lastIndex();
        if (last == 0) return;

        var n = last;
        while (n > self.commit_index) : (n -= 1) {
            const entry = self.log.getEntry(n) orelse continue;
            if (entry.term != self.current_term) continue;

            // Count how many peers have replicated this entry
            var count: u32 = 1; // count self
            for (self.match_index) |mi| {
                if (mi >= n) count += 1;
            }
            // Majority: count > (peer_count + 1) / 2
            if (count * 2 > self.config.peer_count + 1) {
                self.commit_index = n;
                return;
            }
        }
    }

    fn handleRequestVote(self: *RaftNode, from: u32, rv: RequestVote) MessageResponse {
        // If the candidate's term is higher, step down
        if (rv.term > self.current_term) {
            self.becomeFollower(rv.term);
        }

        var granted = false;
        if (rv.term >= self.current_term) {
            const can_vote = (self.voted_for == null or self.voted_for.? == rv.candidate_id);
            const log_ok = (rv.last_log_term > self.log.lastTerm()) or
                (rv.last_log_term == self.log.lastTerm() and rv.last_log_index >= self.log.lastIndex());
            if (can_vote and log_ok) {
                self.voted_for = rv.candidate_id;
                granted = true;
            }
        }

        return .{
            .to = from,
            .msg = .{ .request_vote_response = .{
                .term = self.current_term,
                .vote_granted = granted,
            } },
        };
    }

    fn handleRequestVoteResponse(self: *RaftNode, from: u32, rvr: RequestVoteResponse) void {
        if (rvr.term > self.current_term) {
            self.becomeFollower(rvr.term);
            return;
        }
        // A delayed grant from a PREVIOUS election (rvr.term < current_term)
        // must not count toward the current one.
        if (rvr.term != self.current_term) return;
        if (self.state != .candidate) return;
        if (!rvr.vote_granted) return;

        const slot = self.peerSlot(from) orelse return;
        // Duplicate (e.g. retransmitted) grant from the same peer: count once.
        if (self.votes_granted[slot]) return;
        self.votes_granted[slot] = true;
        self.votes_received += 1;

        if (self.hasVoteMajority()) {
            self.becomeLeader();
        }
    }

    /// Build a failed AppendEntriesResponse. On failure, `match_index` is a
    /// backtracking HINT: the highest log index the follower suggests the
    /// leader use as prev next time (0 = no hint, start from scratch).
    fn appendFailure(self: *const RaftNode, to: u32, hint: u64) MessageResponse {
        return .{
            .to = to,
            .msg = .{ .append_entries_response = .{
                .term = self.current_term,
                .success = false,
                .match_index = hint,
            } },
        };
    }

    fn handleAppendEntries(self: *RaftNode, from: u32, ae: AppendEntries) MessageResponse {
        if (ae.term > self.current_term) {
            self.becomeFollower(ae.term);
        } else if (ae.term == self.current_term and self.state == .candidate) {
            // Another leader exists for this term; step down
            self.becomeFollower(ae.term);
        }

        // Reject if term < currentTerm
        if (ae.term < self.current_term) {
            return self.appendFailure(from, 0);
        }

        // Election Safety: there is at most one leader per term. If we are
        // the leader for this term, a same-term AppendEntries is a protocol
        // invariant violation — reject it without touching our log.
        if (self.state == .leader) {
            return self.appendFailure(from, 0);
        }

        // Check prev_log_index consistency
        if (ae.prev_log_index > 0) {
            const prev_entry = self.log.getEntry(ae.prev_log_index);
            if (prev_entry == null) {
                // Our log is too short: hint our last index so the leader
                // can jump next_index back in one round trip.
                return self.appendFailure(from, self.log.lastIndex());
            }
            if (prev_entry.?.term != ae.prev_log_term) {
                // Term conflict at prev: hint one entry earlier.
                return self.appendFailure(from, ae.prev_log_index - 1);
            }
        }

        // Append new entries (handle conflicts)
        for (ae.entries) |entry| {
            const existing = self.log.getEntry(entry.index);
            if (existing) |ex| {
                if (ex.term != entry.term) {
                    // State Machine Safety: never truncate at or below
                    // commit_index — committed entries may already have been
                    // applied. A conflict here means the sender is broken.
                    if (entry.index <= self.commit_index) {
                        return self.appendFailure(from, 0);
                    }
                    self.log.truncateFrom(entry.index);
                    _ = self.log.append(entry.term, entry.data) catch {
                        return self.appendFailure(from, self.log.lastIndex());
                    };
                }
                // If terms match, entry is already present; skip
            } else {
                _ = self.log.append(entry.term, entry.data) catch {
                    return self.appendFailure(from, self.log.lastIndex());
                };
            }
        }

        // Index of the last entry this RPC actually vouches for. Our own log
        // may be longer, with a stale uncommitted tail from an old leader;
        // neither commit nor match_index may cover that tail.
        const last_new_index = ae.prev_log_index + @as(u64, ae.entries.len);

        // Update commit index, capped at the last entry covered by this RPC
        // (Raft paper: min(leaderCommit, index of last new entry)).
        if (ae.leader_commit > self.commit_index) {
            const bound = @min(ae.leader_commit, last_new_index);
            if (bound > self.commit_index) {
                self.commit_index = bound;
            }
        }

        return .{
            .to = from,
            .msg = .{ .append_entries_response = .{
                .term = self.current_term,
                .success = true,
                .match_index = last_new_index,
            } },
        };
    }

    fn handleAppendEntriesResponse(self: *RaftNode, from: u32, aer: AppendEntriesResponse) void {
        if (aer.term > self.current_term) {
            self.becomeFollower(aer.term);
            return;
        }
        if (self.state != .leader) return;
        // Drop stale responses (e.g. from an earlier leadership stint of this
        // node, or delayed/duplicated packets): the follower's log may have
        // been truncated since, so its old ack proves nothing about the
        // current log. Counting it could "commit" an entry without a real
        // majority, which would then be lost on failover.
        if (aer.term != self.current_term) return;

        const slot = self.peerSlot(from) orelse return;

        if (aer.success) {
            // Monotonic: duplicated or reordered acks must never move
            // match_index backwards.
            if (aer.match_index > self.match_index[slot]) {
                self.match_index[slot] = aer.match_index;
            }
            const ni = self.match_index[slot] + 1;
            if (ni > self.next_index[slot]) {
                self.next_index[slot] = ni;
            }
            self.updateCommitIndex();
        } else {
            // Back off next_index using the follower's hint. min() makes a
            // duplicated failure idempotent (same hint -> same target), and
            // the floor keeps next_index above match_index (>= 1): entries up
            // to match_index are known replicated this term.
            const floor_ni = self.match_index[slot] + 1;
            var target = @min(self.next_index[slot], aer.match_index + 1);
            if (target < floor_ni) target = floor_ni;
            self.next_index[slot] = target;
        }
    }
};

// =============================================================================
// Tests
// =============================================================================

test "RaftNode: initial state is follower" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    try std.testing.expectEqual(NodeState.follower, node.state);
    try std.testing.expectEqual(@as(u64, 0), node.current_term);
    try std.testing.expect(node.voted_for == null);
}

test "RaftNode: startElection transitions to candidate" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 1,
        .peer_count = 2,
    });
    defer node.deinit();

    const msg = node.startElection();
    try std.testing.expectEqual(NodeState.candidate, node.state);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);
    try std.testing.expectEqual(@as(?u32, 1), node.voted_for);
    try std.testing.expectEqual(@as(u32, 1), node.votes_received);

    switch (msg) {
        .request_vote => |rv| {
            try std.testing.expectEqual(@as(u64, 1), rv.term);
            try std.testing.expectEqual(@as(u32, 1), rv.candidate_id);
        },
        else => return error.UnexpectedMessage,
    }
}

test "RaftNode: vote granting" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    const result = node.handleMessage(1, .{ .request_vote = .{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    } });

    try std.testing.expect(result != null);
    const resp = result.?;
    try std.testing.expectEqual(@as(u32, 1), resp.to);
    switch (resp.msg) {
        .request_vote_response => |rvr| {
            try std.testing.expect(rvr.vote_granted);
            try std.testing.expectEqual(@as(u64, 1), rvr.term);
        },
        else => return error.UnexpectedMessage,
    }
    try std.testing.expectEqual(@as(?u32, 1), node.voted_for);
}

test "RaftNode: vote denied when already voted for another" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 3,
    });
    defer node.deinit();

    _ = node.handleMessage(1, .{ .request_vote = .{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    } });

    const result = node.handleMessage(2, .{ .request_vote = .{
        .term = 1,
        .candidate_id = 2,
        .last_log_index = 0,
        .last_log_term = 0,
    } });

    try std.testing.expect(result != null);
    switch (result.?.msg) {
        .request_vote_response => |rvr| {
            try std.testing.expect(!rvr.vote_granted);
        },
        else => return error.UnexpectedMessage,
    }
}

test "RaftNode: leader append entries (propose)" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    _ = node.startElection();
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    try std.testing.expectEqual(NodeState.leader, node.state);

    const idx = try node.propose("hello");
    try std.testing.expectEqual(@as(u64, 1), idx);

    const entry = node.log.getEntry(1).?;
    try std.testing.expectEqualStrings("hello", entry.data);
    try std.testing.expectEqual(@as(u64, 1), entry.term);
}

test "RaftNode: propose fails when not leader" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    const result = node.propose("data");
    try std.testing.expectError(error.NotLeader, result);
}

test "RaftNode: term advancement on higher term message" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    const result = node.handleMessage(1, .{ .append_entries = .{
        .term = 5,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    } });

    try std.testing.expectEqual(@as(u64, 5), node.current_term);
    try std.testing.expectEqual(NodeState.follower, node.state);
    try std.testing.expect(result != null);
    switch (result.?.msg) {
        .append_entries_response => |aer| {
            try std.testing.expect(aer.success);
            try std.testing.expectEqual(@as(u64, 5), aer.term);
        },
        else => return error.UnexpectedMessage,
    }
}

test "RaftNode: candidate steps down on AppendEntries from leader" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    _ = node.startElection();
    try std.testing.expectEqual(NodeState.candidate, node.state);

    _ = node.handleMessage(1, .{ .append_entries = .{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    } });

    try std.testing.expectEqual(NodeState.follower, node.state);
}

test "RaftNode: heartbeat creation" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    _ = node.startElection();
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });

    const hb = node.createHeartbeat(1).?;
    try std.testing.expectEqual(@as(u64, 1), hb.term);
    try std.testing.expectEqual(@as(u32, 0), hb.leader_id);
    try std.testing.expectEqual(@as(usize, 0), hb.entries.len);

    // Unknown peer (including our own id) has no slot
    try std.testing.expect(node.createHeartbeat(0) == null);
    try std.testing.expect(node.createAppendEntries(7) == null);
}

test "RaftNode: getApplicableEntries and markApplied" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    _ = node.startElection();
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });

    _ = try node.propose("entry1");
    _ = try node.propose("entry2");

    const empty = node.getApplicableEntries();
    try std.testing.expectEqual(@as(usize, 0), empty.len);

    node.match_index[0] = 2;
    node.match_index[1] = 2;
    node.updateCommitIndex();
    try std.testing.expectEqual(@as(u64, 2), node.commit_index);

    const applicable = node.getApplicableEntries();
    try std.testing.expectEqual(@as(usize, 2), applicable.len);

    node.markApplied(2);
    try std.testing.expectEqual(@as(u64, 2), node.last_applied);

    const none_left = node.getApplicableEntries();
    try std.testing.expectEqual(@as(usize, 0), none_left.len);
}

// =============================================================================
// Deterministic multi-node simulation harness (tests only).
// Nodes are plain in-memory RaftNodes indexed by node id; RPCs are delivered
// synchronously by calling handleMessage and forwarding any response until
// the exchange goes quiet. No sockets, no disk, no timers.
// =============================================================================

fn testExchange(nodes: []RaftNode, from: u32, to: u32, msg: RaftMessage) void {
    var cur_from = from;
    var cur_to = to;
    var cur_msg = msg;
    while (true) {
        const resp = nodes[cur_to].handleMessage(cur_from, cur_msg) orelse return;
        cur_from = cur_to;
        cur_to = resp.to;
        cur_msg = resp.msg;
    }
}

fn testElect(nodes: []RaftNode, candidate: u32) void {
    const rv = nodes[candidate].startElection();
    for (nodes, 0..) |_, i| {
        const id: u32 = @intCast(i);
        if (id == candidate) continue;
        testExchange(nodes, candidate, id, rv);
    }
}

fn testReplicate(nodes: []RaftNode, leader: u32, peer: u32) void {
    const ae = nodes[leader].createAppendEntries(peer) orelse return;
    testExchange(nodes, leader, peer, .{ .append_entries = ae });
}

fn testHeartbeat(nodes: []RaftNode, leader: u32, peer: u32) void {
    const hb = nodes[leader].createHeartbeat(peer) orelse return;
    testExchange(nodes, leader, peer, .{ .append_entries = hb });
}

test "RaftNode: single-node cluster elects itself leader and commits at once" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 0,
    });
    defer node.deinit();

    _ = node.startElection();
    try std.testing.expectEqual(NodeState.leader, node.state);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);

    // With a quorum of 1, a proposal is committed immediately.
    const idx = try node.propose("solo");
    try std.testing.expectEqual(@as(u64, 1), idx);
    try std.testing.expectEqual(@as(u64, 1), node.commit_index);

    const applicable = node.getApplicableEntries();
    try std.testing.expectEqual(@as(usize, 1), applicable.len);
}

test "RaftNode: duplicate vote grant from same peer is counted once" {
    // 5-node cluster (self + 4 peers): majority is 3 distinct votes.
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 4,
    });
    defer node.deinit();

    _ = node.startElection(); // term 1, self vote = 1
    const grant = RaftMessage{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } };

    // Three copies of the SAME grant (UDP retransmission) must count once.
    _ = node.handleMessage(1, grant);
    _ = node.handleMessage(1, grant);
    _ = node.handleMessage(1, grant);
    try std.testing.expectEqual(NodeState.candidate, node.state);
    try std.testing.expectEqual(@as(u32, 2), node.votes_received);

    // A second DISTINCT peer completes the majority.
    _ = node.handleMessage(2, grant);
    try std.testing.expectEqual(NodeState.leader, node.state);
}

test "RaftNode: vote grant from a previous election term is ignored" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    _ = node.startElection(); // term 1 (responses lost)
    _ = node.startElection(); // term 2

    // Delayed grant from the term-1 election arrives now: must not count.
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    try std.testing.expectEqual(NodeState.candidate, node.state);
    try std.testing.expectEqual(@as(u32, 1), node.votes_received);

    // A current-term grant wins the election.
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 2,
        .vote_granted = true,
    } });
    try std.testing.expectEqual(NodeState.leader, node.state);
}

test "RaftNode: getApplicableEntries is capped at commit_index" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    _ = node.startElection();
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    try std.testing.expectEqual(NodeState.leader, node.state);

    _ = try node.propose("1");
    _ = try node.propose("2");
    _ = try node.propose("3");
    _ = try node.propose("4");
    _ = try node.propose("5");

    node.match_index[0] = 2;
    node.match_index[1] = 2;
    node.updateCommitIndex();
    try std.testing.expectEqual(@as(u64, 2), node.commit_index);

    // log = [1..5], commit = 2: ONLY entries 1 and 2 are applicable.
    const applicable = node.getApplicableEntries();
    try std.testing.expectEqual(@as(usize, 2), applicable.len);
    try std.testing.expectEqual(@as(u64, 1), applicable[0].index);
    try std.testing.expectEqual(@as(u64, 2), applicable[1].index);

    node.markApplied(2);
    try std.testing.expectEqual(@as(usize, 0), node.getApplicableEntries().len);

    // Commit advances to 5: exactly 3, 4, 5 remain applicable (no re-apply).
    node.match_index[0] = 5;
    node.match_index[1] = 5;
    node.updateCommitIndex();
    const rest = node.getApplicableEntries();
    try std.testing.expectEqual(@as(usize, 3), rest.len);
    try std.testing.expectEqual(@as(u64, 3), rest[0].index);
    try std.testing.expectEqual(@as(u64, 5), rest[2].index);
}

test "RaftNode: stale AppendEntries response is dropped and match_index is monotonic" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    // Leadership stint #1 at term 1.
    _ = node.startElection();
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    _ = try node.propose("a");
    _ = try node.propose("b");
    _ = try node.propose("c");

    _ = node.handleMessage(1, .{ .append_entries_response = .{
        .term = 1,
        .success = true,
        .match_index = 3,
    } });
    try std.testing.expectEqual(@as(u64, 3), node.match_index[0]);
    try std.testing.expectEqual(@as(u64, 3), node.commit_index);

    // Step down (higher term seen), then leadership stint #2 at term 4.
    _ = node.handleMessage(2, .{ .request_vote_response = .{
        .term = 3,
        .vote_granted = false,
    } });
    try std.testing.expectEqual(NodeState.follower, node.state);
    _ = node.startElection(); // term 4
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 4,
        .vote_granted = true,
    } });
    try std.testing.expectEqual(NodeState.leader, node.state);
    try std.testing.expectEqual(@as(u64, 0), node.match_index[1]);

    // A STALE success from the term-1 stint arrives from peer 2. Its log may
    // have been truncated since; counting it could fake a majority.
    _ = node.handleMessage(2, .{ .append_entries_response = .{
        .term = 1,
        .success = true,
        .match_index = 3,
    } });
    try std.testing.expectEqual(@as(u64, 0), node.match_index[1]);
    try std.testing.expectEqual(@as(u64, 3), node.commit_index);

    // Fresh current-term ack is accepted...
    _ = node.handleMessage(2, .{ .append_entries_response = .{
        .term = 4,
        .success = true,
        .match_index = 3,
    } });
    try std.testing.expectEqual(@as(u64, 3), node.match_index[1]);
    try std.testing.expectEqual(@as(u64, 4), node.next_index[1]);

    // ...and a reordered OLDER ack never moves match_index backwards.
    _ = node.handleMessage(2, .{ .append_entries_response = .{
        .term = 4,
        .success = true,
        .match_index = 1,
    } });
    try std.testing.expectEqual(@as(u64, 3), node.match_index[1]);
    try std.testing.expectEqual(@as(u64, 4), node.next_index[1]);
}

test "RaftNode: leader only commits current-term entries by counting (Figure 8)" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    // Term 1: propose two entries, no acks arrive.
    _ = node.startElection();
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    _ = try node.propose("x");
    _ = try node.propose("y");
    try std.testing.expectEqual(@as(u64, 0), node.commit_index);

    // Step down, get re-elected at term 3.
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 2,
        .vote_granted = false,
    } });
    _ = node.startElection(); // term 3
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 3,
        .vote_granted = true,
    } });
    try std.testing.expectEqual(NodeState.leader, node.state);

    // Peer acks the OLD-term entries: majority replication alone must NOT
    // commit them (Raft Figure 8).
    _ = node.handleMessage(1, .{ .append_entries_response = .{
        .term = 3,
        .success = true,
        .match_index = 2,
    } });
    try std.testing.expectEqual(@as(u64, 0), node.commit_index);

    // Replicating a CURRENT-term entry commits it and, transitively,
    // everything before it.
    _ = try node.propose("z"); // index 3, term 3
    _ = node.handleMessage(1, .{ .append_entries_response = .{
        .term = 3,
        .success = true,
        .match_index = 3,
    } });
    try std.testing.expectEqual(@as(u64, 3), node.commit_index);
}

test "RaftNode: three-node cluster replicates and commits with a real majority" {
    var nodes: [3]RaftNode = undefined;
    for (&nodes, 0..) |*n, i| {
        n.* = try RaftNode.init(std.testing.allocator, .{
            .node_id = @intCast(i),
            .peer_count = 2,
        });
    }
    defer for (&nodes) |*n| n.deinit();

    testElect(&nodes, 0);
    try std.testing.expectEqual(NodeState.leader, nodes[0].state);
    try std.testing.expectEqual(NodeState.follower, nodes[1].state);
    try std.testing.expectEqual(NodeState.follower, nodes[2].state);

    _ = try nodes[0].propose("a");
    _ = try nodes[0].propose("b");
    _ = try nodes[0].propose("c");
    try std.testing.expectEqual(@as(u64, 0), nodes[0].commit_index);

    // Replicating to ONE follower (self + 1 = 2 of 3) is a real majority.
    testReplicate(&nodes, 0, 1);
    try std.testing.expectEqual(@as(u64, 3), nodes[1].log.lastIndex());
    try std.testing.expectEqual(@as(u64, 3), nodes[0].commit_index);
    try std.testing.expectEqual(@as(u64, 0), nodes[2].log.lastIndex());

    // Peer 2's ack lands in its own slot (peers of node 0 are [1, 2]); with
    // raw id indexing (`from < peer_count`) it would have been dropped.
    testReplicate(&nodes, 0, 2);
    try std.testing.expectEqual(@as(u64, 3), nodes[0].match_index[1]);
    try std.testing.expectEqual(@as(u64, 3), nodes[2].log.lastIndex());
    try std.testing.expectEqual(@as(u64, 3), nodes[2].commit_index);
}

test "RaftNode: heartbeat uses per-peer prev and repairs a lagging follower" {
    var nodes: [3]RaftNode = undefined;
    for (&nodes, 0..) |*n, i| {
        n.* = try RaftNode.init(std.testing.allocator, .{
            .node_id = @intCast(i),
            .peer_count = 2,
        });
    }
    defer for (&nodes) |*n| n.deinit();

    // Term 1: node 0 leads, replicates 3 entries to node 1 only.
    testElect(&nodes, 0);
    _ = try nodes[0].propose("a");
    _ = try nodes[0].propose("b");
    _ = try nodes[0].propose("c");
    testReplicate(&nodes, 0, 1);

    // Term 2: node 1 (full log) takes over; node 2 is still empty.
    testElect(&nodes, 1);
    try std.testing.expectEqual(NodeState.leader, nodes[1].state);
    _ = try nodes[1].propose("d"); // index 4, term 2

    // Heartbeat to node 2 must use next_index[peer]-1 (= 3), NOT the
    // leader's own lastIndex (= 4) blindly for every peer.
    const hb = nodes[1].createHeartbeat(2).?;
    try std.testing.expectEqual(@as(u64, 3), hb.prev_log_index);
    try std.testing.expectEqual(@as(usize, 0), hb.entries.len);

    // The lagging follower fails the consistency check with a hint, and the
    // leader's next_index for it (peers of node 1 are [0, 2] -> slot 1)
    // drops to 1 in a single round trip.
    testHeartbeat(&nodes, 1, 2);
    try std.testing.expectEqual(@as(u64, 1), nodes[1].next_index[1]);
    try std.testing.expectEqual(@as(u64, 0), nodes[1].commit_index);

    // The next regular AppendEntries fully catches the follower up, and the
    // current-term entry commits with a real majority (leader + node 2).
    testReplicate(&nodes, 1, 2);
    try std.testing.expectEqual(@as(u64, 4), nodes[2].log.lastIndex());
    try std.testing.expectEqual(@as(u64, 4), nodes[1].match_index[1]);
    try std.testing.expectEqual(@as(u64, 4), nodes[1].commit_index);

    // The follower learns the new commit index on the next heartbeat.
    testHeartbeat(&nodes, 1, 2);
    try std.testing.expectEqual(@as(u64, 4), nodes[2].commit_index);
}

test "RaftNode: conflicting AppendEntries at or below commit_index is rejected" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 2,
        .peer_count = 2,
    });
    defer node.deinit();

    var data_a = "a".*;
    var data_b = "b".*;
    var data_x = "x".*;

    const e1 = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 1, .data = &data_a };
    const e2 = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 2, .data = &data_b };

    const ok = node.handleMessage(0, .{ .append_entries = .{
        .term = 1,
        .leader_id = 0,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{ e1, e2 },
        .leader_commit = 2,
    } });
    switch (ok.?.msg) {
        .append_entries_response => |aer| try std.testing.expect(aer.success),
        else => return error.UnexpectedMessage,
    }
    try std.testing.expectEqual(@as(u64, 2), node.commit_index);

    // A conflicting entry AT the commit index must be rejected, not truncated.
    const e2_conflict = raft_log.RaftLog.StoredEntry{ .term = 2, .index = 2, .data = &data_x };
    const bad = node.handleMessage(0, .{ .append_entries = .{
        .term = 2,
        .leader_id = 0,
        .prev_log_index = 1,
        .prev_log_term = 1,
        .entries = &.{e2_conflict},
        .leader_commit = 0,
    } });
    switch (bad.?.msg) {
        .append_entries_response => |aer| try std.testing.expect(!aer.success),
        else => return error.UnexpectedMessage,
    }

    // Committed entry survives untouched.
    try std.testing.expectEqual(@as(u64, 2), node.log.lastIndex());
    const kept = node.log.getEntry(2).?;
    try std.testing.expectEqual(@as(u64, 1), kept.term);
    try std.testing.expectEqualStrings("b", kept.data);
    try std.testing.expectEqual(@as(u64, 2), node.commit_index);
}

test "RaftNode: leader rejects same-term AppendEntries from a rival" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    _ = node.startElection();
    _ = node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    try std.testing.expectEqual(NodeState.leader, node.state);
    _ = try node.propose("a");

    var data_z = "z".*;
    const rival = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 2, .data = &data_z };
    const resp = node.handleMessage(1, .{ .append_entries = .{
        .term = 1,
        .leader_id = 1,
        .prev_log_index = 1,
        .prev_log_term = 1,
        .entries = &.{rival},
        .leader_commit = 0,
    } });

    switch (resp.?.msg) {
        .append_entries_response => |aer| try std.testing.expect(!aer.success),
        else => return error.UnexpectedMessage,
    }
    // Still leader, and the rival's entry was NOT appended.
    try std.testing.expectEqual(NodeState.leader, node.state);
    try std.testing.expectEqual(@as(u64, 1), node.log.lastIndex());
}

test "RaftNode: follower never commits a stale tail beyond entries covered by the RPC" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 2,
        .peer_count = 2,
    });
    defer node.deinit();

    var data_a = "a".*;
    var data_b = "b".*;
    var data_c = "c".*;
    const e1 = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 1, .data = &data_a };
    const e2 = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 2, .data = &data_b };
    const e3 = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 3, .data = &data_c };

    // Old leader (term 1) replicated 3 entries, none committed.
    _ = node.handleMessage(0, .{ .append_entries = .{
        .term = 1,
        .leader_id = 0,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{ e1, e2, e3 },
        .leader_commit = 0,
    } });
    try std.testing.expectEqual(@as(u64, 3), node.log.lastIndex());

    // New leader (term 2) only vouches for entry 1 (heartbeat, prev = 1).
    // Entries 2..3 here may differ from the new leader's log; only commit
    // up to min(leader_commit, prev + entries.len) = 1.
    const resp = node.handleMessage(1, .{ .append_entries = .{
        .term = 2,
        .leader_id = 1,
        .prev_log_index = 1,
        .prev_log_term = 1,
        .entries = &.{},
        .leader_commit = 3,
    } });
    switch (resp.?.msg) {
        .append_entries_response => |aer| {
            try std.testing.expect(aer.success);
            // The ack also only vouches for the verified prefix, never the
            // raw lastIndex of a possibly-stale tail.
            try std.testing.expectEqual(@as(u64, 1), aer.match_index);
        },
        else => return error.UnexpectedMessage,
    }
    try std.testing.expectEqual(@as(u64, 1), node.commit_index);
}
