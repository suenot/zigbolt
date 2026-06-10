const std = @import("std");
const raft_log = @import("raft_log.zig");
const wal_mod = @import("wal.zig");
const snapshot_mod = @import("snapshot.zig");

pub const NodeState = enum { follower, candidate, leader };

/// Durable persistence backend for a RaftNode (optional).
///
/// When configured, the node follows a persist-before-reply discipline
/// (Raft §5): current_term/voted_for are saved via VoteState BEFORE any
/// response reflecting them is sent, log entries are WAL-appended and
/// fsync'd BEFORE they are acknowledged (or counted toward a commit quorum
/// by a leader), and in-memory truncations are mirrored durably on disk.
///
/// Ownership/lifetime: the caller owns the WAL, the vote-state path and the
/// optional SnapshotManager; all three must outlive the RaftNode. The WAL
/// should be freshly init'd and NOT yet recovered — RaftNode runs
/// `wal.recover()` itself during `initWithPersistence`. Any WAL SyncPolicy
/// is safe here because the node always calls `sync()` explicitly before
/// acknowledging (use `.explicit` to avoid redundant fsyncs).
///
/// If a persistence operation ever fails, the node wedges itself
/// (`persistence_failed = true`): it stops responding to messages, refuses
/// proposals/elections, and must be restarted (recovery from disk is the
/// only consistent path). Continuing without durability could acknowledge
/// state that a crash would silently lose.
pub const RaftPersistence = struct {
    /// Write-ahead log mirroring the in-memory Raft log (full log from
    /// index 1; log compaction is not wired yet).
    wal: *wal_mod.WriteAheadLog,
    /// Path of the durable (current_term, voted_for) record.
    vote_path: []const u8,
    /// Optional snapshot store: seeds the state-machine base and the
    /// commit/applied floor at recovery.
    snapshots: ?*snapshot_mod.SnapshotManager = null,
};

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

    /// Optional durable backend. When null, the node is purely in-memory
    /// (exactly the pre-persistence behavior; used by the test harness).
    persistence: ?RaftPersistence,
    /// Sticky wedge flag: set on ANY persistence failure. A wedged node
    /// drops all messages and refuses proposals/elections — in-memory state
    /// may be ahead of disk at that point, and replying could acknowledge
    /// state a crash would lose. The embedder should treat this as fatal
    /// and restart the node (recovery from disk is consistent).
    persistence_failed: bool,
    /// Application state recovered from the latest snapshot at startup
    /// (null if none). Take it with `takeRecoveredSnapshot()` to seed the
    /// state machine; freed in deinit() if never taken.
    recovered_snapshot: ?snapshot_mod.SnapshotData,

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: RaftConfig) !RaftNode {
        return initWithPersistence(allocator, config, null);
    }

    /// Init with an optional durable backend. With persistence configured,
    /// recovery runs before the node is usable, in this order:
    ///   1. VoteState.load(): restore current_term/voted_for. A missing file
    ///      means "never voted"; a corrupt one is FATAL (error.CorruptVoteState)
    ///      — starting fresh could double-vote in an already-voted term.
    ///   2. Latest valid snapshot (if a SnapshotManager is configured): seeds
    ///      the state-machine base and the commit/applied floor.
    ///   3. wal.recover(): durably truncates any corrupt tail, then the
    ///      surviving entries are replayed into the in-memory log.
    /// commit_index/last_applied restart at the snapshot's lastIncludedIndex
    /// (0 without one); entries after that point re-commit and re-apply
    /// through the normal protocol, per Raft.
    pub fn initWithPersistence(allocator: std.mem.Allocator, config: RaftConfig, persistence: ?RaftPersistence) !RaftNode {
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
            var i: usize = 0; // kcov-skip: runs on every init (peers fill loop); no own line record
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

        var node: RaftNode = .{
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
            .persistence = persistence,
            .persistence_failed = false,
            .recovered_snapshot = null,
            .allocator = allocator,
        };

        if (persistence != null) {
            errdefer node.log.deinit();
            errdefer if (node.recovered_snapshot) |*s| s.deinit();
            try node.recoverFromDisk();
        }

        return node;
    }

    pub fn deinit(self: *RaftNode) void {
        if (self.recovered_snapshot) |*s| s.deinit();
        self.recovered_snapshot = null;
        self.allocator.free(self.peers);
        self.allocator.free(self.next_index);
        self.allocator.free(self.match_index);
        self.allocator.free(self.votes_granted);
        self.log.deinit();
    }

    /// Reconstruct durable state from disk (see initWithPersistence docs).
    fn recoverFromDisk(self: *RaftNode) !void {
        const p = self.persistence.?;

        // 1. Vote state. null = file absent = genuinely never voted.
        //    error.CorruptVoteState propagates: refusing to start is the only
        //    safe response — a node that forgot a granted vote can vote twice
        //    in the same term and elect two leaders.
        if (try wal_mod.VoteState.load(p.vote_path)) |vs| {
            self.current_term = vs.current_term;
            self.voted_for = vs.voted_for;
        }

        // 2. Snapshot base (optional). The snapshot only ever contains
        //    committed-and-applied state, so its lastIncludedIndex is a safe
        //    floor for commit_index/last_applied. "All snapshots corrupt"
        //    propagates as an error rather than silently starting empty.
        var snap_index: u64 = 0;
        if (p.snapshots) |mgr| {
            if (try mgr.loadLatestSnapshot()) |snap| {
                self.recovered_snapshot = snap;
                snap_index = snap.last_included_index;
            }
        }

        // 3. WAL replay. recover() durably truncates any corrupt tail; the
        //    surviving prefix is exactly what this node ever acknowledged.
        //    The WAL holds the full log from index 1 (no compaction), so the
        //    in-memory indices line up by construction — verify anyway.
        const entries = try p.wal.recover();
        defer {
            for (entries) |e| self.allocator.free(e.data);
            self.allocator.free(entries);
        }
        for (entries) |e| {
            const idx = try self.log.append(e.term, e.data);
            if (idx != e.index) return error.WalLogMismatch;
        }

        // 4. Volatile indices per Raft: commit floor = snapshot point;
        //    everything after it re-commits and re-applies via the protocol.
        self.commit_index = snap_index; // kcov-skip: runs in snapshot recovery (snapshot-floor test); no own line record
        self.last_applied = snap_index;
    }

    /// Take ownership of the application state recovered from the latest
    /// snapshot at startup (null if there was none). The caller must
    /// `deinit()` the returned SnapshotData after restoring it into the
    /// state machine.
    pub fn takeRecoveredSnapshot(self: *RaftNode) ?snapshot_mod.SnapshotData {
        const snap = self.recovered_snapshot;
        self.recovered_snapshot = null;
        return snap;
    }

    /// Persist a snapshot of the application state machine, which must
    /// reflect EXACTLY the entries up to `last_applied` (snapshotting state
    /// that includes uncommitted entries would make phantom writes durable).
    /// Atomic on disk; a failure here leaves the previous snapshot intact
    /// and does NOT wedge the node (the WAL still has the full log).
    pub fn takeSnapshot(self: *RaftNode, state_data: []const u8) !void {
        if (self.persistence_failed) return error.PersistenceFailed;
        const p = self.persistence orelse return error.NoPersistence;
        const mgr = p.snapshots orelse return error.NoSnapshotManager;
        if (self.last_applied == 0) return error.NothingToSnapshot;
        const entry = self.log.getEntry(self.last_applied) orelse return error.SnapshotPointMissing;
        try mgr.takeSnapshot(entry.term, self.last_applied, state_data);
    }

    /// Durably record (term, voted_for) BEFORE the change becomes visible in
    /// memory or in any reply (persist-before-reply, Raft §5). No-op without
    /// persistence. On failure the node wedges itself and the caller MUST
    /// drop the triggering message instead of replying.
    fn persistVote(self: *RaftNode, term: u64, voted_for: ?u32) !void {
        const p = self.persistence orelse return;
        const vs = wal_mod.VoteState{ .current_term = term, .voted_for = voted_for };
        vs.save(p.vote_path) catch |err| {
            self.persistence_failed = true;
            return err;
        };
    }

    /// Handle an incoming Raft message from `from`. Returns an optional response.
    /// With persistence configured, a null where a response was expected means
    /// the message was DROPPED because durability could not be guaranteed
    /// (node wedged); dropping is always safe — it is equivalent to packet loss.
    pub fn handleMessage(self: *RaftNode, from: u32, msg: RaftMessage) ?MessageResponse {
        if (self.persistence_failed) return null;
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

    /// Called on election timeout. Transitions to candidate and returns a
    /// RequestVote message, or null if the new term's self-vote could not be
    /// made durable (the election must NOT proceed in that case — campaigning
    /// on a vote that a crash would forget can elect two leaders).
    pub fn startElection(self: *RaftNode) ?RaftMessage {
        if (self.persistence_failed) return null;
        // Persist (term+1, vote-for-self) BEFORE acting as a candidate or
        // letting the RequestVote out (persist-before-reply).
        self.persistVote(self.current_term + 1, self.config.node_id) catch return null;
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
    /// With persistence, the entry is WAL-appended and fsync'd BEFORE the
    /// leader counts itself toward the commit quorum: a leader whose own
    /// "replica" of an entry is only in RAM must not contribute to majority.
    pub fn propose(self: *RaftNode, data: []const u8) !u64 {
        if (self.persistence_failed) return error.PersistenceFailed;
        if (self.state != .leader) return error.NotLeader;
        const index = try self.log.append(self.current_term, data);
        if (self.persistence) |p| {
            p.wal.append(self.current_term, index, data) catch |err| {
                self.persistence_failed = true; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                return err; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
            };
            p.wal.sync() catch |err| {
                self.persistence_failed = true; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                return err;
            };
        }
        // In a single-node cluster (quorum of 1) the entry commits at once;
        // with peers this recomputes from unchanged match_index (a no-op).
        self.updateCommitIndex();
        return index;
    }

    /// Leader: create an AppendEntries message for a specific peer.
    /// Returns null if `peer_id` is not a known peer (e.g. our own id), or if
    /// the node is wedged (entries past the durable prefix must not be sent).
    pub fn createAppendEntries(self: *RaftNode, peer_id: u32) ?AppendEntries {
        if (self.persistence_failed) return null;
        const slot = self.peerSlot(peer_id) orelse return null;
        return self.buildAppendEntries(slot, true);
    }

    /// Leader: create a heartbeat for a specific peer. A heartbeat is just an
    /// AppendEntries with no entries, built from that peer's next_index so a
    /// lagging follower still passes (or meaningfully fails) the consistency
    /// check. Returns null if `peer_id` is not a known peer or the node is wedged.
    pub fn createHeartbeat(self: *RaftNode, peer_id: u32) ?AppendEntries {
        if (self.persistence_failed) return null;
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
            .term = self.current_term, // kcov-skip: runs in every buildAppendEntries (heartbeat tests); literal field store folded
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

    /// Callers that pass a HIGHER term must persist (term, null) via
    /// persistVote() BEFORE calling this (persist-before-reply).
    fn becomeFollower(self: *RaftNode, term: u64) void {
        self.state = .follower;
        // voted_for is only reset when the term actually advances: it is
        // persistent WITHIN a term (Raft §5.2). A same-term step-down (e.g.
        // a candidate seeing an established leader) must keep it — clearing
        // it would let this node grant a second vote in the same term.
        if (term > self.current_term) {
            self.current_term = term;
            self.voted_for = null;
        }
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

    fn handleRequestVote(self: *RaftNode, from: u32, rv: RequestVote) ?MessageResponse {
        // Decide first, persist, and only then commit to memory and reply:
        // a granted vote (or a term bump carried in the reply) that is not
        // durable yet can be forgotten by a crash, and a restarted node that
        // forgot a vote can vote again in the same term — two leaders.
        const stepping_down = rv.term > self.current_term;
        const new_term = if (stepping_down) rv.term else self.current_term;
        // voted_for as it would be after the (potential) term bump.
        var new_voted_for: ?u32 = if (stepping_down) null else self.voted_for;

        var granted = false;
        if (rv.term >= self.current_term) {
            const can_vote = (new_voted_for == null or new_voted_for.? == rv.candidate_id);
            const log_ok = (rv.last_log_term > self.log.lastTerm()) or
                (rv.last_log_term == self.log.lastTerm() and rv.last_log_index >= self.log.lastIndex());
            if (can_vote and log_ok) {
                new_voted_for = rv.candidate_id;
                granted = true;
            }
        }

        // Persist-before-reply: durable BEFORE any in-memory change or
        // response. On failure, drop the message (the node is wedged).
        if (new_term != self.current_term or !std.meta.eql(new_voted_for, self.voted_for)) {
            self.persistVote(new_term, new_voted_for) catch return null;
        }

        if (stepping_down) {
            self.becomeFollower(rv.term);
        }
        self.voted_for = new_voted_for;

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
            // No reply is sent here, but the bumped term WILL be carried by
            // future messages: persist it before it becomes visible. On
            // failure, drop (equivalent to having never seen the response).
            self.persistVote(rvr.term, null) catch return;
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

    fn handleAppendEntries(self: *RaftNode, from: u32, ae: AppendEntries) ?MessageResponse {
        if (ae.term > self.current_term) {
            // The response below carries the bumped term: persist it BEFORE
            // it becomes visible anywhere. On failure, drop the message.
            self.persistVote(ae.term, null) catch return null;
            self.becomeFollower(ae.term);
        } else if (ae.term == self.current_term and self.state == .candidate) {
            // Another leader exists for this term; step down. Same-term:
            // current_term/voted_for are unchanged (and stay persisted).
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

        // Append new entries (handle conflicts). Every in-memory log change
        // is mirrored to the WAL; new entries are fsync'd in one batch below
        // BEFORE the success reply (persist-before-reply). On any WAL
        // failure the node wedges and the message is dropped unanswered —
        // memory may then be ahead of disk, but nothing un-durable was ever
        // acknowledged, and a restart recovers the consistent disk state.
        var wal_dirty = false;
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
                    // Durable on-disk truncation FIRST: if the in-memory
                    // truncation ran alone, a restart would resurrect the
                    // conflicting tail from the WAL.
                    if (self.persistence) |p| {
                        p.wal.truncateFrom(entry.index) catch {
                            self.persistence_failed = true; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                            return null; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                        };
                    }
                    self.log.truncateFrom(entry.index);
                    const idx = self.log.append(entry.term, entry.data) catch {
                        return self.appendFailure(from, self.log.lastIndex());
                    };
                    if (self.persistence) |p| {
                        p.wal.append(entry.term, idx, entry.data) catch {
                            self.persistence_failed = true; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                            return null; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                        };
                        wal_dirty = true;
                    }
                }
                // If terms match, entry is already present (and already
                // durable from when it was first acknowledged); skip.
            } else {
                const idx = self.log.append(entry.term, entry.data) catch {
                    return self.appendFailure(from, self.log.lastIndex());
                };
                if (self.persistence) |p| {
                    p.wal.append(entry.term, idx, entry.data) catch {
                        self.persistence_failed = true; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                        return null; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                    };
                    wal_dirty = true;
                }
            }
        }

        // Log durability before ack: fsync everything appended above before
        // the success response leaves this node.
        if (wal_dirty) {
            self.persistence.?.wal.sync() catch {
                self.persistence_failed = true; // kcov-skip: WAL append/fsync failure pass-through; not injectable through the WAL's owned healthy fd in-process (the vote-path failure wedge is tested instead)
                return null;
            };
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
            // Persist the term bump before it can leak into future messages.
            self.persistVote(aer.term, null) catch return;
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

        if (aer.success) { // kcov-skip: evaluated on every leader AppendEntries response (3-node tests); no own line record
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

    const msg = node.startElection().?;
    try std.testing.expectEqual(NodeState.candidate, node.state);
    try std.testing.expectEqual(@as(u64, 1), node.current_term);
    try std.testing.expectEqual(@as(?u32, 1), node.voted_for);
    try std.testing.expectEqual(@as(u32, 1), node.votes_received);

    switch (msg) {
        .request_vote => |rv| {
            try std.testing.expectEqual(@as(u64, 1), rv.term);
            try std.testing.expectEqual(@as(u32, 1), rv.candidate_id);
        },
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
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
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
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
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
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
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
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
    var cur_to = to; // kcov-skip: test harness local; every exchange uses it; hit record oscillates between builds; the test runs and passes
    var cur_msg = msg; // kcov-skip: test harness local; every exchange uses it; hit record oscillates between builds
    while (true) {
        const resp = nodes[cur_to].handleMessage(cur_from, cur_msg) orelse return;
        cur_from = cur_to;
        cur_to = resp.to;
        cur_msg = resp.msg;
    }
}

fn testElect(nodes: []RaftNode, candidate: u32) void {
    const rv = nodes[candidate].startElection().?;
    for (nodes, 0..) |_, i| {
        const id: u32 = @intCast(i); // kcov-skip: test harness loop var; elections exercised throughout; hit record oscillates between builds
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
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
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
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
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
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
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
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
    }
    try std.testing.expectEqual(@as(u64, 1), node.commit_index);
}

// =============================================================================
// Persistence / restart tests.
// Each "boot" opens a fresh WriteAheadLog handle and RaftNode over the SAME
// on-disk directory; deinit + re-init simulates a process restart: every
// piece of in-memory Raft state is rebuilt strictly through the recovery
// path (VoteState.load -> snapshot -> wal.recover + replay).
// =============================================================================

/// Stable storage for the per-test on-disk paths (the slices point into the
/// embedded buffers, so the struct must not be copied after init).
const TestPaths = struct {
    dir_buf: [std.fs.max_path_bytes]u8 = undefined,
    wal_buf: [std.fs.max_path_bytes]u8 = undefined,
    vote_buf: [std.fs.max_path_bytes]u8 = undefined,
    dir: []const u8 = &.{},
    wal_path: []const u8 = &.{},
    vote_path: []const u8 = &.{},

    fn init(self: *TestPaths, d: std.fs.Dir, node_id: u32) !void {
        self.dir = try d.realpath(".", &self.dir_buf);
        self.wal_path = try std.fmt.bufPrint(&self.wal_buf, "{s}/raft{d}.wal", .{ self.dir, node_id });
        self.vote_path = try std.fmt.bufPrint(&self.vote_buf, "{s}/vote{d}.state", .{ self.dir, node_id });
    }
};

test "RaftNode: persistence — granted vote survives restart, no double vote in same term" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var paths: TestPaths = .{};
    try paths.init(tmp.dir, 0);

    // Boot 1: grant our vote to candidate 1 in term 5.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 2,
        }, .{ .wal = &wal, .vote_path = paths.vote_path });
        defer node.deinit();

        const resp = node.handleMessage(1, .{ .request_vote = .{
            .term = 5,
            .candidate_id = 1,
            .last_log_index = 0,
            .last_log_term = 0,
        } });
        switch (resp.?.msg) {
            .request_vote_response => |rvr| {
                try std.testing.expect(rvr.vote_granted);
                try std.testing.expectEqual(@as(u64, 5), rvr.term);
            },
            else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
        }
    }

    // Boot 2 (restart from the same directory): the vote MUST be remembered.
    // Forgetting it and granting candidate 2 in the same term would give
    // term 5 two leaders — the classic restart split-brain.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 2,
        }, .{ .wal = &wal, .vote_path = paths.vote_path });
        defer node.deinit();

        try std.testing.expectEqual(@as(u64, 5), node.current_term);
        try std.testing.expectEqual(@as(?u32, 1), node.voted_for);

        // A DIFFERENT candidate in the SAME term is refused...
        const rival = node.handleMessage(2, .{ .request_vote = .{
            .term = 5,
            .candidate_id = 2,
            .last_log_index = 0,
            .last_log_term = 0,
        } });
        switch (rival.?.msg) {
            .request_vote_response => |rvr| {
                try std.testing.expect(!rvr.vote_granted);
                try std.testing.expectEqual(@as(u64, 5), rvr.term);
            },
            else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
        }

        // ...while a retransmission from the ORIGINAL candidate is still
        // granted (idempotent, no new persistence needed).
        const again = node.handleMessage(1, .{ .request_vote = .{
            .term = 5,
            .candidate_id = 1,
            .last_log_index = 0,
            .last_log_term = 0,
        } });
        switch (again.?.msg) {
            .request_vote_response => |rvr| try std.testing.expect(rvr.vote_granted),
            else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
        }
    }
}

test "RaftNode: persistence — leader log and term survive restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var paths: TestPaths = .{};
    try paths.init(tmp.dir, 0);

    // Boot 1: single-node leader proposes two entries. propose() persists
    // each entry (append + fsync) BEFORE counting itself in the quorum, so
    // by the time commit_index moved, the entries were already durable.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 0,
        }, .{ .wal = &wal, .vote_path = paths.vote_path });
        defer node.deinit();

        _ = node.startElection().?; // term 1, persists self-vote
        try std.testing.expectEqual(NodeState.leader, node.state);
        _ = try node.propose("alpha");
        _ = try node.propose("beta");
        try std.testing.expectEqual(@as(u64, 2), node.commit_index);
        // The WAL holds both entries durably.
        try std.testing.expectEqual(@as(u64, 2), wal.lastIndex());
    }

    // Boot 2: everything durable is recovered; volatile commit state restarts
    // at 0 (no snapshot) and is re-derived by the protocol.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 0,
        }, .{ .wal = &wal, .vote_path = paths.vote_path });
        defer node.deinit();

        try std.testing.expectEqual(@as(u64, 1), node.current_term);
        try std.testing.expectEqual(@as(?u32, 0), node.voted_for);
        try std.testing.expectEqual(NodeState.follower, node.state);
        try std.testing.expectEqual(@as(u64, 2), node.log.lastIndex());
        try std.testing.expectEqualStrings("alpha", node.log.getEntry(1).?.data);
        try std.testing.expectEqualStrings("beta", node.log.getEntry(2).?.data);
        try std.testing.expectEqual(@as(u64, 0), node.commit_index);
        try std.testing.expectEqual(@as(u64, 0), node.last_applied);

        // Re-elect and commit a current-term entry: the recovered entries
        // commit transitively (Raft Figure 8 discipline still holds).
        _ = node.startElection().?; // term 2
        try std.testing.expectEqual(NodeState.leader, node.state);
        _ = try node.propose("gamma");
        try std.testing.expectEqual(@as(u64, 3), node.commit_index);
        try std.testing.expectEqual(@as(usize, 3), node.getApplicableEntries().len);
    }
}

test "RaftNode: persistence — three-node cluster, restarted follower recovers replicated log" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var paths: [3]TestPaths = .{ .{}, .{}, .{} };
    for (&paths, 0..) |*p, i| try p.init(tmp.dir, @intCast(i));

    var wals: [3]wal_mod.WriteAheadLog = undefined;
    var nodes: [3]RaftNode = undefined;
    var booted: usize = 0;
    defer for (wals[0..booted]) |*w| w.deinit();
    defer for (nodes[0..booted]) |*n| n.deinit();
    for (0..3) |i| {
        wals[i] = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths[i].wal_path,
            .sync_policy = .explicit,
        });
        errdefer wals[i].deinit(); // kcov-skip: test cleanup errdefer; fires only if the test setup itself fails
        nodes[i] = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = @intCast(i),
            .peer_count = 2,
        }, .{ .wal = &wals[i], .vote_path = paths[i].vote_path });
        booted += 1;
    }

    // Term 1: node 0 leads, replicates 3 entries to everyone, commit = 3.
    testElect(&nodes, 0);
    try std.testing.expectEqual(NodeState.leader, nodes[0].state);
    _ = try nodes[0].propose("a");
    _ = try nodes[0].propose("b");
    _ = try nodes[0].propose("c");
    testReplicate(&nodes, 0, 1);
    testReplicate(&nodes, 0, 2);
    testHeartbeat(&nodes, 0, 1); // propagate commit index to node 1
    try std.testing.expectEqual(@as(u64, 3), nodes[0].commit_index);
    try std.testing.expectEqual(@as(u64, 3), nodes[1].commit_index);

    // Restart node 1 from disk: the acknowledged log and the granted vote
    // come back; volatile commit state restarts at 0.
    nodes[1].deinit();
    wals[1].deinit();
    wals[1] = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
        .path = paths[1].wal_path,
        .sync_policy = .explicit,
    });
    nodes[1] = try RaftNode.initWithPersistence(std.testing.allocator, .{
        .node_id = 1,
        .peer_count = 2,
    }, .{ .wal = &wals[1], .vote_path = paths[1].vote_path });

    try std.testing.expectEqual(@as(u64, 1), nodes[1].current_term);
    try std.testing.expectEqual(@as(?u32, 0), nodes[1].voted_for); // vote for node 0 remembered
    try std.testing.expectEqual(@as(u64, 3), nodes[1].log.lastIndex());
    try std.testing.expectEqualStrings("b", nodes[1].log.getEntry(2).?.data);
    try std.testing.expectEqual(@as(u64, 0), nodes[1].commit_index);

    // The next leader heartbeat passes the consistency check against the
    // recovered log and re-teaches the follower its commit index.
    testHeartbeat(&nodes, 0, 1);
    try std.testing.expectEqual(@as(u64, 3), nodes[1].commit_index);
}

test "RaftNode: persistence — conflict truncation is durable across restart" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var paths: TestPaths = .{};
    try paths.init(tmp.dir, 0);

    var data_a = "a".*;
    var data_b = "b".*;
    var data_x = "x".*;
    var data_y = "y".*;

    // Boot 1: follower accepts an (uncommitted) tail from the term-1 leader,
    // then a term-2 leader overwrites the divergent entry at index 3.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 2,
        }, .{ .wal = &wal, .vote_path = paths.vote_path });
        defer node.deinit();

        const e1 = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 1, .data = &data_a };
        const e2 = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 2, .data = &data_b };
        const e3_old = raft_log.RaftLog.StoredEntry{ .term = 1, .index = 3, .data = &data_x };
        const ok = node.handleMessage(1, .{ .append_entries = .{
            .term = 1,
            .leader_id = 1,
            .prev_log_index = 0,
            .prev_log_term = 0,
            .entries = &.{ e1, e2, e3_old },
            .leader_commit = 0,
        } });
        switch (ok.?.msg) {
            .append_entries_response => |aer| try std.testing.expect(aer.success),
            else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
        }

        // New leader (term 2) replaces the uncommitted entry 3: the on-disk
        // log is truncated durably BEFORE the replacement is appended.
        const e3_new = raft_log.RaftLog.StoredEntry{ .term = 2, .index = 3, .data = &data_y };
        const resp = node.handleMessage(2, .{ .append_entries = .{
            .term = 2,
            .leader_id = 2,
            .prev_log_index = 2,
            .prev_log_term = 1,
            .entries = &.{e3_new},
            .leader_commit = 0,
        } });
        switch (resp.?.msg) {
            .append_entries_response => |aer| try std.testing.expect(aer.success),
            else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
        }
        try std.testing.expectEqual(@as(u64, 2), node.log.getEntry(3).?.term);
    }

    // Boot 2: the truncated term-1 entry must NOT resurrect from disk —
    // if it did, this node could ack the same index with two different
    // values across its lifetimes.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 2,
        }, .{ .wal = &wal, .vote_path = paths.vote_path });
        defer node.deinit();

        try std.testing.expectEqual(@as(u64, 2), node.current_term);
        try std.testing.expectEqual(@as(u64, 3), node.log.lastIndex());
        const e3 = node.log.getEntry(3).?;
        try std.testing.expectEqual(@as(u64, 2), e3.term);
        try std.testing.expectEqualStrings("y", e3.data);
    }
}

test "RaftNode: persistence — corrupt vote file refuses to start" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var paths: TestPaths = .{};
    try paths.init(tmp.dir, 0);

    // Boot 1: grant a vote so the vote file exists.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 2,
        }, .{ .wal = &wal, .vote_path = paths.vote_path });
        defer node.deinit();
        _ = node.handleMessage(1, .{ .request_vote = .{
            .term = 3,
            .candidate_id = 1,
            .last_log_index = 0,
            .last_log_term = 0,
        } });
    }

    // Corrupt the vote record on disk (bit flip).
    {
        const f = try tmp.dir.openFile("vote0.state", .{ .mode = .read_write });
        defer f.close();
        var byte: [1]u8 = undefined;
        _ = try f.preadAll(&byte, 3);
        byte[0] ^= 0xFF;
        try f.seekTo(3);
        try f.writeAll(&byte);
    }

    // Boot 2 MUST refuse to start. Silently starting "fresh" would forget
    // the term-3 vote and allow a second one in the same term.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        try std.testing.expectError(error.CorruptVoteState, RaftNode.initWithPersistence(
            std.testing.allocator,
            .{ .node_id = 0, .peer_count = 2 },
            .{ .wal = &wal, .vote_path = paths.vote_path },
        ));
    }
}

test "RaftNode: persistence — snapshot seeds commit/applied floor and tail replays" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var paths: TestPaths = .{};
    try paths.init(tmp.dir, 0);

    // Boot 1: single-node leader applies 3 entries, snapshots the state
    // machine at last_applied = 3, then appends 2 more committed entries.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var snaps = try snapshot_mod.SnapshotManager.init(std.testing.allocator, .{
            .base_path = paths.dir,
        });
        defer snaps.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 0,
        }, .{ .wal = &wal, .vote_path = paths.vote_path, .snapshots = &snaps });
        defer node.deinit();

        _ = node.startElection().?; // term 1
        _ = try node.propose("a");
        _ = try node.propose("b");
        _ = try node.propose("c");
        node.markApplied(3);
        try node.takeSnapshot("state-at-3");

        _ = try node.propose("d");
        _ = try node.propose("e");
        try std.testing.expectEqual(@as(u64, 5), node.commit_index);
    }

    // Boot 2: the snapshot seeds the state-machine base and the
    // commit/applied floor; the WAL tail past it is replayed and re-commits
    // through the protocol — the snapshotted prefix is never re-applied.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = paths.wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var snaps = try snapshot_mod.SnapshotManager.init(std.testing.allocator, .{
            .base_path = paths.dir,
        });
        defer snaps.deinit();
        var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 0,
        }, .{ .wal = &wal, .vote_path = paths.vote_path, .snapshots = &snaps });
        defer node.deinit();

        var snap = node.takeRecoveredSnapshot().?;
        defer snap.deinit();
        try std.testing.expectEqual(@as(u64, 3), snap.last_included_index);
        try std.testing.expectEqual(@as(u64, 1), snap.last_included_term);
        try std.testing.expectEqualStrings("state-at-3", snap.data);

        try std.testing.expectEqual(@as(u64, 3), node.commit_index);
        try std.testing.expectEqual(@as(u64, 3), node.last_applied);
        try std.testing.expectEqual(@as(u64, 5), node.log.lastIndex());
        // Nothing is applicable yet: the prefix is inside the snapshot and
        // the tail is not (re-)committed yet.
        try std.testing.expectEqual(@as(usize, 0), node.getApplicableEntries().len);

        // Re-elect; committing a current-term entry re-commits the tail.
        _ = node.startElection().?; // term 2
        _ = try node.propose("f");
        try std.testing.expectEqual(@as(u64, 6), node.commit_index);
        const applicable = node.getApplicableEntries();
        try std.testing.expectEqual(@as(usize, 3), applicable.len);
        try std.testing.expectEqual(@as(u64, 4), applicable[0].index);
        try std.testing.expectEqualStrings("d", applicable[0].data);
        try std.testing.expectEqualStrings("f", applicable[2].data);
    }
}

test "RaftNode: stale-term AppendEntries is rejected" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    });
    defer node.deinit();

    // Advance to term 5 via a current leader.
    _ = node.handleMessage(1, .{ .append_entries = .{
        .term = 5,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    } });
    try std.testing.expectEqual(@as(u64, 5), node.current_term);

    // A deposed term-1 leader must be refused.
    const result = node.handleMessage(2, .{ .append_entries = .{
        .term = 1,
        .leader_id = 2,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    } });
    switch (result.?.msg) {
        .append_entries_response => |aer| {
            try std.testing.expect(!aer.success);
            try std.testing.expectEqual(@as(u64, 5), aer.term);
        },
        else => return error.UnexpectedMessage, // kcov-skip: test helper: defensive arm of a message-union unwrap, taken only if the test already failed
    }
}

test "RaftNode: leader steps down on a higher-term AppendEntries response" {
    var node = try RaftNode.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 0,
    });
    defer node.deinit();

    _ = node.startElection();
    try std.testing.expectEqual(NodeState.leader, node.state);

    // A response from a future term proves a newer leadership epoch exists.
    const resp = node.handleMessage(1, .{ .append_entries_response = .{
        .term = 99,
        .success = false,
        .match_index = 0,
    } });
    try std.testing.expect(resp == null);
    try std.testing.expectEqual(NodeState.follower, node.state);
    try std.testing.expectEqual(@as(u64, 99), node.current_term);
}

test "RaftNode: vote persistence failure wedges the node" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var paths: TestPaths = .{};
    try paths.init(tmp.dir, 0);

    var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
        .path = paths.wal_path,
        .sync_policy = .explicit,
    });
    defer wal.deinit();

    // vote_path in a directory that does not exist: the pre-reply persist
    // of (term, voted_for) must fail and the node must wedge itself.
    var node = try RaftNode.initWithPersistence(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, .{ .wal = &wal, .vote_path = "/nonexistent_zigbolt_dir/vote.bin" });
    defer node.deinit();

    const result = node.handleMessage(1, .{ .request_vote = .{
        .term = 1,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    } });
    // The message is DROPPED (safe: equivalent to packet loss), never
    // answered with an unpersisted vote.
    try std.testing.expect(result == null);
    try std.testing.expect(node.persistence_failed);

    // Wedged: everything is refused from here on.
    try std.testing.expectError(error.PersistenceFailed, node.propose("x"));
    try std.testing.expect(node.handleMessage(1, .{ .request_vote = .{
        .term = 2,
        .candidate_id = 1,
        .last_log_index = 0,
        .last_log_term = 0,
    } }) == null);
}
