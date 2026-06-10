const std = @import("std");
const raft = @import("raft.zig");
const raft_log = @import("raft_log.zig");
const wal_mod = @import("wal.zig");
const snapshot_mod = @import("snapshot.zig");

pub const ClusterConfig = struct {
    node_id: u32,
    peer_count: u32,
    election_timeout_min_ms: u32 = 150,
    election_timeout_max_ms: u32 = 300,
    heartbeat_interval_ms: u32 = 50,
};

/// State Machine interface -- user implements business logic.
pub const StateMachine = struct {
    apply_fn: *const fn (entry: []const u8) void,
    snapshot_fn: ?*const fn () []const u8 = null,
    restore_fn: ?*const fn (snapshot: []const u8) void = null,
};

pub const Cluster = struct {
    node: raft.RaftNode,
    state_machine: ?StateMachine,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: ClusterConfig, sm: ?StateMachine) !Cluster {
        return initWithPersistence(allocator, config, sm, null);
    }

    /// Init with an optional durable backend (see raft.RaftPersistence for
    /// the ownership/lifetime contract). With persistence configured, the
    /// RaftNode recovers term/vote/log/snapshot from disk, and the state
    /// machine is seeded here from the recovered snapshot via `restore_fn`
    /// before any new entry can be applied. Entries after the snapshot
    /// point re-apply through tick() as they re-commit, per Raft.
    pub fn initWithPersistence(
        allocator: std.mem.Allocator,
        config: ClusterConfig,
        sm: ?StateMachine,
        persistence: ?raft.RaftPersistence,
    ) !Cluster {
        const raft_config = raft.RaftConfig{
            .node_id = config.node_id,
            .peer_count = config.peer_count, // kcov-skip: runs on every init (cluster tests); literal field store folded, no own line record
            .election_timeout_min_ms = config.election_timeout_min_ms,
            .election_timeout_max_ms = config.election_timeout_max_ms, // kcov-skip: runs on every init (cluster tests); literal field store folded, no own line record
            .heartbeat_interval_ms = config.heartbeat_interval_ms,
        };
        var node = try raft.RaftNode.initWithPersistence(allocator, raft_config, persistence);
        errdefer node.deinit();

        // Seed the application state machine from the recovered snapshot
        // base (last_applied already points at its lastIncludedIndex, so the
        // snapshotted prefix is never re-applied — no double execution).
        if (node.takeRecoveredSnapshot()) |snap_const| {
            var snap = snap_const;
            defer snap.deinit();
            if (sm) |s| {
                if (s.restore_fn) |restore| {
                    restore(snap.data);
                }
            }
        }

        return .{
            .node = node,
            .state_machine = sm,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Cluster) void {
        self.node.deinit();
    }

    /// Propose a command (leader only). Returns the log index.
    pub fn propose(self: *Cluster, data: []const u8) !u64 {
        return try self.node.propose(data);
    }

    /// Process an incoming message.
    pub fn handleMessage(self: *Cluster, from: u32, msg: raft.RaftMessage) ?raft.MessageResponse {
        return self.node.handleMessage(from, msg);
    }

    /// Tick: called periodically. Applies committed entries to the state machine.
    /// getApplicableEntries returns exactly (last_applied, commit_index], so
    /// each committed entry is applied exactly once, in log order, and an
    /// uncommitted entry is never applied.
    pub fn tick(self: *Cluster) void {
        const entries = self.node.getApplicableEntries();
        if (entries.len == 0) return;

        if (self.state_machine) |sm| {
            for (entries) |entry| {
                sm.apply_fn(entry.data);
            }
        }

        // Mark applied up to the last entry actually applied above.
        self.node.markApplied(entries[entries.len - 1].index);
    }

    /// Check if this node is the leader.
    pub fn isLeader(self: *const Cluster) bool {
        return self.node.state == .leader;
    }

    /// Get current node state.
    pub fn getState(self: *const Cluster) raft.NodeState {
        return self.node.state;
    }
};

// =============================================================================
// Tests
// =============================================================================

var test_applied_count: u32 = 0;

fn testApply(_: []const u8) void {
    test_applied_count += 1;
}

test "Cluster: init and deinit" {
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, null);
    defer cluster.deinit();

    try std.testing.expectEqual(raft.NodeState.follower, cluster.getState());
    try std.testing.expect(!cluster.isLeader());
}

test "Cluster: init with state machine" {
    const sm = StateMachine{
        .apply_fn = &testApply,
    };
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, sm);
    defer cluster.deinit();

    try std.testing.expect(!cluster.isLeader());
}

test "Cluster: propose fails when not leader" {
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, null);
    defer cluster.deinit();

    const result = cluster.propose("data");
    try std.testing.expectError(error.NotLeader, result);
}

test "Cluster: propose succeeds as leader" {
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, null);
    defer cluster.deinit();

    // Become leader
    _ = cluster.node.startElection();
    _ = cluster.node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    try std.testing.expect(cluster.isLeader());

    const idx = try cluster.propose("command1");
    try std.testing.expectEqual(@as(u64, 1), idx);
}

test "Cluster: tick applies committed entries to state machine" {
    test_applied_count = 0;

    const sm = StateMachine{
        .apply_fn = &testApply,
    };
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, sm);
    defer cluster.deinit();

    // Become leader
    _ = cluster.node.startElection();
    _ = cluster.node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });

    _ = try cluster.propose("a");
    _ = try cluster.propose("b");

    // Tick before commit — nothing applied
    cluster.tick();
    try std.testing.expectEqual(@as(u32, 0), test_applied_count);

    // Simulate replication acknowledgment from both peers
    cluster.node.match_index[0] = 2;
    cluster.node.match_index[1] = 2;
    cluster.node.updateCommitIndex();

    // Now tick should apply
    cluster.tick();
    try std.testing.expectEqual(@as(u32, 2), test_applied_count);

    // Another tick — nothing new to apply
    cluster.tick();
    try std.testing.expectEqual(@as(u32, 2), test_applied_count);
}

var test_exact_buf: [16]u8 = undefined;
var test_exact_len: usize = 0;

fn testApplyRecord(entry: []const u8) void {
    if (test_exact_len < test_exact_buf.len and entry.len > 0) {
        test_exact_buf[test_exact_len] = entry[0];
    }
    test_exact_len += 1;
}

test "Cluster: applies each committed entry exactly once and never an uncommitted one" {
    test_exact_len = 0;

    const sm = StateMachine{
        .apply_fn = &testApplyRecord,
    };
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, sm);
    defer cluster.deinit();

    // Become leader (term 1)
    _ = cluster.node.startElection();
    _ = cluster.node.handleMessage(1, .{ .request_vote_response = .{
        .term = 1,
        .vote_granted = true,
    } });
    try std.testing.expect(cluster.isLeader());

    _ = try cluster.propose("1");
    _ = try cluster.propose("2");
    _ = try cluster.propose("3");
    _ = try cluster.propose("4");
    _ = try cluster.propose("5");

    // Nothing committed yet: nothing may be applied.
    cluster.tick();
    try std.testing.expectEqual(@as(usize, 0), test_exact_len);

    // commit_index = 2 while log = [1..5]: ONLY entries 1 and 2 are applied.
    cluster.node.match_index[0] = 2;
    cluster.node.match_index[1] = 2;
    cluster.node.updateCommitIndex();
    cluster.tick();
    try std.testing.expectEqual(@as(usize, 2), test_exact_len);
    try std.testing.expectEqualStrings("12", test_exact_buf[0..test_exact_len]);

    // Re-ticking applies nothing again (no double execution).
    cluster.tick();
    cluster.tick();
    try std.testing.expectEqual(@as(usize, 2), test_exact_len);

    // Commit advances to 5: entries 3..5 are applied exactly once, in order.
    cluster.node.match_index[0] = 5;
    cluster.node.match_index[1] = 5;
    cluster.node.updateCommitIndex();
    cluster.tick();
    try std.testing.expectEqual(@as(usize, 5), test_exact_len);
    try std.testing.expectEqualStrings("12345", test_exact_buf[0..test_exact_len]);

    cluster.tick();
    try std.testing.expectEqual(@as(usize, 5), test_exact_len);
}

test "Cluster: single-node cluster elects itself and applies proposals" {
    test_applied_count = 0;

    const sm = StateMachine{
        .apply_fn = &testApply,
    };
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 0,
    }, sm);
    defer cluster.deinit();

    // One election timeout is enough: quorum of 1.
    _ = cluster.node.startElection();
    try std.testing.expect(cluster.isLeader());

    _ = try cluster.propose("only");
    try std.testing.expectEqual(@as(u64, 1), cluster.node.commit_index);

    cluster.tick();
    try std.testing.expectEqual(@as(u32, 1), test_applied_count);
    cluster.tick();
    try std.testing.expectEqual(@as(u32, 1), test_applied_count);
}

// State machine for the persistence test: a counter of applied entries that
// can be snapshotted to / restored from a single byte.
var test_counter: u8 = 0;

fn testCounterApply(_: []const u8) void {
    test_counter += 1;
}

fn testCounterRestore(snap: []const u8) void {
    test_counter = if (snap.len > 0) snap[0] else 0;
}

test "Cluster: persistence — snapshot restores state machine base, tail re-applies exactly once" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir = try tmp.dir.realpath(".", &dir_buf);
    var wal_buf: [std.fs.max_path_bytes]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&wal_buf, "{s}/raft.wal", .{dir});
    var vote_buf: [std.fs.max_path_bytes]u8 = undefined;
    const vote_path = try std.fmt.bufPrint(&vote_buf, "{s}/vote.state", .{dir});

    const sm = StateMachine{
        .apply_fn = &testCounterApply,
        .restore_fn = &testCounterRestore,
    };

    // Boot 1: apply 3 entries, snapshot the state machine at last_applied=3,
    // then commit 2 more entries WITHOUT applying them.
    test_counter = 0;
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var snaps = try snapshot_mod.SnapshotManager.init(std.testing.allocator, .{
            .base_path = dir,
        });
        defer snaps.deinit();
        var cluster = try Cluster.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 0,
        }, sm, .{ .wal = &wal, .vote_path = vote_path, .snapshots = &snaps });
        defer cluster.deinit();

        _ = cluster.node.startElection(); // term 1, single-node leader
        try std.testing.expect(cluster.isLeader());

        _ = try cluster.propose("a");
        _ = try cluster.propose("b");
        _ = try cluster.propose("c");
        cluster.tick();
        try std.testing.expectEqual(@as(u8, 3), test_counter);

        // Snapshot the applied state (a single counter byte).
        try cluster.node.takeSnapshot(&[_]u8{test_counter});

        _ = try cluster.propose("d");
        _ = try cluster.propose("e");
        // Committed but deliberately NOT ticked/applied before the "crash".
        try std.testing.expectEqual(@as(u64, 5), cluster.node.commit_index);
    }

    // Boot 2: restore_fn seeds the state machine from the snapshot; the
    // snapshotted prefix (a,b,c) is never re-applied, and the tail (d,e)
    // re-applies exactly once after it re-commits.
    test_counter = 0;
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var snaps = try snapshot_mod.SnapshotManager.init(std.testing.allocator, .{
            .base_path = dir,
        });
        defer snaps.deinit();
        var cluster = try Cluster.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 0,
        }, sm, .{ .wal = &wal, .vote_path = vote_path, .snapshots = &snaps });
        defer cluster.deinit();

        // restore_fn already ran with the snapshot data.
        try std.testing.expectEqual(@as(u8, 3), test_counter);
        try std.testing.expectEqual(@as(u64, 3), cluster.node.commit_index);
        try std.testing.expectEqual(@as(u64, 3), cluster.node.last_applied);
        try std.testing.expectEqual(@as(u64, 5), cluster.node.log.lastIndex());

        // Nothing applicable until the tail re-commits.
        cluster.tick();
        try std.testing.expectEqual(@as(u8, 3), test_counter);

        _ = cluster.node.startElection(); // term 2
        try std.testing.expect(cluster.isLeader());
        _ = try cluster.propose("f"); // commits 4..6 transitively
        cluster.tick();
        // d, e, f applied exactly once on top of the restored base.
        try std.testing.expectEqual(@as(u8, 6), test_counter);
        cluster.tick();
        try std.testing.expectEqual(@as(u8, 6), test_counter);
    }
}

test "Cluster: handleMessage delegates to raft node" {
    var cluster = try Cluster.init(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 2,
    }, null);
    defer cluster.deinit();

    const result = cluster.handleMessage(1, .{ .append_entries = .{
        .term = 3,
        .leader_id = 1,
        .prev_log_index = 0,
        .prev_log_term = 0,
        .entries = &.{},
        .leader_commit = 0,
    } });

    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 3), cluster.node.current_term);
}

test "Cluster restores the state machine from a recovered snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try std.testing.allocator.dupe(u8, &@as([1]u8, undefined));
    std.testing.allocator.free(base);
    const real = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(real);
    var wal_path_buf: [512]u8 = undefined;
    const wal_path = try std.fmt.bufPrint(&wal_path_buf, "{s}/wal.bin", .{real});
    var vote_path_buf: [512]u8 = undefined;
    const vote_path = try std.fmt.bufPrint(&vote_path_buf, "{s}/vote.bin", .{real});

    // Boot 1 (raw RaftNode): apply one entry and snapshot the state machine.
    {
        var wal = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
            .path = wal_path,
            .sync_policy = .explicit,
        });
        defer wal.deinit();
        var snaps = try snapshot_mod.SnapshotManager.init(std.testing.allocator, .{
            .base_path = real,
        });
        defer snaps.deinit();
        var node = try raft.RaftNode.initWithPersistence(std.testing.allocator, .{
            .node_id = 0,
            .peer_count = 0,
        }, .{ .wal = &wal, .vote_path = vote_path, .snapshots = &snaps });
        defer node.deinit();

        _ = node.startElection();
        _ = try node.propose("a");
        node.markApplied(1);
        try node.takeSnapshot("cluster-snap-state");
    }

    // Boot 2 (Cluster): restore_fn must be seeded from the snapshot.
    const S = struct {
        var restored_buf: [64]u8 = undefined;
        var restored_len: usize = 0;
        var applied: u32 = 0;
        fn apply(entry: []const u8) void {
            _ = entry;
            applied += 1;
        }
        fn restore(snapshot: []const u8) void {
            const n = @min(snapshot.len, restored_buf.len);
            @memcpy(restored_buf[0..n], snapshot[0..n]);
            restored_len = snapshot.len;
        }
    };
    S.restored_len = 0;
    S.applied = 0;

    var wal2 = try wal_mod.WriteAheadLog.init(std.testing.allocator, .{
        .path = wal_path,
        .sync_policy = .explicit,
    });
    defer wal2.deinit();
    var snaps2 = try snapshot_mod.SnapshotManager.init(std.testing.allocator, .{
        .base_path = real,
    });
    defer snaps2.deinit();

    var cluster = try Cluster.initWithPersistence(std.testing.allocator, .{
        .node_id = 0,
        .peer_count = 0,
    }, .{ .apply_fn = S.apply, .restore_fn = S.restore }, .{
        .wal = &wal2,
        .vote_path = vote_path,
        .snapshots = &snaps2,
    });
    defer cluster.deinit();

    try std.testing.expectEqualStrings("cluster-snap-state", S.restored_buf[0..S.restored_len]);
    // The snapshotted prefix is NOT re-applied through apply_fn.
    try std.testing.expectEqual(@as(u32, 0), S.applied);
}
