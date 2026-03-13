const std = @import("std");
const raft = @import("raft.zig");
const raft_log = @import("raft_log.zig");

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
        const raft_config = raft.RaftConfig{
            .node_id = config.node_id,
            .peer_count = config.peer_count,
            .election_timeout_min_ms = config.election_timeout_min_ms,
            .election_timeout_max_ms = config.election_timeout_max_ms,
            .heartbeat_interval_ms = config.heartbeat_interval_ms,
        };
        const node = try raft.RaftNode.init(allocator, raft_config);
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
    pub fn handleMessage(self: *Cluster, from: u32, msg: raft.RaftMessage) ?struct { to: u32, msg: raft.RaftMessage } {
        return self.node.handleMessage(from, msg);
    }

    /// Tick: called periodically. Applies committed entries to the state machine.
    pub fn tick(self: *Cluster) void {
        const entries = self.node.getApplicableEntries();
        if (entries.len == 0) return;

        if (self.state_machine) |sm| {
            for (entries) |entry| {
                sm.apply_fn(entry.data);
            }
        }

        // Mark all as applied up to commit_index
        self.node.markApplied(self.node.commit_index);
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
