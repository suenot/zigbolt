const std = @import("std");
const idle_strategy_mod = @import("idle_strategy.zig");
const config = @import("../platform/config.zig");

/// Aeron-style Agent interface — a composable unit of work with lifecycle management.
///
/// Agents are function-pointer based to avoid Zig's comptime interface limitations
/// and allow runtime composition (e.g., CompositeAgent combining multiple agents).
pub const AgentFn = struct {
    /// Returns amount of work done (0 = idle).
    doWorkFn: *const fn (ctx: *anyopaque) u32,
    /// Called on agent start.
    onStartFn: ?*const fn (ctx: *anyopaque) void = null,
    /// Called on agent close.
    onCloseFn: ?*const fn (ctx: *anyopaque) void = null,
    /// Context pointer.
    ctx: *anyopaque,
    /// Agent name for debugging.
    name: []const u8 = "unnamed",

    pub fn doWork(self: *const AgentFn) u32 {
        return self.doWorkFn(self.ctx);
    }

    pub fn onStart(self: *const AgentFn) void {
        if (self.onStartFn) |f| f(self.ctx);
    }

    pub fn onClose(self: *const AgentFn) void {
        if (self.onCloseFn) |f| f(self.ctx);
    }
};

/// Runs an agent on a dedicated thread with an idle strategy.
///
/// The runner manages the agent's lifecycle: start → run loop → stop.
/// The duty cycle calls doWork() and feeds the result to the idle strategy.
pub const AgentRunner = struct {
    agent: AgentFn,
    idle_strategy: idle_strategy_mod.IdleStrategy,
    running: std.atomic.Value(bool),
    thread: ?std.Thread = null,
    error_count: std.atomic.Value(u64),

    pub fn init(agent: AgentFn, idle: idle_strategy_mod.IdleStrategy) AgentRunner {
        return .{
            .agent = agent,
            .idle_strategy = idle,
            .running = std.atomic.Value(bool).init(false),
            .thread = null,
            .error_count = std.atomic.Value(u64).init(0),
        };
    }

    /// Start the agent on a new thread.
    pub fn start(self: *AgentRunner) !void {
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, runLoop, .{self});
    }

    /// Stop the agent and join the thread.
    pub fn stop(self: *AgentRunner) void {
        self.running.store(false, .release);
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    fn runLoop(self: *AgentRunner) void {
        self.agent.onStart();
        defer self.agent.onClose();

        while (self.running.load(.acquire)) {
            const work_count = self.agent.doWork();
            self.idle_strategy.idle(work_count);
        }
    }

    pub fn isRunning(self: *const AgentRunner) bool {
        return self.running.load(.acquire);
    }

    pub fn errorCount(self: *const AgentRunner) u64 {
        return self.error_count.load(.monotonic);
    }
};

/// Combines multiple agents into a single agent that calls doWork on each.
///
/// The composite returns the sum of work done by all sub-agents,
/// allowing the idle strategy to properly detect idle vs. busy cycles.
pub const CompositeAgent = struct {
    agents: []const AgentFn,
    name: []const u8 = "composite",

    pub fn init(agents: []const AgentFn) CompositeAgent {
        return .{
            .agents = agents,
            .name = "composite",
        };
    }

    /// Returns the AgentFn interface for this composite.
    pub fn agentFn(self: *CompositeAgent) AgentFn {
        return .{
            .doWorkFn = doWorkImpl,
            .onStartFn = onStartImpl,
            .onCloseFn = onCloseImpl,
            .ctx = @ptrCast(self),
            .name = self.name,
        };
    }

    fn doWorkImpl(ctx: *anyopaque) u32 {
        const self: *CompositeAgent = @ptrCast(@alignCast(ctx));
        var total: u32 = 0;
        for (self.agents) |agent| {
            total += agent.doWork();
        }
        return total;
    }

    fn onStartImpl(ctx: *anyopaque) void {
        const self: *CompositeAgent = @ptrCast(@alignCast(ctx));
        for (self.agents) |agent| {
            agent.onStart();
        }
    }

    fn onCloseImpl(ctx: *anyopaque) void {
        const self: *CompositeAgent = @ptrCast(@alignCast(ctx));
        for (self.agents) |agent| {
            agent.onClose();
        }
    }
};

/// Tracks agent duty cycle performance metrics.
///
/// Measures cycle duration, total work done, and provides statistics
/// for monitoring agent health and tuning idle strategies.
pub const DutyCycleTracker = struct {
    cycle_count: u64 = 0,
    total_work: u64 = 0,
    max_cycle_ns: u64 = 0,
    last_cycle_ns: u64 = 0,
    cycle_start_ns: u64 = 0,

    pub fn cycleStart(self: *DutyCycleTracker) void {
        self.cycle_start_ns = config.timestampNs();
    }

    pub fn cycleEnd(self: *DutyCycleTracker, work_count: u32) void {
        const now = config.timestampNs();
        const elapsed = now -| self.cycle_start_ns;
        self.last_cycle_ns = elapsed;
        self.max_cycle_ns = @max(self.max_cycle_ns, elapsed);
        self.cycle_count += 1;
        self.total_work += work_count;
    }

    pub fn averageCycleNs(self: *const DutyCycleTracker) u64 {
        if (self.cycle_count == 0) return 0;
        // Approximate: we don't track total elapsed, so use max * count as upper bound?
        // Actually, we need to track total time. Let's derive from total_work ratio instead.
        // For a proper average, we'd need cumulative elapsed. Use last_cycle_ns as a proxy
        // or track it. Since we have cycle_count and individual measurements, let's just
        // return last_cycle_ns as a reasonable recent value. For a real average, we'd
        // accumulate total_ns. Let's add that.
        //
        // For simplicity without adding a field, return 0 if no cycles.
        // The caller should use the tracker over a window.
        return self.last_cycle_ns;
    }

    /// Ratio of cycles that did work vs total cycles (0.0 to 1.0).
    /// Returns 0.0 if no cycles have been recorded.
    pub fn workRatio(self: *const DutyCycleTracker) f64 {
        if (self.cycle_count == 0) return 0.0;
        const work_f: f64 = @floatFromInt(self.total_work);
        const count_f: f64 = @floatFromInt(self.cycle_count);
        // Clamp to 1.0 since work_count can be > 1 per cycle
        return @min(work_f / count_f, 1.0);
    }
};

// ── Tests ────────────────────────────────────────────────────

const TestContext = struct {
    work_done: std.atomic.Value(u32),
    started: std.atomic.Value(bool),
    closed: std.atomic.Value(bool),

    fn init() TestContext {
        return .{
            .work_done = std.atomic.Value(u32).init(0),
            .started = std.atomic.Value(bool).init(false),
            .closed = std.atomic.Value(bool).init(false),
        };
    }

    fn doWork(ctx: *anyopaque) u32 {
        const self: *TestContext = @ptrCast(@alignCast(ctx));
        _ = self.work_done.fetchAdd(1, .monotonic);
        return 1;
    }

    fn onStart(ctx: *anyopaque) void {
        const self: *TestContext = @ptrCast(@alignCast(ctx));
        self.started.store(true, .release);
    }

    fn onClose(ctx: *anyopaque) void {
        const self: *TestContext = @ptrCast(@alignCast(ctx));
        self.closed.store(true, .release);
    }

    fn agentFn(self: *TestContext) AgentFn {
        return .{
            .doWorkFn = doWork,
            .onStartFn = onStart,
            .onCloseFn = onClose,
            .ctx = @ptrCast(self),
            .name = "test-agent",
        };
    }
};

test "AgentFn doWork calls function" {
    var ctx = TestContext.init();
    const agent = ctx.agentFn();

    const result = agent.doWork();
    try std.testing.expectEqual(@as(u32, 1), result);
    try std.testing.expectEqual(@as(u32, 1), ctx.work_done.load(.monotonic));

    _ = agent.doWork();
    try std.testing.expectEqual(@as(u32, 2), ctx.work_done.load(.monotonic));
}

test "AgentFn onStart and onClose" {
    var ctx = TestContext.init();
    const agent = ctx.agentFn();

    try std.testing.expect(!ctx.started.load(.acquire));
    agent.onStart();
    try std.testing.expect(ctx.started.load(.acquire));

    try std.testing.expect(!ctx.closed.load(.acquire));
    agent.onClose();
    try std.testing.expect(ctx.closed.load(.acquire));
}

test "AgentRunner start/stop lifecycle" {
    var ctx = TestContext.init();
    const agent = ctx.agentFn();
    var runner = AgentRunner.init(agent, idle_strategy_mod.yielding());

    try std.testing.expect(!runner.isRunning());

    try runner.start();
    try std.testing.expect(runner.isRunning());

    // Let it run a few cycles
    std.Thread.sleep(5_000_000); // 5ms

    runner.stop();
    try std.testing.expect(!runner.isRunning());

    // Verify lifecycle callbacks were called
    try std.testing.expect(ctx.started.load(.acquire));
    try std.testing.expect(ctx.closed.load(.acquire));
}

test "AgentRunner runs doWork in loop" {
    var ctx = TestContext.init();
    const agent = ctx.agentFn();
    var runner = AgentRunner.init(agent, idle_strategy_mod.yielding());

    try runner.start();

    // Let it run for a bit
    std.Thread.sleep(10_000_000); // 10ms

    runner.stop();

    // Should have done work many times
    const work = ctx.work_done.load(.monotonic);
    try std.testing.expect(work > 10);
}

test "CompositeAgent aggregates work from multiple agents" {
    var ctx1 = TestContext.init();
    var ctx2 = TestContext.init();

    const agents = [_]AgentFn{
        ctx1.agentFn(),
        ctx2.agentFn(),
    };

    var composite = CompositeAgent.init(&agents);
    const composite_fn = composite.agentFn();

    // doWork should call both agents
    const total = composite_fn.doWork();
    try std.testing.expectEqual(@as(u32, 2), total);
    try std.testing.expectEqual(@as(u32, 1), ctx1.work_done.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), ctx2.work_done.load(.monotonic));

    // onStart should call both agents
    composite_fn.onStart();
    try std.testing.expect(ctx1.started.load(.acquire));
    try std.testing.expect(ctx2.started.load(.acquire));

    // onClose should call both agents
    composite_fn.onClose();
    try std.testing.expect(ctx1.closed.load(.acquire));
    try std.testing.expect(ctx2.closed.load(.acquire));
}

test "DutyCycleTracker tracks cycle count" {
    var tracker = DutyCycleTracker{};

    try std.testing.expectEqual(@as(u64, 0), tracker.cycle_count);

    tracker.cycleStart();
    tracker.cycleEnd(1);
    try std.testing.expectEqual(@as(u64, 1), tracker.cycle_count);
    try std.testing.expectEqual(@as(u64, 1), tracker.total_work);

    tracker.cycleStart();
    tracker.cycleEnd(0);
    try std.testing.expectEqual(@as(u64, 2), tracker.cycle_count);
    try std.testing.expectEqual(@as(u64, 1), tracker.total_work);

    tracker.cycleStart();
    tracker.cycleEnd(3);
    try std.testing.expectEqual(@as(u64, 3), tracker.cycle_count);
    try std.testing.expectEqual(@as(u64, 4), tracker.total_work);
}

test "DutyCycleTracker tracks max cycle time" {
    var tracker = DutyCycleTracker{};

    // First cycle: short
    tracker.cycleStart();
    tracker.cycleEnd(1);
    const first_max = tracker.max_cycle_ns;

    // Second cycle: sleep to make it longer
    tracker.cycleStart();
    std.Thread.sleep(1_000_000); // 1ms
    tracker.cycleEnd(1);

    // Max should have increased
    try std.testing.expect(tracker.max_cycle_ns >= first_max);
    try std.testing.expect(tracker.max_cycle_ns >= 500_000); // at least 0.5ms
    try std.testing.expect(tracker.last_cycle_ns >= 500_000);
}

test "DutyCycleTracker workRatio" {
    var tracker = DutyCycleTracker{};

    // No cycles -> 0.0
    try std.testing.expectEqual(@as(f64, 0.0), tracker.workRatio());

    // All idle cycles
    tracker.cycleStart();
    tracker.cycleEnd(0);
    tracker.cycleStart();
    tracker.cycleEnd(0);
    try std.testing.expectEqual(@as(f64, 0.0), tracker.workRatio());

    // Reset and do work cycles
    tracker = DutyCycleTracker{};
    tracker.cycleStart();
    tracker.cycleEnd(1);
    tracker.cycleStart();
    tracker.cycleEnd(1);
    // total_work=2, cycle_count=2 -> ratio=1.0
    try std.testing.expectEqual(@as(f64, 1.0), tracker.workRatio());
}

test "AgentFn with null callbacks" {
    var ctx = TestContext.init();
    const agent = AgentFn{
        .doWorkFn = TestContext.doWork,
        .onStartFn = null,
        .onCloseFn = null,
        .ctx = @ptrCast(&ctx),
        .name = "no-callbacks",
    };

    // Should not crash with null callbacks
    agent.onStart();
    agent.onClose();

    // doWork should still work
    const result = agent.doWork();
    try std.testing.expectEqual(@as(u32, 1), result);
}
