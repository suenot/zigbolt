const std = @import("std");
const idle_strategy_mod = @import("idle_strategy.zig");
const config = @import("../platform/config.zig");

/// Aeron-style Agent interface — a composable unit of work with lifecycle management.
///
/// Agents are function-pointer based to avoid Zig's comptime interface limitations
/// and allow runtime composition (e.g., CompositeAgent combining multiple agents).
pub const AgentFn = struct {
    /// Returns amount of work done (0 = idle), or an error. Errors are
    /// counted by `AgentRunner` (`error_count`) and treated as an idle
    /// cycle; they do not stop the run loop.
    doWorkFn: *const fn (ctx: *anyopaque) anyerror!u32,
    /// Called on agent start.
    onStartFn: ?*const fn (ctx: *anyopaque) void = null,
    /// Called on agent close.
    onCloseFn: ?*const fn (ctx: *anyopaque) void = null,
    /// Context pointer.
    ctx: *anyopaque,
    /// Agent name for debugging.
    name: []const u8 = "unnamed",

    pub fn doWork(self: *const AgentFn) anyerror!u32 {
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
    /// Returns `error.AlreadyStarted` if the runner already owns a thread
    /// (a second start would orphan the first one).
    pub fn start(self: *AgentRunner) !void {
        if (self.thread != null) return error.AlreadyStarted;
        // The run loop gates on `running`, so it must be set before the
        // thread can observe it; a failed spawn must not leave the runner
        // claiming to be running.
        self.running.store(true, .release);
        errdefer self.running.store(false, .release); // kcov-skip: fires only if Thread.spawn fails — not injectable in-process
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
            const work_count = self.agent.doWork() catch blk: {
                _ = self.error_count.fetchAdd(1, .monotonic);
                break :blk 0;
            };
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

    fn doWorkImpl(ctx: *anyopaque) anyerror!u32 {
        const self: *CompositeAgent = @ptrCast(@alignCast(ctx));
        var total: u32 = 0;
        for (self.agents) |agent| {
            total += try agent.doWork();
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
///
/// Timing uses the monotonic clock (`config.monotonicNs`) so NTP slews/
/// steps of the wall clock cannot corrupt the duty-cycle statistics.
pub const DutyCycleTracker = struct {
    cycle_count: u64 = 0,
    total_work: u64 = 0,
    total_cycle_ns: u64 = 0,
    max_cycle_ns: u64 = 0,
    last_cycle_ns: u64 = 0,
    cycle_start_ns: u64 = 0,

    pub fn cycleStart(self: *DutyCycleTracker) void {
        self.cycle_start_ns = config.monotonicNs();
    }

    pub fn cycleEnd(self: *DutyCycleTracker, work_count: u32) void {
        const now = config.monotonicNs();
        const elapsed = now -| self.cycle_start_ns;
        self.last_cycle_ns = elapsed;
        self.total_cycle_ns += elapsed;
        self.max_cycle_ns = @max(self.max_cycle_ns, elapsed);
        self.cycle_count += 1;
        self.total_work += work_count;
    }

    /// True arithmetic mean of all recorded cycle durations.
    /// Returns 0 if no cycles have been recorded.
    pub fn averageCycleNs(self: *const DutyCycleTracker) u64 {
        if (self.cycle_count == 0) return 0;
        return self.total_cycle_ns / self.cycle_count;
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

    fn doWork(ctx: *anyopaque) anyerror!u32 {
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

    const result = try agent.doWork();
    try std.testing.expectEqual(@as(u32, 1), result);
    try std.testing.expectEqual(@as(u32, 1), ctx.work_done.load(.monotonic));

    _ = try agent.doWork();
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
    try std.testing.expect(work > 10); // kcov-skip: hit record oscillates between builds; the test runs and passes
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
    const total = try composite_fn.doWork();
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
    const result = try agent.doWork();
    try std.testing.expectEqual(@as(u32, 1), result);
}

test "AgentRunner rejects double start" {
    var ctx = TestContext.init();
    const agent = ctx.agentFn();
    var runner = AgentRunner.init(agent, idle_strategy_mod.yielding());

    try runner.start();
    defer runner.stop();

    // A second start must not orphan the running thread.
    try std.testing.expectError(error.AlreadyStarted, runner.start());
    try std.testing.expect(runner.isRunning());
}

test "AgentRunner counts doWork errors and stops cleanly" {
    const FailingContext = struct {
        fn doWork(_: *anyopaque) anyerror!u32 {
            return error.WorkFailed; // kcov-skip: test agent body; runs on the runner thread (error count asserted); hit record oscillates between builds; the test runs and passes
        }
    };

    var dummy: u8 = 0;
    const agent = AgentFn{
        .doWorkFn = FailingContext.doWork,
        .ctx = @ptrCast(&dummy),
        .name = "failing-agent",
    };
    var runner = AgentRunner.init(agent, idle_strategy_mod.yielding());

    try std.testing.expectEqual(@as(u64, 0), runner.errorCount());
    try runner.start();

    // Wait (bounded) until the loop has recorded at least one error.
    var waited: usize = 0;
    while (runner.errorCount() == 0 and waited < 1000) : (waited += 1) {
        std.Thread.sleep(1_000_000); // 1ms
    }

    runner.stop();

    // Errors were counted, the loop kept going, and the runner shut down
    // into a consistent state.
    try std.testing.expect(runner.errorCount() >= 1);
    try std.testing.expect(!runner.isRunning());
    try std.testing.expect(runner.thread == null);
}

test "DutyCycleTracker averageCycleNs is the true arithmetic mean" {
    // Deterministic: drive the accumulated state directly.
    var tracker = DutyCycleTracker{};
    try std.testing.expectEqual(@as(u64, 0), tracker.averageCycleNs());

    tracker.cycle_count = 4;
    tracker.total_cycle_ns = 100 + 200 + 300 + 400;
    tracker.last_cycle_ns = 400;
    try std.testing.expectEqual(@as(u64, 250), tracker.averageCycleNs());
    // Regression for the old bug: the average is NOT just the last cycle.
    try std.testing.expect(tracker.averageCycleNs() != tracker.last_cycle_ns);
}

test "DutyCycleTracker accumulates real cycles into the average" {
    var tracker = DutyCycleTracker{};

    // Two fast cycles and one >= 1ms cycle.
    tracker.cycleStart();
    tracker.cycleEnd(1);
    tracker.cycleStart();
    tracker.cycleEnd(0);
    tracker.cycleStart();
    std.Thread.sleep(1_000_000); // 1ms
    tracker.cycleEnd(1);

    try std.testing.expectEqual(@as(u64, 3), tracker.cycle_count);
    // Mean of 3 cycles, one of which took >= 1ms: at least ~1ms/3.
    try std.testing.expect(tracker.averageCycleNs() >= 300_000);
    // Mean is consistent with the accumulated total and bounded by max.
    try std.testing.expectEqual(tracker.total_cycle_ns / 3, tracker.averageCycleNs());
    try std.testing.expect(tracker.averageCycleNs() <= tracker.max_cycle_ns);
}

test "DutyCycleTracker accumulates cycle statistics" {
    var tracker = DutyCycleTracker{};

    tracker.cycleStart();
    tracker.cycleEnd(3);
    tracker.cycleStart();
    tracker.cycleEnd(0);

    try std.testing.expectEqual(@as(u64, 2), tracker.cycle_count);
    try std.testing.expectEqual(@as(u64, 3), tracker.total_work);
    try std.testing.expect(tracker.max_cycle_ns >= tracker.last_cycle_ns);
    try std.testing.expect(tracker.averageCycleNs() <= tracker.max_cycle_ns);
    try std.testing.expect(tracker.workRatio() <= 1.0);
}
