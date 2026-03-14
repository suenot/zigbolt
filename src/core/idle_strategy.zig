const std = @import("std");

/// Aeron-style idle strategies for back-off when threads have no work.
///
/// Strategies range from lowest-latency/highest-CPU (BusySpin) to
/// highest-latency/lowest-CPU (Sleeping), with BackoffIdleStrategy
/// providing adaptive behavior that transitions between states.
pub const IdleStrategy = union(enum) {
    busy_spin: BusySpinIdleStrategy,
    yielding: YieldingIdleStrategy,
    sleeping: SleepingIdleStrategy,
    backoff: BackoffIdleStrategy,
    no_op: NoOpIdleStrategy,

    /// Called every duty cycle. work_count > 0 means work was done.
    pub fn idle(self: *IdleStrategy, work_count: u32) void {
        switch (self.*) {
            inline else => |*s| s.idle(work_count),
        }
    }

    /// Reset to most active state (after work was done).
    pub fn reset(self: *IdleStrategy) void {
        switch (self.*) {
            inline else => |*s| s.reset(),
        }
    }
};

// ── Strategy implementations ─────────────────────────────────

/// Pure CPU spin using hardware PAUSE/YIELD instruction.
/// Lowest latency, highest CPU usage. Use only when every nanosecond matters.
pub const BusySpinIdleStrategy = struct {
    pub fn idle(self: *BusySpinIdleStrategy, work_count: u32) void {
        _ = self;
        if (work_count == 0) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn reset(self: *BusySpinIdleStrategy) void {
        _ = self;
    }
};

/// Yields the thread to give other threads a chance to run.
/// Still CPU-bound but cooperative with the OS scheduler.
pub const YieldingIdleStrategy = struct {
    pub fn idle(self: *YieldingIdleStrategy, work_count: u32) void {
        _ = self;
        if (work_count == 0) {
            std.Thread.yield() catch {};
        }
    }

    pub fn reset(self: *YieldingIdleStrategy) void {
        _ = self;
    }
};

/// Sleeps for a configurable duration when idle.
/// Low CPU usage, higher latency. Good for background work.
pub const SleepingIdleStrategy = struct {
    sleep_ns: u64 = 1_000, // 1 microsecond default

    pub fn init(sleep_ns: u64) SleepingIdleStrategy {
        return .{ .sleep_ns = sleep_ns };
    }

    pub fn idle(self: *SleepingIdleStrategy, work_count: u32) void {
        if (work_count == 0) {
            std.Thread.sleep(self.sleep_ns);
        }
    }

    pub fn reset(self: *SleepingIdleStrategy) void {
        _ = self;
    }
};

/// Adaptive back-off idle strategy with state machine:
/// NOT_IDLE → SPINNING → YIELDING → PARKING
///
/// Starts with the lowest-latency approach (spin) and progressively
/// backs off to reduce CPU usage when no work is available.
/// The parking phase uses exponential back-off up to max_park_ns.
pub const BackoffIdleStrategy = struct {
    state: State = .not_idle,
    spin_count: u32 = 0,
    yield_count: u32 = 0,
    park_period_ns: u64 = 0,

    // Configuration
    max_spins: u32 = 10,
    max_yields: u32 = 5,
    min_park_ns: u64 = 1_000, // 1µs
    max_park_ns: u64 = 1_000_000, // 1ms

    pub const State = enum {
        not_idle,
        spinning,
        yielding,
        parking,
    };

    pub fn init(max_spins: u32, max_yields: u32, min_park_ns: u64, max_park_ns: u64) BackoffIdleStrategy {
        return .{
            .max_spins = max_spins,
            .max_yields = max_yields,
            .min_park_ns = min_park_ns,
            .max_park_ns = max_park_ns,
        };
    }

    pub fn idle(self: *BackoffIdleStrategy, work_count: u32) void {
        if (work_count > 0) {
            self.reset();
            return;
        }
        switch (self.state) {
            .not_idle => {
                self.state = .spinning;
                self.spin_count = 0;
            },
            .spinning => {
                std.atomic.spinLoopHint();
                self.spin_count += 1;
                if (self.spin_count >= self.max_spins) {
                    self.state = .yielding;
                    self.yield_count = 0;
                }
            },
            .yielding => {
                std.Thread.yield() catch {};
                self.yield_count += 1;
                if (self.yield_count >= self.max_yields) {
                    self.state = .parking;
                    self.park_period_ns = self.min_park_ns;
                }
            },
            .parking => {
                std.Thread.sleep(self.park_period_ns);
                // Exponential backoff capped at max
                self.park_period_ns = @min(self.park_period_ns * 2, self.max_park_ns);
            },
        }
    }

    pub fn reset(self: *BackoffIdleStrategy) void {
        self.state = .not_idle;
        self.spin_count = 0;
        self.yield_count = 0;
        self.park_period_ns = 0;
    }
};

/// Does nothing at all. Use when the loop body already has blocking operations.
pub const NoOpIdleStrategy = struct {
    pub fn idle(self: *NoOpIdleStrategy, work_count: u32) void {
        _ = self;
        _ = work_count;
    }

    pub fn reset(self: *NoOpIdleStrategy) void {
        _ = self;
    }
};

// ── Convenience constructors ─────────────────────────────────

pub fn busySpin() IdleStrategy {
    return .{ .busy_spin = .{} };
}

pub fn yielding() IdleStrategy {
    return .{ .yielding = .{} };
}

pub fn sleeping(sleep_ns: u64) IdleStrategy {
    return .{ .sleeping = .{ .sleep_ns = sleep_ns } };
}

pub fn backoff() IdleStrategy {
    return .{ .backoff = BackoffIdleStrategy.init(10, 5, 1_000, 1_000_000) };
}

pub fn noOp() IdleStrategy {
    return .{ .no_op = .{} };
}

// ── Tests ────────────────────────────────────────────────────

test "BusySpin idle doesn't crash" {
    var strategy = BusySpinIdleStrategy{};
    strategy.idle(0);
    strategy.idle(0);
    strategy.idle(1);
    strategy.reset();
}

test "Yielding idle doesn't crash" {
    var strategy = YieldingIdleStrategy{};
    strategy.idle(0);
    strategy.idle(0);
    strategy.idle(1);
    strategy.reset();
}

test "Sleeping idle sleeps approximately correct duration" {
    var strategy = SleepingIdleStrategy.init(1_000_000); // 1ms
    const start = std.time.nanoTimestamp();
    strategy.idle(0);
    const elapsed = std.time.nanoTimestamp() - start;
    // Should have slept at least ~0.5ms (allowing for timer granularity)
    try std.testing.expect(elapsed >= 500_000);
    // Should not have slept more than 50ms (generous upper bound)
    try std.testing.expect(elapsed < 50_000_000);
}

test "Backoff state transitions: not_idle -> spinning -> yielding -> parking" {
    var strategy = BackoffIdleStrategy.init(2, 2, 1_000, 1_000_000);

    // Start at not_idle
    try std.testing.expectEqual(BackoffIdleStrategy.State.not_idle, strategy.state);

    // First idle(0) transitions to spinning
    strategy.idle(0);
    try std.testing.expectEqual(BackoffIdleStrategy.State.spinning, strategy.state);

    // Spin twice to exhaust max_spins=2
    strategy.idle(0);
    try std.testing.expectEqual(BackoffIdleStrategy.State.spinning, strategy.state);
    strategy.idle(0);
    try std.testing.expectEqual(BackoffIdleStrategy.State.yielding, strategy.state);

    // Yield twice to exhaust max_yields=2
    strategy.idle(0);
    try std.testing.expectEqual(BackoffIdleStrategy.State.yielding, strategy.state);
    strategy.idle(0);
    try std.testing.expectEqual(BackoffIdleStrategy.State.parking, strategy.state);
}

test "Backoff reset returns to not_idle" {
    var strategy = BackoffIdleStrategy.init(2, 2, 1_000, 1_000_000);

    // Drive into spinning state
    strategy.idle(0);
    strategy.idle(0);
    try std.testing.expectEqual(BackoffIdleStrategy.State.spinning, strategy.state);

    // Reset
    strategy.reset();
    try std.testing.expectEqual(BackoffIdleStrategy.State.not_idle, strategy.state);
    try std.testing.expectEqual(@as(u32, 0), strategy.spin_count);
    try std.testing.expectEqual(@as(u32, 0), strategy.yield_count);
    try std.testing.expectEqual(@as(u64, 0), strategy.park_period_ns);
}

test "Backoff exponential park period growth" {
    var strategy = BackoffIdleStrategy.init(0, 0, 1_000, 1_000_000);

    // max_spins=0 and max_yields=0, so transitions fast:
    // idle(0) -> spinning, then immediately spinning->yielding->parking
    strategy.idle(0); // not_idle -> spinning
    strategy.idle(0); // spinning (0 >= 0) -> yielding
    strategy.idle(0); // yielding (0 >= 0) -> parking, park_period = 1000

    try std.testing.expectEqual(BackoffIdleStrategy.State.parking, strategy.state);
    try std.testing.expectEqual(@as(u64, 1_000), strategy.park_period_ns);

    // Park once: period doubles to 2000
    strategy.idle(0);
    try std.testing.expectEqual(@as(u64, 2_000), strategy.park_period_ns);

    // Park again: period doubles to 4000
    strategy.idle(0);
    try std.testing.expectEqual(@as(u64, 4_000), strategy.park_period_ns);
}

test "Backoff park period capped at max_park_ns" {
    var strategy = BackoffIdleStrategy.init(0, 0, 500_000, 1_000_000);

    // Drive to parking
    strategy.idle(0); // -> spinning
    strategy.idle(0); // -> yielding
    strategy.idle(0); // -> parking, period = 500_000

    try std.testing.expectEqual(@as(u64, 500_000), strategy.park_period_ns);

    // Park: doubles to 1_000_000 (== max)
    strategy.idle(0);
    try std.testing.expectEqual(@as(u64, 1_000_000), strategy.park_period_ns);

    // Park again: stays at 1_000_000 (capped)
    strategy.idle(0);
    try std.testing.expectEqual(@as(u64, 1_000_000), strategy.park_period_ns);
}

test "NoOp idle does nothing" {
    var strategy = NoOpIdleStrategy{};
    // Just verify it doesn't crash or block
    strategy.idle(0);
    strategy.idle(1);
    strategy.idle(100);
    strategy.reset();
}

test "IdleStrategy tagged union dispatch works" {
    // Test each variant through the tagged union interface
    var bs = busySpin();
    bs.idle(0);
    bs.idle(1);
    bs.reset();

    var ys = yielding();
    ys.idle(0);
    ys.idle(1);
    ys.reset();

    var sl = sleeping(1_000);
    sl.idle(1); // work_count > 0, should not sleep
    sl.reset();

    var bo = backoff();
    bo.idle(0);
    bo.idle(1); // should reset
    bo.reset();

    var no = noOp();
    no.idle(0);
    no.idle(1);
    no.reset();
}

test "Backoff work_count > 0 resets state" {
    var strategy = BackoffIdleStrategy.init(2, 2, 1_000, 1_000_000);

    // Drive into spinning
    strategy.idle(0);
    strategy.idle(0);
    try std.testing.expectEqual(BackoffIdleStrategy.State.spinning, strategy.state);

    // Work was done — should reset
    strategy.idle(1);
    try std.testing.expectEqual(BackoffIdleStrategy.State.not_idle, strategy.state);
}

test "Sleeping idle with work_count > 0 does not sleep" {
    var strategy = SleepingIdleStrategy.init(100_000_000); // 100ms — would be obvious if it slept
    const start = std.time.nanoTimestamp();
    strategy.idle(1); // work_count > 0
    const elapsed = std.time.nanoTimestamp() - start;
    // Should return almost immediately
    try std.testing.expect(elapsed < 10_000_000); // < 10ms
}
