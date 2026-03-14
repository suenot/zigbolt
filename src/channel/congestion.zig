const std = @import("std");

// ── RTT Estimator ──────────────────────────────────────────

/// RFC 6298 RTT estimator using Exponentially Weighted Moving Average.
///
/// Maintains smoothed RTT, RTT variance, and computes retransmit timeout.
/// All values are in nanoseconds. Alpha = 1/8, Beta = 1/4 per RFC 6298.
pub const RttEstimator = struct {
    /// Smoothed RTT in nanoseconds (EWMA).
    srtt_ns: u64,
    /// RTT variance in nanoseconds.
    rttvar_ns: u64,
    /// Retransmit timeout in nanoseconds.
    rto_ns: u64,
    /// Minimum RTT observed.
    min_rtt_ns: u64,
    /// Has at least one sample been recorded?
    initialized: bool,

    /// Minimum RTO: 1 ms.
    const min_rto_ns: u64 = 1_000_000;
    /// Maximum RTO: 60 seconds.
    const max_rto_ns: u64 = 60_000_000_000;
    /// Clock granularity G for RTO calculation: 1 us.
    const granularity_ns: u64 = 1_000;

    pub fn init() RttEstimator {
        return .{
            .srtt_ns = 0,
            .rttvar_ns = 0,
            .rto_ns = 1_000_000_000, // 1 second initial RTO per RFC 6298
            .min_rtt_ns = std.math.maxInt(u64),
            .initialized = false,
        };
    }

    /// Record a new RTT sample.
    ///
    /// On the first sample:
    ///   SRTT = R
    ///   RTTVAR = R / 2
    ///
    /// On subsequent samples (RFC 6298):
    ///   RTTVAR = (3/4) * RTTVAR + (1/4) * |SRTT - R|
    ///   SRTT   = (7/8) * SRTT   + (1/8) * R
    ///   RTO    = SRTT + max(G, 4 * RTTVAR)
    pub fn update(self: *RttEstimator, rtt_ns: u64) void {
        self.min_rtt_ns = @min(self.min_rtt_ns, rtt_ns);

        if (!self.initialized) {
            self.srtt_ns = rtt_ns;
            self.rttvar_ns = rtt_ns / 2;
            self.initialized = true;
        } else {
            // |SRTT - R|
            const diff = if (self.srtt_ns > rtt_ns)
                self.srtt_ns - rtt_ns
            else
                rtt_ns - self.srtt_ns;

            // RTTVAR = (3/4) * RTTVAR + (1/4) * |SRTT - R|
            self.rttvar_ns = (self.rttvar_ns * 3 / 4) + (diff / 4);

            // SRTT = (7/8) * SRTT + (1/8) * R
            self.srtt_ns = (self.srtt_ns * 7 / 8) + (rtt_ns / 8);
        }

        // RTO = SRTT + max(G, 4 * RTTVAR)
        const k_rttvar = self.rttvar_ns * 4;
        self.rto_ns = self.srtt_ns + @max(granularity_ns, k_rttvar);

        // Clamp RTO
        self.rto_ns = @max(self.rto_ns, min_rto_ns);
        self.rto_ns = @min(self.rto_ns, max_rto_ns);
    }

    /// Get current retransmit timeout in nanoseconds.
    pub fn retransmitTimeout(self: *const RttEstimator) u64 {
        return self.rto_ns;
    }

    /// Get smoothed RTT in nanoseconds.
    pub fn smoothedRtt(self: *const RttEstimator) u64 {
        return self.srtt_ns;
    }
};

// ── Congestion Config ──────────────────────────────────────

pub const CongestionConfig = struct {
    /// Initial congestion window (bytes).
    initial_window: u64 = 64 * 1024, // 64 KB
    /// Maximum window (bytes).
    max_window: u64 = 16 * 1024 * 1024, // 16 MB
    /// Minimum window (bytes).
    min_window: u64 = 4 * 1024, // 4 KB
    /// Maximum segment size.
    mss: u32 = 1460,
    /// Slow start threshold.
    initial_ssthresh: u64 = 1024 * 1024, // 1 MB
    /// RTT smoothing factor (alpha for EWMA, in 1/256 units).
    rtt_alpha: u32 = 32, // 1/8
};

// ── Congestion Control ─────────────────────────────────────

/// AIMD (Additive Increase / Multiplicative Decrease) congestion control.
///
/// Implements TCP-like slow start and congestion avoidance phases:
/// - Slow start: cwnd grows by MSS per ACK (exponential growth).
/// - Congestion avoidance: cwnd grows by MSS*MSS/cwnd per ACK (linear).
/// - On loss: ssthresh = cwnd/2, cwnd = ssthresh (multiplicative decrease).
/// - On timeout: ssthresh = cwnd/2, cwnd = min_window, re-enter slow start.
pub const CongestionControl = struct {
    /// Congestion window in bytes.
    cwnd: u64,
    /// Slow-start threshold.
    ssthresh: u64,
    /// Maximum segment size.
    mss: u32,
    /// Maximum window size.
    max_window: u64,
    /// Minimum window size (never go below).
    min_window: u64,
    /// Are we in slow start?
    in_slow_start: bool,
    /// RTT estimation (EWMA).
    rtt_estimator: RttEstimator,
    /// Bytes acknowledged since last window increase.
    bytes_acked_since_increase: u64,
    /// Loss events in current epoch.
    loss_count: u32,
    /// Total packets sent.
    total_sent: u64,
    /// Total retransmits.
    total_retransmits: u64,
    /// Bytes currently in flight (sent but not yet acknowledged).
    bytes_in_flight: u64,

    pub fn init(cfg: CongestionConfig) CongestionControl {
        return .{
            .cwnd = cfg.initial_window,
            .ssthresh = cfg.initial_ssthresh,
            .mss = cfg.mss,
            .max_window = cfg.max_window,
            .min_window = cfg.min_window,
            .in_slow_start = true,
            .rtt_estimator = RttEstimator.init(),
            .bytes_acked_since_increase = 0,
            .loss_count = 0,
            .total_sent = 0,
            .total_retransmits = 0,
            .bytes_in_flight = 0,
        };
    }

    /// Called when data is acknowledged.
    ///
    /// In slow start: cwnd += mss per ACK (exponential growth).
    /// In congestion avoidance: cwnd += mss * mss / cwnd (linear, AIMD additive increase).
    /// Transitions from slow start to congestion avoidance when cwnd >= ssthresh.
    pub fn onAck(self: *CongestionControl, bytes_acked: u64) void {
        // Reduce in-flight counter.
        if (bytes_acked <= self.bytes_in_flight) {
            self.bytes_in_flight -= bytes_acked;
        } else {
            self.bytes_in_flight = 0;
        }

        if (self.in_slow_start) {
            // Exponential growth: increase by MSS for each ACK.
            self.cwnd += self.mss;

            // Check if we should transition to congestion avoidance.
            if (self.cwnd >= self.ssthresh) {
                self.in_slow_start = false;
            }
        } else {
            // Congestion avoidance: linear growth.
            // Increase cwnd by mss * mss / cwnd (approximately 1 MSS per RTT).
            const mss_u64: u64 = self.mss;
            const increment = (mss_u64 * mss_u64) / self.cwnd;
            // Ensure at least 1 byte of growth to avoid stalling.
            self.cwnd += @max(increment, 1);
        }

        // Clamp to maximum window.
        self.cwnd = @min(self.cwnd, self.max_window);

        self.bytes_acked_since_increase += bytes_acked;
    }

    /// Called on packet loss detection (e.g., triple duplicate ACK or NAK).
    ///
    /// Multiplicative decrease:
    ///   ssthresh = max(cwnd / 2, min_window)
    ///   cwnd = ssthresh
    pub fn onLoss(self: *CongestionControl) void {
        self.ssthresh = @max(self.cwnd / 2, self.min_window);
        self.cwnd = self.ssthresh;
        self.in_slow_start = false;
        self.loss_count += 1;
        self.total_retransmits += 1;
    }

    /// Called on retransmit timeout — more aggressive than onLoss.
    ///
    /// Reset cwnd to minimum and re-enter slow start.
    pub fn onTimeout(self: *CongestionControl) void {
        self.ssthresh = @max(self.cwnd / 2, self.min_window);
        self.cwnd = self.min_window;
        self.in_slow_start = true;
        self.total_retransmits += 1;
    }

    /// Check whether the congestion window allows sending `bytes`.
    pub fn canSend(self: *const CongestionControl, bytes: u64) bool {
        return self.bytes_in_flight + bytes <= self.cwnd;
    }

    /// Record bytes sent (increases bytes in flight).
    pub fn onSend(self: *CongestionControl, bytes: u64) void {
        self.bytes_in_flight += bytes;
        self.total_sent += 1;
    }

    /// Return the number of bytes available in the congestion window.
    pub fn availableWindow(self: *const CongestionControl) u64 {
        if (self.bytes_in_flight >= self.cwnd) return 0;
        return self.cwnd - self.bytes_in_flight;
    }

    /// Reset to initial state.
    pub fn reset(self: *CongestionControl) void {
        const cfg = CongestionConfig{
            .initial_window = self.cwnd,
            .max_window = self.max_window,
            .min_window = self.min_window,
            .mss = self.mss,
            .initial_ssthresh = self.ssthresh,
        };
        _ = cfg;
        // Preserve config but reset dynamic state.
        self.cwnd = self.max_window; // Will be overridden below.
        self.* = init(.{
            .initial_window = 64 * 1024,
            .max_window = self.max_window,
            .min_window = self.min_window,
            .mss = self.mss,
            .initial_ssthresh = 1024 * 1024,
        });
    }
};

// ── NAK Controller ─────────────────────────────────────────

/// Controls NAK (negative acknowledgement) timing and suppression.
///
/// Uses exponential backoff to avoid NAK storms. Tracks per-gap state
/// so that retransmit requests are rate-limited appropriately.
pub const NakController = struct {
    /// Base NAK delay (nanoseconds).
    base_delay_ns: u64,
    /// Current backoff multiplier (power of 2 exponent).
    backoff_multiplier: u32,
    /// Maximum backoff multiplier.
    max_backoff: u32,
    /// Maximum retransmit attempts per sequence.
    max_retransmits: u32,
    /// Timestamp of last NAK sent (nanoseconds).
    last_nak_time_ns: u64,
    /// Number of NAKs sent for current gap.
    nak_count: u32,

    pub const NakConfig = struct {
        base_delay_ns: u64 = 1_000_000, // 1 ms
        max_backoff: u32 = 5, // max 2^5 = 32x delay
        max_retransmits: u32 = 10,
    };

    pub fn init(nak_config: NakConfig) NakController {
        return .{
            .base_delay_ns = nak_config.base_delay_ns,
            .backoff_multiplier = 0,
            .max_backoff = nak_config.max_backoff,
            .max_retransmits = nak_config.max_retransmits,
            .last_nak_time_ns = 0,
            .nak_count = 0,
        };
    }

    /// Should we send a NAK now?
    ///
    /// Returns true if enough time has elapsed since the last NAK,
    /// considering the current backoff delay.
    pub fn shouldSendNak(self: *const NakController, now_ns: u64) bool {
        if (self.nak_count >= self.max_retransmits) return false;
        if (self.last_nak_time_ns == 0) return true; // Never sent one.
        return now_ns >= self.last_nak_time_ns + self.currentDelay();
    }

    /// Record that a NAK was sent.
    pub fn onNakSent(self: *NakController, now_ns: u64) void {
        self.last_nak_time_ns = now_ns;
        self.nak_count += 1;
        // Increase backoff (capped at max).
        if (self.backoff_multiplier < self.max_backoff) {
            self.backoff_multiplier += 1;
        }
    }

    /// Record that the gap was filled (reset state for reuse).
    pub fn onGapFilled(self: *NakController) void {
        self.backoff_multiplier = 0;
        self.last_nak_time_ns = 0;
        self.nak_count = 0;
    }

    /// Has the maximum number of retransmits been exceeded?
    pub fn isExhausted(self: *const NakController) bool {
        return self.nak_count >= self.max_retransmits;
    }

    /// Get current delay with exponential backoff.
    ///
    /// delay = base_delay_ns * 2^backoff_multiplier
    pub fn currentDelay(self: *const NakController) u64 {
        return self.base_delay_ns << @intCast(self.backoff_multiplier);
    }
};

// ── Tests ──────────────────────────────────────────────────

test "AIMD slow start exponential growth" {
    var cc = CongestionControl.init(.{
        .initial_window = 4 * 1460, // 4 segments
        .initial_ssthresh = 100 * 1460, // high threshold so we stay in slow start
        .mss = 1460,
        .min_window = 1460,
        .max_window = 1024 * 1024,
    });

    try std.testing.expect(cc.in_slow_start);
    const initial_cwnd = cc.cwnd;

    // Each ACK in slow start adds 1 MSS.
    cc.onAck(1460);
    try std.testing.expectEqual(initial_cwnd + 1460, cc.cwnd);
    try std.testing.expect(cc.in_slow_start);

    cc.onAck(1460);
    try std.testing.expectEqual(initial_cwnd + 2 * 1460, cc.cwnd);

    cc.onAck(1460);
    try std.testing.expectEqual(initial_cwnd + 3 * 1460, cc.cwnd);
}

test "AIMD congestion avoidance linear growth" {
    var cc = CongestionControl.init(.{
        .initial_window = 64 * 1024,
        .initial_ssthresh = 32 * 1024, // below initial window => start in CA
        .mss = 1460,
        .min_window = 4096,
        .max_window = 16 * 1024 * 1024,
    });

    // Since initial_window > initial_ssthresh, first onAck transitions to CA.
    // Force into CA.
    cc.in_slow_start = false;

    const cwnd_before = cc.cwnd;

    // In CA: increment = mss * mss / cwnd.
    // 1460 * 1460 / 65536 = 2131600 / 65536 = 32 (integer division).
    cc.onAck(1460);
    const expected_increment = (@as(u64, 1460) * 1460) / cwnd_before;
    try std.testing.expectEqual(cwnd_before + @max(expected_increment, 1), cc.cwnd);

    // Growth should be much smaller than MSS (linear, not exponential).
    try std.testing.expect(cc.cwnd - cwnd_before < 1460);
}

test "AIMD window decrease on loss" {
    var cc = CongestionControl.init(.{
        .initial_window = 64 * 1024,
        .initial_ssthresh = 1024 * 1024,
        .mss = 1460,
        .min_window = 4096,
        .max_window = 16 * 1024 * 1024,
    });

    const cwnd_before_loss = cc.cwnd;

    cc.onLoss();

    // ssthresh = max(cwnd/2, min_window)
    try std.testing.expectEqual(@max(cwnd_before_loss / 2, 4096), cc.ssthresh);
    // cwnd = ssthresh
    try std.testing.expectEqual(cc.ssthresh, cc.cwnd);
    // No longer in slow start.
    try std.testing.expect(!cc.in_slow_start);
    // Loss counter incremented.
    try std.testing.expectEqual(@as(u32, 1), cc.loss_count);
}

test "AIMD window reset on timeout" {
    var cc = CongestionControl.init(.{
        .initial_window = 64 * 1024,
        .initial_ssthresh = 1024 * 1024,
        .mss = 1460,
        .min_window = 4096,
        .max_window = 16 * 1024 * 1024,
    });

    const cwnd_before = cc.cwnd;

    cc.onTimeout();

    // ssthresh = max(cwnd/2, min_window)
    try std.testing.expectEqual(@max(cwnd_before / 2, 4096), cc.ssthresh);
    // cwnd reset to minimum.
    try std.testing.expectEqual(@as(u64, 4096), cc.cwnd);
    // Re-enter slow start.
    try std.testing.expect(cc.in_slow_start);
}

test "canSend respects window" {
    var cc = CongestionControl.init(.{
        .initial_window = 4096,
        .initial_ssthresh = 1024 * 1024,
        .mss = 1460,
        .min_window = 1460,
        .max_window = 16 * 1024 * 1024,
    });

    // Window is 4096, nothing in flight => can send up to 4096.
    try std.testing.expect(cc.canSend(4096));
    try std.testing.expect(!cc.canSend(4097));

    // Send 2000 bytes.
    cc.onSend(2000);
    try std.testing.expectEqual(@as(u64, 2096), cc.availableWindow());
    try std.testing.expect(cc.canSend(2096));
    try std.testing.expect(!cc.canSend(2097));

    // Send more to fill window.
    cc.onSend(2096);
    try std.testing.expectEqual(@as(u64, 0), cc.availableWindow());
    try std.testing.expect(!cc.canSend(1));

    // ACK frees space.
    cc.onAck(1000);
    try std.testing.expect(cc.canSend(1000));
}

test "RTT estimator convergence" {
    var rtt = RttEstimator.init();
    try std.testing.expect(!rtt.initialized);

    // First sample: 10 ms.
    rtt.update(10_000_000);
    try std.testing.expect(rtt.initialized);
    try std.testing.expectEqual(@as(u64, 10_000_000), rtt.srtt_ns);
    try std.testing.expectEqual(@as(u64, 5_000_000), rtt.rttvar_ns);
    try std.testing.expectEqual(@as(u64, 10_000_000), rtt.min_rtt_ns);

    // Feed several samples of 10 ms — should converge to 10 ms.
    for (0..20) |_| {
        rtt.update(10_000_000);
    }
    // SRTT should be very close to 10 ms (within 5%).
    try std.testing.expect(rtt.srtt_ns >= 9_500_000);
    try std.testing.expect(rtt.srtt_ns <= 10_500_000);
}

test "RTT estimator with varying samples" {
    var rtt = RttEstimator.init();

    // Start with 10 ms.
    rtt.update(10_000_000);

    // Suddenly increase to 50 ms.
    rtt.update(50_000_000);

    // SRTT should move towards 50 ms but not jump there instantly.
    try std.testing.expect(rtt.srtt_ns > 10_000_000);
    try std.testing.expect(rtt.srtt_ns < 50_000_000);

    // RTTVAR should increase due to the variance.
    try std.testing.expect(rtt.rttvar_ns > 5_000_000);

    // RTO should be larger than SRTT to account for variance.
    try std.testing.expect(rtt.rto_ns > rtt.srtt_ns);

    // Min RTT should remain 10 ms.
    try std.testing.expectEqual(@as(u64, 10_000_000), rtt.min_rtt_ns);
}

test "NakController delay with backoff" {
    var nak = NakController.init(.{
        .base_delay_ns = 1_000_000,
        .max_backoff = 5,
        .max_retransmits = 10,
    });

    // Initial delay is base (2^0 = 1x).
    try std.testing.expectEqual(@as(u64, 1_000_000), nak.currentDelay());

    // Should send immediately (never sent before).
    try std.testing.expect(nak.shouldSendNak(0));

    // Send a NAK.
    nak.onNakSent(1_000_000);
    try std.testing.expectEqual(@as(u32, 1), nak.nak_count);
    // Backoff increased: delay = 1ms * 2^1 = 2ms.
    try std.testing.expectEqual(@as(u64, 2_000_000), nak.currentDelay());

    // Too early to send again.
    try std.testing.expect(!nak.shouldSendNak(2_000_000));
    // Enough time elapsed.
    try std.testing.expect(nak.shouldSendNak(3_000_001));

    // Send another NAK.
    nak.onNakSent(3_000_001);
    // delay = 1ms * 2^2 = 4ms.
    try std.testing.expectEqual(@as(u64, 4_000_000), nak.currentDelay());

    // Continue until max backoff.
    nak.onNakSent(10_000_000); // backoff 3 => 8ms
    nak.onNakSent(20_000_000); // backoff 4 => 16ms
    nak.onNakSent(40_000_000); // backoff 5 (max) => 32ms
    try std.testing.expectEqual(@as(u64, 32_000_000), nak.currentDelay());

    // Further NAKs should not increase beyond max.
    nak.onNakSent(80_000_000); // still 5 => 32ms
    try std.testing.expectEqual(@as(u64, 32_000_000), nak.currentDelay());
}

test "NakController exhaustion after max retries" {
    var nak = NakController.init(.{
        .base_delay_ns = 1_000_000,
        .max_backoff = 3,
        .max_retransmits = 3,
    });

    try std.testing.expect(!nak.isExhausted());

    nak.onNakSent(1_000_000);
    try std.testing.expect(!nak.isExhausted());

    nak.onNakSent(5_000_000);
    try std.testing.expect(!nak.isExhausted());

    nak.onNakSent(15_000_000);
    try std.testing.expect(nak.isExhausted());

    // Should no longer allow sending NAKs.
    try std.testing.expect(!nak.shouldSendNak(100_000_000));
}

test "NakController reset on gap filled" {
    var nak = NakController.init(.{
        .base_delay_ns = 1_000_000,
        .max_backoff = 5,
        .max_retransmits = 10,
    });

    // Send several NAKs to build up state.
    nak.onNakSent(1_000_000);
    nak.onNakSent(5_000_000);
    nak.onNakSent(15_000_000);
    try std.testing.expectEqual(@as(u32, 3), nak.nak_count);
    try std.testing.expect(nak.backoff_multiplier > 0);

    // Gap filled — reset.
    nak.onGapFilled();
    try std.testing.expectEqual(@as(u32, 0), nak.nak_count);
    try std.testing.expectEqual(@as(u32, 0), nak.backoff_multiplier);
    try std.testing.expectEqual(@as(u64, 0), nak.last_nak_time_ns);

    // Should be ready to send a NAK again immediately.
    try std.testing.expect(nak.shouldSendNak(20_000_000));
}

test "window never goes below minimum" {
    var cc = CongestionControl.init(.{
        .initial_window = 8192,
        .initial_ssthresh = 1024 * 1024,
        .mss = 1460,
        .min_window = 4096,
        .max_window = 16 * 1024 * 1024,
    });

    // Repeatedly trigger loss to drive cwnd down.
    for (0..20) |_| {
        cc.onLoss();
    }

    // cwnd should never go below min_window.
    try std.testing.expect(cc.cwnd >= 4096);
    try std.testing.expect(cc.ssthresh >= 4096);

    // Also test timeout path.
    cc.cwnd = 8192;
    for (0..20) |_| {
        cc.onTimeout();
    }
    try std.testing.expectEqual(@as(u64, 4096), cc.cwnd);
}

test "window never exceeds maximum" {
    var cc = CongestionControl.init(.{
        .initial_window = 1024 * 1024,
        .initial_ssthresh = 512 * 1024, // start in CA
        .mss = 1460,
        .min_window = 4096,
        .max_window = 2 * 1024 * 1024, // 2 MB max
    });

    cc.in_slow_start = true;
    cc.ssthresh = cc.max_window + 1; // Keep in slow start.

    // Pump many ACKs in slow start to try to exceed max.
    for (0..5000) |_| {
        cc.onAck(1460);
    }

    try std.testing.expect(cc.cwnd <= cc.max_window);
}
