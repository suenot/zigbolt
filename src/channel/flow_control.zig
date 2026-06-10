const std = @import("std");
const config = @import("../platform/config.zig");

// ── Receiver Status ────────────────────────────────────────

/// Status message sent by a receiver back to the sender.
///
/// Carries the receiver's current consumption position and window size,
/// enabling the sender to compute how far ahead it may publish. Modelled
/// after Aeron's Status Message (SM) frame.
pub const ReceiverStatus = struct {
    /// Session identifier (matches the publication session).
    session_id: i32,
    /// Stream identifier within the session.
    stream_id: i32,
    /// Term ID up to which the receiver has consumed.
    consumption_term_id: i32,
    /// Offset within the consumption term.
    consumption_term_offset: i32,
    /// Receiver's advertised window length (bytes).
    receiver_window_length: i32,
    /// Unique receiver identifier.
    receiver_id: i64,
    /// Timestamp when the status was generated (nanoseconds).
    timestamp_ns: u64,
};

// ── Flow Control Config ────────────────────────────────────

pub const FlowControlConfig = struct {
    /// Which strategy to use.
    strategy: StrategyType = .min,
    /// Remove a receiver if no status message is received within this period.
    receiver_timeout_ns: u64 = 5_000_000_000, // 5 seconds
    /// For tagged strategy: only receivers with this group tag limit the sender.
    required_group_tag: i64 = 0,
    /// Initial receiver window size (bytes).
    initial_receiver_window: i32 = 256 * 1024, // 256 KB

    pub const StrategyType = enum {
        min,
        max,
        tagged,
    };
};

// ── Position Helper ────────────────────────────────────────

/// Compute an absolute stream position from term-based addressing.
///
/// This mirrors Aeron's position calculation:
///   position = (term_id - initial_term_id) << position_bits_to_shift + term_offset
pub inline fn computePosition(
    term_id: i32,
    term_offset: i32,
    position_bits_to_shift: u8,
    initial_term_id: i32,
) i64 {
    const term_count: i64 = @as(i64, term_id) - @as(i64, initial_term_id);
    // Status-message fields are attacker-controlled: clamp the shift (an
    // i64 shift of >= 64 panics) and saturate so extreme term ids cannot
    // overflow into a panic; the resulting limit simply pins at i64 max/min.
    const shift: u6 = @intCast(@min(position_bits_to_shift, 63));
    return (term_count <<| shift) +| @as(i64, term_offset);
}

// ── Min Flow Control ───────────────────────────────────────

/// Minimum-of-all-receivers flow control (reliable multicast).
///
/// The sender limit is the minimum consumption position across all active
/// receivers. This guarantees no receiver is left behind — the sender
/// cannot advance beyond the slowest consumer.
///
/// Up to MAX_RECEIVERS are tracked in a fixed-size array (zero allocation
/// on the hot path). Receivers that stop sending status messages are
/// timed out and removed after `receiver_timeout_ns`.
pub const MinFlowControl = struct {
    receivers: [MAX_RECEIVERS]ReceiverState = [_]ReceiverState{.{}} ** MAX_RECEIVERS,
    receiver_count: u32 = 0,
    receiver_timeout_ns: u64 = 5_000_000_000,

    const MAX_RECEIVERS = 16;

    const ReceiverState = struct {
        receiver_id: i64 = 0,
        last_position: i64 = 0,
        last_seen_ns: u64 = 0,
        active: bool = false,
    };

    /// Process an incoming status message from a receiver.
    ///
    /// If the receiver is already tracked its position and timestamp are
    /// updated; otherwise a new slot is allocated. Returns the minimum
    /// position across all active receivers, which becomes the new sender
    /// limit.
    pub fn onStatusMessage(
        self: *MinFlowControl,
        status: ReceiverStatus,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: u8,
        now_ns: u64,
    ) i64 {
        const position = computePosition(
            status.consumption_term_id,
            status.consumption_term_offset,
            position_bits_to_shift,
            initial_term_id,
        );
        const window: i64 = @as(i64, status.receiver_window_length);
        // Saturating: position near i64 max plus a window must not overflow.
        const limit_position = position +| window;

        // Update existing receiver or add new one.
        var found = false;
        for (&self.receivers) |*r| {
            if (r.active and r.receiver_id == status.receiver_id) {
                r.last_position = limit_position;
                r.last_seen_ns = now_ns;
                found = true;
                break;
            }
        }

        if (!found) {
            // Try to add a new receiver; when the table is full, evict the
            // least-recently-seen entry instead of silently dropping the
            // newcomer. Otherwise 16 stale/spoofed receiver_ids would pin
            // the sender forever (receiver identity is NOT authenticated —
            // see module notes; full auth is out of scope here).
            var free_slot: ?*ReceiverState = null;
            var oldest: *ReceiverState = &self.receivers[0];
            for (&self.receivers) |*r| {
                if (!r.active) {
                    free_slot = r;
                    break;
                }
                if (r.last_seen_ns < oldest.last_seen_ns) oldest = r;
            }
            const slot = free_slot orelse oldest;
            slot.* = .{
                .receiver_id = status.receiver_id,
                .last_position = limit_position,
                .last_seen_ns = now_ns,
                .active = true,
            };
            if (free_slot != null) self.receiver_count += 1;
        }

        return self.computeMinPosition(sender_limit);
    }

    /// Called periodically when idle. Removes timed-out receivers and
    /// returns the current minimum position.
    pub fn onIdle(
        self: *MinFlowControl,
        now_ns: u64,
        sender_limit: i64,
        sender_position: i64,
        is_eos: bool,
    ) i64 {
        _ = sender_position;
        _ = is_eos;

        // Remove stale receivers.
        for (&self.receivers) |*r| {
            if (r.active and now_ns > r.last_seen_ns and
                (now_ns - r.last_seen_ns) > self.receiver_timeout_ns)
            {
                r.active = false;
                if (self.receiver_count > 0) {
                    self.receiver_count -= 1;
                }
            }
        }

        return self.computeMinPosition(sender_limit);
    }

    /// Returns true if at least one receiver is actively tracked.
    pub fn hasRequiredReceivers(self: *const MinFlowControl) bool {
        return self.receiver_count > 0;
    }

    /// Compute the minimum position across all active receivers.
    /// If no receivers are active, returns `fallback`.
    fn computeMinPosition(self: *const MinFlowControl, fallback: i64) i64 {
        var min_pos: i64 = std.math.maxInt(i64);
        var any_active = false;

        for (&self.receivers) |*r| {
            if (r.active) {
                if (r.last_position < min_pos) {
                    min_pos = r.last_position;
                }
                any_active = true;
            }
        }

        return if (any_active) min_pos else fallback;
    }
};

// ── Max Flow Control ───────────────────────────────────────

/// Maximum (best-effort) flow control.
///
/// The sender is never limited — it can always advance up to
/// `sender_position + window`. This is appropriate when losing slow
/// receivers is acceptable (e.g. market data where stale quotes are
/// worthless).
pub const MaxFlowControl = struct {
    receiver_window: i64 = 256 * 1024,

    /// Always returns `sender_position + window`, effectively imposing no
    /// flow-control back-pressure.
    pub fn onStatusMessage(
        self: *MaxFlowControl,
        status: ReceiverStatus,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: u8,
        now_ns: u64,
    ) i64 {
        _ = now_ns;
        _ = sender_limit;

        const position = computePosition(
            status.consumption_term_id,
            status.consumption_term_offset,
            position_bits_to_shift,
            initial_term_id,
        );
        const window: i64 = @as(i64, status.receiver_window_length);
        return @max(position +| window, position +| self.receiver_window);
    }

    /// On idle, returns `sender_position + window` — no restriction.
    pub fn onIdle(
        self: *MaxFlowControl,
        now_ns: u64,
        sender_limit: i64,
        sender_position: i64,
        is_eos: bool,
    ) i64 {
        _ = now_ns;
        _ = is_eos;
        _ = sender_limit;

        return sender_position +| self.receiver_window;
    }

    /// Max flow control does not require any receivers.
    pub fn hasRequiredReceivers(self: *const MaxFlowControl) bool {
        _ = self;
        return true;
    }
};

// ── Tagged Flow Control ────────────────────────────────────

/// Tagged flow control — only receivers matching a required group tag
/// constrain the sender.
///
/// Receivers whose `group_tag` matches `required_group_tag` are treated
/// like MinFlowControl (sender limit = minimum of their positions).
/// All other receivers are tracked but have no effect on the sender
/// limit, behaving like MaxFlowControl for those streams.
///
/// This is useful in capital-markets architectures where a matching
/// engine must wait for all risk checkers (tagged) but not for market
/// data consumers (untagged).
pub const TaggedFlowControl = struct {
    receivers: [MAX_RECEIVERS]TaggedReceiverState = [_]TaggedReceiverState{.{}} ** MAX_RECEIVERS,
    receiver_count: u32 = 0,
    required_group_tag: i64 = 0,
    receiver_timeout_ns: u64 = 5_000_000_000,

    const MAX_RECEIVERS = 16;

    const TaggedReceiverState = struct {
        receiver_id: i64 = 0,
        group_tag: i64 = 0,
        last_position: i64 = 0,
        last_seen_ns: u64 = 0,
        active: bool = false,
    };

    /// Process a status message. The `group_tag` is embedded in the
    /// `receiver_id` field's upper bits (convention: lower 32 bits are
    /// receiver ID, upper 32 bits are group tag). For simplicity we
    /// accept the group_tag as a separate parameter derived from the
    /// status message's receiver_id.
    pub fn onStatusMessageTagged(
        self: *TaggedFlowControl,
        status: ReceiverStatus,
        group_tag: i64,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: u8,
        now_ns: u64,
    ) i64 {
        const position = computePosition(
            status.consumption_term_id,
            status.consumption_term_offset,
            position_bits_to_shift,
            initial_term_id,
        );
        const window: i64 = @as(i64, status.receiver_window_length);
        // Saturating: position near i64 max plus a window must not overflow.
        const limit_position = position +| window;

        var found = false;
        for (&self.receivers) |*r| {
            if (r.active and r.receiver_id == status.receiver_id) {
                r.last_position = limit_position;
                r.last_seen_ns = now_ns;
                r.group_tag = group_tag;
                found = true;
                break;
            }
        }

        if (!found) {
            // Same eviction policy as MinFlowControl: when full, replace
            // the least-recently-seen entry so stale registrations cannot
            // permanently exclude live receivers.
            var free_slot: ?*TaggedReceiverState = null;
            var oldest: *TaggedReceiverState = &self.receivers[0];
            for (&self.receivers) |*r| {
                if (!r.active) {
                    free_slot = r;
                    break;
                }
                if (r.last_seen_ns < oldest.last_seen_ns) oldest = r;
            }
            const slot = free_slot orelse oldest;
            slot.* = .{
                .receiver_id = status.receiver_id,
                .group_tag = group_tag,
                .last_position = limit_position,
                .last_seen_ns = now_ns,
                .active = true,
            };
            if (free_slot != null) self.receiver_count += 1;
        }

        return self.computeTaggedMinPosition(sender_limit);
    }

    /// Convenience wrapper matching the FlowControl onStatusMessage
    /// signature. Uses `receiver_id` as both ID and group tag.
    pub fn onStatusMessage(
        self: *TaggedFlowControl,
        status: ReceiverStatus,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: u8,
        now_ns: u64,
    ) i64 {
        // By convention the group tag is the receiver_id itself when
        // no separate tag is supplied.
        return self.onStatusMessageTagged(
            status,
            status.receiver_id,
            sender_limit,
            initial_term_id,
            position_bits_to_shift,
            now_ns,
        );
    }

    /// Remove timed-out receivers and return current tagged minimum.
    pub fn onIdle(
        self: *TaggedFlowControl,
        now_ns: u64,
        sender_limit: i64,
        sender_position: i64,
        is_eos: bool,
    ) i64 {
        _ = sender_position;
        _ = is_eos;

        for (&self.receivers) |*r| {
            if (r.active and now_ns > r.last_seen_ns and
                (now_ns - r.last_seen_ns) > self.receiver_timeout_ns)
            {
                r.active = false;
                if (self.receiver_count > 0) {
                    self.receiver_count -= 1;
                }
            }
        }

        return self.computeTaggedMinPosition(sender_limit);
    }

    /// Returns true if at least one receiver with the required tag is
    /// active.
    pub fn hasRequiredReceivers(self: *const TaggedFlowControl) bool {
        for (&self.receivers) |*r| {
            if (r.active and r.group_tag == self.required_group_tag) {
                return true;
            }
        }
        return false;
    }

    /// Compute the minimum position among receivers whose group_tag
    /// matches `required_group_tag`. Returns `fallback` if no tagged
    /// receivers are active.
    fn computeTaggedMinPosition(self: *const TaggedFlowControl, fallback: i64) i64 {
        var min_pos: i64 = std.math.maxInt(i64);
        var any_tagged = false;

        for (&self.receivers) |*r| {
            if (r.active and r.group_tag == self.required_group_tag) {
                if (r.last_position < min_pos) {
                    min_pos = r.last_position;
                }
                any_tagged = true;
            }
        }

        return if (any_tagged) min_pos else fallback;
    }
};

// ── Flow Control (Strategy Dispatch) ───────────────────────

/// Unified flow control interface dispatching to one of the three
/// concrete strategies via a tagged union. Zero-cost at runtime when
/// the strategy is known at init time.
pub const FlowControl = struct {
    strategy: Strategy,

    pub const Strategy = union(enum) {
        min: MinFlowControl,
        max: MaxFlowControl,
        tagged: TaggedFlowControl,
    };

    /// Create a FlowControl from a configuration.
    pub fn init(cfg: FlowControlConfig) FlowControl {
        return switch (cfg.strategy) {
            .min => .{
                .strategy = .{
                    .min = .{
                        .receiver_timeout_ns = cfg.receiver_timeout_ns,
                    },
                },
            },
            .max => .{
                .strategy = .{
                    .max = .{
                        .receiver_window = @as(i64, cfg.initial_receiver_window),
                    },
                },
            },
            .tagged => .{
                .strategy = .{
                    .tagged = .{
                        .required_group_tag = cfg.required_group_tag,
                        .receiver_timeout_ns = cfg.receiver_timeout_ns,
                    },
                },
            },
        };
    }

    /// Called when a status message is received from a receiver.
    /// Returns the new sender limit position.
    pub fn onStatusMessage(
        self: *FlowControl,
        status: ReceiverStatus,
        sender_limit: i64,
        initial_term_id: i32,
        position_bits_to_shift: u8,
        now_ns: u64,
    ) i64 {
        return switch (self.strategy) {
            .min => |*s| s.onStatusMessage(status, sender_limit, initial_term_id, position_bits_to_shift, now_ns),
            .max => |*s| s.onStatusMessage(status, sender_limit, initial_term_id, position_bits_to_shift, now_ns),
            .tagged => |*s| s.onStatusMessage(status, sender_limit, initial_term_id, position_bits_to_shift, now_ns),
        };
    }

    /// Called periodically when idle. Returns the current sender limit.
    pub fn onIdle(
        self: *FlowControl,
        now_ns: u64,
        sender_limit: i64,
        sender_position: i64,
        is_eos: bool,
    ) i64 {
        return switch (self.strategy) {
            .min => |*s| s.onIdle(now_ns, sender_limit, sender_position, is_eos),
            .max => |*s| s.onIdle(now_ns, sender_limit, sender_position, is_eos),
            .tagged => |*s| s.onIdle(now_ns, sender_limit, sender_position, is_eos),
        };
    }

    /// Check if there are any required receivers.
    pub fn hasRequiredReceivers(self: *const FlowControl) bool {
        return switch (self.strategy) {
            .min => |*s| s.hasRequiredReceivers(),
            .max => |*s| s.hasRequiredReceivers(),
            .tagged => |*s| s.hasRequiredReceivers(),
        };
    }
};

// ── Tests ──────────────────────────────────────────────────

test "MinFlowControl — single receiver tracks position" {
    var fc = MinFlowControl{};

    const status = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 1024,
        .receiver_window_length = 4096,
        .receiver_id = 100,
        .timestamp_ns = 1000,
    };

    const limit = fc.onStatusMessage(status, 0, 0, 16, 1000);

    // position = (0 - 0) << 16 + 1024 = 1024; limit = 1024 + 4096 = 5120
    try std.testing.expectEqual(@as(i64, 5120), limit);
    try std.testing.expectEqual(@as(u32, 1), fc.receiver_count);
}

test "MinFlowControl — multiple receivers, returns minimum" {
    var fc = MinFlowControl{};

    const status_slow = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 500,
        .receiver_window_length = 1000,
        .receiver_id = 1,
        .timestamp_ns = 1000,
    };

    const status_fast = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 2000,
        .receiver_window_length = 1000,
        .receiver_id = 2,
        .timestamp_ns = 1000,
    };

    _ = fc.onStatusMessage(status_fast, 0, 0, 16, 1000);
    const limit = fc.onStatusMessage(status_slow, 0, 0, 16, 1000);

    // slow: 500 + 1000 = 1500; fast: 2000 + 1000 = 3000; min = 1500
    try std.testing.expectEqual(@as(i64, 1500), limit);
    try std.testing.expectEqual(@as(u32, 2), fc.receiver_count);
}

test "MinFlowControl — receiver timeout removes stale receivers" {
    var fc = MinFlowControl{};
    fc.receiver_timeout_ns = 100; // 100 ns for test speed

    const status = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 0,
        .receiver_window_length = 4096,
        .receiver_id = 42,
        .timestamp_ns = 1000,
    };

    _ = fc.onStatusMessage(status, 0, 0, 16, 1000);
    try std.testing.expectEqual(@as(u32, 1), fc.receiver_count);

    // Idle at now=1200, 200 ns after last_seen (100ns timeout)
    _ = fc.onIdle(1200, 0, 0, false);
    try std.testing.expectEqual(@as(u32, 0), fc.receiver_count);
}

test "MinFlowControl — hasRequiredReceivers returns false when empty" {
    const fc = MinFlowControl{};
    try std.testing.expect(!fc.hasRequiredReceivers());
}

test "MaxFlowControl — returns position + window (no limit)" {
    var fc = MaxFlowControl{ .receiver_window = 8192 };

    const status = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 1000,
        .receiver_window_length = 4096,
        .receiver_id = 1,
        .timestamp_ns = 1000,
    };

    const limit = fc.onStatusMessage(status, 0, 0, 16, 1000);

    // position = 1000, max(1000+4096, 1000+8192) = 9192
    try std.testing.expectEqual(@as(i64, 9192), limit);

    // onIdle also just returns sender_position + window
    const idle_limit = fc.onIdle(2000, 0, 5000, false);
    try std.testing.expectEqual(@as(i64, 5000 + 8192), idle_limit);
}

test "TaggedFlowControl — only tagged receivers limit" {
    var fc = TaggedFlowControl{};
    fc.required_group_tag = 42;

    const tagged_status = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 500,
        .receiver_window_length = 1000,
        .receiver_id = 1,
        .timestamp_ns = 1000,
    };

    const limit = fc.onStatusMessageTagged(tagged_status, 42, 0, 0, 16, 1000);

    // position = 500, limit = 500 + 1000 = 1500
    try std.testing.expectEqual(@as(i64, 1500), limit);
    try std.testing.expect(fc.hasRequiredReceivers());
}

test "TaggedFlowControl — untagged receivers don't limit" {
    var fc = TaggedFlowControl{};
    fc.required_group_tag = 42;

    // Add an untagged receiver (group_tag = 99, not 42)
    const untagged_status = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 100,
        .receiver_window_length = 500,
        .receiver_id = 1,
        .timestamp_ns = 1000,
    };

    const limit = fc.onStatusMessageTagged(untagged_status, 99, 9999, 0, 16, 1000);

    // No tagged receivers => returns fallback (9999)
    try std.testing.expectEqual(@as(i64, 9999), limit);
    try std.testing.expect(!fc.hasRequiredReceivers());
    // But receiver is still tracked
    try std.testing.expectEqual(@as(u32, 1), fc.receiver_count);
}

test "FlowControl — strategy dispatch works" {
    // Test with min strategy
    var fc_min = FlowControl.init(.{ .strategy = .min });

    const status = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 2048,
        .receiver_window_length = 4096,
        .receiver_id = 1,
        .timestamp_ns = 1000,
    };

    const limit = fc_min.onStatusMessage(status, 0, 0, 16, 1000);
    try std.testing.expectEqual(@as(i64, 2048 + 4096), limit);
    try std.testing.expect(fc_min.hasRequiredReceivers());

    // Test with max strategy
    var fc_max = FlowControl.init(.{
        .strategy = .max,
        .initial_receiver_window = 8192,
    });

    const limit_max = fc_max.onStatusMessage(status, 0, 0, 16, 1000);
    // position=2048, max(2048+4096, 2048+8192) = 10240
    try std.testing.expectEqual(@as(i64, 10240), limit_max);
    try std.testing.expect(fc_max.hasRequiredReceivers());
}

test "computePosition roundtrip" {
    // Term 0, offset 1024, 16-bit shift, initial term 0
    const pos1 = computePosition(0, 1024, 16, 0);
    try std.testing.expectEqual(@as(i64, 1024), pos1);

    // Term 1, offset 0, 16-bit shift (64K terms), initial term 0
    const pos2 = computePosition(1, 0, 16, 0);
    try std.testing.expectEqual(@as(i64, 65536), pos2);

    // Term 3, offset 512, 20-bit shift (1M terms), initial term 1
    const pos3 = computePosition(3, 512, 20, 1);
    // (3-1) << 20 + 512 = 2*1048576 + 512 = 2097664
    try std.testing.expectEqual(@as(i64, 2097664), pos3);

    // Negative term count (term_id < initial_term_id)
    const pos4 = computePosition(0, 0, 16, 1);
    // (0-1) << 16 = -65536
    try std.testing.expectEqual(@as(i64, -65536), pos4);
}

test "ReceiverStatus creation and field access" {
    const status = ReceiverStatus{
        .session_id = 42,
        .stream_id = 7,
        .consumption_term_id = 3,
        .consumption_term_offset = 8192,
        .receiver_window_length = 65536,
        .receiver_id = 12345,
        .timestamp_ns = 999_999_999,
    };

    try std.testing.expectEqual(@as(i32, 42), status.session_id);
    try std.testing.expectEqual(@as(i32, 7), status.stream_id);
    try std.testing.expectEqual(@as(i32, 3), status.consumption_term_id);
    try std.testing.expectEqual(@as(i32, 8192), status.consumption_term_offset);
    try std.testing.expectEqual(@as(i32, 65536), status.receiver_window_length);
    try std.testing.expectEqual(@as(i64, 12345), status.receiver_id);
    try std.testing.expectEqual(@as(u64, 999_999_999), status.timestamp_ns);
}

test "MinFlowControl — receiver re-registration updates existing entry" {
    var fc = MinFlowControl{};

    const status1 = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 1000,
        .receiver_window_length = 2000,
        .receiver_id = 77,
        .timestamp_ns = 1000,
    };

    _ = fc.onStatusMessage(status1, 0, 0, 16, 1000);
    try std.testing.expectEqual(@as(u32, 1), fc.receiver_count);

    // Same receiver_id, updated position
    const status2 = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 5000,
        .receiver_window_length = 2000,
        .receiver_id = 77,
        .timestamp_ns = 2000,
    };

    const limit = fc.onStatusMessage(status2, 0, 0, 16, 2000);

    // Should still be 1 receiver, not 2
    try std.testing.expectEqual(@as(u32, 1), fc.receiver_count);
    // Position updated: 5000 + 2000 = 7000
    try std.testing.expectEqual(@as(i64, 7000), limit);
}

test "MaxFlowControl — hasRequiredReceivers always true" {
    const fc = MaxFlowControl{};
    try std.testing.expect(fc.hasRequiredReceivers());
}

test "TaggedFlowControl — mixed tagged and untagged receivers" {
    var fc = TaggedFlowControl{};
    fc.required_group_tag = 10;

    // Add a tagged receiver at position 500+1000 = 1500
    const tagged = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 500,
        .receiver_window_length = 1000,
        .receiver_id = 1,
        .timestamp_ns = 1000,
    };
    _ = fc.onStatusMessageTagged(tagged, 10, 0, 0, 16, 1000);

    // Add an untagged receiver at position 100+500 = 600 (lower, but should not limit)
    const untagged = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 100,
        .receiver_window_length = 500,
        .receiver_id = 2,
        .timestamp_ns = 1000,
    };
    const limit = fc.onStatusMessageTagged(untagged, 99, 0, 0, 16, 1000);

    // Limit should be based on tagged receiver only: 1500
    try std.testing.expectEqual(@as(i64, 1500), limit);
    try std.testing.expectEqual(@as(u32, 2), fc.receiver_count);
}

test "MinFlowControl — 17th receiver evicts least-recently-seen entry" {
    var fc = MinFlowControl{};

    // Fill all 16 slots; receiver 1 is the stalest (last_seen = 1).
    var id: i64 = 1;
    while (id <= 16) : (id += 1) {
        const status = ReceiverStatus{
            .session_id = 1,
            .stream_id = 1,
            .consumption_term_id = 0,
            .consumption_term_offset = 10_000,
            .receiver_window_length = 1000,
            .receiver_id = id,
            .timestamp_ns = 0,
        };
        _ = fc.onStatusMessage(status, 0, 0, 16, @intCast(id));
    }
    try std.testing.expectEqual(@as(u32, 16), fc.receiver_count);

    // A 17th (slow) receiver used to be silently dropped, letting 16
    // stale/spoofed registrations stall the sender. It must now replace
    // the stalest slot and constrain the limit.
    const newcomer = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 100,
        .receiver_window_length = 100,
        .receiver_id = 999,
        .timestamp_ns = 0,
    };
    const limit = fc.onStatusMessage(newcomer, 0, 0, 16, 1_000_000);

    try std.testing.expectEqual(@as(u32, 16), fc.receiver_count);
    // Newcomer is the minimum: 100 + 100 = 200.
    try std.testing.expectEqual(@as(i64, 200), limit);

    // The evicted receiver (id 1) re-registering must take a slot again
    // (now evicting the next-stalest), not vanish.
    const evicted = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 50,
        .receiver_window_length = 100,
        .receiver_id = 1,
        .timestamp_ns = 0,
    };
    const limit2 = fc.onStatusMessage(evicted, 0, 0, 16, 1_000_001);
    try std.testing.expectEqual(@as(i64, 150), limit2);
}

test "flow control position arithmetic saturates instead of overflowing" {
    // computePosition with a hostile shift used to panic (@intCast of a
    // shift >= 64); extreme term ids used to overflow the i64 shift/add.
    const p1 = computePosition(std.math.maxInt(i32), std.math.maxInt(i32), 255, 0);
    try std.testing.expectEqual(std.math.maxInt(i64), p1);

    const p2 = computePosition(std.math.maxInt(i32), 0, 63, std.math.minInt(i32));
    try std.testing.expectEqual(std.math.maxInt(i64), p2);

    // position + window near i64 max must saturate, not panic (Min path).
    var fc = MinFlowControl{};
    const hostile = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = std.math.maxInt(i32),
        .consumption_term_offset = std.math.maxInt(i32),
        .receiver_window_length = std.math.maxInt(i32),
        .receiver_id = 7,
        .timestamp_ns = 0,
    };
    const limit = fc.onStatusMessage(hostile, 0, 0, 63, 1000);
    try std.testing.expectEqual(std.math.maxInt(i64), limit);

    // Max path saturates too.
    var fc_max = MaxFlowControl{ .receiver_window = std.math.maxInt(i64) };
    const limit_max = fc_max.onIdle(0, 0, std.math.maxInt(i64), false);
    try std.testing.expectEqual(std.math.maxInt(i64), limit_max);

    // Tagged path saturates too.
    var fc_tagged = TaggedFlowControl{};
    fc_tagged.required_group_tag = 1;
    const limit_tagged = fc_tagged.onStatusMessageTagged(hostile, 1, 0, 0, 63, 1000);
    try std.testing.expectEqual(std.math.maxInt(i64), limit_tagged);
}

test "TaggedFlowControl — full table evicts least-recently-seen entry" {
    var fc = TaggedFlowControl{};
    fc.required_group_tag = 5;

    var id: i64 = 1;
    while (id <= 16) : (id += 1) {
        const status = ReceiverStatus{
            .session_id = 1,
            .stream_id = 1,
            .consumption_term_id = 0,
            .consumption_term_offset = 10_000,
            .receiver_window_length = 1000,
            .receiver_id = id,
            .timestamp_ns = 0,
        };
        // None carry the required tag.
        _ = fc.onStatusMessageTagged(status, 99, 0, 0, 16, @intCast(id));
    }
    try std.testing.expectEqual(@as(u32, 16), fc.receiver_count);

    // The 17th receiver carries the required tag — it must get a slot.
    const tagged = ReceiverStatus{
        .session_id = 1,
        .stream_id = 1,
        .consumption_term_id = 0,
        .consumption_term_offset = 300,
        .receiver_window_length = 100,
        .receiver_id = 999,
        .timestamp_ns = 0,
    };
    const limit = fc.onStatusMessageTagged(tagged, 5, 0, 0, 16, 1_000_000);
    try std.testing.expectEqual(@as(i64, 400), limit);
    try std.testing.expect(fc.hasRequiredReceivers());
}

test "FlowControlConfig defaults" {
    const cfg = FlowControlConfig{};
    try std.testing.expectEqual(FlowControlConfig.StrategyType.min, cfg.strategy);
    try std.testing.expectEqual(@as(u64, 5_000_000_000), cfg.receiver_timeout_ns);
    try std.testing.expectEqual(@as(i64, 0), cfg.required_group_tag);
    try std.testing.expectEqual(@as(i32, 256 * 1024), cfg.initial_receiver_window);
}
