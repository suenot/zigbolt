const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("../core/frame.zig");

// ── Network Header ──────────────────────────────────────────

/// Network frame header (prepended to every UDP datagram).
///
/// Provides session multiplexing, stream identification, and sequencing
/// for the NAK-based reliability protocol.
pub const NetworkHeader = extern struct {
    /// Protocol version.
    version: u8 = 1,
    /// Header type.
    header_type: HeaderType,
    /// Session ID (identifies a publisher-subscriber pair).
    session_id: u32,
    /// Stream ID (topic/channel within a session).
    stream_id: u32,
    /// Sequence number (monotonically increasing per stream).
    sequence: u64,
    /// Payload length.
    payload_length: u32,
    _reserved: [3]u8 = .{ 0, 0, 0 },

    pub const SIZE: usize = @sizeOf(NetworkHeader);

    pub const HeaderType = enum(u8) {
        data = 0,
        nak = 1,
        heartbeat = 2,
        setup = 3,
        teardown = 4,
    };
};

// ── NAK Message ─────────────────────────────────────────────

/// NAK message: "I'm missing sequences X through Y".
///
/// Sent by the receiver when it detects a gap in the sequence space.
/// The sender should retransmit the requested range.
pub const NakMessage = extern struct {
    session_id: u32,
    stream_id: u32,
    /// First missing sequence number.
    from_sequence: u64,
    /// Number of missing sequences.
    count: u32,
    _padding: [4]u8 = .{ 0, 0, 0, 0 },
};

// ── Send Buffer ─────────────────────────────────────────────

/// Tracks sent messages for potential retransmission.
///
/// Stores copies of recently sent payloads in a ring buffer indexed
/// by sequence number. When a NAK is received, the sender looks up
/// the requested sequence(s) here and retransmits.
pub const SendBuffer = struct {
    /// Ring buffer of sent frames indexed by sequence number.
    entries: []SendEntry,
    capacity: usize,
    next_sequence: u64,
    allocator: std.mem.Allocator,

    pub const SendEntry = struct {
        sequence: u64,
        data: []u8,
        timestamp_ns: u64,
        retransmit_count: u8,
        occupied: bool,
    };

    pub fn init(allocator: std.mem.Allocator, capacity: usize) !SendBuffer {
        const entries = try allocator.alloc(SendEntry, capacity);
        for (entries) |*entry| {
            entry.* = .{
                .sequence = 0,
                .data = &.{},
                .timestamp_ns = 0,
                .retransmit_count = 0,
                .occupied = false,
            };
        }
        return .{
            .entries = entries,
            .capacity = capacity,
            .next_sequence = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SendBuffer, allocator: std.mem.Allocator) void {
        for (self.entries) |*entry| {
            if (entry.occupied and entry.data.len > 0) {
                allocator.free(entry.data);
            }
        }
        allocator.free(self.entries);
    }

    /// Store a copy of `data` at the given sequence number for later retransmission.
    /// Returns the number of payload bytes evicted from an overwritten slot
    /// (0 if the slot was free) so the caller can return those bytes to
    /// flow control — an evicted entry can never be retransmitted, so it
    /// must not stay accounted as in flight.
    pub fn store(self: *SendBuffer, sequence: u64, data: []const u8, allocator: std.mem.Allocator) !usize {
        const idx = sequence % self.capacity;
        const entry = &self.entries[idx];

        // Free old data if slot was occupied.
        var evicted: usize = 0;
        if (entry.occupied) { // kcov-skip: runs on every store() (e.g. "SendBuffer store, get, and release"); kcov emits no hit record for this line
            evicted = entry.data.len;
            if (entry.data.len > 0) allocator.free(entry.data);
        }

        const copy = try allocator.alloc(u8, data.len);
        @memcpy(copy, data);

        entry.* = .{
            .sequence = sequence,
            .data = copy,
            .timestamp_ns = config.timestampNs(),
            .retransmit_count = 0,
            .occupied = true,
        };

        if (sequence >= self.next_sequence) {
            self.next_sequence = sequence +| 1;
        }

        return evicted;
    }

    /// Look up a stored entry by sequence number.
    /// Returns `null` if the slot is empty or contains a different sequence.
    pub fn get(self: *SendBuffer, sequence: u64) ?*SendEntry {
        const idx = sequence % self.capacity;
        const entry = &self.entries[idx];
        if (entry.occupied and entry.sequence == sequence) {
            return entry;
        }
        return null;
    }

    /// Release all entries with sequence numbers up to (but not including) `up_to_sequence`.
    /// This is called when the receiver confirms it has received everything below this point.
    /// Returns the total payload bytes released, which the caller uses to
    /// replenish flow-control credits (the bytes are no longer in flight).
    pub fn release(self: *SendBuffer, up_to_sequence: u64) usize {
        var freed: usize = 0;
        for (self.entries) |*entry| {
            if (entry.occupied and entry.sequence < up_to_sequence) {
                freed += entry.data.len;
                if (entry.data.len > 0) {
                    self.allocator.free(entry.data);
                    entry.data = &.{};
                }
                entry.occupied = false;
            }
        }
        return freed;
    }
};

// ── Recv Tracker ────────────────────────────────────────────

/// Tracks received sequences and detects gaps.
///
/// Uses a bitmap over a sliding window to record which sequence numbers
/// have been received. When a sequence arrives that is ahead of the
/// expected position, the gap is reported so a NAK can be generated.
pub const RecvTracker = struct {
    /// Next expected sequence number.
    next_expected: u64,
    /// Bitmap of received sequences (for gap detection).
    received_bitmap: std.DynamicBitSet,
    /// Window size.
    window_size: u64,
    /// Base sequence number of the bitmap window.
    bitmap_base: u64,
    allocator: std.mem.Allocator,

    pub const GapInfo = struct {
        from: u64,
        count: u32,
    };

    /// Outcome of recording a received sequence.
    pub const RecordResult = struct {
        /// True when the sequence is fresh (first time seen, inside the
        /// receive window) and should be delivered to the application.
        /// False for duplicates/replays, sequences behind the window
        /// (already delivered), and sequences too far ahead of the window
        /// (spoofed/out-of-window — see `recordReceived`).
        accepted: bool,
        /// Gap detected behind this sequence (missing intermediates), if any.
        gap: ?GapInfo,
    };

    pub fn init(allocator: std.mem.Allocator, window_size: u64) !RecvTracker {
        var bitmap = try std.DynamicBitSet.initEmpty(allocator, window_size);
        _ = &bitmap;
        return .{
            .next_expected = 0,
            .received_bitmap = bitmap,
            .window_size = window_size,
            .bitmap_base = 0,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RecvTracker) void {
        self.received_bitmap.deinit();
    }

    /// Record a received sequence number.
    ///
    /// Returns whether the sequence is fresh (deliverable) and any gap
    /// detected behind it. The window slides automatically past the
    /// contiguously received prefix, so it tracks the live sequence range.
    ///
    /// Sequences that remain more than `window_size` ahead even after
    /// sliding are rejected: a hostile peer spoofing a far-ahead sequence
    /// (e.g. u64 max) would otherwise yank `next_expected` ahead (perpetual
    /// bogus NAKs) and overflow `sequence + 1`.
    pub fn recordReceived(self: *RecvTracker, sequence: u64) RecordResult {
        // Behind the window: already delivered (or pre-history) — drop.
        if (sequence < self.bitmap_base) {
            return .{ .accepted = false, .gap = null };
        }

        var offset = sequence - self.bitmap_base;
        if (offset >= self.window_size) {
            // Make room by sliding past the contiguously received prefix.
            self.slideWindow(self.contiguousPosition());
            offset = sequence - self.bitmap_base;
            if (offset >= self.window_size) {
                // Still out of window: reject (out-of-window/spoofed).
                return .{ .accepted = false, .gap = null };
            }
        }

        if (self.received_bitmap.isSet(@intCast(offset))) {
            // Duplicate/replayed sequence: drop, deliver only once.
            return .{ .accepted = false, .gap = null };
        }
        self.received_bitmap.set(@intCast(offset));

        var gap: ?GapInfo = null;

        if (sequence > self.next_expected) {
            // Gap detected: we expected next_expected but got something higher.
            const missing_count = sequence - self.next_expected;
            gap = .{
                .from = self.next_expected,
                .count = @intCast(@min(missing_count, std.math.maxInt(u32))),
            };
        }

        // Advance next_expected past any contiguous received block.
        if (sequence >= self.next_expected) {
            self.next_expected = sequence +| 1;
            self.advanceContiguous();
        }

        return .{ .accepted = true, .gap = gap };
    }

    /// Highest sequence S such that every sequence below S has been
    /// received (the contiguous floor: bitmap_base plus the run of set
    /// bits at the bottom of the window).
    pub fn contiguousPosition(self: *const RecvTracker) u64 {
        var run: u64 = 0;
        while (run < self.window_size and self.received_bitmap.isSet(@intCast(run))) : (run += 1) {}
        return self.bitmap_base +| run;
    }

    /// Advance next_expected past any contiguous received entries in the bitmap.
    fn advanceContiguous(self: *RecvTracker) void {
        while (true) {
            if (self.next_expected < self.bitmap_base) break;
            const offset = self.next_expected - self.bitmap_base;
            if (offset >= self.window_size) break;
            if (!self.received_bitmap.isSet(offset)) break;
            self.next_expected += 1;
        }
    }

    /// Get a list of missing sequence numbers within the current window.
    /// Caller owns the returned slice.
    pub fn getMissing(self: *RecvTracker, allocator: std.mem.Allocator) ![]u64 {
        var missing = std.ArrayListUnmanaged(u64){};
        errdefer missing.deinit(allocator);

        // Scan from bitmap_base up to next_expected for unset bits
        // (saturating: next_expected can never lag bitmap_base, but a wrap
        // here must not turn into a giant bogus scan).
        const scan_end = @min(self.next_expected -| self.bitmap_base, self.window_size);
        var i: u64 = 0;
        while (i < scan_end) : (i += 1) {
            if (!self.received_bitmap.isSet(i)) {
                try missing.append(allocator, self.bitmap_base + i);
            }
        }

        return missing.toOwnedSlice(allocator);
    }

    /// Zero-allocation variant of `getMissing` for the poll hot path:
    /// writes missing sequence numbers into `out` (capped at out.len)
    /// and returns how many were written.
    pub fn getMissingInto(self: *const RecvTracker, out: []u64) usize {
        const scan_end = @min(self.next_expected -| self.bitmap_base, self.window_size);
        var n: usize = 0;
        var i: u64 = 0;
        while (i < scan_end and n < out.len) : (i += 1) {
            if (!self.received_bitmap.isSet(@intCast(i))) {
                out[n] = self.bitmap_base + i;
                n += 1;
            }
        }
        return n;
    }

    /// Slide the window forward, clearing bits that are now behind.
    pub fn slideWindow(self: *RecvTracker, new_base: u64) void {
        if (new_base <= self.bitmap_base) return;

        const shift = new_base - self.bitmap_base;
        if (shift >= self.window_size) {
            // Entire window moved past; clear everything.
            var i: usize = 0;
            while (i < self.window_size) : (i += 1) {
                self.received_bitmap.unset(i);
            }
        } else {
            // Shift bits left by `shift` positions.
            var dst: usize = 0;
            var src: usize = @intCast(shift);
            while (src < self.window_size) : ({
                dst += 1;
                src += 1;
            }) {
                if (self.received_bitmap.isSet(src)) {
                    self.received_bitmap.set(dst);
                } else {
                    self.received_bitmap.unset(dst);
                }
            }
            // Clear the vacated upper portion.
            while (dst < self.window_size) : (dst += 1) {
                self.received_bitmap.unset(dst);
            }
        }

        self.bitmap_base = new_base;
    }
};

// ── Flow Control ────────────────────────────────────────────

/// Credit-based flow control.
///
/// The receiver advertises how many bytes it can accept (credits).
/// The sender consumes credits on each send and the receiver replenishes
/// credits as it processes data. This prevents the sender from
/// overwhelming a slow receiver.
pub const FlowControl = struct {
    /// Credits available (bytes the receiver can accept).
    credits: std.atomic.Value(i64),
    /// Window size.
    window_size: i64,

    pub fn init(window_size: i64) FlowControl {
        return .{
            .credits = std.atomic.Value(i64).init(window_size),
            .window_size = window_size,
        };
    }

    /// Try to consume `bytes` credits. Returns true if credits were
    /// available and consumed, false if insufficient credits.
    pub fn tryConsume(self: *FlowControl, bytes: usize) bool {
        const cost: i64 = @intCast(bytes);
        while (true) {
            const current = self.credits.load(.acquire);
            if (current < cost) return false;
            if (self.credits.cmpxchgWeak(
                current,
                current - cost,
                .acq_rel,
                .monotonic,
            ) == null) {
                return true;
            }
        } // kcov-skip: weak-CAS retry back-edge (exercised by the contention test); kcov attributes the branch to the cmpxchg line and ptrace serializes threads, suppressing retries
    }

    /// Replenish credits (called by the receiver after processing data).
    pub fn replenish(self: *FlowControl, bytes: usize) void {
        const amount: i64 = @intCast(bytes);
        _ = self.credits.fetchAdd(amount, .release);
    }

    /// Return the currently available credits.
    pub fn available(self: *FlowControl) i64 {
        return self.credits.load(.acquire);
    }
};

// ── Tests ───────────────────────────────────────────────────

test "NetworkHeader size is stable" {
    // The header is an extern struct so its size is ABI-defined.
    // Verify it matches what we expect for serialisation.
    try std.testing.expectEqual(@as(usize, NetworkHeader.SIZE), @sizeOf(NetworkHeader));
    // It should fit in a single cache line.
    try std.testing.expect(NetworkHeader.SIZE <= config.cache_line_size);
}

test "SendBuffer store, get, and release" {
    const allocator = std.testing.allocator;
    var buf = try SendBuffer.init(allocator, 8);
    defer buf.deinit(allocator);

    // Store a few entries (fresh slots evict nothing).
    try std.testing.expectEqual(@as(usize, 0), try buf.store(0, "hello", allocator));
    try std.testing.expectEqual(@as(usize, 0), try buf.store(1, "world", allocator));
    try std.testing.expectEqual(@as(usize, 0), try buf.store(2, "zigbolt", allocator));

    // Retrieve them.
    {
        const e0 = buf.get(0).?;
        try std.testing.expectEqualSlices(u8, "hello", e0.data);
        try std.testing.expectEqual(@as(u64, 0), e0.sequence);
    }
    {
        const e1 = buf.get(1).?;
        try std.testing.expectEqualSlices(u8, "world", e1.data);
    }
    {
        const e2 = buf.get(2).?;
        try std.testing.expectEqualSlices(u8, "zigbolt", e2.data);
    }

    // Non-existent sequence returns null.
    try std.testing.expect(buf.get(99) == null);

    // Release up to sequence 2 (exclusive) — removes 0 and 1, returning
    // the bytes freed ("hello" + "world" = 10).
    try std.testing.expectEqual(@as(usize, 10), buf.release(2));
    try std.testing.expect(buf.get(0) == null);
    try std.testing.expect(buf.get(1) == null);
    try std.testing.expect(buf.get(2) != null);
}

test "SendBuffer overwrites on wrap-around" {
    const allocator = std.testing.allocator;
    var buf = try SendBuffer.init(allocator, 4);
    defer buf.deinit(allocator);

    _ = try buf.store(0, "a", allocator);
    // Wraps to the same slot — reports the evicted byte ("a").
    try std.testing.expectEqual(@as(usize, 1), try buf.store(4, "b", allocator));

    const entry = buf.get(4).?;
    try std.testing.expectEqualSlices(u8, "b", entry.data);
    // Old sequence 0 is gone (overwritten).
    try std.testing.expect(buf.get(0) == null);
}

test "RecvTracker gap detection" {
    const allocator = std.testing.allocator;
    var tracker = try RecvTracker.init(allocator, 64);
    defer tracker.deinit();

    // Receive sequence 0 — no gap.
    const r0 = tracker.recordReceived(0);
    try std.testing.expect(r0.accepted);
    try std.testing.expect(r0.gap == null);
    try std.testing.expectEqual(@as(u64, 1), tracker.next_expected);

    // Receive sequence 1 — still contiguous.
    const r1 = tracker.recordReceived(1);
    try std.testing.expect(r1.accepted);
    try std.testing.expect(r1.gap == null);
    try std.testing.expectEqual(@as(u64, 2), tracker.next_expected);

    // Receive sequence 5 — gap from 2..4.
    const r5 = tracker.recordReceived(5);
    try std.testing.expect(r5.accepted);
    try std.testing.expect(r5.gap != null);
    try std.testing.expectEqual(@as(u64, 2), r5.gap.?.from);
    try std.testing.expectEqual(@as(u32, 3), r5.gap.?.count);

    // getMissing should report 2, 3, 4.
    const missing = try tracker.getMissing(allocator);
    defer allocator.free(missing);
    try std.testing.expectEqual(@as(usize, 3), missing.len);
    try std.testing.expectEqual(@as(u64, 2), missing[0]);
    try std.testing.expectEqual(@as(u64, 3), missing[1]);
    try std.testing.expectEqual(@as(u64, 4), missing[2]);

    // Fill the gap.
    _ = tracker.recordReceived(2);
    _ = tracker.recordReceived(3);
    _ = tracker.recordReceived(4);
    try std.testing.expectEqual(@as(u64, 6), tracker.next_expected);

    // No more missing.
    const missing2 = try tracker.getMissing(allocator);
    defer allocator.free(missing2);
    try std.testing.expectEqual(@as(usize, 0), missing2.len);
}

test "FlowControl credit consume and replenish" {
    var fc = FlowControl.init(1000);

    try std.testing.expectEqual(@as(i64, 1000), fc.available());

    // Consume 400 bytes.
    try std.testing.expect(fc.tryConsume(400));
    try std.testing.expectEqual(@as(i64, 600), fc.available());

    // Consume 600 bytes — should succeed (exactly zero remaining).
    try std.testing.expect(fc.tryConsume(600));
    try std.testing.expectEqual(@as(i64, 0), fc.available());

    // Consume 1 byte — should fail, no credits left.
    try std.testing.expect(!fc.tryConsume(1));

    // Replenish 500 bytes.
    fc.replenish(500);
    try std.testing.expectEqual(@as(i64, 500), fc.available());

    // Now we can consume again.
    try std.testing.expect(fc.tryConsume(250));
    try std.testing.expectEqual(@as(i64, 250), fc.available());
}

test "SendBuffer get returns null for empty buffer" {
    const allocator = std.testing.allocator;
    var buf = try SendBuffer.init(allocator, 8);
    defer buf.deinit(allocator);

    try std.testing.expect(buf.get(0) == null);
    try std.testing.expect(buf.get(1) == null);
    try std.testing.expect(buf.get(7) == null);
}

test "SendBuffer store advances next_sequence" {
    const allocator = std.testing.allocator;
    var buf = try SendBuffer.init(allocator, 8);
    defer buf.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 0), buf.next_sequence);

    _ = try buf.store(0, "a", allocator);
    try std.testing.expectEqual(@as(u64, 1), buf.next_sequence);

    _ = try buf.store(5, "b", allocator);
    try std.testing.expectEqual(@as(u64, 6), buf.next_sequence);
}

test "RecvTracker contiguous receive no gaps" {
    const allocator = std.testing.allocator;
    var tracker = try RecvTracker.init(allocator, 64);
    defer tracker.deinit();

    // Receive 0, 1, 2, 3 in order — no gaps
    for (0..4) |i| {
        const r = tracker.recordReceived(i);
        try std.testing.expect(r.accepted);
        try std.testing.expect(r.gap == null);
    }
    try std.testing.expectEqual(@as(u64, 4), tracker.next_expected);

    const missing = try tracker.getMissing(allocator);
    defer allocator.free(missing);
    try std.testing.expectEqual(@as(usize, 0), missing.len);
}

test "RecvTracker duplicate and old sequences" {
    const allocator = std.testing.allocator;
    var tracker = try RecvTracker.init(allocator, 64);
    defer tracker.deinit();

    try std.testing.expect(tracker.recordReceived(0).accepted);
    try std.testing.expect(tracker.recordReceived(1).accepted);
    try std.testing.expectEqual(@as(u64, 2), tracker.next_expected);

    // A duplicate/replayed sequence must be rejected (delivered only once)
    // and must not regress next_expected.
    try std.testing.expect(!tracker.recordReceived(0).accepted);
    try std.testing.expect(!tracker.recordReceived(1).accepted);
    try std.testing.expectEqual(@as(u64, 2), tracker.next_expected);
}

test "RecvTracker slideWindow" {
    const allocator = std.testing.allocator;
    var tracker = try RecvTracker.init(allocator, 64);
    defer tracker.deinit();

    _ = tracker.recordReceived(0);
    _ = tracker.recordReceived(1);
    _ = tracker.recordReceived(2);

    tracker.slideWindow(2);
    try std.testing.expectEqual(@as(u64, 2), tracker.bitmap_base);
}

test "RecvTracker window slides: gap still detected after > window_size messages" {
    const allocator = std.testing.allocator;
    const window: u64 = 64;
    var tracker = try RecvTracker.init(allocator, window);
    defer tracker.deinit();

    // Receive 200 in-order messages — more than 3x the window. Before the
    // slide fix, bitmap_base was stuck at 0 and the window filled up after
    // `window_size` messages, killing gap detection for good.
    var seq: u64 = 0;
    while (seq < 200) : (seq += 1) {
        const r = tracker.recordReceived(seq);
        try std.testing.expect(r.accepted);
        try std.testing.expect(r.gap == null);
    }
    try std.testing.expectEqual(@as(u64, 200), tracker.next_expected);
    // The window must have slid off its initial base.
    try std.testing.expect(tracker.bitmap_base > 0);
    try std.testing.expectEqual(@as(u64, 200), tracker.contiguousPosition());

    // Drop 200, deliver 201: the gap must still be detected.
    const r = tracker.recordReceived(201);
    try std.testing.expect(r.accepted);
    try std.testing.expect(r.gap != null);
    try std.testing.expectEqual(@as(u64, 200), r.gap.?.from);
    try std.testing.expectEqual(@as(u32, 1), r.gap.?.count);

    var missing_buf: [8]u64 = undefined;
    const n = tracker.getMissingInto(&missing_buf);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u64, 200), missing_buf[0]);

    // Fill the gap: tracker fully contiguous again, and the retransmitted
    // sequence is accepted exactly once.
    try std.testing.expect(tracker.recordReceived(200).accepted);
    try std.testing.expect(!tracker.recordReceived(200).accepted);
    try std.testing.expectEqual(@as(usize, 0), tracker.getMissingInto(&missing_buf));
}

test "RecvTracker rejects far-ahead and u64-max sequences" {
    const allocator = std.testing.allocator;
    var tracker = try RecvTracker.init(allocator, 64);
    defer tracker.deinit();

    try std.testing.expect(tracker.recordReceived(0).accepted);

    // A spoofed sequence of u64 max used to overflow `sequence + 1` and
    // yank next_expected ~2^64 ahead (perpetual bogus NAKs). It must be
    // rejected without touching tracker state.
    const r_max = tracker.recordReceived(std.math.maxInt(u64));
    try std.testing.expect(!r_max.accepted);
    try std.testing.expect(r_max.gap == null);
    try std.testing.expectEqual(@as(u64, 1), tracker.next_expected);

    // Just past the window edge (with the contiguous prefix slid) is still
    // out of window and rejected.
    const r_edge = tracker.recordReceived(1 + 64);
    try std.testing.expect(!r_edge.accepted);
    try std.testing.expectEqual(@as(u64, 1), tracker.next_expected);

    // The last in-window sequence is accepted (window = [1, 65) after slide).
    const r_in = tracker.recordReceived(64);
    try std.testing.expect(r_in.accepted);

    // No bogus NAK state: missing covers only the real gap 1..63.
    var missing_buf: [128]u64 = undefined;
    const n = tracker.getMissingInto(&missing_buf);
    try std.testing.expectEqual(@as(usize, 63), n);
    try std.testing.expectEqual(@as(u64, 1), missing_buf[0]);
    try std.testing.expectEqual(@as(u64, 63), missing_buf[62]);
}

test "FlowControl zero window" {
    var fc = FlowControl.init(0);
    try std.testing.expectEqual(@as(i64, 0), fc.available());
    try std.testing.expect(!fc.tryConsume(1));

    fc.replenish(100);
    try std.testing.expectEqual(@as(i64, 100), fc.available());
    try std.testing.expect(fc.tryConsume(100));
    try std.testing.expect(!fc.tryConsume(1));
}

test "NakMessage layout" {
    try std.testing.expectEqual(@as(usize, 24), @sizeOf(NakMessage));
}

test "RecvTracker advanceContiguous walks past already-set bits" {
    const allocator = std.testing.allocator;
    var tracker = try RecvTracker.init(allocator, 64);
    defer tracker.deinit();

    // Drive the helper's documented contract directly: with a contiguous
    // run of received bits at next_expected, it must advance past all of
    // them (the public path can only ever find an unset bit there, so the
    // walk is otherwise pure defence).
    tracker.received_bitmap.set(0);
    tracker.received_bitmap.set(1);
    tracker.received_bitmap.set(2);
    tracker.advanceContiguous();
    try std.testing.expectEqual(@as(u64, 3), tracker.next_expected);

    // Stops at the first gap.
    tracker.received_bitmap.set(4);
    tracker.advanceContiguous();
    try std.testing.expectEqual(@as(u64, 3), tracker.next_expected);
}

test "RecvTracker getMissing surfaces allocation failure without leaking" {
    const allocator = std.testing.allocator;
    var tracker = try RecvTracker.init(allocator, 64);
    defer tracker.deinit();

    _ = tracker.recordReceived(0);
    _ = tracker.recordReceived(5); // gap: 1..4 missing

    // The first append allocation fails: the errdefer must clean up the
    // partial list (the testing allocator's leak check verifies it).
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, tracker.getMissing(failing.allocator()));

    // The tracker itself is unharmed and reports the gap normally.
    const missing = try tracker.getMissing(allocator);
    defer allocator.free(missing);
    try std.testing.expectEqual(@as(usize, 4), missing.len);
}

test "FlowControl concurrent consume stays exact under contention" {
    var fc = FlowControl.init(1_000_000_000);

    // Two threads hammering tryConsume on the same cache line force the
    // weak CAS to fail and retry; the final balance must still be exact.
    const per_thread = 50_000;
    const Worker = struct {
        fn consume(f: *FlowControl, n: usize) void {
            var done: usize = 0;
            while (done < n) {
                if (f.tryConsume(1)) done += 1;
            }
        }
    };
    var t1 = try std.Thread.spawn(.{}, Worker.consume, .{ &fc, per_thread });
    var t2 = try std.Thread.spawn(.{}, Worker.consume, .{ &fc, per_thread });
    t1.join();
    t2.join();

    try std.testing.expectEqual(@as(i64, 1_000_000_000 - 2 * per_thread), fc.available());
}
