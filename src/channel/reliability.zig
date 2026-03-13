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
    pub fn store(self: *SendBuffer, sequence: u64, data: []const u8, allocator: std.mem.Allocator) !void {
        const idx = sequence % self.capacity;
        const entry = &self.entries[idx];

        // Free old data if slot was occupied.
        if (entry.occupied and entry.data.len > 0) {
            allocator.free(entry.data);
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
            self.next_sequence = sequence + 1;
        }
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
    pub fn release(self: *SendBuffer, up_to_sequence: u64) void {
        for (self.entries) |*entry| {
            if (entry.occupied and entry.sequence < up_to_sequence) {
                if (entry.data.len > 0) {
                    self.allocator.free(entry.data);
                    entry.data = &.{};
                }
                entry.occupied = false;
            }
        }
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
    /// Returns gap info if the received sequence is ahead of what we expected
    /// (indicating missing intermediate sequences).
    pub fn recordReceived(self: *RecvTracker, sequence: u64) ?GapInfo {
        // Ignore sequences behind our window.
        if (sequence < self.bitmap_base) {
            return null;
        }

        const offset = sequence - self.bitmap_base;
        if (offset < self.window_size) {
            self.received_bitmap.set(offset);
        }

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
            self.next_expected = sequence + 1;
            self.advanceContiguous();
        }

        return gap;
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

        // Scan from bitmap_base up to next_expected for unset bits.
        const scan_end = @min(self.next_expected - self.bitmap_base, self.window_size);
        var i: u64 = 0;
        while (i < scan_end) : (i += 1) {
            if (!self.received_bitmap.isSet(i)) {
                try missing.append(allocator, self.bitmap_base + i);
            }
        }

        return missing.toOwnedSlice(allocator);
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
        }
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

    // Store a few entries.
    try buf.store(0, "hello", allocator);
    try buf.store(1, "world", allocator);
    try buf.store(2, "zigbolt", allocator);

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

    // Release up to sequence 2 (exclusive) — removes 0 and 1.
    buf.release(2);
    try std.testing.expect(buf.get(0) == null);
    try std.testing.expect(buf.get(1) == null);
    try std.testing.expect(buf.get(2) != null);
}

test "SendBuffer overwrites on wrap-around" {
    const allocator = std.testing.allocator;
    var buf = try SendBuffer.init(allocator, 4);
    defer buf.deinit(allocator);

    try buf.store(0, "a", allocator);
    try buf.store(4, "b", allocator); // wraps to same slot

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
    try std.testing.expect(r0 == null);
    try std.testing.expectEqual(@as(u64, 1), tracker.next_expected);

    // Receive sequence 1 — still contiguous.
    const r1 = tracker.recordReceived(1);
    try std.testing.expect(r1 == null);
    try std.testing.expectEqual(@as(u64, 2), tracker.next_expected);

    // Receive sequence 5 — gap from 2..4.
    const r5 = tracker.recordReceived(5);
    try std.testing.expect(r5 != null);
    try std.testing.expectEqual(@as(u64, 2), r5.?.from);
    try std.testing.expectEqual(@as(u32, 3), r5.?.count);

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
