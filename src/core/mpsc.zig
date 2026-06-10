const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("frame.zig");

/// Multi-Producer Single-Consumer ring buffer.
///
/// Uses CAS-based two-phase commit for lock-free writes:
///   1. CAS claims space by advancing `head`
///   2. Payload is copied into the claimed slot
///   3. `frame_length` is stored with `.release` to commit
///
/// The single consumer reads committed frames sequentially from `tail`.
/// Uncommitted slots (frame_length == 0) cause the reader to return null,
/// preserving ordering.
///
/// **Limitation (C3):** If a producer claims a slot (phase 1) but crashes or
/// stalls before committing (phase 2), the reader will be blocked forever at
/// that slot since frame_length remains 0. This is inherent to two-phase
/// commit MPSC designs (Aeron has the same limitation). A timeout mechanism
/// would be needed to recover from this, but is not implemented here.
pub fn MpscRingBuffer(comptime capacity: usize) type {
    comptime {
        if (capacity == 0) @compileError("capacity must be > 0");
        if (capacity & (capacity - 1) != 0) @compileError("capacity must be a power of 2");
    }

    const mask: usize = capacity - 1;

    return struct {
        const Self = @This();

        /// Result returned by a successful read.
        pub const ReadResult = struct {
            data: []const u8,
            msg_type_id: i32,
        };

        /// Producer head — advanced atomically via CAS.
        head: std.atomic.Value(usize) align(config.cache_line_size) = std.atomic.Value(usize).init(0),

        /// Consumer tail — only advanced by the single reader, but stored
        /// atomically because producers acquire-load it to compute free space.
        tail: std.atomic.Value(usize) align(config.cache_line_size) = std.atomic.Value(usize).init(0),

        /// Backing storage for the ring buffer.
        buffer: [capacity]u8 align(config.cache_line_size) = [_]u8{0} ** capacity,

        /// Scratch buffer the consumer copies payloads into before releasing
        /// the consumed region back to the producers. The slice returned by
        /// `read()` stays valid until the next `read()` call.
        _scratch: [capacity]u8 = undefined,

        /// Return an initialized ring buffer (all zeros).
        pub fn init() Self {
            return .{};
        }

        /// Write `data` into the ring buffer with the given `msg_type_id`.
        ///
        /// Returns `true` on success, `false` if there is not enough space.
        /// Thread-safe for multiple concurrent producers.
        pub fn write(self: *Self, data: []const u8, msg_type_id: i32) bool {
            // Reject payloads whose frame arithmetic would overflow u32
            // (a data.len near 4 GB would wrap @intCast/alignedFrameLength).
            if (data.len > frame.MAX_PAYLOAD_SIZE) return false;

            const payload_len: u32 = @intCast(data.len);
            const total_len: usize = @intCast(frame.alignedFrameLength(payload_len));

            // Phase 1: CAS loop to claim space.
            while (true) {
                const current_head = self.head.load(.monotonic);
                // Read tail with acquire to see the latest consumer progress.
                const current_tail = self.tail.load(.acquire);

                // The head snapshot can be stale relative to the tail: another
                // producer advances head, then the consumer advances tail past
                // our snapshot. `head -% tail` would underflow to a huge value
                // and report a spurious "full" — reload and retry instead.
                if (current_tail > current_head) continue;

                const offset = current_head & mask;

                // Check if the claim would cross the wrap boundary.
                if (offset + total_len > capacity) {
                    // Need to insert padding to fill the remainder, then claim
                    // from offset 0. Calculate how much padding is needed.
                    const padding_len = capacity - offset;
                    const claim_len = padding_len + total_len;

                    const used = current_head -% current_tail;
                    if (used + claim_len > capacity) {
                        return false; // not enough space for padding + message
                    }

                    const new_head = current_head + claim_len;
                    // H2 fix: use .acq_rel on CAS success ordering.
                    if (self.head.cmpxchgWeak(current_head, new_head, .acq_rel, .monotonic)) |_| {
                        continue; // CAS failed, retry
                    }

                    // CAS succeeded — write padding frame at offset.
                    const pad_header: *frame.FrameHeader = @ptrCast(@alignCast(&self.buffer[offset]));
                    pad_header.msg_type_id = 0;
                    // Commit padding with a negative TOTAL length (header
                    // included — same convention as log_buffer). Encoding the
                    // payload length instead would make an 8-byte header-only
                    // padding commit as -(8 - 8) == 0 == "uncommitted" and
                    // wedge the consumer forever.
                    @atomicStore(i32, &pad_header.frame_length, -@as(i32, @intCast(padding_len)), .release);

                    // Now write the actual message at offset 0.
                    const real_header: *frame.FrameHeader = @ptrCast(@alignCast(&self.buffer[0]));
                    real_header.msg_type_id = msg_type_id;

                    const dest = self.buffer[frame.FrameHeader.SIZE..][0..data.len];
                    @memcpy(dest, data);

                    // Phase 2: Commit the real frame.
                    @atomicStore(i32, &real_header.frame_length, @intCast(data.len), .release);

                    return true;
                }

                // No wrap — normal path.
                const used = current_head -% current_tail;
                if (used + total_len > capacity) {
                    return false; // not enough space
                }

                const new_head = current_head + total_len;
                // H2 fix: use .acq_rel on CAS success ordering.
                if (self.head.cmpxchgWeak(current_head, new_head, .acq_rel, .monotonic)) |_| {
                    continue; // CAS failed, retry
                }

                // CAS succeeded — we own [current_head, current_head + total_len).
                // No wrap-around: safe to use direct slicing.
                const header_ptr: *frame.FrameHeader = @ptrCast(@alignCast(&self.buffer[offset]));
                header_ptr.msg_type_id = msg_type_id;

                // Write the payload after the header.
                const payload_start = offset + frame.FrameHeader.SIZE;
                const dest = self.buffer[payload_start..][0..data.len];
                @memcpy(dest, data);

                // Phase 2: Commit by storing frame_length with release semantics.
                // This makes the payload visible to the consumer.
                @atomicStore(i32, &header_ptr.frame_length, @intCast(data.len), .release);

                return true;
            }
        }

        /// Read the next committed frame from the ring buffer.
        ///
        /// Returns `null` if no committed frame is available (either empty
        /// or the next slot is still uncommitted).
        /// Must only be called from a single consumer thread.
        ///
        /// The returned slice points into a consumer-owned scratch buffer and
        /// remains valid until the NEXT call to `read()`.
        pub fn read(self: *Self) ?ReadResult {
            var current_tail = self.tail.load(.monotonic);

            while (true) {
                const current_head = self.head.load(.acquire);

                if (current_tail == current_head) {
                    return null; // ring buffer is empty
                }

                const offset = current_tail & mask;
                const header_ptr: *const frame.FrameHeader = @ptrCast(@alignCast(&self.buffer[offset]));

                // Check if the frame is committed (frame_length != 0).
                const fl = @atomicLoad(i32, &header_ptr.frame_length, .acquire);
                if (fl == 0) {
                    return null; // not yet committed, spin/retry later
                }

                if (frame.isPaddingFrame(fl)) {
                    // Padding frame — |frame_length| is the TOTAL padding size
                    // (header included), so even an 8-byte header-only padding
                    // is a non-zero committed value.
                    const padding_total: usize = @intCast(-fl);

                    // Zero the entire consumed padding region before releasing
                    // the tail so the next lap starts from a clean slate.
                    @memset(self.buffer[offset..][0..padding_total], 0);

                    // Advance tail past the padding frame — single atomic
                    // release store (producers acquire-load this; an extra
                    // plain store would be a data race).
                    current_tail = current_tail + padding_total;
                    self.tail.store(current_tail, .release);

                    // Loop to read the actual data frame that follows.
                    continue;
                }

                const payload_len: u32 = @intCast(fl);
                const total_len: usize = @intCast(frame.alignedFrameLength(payload_len));

                const payload_start = offset + frame.FrameHeader.SIZE;
                const msg_type_id = header_ptr.msg_type_id;

                // Copy the payload out BEFORE releasing the tail: once the
                // release store below is visible, producers may immediately
                // reuse this region, so returning a slice into `buffer` would
                // be a use-after-release data race.
                @memcpy(self._scratch[0..payload_len], self.buffer[payload_start..][0..payload_len]);

                // Aeron-style consumer hygiene: zero the ENTIRE consumed
                // region (header + payload + alignment padding), not just the
                // frame_length word. With variable-size messages, a later
                // lap can place a frame header on bytes that were payload in
                // this lap; stale non-zero bytes there would be misread as a
                // committed frame_length.
                @memset(self.buffer[offset..][0..total_len], 0);

                // Advance tail — single atomic release store so producers
                // see the freed (and zeroed) space.
                current_tail = current_tail + total_len;
                self.tail.store(current_tail, .release);

                return ReadResult{
                    .data = self._scratch[0..payload_len],
                    .msg_type_id = msg_type_id,
                };
            }
        }
    };
}

// =============================================================================
// Tests
// =============================================================================

test "single producer write and read" {
    var rb = MpscRingBuffer(1024).init();

    const payload = "hello world";
    const ok = rb.write(payload, 42);
    try std.testing.expect(ok);

    const result = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, payload, result.data);
    try std.testing.expectEqual(@as(i32, 42), result.msg_type_id);

    // After reading, buffer should be empty.
    try std.testing.expect(rb.read() == null);
}

test "uncommitted frames are not visible to reader" {
    const RB = MpscRingBuffer(1024);
    var rb = RB.init();

    const payload = "test data";
    const payload_len: u32 = @intCast(payload.len);
    const total_len: usize = @intCast(frame.alignedFrameLength(payload_len));

    // Manually simulate a CAS claim without committing:
    // advance head but leave frame_length == 0.
    rb.head = std.atomic.Value(usize).init(total_len);

    // Reader should see no committed frame.
    try std.testing.expect(rb.read() == null);
}

test "read result survives producer reusing the region (no use-after-release)" {
    // Capacity 32: one 24-byte-payload frame fills the whole ring, so the
    // next write reuses exactly the bytes of the frame we just read.
    var rb = MpscRingBuffer(32).init();

    var msg_a: [24]u8 = undefined;
    for (&msg_a, 0..) |*b, i| b.* = @intCast(i + 1);
    var msg_b: [24]u8 = undefined;
    for (&msg_b, 0..) |*b, i| b.* = @intCast(0xC0 - i);

    try std.testing.expect(rb.write(&msg_a, 1));
    const r = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(i32, 1), r.msg_type_id);

    // Producer reuses the freed region with a same-size message.
    try std.testing.expect(rb.write(&msg_b, 2));

    // The slice returned by the earlier read must still hold msg A even
    // though the producer overwrote the ring region it came from.
    try std.testing.expectEqualSlices(u8, &msg_a, r.data);

    const r2 = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, &msg_b, r2.data);
    try std.testing.expectEqual(@as(i32, 2), r2.msg_type_id);
}

test "consumed region is fully zeroed: stale payload not misread as a committed header" {
    var rb = MpscRingBuffer(64).init();

    // Frame A occupies [0..32): header [0..8), payload [8..32).
    // Craft A's payload so that buffer bytes [16..24) — payload bytes 8..16 —
    // look exactly like a committed 8-byte frame header (frame_length = 8,
    // msg_type_id = 99) left over after A is consumed.
    var msg_a: [24]u8 = @splat(0xEE);
    std.mem.writeInt(i32, msg_a[8..12], 8, .little); // fake frame_length
    std.mem.writeInt(i32, msg_a[12..16], 99, .little); // fake msg_type_id

    const msg_b: [24]u8 = @splat(0xBB);

    try std.testing.expect(rb.write(&msg_a, 1)); // frame at [0..32)
    try std.testing.expect(rb.write(&msg_b, 2)); // frame at [32..64)
    _ = rb.read() orelse return error.TestUnexpectedResult;
    _ = rb.read() orelse return error.TestUnexpectedResult;

    // Lap 2: a real committed 16-byte frame at offset 0...
    try std.testing.expect(rb.write("CCCCCCCC", 3)); // frame at [0..16)
    const r = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, "CCCCCCCC", r.data);

    // ...then simulate a producer that CLAIMED a 16-byte frame at offset 16
    // (where A's crafted payload bytes used to live) but has not committed.
    rb.head.store(rb.head.load(.monotonic) + 16, .monotonic);

    // The reader must see an uncommitted slot (zeroed region), NOT the stale
    // fake header (frame_length 8 / type 99) from last lap's payload.
    try std.testing.expect(rb.read() == null);
}

test "wrap padding of exactly one header does not wedge the consumer" {
    var rb = MpscRingBuffer(64).init();

    // Land the head at offset 56: frames of 16 + 16 + 24 bytes.
    try std.testing.expect(rb.write("aaaaaaaa", 1)); // 16B frame at 0
    try std.testing.expect(rb.write("bbbbbbbb", 2)); // 16B frame at 16
    try std.testing.expect(rb.write("cccccccccccccccc", 3)); // 24B frame at 32

    const r1 = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, "aaaaaaaa", r1.data);
    const r2 = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, "bbbbbbbb", r2.data);
    const r3 = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, "cccccccccccccccc", r3.data);

    // head == tail == 56; only 8 bytes remain before the wrap boundary, so
    // this write inserts a header-only (8-byte) padding frame at offset 56.
    // The padding must be committed as a non-zero value or the consumer
    // would stall on an "uncommitted" slot forever.
    try std.testing.expect(rb.write("dddddddd", 4)); // 16B frame at 0 after padding

    const r4 = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualSlices(u8, "dddddddd", r4.data);
    try std.testing.expectEqual(@as(i32, 4), r4.msg_type_id);
    try std.testing.expect(rb.read() == null);
}

test "variable-size messages round-trip across multiple wraps" {
    var rb = MpscRingBuffer(128).init();

    const sizes = [_]usize{ 4, 12, 20, 28, 8, 16 };
    for (0..60) |round| {
        const size = sizes[round % sizes.len];
        var payload: [32]u8 = undefined;
        for (payload[0..size], 0..) |*b, i| {
            b.* = @truncate(round * 31 + i);
        }
        try std.testing.expect(rb.write(payload[0..size], @intCast(round)));

        const r = rb.read() orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualSlices(u8, payload[0..size], r.data);
        try std.testing.expectEqual(@as(i32, @intCast(round)), r.msg_type_id);
    }
}

test "oversized message is rejected" {
    var rb = MpscRingBuffer(64).init();
    // Slice with a huge length and no real backing memory — write() must
    // reject it via the MAX_PAYLOAD_SIZE guard before touching any byte.
    var dummy: [1]u8 = .{0};
    const huge = @as([*]const u8, &dummy)[0 .. @as(usize, frame.MAX_PAYLOAD_SIZE) + 1];
    try std.testing.expect(!rb.write(huge, 1));

    // A length near 4 GB previously overflowed u32 in alignedFrameLength.
    const near_4g = @as([*]const u8, &dummy)[0 .. (@as(usize, 1) << 32) - 4];
    try std.testing.expect(!rb.write(near_4g, 1));
}

test "wrap-around write and read" {
    // Use a small ring buffer to force wrapping.
    const cap = 64;
    var rb = MpscRingBuffer(cap).init();

    // Write and read several messages so that head wraps around.
    var i: i32 = 0;
    while (i < 10) : (i += 1) {
        const msg = "wrap";
        const ok = rb.write(msg, i);
        try std.testing.expect(ok);

        const result = rb.read() orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualSlices(u8, msg, result.data);
        try std.testing.expectEqual(i, result.msg_type_id);
    }

    // Head should have wrapped past capacity.
    const head_val = rb.head.load(.monotonic);
    try std.testing.expect(head_val > cap);
}
