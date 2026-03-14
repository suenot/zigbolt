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

        /// Consumer tail — only mutated by the single reader.
        tail: usize align(config.cache_line_size) = 0,

        /// Backing storage for the ring buffer.
        buffer: [capacity]u8 align(config.cache_line_size) = [_]u8{0} ** capacity,

        /// Return an initialized ring buffer (all zeros).
        pub fn init() Self {
            return .{};
        }

        /// Write `data` into the ring buffer with the given `msg_type_id`.
        ///
        /// Returns `true` on success, `false` if there is not enough space.
        /// Thread-safe for multiple concurrent producers.
        pub fn write(self: *Self, data: []const u8, msg_type_id: i32) bool {
            const payload_len: u32 = @intCast(data.len);
            const total_len: usize = @intCast(frame.alignedFrameLength(payload_len));

            // Phase 1: CAS loop to claim space.
            while (true) {
                const current_head = self.head.load(.monotonic);
                // Read tail with acquire to see the latest consumer progress.
                const current_tail = @atomicLoad(usize, &self.tail, .acquire);

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
                    // Commit padding frame with negative frame_length.
                    // The absolute value is the padding payload length (total padding minus header).
                    @atomicStore(i32, &pad_header.frame_length, -@as(i32, @intCast(padding_len - frame.FrameHeader.SIZE)), .release);

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
        pub fn read(self: *Self) ?ReadResult {
            var current_tail = self.tail;

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
                    // Padding frame — skip it and advance tail past the padding.
                    const padding_payload: u32 = @intCast(-fl);
                    const padding_total: usize = @intCast(frame.alignedFrameLength(padding_payload));

                    // Zero out the header to reset the slot for reuse.
                    @atomicStore(i32, @constCast(&header_ptr.frame_length), 0, .monotonic);

                    // Advance tail past the padding frame.
                    current_tail = current_tail + padding_total;
                    self.tail = current_tail;
                    @atomicStore(usize, &self.tail, current_tail, .release);

                    // Loop to read the actual data frame that follows.
                    continue;
                }

                const payload_len: u32 = @intCast(fl);
                const total_len: usize = @intCast(frame.alignedFrameLength(payload_len));

                const payload_start = offset + frame.FrameHeader.SIZE;
                const data = self.buffer[payload_start..][0..payload_len];
                const msg_type_id = header_ptr.msg_type_id;

                // Zero out the header to reset the slot for reuse.
                @atomicStore(i32, @constCast(&header_ptr.frame_length), 0, .monotonic);

                // Advance tail — release so producers see freed space.
                current_tail = current_tail + total_len;
                self.tail = current_tail;
                @atomicStore(usize, &self.tail, current_tail, .release);

                return ReadResult{
                    .data = data,
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
