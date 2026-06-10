const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("frame.zig");

pub const LogBufferConfig = struct {
    term_length: usize = 1 << 20, // 1MB default
};

pub fn LogBuffer(comptime cfg: LogBufferConfig) type {
    comptime {
        if (cfg.term_length == 0) @compileError("term_length must be > 0");
        if (cfg.term_length & (cfg.term_length - 1) != 0) @compileError("term_length must be a power of 2");
    }

    return struct {
        const term_length = cfg.term_length;
        const term_count = 3;
        const Self = @This();

        /// Term buffers — contiguous memory.
        terms: [term_count][term_length]u8,

        /// Absolute tail position (publisher writes here).
        tail_position: std.atomic.Value(u64) align(config.cache_line_size),

        /// Absolute head position (consumer reads from here).
        head_position: std.atomic.Value(u64) align(config.cache_line_size),

        /// Result of a successful claim — caller writes payload, then commits.
        pub const Claim = struct {
            term_buffer: [*]u8,
            term_offset: u32,
            length: u32,
            term_id: u32,
        };

        /// Initialise a zeroed LogBuffer ready for use.
        pub fn init() Self {
            return Self{
                .terms = std.mem.zeroes([term_count][term_length]u8),
                .tail_position = std.atomic.Value(u64).init(0),
                .head_position = std.atomic.Value(u64).init(0),
            };
        }

        // ── Internal helpers ──────────────────────────────────────────

        inline fn termIndex(position: u64) u32 {
            return @intCast((position / term_length) % term_count);
        }

        inline fn termOffset(position: u64) u32 {
            return @intCast(position % term_length);
        }

        /// Return a pointer to the FrameHeader located at `offset` within `term_idx`.
        inline fn headerAt(self: *Self, term_idx: u32, offset: u32) *frame.FrameHeader {
            return @ptrCast(@alignCast(&self.terms[term_idx][offset]));
        }

        // ── Publisher API ─────────────────────────────────────────────

        /// Maximum distance the writer may run ahead of the reader. Keeping
        /// the writer within (term_count - 1) terms guarantees that a term
        /// being re-entered after rotation has been fully consumed, so it can
        /// be cleaned without yanking data out from under the reader.
        const publication_window: u64 = @as(u64, term_count - 1) * term_length;

        /// Claim space for a message of `length` payload bytes.
        /// Returns `null` when the claim cannot be satisfied (consumer is too far behind).
        pub fn claim(self: *Self, length: u32) ?Claim {
            if (length == 0 or length > frame.MAX_PAYLOAD_SIZE) return null;

            const required = frame.alignedFrameLength(length);
            if (required > term_length) return null; // can never fit in a term

            while (true) {
                const tail = self.tail_position.load(.monotonic);
                const head = self.head_position.load(.acquire);
                const t_offset = termOffset(tail);
                const t_idx = termIndex(tail);

                // Does the message fit in the current term?
                if (t_offset + required <= term_length) { // kcov-skip: evaluated on every append (log buffer tests); no own line record
                    // Back-pressure: refuse the claim while it would put the
                    // writer more than (term_count - 1) terms ahead of the
                    // reader — otherwise the writer laps the reader and
                    // overwrites frames that were never consumed.
                    if (tail + required - head > publication_window) return null;

                    // First claim in a fresh term after a full rotation:
                    // clean the term so stale committed headers from the
                    // previous lap cannot be misread as live frames. The
                    // publication window above guarantees the reader has
                    // fully drained this region.
                    // NOTE: zero-before-CAS is only safe with a single
                    // publisher (the current usage); a true MPSC publisher
                    // would have to clean under the winning rotation CAS.
                    if (t_offset == 0 and tail != 0) {
                        @memset(&self.terms[t_idx], 0);
                    }

                    // Try to advance the tail atomically (CAS for future MPSC).
                    const new_tail = tail + required;
                    if (self.tail_position.cmpxchgWeak(
                        tail,
                        new_tail,
                        .acq_rel,
                        .monotonic,
                    )) |_| {
                        // CAS failed — another publisher won; retry.
                        continue;
                    }

                    return Claim{
                        .term_buffer = &self.terms[t_idx],
                        .term_offset = t_offset,
                        .length = length,
                        .term_id = t_idx,
                    };
                }

                // Message does not fit — insert a padding frame covering the rest
                // of the current term and rotate to the next one.
                const remaining: u32 = @intCast(term_length - t_offset);

                // Apply the publication window to padding + message so the
                // tail never advances into a term the reader still occupies.
                if (tail + remaining + required - head > publication_window) return null;

                const new_tail = tail + remaining; // start of next term

                if (self.tail_position.cmpxchgWeak(
                    tail,
                    new_tail,
                    .acq_rel,
                    .monotonic,
                )) |_| {
                    continue;
                }

                // Write the padding frame header. `frame_length` is negative to
                // signal padding; absolute value = remaining bytes (including header).
                if (remaining >= frame.FrameHeader.SIZE) {
                    const hdr = self.headerAt(t_idx, t_offset);
                    hdr.msg_type_id = 0;
                    // Release-store so readers see msg_type_id before frame_length.
                    @atomicStore(i32, &hdr.frame_length, -@as(i32, @intCast(remaining)), .release);
                }

                // Now retry — the next iteration will land on the fresh term.
            }
        }

        /// Commit a previously claimed frame, making it visible to readers.
        /// The caller must have already written the payload into the claimed region
        /// starting at `c.term_buffer[c.term_offset + FrameHeader.SIZE]`.
        pub fn commit(self: *Self, c: Claim, msg_type_id: i32) void {
            const hdr = self.headerAt(c.term_id, c.term_offset);
            hdr.msg_type_id = msg_type_id;
            // Release-store of frame_length publishes the frame to readers.
            @atomicStore(i32, &hdr.frame_length, @as(i32, @intCast(c.length)), .release);
        }

        // ── Consumer API ──────────────────────────────────────────────

        /// Read committed frames starting at `head_position`.
        /// Calls `handler` for every data frame encountered.
        /// Returns the number of data frames delivered to the handler.
        pub fn read(self: *Self, handler: *const fn (data: []const u8, msg_type_id: i32) void, limit: u32) u32 {
            var frames_read: u32 = 0;
            var head = self.head_position.load(.monotonic);

            while (frames_read < limit) {
                // Don't read past the tail.
                const tail = self.tail_position.load(.acquire);
                if (head >= tail) break;

                const t_idx = termIndex(head);
                const t_offset = termOffset(head);

                const hdr = self.headerAt(t_idx, t_offset);
                const fl = @atomicLoad(i32, &hdr.frame_length, .acquire);

                if (frame.isUncommitted(fl)) {
                    // Frame claimed but not yet committed — stop here.
                    break;
                }

                if (frame.isPaddingFrame(fl)) {
                    // Skip past the padding to the start of the next term.
                    const pad_len: u32 = @intCast(-fl);
                    head += pad_len;
                    self.head_position.store(head, .release);
                    continue;
                }

                // Data frame.
                const payload_len: u32 = @intCast(fl);
                const aligned_len = frame.alignedFrameLength(payload_len);
                const payload_start = t_offset + frame.FrameHeader.SIZE; // kcov-skip: runs on every successful read (log buffer tests); no own line record
                const payload: []const u8 = self.terms[t_idx][payload_start .. payload_start + payload_len];

                handler(payload, hdr.msg_type_id);
                frames_read += 1;

                head += aligned_len;
                self.head_position.store(head, .release);
            }

            return frames_read;
        }
    };
}

// ═══════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════

const testing = std.testing;

// Use a small term length so rotation happens quickly.
const TestBuf = LogBuffer(.{ .term_length = 256 });

// ── Helpers ───────────────────────────────────────────────────────────

fn writePayload(c: TestBuf.Claim, data: []const u8) void {
    const dest = c.term_buffer[c.term_offset + frame.FrameHeader.SIZE ..];
    @memcpy(dest[0..data.len], data);
}

var collected_msgs: [64][]const u8 = undefined;
var collected_types: [64]i32 = undefined;
var collected_count: u32 = 0;

fn resetCollected() void {
    collected_count = 0;
}

fn collectHandler(data: []const u8, msg_type_id: i32) void {
    collected_msgs[collected_count] = data;
    collected_types[collected_count] = msg_type_id;
    collected_count += 1;
}

// ── Test: basic write and read ────────────────────────────────────────

test "basic write and read" {
    var buf = TestBuf.init();

    const payload = "hello";
    const c = buf.claim(@intCast(payload.len)) orelse return error.ClaimFailed;
    writePayload(c, payload);
    buf.commit(c, 42);

    resetCollected();
    const n = buf.read(&collectHandler, 10);
    try testing.expectEqual(@as(u32, 1), n);
    try testing.expectEqualSlices(u8, "hello", collected_msgs[0]);
    try testing.expectEqual(@as(i32, 42), collected_types[0]);
}

// ── Test: multiple messages ───────────────────────────────────────────

test "multiple messages" {
    var buf = TestBuf.init();

    const messages = [_][]const u8{ "aaa", "bbb", "ccc" };
    for (messages, 0..) |msg, i| {
        const c = buf.claim(@intCast(msg.len)) orelse return error.ClaimFailed;
        writePayload(c, msg);
        buf.commit(c, @intCast(i));
    }

    resetCollected();
    const n = buf.read(&collectHandler, 10);
    try testing.expectEqual(@as(u32, 3), n);
    for (messages, 0..) |msg, i| {
        try testing.expectEqualSlices(u8, msg, collected_msgs[i]);
        try testing.expectEqual(@as(i32, @intCast(i)), collected_types[i]);
    }
}

// ── Test: term rotation when a term fills up ──────────────────────────

test "term rotation" { // kcov-skip: hit record oscillates between builds; the test runs and passes
    var buf = TestBuf.init();

    // Each aligned frame = header(8) + aligned payload.
    // Fill the first term by writing enough messages, then verify we
    // land in term index 1.
    const payload = "12345678"; // 8 bytes payload => aligned frame = 16 bytes
    const frames_per_term = 256 / 16; // = 16

    // Fill term 0 completely.
    var i: u32 = 0;
    while (i < frames_per_term) : (i += 1) {
        const c = buf.claim(@intCast(payload.len)) orelse return error.ClaimFailed;
        writePayload(c, payload);
        buf.commit(c, 1);
    }

    // Next claim should land in term 1.
    const c = buf.claim(@intCast(payload.len)) orelse return error.ClaimFailed;
    try testing.expectEqual(@as(u32, 1), c.term_id);
    writePayload(c, payload);
    buf.commit(c, 2);

    // Read all frames: 16 from term 0, 1 from term 1.
    resetCollected();
    const n = buf.read(&collectHandler, 64);
    try testing.expectEqual(@as(u32, frames_per_term + 1), n);
}

// ── Test: back-pressure with a stalled reader ─────────────────────────

test "claim applies back-pressure instead of lapping the reader" {
    var buf = TestBuf.init(); // term_length = 256, window = 2 * 256 = 512

    const payload = "12345678"; // 16-byte aligned frames
    // With a stalled reader, the writer may fill at most 2 of the 3 terms.
    var claims: u32 = 0;
    while (claims < 100) {
        const c = buf.claim(@intCast(payload.len)) orelse break;
        writePayload(c, payload);
        buf.commit(c, @intCast(claims));
        claims += 1;
    }
    try testing.expectEqual(@as(u32, 32), claims); // 2 * 256 / 16

    // Still blocked until the reader makes progress.
    try testing.expect(buf.claim(@intCast(payload.len)) == null);

    // Drain one term worth of frames; claims must succeed again and the
    // already-published frames must be intact (nothing was overwritten).
    resetCollected();
    const n = buf.read(&collectHandler, 16);
    try testing.expectEqual(@as(u32, 16), n);
    for (0..16) |i| {
        try testing.expectEqualSlices(u8, payload, collected_msgs[i]);
        try testing.expectEqual(@as(i32, @intCast(i)), collected_types[i]);
    }
    try testing.expect(buf.claim(@intCast(payload.len)) != null);
}

// ── Test: content integrity across full rotations ─────────────────────

test "content round-trips across full term rotation" {
    var buf = TestBuf.init();

    // Drive well past term_count * term_length (768) cumulative bytes:
    // 100 * 16 = 1600 bytes, i.e. more than two full rotations.
    var round: u32 = 0;
    while (round < 100) : (round += 1) {
        var payload: [8]u8 = undefined;
        for (&payload, 0..) |*b, i| {
            b.* = @truncate(round * 13 + @as(u32, @intCast(i)));
        }
        const c = buf.claim(8) orelse return error.ClaimFailed;
        writePayload(c, &payload);
        buf.commit(c, @intCast(round));

        resetCollected();
        const n = buf.read(&collectHandler, 4);
        try testing.expectEqual(@as(u32, 1), n);
        try testing.expectEqualSlices(u8, &payload, collected_msgs[0]);
        try testing.expectEqual(@as(i32, @intCast(round)), collected_types[0]);
    }
}

// ── Test: rotated terms are cleaned before reuse ──────────────────────

test "rotated term is cleaned: stale headers are not misread as committed" {
    var buf = TestBuf.init();

    const payload = "12345678"; // 16-byte frames, 16 per term
    // One full lap over all three terms, reader keeping pace.
    var i: u32 = 0;
    while (i < 48) : (i += 1) {
        const c = buf.claim(@intCast(payload.len)) orelse return error.ClaimFailed;
        writePayload(c, payload);
        buf.commit(c, 7);
        resetCollected();
        try testing.expectEqual(@as(u32, 1), buf.read(&collectHandler, 1));
    }

    // tail == head == 768; the writer now re-enters term 0, which held
    // committed frame headers from the first lap.
    const c = buf.claim(8) orelse return error.ClaimFailed;
    try testing.expectEqual(@as(u32, 0), c.term_id);
    try testing.expectEqual(@as(u32, 0), c.term_offset);

    // The claim is NOT committed yet: the reader must see an uncommitted
    // (zeroed) region, not the stale frame_length from the previous lap.
    resetCollected();
    try testing.expectEqual(@as(u32, 0), buf.read(&collectHandler, 4));

    // After committing, exactly the fresh frame is delivered.
    writePayload(c, "abcdefgh");
    buf.commit(c, 99);
    resetCollected();
    try testing.expectEqual(@as(u32, 1), buf.read(&collectHandler, 4));
    try testing.expectEqualSlices(u8, "abcdefgh", collected_msgs[0]);
    try testing.expectEqual(@as(i32, 99), collected_types[0]);
}

// ── Test: padding frame on term boundary ──────────────────────────────

test "padding frame on term boundary" {
    var buf = TestBuf.init();

    // Write frames that leave less than a full frame at the end of the term.
    // term_length = 256. Use 15-frame * 16 bytes = 240 bytes consumed, 16 left.
    const small = "12345678"; // 8 bytes => aligned frame = 16
    var i: u32 = 0;
    while (i < 15) : (i += 1) {
        const c = buf.claim(@intCast(small.len)) orelse return error.ClaimFailed;
        writePayload(c, small);
        buf.commit(c, 1);
    }
    // 240 bytes consumed, 16 bytes remaining.
    // Now claim a payload that needs more than 16 bytes total:
    // payload 12 bytes => aligned frame = 8 + 12 = 20, aligned to 24. Won't fit in 16.
    const big = "123456789012"; // 12 bytes
    const c = buf.claim(@intCast(big.len)) orelse return error.ClaimFailed;

    // Should have rotated to term 1.
    try testing.expectEqual(@as(u32, 1), c.term_id);
    try testing.expectEqual(@as(u32, 0), c.term_offset);

    writePayload(c, big);
    buf.commit(c, 99);

    // Read everything: 15 data frames + padding (skipped) + 1 data frame.
    resetCollected();
    const n = buf.read(&collectHandler, 64);
    try testing.expectEqual(@as(u32, 16), n);
    try testing.expectEqualSlices(u8, big, collected_msgs[15]);
    try testing.expectEqual(@as(i32, 99), collected_types[15]);
}
