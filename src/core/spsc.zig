const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("frame.zig");

/// Lock-free Single-Producer Single-Consumer ring buffer.
///
/// Byte-level circular buffer with frame headers for variable-length messages.
/// The writer only modifies `head`, the reader only modifies `tail`, so no CAS
/// is needed — plain acquire/release atomics suffice.
///
/// `capacity` must be a power of 2 so that masking works for wrap-around.
pub fn SpscRingBuffer(comptime capacity: usize) type {
    comptime {
        std.debug.assert(capacity > 0 and (capacity & (capacity - 1)) == 0);
    }

    const mask: usize = capacity - 1;

    return struct {
        const Self = @This();

        /// Result returned by `read`.
        pub const ReadResult = struct {
            data: []const u8,
            msg_type_id: i32,
        };

        // Head (write position) — padded to its own cache line.
        head: std.atomic.Value(usize) align(config.cache_line_size) = std.atomic.Value(usize).init(0),

        // Padding to push tail to the next cache line.
        _pad0: [config.cache_line_size - @sizeOf(std.atomic.Value(usize))]u8 = [_]u8{0} ** (config.cache_line_size - @sizeOf(std.atomic.Value(usize))),

        // Tail (read position) — on its own cache line.
        tail: std.atomic.Value(usize) align(config.cache_line_size) = std.atomic.Value(usize).init(0),

        _pad1: [config.cache_line_size - @sizeOf(std.atomic.Value(usize))]u8 = [_]u8{0} ** (config.cache_line_size - @sizeOf(std.atomic.Value(usize))),

        buffer: [capacity]u8 align(config.cache_line_size) = [_]u8{0} ** capacity,

        // Scratch buffer used when a read payload wraps around the ring boundary.
        _scratch: [capacity]u8 = undefined,

        /// Create an initialized ring buffer with the buffer zeroed.
        pub fn init() Self {
            return .{
                .buffer = [_]u8{0} ** capacity,
            };
        }

        /// Write a framed message into the ring buffer.
        /// Returns `false` if there is not enough space (buffer full).
        pub fn write(self: *Self, data: []const u8, msg_type_id: i32) bool {
            const payload_len: u32 = @intCast(data.len);
            const total_len: usize = frame.alignedFrameLength(payload_len);

            const head_pos = self.head.load(.monotonic);
            const tail_pos = self.tail.load(.acquire);

            // Available space: capacity minus currently used bytes.
            const used = head_pos -% tail_pos;
            const available = capacity - used;

            if (total_len > available) {
                return false;
            }

            // Write the frame header.
            const header_size = frame.FrameHeader.SIZE;
            const header = frame.FrameHeader{
                .frame_length = @intCast(data.len),
                .msg_type_id = msg_type_id,
            };
            const header_bytes = std.mem.asBytes(&header);

            var pos = head_pos;
            for (header_bytes) |b| {
                self.buffer[pos & mask] = b;
                pos += 1;
            }

            // Write payload.
            for (data) |b| {
                self.buffer[pos & mask] = b;
                pos += 1;
            }

            // Zero-fill padding bytes to reach aligned length.
            const padding = total_len - header_size - data.len;
            for (0..padding) |_| {
                self.buffer[pos & mask] = 0;
                pos += 1;
            }

            // Publish the new head so the reader can see the frame.
            self.head.store(head_pos + total_len, .release);
            return true;
        }

        /// Read the next framed message from the ring buffer.
        /// Returns `null` if the buffer is empty.
        pub fn read(self: *Self) ?ReadResult {
            const tail_pos = self.tail.load(.acquire);
            const head_pos = self.head.load(.acquire);

            if (tail_pos == head_pos) {
                return null;
            }

            // Read frame header.
            var header_bytes: [@sizeOf(frame.FrameHeader)]u8 = undefined;
            var pos = tail_pos;
            for (&header_bytes) |*b| {
                b.* = self.buffer[pos & mask];
                pos += 1;
            }
            const header: frame.FrameHeader = @bitCast(header_bytes);

            const payload_len: u32 = @intCast(header.frame_length);
            const total_len: usize = frame.alignedFrameLength(payload_len);

            // Return a slice view into the ring buffer for the payload.
            // Because the buffer wraps, we need to check whether the payload
            // is contiguous. For the common case we return a direct slice;
            // when wrapping occurs we copy into _scratch byte-by-byte.
            const payload_start = (tail_pos + @sizeOf(frame.FrameHeader)) & mask;
            const payload_end = payload_start + payload_len;

            if (payload_end <= capacity) {
                // Contiguous — return direct slice.
                const result = ReadResult{
                    .data = self.buffer[payload_start .. payload_start + payload_len],
                    .msg_type_id = header.msg_type_id,
                };
                // Advance the tail past the entire aligned frame.
                self.tail.store(tail_pos + total_len, .release);
                return result;
            } else {
                // Wrapped — copy the full payload through the mask into _scratch.
                var read_pos = tail_pos + @sizeOf(frame.FrameHeader);
                for (0..payload_len) |i| {
                    self._scratch[i] = self.buffer[read_pos & mask];
                    read_pos += 1;
                }
                const result = ReadResult{
                    .data = self._scratch[0..payload_len],
                    .msg_type_id = header.msg_type_id,
                };
                // Advance the tail past the entire aligned frame.
                self.tail.store(tail_pos + total_len, .release);
                return result;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "basic write and read" {
    var rb = SpscRingBuffer(1024).init();

    const payload = "hello zigbolt";
    const ok = rb.write(payload, 42);
    try std.testing.expect(ok);

    const result = rb.read() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(payload, result.data);
    try std.testing.expectEqual(@as(i32, 42), result.msg_type_id);
}

test "empty read returns null" {
    var rb = SpscRingBuffer(256).init();
    try std.testing.expect(rb.read() == null);
}

test "full write returns false" {
    // Capacity 64 bytes. Each frame is header(8) + payload + padding.
    var rb = SpscRingBuffer(64).init();

    // A 24-byte payload → aligned frame = alignUp(8+24, 8) = 32 bytes.
    const big = "aaaaaaaaaaaaaaaaaaaaaaaa"; // 24 bytes
    try std.testing.expect(rb.write(big, 1));

    // 32 bytes used, 32 remaining. Another 32-byte frame should fit.
    try std.testing.expect(rb.write(big, 2));

    // Now full — 64/64 used.
    try std.testing.expect(!rb.write("x", 3));
}

test "fill and drain" {
    const Cap = 256;
    var rb = SpscRingBuffer(Cap).init();

    // Write multiple small messages.
    var count: usize = 0;
    while (rb.write("msg", @intCast(@as(i32, @truncate(@as(isize, @intCast(count))))))) {
        count += 1;
    }
    try std.testing.expect(count > 0);

    // Drain them all.
    var drained: usize = 0;
    while (rb.read()) |_| {
        drained += 1;
    }
    try std.testing.expectEqual(count, drained);

    // Buffer should be empty now.
    try std.testing.expect(rb.read() == null);
}

test "wrap-around" {
    // Small buffer to force wrap-around quickly.
    const Cap = 128;
    var rb = SpscRingBuffer(Cap).init();

    // Fill then drain several times to force head/tail past capacity.
    for (0..10) |_| {
        // Each "test" payload is 4 bytes → frame = alignUp(8+4,8) = 16 bytes.
        // 128 / 16 = 8 frames fit.
        for (0..8) |_| {
            try std.testing.expect(rb.write("test", 7));
        }
        // Should be full now.
        try std.testing.expect(!rb.write("x", 0));

        // Drain all.
        var n: usize = 0;
        while (rb.read()) |res| {
            try std.testing.expectEqualStrings("test", res.data);
            try std.testing.expectEqual(@as(i32, 7), res.msg_type_id);
            n += 1;
        }
        try std.testing.expectEqual(@as(usize, 8), n);
    }
}
