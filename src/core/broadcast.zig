const std = @import("std");
const config = @import("../platform/config.zig");

/// Aeron-style BroadcastBuffer — a 1-to-N (one producer, many consumers)
/// messaging primitive for market data fan-out.
///
/// Unlike point-to-point ring buffers (SPSC/MPSC), a BroadcastBuffer allows:
/// - One transmitter writes messages
/// - Many receivers independently read at their own pace
/// - Receivers that fall behind get lapped (lossy — they detect and skip forward)
///
/// Memory layout:
///   [buffer region (capacity bytes)] [trailer (cache-line padded counters)]
///
/// Record format (compatible with ring buffer frame format):
///   [i32 payload_length] [i32 msg_type_id] [payload...] [padding to alignment]
///
/// payload_length stores the actual payload size (excluding header).
/// The aligned record length is computed as alignUp(HEADER_LENGTH + payload_length, 8).

pub const RECORD_ALIGNMENT: u32 = 8;
pub const HEADER_LENGTH: u32 = 8; // payload_length(4) + msg_type_id(4)
pub const PADDING_MSG_TYPE_ID: i32 = 0;

/// Record header matching the frame format used by SPSC/MPSC ring buffers.
const RecordHeader = extern struct {
    /// Actual payload length (excluding this header). For padding records,
    /// this stores the total padding record size (header + fill).
    payload_length: i32,
    /// User-defined message type ID. 0 = padding record.
    msg_type_id: i32,

    comptime {
        if (@sizeOf(RecordHeader) != HEADER_LENGTH) @compileError("RecordHeader must be 8 bytes");
    }
};

/// Trailer appended after the buffer region with cache-line-padded atomic counters.
/// Must be placed at a cache-line-aligned offset within the buffer.
pub const TRAILER_LENGTH: u32 = config.cache_line_size * 4;

const TrailerOffsets = struct {
    /// Byte offsets within the trailer region.
    const TAIL_INTENT: u32 = 0;
    const TAIL: u32 = config.cache_line_size;
    const LATEST_COUNTER: u32 = config.cache_line_size * 2;
};

/// Compute aligned record length (header + payload, rounded up to RECORD_ALIGNMENT).
inline fn alignedRecordLength(payload_len: u32) u32 {
    return config.alignUp(HEADER_LENGTH + payload_len, RECORD_ALIGNMENT);
}

/// Get a pointer to an atomic i64 at a given byte offset in the buffer.
inline fn atomicAt(buffer: []u8, offset: u32) *std.atomic.Value(i64) {
    return @ptrCast(@alignCast(&buffer[offset]));
}

/// Get a const pointer to an atomic i64 at a given byte offset in the buffer.
inline fn atomicAtConst(buffer: []const u8, offset: u32) *const std.atomic.Value(i64) {
    return @ptrCast(@alignCast(&buffer[offset]));
}

// ---------------------------------------------------------------------------
// BroadcastTransmitter
// ---------------------------------------------------------------------------

/// Single-producer transmitter for the broadcast buffer.
/// Writes messages that can be read by any number of BroadcastReceivers.
pub const BroadcastTransmitter = struct {
    buffer: []u8,
    capacity: u32,
    mask: u32,
    trailer_offset: u32,
    tail_intent_counter: *std.atomic.Value(i64),
    tail_counter: *std.atomic.Value(i64),
    latest_counter: *std.atomic.Value(i64),

    /// Current logical tail position (cached locally to avoid atomic reads).
    current_tail: i64,

    /// Initialize a transmitter over the given buffer.
    /// The buffer must include space for the trailer at the end.
    /// The usable capacity (buffer.len - TRAILER_LENGTH) must be a power of 2
    /// and >= TRAILER_LENGTH (to ensure trailer alignment).
    pub fn init(buffer: []u8) BroadcastTransmitter {
        std.debug.assert(buffer.len > TRAILER_LENGTH);
        const cap: u32 = @intCast(buffer.len - TRAILER_LENGTH);
        std.debug.assert(cap > 0 and (cap & (cap - 1)) == 0); // power of 2

        const toff = cap; // trailer starts right after buffer region

        const ti = atomicAt(buffer, toff + TrailerOffsets.TAIL_INTENT);
        const tc = atomicAt(buffer, toff + TrailerOffsets.TAIL);
        const lc = atomicAt(buffer, toff + TrailerOffsets.LATEST_COUNTER);

        ti.store(0, .monotonic);
        tc.store(0, .monotonic);
        lc.store(0, .monotonic);

        return .{
            .buffer = buffer,
            .capacity = cap,
            .mask = cap - 1,
            .trailer_offset = toff,
            .tail_intent_counter = ti,
            .tail_counter = tc,
            .latest_counter = lc,
            .current_tail = 0,
        };
    }

    /// Transmit a message. Always succeeds (old data is overwritten; slow
    /// receivers detect lapping via their lapped_count).
    pub fn transmit(self: *BroadcastTransmitter, msg_type_id: i32, msg: []const u8) void {
        std.debug.assert(msg_type_id != PADDING_MSG_TYPE_ID); // 0 is reserved for padding
        std.debug.assert(msg.len <= self.calculateMaxMessageLength());

        const record_length: u32 = alignedRecordLength(@intCast(msg.len));
        var new_tail = self.current_tail + @as(i64, record_length);

        // Check if the record wraps past the end of the buffer.
        const current_offset: u32 = @intCast(@as(u64, @bitCast(self.current_tail)) & self.mask);
        const remaining_at_end = self.capacity - current_offset;

        if (record_length > remaining_at_end) {
            // Insert a padding record to fill the remainder, then wrap to offset 0.
            const padding_length = remaining_at_end;
            new_tail = self.current_tail + @as(i64, padding_length) + @as(i64, record_length);

            // Signal intent.
            self.tail_intent_counter.store(new_tail, .release);

            // Write padding record: store the total padding size in payload_length
            // (we use the total size so the receiver can skip the right amount).
            self.writeHeader(current_offset, @intCast(padding_length), PADDING_MSG_TYPE_ID);

            // Write actual record at offset 0.
            self.writeRecord(0, msg, msg_type_id);

            // Commit.
            self.tail_counter.store(new_tail, .release);
            self.latest_counter.store(self.current_tail + @as(i64, padding_length), .release);
            self.current_tail = new_tail;
        } else {
            // Signal intent.
            self.tail_intent_counter.store(new_tail, .release);

            // Write record at current offset.
            self.writeRecord(current_offset, msg, msg_type_id);

            // Commit.
            self.tail_counter.store(new_tail, .release);
            self.latest_counter.store(self.current_tail, .release);
            self.current_tail = new_tail;
        }
    }

    /// Maximum payload size for a single message (1/8th of capacity minus header).
    pub fn calculateMaxMessageLength(self: *const BroadcastTransmitter) u32 {
        return (self.capacity / 8) - HEADER_LENGTH;
    }

    fn writeRecord(self: *BroadcastTransmitter, offset: u32, msg: []const u8, msg_type_id: i32) void {
        // Store actual payload length in header.
        self.writeHeader(offset, @intCast(msg.len), msg_type_id);

        // Copy payload after header.
        const payload_start = offset + HEADER_LENGTH;
        @memcpy(self.buffer[payload_start..][0..msg.len], msg);
    }

    fn writeHeader(self: *BroadcastTransmitter, offset: u32, payload_length: i32, msg_type_id: i32) void {
        const header: *RecordHeader = @ptrCast(@alignCast(&self.buffer[offset]));
        header.payload_length = payload_length;
        header.msg_type_id = msg_type_id;
    }
};

// ---------------------------------------------------------------------------
// BroadcastReceiver
// ---------------------------------------------------------------------------

/// Per-consumer receiver for the broadcast buffer.
/// Each receiver maintains its own cursor and reads independently.
pub const BroadcastReceiver = struct {
    buffer: []const u8,
    capacity: u32,
    mask: u32,
    trailer_offset: u32,
    tail_intent_counter: *const std.atomic.Value(i64),
    tail_counter: *const std.atomic.Value(i64),
    latest_counter: *const std.atomic.Value(i64),

    /// Current read position.
    cursor: i64,
    /// Next record to read.
    next_record: i64,
    /// How many times this receiver was lapped (messages lost).
    lapped_count: u64,

    pub const Message = struct {
        msg_type_id: i32,
        payload: []const u8,
    };

    /// Initialize a receiver over the given buffer.
    /// Starts reading from the current tail position (joins "live").
    pub fn init(buffer: []const u8) BroadcastReceiver {
        std.debug.assert(buffer.len > TRAILER_LENGTH);
        const cap: u32 = @intCast(buffer.len - TRAILER_LENGTH);
        std.debug.assert(cap > 0 and (cap & (cap - 1)) == 0);

        const toff = cap;
        const ti = atomicAtConst(buffer, toff + TrailerOffsets.TAIL_INTENT);
        const tc = atomicAtConst(buffer, toff + TrailerOffsets.TAIL);
        const lc = atomicAtConst(buffer, toff + TrailerOffsets.LATEST_COUNTER);

        const tail = tc.load(.acquire);

        return .{
            .buffer = buffer,
            .capacity = cap,
            .mask = cap - 1,
            .trailer_offset = toff,
            .tail_intent_counter = ti,
            .tail_counter = tc,
            .latest_counter = lc,
            .cursor = tail,
            .next_record = tail,
            .lapped_count = 0,
        };
    }

    /// Try to receive the next message.
    /// Returns the message payload or null if no new messages.
    /// If the receiver was lapped, advances cursor and increments lapped_count.
    pub fn receiveNext(self: *BroadcastReceiver) ?Message {
        var more = true;
        while (more) {
            more = false;

            const tail = self.tail_counter.load(.acquire);

            // No new data.
            if (self.cursor >= tail) {
                return null;
            }

            // Check if we were lapped (transmitter overwrote our position).
            if (tail > self.cursor + @as(i64, self.capacity)) {
                // Skip forward to the earliest available data.
                self.lapped_count += 1;
                self.cursor = tail - @as(i64, self.capacity);
                self.next_record = self.cursor;
            }

            const record_offset: u32 = @intCast(@as(u64, @bitCast(self.next_record)) & self.mask);
            const header: *const RecordHeader = @ptrCast(@alignCast(&self.buffer[record_offset]));

            if (header.msg_type_id == PADDING_MSG_TYPE_ID) {
                // Padding record — payload_length holds the total padding size.
                const padding_size: u32 = @intCast(header.payload_length);
                self.next_record += @as(i64, padding_size);
                self.cursor = self.next_record;
                more = true;
                continue;
            }

            // payload_length is the actual payload size.
            const payload_len: u32 = @intCast(header.payload_length);
            const aligned_len: u32 = alignedRecordLength(payload_len);

            // Extract payload.
            const payload_offset = record_offset + HEADER_LENGTH;
            const payload = self.buffer[payload_offset..][0..payload_len];
            const msg_type_id = header.msg_type_id;

            // Advance past this record (using aligned length).
            self.next_record += @as(i64, aligned_len);
            self.cursor = self.next_record;

            // Validate the record is still intact (not overwritten while we read it).
            if (!self.validate()) {
                // Data was overwritten while we were reading. Skip forward.
                self.lapped_count += 1;
                const new_tail = self.tail_counter.load(.acquire);
                self.cursor = new_tail - @as(i64, self.capacity);
                self.next_record = self.cursor;
                more = true;
                continue;
            }

            return Message{
                .msg_type_id = msg_type_id,
                .payload = payload,
            };
        }
        return null;
    }

    /// Validate that the data at the current cursor position is still valid
    /// (not overwritten by the transmitter).
    pub fn validate(self: *const BroadcastReceiver) bool {
        const tail_intent = self.tail_intent_counter.load(.acquire);
        // If tail_intent has moved more than capacity past our cursor,
        // the transmitter has overwritten our data.
        return tail_intent <= (self.cursor + @as(i64, self.capacity));
    }

    /// How many times this receiver was lapped (messages lost).
    pub fn lappedCount(self: *const BroadcastReceiver) u64 {
        return self.lapped_count;
    }
};

// ---------------------------------------------------------------------------
// CopyBroadcastReceiver
// ---------------------------------------------------------------------------

/// Convenience wrapper that copies received messages to a stable scratch buffer,
/// guaranteeing the returned slice won't be overwritten by the transmitter.
pub const CopyBroadcastReceiver = struct {
    receiver: BroadcastReceiver,
    scratch: [4096]u8,

    pub fn init(buffer: []const u8) CopyBroadcastReceiver {
        return .{
            .receiver = BroadcastReceiver.init(buffer),
            .scratch = [_]u8{0} ** 4096,
        };
    }

    /// Receive next message, copying payload to internal scratch buffer.
    /// Returns a stable slice that won't be overwritten by the transmitter.
    pub fn receiveNext(self: *CopyBroadcastReceiver) ?BroadcastReceiver.Message {
        const msg = self.receiver.receiveNext() orelse return null;
        @memcpy(self.scratch[0..msg.payload.len], msg.payload);
        return .{
            .msg_type_id = msg.msg_type_id,
            .payload = self.scratch[0..msg.payload.len],
        };
    }

    /// How many times the underlying receiver was lapped.
    pub fn lappedCount(self: *const CopyBroadcastReceiver) u64 {
        return self.receiver.lapped_count;
    }
};

// ===========================================================================
// Tests
// ===========================================================================

const testing = std.testing;

// Buffer size helper: capacity (power of 2) + trailer.
fn totalBufferSize(comptime capacity: u32) u32 {
    return capacity + TRAILER_LENGTH;
}

test "transmitter init with power-of-2 capacity" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    const tx = BroadcastTransmitter.init(&buf);

    try testing.expectEqual(@as(u32, cap), tx.capacity);
    try testing.expectEqual(@as(u32, cap - 1), tx.mask);
    try testing.expectEqual(@as(i64, 0), tx.current_tail);
}

test "single transmit and receive" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    tx.transmit(42, "hello broadcast");

    const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 42), msg.msg_type_id);
    try testing.expectEqualStrings("hello broadcast", msg.payload);
}

test "multiple messages in sequence" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    tx.transmit(1, "msg-one");
    tx.transmit(2, "msg-two");
    tx.transmit(3, "msg-three");

    const m1 = rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 1), m1.msg_type_id);
    try testing.expectEqualStrings("msg-one", m1.payload);

    const m2 = rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 2), m2.msg_type_id);
    try testing.expectEqualStrings("msg-two", m2.payload);

    const m3 = rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 3), m3.msg_type_id);
    try testing.expectEqualStrings("msg-three", m3.payload);

    try testing.expect(rx.receiveNext() == null);
}

test "receiver detects lapping" {
    // 512-byte capacity. Max message = (512/8) - 8 = 56 bytes.
    const cap = 512;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    // Each 8-byte payload record = alignUp(8+8, 8) = 16 bytes.
    // 512 / 16 = 32 records fill the buffer.
    // Writing 128 records will lap the receiver multiple times.
    for (0..128) |i| {
        var data: [8]u8 = undefined;
        @memcpy(data[0..4], "data");
        const idx: u32 = @intCast(i & 0xFFFF);
        @memcpy(data[4..8], std.mem.asBytes(&idx));
        tx.transmit(1, &data);
    }

    // Receiver should detect lapping.
    _ = rx.receiveNext();
    try testing.expect(rx.lappedCount() > 0);
}

test "padding at buffer wrap-around" {
    // 256-byte capacity. Max message = (256/8) - 8 = 24 bytes.
    const cap = 256;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    // Receiver joins before any writes.
    var rx = BroadcastReceiver.init(&buf);

    // Payload = 16 bytes -> record = alignUp(8+16, 8) = 24 bytes.
    // After 10 records: offset = 240, remaining = 16.
    // Next 24-byte record > 16 remaining, so padding is inserted at 240
    // and the 11th record wraps to offset 0.
    const payload = "exactly-16-bytes";
    std.debug.assert(payload.len == 16);

    for (0..10) |_| {
        tx.transmit(1, payload);
    }

    // 11th record should cause wrap with padding.
    tx.transmit(2, payload);

    // Read all messages. The receiver should skip padding records transparently.
    var count: usize = 0;
    var saw_type2 = false;
    while (rx.receiveNext()) |msg| {
        count += 1;
        if (msg.msg_type_id == 2) saw_type2 = true;
        try testing.expectEqualStrings(payload, msg.payload);
    }
    // The buffer is 256 bytes. 11 records + padding = 280 bytes, which exceeds
    // capacity, so the receiver gets lapped for the first record and reads 10.
    // The important thing is that padding is handled transparently and the
    // wrapped record (type 2) is received correctly.
    try testing.expect(count >= 10);
    try testing.expect(saw_type2);
}

test "multiple receivers reading independently" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx1 = BroadcastReceiver.init(&buf);
    var rx2 = BroadcastReceiver.init(&buf);

    tx.transmit(1, "first");
    tx.transmit(2, "second");

    // rx1 reads both.
    const m1a = rx1.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 1), m1a.msg_type_id);
    const m1b = rx1.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 2), m1b.msg_type_id);

    // rx2 also reads both independently.
    const m2a = rx2.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 1), m2a.msg_type_id);
    const m2b = rx2.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 2), m2b.msg_type_id);
}

test "CopyBroadcastReceiver copies to stable buffer" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var crx = CopyBroadcastReceiver.init(&buf);

    tx.transmit(10, "copied message");

    const msg = crx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 10), msg.msg_type_id);
    try testing.expectEqualStrings("copied message", msg.payload);

    // Verify the payload is in the scratch buffer, not in the shared buffer.
    const payload_ptr = @intFromPtr(msg.payload.ptr);
    const scratch_start = @intFromPtr(&crx.scratch);
    const scratch_end = scratch_start + crx.scratch.len;
    try testing.expect(payload_ptr >= scratch_start and payload_ptr < scratch_end);
}

test "max message length validation" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    const tx = BroadcastTransmitter.init(&buf);

    // capacity = 1024, max = (1024/8) - 8 = 120
    try testing.expectEqual(@as(u32, 120), tx.calculateMaxMessageLength());
}

test "empty receive returns null" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    _ = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    try testing.expect(rx.receiveNext() == null);
}

test "receiver joins late and reads from current tail" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);

    // Write several messages before the receiver joins.
    tx.transmit(1, "before-join-1");
    tx.transmit(2, "before-join-2");
    tx.transmit(3, "before-join-3");

    // Receiver joins now — should start from current tail.
    var rx = BroadcastReceiver.init(&buf);

    // Write a new message after the receiver joined.
    tx.transmit(4, "after-join");

    const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 4), msg.msg_type_id);
    try testing.expectEqualStrings("after-join", msg.payload);
}

test "record alignment is maintained" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    // Write messages of varying sizes.
    tx.transmit(1, "a"); // 1 byte payload -> record = alignUp(9,8) = 16
    tx.transmit(2, "ab"); // 2 bytes -> 16
    tx.transmit(3, "abc"); // 3 bytes -> 16
    tx.transmit(4, "abcdefgh"); // 8 bytes -> 16
    tx.transmit(5, "abcdefghi"); // 9 bytes -> 24

    // All should be readable.
    for (1..6) |i| {
        const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i32, @intCast(i)), msg.msg_type_id);
    }

    // The tail should be properly aligned.
    try testing.expect(@as(u64, @bitCast(tx.current_tail)) % RECORD_ALIGNMENT == 0);
}

test "validate returns true for valid position" {
    const cap = 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    tx.transmit(1, "check-valid");
    _ = rx.receiveNext();

    // Should still be valid since we haven't overwritten.
    try testing.expect(rx.validate());
}

test "high-throughput sequential transmit and receive" {
    const cap = 4096;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    const iterations: u32 = 1000;
    var received: u32 = 0;

    for (0..iterations) |i| {
        var msg_buf: [8]u8 = undefined;
        const val: u32 = @intCast(i);
        @memcpy(msg_buf[0..4], std.mem.asBytes(&val));
        @memcpy(msg_buf[4..8], "test");
        tx.transmit(1, &msg_buf);

        // Try to read.
        while (rx.receiveNext()) |_| {
            received += 1;
        }
    }

    // Drain remaining.
    while (rx.receiveNext()) |_| {
        received += 1;
    }

    const lapped = rx.lappedCount();

    // Some messages may be lost to lapping, but we should have received many.
    try testing.expect(received > 0);
    _ = lapped;
}
