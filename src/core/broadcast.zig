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
            new_tail = self.current_tail + @as(i64, padding_length) + @as(i64, record_length); // kcov-skip: runs on every padding transmit (padding tests assert the wrapped record arrives); no own line record

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
                if (!self.resyncToLatest()) return null;
            }

            const record_start = self.next_record;
            const record_offset: u32 = @intCast(@as(u64, @bitCast(record_start)) & self.mask);
            const header: *const RecordHeader = @ptrCast(@alignCast(&self.buffer[record_offset]));

            // The header may belong to a record the transmitter is currently
            // overwriting; sanity-check the length BEFORE using it so a
            // garbage value cannot panic (negative @intCast / OOB slice)
            // ahead of the validate() check.
            const raw_len = header.payload_length;

            if (header.msg_type_id == PADDING_MSG_TYPE_ID) {
                // Padding record — payload_length holds the total padding
                // size, which always fills exactly to the end of the buffer.
                if (raw_len <= 0 or @as(u32, @intCast(raw_len)) > self.capacity - record_offset) {
                    // Garbage header — we must have been lapped mid-read.
                    if (!self.resyncToLatest()) return null;
                    more = true;
                    continue;
                }
                if (!self.validateAt(record_start)) {
                    if (!self.resyncToLatest()) return null;
                    more = true;
                    continue;
                }
                self.next_record += @as(i64, @as(u32, @intCast(raw_len)));
                self.cursor = self.next_record;
                more = true;
                continue;
            }

            // Data record — payload_length is the actual payload size.
            if (raw_len <= 0 or @as(u32, @intCast(raw_len)) > self.calculateMaxMessageLength()) {
                // Garbage header — we must have been lapped mid-read.
                if (!self.resyncToLatest()) return null;
                more = true;
                continue;
            }
            const payload_len: u32 = @intCast(raw_len);
            const aligned_len: u32 = alignedRecordLength(payload_len);
            if (record_offset + aligned_len > self.capacity) {
                // A genuine record never crosses the end of the buffer.
                if (!self.resyncToLatest()) return null;
                more = true;
                continue;
            }

            // Extract payload.
            const payload_offset = record_offset + HEADER_LENGTH;
            const payload = self.buffer[payload_offset..][0..payload_len];
            const msg_type_id = header.msg_type_id;

            // Advance past this record (using aligned length).
            self.next_record += @as(i64, aligned_len);
            self.cursor = self.next_record;

            // Validate against the START of the record we just read — not
            // the advanced cursor, which would tolerate the transmitter
            // overwriting exactly the record being returned.
            if (!self.validateAt(record_start)) {
                // Data was overwritten while we were reading. Skip forward.
                if (!self.resyncToLatest()) return null;
                more = true;
                continue;
            }

            return Message{
                .msg_type_id = msg_type_id,
                .payload = payload,
            };
        }
        return null; // kcov-skip: structurally unreachable: every loop arm returns or sets more=true; required because while(more) needs a fallthrough return
    }

    /// Resync to the start of the most recent record published by the
    /// transmitter (`latest_counter` always holds a real record start —
    /// unlike `tail - capacity`, which can land on an arbitrary mid-record
    /// byte). Increments lapped_count and returns true on success; returns
    /// false when there is no newer record to jump to (prevents spinning).
    fn resyncToLatest(self: *BroadcastReceiver) bool {
        const latest = self.latest_counter.load(.acquire);
        if (self.next_record == latest) return false;
        self.lapped_count += 1;
        self.cursor = latest;
        self.next_record = latest;
        return true;
    }

    /// Validate that the record starting at `record_position` has not been
    /// overwritten by the transmitter.
    fn validateAt(self: *const BroadcastReceiver, record_position: i64) bool {
        const tail_intent = self.tail_intent_counter.load(.acquire);
        // If tail_intent has moved more than capacity past the record start,
        // the transmitter has (or is about to have) overwritten the record.
        return tail_intent <= (record_position + @as(i64, self.capacity)); // kcov-skip: runs on every receive validation (all broadcast tests); no own line record
    }

    /// Validate that the data at the current cursor position is still valid
    /// (not overwritten by the transmitter).
    pub fn validate(self: *const BroadcastReceiver) bool {
        return self.validateAt(self.cursor);
    }

    /// Maximum payload size for a single message (mirrors the transmitter).
    pub fn calculateMaxMessageLength(self: *const BroadcastReceiver) u32 {
        return (self.capacity / 8) - HEADER_LENGTH;
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
    /// Returns null when no message is available, when the message is larger
    /// than the scratch buffer (it is dropped), or when the record was
    /// overwritten by the transmitter while it was being copied (discarded).
    pub fn receiveNext(self: *CopyBroadcastReceiver) ?BroadcastReceiver.Message {
        const msg = self.receiver.receiveNext() orelse return null;

        // Guard the scratch buffer: with capacity > 32 KB the max message
        // length exceeds the scratch size; drop oversized messages instead
        // of overflowing the copy.
        if (msg.payload.len > self.scratch.len) {
            return null;
        }

        @memcpy(self.scratch[0..msg.payload.len], msg.payload);

        // Aeron requirement: re-validate AFTER the copy. The transmitter may
        // have overwritten the record while we were copying from the live
        // buffer; if so the scratch contents are torn and must be discarded.
        const record_start = self.receiver.cursor -
            @as(i64, alignedRecordLength(@intCast(msg.payload.len)));
        if (!self.receiver.validateAt(record_start)) {
            self.receiver.lapped_count += 1; // kcov-skip: requires the transmitter to lap the receiver between receiveNext's validate and the post-copy re-validate — a sub-microsecond window not deterministically stageable in-process
            return null;
        }

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
    // capacity, so the receiver gets lapped and resyncs to the most recent
    // record via latest_counter (Aeron semantics: lossy jump-to-latest).
    // The important thing is that padding is handled transparently and the
    // wrapped record (type 2) is received correctly.
    try testing.expect(count >= 1);
    try testing.expect(saw_type2);
    try testing.expect(rx.lappedCount() >= 1);
}

test "lapped receiver resyncs to a real record via latest_counter" {
    const cap = 256;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf); // joins at position 0

    // Variable-size records so that (tail - capacity) — the old, broken
    // resync point — falls mid-record rather than on a record boundary.
    // Payload bytes are ASCII letters: parsed as a header they would yield
    // a huge bogus payload_length.
    const sizes = [_]usize{ 5, 13, 21 };
    var i: usize = 0;
    while (i < 40) : (i += 1) {
        var payload: [24]u8 = undefined;
        @memset(&payload, 'A' + @as(u8, @intCast(i % 26)));
        tx.transmit(1, payload[0..sizes[i % sizes.len]]);
    }
    tx.transmit(7, "LAST-MSG");

    // The receiver is far behind; it must resync to the latest record start
    // (a real boundary) and deliver intact data, not misframe garbage.
    const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 7), msg.msg_type_id);
    try testing.expectEqualStrings("LAST-MSG", msg.payload);
    try testing.expectEqual(@as(u64, 1), rx.lappedCount());

    // Nothing newer after the latest record.
    try testing.expect(rx.receiveNext() == null);
}

test "CopyBroadcastReceiver rejects messages larger than its scratch buffer" {
    // 64 KB capacity → max message = 8184 bytes > the 4096-byte scratch.
    const cap = 64 * 1024;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var crx = CopyBroadcastReceiver.init(&buf);

    const big: [5000]u8 = @splat(0x55);
    tx.transmit(9, &big);

    // Oversized message is dropped instead of overflowing the scratch copy.
    try testing.expect(crx.receiveNext() == null);

    // Subsequent messages flow normally.
    tx.transmit(10, "fits-fine");
    const msg = crx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 10), msg.msg_type_id);
    try testing.expectEqualStrings("fits-fine", msg.payload);
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
        received += 1; // kcov-skip: drain-loop body; whether messages remain after the main loop is timing/lapping dependent
    }

    const lapped = rx.lappedCount();

    // Some messages may be lost to lapping, but we should have received many.
    try testing.expect(received > 0);
    _ = lapped;
}

test "receiver skips a padding record without being lapped" {
    const cap = 256;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var rx = BroadcastReceiver.init(&buf);

    // 10 records of 24 bytes fill to offset 240; drain them all so the
    // receiver is current (no lapping later).
    const payload = "exactly-16-bytes";
    for (0..10) |_| tx.transmit(1, payload);
    var drained: usize = 0; // kcov-skip: hit record oscillates between builds; the test runs and passes
    while (rx.receiveNext()) |_| drained += 1;
    try testing.expectEqual(@as(usize, 10), drained);

    // The 11th record does not fit in the 16 remaining bytes: the
    // transmitter writes a padding record at 240 and wraps. The receiver
    // must walk THROUGH the padding (validate, advance) — not resync.
    tx.transmit(2, payload);
    const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(i32, 2), msg.msg_type_id);
    try testing.expectEqualStrings(payload, msg.payload);
    try testing.expectEqual(@as(u64, 0), rx.lappedCount());
}

test "receiver resyncs on garbage or invalidated headers" {
    const cap = 256;
    const payload = "exactly-16-bytes";

    // Stage A: padding record whose length was scribbled to garbage.
    {
        var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
        var tx = BroadcastTransmitter.init(&buf);
        var rx = BroadcastReceiver.init(&buf);
        for (0..10) |_| tx.transmit(1, payload);
        while (rx.receiveNext()) |_| {}
        tx.transmit(2, payload); // padding at 240 + record at 0

        const pad_hdr: *RecordHeader = @ptrCast(@alignCast(&buf[240]));
        try testing.expectEqual(@as(i32, PADDING_MSG_TYPE_ID), pad_hdr.msg_type_id);
        pad_hdr.payload_length = 0; // garbage: padding length must be positive

        // The receiver must treat it as a lap and resync to the latest record.
        const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i32, 2), msg.msg_type_id);
        try testing.expectEqual(@as(u64, 1), rx.lappedCount());
    }

    // Stage B: intact padding record invalidated by transmitter intent.
    {
        var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
        var tx = BroadcastTransmitter.init(&buf);
        var rx = BroadcastReceiver.init(&buf);
        for (0..10) |_| tx.transmit(1, payload);
        while (rx.receiveNext()) |_| {}
        tx.transmit(2, payload);

        // Claim the transmitter is far past the padding record: validateAt
        // must fail and the receiver resync.
        tx.tail_intent_counter.store(240 + cap + 8, .release);
        const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i32, 2), msg.msg_type_id);
        try testing.expectEqual(@as(u64, 1), rx.lappedCount());
    }

    // Stage C: data record with garbage length. With nothing newer the
    // receiver cannot resync (latest == current) and reports no message;
    // once a newer record exists it resyncs to it and delivers.
    {
        var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
        var tx = BroadcastTransmitter.init(&buf);
        var rx = BroadcastReceiver.init(&buf);
        tx.transmit(3, payload);
        const hdr: *RecordHeader = @ptrCast(@alignCast(&buf[0]));
        hdr.payload_length = -5;
        try testing.expect(rx.receiveNext() == null);
        try testing.expectEqual(@as(u64, 0), rx.lappedCount());

        // A newer record gives the resync a target: the garbage record is
        // skipped and the new one delivered.
        tx.transmit(4, payload);
        const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i32, 4), msg.msg_type_id);
        try testing.expectEqual(@as(u64, 1), rx.lappedCount());
    }

    // Stage D: data record that would cross the end of the buffer.
    {
        var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
        var tx = BroadcastTransmitter.init(&buf);
        var rx = BroadcastReceiver.init(&buf);
        for (0..10) |_| tx.transmit(1, payload);
        while (rx.receiveNext()) |_| {}

        // Forge a data record at offset 240 claiming 20 payload bytes
        // (24-byte record would end at 268 > 256) and advance the tail.
        const hdr: *RecordHeader = @ptrCast(@alignCast(&buf[240]));
        hdr.msg_type_id = 9;
        hdr.payload_length = 20;
        tx.tail_counter.store(240 + 28, .release);
        _ = rx.receiveNext();
        try testing.expectEqual(@as(u64, 1), rx.lappedCount());
    }

    // Stage E: a valid record invalidated between header read and return —
    // the receiver resyncs to the following record.
    {
        var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
        var tx = BroadcastTransmitter.init(&buf);
        var rx = BroadcastReceiver.init(&buf);
        tx.transmit(1, payload); // record at 0
        tx.transmit(2, payload); // record at 24
        tx.transmit(3, payload); // record at 48 (the resync target)

        // Intent just past record 0 + capacity: record 0 fails the final
        // validate, the later records still pass, so the receiver resyncs
        // to the latest record start.
        tx.tail_intent_counter.store(cap + 1, .release);
        const msg = rx.receiveNext() orelse return error.TestUnexpectedResult;
        try testing.expectEqual(@as(i32, 3), msg.msg_type_id);
        try testing.expectEqual(@as(u64, 1), rx.lappedCount());
    }
}

test "CopyBroadcastReceiver exposes its lapped count" {
    const cap = 256;
    var buf: [totalBufferSize(cap)]u8 align(config.cache_line_size) = [_]u8{0} ** totalBufferSize(cap);
    var tx = BroadcastTransmitter.init(&buf);
    var copy_rx = CopyBroadcastReceiver.init(&buf);

    tx.transmit(1, "copy-me");
    const msg = copy_rx.receiveNext() orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("copy-me", msg.payload);
    try testing.expectEqual(@as(u64, 0), copy_rx.lappedCount());
}
