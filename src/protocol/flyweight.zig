//! Aeron-compatible wire protocol flyweights for ZigBolt.
//!
//! Each flyweight wraps a raw `[]u8` buffer and provides typed accessor
//! methods that read/write at fixed byte offsets using little-endian
//! encoding — the same on-wire layout as Aeron (Real Logic).
//!
//! All sizes match Aeron's C++ / Java media driver exactly.

const std = @import("std");

// ---------------------------------------------------------------------------
// Frame type enumeration
// ---------------------------------------------------------------------------

pub const FrameType = enum(u16) {
    PAD = 0x00,
    DATA = 0x01,
    NAK = 0x02,
    SM = 0x03,
    ERR = 0x04,
    SETUP = 0x05,
    RTTM = 0x06,
    RES = 0x07,
};

// ---------------------------------------------------------------------------
// Error codes (matches Aeron ErrorCode)
// ---------------------------------------------------------------------------

pub const ErrorCode = enum(i32) {
    GENERIC_ERROR = 0,
    INVALID_CHANNEL = 1,
    UNKNOWN_SUBSCRIPTION = 2,
    UNKNOWN_PUBLICATION = 3,
    CHANNEL_ENDPOINT_ERROR = 4,
    UNKNOWN_COUNTER = 5,
    UNKNOWN_COMMAND_TYPE_ID = 6,
    MALFORMED_COMMAND = 7,
    NOT_SUPPORTED = 8,
    UNKNOWN_HOST = 9,
    RESOURCE_TEMPORARILY_UNAVAILABLE = 10,
};

// ---------------------------------------------------------------------------
// Data frame flags
// ---------------------------------------------------------------------------

pub const DataFlags = struct {
    pub const BEGIN: u8 = 0x80;
    pub const END: u8 = 0x40;
    pub const EOS: u8 = 0x20;
};

// ---------------------------------------------------------------------------
// Status message flags
// ---------------------------------------------------------------------------

pub const SmFlags = struct {
    pub const SEND_SETUP: u8 = 0x80;
    pub const END_OF_STREAM: u8 = 0x40;
};

// ---------------------------------------------------------------------------
// RTT measurement flags
// ---------------------------------------------------------------------------

pub const RttmFlags = struct {
    pub const REPLY: u8 = 0x80;
};

// ---------------------------------------------------------------------------
// Helper: read/write at offset (typed)
// ---------------------------------------------------------------------------

fn readField(comptime T: type, buf: []const u8, offset: usize) T {
    const size = @sizeOf(T);
    return std.mem.readInt(T, @as(*const [size]u8, @ptrCast(buf[offset..][0..size])), .little);
}

fn writeField(comptime T: type, buf: []u8, offset: usize, value: T) void {
    const size = @sizeOf(T);
    std.mem.writeInt(T, @as(*[size]u8, @ptrCast(buf[offset..][0..size])), value, .little);
}

// ---------------------------------------------------------------------------
// 1. HeaderFlyweight — 8 bytes base header
// ---------------------------------------------------------------------------

pub const HeaderFlyweight = struct {
    buf: []u8,

    pub const SIZE: usize = 8;

    pub fn wrap(buf: []u8) HeaderFlyweight {
        std.debug.assert(buf.len >= SIZE);
        return .{ .buf = buf };
    }

    // -- frame_length (i32, offset 0) --
    pub fn frameLength(self: HeaderFlyweight) i32 {
        return readField(i32, self.buf, 0);
    }
    pub fn setFrameLength(self: HeaderFlyweight, v: i32) void {
        writeField(i32, self.buf, 0, v);
    }

    /// A negative frame_length indicates a padding frame.
    pub fn isPaddingFrame(self: HeaderFlyweight) bool {
        return self.frameLength() < 0;
    }

    // -- version (u8, offset 4) --
    pub fn version(self: HeaderFlyweight) u8 {
        return self.buf[4];
    }
    pub fn setVersion(self: HeaderFlyweight, v: u8) void {
        self.buf[4] = v;
    }

    // -- flags (u8, offset 5) --
    pub fn flags(self: HeaderFlyweight) u8 {
        return self.buf[5];
    }
    pub fn setFlags(self: HeaderFlyweight, v: u8) void {
        self.buf[5] = v;
    }

    // -- frame_type (u16, offset 6) --
    pub fn frameType(self: HeaderFlyweight) u16 {
        return readField(u16, self.buf, 6);
    }
    pub fn setFrameType(self: HeaderFlyweight, v: u16) void {
        writeField(u16, self.buf, 6, v);
    }

    pub fn frameTypeEnum(self: HeaderFlyweight) ?FrameType {
        return std.meta.intToEnum(FrameType, self.frameType()) catch null;
    }
};

// ---------------------------------------------------------------------------
// 2. DataHeaderFlyweight — 32 bytes
// ---------------------------------------------------------------------------

pub const DataHeaderFlyweight = struct {
    buf: []u8,

    pub const SIZE: usize = 32;

    pub fn wrap(buf: []u8) DataHeaderFlyweight {
        std.debug.assert(buf.len >= SIZE);
        return .{ .buf = buf };
    }

    /// Wrap buffer and initialise type field to DATA.
    pub fn init(buf: []u8) DataHeaderFlyweight {
        std.debug.assert(buf.len >= SIZE);
        const self = DataHeaderFlyweight{ .buf = buf };
        self.header().setFrameType(@intFromEnum(FrameType.DATA));
        self.header().setVersion(0);
        return self;
    }

    /// Access the base header overlay.
    pub fn header(self: DataHeaderFlyweight) HeaderFlyweight {
        return HeaderFlyweight{ .buf = self.buf };
    }

    // -- frame_length (delegated) --
    pub fn frameLength(self: DataHeaderFlyweight) i32 {
        return self.header().frameLength();
    }
    pub fn setFrameLength(self: DataHeaderFlyweight, v: i32) void {
        self.header().setFrameLength(v);
    }

    // -- flags (delegated) --
    pub fn flags(self: DataHeaderFlyweight) u8 {
        return self.header().flags();
    }
    pub fn setFlags(self: DataHeaderFlyweight, v: u8) void {
        self.header().setFlags(v);
    }

    pub fn isBeginMessage(self: DataHeaderFlyweight) bool {
        return (self.flags() & DataFlags.BEGIN) != 0;
    }
    pub fn isEndMessage(self: DataHeaderFlyweight) bool {
        return (self.flags() & DataFlags.END) != 0;
    }
    pub fn isEndOfStream(self: DataHeaderFlyweight) bool {
        return (self.flags() & DataFlags.EOS) != 0;
    }

    // -- term_offset (u32, offset 8) --
    pub fn termOffset(self: DataHeaderFlyweight) u32 {
        return readField(u32, self.buf, 8);
    }
    pub fn setTermOffset(self: DataHeaderFlyweight, v: u32) void {
        writeField(u32, self.buf, 8, v);
    }

    // -- session_id (i32, offset 12) --
    pub fn sessionId(self: DataHeaderFlyweight) i32 {
        return readField(i32, self.buf, 12);
    }
    pub fn setSessionId(self: DataHeaderFlyweight, v: i32) void {
        writeField(i32, self.buf, 12, v);
    }

    // -- stream_id (i32, offset 16) --
    pub fn streamId(self: DataHeaderFlyweight) i32 {
        return readField(i32, self.buf, 16);
    }
    pub fn setStreamId(self: DataHeaderFlyweight, v: i32) void {
        writeField(i32, self.buf, 16, v);
    }

    // -- term_id (i32, offset 20) --
    pub fn termId(self: DataHeaderFlyweight) i32 {
        return readField(i32, self.buf, 20);
    }
    pub fn setTermId(self: DataHeaderFlyweight, v: i32) void {
        writeField(i32, self.buf, 20, v);
    }

    // -- reserved_value (i64, offset 24) --
    pub fn reservedValue(self: DataHeaderFlyweight) i64 {
        return readField(i64, self.buf, 24);
    }
    pub fn setReservedValue(self: DataHeaderFlyweight, v: i64) void {
        writeField(i64, self.buf, 24, v);
    }

    /// Returns the payload region after the 32-byte header.
    pub fn payload(self: DataHeaderFlyweight) []u8 {
        const fl = self.frameLength();
        if (fl <= @as(i32, SIZE)) return self.buf[SIZE..SIZE];
        const end: usize = @intCast(fl);
        if (end > self.buf.len) return self.buf[SIZE..];
        return self.buf[SIZE..end];
    }
};

// ---------------------------------------------------------------------------
// 3. StatusMessageFlyweight — 36 bytes
// ---------------------------------------------------------------------------

pub const StatusMessageFlyweight = struct {
    buf: []u8,

    pub const SIZE: usize = 36;

    pub fn wrap(buf: []u8) StatusMessageFlyweight {
        std.debug.assert(buf.len >= SIZE);
        return .{ .buf = buf };
    }

    pub fn init(buf: []u8) StatusMessageFlyweight {
        std.debug.assert(buf.len >= SIZE);
        const self = StatusMessageFlyweight{ .buf = buf };
        self.header().setFrameType(@intFromEnum(FrameType.SM));
        self.header().setVersion(0);
        self.header().setFrameLength(@intCast(SIZE));
        return self;
    }

    pub fn header(self: StatusMessageFlyweight) HeaderFlyweight {
        return HeaderFlyweight{ .buf = self.buf };
    }

    // -- session_id (i32, offset 8) --
    pub fn sessionId(self: StatusMessageFlyweight) i32 {
        return readField(i32, self.buf, 8);
    }
    pub fn setSessionId(self: StatusMessageFlyweight, v: i32) void {
        writeField(i32, self.buf, 8, v);
    }

    // -- stream_id (i32, offset 12) --
    pub fn streamId(self: StatusMessageFlyweight) i32 {
        return readField(i32, self.buf, 12);
    }
    pub fn setStreamId(self: StatusMessageFlyweight, v: i32) void {
        writeField(i32, self.buf, 12, v);
    }

    // -- consumption_term_id (i32, offset 16) --
    pub fn consumptionTermId(self: StatusMessageFlyweight) i32 {
        return readField(i32, self.buf, 16);
    }
    pub fn setConsumptionTermId(self: StatusMessageFlyweight, v: i32) void {
        writeField(i32, self.buf, 16, v);
    }

    // -- consumption_term_offset (i32, offset 20) --
    pub fn consumptionTermOffset(self: StatusMessageFlyweight) i32 {
        return readField(i32, self.buf, 20);
    }
    pub fn setConsumptionTermOffset(self: StatusMessageFlyweight, v: i32) void {
        writeField(i32, self.buf, 20, v);
    }

    // -- receiver_window_length (i32, offset 24) --
    pub fn receiverWindowLength(self: StatusMessageFlyweight) i32 {
        return readField(i32, self.buf, 24);
    }
    pub fn setReceiverWindowLength(self: StatusMessageFlyweight, v: i32) void {
        writeField(i32, self.buf, 24, v);
    }

    // -- receiver_id (i64, offset 28) --
    pub fn receiverId(self: StatusMessageFlyweight) i64 {
        return readField(i64, self.buf, 28);
    }
    pub fn setReceiverId(self: StatusMessageFlyweight, v: i64) void {
        writeField(i64, self.buf, 28, v);
    }
};

// ---------------------------------------------------------------------------
// 4. NakFlyweight — 28 bytes
// ---------------------------------------------------------------------------

pub const NakFlyweight = struct {
    buf: []u8,

    pub const SIZE: usize = 28;

    pub fn wrap(buf: []u8) NakFlyweight {
        std.debug.assert(buf.len >= SIZE);
        return .{ .buf = buf };
    }

    pub fn init(buf: []u8) NakFlyweight {
        std.debug.assert(buf.len >= SIZE);
        const self = NakFlyweight{ .buf = buf };
        self.header().setFrameType(@intFromEnum(FrameType.NAK));
        self.header().setVersion(0);
        self.header().setFrameLength(@intCast(SIZE));
        return self;
    }

    pub fn header(self: NakFlyweight) HeaderFlyweight {
        return HeaderFlyweight{ .buf = self.buf };
    }

    // -- session_id (i32, offset 8) --
    pub fn sessionId(self: NakFlyweight) i32 {
        return readField(i32, self.buf, 8);
    }
    pub fn setSessionId(self: NakFlyweight, v: i32) void {
        writeField(i32, self.buf, 8, v);
    }

    // -- stream_id (i32, offset 12) --
    pub fn streamId(self: NakFlyweight) i32 {
        return readField(i32, self.buf, 12);
    }
    pub fn setStreamId(self: NakFlyweight, v: i32) void {
        writeField(i32, self.buf, 12, v);
    }

    // -- term_id (i32, offset 16) --
    pub fn termId(self: NakFlyweight) i32 {
        return readField(i32, self.buf, 16);
    }
    pub fn setTermId(self: NakFlyweight, v: i32) void {
        writeField(i32, self.buf, 16, v);
    }

    // -- term_offset (i32, offset 20) --
    pub fn termOffset(self: NakFlyweight) i32 {
        return readField(i32, self.buf, 20);
    }
    pub fn setTermOffset(self: NakFlyweight, v: i32) void {
        writeField(i32, self.buf, 20, v);
    }

    // -- length (i32, offset 24) — missing data range length --
    pub fn nakLength(self: NakFlyweight) i32 {
        return readField(i32, self.buf, 24);
    }
    pub fn setNakLength(self: NakFlyweight, v: i32) void {
        writeField(i32, self.buf, 24, v);
    }
};

// ---------------------------------------------------------------------------
// 5. SetupFlyweight — 40 bytes
// ---------------------------------------------------------------------------

pub const SetupFlyweight = struct {
    buf: []u8,

    pub const SIZE: usize = 40;

    pub fn wrap(buf: []u8) SetupFlyweight {
        std.debug.assert(buf.len >= SIZE);
        return .{ .buf = buf };
    }

    pub fn init(buf: []u8) SetupFlyweight {
        std.debug.assert(buf.len >= SIZE);
        const self = SetupFlyweight{ .buf = buf };
        self.header().setFrameType(@intFromEnum(FrameType.SETUP));
        self.header().setVersion(0);
        self.header().setFrameLength(@intCast(SIZE));
        return self;
    }

    pub fn header(self: SetupFlyweight) HeaderFlyweight {
        return HeaderFlyweight{ .buf = self.buf };
    }

    // -- term_offset (i32, offset 8) --
    pub fn termOffset(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 8);
    }
    pub fn setTermOffset(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 8, v);
    }

    // -- session_id (i32, offset 12) --
    pub fn sessionId(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 12);
    }
    pub fn setSessionId(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 12, v);
    }

    // -- stream_id (i32, offset 16) --
    pub fn streamId(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 16);
    }
    pub fn setStreamId(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 16, v);
    }

    // -- initial_term_id (i32, offset 20) --
    pub fn initialTermId(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 20);
    }
    pub fn setInitialTermId(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 20, v);
    }

    // -- active_term_id (i32, offset 24) --
    pub fn activeTermId(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 24);
    }
    pub fn setActiveTermId(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 24, v);
    }

    // -- term_length (i32, offset 28) --
    pub fn termLength(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 28);
    }
    pub fn setTermLength(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 28, v);
    }

    // -- mtu_length (i32, offset 32) --
    pub fn mtuLength(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 32);
    }
    pub fn setMtuLength(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 32, v);
    }

    // -- ttl (i32, offset 36) --
    pub fn ttl(self: SetupFlyweight) i32 {
        return readField(i32, self.buf, 36);
    }
    pub fn setTtl(self: SetupFlyweight, v: i32) void {
        writeField(i32, self.buf, 36, v);
    }
};

// ---------------------------------------------------------------------------
// 6. RttMeasurementFlyweight — 40 bytes
// ---------------------------------------------------------------------------

pub const RttMeasurementFlyweight = struct {
    buf: []u8,

    pub const SIZE: usize = 40;

    pub fn wrap(buf: []u8) RttMeasurementFlyweight {
        std.debug.assert(buf.len >= SIZE);
        return .{ .buf = buf };
    }

    pub fn init(buf: []u8) RttMeasurementFlyweight {
        std.debug.assert(buf.len >= SIZE);
        const self = RttMeasurementFlyweight{ .buf = buf };
        self.header().setFrameType(@intFromEnum(FrameType.RTTM));
        self.header().setVersion(0);
        self.header().setFrameLength(@intCast(SIZE));
        return self;
    }

    pub fn header(self: RttMeasurementFlyweight) HeaderFlyweight {
        return HeaderFlyweight{ .buf = self.buf };
    }

    // -- session_id (i32, offset 8) --
    pub fn sessionId(self: RttMeasurementFlyweight) i32 {
        return readField(i32, self.buf, 8);
    }
    pub fn setSessionId(self: RttMeasurementFlyweight, v: i32) void {
        writeField(i32, self.buf, 8, v);
    }

    // -- stream_id (i32, offset 12) --
    pub fn streamId(self: RttMeasurementFlyweight) i32 {
        return readField(i32, self.buf, 12);
    }
    pub fn setStreamId(self: RttMeasurementFlyweight, v: i32) void {
        writeField(i32, self.buf, 12, v);
    }

    // -- receiver_id (i64, offset 16) --
    pub fn receiverId(self: RttMeasurementFlyweight) i64 {
        return readField(i64, self.buf, 16);
    }
    pub fn setReceiverId(self: RttMeasurementFlyweight, v: i64) void {
        writeField(i64, self.buf, 16, v);
    }

    // -- echo_timestamp_ns (i64, offset 24) --
    pub fn echoTimestampNs(self: RttMeasurementFlyweight) i64 {
        return readField(i64, self.buf, 24);
    }
    pub fn setEchoTimestampNs(self: RttMeasurementFlyweight, v: i64) void {
        writeField(i64, self.buf, 24, v);
    }

    // -- reception_delta_ns (i64, offset 32) --
    pub fn receptionDeltaNs(self: RttMeasurementFlyweight) i64 {
        return readField(i64, self.buf, 32);
    }
    pub fn setReceptionDeltaNs(self: RttMeasurementFlyweight, v: i64) void {
        writeField(i64, self.buf, 32, v);
    }

    pub fn isReply(self: RttMeasurementFlyweight) bool {
        return (self.header().flags() & RttmFlags.REPLY) != 0;
    }
};

// ---------------------------------------------------------------------------
// 7. ErrorFlyweight — variable length (min 28 bytes)
// ---------------------------------------------------------------------------

pub const ErrorFlyweight = struct {
    buf: []u8,

    /// Minimum size: header(8) + correlation_id(8) + offending_frame_length(4)
    ///               + error_code(4) + error_string_length(4) = 28
    pub const MIN_SIZE: usize = 28;

    pub fn wrap(buf: []u8) ErrorFlyweight {
        std.debug.assert(buf.len >= MIN_SIZE);
        return .{ .buf = buf };
    }

    pub fn init(buf: []u8) ErrorFlyweight {
        std.debug.assert(buf.len >= MIN_SIZE);
        const self = ErrorFlyweight{ .buf = buf };
        self.header().setFrameType(@intFromEnum(FrameType.ERR));
        self.header().setVersion(0);
        return self;
    }

    pub fn header(self: ErrorFlyweight) HeaderFlyweight {
        return HeaderFlyweight{ .buf = self.buf };
    }

    // -- offending_command_correlation_id (i64, offset 8) --
    pub fn offendingCommandCorrelationId(self: ErrorFlyweight) i64 {
        return readField(i64, self.buf, 8);
    }
    pub fn setOffendingCommandCorrelationId(self: ErrorFlyweight, v: i64) void {
        writeField(i64, self.buf, 8, v);
    }

    // -- offending_frame_length (i32, offset 16) --
    pub fn offendingFrameLength(self: ErrorFlyweight) i32 {
        return readField(i32, self.buf, 16);
    }
    pub fn setOffendingFrameLength(self: ErrorFlyweight, v: i32) void {
        writeField(i32, self.buf, 16, v);
    }

    // -- error_code (i32, offset 20) --
    pub fn errorCode(self: ErrorFlyweight) i32 {
        return readField(i32, self.buf, 20);
    }
    pub fn setErrorCode(self: ErrorFlyweight, v: i32) void {
        writeField(i32, self.buf, 20, v);
    }

    pub fn errorCodeEnum(self: ErrorFlyweight) ?ErrorCode {
        return std.meta.intToEnum(ErrorCode, self.errorCode()) catch null;
    }

    // -- error_string_length (i32, offset 24) --
    pub fn errorStringLength(self: ErrorFlyweight) i32 {
        return readField(i32, self.buf, 24);
    }
    pub fn setErrorStringLength(self: ErrorFlyweight, v: i32) void {
        writeField(i32, self.buf, 24, v);
    }

    /// Returns the UTF-8 error string slice (offset 28+).
    pub fn errorString(self: ErrorFlyweight) []const u8 {
        const len = self.errorStringLength();
        if (len <= 0) return &[_]u8{};
        const str_len: usize = @intCast(len);
        const end = MIN_SIZE + str_len;
        if (end > self.buf.len) return self.buf[MIN_SIZE..];
        return self.buf[MIN_SIZE..end];
    }

    /// Write an error string into the buffer and update length + frame_length.
    pub fn setErrorString(self: ErrorFlyweight, str: []const u8) void {
        const max_len = if (self.buf.len > MIN_SIZE) self.buf.len - MIN_SIZE else 0;
        const copy_len = @min(str.len, max_len);
        @memcpy(self.buf[MIN_SIZE..][0..copy_len], str[0..copy_len]);
        self.setErrorStringLength(@intCast(copy_len));
        self.header().setFrameLength(@intCast(MIN_SIZE + copy_len));
    }
};

// ---------------------------------------------------------------------------
// Position computation helpers (Aeron term-based addressing)
// ---------------------------------------------------------------------------

/// Compute the absolute stream position from term-based addressing.
pub fn computePosition(
    term_offset: i32,
    term_id: i32,
    position_bits_to_shift: u8,
    initial_term_id: i32,
) i64 {
    const term_count: i64 = @as(i64, term_id) - @as(i64, initial_term_id);
    const shift: u6 = @intCast(position_bits_to_shift);
    return (term_count << shift) + @as(i64, term_offset);
}

/// Compute term_id from an absolute position.
pub fn computeTermIdFromPosition(
    position: i64,
    position_bits_to_shift: u8,
    initial_term_id: i32,
) i32 {
    const shift: u6 = @intCast(position_bits_to_shift);
    const term_count = position >> shift;
    return @intCast(term_count + @as(i64, initial_term_id));
}

/// Compute term_offset from an absolute position.
pub fn computeTermOffsetFromPosition(
    position: i64,
    position_bits_to_shift: u8,
) i32 {
    const shift: u6 = @intCast(position_bits_to_shift);
    const mask = (@as(i64, 1) << shift) - 1;
    return @intCast(position & mask);
}

/// Compute term count (number of terms elapsed since initial).
pub fn computeTermCount(term_id: i32, initial_term_id: i32) i32 {
    return term_id -% initial_term_id;
}

// ===========================================================================
// Tests
// ===========================================================================

test "HeaderFlyweight size is 8 bytes" {
    try std.testing.expectEqual(@as(usize, 8), HeaderFlyweight.SIZE);
}

test "DataHeaderFlyweight size is 32 bytes" {
    try std.testing.expectEqual(@as(usize, 32), DataHeaderFlyweight.SIZE);
}

test "StatusMessageFlyweight size is 36 bytes" {
    try std.testing.expectEqual(@as(usize, 36), StatusMessageFlyweight.SIZE);
}

test "NakFlyweight size is 28 bytes" {
    try std.testing.expectEqual(@as(usize, 28), NakFlyweight.SIZE);
}

test "SetupFlyweight size is 40 bytes" {
    try std.testing.expectEqual(@as(usize, 40), SetupFlyweight.SIZE);
}

test "RttMeasurementFlyweight size is 40 bytes" {
    try std.testing.expectEqual(@as(usize, 40), RttMeasurementFlyweight.SIZE);
}

test "ErrorFlyweight min size is 28 bytes" {
    try std.testing.expectEqual(@as(usize, 28), ErrorFlyweight.MIN_SIZE);
}

test "DataHeader encode/decode roundtrip" {
    var buf: [64]u8 = @splat(0);
    const dh = DataHeaderFlyweight.init(&buf);

    dh.setFrameLength(64);
    dh.setSessionId(0x12345678);
    dh.setStreamId(42);
    dh.setTermId(7);
    dh.setTermOffset(1024);
    dh.setReservedValue(-99);

    try std.testing.expectEqual(@as(i32, 64), dh.frameLength());
    try std.testing.expectEqual(@as(i32, 0x12345678), dh.sessionId());
    try std.testing.expectEqual(@as(i32, 42), dh.streamId());
    try std.testing.expectEqual(@as(i32, 7), dh.termId());
    try std.testing.expectEqual(@as(u32, 1024), dh.termOffset());
    try std.testing.expectEqual(@as(i64, -99), dh.reservedValue());

    // Verify frame type was set correctly by init
    try std.testing.expectEqual(@as(u16, @intFromEnum(FrameType.DATA)), dh.header().frameType());
    try std.testing.expectEqual(FrameType.DATA, dh.header().frameTypeEnum().?);
}

test "DataHeader flags BEGIN, END, EOS" {
    var buf: [32]u8 = @splat(0);
    const dh = DataHeaderFlyweight.init(&buf);

    dh.setFlags(DataFlags.BEGIN | DataFlags.END);
    try std.testing.expect(dh.isBeginMessage());
    try std.testing.expect(dh.isEndMessage());
    try std.testing.expect(!dh.isEndOfStream());

    dh.setFlags(DataFlags.EOS);
    try std.testing.expect(!dh.isBeginMessage());
    try std.testing.expect(!dh.isEndMessage());
    try std.testing.expect(dh.isEndOfStream());
}

test "DataHeader payload slice" {
    var buf: [48]u8 = @splat(0);
    const dh = DataHeaderFlyweight.init(&buf);
    dh.setFrameLength(48);

    const pl = dh.payload();
    try std.testing.expectEqual(@as(usize, 16), pl.len);

    // Write into payload and verify
    pl[0] = 0xAB;
    try std.testing.expectEqual(@as(u8, 0xAB), buf[32]);
}

test "StatusMessage encode/decode roundtrip" {
    var buf: [36]u8 = @splat(0);
    const sm = StatusMessageFlyweight.init(&buf);

    sm.setSessionId(100);
    sm.setStreamId(200);
    sm.setConsumptionTermId(5);
    sm.setConsumptionTermOffset(4096);
    sm.setReceiverWindowLength(65536);
    sm.setReceiverId(0x0102030405060708);

    try std.testing.expectEqual(@as(i32, 100), sm.sessionId());
    try std.testing.expectEqual(@as(i32, 200), sm.streamId());
    try std.testing.expectEqual(@as(i32, 5), sm.consumptionTermId());
    try std.testing.expectEqual(@as(i32, 4096), sm.consumptionTermOffset());
    try std.testing.expectEqual(@as(i32, 65536), sm.receiverWindowLength());
    try std.testing.expectEqual(@as(i64, 0x0102030405060708), sm.receiverId());
    try std.testing.expectEqual(FrameType.SM, sm.header().frameTypeEnum().?);
}

test "NakFlyweight encode/decode roundtrip" {
    var buf: [28]u8 = @splat(0);
    const nak = NakFlyweight.init(&buf);

    nak.setSessionId(10);
    nak.setStreamId(20);
    nak.setTermId(3);
    nak.setTermOffset(512);
    nak.setNakLength(256);

    try std.testing.expectEqual(@as(i32, 10), nak.sessionId());
    try std.testing.expectEqual(@as(i32, 20), nak.streamId());
    try std.testing.expectEqual(@as(i32, 3), nak.termId());
    try std.testing.expectEqual(@as(i32, 512), nak.termOffset());
    try std.testing.expectEqual(@as(i32, 256), nak.nakLength());
    try std.testing.expectEqual(FrameType.NAK, nak.header().frameTypeEnum().?);
}

test "SetupFlyweight encode/decode roundtrip" {
    var buf: [40]u8 = @splat(0);
    const setup = SetupFlyweight.init(&buf);

    setup.setTermOffset(0);
    setup.setSessionId(0x7FFFFFFF);
    setup.setStreamId(1);
    setup.setInitialTermId(100);
    setup.setActiveTermId(105);
    setup.setTermLength(1 << 16);
    setup.setMtuLength(1408);
    setup.setTtl(64);

    try std.testing.expectEqual(@as(i32, 0), setup.termOffset());
    try std.testing.expectEqual(@as(i32, 0x7FFFFFFF), setup.sessionId());
    try std.testing.expectEqual(@as(i32, 1), setup.streamId());
    try std.testing.expectEqual(@as(i32, 100), setup.initialTermId());
    try std.testing.expectEqual(@as(i32, 105), setup.activeTermId());
    try std.testing.expectEqual(@as(i32, 1 << 16), setup.termLength());
    try std.testing.expectEqual(@as(i32, 1408), setup.mtuLength());
    try std.testing.expectEqual(@as(i32, 64), setup.ttl());
    try std.testing.expectEqual(FrameType.SETUP, setup.header().frameTypeEnum().?);
}

test "RttMeasurement encode/decode roundtrip" {
    var buf: [40]u8 = @splat(0);
    const rtt = RttMeasurementFlyweight.init(&buf);

    rtt.setSessionId(42);
    rtt.setStreamId(7);
    rtt.setReceiverId(0xDEADBEEF);
    rtt.setEchoTimestampNs(1_000_000_000);
    rtt.setReceptionDeltaNs(500);
    rtt.header().setFlags(RttmFlags.REPLY);

    try std.testing.expectEqual(@as(i32, 42), rtt.sessionId());
    try std.testing.expectEqual(@as(i32, 7), rtt.streamId());
    try std.testing.expectEqual(@as(i64, 0xDEADBEEF), rtt.receiverId());
    try std.testing.expectEqual(@as(i64, 1_000_000_000), rtt.echoTimestampNs());
    try std.testing.expectEqual(@as(i64, 500), rtt.receptionDeltaNs());
    try std.testing.expect(rtt.isReply());
    try std.testing.expectEqual(FrameType.RTTM, rtt.header().frameTypeEnum().?);
}

test "computePosition / computeTermIdFromPosition roundtrip" {
    const initial_term_id: i32 = 100;
    const term_id: i32 = 105;
    const term_offset: i32 = 4096;
    const shift: u8 = 16; // term_length = 64KB

    const pos = computePosition(term_offset, term_id, shift, initial_term_id);

    // position = (105 - 100) << 16 + 4096 = 5 * 65536 + 4096 = 331776
    try std.testing.expectEqual(@as(i64, 5 * 65536 + 4096), pos);

    const recovered_term_id = computeTermIdFromPosition(pos, shift, initial_term_id);
    try std.testing.expectEqual(term_id, recovered_term_id);

    const recovered_offset = computeTermOffsetFromPosition(pos, shift);
    try std.testing.expectEqual(term_offset, recovered_offset);

    const term_count = computeTermCount(term_id, initial_term_id);
    try std.testing.expectEqual(@as(i32, 5), term_count);
}

test "ErrorFlyweight with variable-length string" {
    var buf: [128]u8 = @splat(0);
    const err = ErrorFlyweight.init(&buf);

    err.setOffendingCommandCorrelationId(12345);
    err.setOffendingFrameLength(64);
    err.setErrorCode(@intFromEnum(ErrorCode.INVALID_CHANNEL));

    const msg = "invalid channel URI";
    err.setErrorString(msg);

    try std.testing.expectEqual(@as(i64, 12345), err.offendingCommandCorrelationId());
    try std.testing.expectEqual(@as(i32, 64), err.offendingFrameLength());
    try std.testing.expectEqual(ErrorCode.INVALID_CHANNEL, err.errorCodeEnum().?);
    try std.testing.expectEqual(@as(i32, @intCast(msg.len)), err.errorStringLength());
    try std.testing.expectEqualStrings(msg, err.errorString());

    // frame_length should be MIN_SIZE + string length
    try std.testing.expectEqual(@as(i32, @intCast(ErrorFlyweight.MIN_SIZE + msg.len)), err.header().frameLength());
}

test "frame type identification from header" {
    var buf: [40]u8 = @splat(0);

    // Write a SETUP frame
    const setup = SetupFlyweight.init(&buf);
    _ = setup;

    // Read back using just the header
    const hdr = HeaderFlyweight.wrap(&buf);
    try std.testing.expectEqual(FrameType.SETUP, hdr.frameTypeEnum().?);
    try std.testing.expectEqual(@as(u8, 0), hdr.version());

    // Padding frame detection
    hdr.setFrameLength(-1);
    try std.testing.expect(hdr.isPaddingFrame());

    hdr.setFrameLength(40);
    try std.testing.expect(!hdr.isPaddingFrame());
}
