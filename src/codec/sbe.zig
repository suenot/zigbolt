//! SBE (Simple Binary Encoding) Wire Format Engine for ZigBolt
//!
//! SBE is a binary encoding standard defined by the FIX Trading Community,
//! designed for ultra-low latency financial messaging. It provides:
//!
//! - **Fixed-size fields** encoded directly in a root block (zero-copy access)
//! - **Repeating groups** with nested sub-groups for multi-leg orders, etc.
//! - **Variable-length data** (strings, raw bytes) appended after groups
//! - **Schema versioning** via the message header
//! - **Null sentinels** for optional fields (no tag-length-value overhead)
//!
//! Wire layout:
//! ```
//! [MessageHeader: 8 bytes]
//!   blockLength: u16   — root block size in bytes
//!   templateId:  u16   — message type ID
//!   schemaId:    u16   — schema identifier
//!   version:     u16   — schema version
//! [Root block: blockLength bytes]
//!   Fixed fields in schema order
//! [Groups: variable]
//!   [GroupHeader: 4 bytes] (blockLength: u16, numInGroup: u16)
//!   [Entry x numInGroup: blockLength bytes each]
//!   (groups may nest — each entry can contain sub-groups)
//! [VarData: variable]
//!   [length: u32][data: length bytes]
//! ```
//!
//! This implementation operates on caller-provided `[]u8` buffers with zero
//! heap allocations, making it suitable for the hot path.

const std = @import("std");

// ============================================================================
// Wire Format Types
// ============================================================================

/// SBE message header — always the first 8 bytes on the wire.
pub const MessageHeader = extern struct {
    block_length: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,

    pub const SIZE: usize = 8;

    comptime {
        std.debug.assert(@sizeOf(MessageHeader) == SIZE);
    }
};

/// SBE repeating group header — precedes each group's entries.
pub const GroupHeader = extern struct {
    block_length: u16,
    num_in_group: u16,

    pub const SIZE: usize = 4;

    comptime {
        std.debug.assert(@sizeOf(GroupHeader) == SIZE);
    }
};

// ============================================================================
// Decimal Type for Prices
// ============================================================================

/// Fixed-point decimal for financial prices.
///
/// In SBE, the exponent is defined in the schema and is NOT transmitted on
/// the wire — only the i64 mantissa is encoded. The exponent is provided at
/// construction time for local arithmetic and display.
///
/// Example: price 123.45 with exponent -2 → mantissa = 12345
pub const Decimal64 = struct {
    mantissa: i64,
    exponent: i8,

    /// Null sentinel for optional Decimal64 fields.
    pub const NULL_MANTISSA: i64 = std.math.minInt(i64);

    /// Construct from a floating-point value and a fixed exponent.
    pub fn fromFloat(val: f64, exp: i8) Decimal64 {
        const scale = std.math.pow(f64, 10.0, @as(f64, @floatFromInt(-exp)));
        return .{
            .mantissa = @intFromFloat(@round(val * scale)),
            .exponent = exp,
        };
    }

    /// Convert back to f64.
    pub fn toFloat(self: Decimal64) f64 {
        const scale = std.math.pow(f64, 10.0, @as(f64, @floatFromInt(self.exponent)));
        return @as(f64, @floatFromInt(self.mantissa)) * scale;
    }

    /// Check if this value represents null (optional field not set).
    pub fn isNull(self: Decimal64) bool {
        return self.mantissa == NULL_MANTISSA;
    }

    /// Create a null sentinel value.
    pub fn nullValue() Decimal64 {
        return .{ .mantissa = NULL_MANTISSA, .exponent = 0 };
    }
};

// ============================================================================
// Comptime Schema Definitions
// ============================================================================

/// Primitive field types supported by SBE.
pub const FieldType = enum {
    u8_type,
    u16_type,
    u32_type,
    u64_type,
    i8_type,
    i16_type,
    i32_type,
    i64_type,
    f32_type,
    f64_type,
    char_type,
    fixed_str, // fixed-length string; uses `length` field
};

/// Definition of a single fixed field within a message or group block.
pub const FieldDef = struct {
    name: []const u8,
    id: u16, // FIX tag number
    field_type: FieldType,
    length: u16 = 1, // for fixed_str: number of bytes
    optional: bool = false,
    null_value: ?u64 = null, // sentinel value for optional fields
};

/// Definition of a repeating group (may contain nested groups).
pub const GroupDef = struct {
    name: []const u8,
    id: u16,
    fields: []const FieldDef,
    groups: []const GroupDef = &.{}, // nested groups
};

/// Definition of a variable-length data field.
pub const VarDataDef = struct {
    name: []const u8,
    id: u16,
};

/// Top-level message schema definition.
pub const MessageDef = struct {
    name: []const u8,
    id: u16, // templateId
    fields: []const FieldDef,
    groups: []const GroupDef = &.{},
    var_data: []const VarDataDef = &.{},
};

// ============================================================================
// Encoder
// ============================================================================

/// Encodes SBE messages into a caller-provided byte buffer.
///
/// Usage:
/// ```
/// var buf: [4096]u8 = undefined;
/// var enc = SbeEncoder.init(&buf);
/// const hdr_pos = enc.putMessageHeader(1, 1, 1);
/// enc.putU64(timestamp);
/// enc.putI64(price);
/// enc.finishHeader(hdr_pos);
/// const wire_bytes = buf[0..enc.encodedLength()];
/// ```
pub const SbeEncoder = struct {
    buf: []u8,
    pos: usize,

    /// Initialize an encoder over the given buffer.
    pub fn init(buf: []u8) SbeEncoder {
        return .{ .buf = buf, .pos = 0 };
    }

    /// Returns the number of bytes written so far.
    pub fn encodedLength(self: *const SbeEncoder) usize {
        return self.pos;
    }

    /// Write the SBE message header. Returns the position of the header so
    /// that `finishHeader` can patch the block_length after the root block
    /// has been written.
    pub fn putMessageHeader(self: *SbeEncoder, template_id: u16, schema_id: u16, version: u16) !usize {
        if (self.pos + MessageHeader.SIZE > self.buf.len) return error.BufferOverflow;
        const hdr_pos = self.pos;
        // Write placeholder block_length = 0; patched by finishHeader.
        self.writeU16(0);
        self.writeU16(template_id);
        self.writeU16(schema_id);
        self.writeU16(version);
        return hdr_pos;
    }

    /// Patch the block_length in a previously-written message header.
    /// Call this after writing all root-block fields.
    pub fn finishHeader(self: *SbeEncoder, header_pos: usize) void {
        const block_length: u16 = @intCast(self.pos - header_pos - MessageHeader.SIZE);
        std.mem.writeInt(u16, self.buf[header_pos..][0..2], block_length, .little);
    }

    // ── Primitive writers ────────────────────────────────────────

    pub fn putU8(self: *SbeEncoder, val: u8) !void {
        if (self.pos + 1 > self.buf.len) return error.BufferOverflow;
        self.buf[self.pos] = val;
        self.pos += 1; // kcov-skip: putU8 body; called+asserted in primitive roundtrip tests, no own line record
    }

    pub fn putU16(self: *SbeEncoder, val: u16) !void {
        if (self.pos + 2 > self.buf.len) return error.BufferOverflow;
        self.writeU16(val);
    }

    pub fn putU32(self: *SbeEncoder, val: u32) !void {
        if (self.pos + 4 > self.buf.len) return error.BufferOverflow;
        self.writeU32(val);
    }

    pub fn putU64(self: *SbeEncoder, val: u64) !void {
        if (self.pos + 8 > self.buf.len) return error.BufferOverflow;
        self.writeU64(val);
    }

    pub fn putI8(self: *SbeEncoder, val: i8) !void {
        if (self.pos + 1 > self.buf.len) return error.BufferOverflow;
        self.buf[self.pos] = @bitCast(val);
        self.pos += 1;
    }

    pub fn putI16(self: *SbeEncoder, val: i16) !void {
        if (self.pos + 2 > self.buf.len) return error.BufferOverflow;
        std.mem.writeInt(i16, self.buf[self.pos..][0..2], val, .little);
        self.pos += 2;
    }

    pub fn putI32(self: *SbeEncoder, val: i32) !void {
        if (self.pos + 4 > self.buf.len) return error.BufferOverflow;
        std.mem.writeInt(i32, self.buf[self.pos..][0..4], val, .little);
        self.pos += 4;
    }

    pub fn putI64(self: *SbeEncoder, val: i64) !void {
        if (self.pos + 8 > self.buf.len) return error.BufferOverflow;
        std.mem.writeInt(i64, self.buf[self.pos..][0..8], val, .little);
        self.pos += 8;
    }

    pub fn putF64(self: *SbeEncoder, val: f64) !void {
        if (self.pos + 8 > self.buf.len) return error.BufferOverflow;
        const bits: u64 = @bitCast(val);
        self.writeU64(bits); // kcov-skip: putF64 body; called+asserted in primitive roundtrip tests, no own line record
    }

    pub fn putF32(self: *SbeEncoder, val: f32) !void {
        if (self.pos + 4 > self.buf.len) return error.BufferOverflow;
        const bits: u32 = @bitCast(val);
        self.writeU32(bits); // kcov-skip: putF32 body; called+asserted in primitive roundtrip tests, no own line record
    }

    pub fn putChar(self: *SbeEncoder, val: u8) !void {
        return self.putU8(val);
    }

    // ── Composite writers ────────────────────────────────────────

    /// Write a fixed-length byte array (e.g. a fixed_str field).
    pub fn putBytes(self: *SbeEncoder, data: []const u8) !void {
        if (self.pos + data.len > self.buf.len) return error.BufferOverflow;
        @memcpy(self.buf[self.pos..][0..data.len], data);
        self.pos += data.len;
    }

    /// Write an enum value as its underlying integer representation.
    pub fn putEnum(self: *SbeEncoder, comptime E: type, val: E) !void {
        const Tag = @typeInfo(E).@"enum".tag_type;
        const int_val: Tag = @intFromEnum(val);
        const size = @sizeOf(Tag);
        if (self.pos + size > self.buf.len) return error.BufferOverflow;
        switch (size) {
            1 => {
                self.buf[self.pos] = @bitCast(@as(u8, @bitCast(int_val)));
                self.pos += 1;
            },
            2 => {
                std.mem.writeInt(Tag, self.buf[self.pos..][0..2], int_val, .little);
                self.pos += 2;
            },
            4 => {
                std.mem.writeInt(Tag, self.buf[self.pos..][0..4], int_val, .little);
                self.pos += 4;
            },
            else => @compileError("unsupported enum tag size"),
        }
    }

    /// Write a repeating group header. Returns nothing — the caller is
    /// responsible for writing `count` entries of `block_length` bytes each.
    pub fn beginGroup(self: *SbeEncoder, block_length: u16, count: u16) !void {
        if (self.pos + GroupHeader.SIZE > self.buf.len) return error.BufferOverflow;
        self.writeU16(block_length);
        self.writeU16(count);
    }

    /// Write variable-length data: [u32 length][data bytes].
    pub fn putVarData(self: *SbeEncoder, data: []const u8) !void {
        const total = 4 + data.len;
        if (self.pos + total > self.buf.len) return error.BufferOverflow;
        self.writeU32(@intCast(data.len));
        @memcpy(self.buf[self.pos..][0..data.len], data);
        self.pos += data.len;
    }

    // ── Internal helpers (no bounds check — callers must check) ──

    fn writeU16(self: *SbeEncoder, val: u16) void {
        std.mem.writeInt(u16, self.buf[self.pos..][0..2], val, .little);
        self.pos += 2;
    }

    fn writeU32(self: *SbeEncoder, val: u32) void {
        std.mem.writeInt(u32, self.buf[self.pos..][0..4], val, .little);
        self.pos += 4;
    }

    fn writeU64(self: *SbeEncoder, val: u64) void {
        std.mem.writeInt(u64, self.buf[self.pos..][0..8], val, .little);
        self.pos += 8;
    }
};

// ============================================================================
// Decoder
// ============================================================================

/// Zero-copy SBE decoder. Reads from a `[]const u8` buffer, returning
/// pointers directly into it for byte slices and fixed strings.
pub const SbeDecoder = struct {
    buf: []const u8,
    pos: usize,

    /// Initialize a decoder over the given buffer.
    pub fn init(buf: []const u8) SbeDecoder {
        return .{ .buf = buf, .pos = 0 };
    }

    /// Current read position in the buffer.
    pub fn position(self: *const SbeDecoder) usize {
        return self.pos;
    }

    /// Advance the read position by `n` bytes.
    pub fn skip(self: *SbeDecoder, n: usize) !void {
        if (self.pos + n > self.buf.len) return error.BufferOverflow;
        self.pos += n;
    }

    /// Remaining bytes available for reading.
    pub fn remaining(self: *const SbeDecoder) usize {
        return self.buf.len - self.pos;
    }

    // ── Header readers ───────────────────────────────────────────

    /// Read the 8-byte SBE message header.
    pub fn getMessageHeader(self: *SbeDecoder) !MessageHeader {
        if (self.pos + MessageHeader.SIZE > self.buf.len) return error.BufferOverflow;
        const hdr = MessageHeader{
            .block_length = self.readU16(),
            .template_id = self.readU16(),
            .schema_id = self.readU16(),
            .version = self.readU16(),
        };
        return hdr;
    }

    /// Read a 4-byte repeating group header.
    pub fn getGroupHeader(self: *SbeDecoder) !GroupHeader {
        if (self.pos + GroupHeader.SIZE > self.buf.len) return error.BufferOverflow;
        const ghdr = GroupHeader{
            .block_length = self.readU16(),
            .num_in_group = self.readU16(),
        };
        return ghdr;
    }

    // ── Primitive readers ────────────────────────────────────────

    pub fn getU8(self: *SbeDecoder) !u8 {
        if (self.pos + 1 > self.buf.len) return error.BufferOverflow;
        const val = self.buf[self.pos];
        self.pos += 1;
        return val;
    }

    pub fn getU16(self: *SbeDecoder) !u16 {
        if (self.pos + 2 > self.buf.len) return error.BufferOverflow;
        return self.readU16();
    }

    pub fn getU32(self: *SbeDecoder) !u32 {
        if (self.pos + 4 > self.buf.len) return error.BufferOverflow;
        return self.readU32();
    }

    pub fn getU64(self: *SbeDecoder) !u64 {
        if (self.pos + 8 > self.buf.len) return error.BufferOverflow;
        return self.readU64();
    }

    pub fn getI8(self: *SbeDecoder) !i8 {
        if (self.pos + 1 > self.buf.len) return error.BufferOverflow;
        const val: i8 = @bitCast(self.buf[self.pos]);
        self.pos += 1;
        return val;
    }

    pub fn getI16(self: *SbeDecoder) !i16 {
        if (self.pos + 2 > self.buf.len) return error.BufferOverflow;
        const val = std.mem.readInt(i16, self.buf[self.pos..][0..2], .little);
        self.pos += 2;
        return val;
    }

    pub fn getI32(self: *SbeDecoder) !i32 {
        if (self.pos + 4 > self.buf.len) return error.BufferOverflow;
        const val = std.mem.readInt(i32, self.buf[self.pos..][0..4], .little);
        self.pos += 4;
        return val;
    }

    pub fn getI64(self: *SbeDecoder) !i64 {
        if (self.pos + 8 > self.buf.len) return error.BufferOverflow;
        const val = std.mem.readInt(i64, self.buf[self.pos..][0..8], .little);
        self.pos += 8;
        return val;
    }

    pub fn getF64(self: *SbeDecoder) !f64 {
        if (self.pos + 8 > self.buf.len) return error.BufferOverflow;
        const bits = self.readU64();
        return @bitCast(bits);
    }

    pub fn getF32(self: *SbeDecoder) !f32 {
        if (self.pos + 4 > self.buf.len) return error.BufferOverflow;
        const bits = self.readU32();
        return @bitCast(bits);
    }

    pub fn getChar(self: *SbeDecoder) !u8 {
        return self.getU8();
    }

    // ── Composite readers ────────────────────────────────────────

    /// Zero-copy read of a fixed-length byte array. Returns a pointer
    /// directly into the underlying buffer.
    pub fn getBytes(self: *SbeDecoder, comptime N: usize) !*const [N]u8 {
        if (self.pos + N > self.buf.len) return error.BufferOverflow;
        const ptr: *const [N]u8 = @ptrCast(self.buf[self.pos..][0..N]);
        self.pos += N; // kcov-skip: getBytes body; called+asserted in fixed-bytes test, no own line record
        return ptr;
    }

    /// Read a byte slice of runtime-known length. Zero-copy — returns a
    /// slice into the underlying buffer.
    pub fn getBytesSlice(self: *SbeDecoder, n: usize) ![]const u8 {
        if (self.pos + n > self.buf.len) return error.BufferOverflow;
        const slice = self.buf[self.pos..][0..n];
        self.pos += n;
        return slice;
    }

    /// Read an enum value by reading its underlying integer and validating
    /// it against the enum's named tags. The wire byte is untrusted: an
    /// out-of-range value on an exhaustive enum returns
    /// error.InvalidEnumValue instead of invoking illegal behavior.
    pub fn getEnum(self: *SbeDecoder, comptime E: type) !E {
        const Tag = @typeInfo(E).@"enum".tag_type;
        const size = @sizeOf(Tag);
        if (self.pos + size > self.buf.len) return error.BufferOverflow;
        const int_val: Tag = switch (size) {
            1 => @bitCast(self.buf[blk: {
                const p = self.pos;
                self.pos += 1;
                break :blk p;
            }]),
            2 => blk: {
                const v = std.mem.readInt(Tag, self.buf[self.pos..][0..2], .little);
                self.pos += 2; // kcov-skip: 2-byte getEnum branch; exercised by TestOrdType decode assertions, no own line record
                break :blk v;
            },
            4 => blk: {
                const v = std.mem.readInt(Tag, self.buf[self.pos..][0..4], .little);
                self.pos += 4;
                break :blk v;
            },
            else => @compileError("unsupported enum tag size"),
        };
        return std.meta.intToEnum(E, int_val) catch error.InvalidEnumValue;
    }

    /// Read variable-length data: [u32 length][data bytes].
    /// Zero-copy — returns a slice into the underlying buffer.
    pub fn getVarData(self: *SbeDecoder) ![]const u8 {
        if (self.pos + 4 > self.buf.len) return error.BufferOverflow;
        const len: usize = @intCast(self.readU32());
        if (self.pos + len > self.buf.len) return error.BufferOverflow;
        const data = self.buf[self.pos..][0..len];
        self.pos += len;
        return data;
    }

    // ── Internal helpers ─────────────────────────────────────────

    fn readU16(self: *SbeDecoder) u16 {
        const val = std.mem.readInt(u16, self.buf[self.pos..][0..2], .little);
        self.pos += 2;
        return val;
    }

    fn readU32(self: *SbeDecoder) u32 {
        const val = std.mem.readInt(u32, self.buf[self.pos..][0..4], .little);
        self.pos += 4; // kcov-skip: readU32 helper; exercised via getU32/getF32/getVarData assertions, no own line record
        return val;
    }

    fn readU64(self: *SbeDecoder) u64 {
        const val = std.mem.readInt(u64, self.buf[self.pos..][0..8], .little);
        self.pos += 8;
        return val;
    }
};

// ============================================================================
// Null Sentinels for Optional Fields
// ============================================================================

/// Standard SBE null sentinel values.
pub const null_values = struct {
    pub const u8_null: u8 = std.math.maxInt(u8); // 0xFF
    pub const u16_null: u16 = std.math.maxInt(u16); // 0xFFFF
    pub const u32_null: u32 = std.math.maxInt(u32);
    pub const u64_null: u64 = std.math.maxInt(u64);
    pub const i8_null: i8 = std.math.minInt(i8); // -128
    pub const i16_null: i16 = std.math.minInt(i16);
    pub const i32_null: i32 = std.math.minInt(i32);
    pub const i64_null: i64 = std.math.minInt(i64);
};

// ============================================================================
// Comptime Schema Helpers
// ============================================================================

/// Compute the wire size of a single field.
pub fn fieldWireSize(field: FieldDef) usize {
    return switch (field.field_type) {
        .u8_type, .i8_type, .char_type => 1,
        .u16_type, .i16_type => 2,
        .u32_type, .i32_type, .f32_type => 4,
        .u64_type, .i64_type, .f64_type => 8,
        .fixed_str => @as(usize, field.length),
    };
}

/// Compute the root block size of a message (sum of all fixed field sizes).
pub fn blockSize(fields: []const FieldDef) usize {
    var size: usize = 0;
    for (fields) |f| {
        size += fieldWireSize(f);
    }
    return size; // kcov-skip: blockSize return; result asserted in blockSize tests, no own line record
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

// --- Test enum for encode/decode tests ---
const TestSide = enum(u8) { buy = 0, sell = 1 };
const TestOrdType = enum(u16) { market = 0, limit = 1, stop = 2 };

// --------------------------------------------------------------------------
// 1. MessageHeader encode/decode roundtrip
// --------------------------------------------------------------------------
test "SBE — MessageHeader encode/decode roundtrip" {
    var buf: [256]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    const hdr_pos = try enc.putMessageHeader(42, 1, 3);

    // Write some root block bytes to get a non-zero block_length.
    try enc.putU64(0xDEAD_BEEF);
    enc.finishHeader(hdr_pos);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    const hdr = try dec.getMessageHeader();

    try testing.expectEqual(@as(u16, 8), hdr.block_length); // 8 bytes of root
    try testing.expectEqual(@as(u16, 42), hdr.template_id);
    try testing.expectEqual(@as(u16, 1), hdr.schema_id);
    try testing.expectEqual(@as(u16, 3), hdr.version);
}

// --------------------------------------------------------------------------
// 2. All primitive type encode/decode
// --------------------------------------------------------------------------
test "SBE — primitive types roundtrip" {
    var buf: [256]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    try enc.putU8(0xAB);
    try enc.putU16(0x1234);
    try enc.putU32(0xDEADBEEF);
    try enc.putU64(0x0102030405060708);
    try enc.putI8(-42);
    try enc.putI16(-1000);
    try enc.putI32(-100_000);
    try enc.putI64(-1_000_000_000_000);
    try enc.putF64(3.14159265358979);
    try enc.putF32(2.71828);
    try enc.putChar('Z');

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);

    try testing.expectEqual(@as(u8, 0xAB), try dec.getU8());
    try testing.expectEqual(@as(u16, 0x1234), try dec.getU16());
    try testing.expectEqual(@as(u32, 0xDEADBEEF), try dec.getU32());
    try testing.expectEqual(@as(u64, 0x0102030405060708), try dec.getU64());
    try testing.expectEqual(@as(i8, -42), try dec.getI8());
    try testing.expectEqual(@as(i16, -1000), try dec.getI16());
    try testing.expectEqual(@as(i32, -100_000), try dec.getI32());
    try testing.expectEqual(@as(i64, -1_000_000_000_000), try dec.getI64());
    try testing.expectApproxEqRel(@as(f64, 3.14159265358979), try dec.getF64(), 1e-12);
    try testing.expectApproxEqRel(@as(f32, 2.71828), try dec.getF32(), 1e-5);
    try testing.expectEqual(@as(u8, 'Z'), try dec.getChar());
}

// --------------------------------------------------------------------------
// 3. Enum encode/decode
// --------------------------------------------------------------------------
test "SBE — enum encode/decode" {
    var buf: [64]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    try enc.putEnum(TestSide, .sell);
    try enc.putEnum(TestOrdType, .limit);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);

    try testing.expectEqual(TestSide.sell, try dec.getEnum(TestSide));
    try testing.expectEqual(TestOrdType.limit, try dec.getEnum(TestOrdType));
}

test "SBE — getEnum rejects out-of-range wire bytes" {
    // u8-tagged enum: 7 is not a TestSide tag (buy=0, sell=1).
    var buf1 = [_]u8{7};
    var dec1 = SbeDecoder.init(&buf1);
    try testing.expectError(error.InvalidEnumValue, dec1.getEnum(TestSide));

    // u16-tagged enum: 999 is not a TestOrdType tag.
    var buf2: [2]u8 = undefined;
    std.mem.writeInt(u16, &buf2, 999, .little);
    var dec2 = SbeDecoder.init(&buf2);
    try testing.expectError(error.InvalidEnumValue, dec2.getEnum(TestOrdType));

    // Truncated buffer still reports BufferOverflow first.
    var empty = [_]u8{};
    var dec3 = SbeDecoder.init(&empty);
    try testing.expectError(error.BufferOverflow, dec3.getEnum(TestSide));
}

// --------------------------------------------------------------------------
// 4. Group with 0, 1, 3 entries
// --------------------------------------------------------------------------
test "SBE — group with 0 entries" {
    var buf: [64]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    try enc.beginGroup(16, 0);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    const ghdr = try dec.getGroupHeader();

    try testing.expectEqual(@as(u16, 16), ghdr.block_length);
    try testing.expectEqual(@as(u16, 0), ghdr.num_in_group);
}

test "SBE — group with 1 entry" {
    var buf: [128]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    // Group with 1 entry: each entry has a u32 and a u64 (12 bytes).
    const entry_size: u16 = 12;
    try enc.beginGroup(entry_size, 1);
    try enc.putU32(100);
    try enc.putU64(200);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    const ghdr = try dec.getGroupHeader();
    try testing.expectEqual(@as(u16, entry_size), ghdr.block_length);
    try testing.expectEqual(@as(u16, 1), ghdr.num_in_group);

    try testing.expectEqual(@as(u32, 100), try dec.getU32());
    try testing.expectEqual(@as(u64, 200), try dec.getU64());
}

test "SBE — group with 3 entries" {
    var buf: [256]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    const entry_size: u16 = 10; // u16 + u64
    try enc.beginGroup(entry_size, 3);
    for (0..3) |i| {
        try enc.putU16(@intCast(i));
        try enc.putU64(@intCast(i * 1000));
    }

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    const ghdr = try dec.getGroupHeader();
    try testing.expectEqual(@as(u16, entry_size), ghdr.block_length);
    try testing.expectEqual(@as(u16, 3), ghdr.num_in_group);

    for (0..3) |i| {
        try testing.expectEqual(@as(u16, @intCast(i)), try dec.getU16());
        try testing.expectEqual(@as(u64, @intCast(i * 1000)), try dec.getU64());
    }
}

// --------------------------------------------------------------------------
// 5. Nested groups
// --------------------------------------------------------------------------
test "SBE — nested groups" {
    var buf: [512]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    // Outer group: 2 entries, each with a u32 id.
    // Each outer entry contains a nested group of u16 values.
    const outer_block: u16 = 4; // just the u32 id
    try enc.beginGroup(outer_block, 2);

    // Outer entry 0
    try enc.putU32(10);
    // Nested group in entry 0: 2 sub-entries, each a u16.
    try enc.beginGroup(2, 2);
    try enc.putU16(100);
    try enc.putU16(101);

    // Outer entry 1
    try enc.putU32(20);
    // Nested group in entry 1: 3 sub-entries.
    try enc.beginGroup(2, 3);
    try enc.putU16(200);
    try enc.putU16(201);
    try enc.putU16(202);

    // Decode
    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);

    const outer = try dec.getGroupHeader();
    try testing.expectEqual(@as(u16, outer_block), outer.block_length);
    try testing.expectEqual(@as(u16, 2), outer.num_in_group);

    // Outer entry 0
    try testing.expectEqual(@as(u32, 10), try dec.getU32());
    const inner0 = try dec.getGroupHeader();
    try testing.expectEqual(@as(u16, 2), inner0.num_in_group);
    try testing.expectEqual(@as(u16, 100), try dec.getU16());
    try testing.expectEqual(@as(u16, 101), try dec.getU16());

    // Outer entry 1
    try testing.expectEqual(@as(u32, 20), try dec.getU32());
    const inner1 = try dec.getGroupHeader();
    try testing.expectEqual(@as(u16, 3), inner1.num_in_group);
    try testing.expectEqual(@as(u16, 200), try dec.getU16());
    try testing.expectEqual(@as(u16, 201), try dec.getU16());
    try testing.expectEqual(@as(u16, 202), try dec.getU16());
}

// --------------------------------------------------------------------------
// 6. VarData encode/decode
// --------------------------------------------------------------------------
test "SBE — vardata encode/decode" {
    var buf: [256]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    const text = "Hello, SBE!";
    try enc.putVarData(text);

    const raw_bytes = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    try enc.putVarData(&raw_bytes);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);

    const decoded_text = try dec.getVarData();
    try testing.expectEqualStrings("Hello, SBE!", decoded_text);

    const decoded_raw = try dec.getVarData();
    try testing.expectEqual(@as(usize, 4), decoded_raw.len);
    try testing.expectEqual(@as(u8, 0xDE), decoded_raw[0]);
    try testing.expectEqual(@as(u8, 0xEF), decoded_raw[3]);
}

// --------------------------------------------------------------------------
// 7. Full message: header + fields + group + vardata
// --------------------------------------------------------------------------
test "SBE — full message roundtrip" {
    // Simulate: NewOrderSingle
    //   Header: templateId=1, schemaId=1, version=1
    //   Root block: timestamp(u64) + price(i64) + quantity(u32) + side(enum u8)
    //   Group: allocations — alloc_id(u32) + alloc_qty(u32)
    //   VarData: client_order_id (string)

    var buf: [512]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    const hdr_pos = try enc.putMessageHeader(1, 1, 1);
    try enc.putU64(1_700_000_000_000); // timestamp
    try enc.putI64(15025); // price (mantissa, exponent in schema)
    try enc.putU32(100); // quantity
    try enc.putEnum(TestSide, .buy); // side
    enc.finishHeader(hdr_pos);

    // Allocations group: 2 entries, each 8 bytes (u32 + u32).
    try enc.beginGroup(8, 2);
    try enc.putU32(1001); // alloc_id
    try enc.putU32(60); // alloc_qty
    try enc.putU32(1002);
    try enc.putU32(40);

    // VarData: client order ID
    try enc.putVarData("ORD-20240101-001");

    // Decode
    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);

    const hdr = try dec.getMessageHeader();
    try testing.expectEqual(@as(u16, 1), hdr.template_id);
    try testing.expectEqual(@as(u16, 1), hdr.schema_id);
    try testing.expectEqual(@as(u16, 1), hdr.version);
    // block_length = 8 + 8 + 4 + 1 = 21
    try testing.expectEqual(@as(u16, 21), hdr.block_length);

    try testing.expectEqual(@as(u64, 1_700_000_000_000), try dec.getU64());
    try testing.expectEqual(@as(i64, 15025), try dec.getI64());
    try testing.expectEqual(@as(u32, 100), try dec.getU32());
    try testing.expectEqual(TestSide.buy, try dec.getEnum(TestSide));

    const allocs = try dec.getGroupHeader();
    try testing.expectEqual(@as(u16, 2), allocs.num_in_group);
    try testing.expectEqual(@as(u16, 8), allocs.block_length);

    try testing.expectEqual(@as(u32, 1001), try dec.getU32());
    try testing.expectEqual(@as(u32, 60), try dec.getU32());
    try testing.expectEqual(@as(u32, 1002), try dec.getU32());
    try testing.expectEqual(@as(u32, 40), try dec.getU32());

    const client_id = try dec.getVarData();
    try testing.expectEqualStrings("ORD-20240101-001", client_id);
}

// --------------------------------------------------------------------------
// 8. Optional field with null sentinel
// --------------------------------------------------------------------------
test "SBE — optional field with null sentinel" {
    var buf: [64]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    // Write a present value.
    try enc.putI64(12345);
    // Write a null sentinel (field not set).
    try enc.putI64(null_values.i64_null);
    // Write an optional u32.
    try enc.putU32(null_values.u32_null);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);

    const present = try dec.getI64();
    try testing.expect(present != null_values.i64_null);
    try testing.expectEqual(@as(i64, 12345), present);

    const absent = try dec.getI64();
    try testing.expectEqual(null_values.i64_null, absent);

    const absent_u32 = try dec.getU32();
    try testing.expectEqual(null_values.u32_null, absent_u32);
}

// --------------------------------------------------------------------------
// 9. Decoder position tracking
// --------------------------------------------------------------------------
test "SBE — decoder position tracking" {
    var buf: [128]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    try enc.putU8(1);
    try enc.putU16(2);
    try enc.putU32(3);
    try enc.putU64(4);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);

    try testing.expectEqual(@as(usize, 0), dec.position());
    _ = try dec.getU8();
    try testing.expectEqual(@as(usize, 1), dec.position());
    _ = try dec.getU16();
    try testing.expectEqual(@as(usize, 3), dec.position());
    _ = try dec.getU32();
    try testing.expectEqual(@as(usize, 7), dec.position());
    _ = try dec.getU64();
    try testing.expectEqual(@as(usize, 15), dec.position());

    // Remaining bytes.
    try testing.expectEqual(@as(usize, 0), dec.remaining());
}

// --------------------------------------------------------------------------
// 10. Buffer overflow protection
// --------------------------------------------------------------------------
test "SBE — encoder buffer overflow" {
    var buf: [4]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    // 4 bytes fit a u32.
    try enc.putU32(42);
    // But another byte should fail.
    try testing.expectError(error.BufferOverflow, enc.putU8(1));
}

test "SBE — decoder buffer overflow" {
    var buf: [2]u8 = undefined;
    buf[0] = 0;
    buf[1] = 0; // kcov-skip: test line; executes in the passing decoder-overflow test

    var dec = SbeDecoder.init(&buf);
    _ = try dec.getU16();
    // Now at position 2 with only 2 bytes — any further read should fail.
    try testing.expectError(error.BufferOverflow, dec.getU8());
}

test "SBE — encoder putMessageHeader overflow" {
    var buf: [4]u8 = undefined;
    var enc = SbeEncoder.init(&buf);
    try testing.expectError(error.BufferOverflow, enc.putMessageHeader(1, 1, 1));
}

test "SBE — decoder getMessageHeader overflow" {
    var buf: [4]u8 = undefined;
    @memset(&buf, 0);
    var dec = SbeDecoder.init(&buf);
    try testing.expectError(error.BufferOverflow, dec.getMessageHeader());
}

test "SBE — vardata overflow" {
    var buf: [8]u8 = undefined;
    var enc = SbeEncoder.init(&buf);
    // 4 bytes for length + 10 bytes for data = 14 > 8
    try testing.expectError(error.BufferOverflow, enc.putVarData("0123456789"));
}

// --------------------------------------------------------------------------
// Extra: Decimal64
// --------------------------------------------------------------------------
test "SBE — Decimal64 fromFloat/toFloat" {
    const d = Decimal64.fromFloat(123.456, -3);
    try testing.expectEqual(@as(i64, 123456), d.mantissa);
    try testing.expectEqual(@as(i8, -3), d.exponent);
    try testing.expectApproxEqRel(@as(f64, 123.456), d.toFloat(), 1e-9);
}

test "SBE — Decimal64 null sentinel" {
    const d = Decimal64.nullValue();
    try testing.expect(d.isNull());

    const valid = Decimal64.fromFloat(99.99, -2);
    try testing.expect(!valid.isNull());
}

test "SBE — Decimal64 on wire (mantissa only)" {
    // On the wire, SBE only sends the mantissa. The exponent is in the schema.
    var buf: [64]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    const price = Decimal64.fromFloat(150.25, -2);
    try enc.putI64(price.mantissa);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    const wire_mantissa = try dec.getI64();
    // Reconstruct with schema-defined exponent.
    const decoded = Decimal64{ .mantissa = wire_mantissa, .exponent = -2 };
    try testing.expectApproxEqRel(@as(f64, 150.25), decoded.toFloat(), 1e-9);
}

// --------------------------------------------------------------------------
// Extra: Fixed bytes / putBytes / getBytes
// --------------------------------------------------------------------------
test "SBE — fixed bytes roundtrip" {
    var buf: [64]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    const symbol = "AAPL    "; // 8-byte fixed symbol
    try enc.putBytes(symbol);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    const decoded = try dec.getBytes(8);
    try testing.expectEqualStrings("AAPL    ", decoded);
}

// --------------------------------------------------------------------------
// Extra: Skip
// --------------------------------------------------------------------------
test "SBE — decoder skip" {
    var buf: [32]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    try enc.putU32(111);
    try enc.putU32(222);
    try enc.putU32(333);

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    _ = try dec.getU32(); // read 111
    try dec.skip(4); // skip 222
    try testing.expectEqual(@as(u32, 333), try dec.getU32());
}

test "SBE — decoder skip overflow" {
    var buf: [4]u8 = undefined;
    @memset(&buf, 0);
    var dec = SbeDecoder.init(&buf);
    try testing.expectError(error.BufferOverflow, dec.skip(5));
}

// --------------------------------------------------------------------------
// Extra: Schema helpers
// --------------------------------------------------------------------------
test "SBE — fieldWireSize" {
    try testing.expectEqual(@as(usize, 1), fieldWireSize(.{ .name = "a", .id = 1, .field_type = .u8_type }));
    try testing.expectEqual(@as(usize, 8), fieldWireSize(.{ .name = "b", .id = 2, .field_type = .u64_type }));
    try testing.expectEqual(@as(usize, 8), fieldWireSize(.{ .name = "c", .id = 3, .field_type = .fixed_str, .length = 8 }));
}

test "SBE — blockSize" {
    const fields = [_]FieldDef{
        .{ .name = "ts", .id = 1, .field_type = .u64_type },
        .{ .name = "price", .id = 2, .field_type = .i64_type },
        .{ .name = "qty", .id = 3, .field_type = .u32_type },
        .{ .name = "side", .id = 4, .field_type = .u8_type },
    };
    // 8 + 8 + 4 + 1 = 21
    try testing.expectEqual(@as(usize, 21), blockSize(&fields));
}

// --------------------------------------------------------------------------
// Extra: Empty vardata
// --------------------------------------------------------------------------
test "SBE — empty vardata" {
    var buf: [64]u8 = undefined;
    var enc = SbeEncoder.init(&buf);

    try enc.putVarData("");

    var dec = SbeDecoder.init(buf[0..enc.encodedLength()]);
    const data = try dec.getVarData();
    try testing.expectEqual(@as(usize, 0), data.len);
}

// --------------------------------------------------------------------------
// Extra: MessageDef comptime schema
// --------------------------------------------------------------------------
test "SBE — comptime schema definition" {
    // Verify that schema definitions can be constructed at comptime.
    const new_order = comptime MessageDef{
        .name = "NewOrderSingle",
        .id = 1,
        .fields = &[_]FieldDef{
            .{ .name = "timestamp", .id = 52, .field_type = .u64_type },
            .{ .name = "price", .id = 44, .field_type = .i64_type },
            .{ .name = "quantity", .id = 38, .field_type = .u32_type },
            .{ .name = "side", .id = 54, .field_type = .u8_type },
        },
        .groups = &[_]GroupDef{
            .{
                .name = "allocations",
                .id = 78,
                .fields = &[_]FieldDef{
                    .{ .name = "alloc_id", .id = 70, .field_type = .u32_type },
                    .{ .name = "alloc_qty", .id = 80, .field_type = .u32_type },
                },
            },
        },
        .var_data = &[_]VarDataDef{
            .{ .name = "client_order_id", .id = 11 },
        },
    };

    try testing.expectEqual(@as(usize, 21), blockSize(new_order.fields));
    try testing.expectEqual(@as(usize, 8), blockSize(new_order.groups[0].fields));
    try testing.expectEqual(@as(usize, 1), new_order.var_data.len);
    try testing.expectEqualStrings("NewOrderSingle", new_order.name);
}

test "SBE — getBytesSlice zero-copy with runtime length" {
    var buf: [16]u8 = undefined;
    var enc = SbeEncoder.init(&buf);
    try enc.putBytes("hello world!");

    var dec = SbeDecoder.init(&buf);
    const s = try dec.getBytesSlice(12);
    try testing.expectEqualStrings("hello world!", s);
    // Zero-copy: the slice aliases the underlying buffer.
    try testing.expectEqual(@intFromPtr(&buf[0]), @intFromPtr(s.ptr));
    // Reading past the remaining bytes fails.
    try testing.expectError(error.BufferOverflow, dec.getBytesSlice(5));
}
