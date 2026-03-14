const config = @import("../platform/config.zig");

/// Frame header prepended to every message in the log buffer / ring buffer.
///
/// Layout (8 bytes total):
///   [0:4] frame_length: i32  — positive = data frame, negative = padding frame, 0 = uncommitted
///   [4:8] msg_type_id: i32   — user-defined message type identifier
///
/// A frame in the buffer looks like:
///   [FrameHeader (8 bytes)][payload (frame_length bytes)][padding to alignment]
pub const FrameHeader = extern struct {
    /// Length of the payload (excluding this header).
    /// Positive: committed data frame.
    /// Negative: padding frame (absolute value = padding length).
    /// Zero: slot is claimed but not yet committed.
    frame_length: i32 = 0,

    /// User-defined message type ID (for routing / codec selection).
    msg_type_id: i32 = 0,

    pub const SIZE: u32 = @sizeOf(FrameHeader);

    comptime {
        // Must be exactly 8 bytes
        if (@sizeOf(FrameHeader) != 8) @compileError("FrameHeader must be 8 bytes");
    }
};

/// Alignment for frames within buffers.
pub const FRAME_ALIGNMENT: u32 = config.frame_alignment;

/// Calculate the total aligned size of a frame (header + payload + padding).
pub inline fn alignedFrameLength(payload_length: u32) u32 {
    return config.alignUp(FrameHeader.SIZE + payload_length, FRAME_ALIGNMENT);
}

/// Maximum payload size for a single frame (prevents overflow).
pub const MAX_PAYLOAD_SIZE: u32 = 1 << 24; // 16 MB

/// Check if a frame length value indicates a padding frame.
pub inline fn isPaddingFrame(frame_length: i32) bool {
    return frame_length < 0;
}

/// Check if a frame length value indicates a committed data frame.
pub inline fn isDataFrame(frame_length: i32) bool {
    return frame_length > 0;
}

/// Check if a frame slot is uncommitted (claimed but not written).
pub inline fn isUncommitted(frame_length: i32) bool {
    return frame_length == 0;
}

// ── Tests ────────────────────────────────────────────────────

const std = @import("std");
const testing = std.testing;

test "FrameHeader size and layout" {
    try testing.expectEqual(@as(usize, 8), @sizeOf(FrameHeader));
    try testing.expectEqual(@as(u32, 8), FrameHeader.SIZE);
}

test "alignedFrameLength" {
    // header(8) + 0 payload = 8, aligned to 8 = 8
    try testing.expectEqual(@as(u32, 8), alignedFrameLength(0));
    // header(8) + 1 payload = 9, aligned to 8 = 16
    try testing.expectEqual(@as(u32, 16), alignedFrameLength(1));
    // header(8) + 8 payload = 16, aligned to 8 = 16
    try testing.expectEqual(@as(u32, 16), alignedFrameLength(8));
    // header(8) + 9 payload = 17, aligned to 8 = 24
    try testing.expectEqual(@as(u32, 24), alignedFrameLength(9));
}

test "isPaddingFrame, isDataFrame, isUncommitted" {
    try testing.expect(isPaddingFrame(-1));
    try testing.expect(isPaddingFrame(-100));
    try testing.expect(!isPaddingFrame(0));
    try testing.expect(!isPaddingFrame(1));

    try testing.expect(isDataFrame(1));
    try testing.expect(isDataFrame(100));
    try testing.expect(!isDataFrame(0));
    try testing.expect(!isDataFrame(-1));

    try testing.expect(isUncommitted(0));
    try testing.expect(!isUncommitted(1));
    try testing.expect(!isUncommitted(-1));
}

test "FrameHeader default values" {
    const hdr = FrameHeader{};
    try testing.expectEqual(@as(i32, 0), hdr.frame_length);
    try testing.expectEqual(@as(i32, 0), hdr.msg_type_id);
}
