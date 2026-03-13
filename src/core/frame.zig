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
