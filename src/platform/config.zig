const std = @import("std");
const builtin = @import("builtin");

/// Cache line size for false-sharing prevention.
/// 128 bytes on both aarch64 (Apple Silicon) and modern x86_64.
pub const cache_line_size = std.atomic.cache_line;

/// System page size.
pub const page_size = std.heap.page_size_min;

/// Platform detection.
pub const is_linux = builtin.os.tag == .linux;
pub const is_macos = builtin.os.tag == .macos;

/// Whether the platform supports hugepages natively.
pub const supports_hugepages = is_linux;

/// Whether io_uring is available.
pub const supports_io_uring = is_linux;

/// Frame alignment for wire format (8 bytes).
pub const frame_alignment: u32 = 8;

/// Default term length for log buffers (1 MB).
pub const default_term_length: usize = 1 << 20;

/// Default ring buffer capacity (64K entries).
pub const default_ring_capacity: usize = 1 << 16;

/// Number of term buffers (triple-buffered).
pub const default_num_terms: usize = 3;

/// Nanosecond timestamp using the most accurate source available.
pub inline fn timestampNs() u64 {
    if (is_macos) {
        // mach_absolute_time is ~nanosecond resolution on Apple Silicon
        return @intCast(std.time.nanoTimestamp());
    } else if (is_linux) {
        return @intCast(std.time.nanoTimestamp());
    } else {
        return @intCast(std.time.nanoTimestamp());
    }
}

/// Aligned size: rounds up `size` to the nearest multiple of `alignment`.
pub inline fn alignUp(size: u32, alignment: u32) u32 {
    return (size + alignment - 1) & ~(alignment - 1);
}

comptime {
    // Sanity checks
    std.debug.assert(cache_line_size >= 64);
    std.debug.assert(page_size >= 4096);
    std.debug.assert(frame_alignment == 8);
}
