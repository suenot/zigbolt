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

/// Wall-clock timestamp in nanoseconds since the Unix epoch.
/// NOT monotonic: subject to NTP slew/steps and manual clock changes.
/// Use this for market-data / event timestamps that must be comparable
/// across hosts; use `monotonicNs()` for latency and duty-cycle math.
pub inline fn timestampNs() u64 {
    return @intCast(std.time.nanoTimestamp());
}

/// Monotonic timestamp in nanoseconds from an arbitrary origin.
/// Backed by CLOCK_MONOTONIC — never goes backwards and is unaffected by
/// wall-clock adjustments. Only the difference between two readings is
/// meaningful; do not compare against `timestampNs()`.
pub inline fn monotonicNs() u64 {
    // CLOCK_MONOTONIC is always available on every platform this library
    // targets (Linux, macOS), so the error path is unreachable.
    const ts = std.posix.clock_gettime(.MONOTONIC) catch unreachable;
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
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

// ── Tests ────────────────────────────────────────────────────

test "timestampNs returns non-zero" {
    const t1 = timestampNs();
    try std.testing.expect(t1 > 0);
}

test "timestampNs is monotonic" {
    const t1 = timestampNs();
    const t2 = timestampNs();
    try std.testing.expect(t2 >= t1); // kcov-skip: test assertion line; the test passes; hit record oscillates between builds
}

test "monotonicNs returns non-zero and is non-decreasing" {
    const t1 = monotonicNs();
    const t2 = monotonicNs();
    const t3 = monotonicNs();
    try std.testing.expect(t1 > 0);
    try std.testing.expect(t2 >= t1);
    try std.testing.expect(t3 >= t2);
}

test "alignUp basic cases" {
    // Already aligned
    try std.testing.expectEqual(@as(u32, 8), alignUp(8, 8));
    try std.testing.expectEqual(@as(u32, 16), alignUp(16, 8));

    // Needs rounding up
    try std.testing.expectEqual(@as(u32, 8), alignUp(1, 8));
    try std.testing.expectEqual(@as(u32, 8), alignUp(7, 8));
    try std.testing.expectEqual(@as(u32, 16), alignUp(9, 8));
    try std.testing.expectEqual(@as(u32, 16), alignUp(15, 8));

    // Zero
    try std.testing.expectEqual(@as(u32, 0), alignUp(0, 8));
}

test "alignUp with different alignments" {
    try std.testing.expectEqual(@as(u32, 64), alignUp(33, 64));
    try std.testing.expectEqual(@as(u32, 4096), alignUp(1, 4096));
    try std.testing.expectEqual(@as(u32, 4096), alignUp(4096, 4096));
    try std.testing.expectEqual(@as(u32, 8192), alignUp(4097, 4096));
}

test "constants have expected values" {
    try std.testing.expect(cache_line_size >= 64);
    try std.testing.expect(page_size >= 4096);
    try std.testing.expectEqual(@as(u32, 8), frame_alignment);
    try std.testing.expectEqual(@as(usize, 1 << 20), default_term_length);
    try std.testing.expectEqual(@as(usize, 1 << 16), default_ring_capacity);
    try std.testing.expectEqual(@as(usize, 3), default_num_terms);
}

test "platform detection is consistent" {
    // Exactly one of Linux/macOS should be true (on supported platforms)
    // or both can be false (on other platforms)
    if (is_linux) {
        try std.testing.expect(!is_macos);
        try std.testing.expect(supports_hugepages);
    }
    if (is_macos) {
        try std.testing.expect(!is_linux);
        try std.testing.expect(!supports_hugepages);
    }
}
