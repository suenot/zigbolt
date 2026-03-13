const std = @import("std");

/// HDR (High Dynamic Range) Histogram for accurate latency percentile measurement.
/// Pre-allocated, zero-allocation on the hot path.
///
/// Range: 1 ns to 1 second (1,000,000,000 ns) with 3 significant digits.
/// Uses a logarithmic bucket structure for compact representation.
pub const HdrHistogram = struct {
    const BUCKET_COUNT = 10_000; // covers up to ~10ms with 1ns precision in low range
    const OVERFLOW_BUCKET = BUCKET_COUNT - 1;

    counts: [BUCKET_COUNT]u64 = [_]u64{0} ** BUCKET_COUNT,
    total_count: u64 = 0,
    min_value: u64 = std.math.maxInt(u64),
    max_value: u64 = 0,
    sum: u64 = 0,

    /// Record a latency value in nanoseconds.
    pub fn record(self: *HdrHistogram, value: u64) void {
        const bucket = bucketIndex(value);
        self.counts[bucket] += 1;
        self.total_count += 1;
        self.sum += value;

        if (value < self.min_value) self.min_value = value;
        if (value > self.max_value) self.max_value = value;
    }

    /// Get the value at a given percentile (0.0 to 100.0).
    pub fn percentile(self: *const HdrHistogram, p: f64) u64 {
        if (self.total_count == 0) return 0;

        const target: u64 = @intFromFloat(@ceil(@as(f64, @floatFromInt(self.total_count)) * p / 100.0));
        var cumulative: u64 = 0;

        for (self.counts, 0..) |count, i| {
            cumulative += count;
            if (cumulative >= target) {
                return bucketToValue(i);
            }
        }
        return self.max_value;
    }

    /// Mean value.
    pub fn mean(self: *const HdrHistogram) f64 {
        if (self.total_count == 0) return 0;
        return @as(f64, @floatFromInt(self.sum)) / @as(f64, @floatFromInt(self.total_count));
    }

    /// Reset all counters.
    pub fn reset(self: *HdrHistogram) void {
        @memset(&self.counts, 0);
        self.total_count = 0;
        self.min_value = std.math.maxInt(u64);
        self.max_value = 0;
        self.sum = 0;
    }

    /// Print a summary of the histogram.
    pub fn printSummary(self: *const HdrHistogram, writer: anytype) !void {
        try writer.print(
            \\╔══════════════════════════════════════╗
            \\║       ZigBolt Latency Report          ║
            \\╠══════════════════════════════════════╣
            \\║  Total samples: {d:>20}   ║
            \\║  Min:           {d:>14} ns   ║
            \\║  Mean:          {d:>14.1} ns   ║
            \\║  p50:           {d:>14} ns   ║
            \\║  p90:           {d:>14} ns   ║
            \\║  p99:           {d:>14} ns   ║
            \\║  p99.9:         {d:>14} ns   ║
            \\║  p99.99:        {d:>14} ns   ║
            \\║  Max:           {d:>14} ns   ║
            \\╚══════════════════════════════════════╝
            \\
        , .{
            self.total_count,
            self.min_value,
            self.mean(),
            self.percentile(50),
            self.percentile(90),
            self.percentile(99),
            self.percentile(99.9),
            self.percentile(99.99),
            self.max_value,
        });
    }

    // ── Internal ─────────────────────────────────────────

    /// Map a nanosecond value to a bucket index.
    /// Linear for 0-999 ns, then logarithmic scaling.
    fn bucketIndex(value: u64) usize {
        if (value < 1000) {
            // 0-999 ns: 1 ns per bucket
            return @intCast(value);
        } else if (value < 10_000) {
            // 1-10 μs: 10 ns per bucket
            return 1000 + @as(usize, @intCast((value - 1000) / 10));
        } else if (value < 100_000) {
            // 10-100 μs: 100 ns per bucket
            return 1900 + @as(usize, @intCast((value - 10_000) / 100));
        } else if (value < 1_000_000) {
            // 100 μs - 1 ms: 1 μs per bucket
            return 2800 + @as(usize, @intCast((value - 100_000) / 1000));
        } else if (value < 10_000_000) {
            // 1-10 ms: 10 μs per bucket
            return 3700 + @as(usize, @intCast((value - 1_000_000) / 10_000));
        } else {
            // > 10 ms: overflow
            const idx = 4600 + @as(usize, @intCast((value - 10_000_000) / 100_000));
            return @min(idx, OVERFLOW_BUCKET);
        }
    }

    /// Map a bucket index back to a representative nanosecond value.
    fn bucketToValue(idx: usize) u64 {
        if (idx < 1000) {
            return idx;
        } else if (idx < 1900) {
            return 1000 + (idx - 1000) * 10;
        } else if (idx < 2800) {
            return 10_000 + (idx - 1900) * 100;
        } else if (idx < 3700) {
            return 100_000 + (idx - 2800) * 1000;
        } else if (idx < 4600) {
            return 1_000_000 + (idx - 3700) * 10_000;
        } else {
            return 10_000_000 + (idx - 4600) * 100_000;
        }
    }
};

test "HdrHistogram basic" {
    var h = HdrHistogram{};

    h.record(100);
    h.record(200);
    h.record(300);

    try std.testing.expectEqual(@as(u64, 3), h.total_count);
    try std.testing.expectEqual(@as(u64, 100), h.min_value);
    try std.testing.expectEqual(@as(u64, 300), h.max_value);
    try std.testing.expectEqual(@as(u64, 200), h.percentile(50));
}
