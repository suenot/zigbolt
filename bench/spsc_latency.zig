const std = @import("std");
const zigbolt = @import("zigbolt");
const HdrHistogram = @import("hdr_histogram.zig").HdrHistogram;

const WARMUP_COUNT: u32 = 10_000;
const SAMPLES: u32 = 100_000;
const BATCH: u32 = 100;

const print = std.debug.print;

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt SPSC Ring Buffer Latency Benchmark ===
        \\  Warmup:  {d} iterations
        \\  Samples: {d} x {d} ops = {d} total
        \\
    , .{ WARMUP_COUNT, SAMPLES, BATCH, @as(u64, SAMPLES) * BATCH });

    inline for (.{ 8, 32, 64, 256 }) |msg_size| {
        try benchSize(msg_size);
    }
}

fn benchSize(comptime msg_size: u32) !void {
    const Cap = 1 << 16;
    var rb = zigbolt.SpscRingBuffer(Cap).init();
    var histogram = HdrHistogram{};

    const payload: [msg_size]u8 = @splat(0xAB);

    // Warmup
    for (0..WARMUP_COUNT) |_| {
        _ = rb.write(&payload, 1);
        _ = rb.read();
    }

    // Batched measurement
    for (0..SAMPLES) |_| {
        const t0 = zigbolt.timestampNs();
        for (0..BATCH) |_| {
            _ = rb.write(&payload, 1);
            _ = rb.read();
        }
        const t1 = zigbolt.timestampNs();
        histogram.record((t1 -| t0) / BATCH);
    }

    const p50 = histogram.percentile(50);
    const mean_ns = histogram.mean();

    print("\n  [{d}B payload]\n", .{msg_size});
    print("    Samples:  {d}\n", .{histogram.total_count});
    print("    Min:      {d} ns\n", .{histogram.min_value});
    print("    Mean:     {d:.1} ns\n", .{mean_ns});
    print("    p50:      {d} ns\n", .{p50});
    print("    p99:      {d} ns\n", .{histogram.percentile(99)});
    print("    p99.9:    {d} ns\n", .{histogram.percentile(99.9)});
    print("    Max:      {d} ns\n", .{histogram.max_value});
    print("    Thru:     {d:.1} M/s\n", .{if (mean_ns > 0) 1e9 / mean_ns / 1e6 else 0});

    if (p50 < 100) {
        print("    [PASS] p50 = {d} ns (target: <100 ns)\n", .{p50});
    } else {
        print("    [MISS] p50 = {d} ns (target: <100 ns)\n", .{p50});
    }
}
