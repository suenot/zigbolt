const std = @import("std");
const zigbolt = @import("zigbolt");
const HdrHistogram = @import("hdr_histogram.zig").HdrHistogram;

const WARMUP_COUNT: u32 = 10_000;
const SAMPLES: u32 = 50_000;
const BATCH: u32 = 100;

const print = std.debug.print;

fn logCountHandler(_: []const u8, _: i32) void {}

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt LogBuffer Claim/Commit/Read Benchmark ===
        \\  Warmup:  {d} iterations
        \\  Samples: {d} x {d} ops
        \\
    , .{ WARMUP_COUNT, SAMPLES, BATCH });

    inline for (.{ 8, 32, 64, 256 }) |msg_size| {
        try benchSize(msg_size);
    }
}

fn benchSize(comptime msg_size: u32) !void {
    const LB = zigbolt.LogBuffer(.{ .term_length = 1 << 16 }); // 64K terms
    var buf = LB.init();
    var histogram = HdrHistogram{};
    const frame_hdr_size = @sizeOf(zigbolt.FrameHeader);

    const payload: [msg_size]u8 = @splat(0xEF);

    // Warmup
    for (0..WARMUP_COUNT) |_| {
        if (buf.claim(msg_size)) |c| {
            const dest = c.term_buffer[c.term_offset + frame_hdr_size ..][0..msg_size];
            @memcpy(dest, &payload);
            buf.commit(c, 1);
        }
        _ = buf.read(&logCountHandler, 1);
    }

    buf = LB.init();

    // Batched measurement
    for (0..SAMPLES) |_| {
        const t0 = zigbolt.timestampNs();
        for (0..BATCH) |_| {
            const c = buf.claim(msg_size) orelse {
                _ = buf.read(&logCountHandler, 4096);
                continue;
            };
            const dest = c.term_buffer[c.term_offset + frame_hdr_size ..][0..msg_size];
            @memcpy(dest, &payload);
            buf.commit(c, 1);
            _ = buf.read(&logCountHandler, 1);
        }
        const t1 = zigbolt.timestampNs();
        histogram.record((t1 -| t0) / BATCH);
    }

    const p50 = histogram.percentile(50);
    const p99 = histogram.percentile(99);
    const mean_ns = histogram.mean();
    const throughput = if (mean_ns > 0) 1_000_000_000.0 / mean_ns else 0;

    print("\n  [{d}B payload]\n", .{msg_size});
    print("    Samples:    {d}\n", .{histogram.total_count});
    print("    Min:        {d} ns\n", .{histogram.min_value});
    print("    p50:        {d} ns (claim+commit+read)\n", .{p50});
    print("    p99:        {d} ns\n", .{p99});
    print("    p99.9:      {d} ns\n", .{histogram.percentile(99.9)});
    print("    Max:        {d} ns\n", .{histogram.max_value});
    print("    Throughput: {d:.1} M msg/sec\n", .{throughput / 1_000_000.0});

    if (p50 < 200) {
        print("    [PASS] p50 = {d} ns (target: <200 ns)\n", .{p50});
    } else {
        print("    [MISS] p50 = {d} ns (target: <200 ns)\n", .{p50});
    }
}
