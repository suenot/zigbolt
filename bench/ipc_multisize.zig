const std = @import("std");
const zigbolt = @import("zigbolt");
const HdrHistogram = @import("hdr_histogram.zig").HdrHistogram;

const WARMUP_COUNT: u32 = 10_000;
const SAMPLES: u32 = 50_000;
const BATCH: u32 = 100;
const TERM_LENGTH: usize = 4 << 20; // 4 MB

const print = std.debug.print;

/// Message sizes inspired by trading-ipc-bench:
///   64B  = trading signal / tick
///   256B = order message with metadata
///  1024B = market data snapshot
const sizes = [_]u32{ 64, 256, 1024 };

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt IPC Multi-Size Latency Benchmark ===
        \\  Warmup:  {d} messages
        \\  Samples: {d} x {d} ops
        \\  Sizes:   64B, 256B, 1024B
        \\
    , .{ WARMUP_COUNT, SAMPLES, BATCH });

    print("\n  ┌────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────────┐\n", .{});
    print("  │  Size  │  Min     │  p50     │  p99     │  p99.9   │  Max     │  Throughput   │\n", .{});
    print("  ├────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────────┤\n", .{});

    for (sizes) |msg_size| {
        try benchSize(msg_size);
    }

    print("  └────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────────┘\n", .{});
}

fn benchSize(msg_size: u32) !void {
    const channel_name = "/zigbolt_bench_ms";

    var ch = try zigbolt.IpcChannel.create(channel_name, .{
        .term_length = TERM_LENGTH,
        .pre_fault = true,
    });
    defer ch.deinit();

    var histogram = HdrHistogram{};
    var msg_buf: [1024]u8 = @splat(0xAB);
    const payload = msg_buf[0..msg_size];

    // Warmup
    for (0..WARMUP_COUNT) |_| {
        ch.publish(payload, 1) catch {
            _ = ch.poll(&noopHandler, 1024);
            ch.publish(payload, 1) catch continue;
        };
        _ = ch.poll(&noopHandler, 1);
    }

    // Reset
    ch.deinit();
    ch = try zigbolt.IpcChannel.create(channel_name, .{
        .term_length = TERM_LENGTH,
        .pre_fault = true,
    });

    // Batched measurement
    for (0..SAMPLES) |_| {
        const t0 = zigbolt.timestampNs();
        for (0..BATCH) |_| {
            ch.publish(payload, 1) catch {
                _ = ch.poll(&noopHandler, 1024);
                ch.publish(payload, 1) catch continue;
            };
            _ = ch.poll(&noopHandler, 1);
        }
        const t1 = zigbolt.timestampNs();
        histogram.record((t1 -| t0) / BATCH);
    }

    const min_ns = histogram.min_value;
    const p50 = histogram.percentile(50);
    const p99 = histogram.percentile(99);
    const p999 = histogram.percentile(99.9);
    const max_ns = histogram.max_value;
    const mean_ns = histogram.mean();
    const throughput = if (mean_ns > 0) 1_000_000_000.0 / mean_ns else 0;

    print("  │ {d:>4}B  │ {d:>6} ns │ {d:>6} ns │ {d:>6} ns │ {d:>6} ns │ {d:>6} ns │ {d:>7.1} M/s   │\n", .{
        msg_size,
        min_ns,
        p50,
        p99,
        p999,
        max_ns,
        throughput / 1_000_000.0,
    });
}

fn noopHandler(_: zigbolt.IpcChannel.ReadResult) void {}
