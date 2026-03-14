const std = @import("std");
const zigbolt = @import("zigbolt");
const HdrHistogram = @import("hdr_histogram.zig").HdrHistogram;

const WARMUP_COUNT: u32 = 10_000;
const MEASURE_COUNT: u32 = 500_000;
const MSG_SIZE: u32 = 32;

const print = std.debug.print;

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt MPSC Ring Buffer Benchmark ===
        \\  Warmup:  {d} iterations
        \\  Measure: {d} iterations
        \\  Msg size: {d} bytes
        \\
    , .{ WARMUP_COUNT, MEASURE_COUNT, MSG_SIZE });

    // Single producer baseline
    try benchProducers(1);
    // Multiple producers
    try benchProducers(2);
    try benchProducers(4);
}

fn benchProducers(comptime num_producers: u32) !void {
    const Cap = 1 << 16; // 64K
    var rb = zigbolt.MpscRingBuffer(Cap).init();

    const payload: [MSG_SIZE]u8 = @splat(0xCD);

    // ── Warmup (single-threaded) ────────────────────────
    for (0..WARMUP_COUNT) |_| {
        _ = rb.write(&payload, 1);
        _ = rb.read();
    }

    // ── Throughput measurement: N producers → 1 consumer ──
    const msgs_per_producer = MEASURE_COUNT / num_producers;

    var producer_threads: [num_producers]std.Thread = undefined;
    const start = zigbolt.timestampNs();

    // Launch producers
    for (&producer_threads) |*t| {
        t.* = try std.Thread.spawn(.{}, producerFn, .{ &rb, &payload, msgs_per_producer });
    }

    // Consumer: drain all messages
    var consumed: u64 = 0;
    const target = @as(u64, msgs_per_producer) * num_producers;
    while (consumed < target) {
        if (rb.read()) |_| {
            consumed += 1;
        }
    }

    for (&producer_threads) |*t| {
        t.join();
    }

    const elapsed_ns = zigbolt.timestampNs() - start;
    const elapsed_sec = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const msg_per_sec = @as(f64, @floatFromInt(consumed)) / elapsed_sec;

    print("\n  [{d} producer(s) → 1 consumer]\n", .{num_producers});
    print("    Messages:   {d}\n", .{consumed});
    print("    Elapsed:    {d:.3} sec\n", .{elapsed_sec});
    print("    Throughput: {d:.1} M msg/sec\n", .{msg_per_sec / 1_000_000.0});

    if (msg_per_sec > 10_000_000) {
        print("    [PASS] > 10M msg/sec\n", .{});
    } else {
        print("    [MISS] {d:.1}M msg/sec (target: >10M)\n", .{msg_per_sec / 1_000_000.0});
    }
}

fn producerFn(rb: anytype, payload: []const u8, count: u32) void {
    var sent: u32 = 0;
    while (sent < count) {
        if (rb.write(payload, 1)) {
            sent += 1;
        } else {
            // Backoff on full buffer
            std.atomic.spinLoopHint();
        }
    }
}
