const std = @import("std");
const zigbolt = @import("zigbolt");
const HdrHistogram = @import("hdr_histogram.zig").HdrHistogram;

const WARMUP_COUNT: u32 = 10_000;
const MEASURE_COUNT: u32 = 100_000;
const MSG_SIZE: u32 = 32;
const CHANNEL_NAME = "/zigbolt_bench_pp";
const TERM_LENGTH: usize = 1 << 20; // 1 MB

const print = std.debug.print;

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt IPC Ping-Pong Benchmark ===
        \\  Target: RTT < 200 ns
        \\  Warmup:  {d} messages
        \\  Measure: {d} messages
        \\  Msg size: {d} bytes
        \\
    , .{ WARMUP_COUNT, MEASURE_COUNT, MSG_SIZE });

    var pub_ch = zigbolt.IpcChannel.create(CHANNEL_NAME, .{
        .term_length = TERM_LENGTH,
        .pre_fault = true,
    }) catch |err| {
        print("Failed to create IPC channel: {}\n", .{err});
        return;
    };
    defer pub_ch.deinit();

    var histogram = HdrHistogram{};

    // ── Warmup ───────────────────────────────────────────
    print("Warming up ({d} messages)...\n", .{WARMUP_COUNT});

    var seq: u64 = 0;
    while (seq < WARMUP_COUNT) : (seq += 1) {
        var msg_bytes: [MSG_SIZE]u8 = @splat(0);
        pub_ch.publish(&msg_bytes, 1) catch continue;
        _ = pub_ch.poll(&noopHandler, 1);
    }

    // Reset channel for measurement
    pub_ch.deinit();
    pub_ch = try zigbolt.IpcChannel.create(CHANNEL_NAME, .{
        .term_length = TERM_LENGTH,
        .pre_fault = true,
    });

    // ── Measurement ──────────────────────────────────────
    print("Measuring ({d} messages)...\n", .{MEASURE_COUNT});

    seq = 0;
    while (seq < MEASURE_COUNT) : (seq += 1) {
        const send_time = zigbolt.timestampNs();

        var msg_bytes: [MSG_SIZE]u8 = @splat(0);
        @memcpy(msg_bytes[0..8], std.mem.asBytes(&send_time));

        pub_ch.publish(&msg_bytes, 1) catch continue;
        _ = pub_ch.poll(&noopHandler, 1);

        const recv_time = zigbolt.timestampNs();
        const rtt = recv_time -| send_time;
        histogram.record(rtt);
    }

    // ── Results ──────────────────────────────────────────
    print("\n=== Results ===\n", .{});
    print("  Total samples: {d}\n", .{histogram.total_count});
    print("  Min:     {d} ns\n", .{histogram.min_value});
    print("  Mean:    {d:.1} ns\n", .{histogram.mean()});
    print("  p50:     {d} ns\n", .{histogram.percentile(50)});
    print("  p90:     {d} ns\n", .{histogram.percentile(90)});
    print("  p99:     {d} ns\n", .{histogram.percentile(99)});
    print("  p99.9:   {d} ns\n", .{histogram.percentile(99.9)});
    print("  p99.99:  {d} ns\n", .{histogram.percentile(99.99)});
    print("  Max:     {d} ns\n", .{histogram.max_value});

    const p50 = histogram.percentile(50);
    const p99 = histogram.percentile(99);
    print("\n", .{});
    if (p50 < 200) {
        print("  [PASS] p50 = {d} ns (target: <200 ns)\n", .{p50});
    } else {
        print("  [MISS] p50 = {d} ns (target: <200 ns)\n", .{p50});
    }
    if (p99 < 1000) {
        print("  [PASS] p99 = {d} ns (target: <1000 ns)\n", .{p99});
    } else {
        print("  [MISS] p99 = {d} ns (target: <1000 ns)\n", .{p99});
    }
}

fn noopHandler(_: zigbolt.IpcChannel.ReadResult) void {}
