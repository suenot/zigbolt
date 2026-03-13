const std = @import("std");
const zigbolt = @import("zigbolt");
const HdrHistogram = @import("hdr_histogram.zig").HdrHistogram;

const WARMUP_COUNT: u32 = 5_000;
const MEASURE_COUNT: u32 = 50_000;
const MSG_SIZE: u32 = 32;
const PORT: u16 = 44444;

const print = std.debug.print;

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt UDP Ping-Pong Benchmark ===
        \\  Target: RTT < 5 μs (io_uring on Linux)
        \\  Warmup:  {d} messages
        \\  Measure: {d} messages
        \\  Msg size: {d} bytes
        \\  Port: {d}
        \\
    , .{ WARMUP_COUNT, MEASURE_COUNT, MSG_SIZE, PORT });

    // Create sender socket (sends to localhost)
    const loopback = try std.net.Address.parseIp4("127.0.0.1", PORT);

    var sender = try zigbolt.UdpChannel.init(.{
        .bind_address = try std.net.Address.parseIp4("127.0.0.1", PORT + 1),
        .remote_address = loopback,
        .non_blocking = true,
    });
    defer sender.deinit();

    // Create receiver socket
    var receiver = try zigbolt.UdpChannel.init(.{
        .bind_address = loopback,
        .non_blocking = true,
    });
    defer receiver.deinit();

    var histogram = HdrHistogram{};
    var buf: [1024]u8 = undefined;

    // ── Warmup ───────────────────────────────────────────
    print("Warming up ({d} messages)...\n", .{WARMUP_COUNT});

    var seq: u32 = 0;
    while (seq < WARMUP_COUNT) : (seq += 1) {
        var msg: [MSG_SIZE]u8 = @splat(0);
        _ = sender.send(&msg, null) catch continue;

        // Try to receive (non-blocking)
        _ = receiver.recv(&buf) catch {};
    }

    // Small delay to let things settle
    std.Thread.sleep(1_000_000); // 1ms

    // Drain any remaining datagrams
    while (true) {
        const r = receiver.recv(&buf) catch break;
        if (r == null) break;
    }

    // ── Measurement ──────────────────────────────────────
    print("Measuring ({d} messages)...\n", .{MEASURE_COUNT});

    seq = 0;
    while (seq < MEASURE_COUNT) : (seq += 1) {
        const send_time = zigbolt.timestampNs();

        var msg: [MSG_SIZE]u8 = @splat(0);
        @memcpy(msg[0..8], std.mem.asBytes(&send_time));

        _ = sender.send(&msg, null) catch continue;

        // Busy-poll for response (loopback)
        var attempts: u32 = 0;
        while (attempts < 10000) : (attempts += 1) {
            if (receiver.recv(&buf) catch null) |_| {
                break;
            }
        }

        const recv_time = zigbolt.timestampNs();
        const rtt = recv_time -| send_time;
        histogram.record(rtt);
    }

    // ── Results ──────────────────────────────────────────
    print("\n=== UDP Loopback Results ===\n", .{});
    print("  Total samples: {d}\n", .{histogram.total_count});
    print("  Min:     {d} ns\n", .{histogram.min_value});
    print("  Mean:    {d:.1} ns\n", .{histogram.mean()});
    print("  p50:     {d} ns\n", .{histogram.percentile(50)});
    print("  p90:     {d} ns\n", .{histogram.percentile(90)});
    print("  p99:     {d} ns\n", .{histogram.percentile(99)});
    print("  p99.9:   {d} ns\n", .{histogram.percentile(99.9)});
    print("  Max:     {d} ns\n", .{histogram.max_value});

    const p50 = histogram.percentile(50);
    print("\n", .{});
    if (p50 < 5000) {
        print("  [PASS] p50 = {d} ns (target: <5 μs)\n", .{p50});
    } else {
        print("  [MISS] p50 = {d} ns (target: <5 μs)\n", .{p50});
    }
}
