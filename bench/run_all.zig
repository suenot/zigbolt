const std = @import("std");
const zigbolt = @import("zigbolt");
const HdrHistogram = @import("hdr_histogram.zig").HdrHistogram;

const print = std.debug.print;

/// Number of operations per timing sample (to overcome timer resolution).
const BATCH = 100;

/// Benchmark result for JSON output.
const BenchResult = struct {
    name: []const u8,
    msg_size: u32,
    iterations: u64,
    min_ns: u64,
    p50_ns: u64,
    p99_ns: u64,
    p999_ns: u64,
    max_ns: u64,
    mean_ns: f64,
    throughput_msg_s: f64,
};

var results: [32]BenchResult = undefined;
var result_count: usize = 0;

fn addResult(r: BenchResult) void {
    results[result_count] = r;
    result_count += 1;
}

pub fn main() !void {
    print(
        \\
        \\╔═══════════════════════════════════════════════════╗
        \\║        ZigBolt Full Benchmark Suite               ║
        \\╚═══════════════════════════════════════════════════╝
        \\
    , .{});

    // ── 1. SPSC Ring Buffer ──────────────────────────────
    print("\n── SPSC Ring Buffer ────────────────────────────────\n", .{});
    inline for (.{ 8, 32, 64, 256 }) |sz| {
        try benchSpsc(sz);
    }

    // ── 2. IPC Channel ───────────────────────────────────
    print("\n── IPC Channel (shared memory) ─────────────────────\n", .{});
    inline for (.{ 64, 256, 1024 }) |sz| {
        try benchIpc(sz);
    }

    // ── 3. WireCodec ─────────────────────────────────────
    print("\n── WireCodec ───────────────────────────────────────\n", .{});
    try benchCodecTick();

    // ── 4. LogBuffer ─────────────────────────────────────
    print("\n── LogBuffer (claim/commit/read) ────────────────────\n", .{});
    inline for (.{ 32, 64, 256 }) |sz| {
        try benchLogBuffer(sz);
    }

    // ── Summary Table ────────────────────────────────────
    printSummary();

    // ── JSON Output ──────────────────────────────────────
    writeJson();
}

// ═════════════════════════════════════════════════════════
// Benchmark implementations
// ═════════════════════════════════════════════════════════

fn benchSpsc(comptime msg_size: u32) !void {
    const Cap = 1 << 16;
    var rb = zigbolt.SpscRingBuffer(Cap).init();
    var h = HdrHistogram{};
    const payload: [msg_size]u8 = @splat(0xAB);

    // Warmup
    for (0..10_000) |_| {
        _ = rb.write(&payload, 1);
        _ = rb.read();
    }

    // Batched measurement: time BATCH ops, record per-op average
    const samples: u64 = 100_000;
    for (0..samples) |_| {
        const t0 = zigbolt.timestampNs();
        for (0..BATCH) |_| {
            _ = rb.write(&payload, 1);
            _ = rb.read();
        }
        const t1 = zigbolt.timestampNs();
        h.record((t1 -| t0) / BATCH);
    }

    const mean_ns = h.mean();
    addResult(.{
        .name = "SPSC",
        .msg_size = msg_size,
        .iterations = samples * BATCH,
        .min_ns = h.min_value,
        .p50_ns = h.percentile(50),
        .p99_ns = h.percentile(99),
        .p999_ns = h.percentile(99.9),
        .max_ns = h.max_value,
        .mean_ns = mean_ns,
        .throughput_msg_s = if (mean_ns > 0) 1e9 / mean_ns else 0,
    });

    print("  SPSC {d:>4}B:  p50={d:>5} ns  p99={d:>5} ns  ({d:.1} M/s)\n", .{
        msg_size,
        h.percentile(50),
        h.percentile(99),
        if (mean_ns > 0) 1e9 / mean_ns / 1e6 else 0,
    });
}

fn benchIpc(comptime msg_size: u32) !void {
    const channel_name = "/zigbolt_bench_all";
    var ch = try zigbolt.IpcChannel.create(channel_name, .{
        .term_length = 4 << 20,
        .pre_fault = true,
    });
    defer ch.deinit();

    var h = HdrHistogram{};
    var payload: [msg_size]u8 = @splat(0xAB);

    // Warmup
    for (0..10_000) |_| {
        ch.publish(&payload, 1) catch {
            _ = ch.poll(&noopHandler, 1024);
            ch.publish(&payload, 1) catch continue;
        };
        _ = ch.poll(&noopHandler, 1);
    }

    // Reset
    ch.deinit();
    ch = try zigbolt.IpcChannel.create(channel_name, .{
        .term_length = 4 << 20,
        .pre_fault = true,
    });

    const samples: u64 = 50_000;
    for (0..samples) |_| {
        const t0 = zigbolt.timestampNs();
        for (0..BATCH) |_| {
            ch.publish(&payload, 1) catch {
                _ = ch.poll(&noopHandler, 1024);
                ch.publish(&payload, 1) catch continue;
            };
            _ = ch.poll(&noopHandler, 1);
        }
        const t1 = zigbolt.timestampNs();
        h.record((t1 -| t0) / BATCH);
    }

    const mean_ns = h.mean();
    addResult(.{
        .name = "IPC",
        .msg_size = msg_size,
        .iterations = samples * BATCH,
        .min_ns = h.min_value,
        .p50_ns = h.percentile(50),
        .p99_ns = h.percentile(99),
        .p999_ns = h.percentile(99.9),
        .max_ns = h.max_value,
        .mean_ns = mean_ns,
        .throughput_msg_s = if (mean_ns > 0) 1e9 / mean_ns else 0,
    });

    print("  IPC  {d:>4}B:  p50={d:>5} ns  p99={d:>5} ns  ({d:.1} M/s)\n", .{
        msg_size,
        h.percentile(50),
        h.percentile(99),
        if (mean_ns > 0) 1e9 / mean_ns / 1e6 else 0,
    });
}

fn benchCodecTick() !void {
    const Codec = zigbolt.WireCodec(zigbolt.TickMessage);
    var buf: [@sizeOf(zigbolt.TickMessage)]u8 = undefined;

    const N: u64 = 10_000_000;

    // Warmup with varying data to prevent constant folding
    for (0..100_000) |i| {
        var msg = std.mem.zeroes(zigbolt.TickMessage);
        msg.timestamp_ns = @intCast(i);
        Codec.encode(&msg, &buf);
    }

    // Encode — vary input to prevent dead-code elimination
    var encode_sink: u8 = 0;
    const t0 = zigbolt.timestampNs();
    for (0..N) |i| {
        var msg = std.mem.zeroes(zigbolt.TickMessage);
        msg.timestamp_ns = @intCast(i);
        msg.symbol_id = @intCast(i & 0xFFFF);
        Codec.encode(&msg, &buf);
        encode_sink +%= buf[0];
    }
    const encode_ns = zigbolt.timestampNs() - t0;
    std.mem.doNotOptimizeAway(encode_sink);

    // Decode — vary buffer to prevent loop folding
    var msg_for_dec = std.mem.zeroes(zigbolt.TickMessage);
    msg_for_dec.timestamp_ns = 42;
    Codec.encode(&msg_for_dec, &buf);
    const t1 = zigbolt.timestampNs();
    var dummy: u64 = 0;
    for (0..N) |i| {
        buf[0] = @intCast(i & 0xFF);
        const d = Codec.decode(&buf);
        dummy +%= d.timestamp_ns;
    }
    const decode_ns = zigbolt.timestampNs() - t1;
    std.mem.doNotOptimizeAway(dummy);

    const enc_ns_each = @as(f64, @floatFromInt(encode_ns)) / @as(f64, @floatFromInt(N));
    const dec_ns_each = @as(f64, @floatFromInt(decode_ns)) / @as(f64, @floatFromInt(N));
    const enc_rate = @as(f64, @floatFromInt(N)) / (@as(f64, @floatFromInt(encode_ns)) / 1e9);
    const dec_rate = @as(f64, @floatFromInt(N)) / (@as(f64, @floatFromInt(decode_ns)) / 1e9);

    // Clamp to avoid inf/NaN in JSON output (sub-ns ops are legitimate)
    const enc_mean = @max(enc_ns_each, 0.01);
    const dec_mean = @max(dec_ns_each, 0.01);
    const safe_enc_rate = if (encode_ns > 0) enc_rate else 999_999_999_999.0;
    const safe_dec_rate = if (decode_ns > 0) dec_rate else 999_999_999_999.0;

    addResult(.{
        .name = "Codec-Enc",
        .msg_size = @sizeOf(zigbolt.TickMessage),
        .iterations = N,
        .min_ns = 0,
        .p50_ns = @intFromFloat(@max(enc_ns_each, 0)),
        .p99_ns = 0,
        .p999_ns = 0,
        .max_ns = 0,
        .mean_ns = enc_mean,
        .throughput_msg_s = safe_enc_rate,
    });
    addResult(.{
        .name = "Codec-Dec",
        .msg_size = @sizeOf(zigbolt.TickMessage),
        .iterations = N,
        .min_ns = 0,
        .p50_ns = @intFromFloat(@max(dec_ns_each, 0)),
        .p99_ns = 0,
        .p999_ns = 0,
        .max_ns = 0,
        .mean_ns = dec_mean,
        .throughput_msg_s = safe_dec_rate,
    });

    print("  Encode 32B: {d:.1} ns/msg  ({d:.0} M/s)\n", .{ enc_ns_each, safe_enc_rate / 1e6 });
    print("  Decode 32B: {d:.1} ns/msg  ({d:.0} M/s)\n", .{ dec_ns_each, safe_dec_rate / 1e6 });
}

fn benchLogBuffer(comptime msg_size: u32) !void {
    const LB = zigbolt.LogBuffer(.{ .term_length = 1 << 16 });
    var buf = LB.init();
    var h = HdrHistogram{};
    const frame_hdr_size = @sizeOf(zigbolt.FrameHeader);
    const payload: [msg_size]u8 = @splat(0xEF);

    // Warmup
    for (0..10_000) |_| {
        if (buf.claim(msg_size)) |c| {
            @memcpy(c.term_buffer[c.term_offset + frame_hdr_size ..][0..msg_size], &payload);
            buf.commit(c, 1);
        }
        _ = buf.read(&logCountHandler, 1);
    }

    buf = LB.init();

    const samples: u64 = 50_000;
    for (0..samples) |_| {
        const t0 = zigbolt.timestampNs();
        for (0..BATCH) |_| {
            const c = buf.claim(msg_size) orelse {
                _ = buf.read(&logCountHandler, 4096);
                continue;
            };
            @memcpy(c.term_buffer[c.term_offset + frame_hdr_size ..][0..msg_size], &payload);
            buf.commit(c, 1);
            _ = buf.read(&logCountHandler, 1);
        }
        const t1 = zigbolt.timestampNs();
        h.record((t1 -| t0) / BATCH);
    }

    const mean_ns = h.mean();
    addResult(.{
        .name = "LogBuffer",
        .msg_size = msg_size,
        .iterations = samples * BATCH,
        .min_ns = h.min_value,
        .p50_ns = h.percentile(50),
        .p99_ns = h.percentile(99),
        .p999_ns = h.percentile(99.9),
        .max_ns = h.max_value,
        .mean_ns = mean_ns,
        .throughput_msg_s = if (mean_ns > 0) 1e9 / mean_ns else 0,
    });

    print("  LogBuf {d:>3}B:  p50={d:>5} ns  p99={d:>5} ns  ({d:.1} M/s)\n", .{
        msg_size,
        h.percentile(50),
        h.percentile(99),
        if (mean_ns > 0) 1e9 / mean_ns / 1e6 else 0,
    });
}

fn logCountHandler(_: []const u8, _: i32) void {}

fn noopHandler(_: zigbolt.IpcChannel.ReadResult) void {}

// ═════════════════════════════════════════════════════════
// Output
// ═════════════════════════════════════════════════════════

fn printSummary() void {
    print(
        \\
        \\╔═══════════════════════════════════════════════════════════════════════════════╗
        \\║                         Benchmark Summary                                    ║
        \\╠════════════════╦═══════╦═════════╦═════════╦═════════╦═════════╦══════════════╣
        \\║ Transport      ║  Size ║  p50    ║  p99    ║  p99.9  ║  Max    ║  Throughput  ║
        \\╠════════════════╬═══════╬═════════╬═════════╬═════════╬═════════╬══════════════╣
        \\
    , .{});

    for (results[0..result_count]) |r| {
        print("║ {s:<14} ║ {d:>3}B  ║ {d:>5} ns ║ {d:>5} ns ║ {d:>5} ns ║ {d:>5} ns ║ {d:>7.1} M/s  ║\n", .{
            r.name,
            r.msg_size,
            r.p50_ns,
            r.p99_ns,
            r.p999_ns,
            r.max_ns,
            r.throughput_msg_s / 1e6,
        });
    }

    print("╚════════════════╩═══════╩═════════╩═════════╩═════════╩═════════╩══════════════╝\n", .{});
}

fn writeJson() void {
    // Build JSON string in a stack buffer, then write atomically.
    var json_buf: [16384]u8 = undefined;
    var pos: usize = 0;

    pos += (std.fmt.bufPrint(json_buf[pos..], "[\n", .{}) catch return).len;

    for (results[0..result_count], 0..) |r, i| {
        const entry = std.fmt.bufPrint(json_buf[pos..],
            \\  {{
            \\    "transport": "{s}",
            \\    "msg_size_bytes": {d},
            \\    "iterations": {d},
            \\    "min_ns": {d},
            \\    "p50_ns": {d},
            \\    "p99_ns": {d},
            \\    "p999_ns": {d},
            \\    "max_ns": {d},
            \\    "mean_ns": {d:.1},
            \\    "throughput_msg_s": {d:.0}
            \\  }}
        , .{
            r.name,
            r.msg_size,
            r.iterations,
            r.min_ns,
            r.p50_ns,
            r.p99_ns,
            r.p999_ns,
            r.max_ns,
            r.mean_ns,
            r.throughput_msg_s,
        }) catch return;
        pos += entry.len;

        if (i < result_count - 1) {
            pos += (std.fmt.bufPrint(json_buf[pos..], ",\n", .{}) catch return).len;
        } else {
            pos += (std.fmt.bufPrint(json_buf[pos..], "\n", .{}) catch return).len;
        }
    }

    pos += (std.fmt.bufPrint(json_buf[pos..], "]\n", .{}) catch return).len;

    std.fs.cwd().writeFile(.{
        .sub_path = "bench/results.json",
        .data = json_buf[0..pos],
    }) catch {
        print("\n  (Could not write results.json)\n", .{});
        return;
    };
    print("\n  Results saved to bench/results.json\n", .{});
}
