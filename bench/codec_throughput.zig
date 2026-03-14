const std = @import("std");
const zigbolt = @import("zigbolt");

const ITERATIONS: u64 = 10_000_000;
const BATCH_SIZE: u32 = 64;

const print = std.debug.print;

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt WireCodec Throughput Benchmark ===
        \\  Iterations:  {d}
        \\  Batch size:  {d}
        \\
    , .{ ITERATIONS, BATCH_SIZE });

    // ── TickMessage encode/decode ─────────────────────────
    try benchCodec(zigbolt.TickMessage, "TickMessage (32B)");

    // ── OrderMessage encode/decode ────────────────────────
    try benchCodec(zigbolt.OrderMessage, "OrderMessage (48B)");
}

fn benchCodec(comptime T: type, comptime label: []const u8) !void {
    const Codec = zigbolt.WireCodec(T);
    const msg_size = @sizeOf(T);

    var buf: [msg_size]u8 = undefined;

    // ── Single encode throughput ──────────────────────────
    var encode_sink: u8 = 0;
    const encode_start = zigbolt.timestampNs();
    for (0..ITERATIONS) |i| {
        var msg = std.mem.zeroes(T);
        msg.timestamp_ns = @intCast(i);
        msg.symbol_id = @intCast(i & 0xFFFF);
        Codec.encode(&msg, &buf);
        encode_sink +%= buf[0];
    }
    const encode_ns = zigbolt.timestampNs() - encode_start;
    std.mem.doNotOptimizeAway(encode_sink);

    // ── Single decode throughput ──────────────────────────
    var msg_for_dec = std.mem.zeroes(T);
    msg_for_dec.timestamp_ns = 42;
    Codec.encode(&msg_for_dec, &buf);
    const decode_start = zigbolt.timestampNs();
    var dummy: u64 = 0;
    for (0..ITERATIONS) |i| {
        buf[0] = @intCast(i & 0xFF);
        const decoded = Codec.decode(&buf);
        dummy +%= decoded.timestamp_ns;
    }
    const decode_ns = zigbolt.timestampNs() - decode_start;
    std.mem.doNotOptimizeAway(dummy);

    // ── Batch encode throughput ───────────────────────────
    var batch_msgs: [BATCH_SIZE]T = @splat(std.mem.zeroes(T));
    var batch_buf: [BATCH_SIZE * msg_size]u8 = undefined;

    const batch_iters = ITERATIONS / BATCH_SIZE;
    const batch_encode_start = zigbolt.timestampNs();
    for (0..batch_iters) |_| {
        _ = Codec.batchEncode(&batch_msgs, &batch_buf);
    }
    const batch_encode_ns = zigbolt.timestampNs() - batch_encode_start;

    // ── Batch decode throughput ───────────────────────────
    _ = Codec.batchEncode(&batch_msgs, &batch_buf);
    var decode_out: [BATCH_SIZE]T = undefined;

    const batch_decode_start = zigbolt.timestampNs();
    for (0..batch_iters) |_| {
        _ = Codec.batchDecode(&batch_buf, &decode_out);
    }
    const batch_decode_ns = zigbolt.timestampNs() - batch_decode_start;
    std.mem.doNotOptimizeAway(decode_out[0].timestamp_ns);

    // ── Results ──────────────────────────────────────────
    const encode_rate = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(encode_ns)) / 1e9);
    const decode_rate = @as(f64, @floatFromInt(ITERATIONS)) / (@as(f64, @floatFromInt(decode_ns)) / 1e9);
    const batch_enc_rate = @as(f64, @floatFromInt(batch_iters * BATCH_SIZE)) / (@as(f64, @floatFromInt(batch_encode_ns)) / 1e9);
    const batch_dec_rate = @as(f64, @floatFromInt(batch_iters * BATCH_SIZE)) / (@as(f64, @floatFromInt(batch_decode_ns)) / 1e9);
    const encode_ns_each = @as(f64, @floatFromInt(encode_ns)) / @as(f64, @floatFromInt(ITERATIONS));
    const decode_ns_each = @as(f64, @floatFromInt(decode_ns)) / @as(f64, @floatFromInt(ITERATIONS));

    print("\n  [{s}]\n", .{label});
    print("    Encode:       {d:.1} ns/msg  ({d:.0} M/sec)\n", .{ encode_ns_each, encode_rate / 1e6 });
    print("    Decode:       {d:.1} ns/msg  ({d:.0} M/sec)\n", .{ decode_ns_each, decode_rate / 1e6 });
    print("    Batch encode: {d:.0} M/sec\n", .{batch_enc_rate / 1e6});
    print("    Batch decode: {d:.0} M/sec\n", .{batch_dec_rate / 1e6});
    print("    Bandwidth:    {d:.0} MB/sec (encode)\n", .{encode_rate * @as(f64, @floatFromInt(msg_size)) / (1024 * 1024)});

    if (encode_ns_each < 10) {
        print("    [PASS] encode < 10 ns/msg\n", .{});
    } else {
        print("    [MISS] encode {d:.1} ns/msg (target: <10 ns)\n", .{encode_ns_each});
    }
}
