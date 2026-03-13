const std = @import("std");
const zigbolt = @import("zigbolt");

const MSG_COUNT: u64 = 10_000_000;
const MSG_SIZE: u32 = 64;
const CHANNEL_NAME = "/zigbolt_bench_tp";
const TERM_LENGTH: usize = 4 << 20; // 4 MB

const print = std.debug.print;

pub fn main() !void {
    print(
        \\
        \\=== ZigBolt IPC Throughput Benchmark ===
        \\  Target: > 50M msg/sec
        \\  Messages: {d}
        \\  Msg size: {d} bytes
        \\
    , .{ MSG_COUNT, MSG_SIZE });

    var ch = zigbolt.IpcChannel.create(CHANNEL_NAME, .{
        .term_length = TERM_LENGTH,
        .pre_fault = true,
    }) catch |err| {
        print("Failed to create IPC channel: {}\n", .{err});
        return;
    };
    defer ch.deinit();

    const msg_data: [MSG_SIZE]u8 = @splat(0xAB);

    print("Publishing {d} messages...\n", .{MSG_COUNT});

    const start = zigbolt.timestampNs();
    var published: u64 = 0;

    while (published < MSG_COUNT) : (published += 1) {
        ch.publish(&msg_data, 1) catch {
            _ = ch.poll(&noopHandler, 1024);
            ch.publish(&msg_data, 1) catch continue;
        };

        if (published % 10_000 == 0) {
            _ = ch.poll(&noopHandler, 10_000);
        }
    }

    const elapsed_ns = zigbolt.timestampNs() - start;
    const elapsed_sec = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const msg_per_sec = @as(f64, @floatFromInt(published)) / elapsed_sec;
    const bytes_per_sec = msg_per_sec * @as(f64, @floatFromInt(MSG_SIZE));
    const mb_per_sec = bytes_per_sec / (1024.0 * 1024.0);

    print(
        \\
        \\=== Throughput Results ===
        \\  Published:  {d} msgs
        \\  Elapsed:    {d:.3} sec
        \\  Throughput: {d:.1} M/sec
        \\  Bandwidth:  {d:.1} MB/sec
        \\
    , .{ published, elapsed_sec, msg_per_sec / 1_000_000.0, mb_per_sec });

    if (msg_per_sec > 50_000_000) {
        print("  [PASS] > 50M msg/sec target met!\n", .{});
    } else {
        print("  [MISS] Below 50M msg/sec target ({d:.1}M)\n", .{msg_per_sec / 1_000_000.0});
    }
}

fn noopHandler(_: zigbolt.IpcChannel.ReadResult) void {}
