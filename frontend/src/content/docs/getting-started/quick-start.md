---
title: Quick Start
description: Get up and running with ZigBolt in 5 minutes
---

## Prerequisites

- [Zig 0.15.1](https://ziglang.org/download/) or later

## Build

```bash
git clone https://github.com/suenot/zigbolt.git
cd zigbolt
zig build
```

## Run Tests

```bash
zig build test
```

All 199 tests should pass.

## Run Benchmarks

```bash
zig build bench
./zig-out/bin/bench_run_all
```

## Hello World: IPC Publisher / Subscriber

### Publisher

```zig
const zigbolt = @import("zigbolt");

pub fn main() !void {
    var channel = try zigbolt.IpcChannel.create("/market-feed", .{
        .term_length = 1 << 20,  // 1 MB term buffers
        .pre_fault = true,
    });
    defer channel.deinit();

    var publisher = zigbolt.Publisher(zigbolt.TickMessage).init(&channel, 1);

    const tick = zigbolt.TickMessage{
        .timestamp_ns = zigbolt.timestampNs(),
        .symbol_id = 42,
        .price = 15025_00,
        .volume = 100,
        .side = .bid,
    };

    try publisher.offer(&tick);
}
```

### Subscriber

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

var tick_count: u64 = 0;

fn handleTick(msg: *const zigbolt.TickMessage) void {
    tick_count += 1;
    if (tick_count % 100_000 == 0) {
        const latency = zigbolt.timestampNs() - msg.timestamp_ns;
        std.debug.print("Tick #{d}: latency={d}ns\n", .{ tick_count, latency });
    }
}

pub fn main() !void {
    var channel = try zigbolt.IpcChannel.open("/market-feed", .{
        .term_length = 1 << 20,
    });
    defer channel.deinit();

    var subscriber = zigbolt.Subscriber(zigbolt.TickMessage).init(&channel, 1);

    while (true) {
        const count = subscriber.poll(&handleTick, 256);
        if (count == 0) std.atomic.spinLoopHint();
    }
}
```

## Next Steps

- Read the full [API Reference](/reference/api-reference/) for all modules
- Check out more [Usage Examples](/examples/usage/)
- Review the [Architecture](/architecture/overview/) for design details
- Run [Benchmarks](/performance/benchmarks/) on your hardware
