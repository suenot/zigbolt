# ZigBolt — Ultra-Low Latency Messaging System for HFT

## Build & Test
- `zig build` — build all targets
- `zig build test` — run all unit tests
- `zig build bench` — run the FULL benchmark suite (bench/run_all.zig; writes bench/results.json)
- `./zig-out/bin/bench_ping_pong` — IPC ping-pong (RTT) benchmark
- `./zig-out/bin/bench_throughput` — throughput benchmark
- `./zig-out/bin/bench_udp_rtt` — UDP RTT benchmark
- Zig version: 0.15.1

## Architecture
```
src/
├── platform/          # OS abstraction (mmap, config)
├── core/              # Lock-free data structures (SPSC, MPSC, LogBuffer, Frame)
├── codec/             # Comptime wire codec, SBE engine, FIX/SBE messages
├── protocol/          # Aeron-compatible wire protocol flyweights (flyweight.zig)
├── channel/           # IPC (shared memory), UDP, reliability, fragmentation, network
├── api/               # Publisher, Subscriber, Transport
├── ffi/               # C ABI exports for Rust/Python/C interop
├── archive/           # Segment-based recording and replay
├── cluster/           # Raft consensus (leader election, log replication)
└── sequencer/         # Total ordering for capital markets
bench/                 # Benchmarks with HDR histogram
```

## Key Rules
- Zig 0.15.1 API: use `std.Thread.sleep()` not `std.time.sleep()`, `std.debug.print()` for output
- `std.io.getStdOut()` does NOT exist in 0.15.1
- mmap PROT flags: use `posix.PROT.READ | posix.PROT.WRITE` (integer OR, not struct)
- mmap MAP flags: use struct syntax `.{ .TYPE = .SHARED }`
- shm_open: use `std.c.shm_open(name, @bitCast(std.c.O{ ... }), mode)`
- mmap returns slice — use `.ptr` for multi-pointer fields
- Cache line size: `std.atomic.cache_line` = 128 bytes (both aarch64 and x86_64)
- Atomics: `std.atomic.Value(T)` with `.acquire`/`.release` ordering
- All hot-path memory pre-allocated via mmap, zero allocations after init
