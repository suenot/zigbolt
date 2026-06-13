# ZigBolt — Ultra-Low Latency Messaging System for HFT

## Build & Test
- `zig build` — build all targets
- `zig build test` — run all unit tests (503; pass on macOS and Linux, Debug and ReleaseFast)
- `./scripts/coverage.sh` — line coverage via kcov (100% of measurable lines); `COVERAGE_MIN=100 ./scripts/coverage.sh` fails if it regresses; `python3 scripts/uncovered.py` lists uncovered lines. Lines with a `kcov-skip: <reason>` marker are excluded from measurement — use only for kcov attribution gaps or non-injectable OS-failure branches, always with the justification inline
- `zig build install-tests -Dcoverage` — build test binaries (without running) with the LLVM backend so kcov can parse the DWARF line table. REQUIRED for native x86_64 coverage: Zig 0.15.1's self-hosted x86_64 backend emits a DWARF5 file entry with content type `DW_LNCT_LLVM_source` (0x2001) that kcov silently skips, reporting 0/0. coverage.sh sets this automatically on Linux; macOS cross-compiles (`-Dtarget=aarch64-linux-gnu`, which already uses LLVM) and runs under kcov in a Docker container
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
