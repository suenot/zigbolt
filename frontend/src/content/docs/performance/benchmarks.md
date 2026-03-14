---
title: Benchmarks
description: ZigBolt benchmark methodology, results, and performance targets
---

This document describes the benchmark methodology, how to run each benchmark,
and the target performance numbers.

## Overview

ZigBolt ships a comprehensive benchmark suite covering latency, throughput,
codec performance, and data structure operations:

| Benchmark | Binary | What it Measures |
|-----------|--------|------------------|
| Ping-Pong | `bench_ping_pong` | IPC round-trip latency (RTT) |
| Throughput | `bench_throughput` | IPC single-direction message rate |
| UDP RTT | `bench_udp_rtt` | UDP loopback round-trip latency |
| Codec Throughput | `bench_codec_throughput` | WireCodec encode/decode rate (single + batch) |
| SPSC Latency | `bench_spsc_latency` | SPSC ring buffer write/read latency |
| MPSC Latency | `bench_mpsc_latency` | MPSC ring buffer contention latency |
| LogBuffer Throughput | `bench_logbuffer_throughput` | LogBuffer claim/commit/read rate |
| IPC Multi-Size | `bench_ipc_multisize` | IPC latency across message sizes |
| Full Suite | `bench_run_all` | All-in-one suite with JSON output |

All benchmarks are compiled with `-OReleaseFast` for maximum optimization.

## Building

```bash
zig build bench
```

This compiles all benchmarks and places them in `zig-out/bin/`.

To build individually:

```bash
zig build && ./zig-out/bin/bench_ping_pong
zig build && ./zig-out/bin/bench_throughput
zig build && ./zig-out/bin/bench_udp_rtt
zig build && ./zig-out/bin/bench_codec_throughput
zig build && ./zig-out/bin/bench_spsc_latency
zig build && ./zig-out/bin/bench_mpsc_latency
zig build && ./zig-out/bin/bench_logbuffer_throughput
zig build && ./zig-out/bin/bench_ipc_multisize
zig build && ./zig-out/bin/bench_run_all
```

The `bench_run_all` binary runs all benchmarks in sequence and outputs a summary
table plus a `bench/results.json` file for CI integration.

## Methodology

### Ping-Pong (IPC RTT)

**What**: Measures the time between publishing a message into an IPC channel and
immediately polling it back in the same process. This captures the raw shared
memory write/read latency without cross-process scheduling overhead.

**Procedure**:
1. Create an IPC channel (`/zigbolt_bench_pp`) with 1 MB term length, pre-faulted pages
2. Warm up with 10,000 messages (discarded)
3. Recreate the channel to start clean
4. For each of 100,000 measurement iterations:
   - Record `send_time` via `timestampNs()`
   - Publish a 32-byte message containing the timestamp
   - Poll it back immediately
   - Record `recv_time`, compute `rtt = recv_time - send_time`
   - Add RTT to HDR histogram
5. Report percentiles: min, mean, p50, p90, p99, p99.9, p99.99, max

**Configuration**:
- Message size: 32 bytes
- Term length: 1 MB (1,048,576 bytes)
- Warmup: 10,000 messages
- Measurement: 100,000 messages
- Pre-fault: enabled

**Target**:
- p50 < 200 ns
- p99 < 1,000 ns

### Throughput (IPC)

**What**: Measures the maximum sustained message publish rate through an IPC
channel, with periodic polling to prevent buffer exhaustion.

**Procedure**:
1. Create an IPC channel (`/zigbolt_bench_tp`) with 4 MB term length
2. Record start timestamp
3. Publish 10,000,000 messages of 64 bytes each
   - On publish failure (buffer full): poll 1,024 messages, retry
   - Every 10,000 publishes: poll up to 10,000 messages
4. Record end timestamp
5. Compute: `msg/sec = count / elapsed`, `MB/sec = msg/sec * msg_size / 1MB`

**Configuration**:
- Message size: 64 bytes
- Term length: 4 MB
- Message count: 10,000,000
- Pre-fault: enabled

**Target**:
- \> 50 million messages/second

### UDP RTT (Loopback)

**What**: Measures UDP round-trip latency over the loopback interface. Sends a
datagram from one socket and receives it on another, both bound to localhost.

**Procedure**:
1. Create sender UDP channel (port 44445, non-blocking)
2. Create receiver UDP channel (port 44444, non-blocking)
3. Warm up with 5,000 messages (discarded)
4. Drain any remaining datagrams
5. For each of 50,000 measurement iterations:
   - Record `send_time`, embed in 32-byte message
   - Send via sender socket to receiver's port
   - Busy-poll receiver socket (up to 10,000 attempts)
   - Record `recv_time`, compute RTT
   - Add to HDR histogram
6. Report percentiles

**Configuration**:
- Message size: 32 bytes
- Ports: 44444 (receiver), 44445 (sender)
- Warmup: 5,000 messages
- Measurement: 50,000 messages
- Non-blocking: enabled

**Target**:
- p50 < 5 us (expected to be lower with io_uring on Linux)

### WireCodec Throughput

**What**: Measures the raw encode/decode throughput of the comptime WireCodec
for `TickMessage` (32B) and `OrderMessage` (48B), including both single-message
and batch (64-message) modes.

**Procedure**:
1. Warm up with 100,000 encode operations (discarded)
2. For 10,000,000 iterations:
   - Encode a message with varying fields (prevents constant-folding)
   - Accumulate a sink byte to prevent dead-code elimination
3. Repeat for decode with `doNotOptimizeAway` on the result
4. Repeat for batch encode/decode (64 messages per batch)
5. Report: ns/msg, M/sec, MB/sec bandwidth

**Configuration**:
- Message types: TickMessage (32B), OrderMessage (48B)
- Iterations: 10,000,000
- Batch size: 64 messages
- Anti-optimization: varying input fields + sink accumulator

**Target**:
- Encode: < 10 ns/msg (> 100M msg/sec)
- Decode: < 10 ns/msg (> 100M msg/sec)
- Batch encode: > 150M msg/sec

---

### SPSC Ring Buffer Latency

**What**: Measures the single-producer single-consumer ring buffer write/read
round-trip latency across multiple message sizes.

**Procedure**:
1. Initialize a 64K-entry SPSC ring buffer
2. Warm up with 10,000 write/read pairs (discarded)
3. For 100,000 measurement samples:
   - Batch 100 write/read pairs
   - Record per-operation average in HDR histogram
4. Report percentiles for each message size

**Configuration**:
- Ring capacity: 65,536 entries
- Message sizes: 8B, 32B, 64B, 256B
- Warmup: 10,000 ops
- Samples: 100,000 (x100 batch = 10M ops)

**Target**:
- p50 < 50 ns (8B-64B messages)
- p99 < 200 ns

---

### MPSC Ring Buffer Latency

**What**: Measures the multi-producer single-consumer ring buffer latency
under contention from multiple writer threads.

**Configuration**:
- Multiple producer threads writing concurrently
- Single consumer thread reading
- Measures contention overhead vs SPSC baseline

**Target**:
- p50 < 100 ns (under moderate contention)
- p99 < 500 ns

---

### LogBuffer Throughput

**What**: Measures the LogBuffer claim/commit/read cycle latency, which is the
foundation of the Aeron-style term buffer used by IPC channels.

**Procedure**:
1. Initialize a LogBuffer with 64K term length
2. Warm up with 10,000 claim/commit/read cycles
3. Reset the buffer
4. For 50,000 measurement samples:
   - Batch 100 claim/commit/read cycles
   - On claim failure: drain 4,096 messages and retry
   - Record per-operation average in HDR histogram
5. Report percentiles for each message size

**Configuration**:
- Term length: 65,536 bytes
- Message sizes: 32B, 64B, 256B
- Warmup: 10,000 ops
- Samples: 50,000 (x100 batch = 5M ops)

**Target**:
- p50 < 100 ns
- p99 < 500 ns

---

### IPC Multi-Size

**What**: Measures IPC channel latency across different message sizes to
characterize how payload size affects publish/poll performance.

**Configuration**:
- Message sizes: 64B, 256B, 1024B
- Term length: 4 MB
- Pre-fault: enabled

**Target**:
- 64B: p50 < 200 ns
- 1024B: p50 < 500 ns

---

### Broadcast Buffer Throughput

**What**: Measures the 1-to-N broadcast buffer transmit/receive throughput.
The broadcast buffer uses lossy semantics with lapping detection, making it
suitable for market data distribution where latest-value-wins.

**Configuration**:
- Buffer size: 1 MB
- Message sizes: 32B, 64B
- Single transmitter, multiple receivers
- Measures both transmit rate and receive-with-lapping rate

**Target**:
- Transmit: > 30M msg/sec (64B messages)
- Receive: > 25M msg/sec per receiver

---

### SBE Encode/Decode Throughput

**What**: Measures the SBE (Simple Binary Encoding) codec encode/decode
throughput for FIX trading messages. SBE provides zero-allocation encoding
with schema-driven field layout, suitable for FIX Trading Community wire format.

**Configuration**:
- Message types: NewOrderSingle, ExecutionReport, MarketDataIncrementalRefresh
- Encoding: schema-driven with MessageHeader (8B) + root block + repeating groups
- Iterations: 1,000,000 per message type
- Measures: encode ns/msg, decode ns/msg, round-trip validation

**Target**:
- NewOrderSingle encode: < 50 ns/msg
- ExecutionReport decode: < 50 ns/msg
- Full round-trip (encode + decode): < 100 ns/msg

---

### Compression Throughput

**What**: Measures the LZ4-style compression/decompression throughput for
archive segment data. Compression is used by the archive subsystem to reduce
storage requirements for recorded message streams.

**Configuration**:
- Input sizes: 1KB, 4KB, 16KB blocks
- Data patterns: market data (partially compressible), random (incompressible)
- Framed API with CRC32 validation

**Target**:
- Compression: > 500 MB/sec
- Decompression: > 1 GB/sec
- Compression ratio: > 2x for structured market data

---

## Results Format

All latency benchmarks output HDR histogram percentiles:

```
=== Results ===
  Total samples: 100000
  Min:     45 ns
  Mean:    132.7 ns
  p50:     120 ns
  p90:     180 ns
  p99:     450 ns
  p99.9:   1200 ns
  p99.99:  3500 ns
  Max:     15000 ns

  [PASS] p50 = 120 ns (target: <200 ns)
  [PASS] p99 = 450 ns (target: <1000 ns)
```

Throughput benchmark output:

```
=== Throughput Results ===
  Published:  10000000 msgs
  Elapsed:    0.150 sec
  Throughput: 66.7 M/sec
  Bandwidth:  4053.3 MB/sec

  [PASS] > 50M msg/sec target met!
```

WireCodec benchmark output:

```
=== ZigBolt WireCodec Throughput Benchmark ===
  Iterations:  10000000
  Batch size:  64

  [TickMessage (32B)]
    Encode:       3.2 ns/msg  (312 M/sec)
    Decode:       2.8 ns/msg  (357 M/sec)
    Batch encode: 450 M/sec
    Batch decode: 420 M/sec
    Bandwidth:    9536 MB/sec (encode)
    [PASS] encode < 10 ns/msg
```

Full suite (`bench_run_all`) summary output:

```
╔═══════════════════════════════════════════════════════════════════════════════╗
║                         Benchmark Summary                                    ║
╠════════════════╦═══════╦═════════╦═════════╦═════════╦═════════╦══════════════╣
║ Transport      ║  Size ║  p50    ║  p99    ║  p99.9  ║  Max    ║  Throughput  ║
╠════════════════╬═══════╬═════════╬═════════╬═════════╬═════════╬══════════════╣
║ SPSC           ║   8B  ║    12 ns ║    45 ns ║   120 ns ║   500 ns ║    83.3 M/s  ║
║ SPSC           ║  32B  ║    15 ns ║    50 ns ║   150 ns ║   600 ns ║    66.7 M/s  ║
║ IPC            ║  64B  ║   120 ns ║   350 ns ║   900 ns ║  3000 ns ║     8.3 M/s  ║
║ Codec-Enc      ║  32B  ║     3 ns ║     0 ns ║     0 ns ║     0 ns ║   333.3 M/s  ║
║ Codec-Dec      ║  32B  ║     2 ns ║     0 ns ║     0 ns ║     0 ns ║   500.0 M/s  ║
║ LogBuffer      ║  64B  ║    35 ns ║   120 ns ║   300 ns ║  1500 ns ║    28.6 M/s  ║
╚════════════════╩═══════╩═════════╩═════════╩═════════╩═════════╩══════════════╝
```

The full suite also writes `bench/results.json` with structured data for CI
integration and automated regression detection.

## Performance Targets vs Expected Actuals

| Benchmark | Metric | Target | Expected (Apple M2) | Expected (Linux x86_64) |
|-----------|--------|--------|---------------------|------------------------|
| IPC Ping-Pong | p50 RTT | < 200 ns | ~50-150 ns | ~40-120 ns |
| IPC Ping-Pong | p99 RTT | < 1,000 ns | ~200-500 ns | ~150-400 ns |
| IPC Throughput | msg/sec | > 50M | ~60-80M | ~70-100M |
| IPC Throughput | bandwidth | > 3 GB/s | ~4-5 GB/s | ~5-6 GB/s |
| UDP RTT | p50 | < 5 us | ~2-4 us | ~1-3 us (io_uring) |
| WireCodec Encode | ns/msg | < 10 ns | ~2-5 ns | ~1-4 ns |
| WireCodec Decode | ns/msg | < 10 ns | ~2-5 ns | ~1-4 ns |
| WireCodec Batch | msg/sec | > 150M | ~200-400M | ~300-500M |
| SPSC Ring | p50 | < 50 ns | ~10-30 ns | ~8-25 ns |
| SPSC Ring | p99 | < 200 ns | ~50-150 ns | ~40-100 ns |
| MPSC Ring | p50 | < 100 ns | ~30-80 ns | ~20-60 ns |
| LogBuffer | p50 | < 100 ns | ~30-80 ns | ~25-60 ns |
| Broadcast Tx | msg/sec | > 30M | ~40-60M | ~50-80M |
| SBE Encode | ns/msg | < 50 ns | ~15-30 ns | ~10-25 ns |
| SBE Decode | ns/msg | < 50 ns | ~15-30 ns | ~10-25 ns |
| LZ4 Compress | bandwidth | > 500 MB/s | ~600-900 MB/s | ~800-1200 MB/s |
| LZ4 Decompress | bandwidth | > 1 GB/s | ~1.5-2.5 GB/s | ~2-4 GB/s |

Performance varies by:
- CPU architecture and cache hierarchy
- OS kernel version and scheduler configuration
- NUMA topology (for multi-socket systems)
- Core isolation (`isolcpus`, `nohz_full`) on Linux
- Background system load

## Tuning for Best Results

### Linux

```bash
# Isolate CPU cores for benchmarks
sudo grubby --update-kernel=ALL --args="isolcpus=2,3 nohz_full=2,3"

# Pin benchmark to isolated core
taskset -c 2 ./zig-out/bin/bench_ping_pong

# Disable frequency scaling
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor

# Increase socket buffer sizes
sudo sysctl -w net.core.rmem_max=16777216
sudo sysctl -w net.core.wmem_max=16777216
```

### macOS

```bash
# Ensure Xcode command-line tools are installed
xcode-select --install

# Disable Spotlight indexing on benchmark paths
sudo mdutil -i off /tmp

# Close unnecessary applications to reduce noise
```

## HDR Histogram

The benchmarks use a custom lightweight HDR (High Dynamic Range) histogram
implementation in `bench/hdr_histogram.zig`. It provides:

- Constant memory footprint (bucket array)
- O(1) recording
- Accurate percentile computation
- No allocations during measurement

This avoids measurement perturbation that would occur with a heap-allocating
histogram.
