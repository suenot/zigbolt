# ZigBolt Benchmarks

This document describes the benchmark methodology, how to run each benchmark,
and the target performance numbers.

## Overview

ZigBolt ships three benchmarks covering the primary performance dimensions:

| Benchmark | Binary | What it Measures |
|-----------|--------|------------------|
| Ping-Pong | `bench_ping_pong` | IPC round-trip latency (RTT) |
| Throughput | `bench_throughput` | IPC single-direction message rate |
| UDP RTT | `bench_udp_rtt` | UDP loopback round-trip latency |

All benchmarks are compiled with `-OReleaseFast` for maximum optimization.

## Building

```bash
zig build bench
```

This compiles all three benchmarks and places them in `zig-out/bin/`.

To build individually:

```bash
zig build && ./zig-out/bin/bench_ping_pong
zig build && ./zig-out/bin/bench_throughput
zig build && ./zig-out/bin/bench_udp_rtt
```

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

## Performance Targets vs Expected Actuals

| Benchmark | Metric | Target | Expected (Apple M2) | Expected (Linux x86_64) |
|-----------|--------|--------|---------------------|------------------------|
| IPC Ping-Pong | p50 RTT | < 200 ns | ~50-150 ns | ~40-120 ns |
| IPC Ping-Pong | p99 RTT | < 1,000 ns | ~200-500 ns | ~150-400 ns |
| IPC Throughput | msg/sec | > 50M | ~60-80M | ~70-100M |
| IPC Throughput | bandwidth | > 3 GB/s | ~4-5 GB/s | ~5-6 GB/s |
| UDP RTT | p50 | < 5 us | ~2-4 us | ~1-3 us (io_uring) |

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
