# ZigBolt -- Ultra-Low Latency Messaging for HFT

[![Zig 0.15.1](https://img.shields.io/badge/Zig-0.15.1-orange)](https://ziglang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

ZigBolt is a pure-Zig, zero-allocation, lock-free messaging library for
high-frequency trading systems. Zero GC pauses, zero JVM safepoints, zero
runtime overhead.

## Features

- **Sub-100ns IPC latency** -- shared memory channels with cache-line-padded atomics
- **Zero-copy** -- messages decoded in-place via pointer cast, no serialization overhead
- **Comptime wire codecs** -- `WireCodec(T)` validates packed structs at compile time
- **SBE codec** -- FIX-standard Simple Binary Encoding with groups, vardata, and comptime schemas
- **FIX/SBE messages** -- NewOrderSingle, ExecutionReport, MarketDataIncrementalRefresh, MassQuote, Heartbeat, Logon
- **Wire protocol flyweights** -- Aeron-compatible DataHeader, StatusMessage, NAK, Setup, RTT, Error frames
- **Lock-free ring buffers** -- SPSC (acquire/release atomics) and MPSC (CAS two-phase commit)
- **Broadcast buffer** -- 1-to-N fan-out for market data (one producer, many consumers, lossy)
- **NAK-based reliability** -- receiver-driven retransmission with gap detection bitmap
- **AIMD congestion control** -- TCP-like slow start / congestion avoidance with RTT estimation
- **Flow control strategies** -- Min (reliable multicast), Max (best-effort), Tagged (group-based)
- **Fragmentation/reassembly** -- transparent large-message support over UDP
- **Idle strategies** -- BusySpin, Yielding, Sleeping, Backoff for adaptive CPU/latency trade-offs
- **Agent pattern** -- composable threaded agents with lifecycle management and duty cycle tracking
- **Shared counters** -- atomic counter system for monitoring all subsystems (IPC, network, cluster, etc.)
- **Raft consensus cluster** -- leader election, log replication, state machine application
- **Write-ahead log** -- CRC32-validated persistent WAL with crash recovery and truncation
- **Raft snapshots** -- point-in-time state snapshots with CRC validation and cleanup
- **Total-order sequencer** -- monotonic sequence assignment across multiple input streams
- **Message archive** -- segment-based record/replay with stream filtering
- **Archive catalog & index** -- segment metadata catalog with time/stream queries and sparse index
- **LZ4-style compression** -- hash-table-based compression for archive segments
- **C/Rust/Python FFI** -- shared library with C-ABI exports

## Architecture

```
 Producers                            Consumers
 +--------+                          +--------+
 | App    |                          | App    |
 +---+----+                          +---+----+
     |                                   ^
     v                                   |
 +---+----+     +-------------+     +----+---+
 |Publisher| --> |  Transport  | --> |Subscriber|
 +---+----+     +------+------+     +----+---+
     |                 |                 ^
     v                 v                 |
 +---+----+     +------+------+     +----+---+
 |WireCodec|    | IPC Channel |     |WireCodec|
 |(encode) |    | (shm/mmap)  |     |(decode) |
 +---+----+     +------+------+     +----+---+
     |                 |                 ^
     |                 v                 |
     |          +------+------+          |
     |          | LogBuffer / |          |
     |          | SPSC / MPSC |          |
     |          +------+------+          |
     |                 |                 |
     |    +------------+------------+    |
     |    |            |            |    |
     v    v            v            v    |
 +---+----+     +------+------+  +--+---+--+
 |   UDP   |    |  Reliability |  | Archive |
 | Channel |    |  (NAK/Flow)  |  | (Replay)|
 +---+----+     +------+------+  +---------+
     |                 |
     v                 v
 +---+----+     +------+------+
 | Network |    |   Cluster   |
 | Channel |    | (Raft/SM)   |
 +---------+    +------+------+
                       |
                +------+------+
                |  Sequencer  |
                | (Total Ord) |
                +-------------+
```

## Quick Start

### Requirements

- **Zig 0.15.1+** (minimum 0.15.0, per `build.zig.zon`)
- **macOS** (ARM64 / x86_64) or **Linux** (x86_64, aarch64)

### Build

```bash
zig build
```

### Run Tests

```bash
zig build test
```

Runs the full unit test suite (423 tests, including the FFI surface).

### Run Benchmarks

```bash
# All benchmarks
zig build bench

# Individual benchmarks
zig build && ./zig-out/bin/bench_ping_pong
zig build && ./zig-out/bin/bench_throughput
zig build && ./zig-out/bin/bench_udp_rtt
```

## Module Overview

| Module | Path | Description |
|--------|------|-------------|
| **SpscRingBuffer** | `src/core/spsc.zig` | Lock-free single-producer single-consumer ring buffer |
| **MpscRingBuffer** | `src/core/mpsc.zig` | Lock-free multi-producer single-consumer ring buffer (CAS) |
| **LogBuffer** | `src/core/log_buffer.zig` | Triple-buffered log with term rotation (Aeron-style) |
| **FrameHeader** | `src/core/frame.zig` | 8-byte frame header (length + type ID) |
| **WireCodec** | `src/codec/wire.zig` | Comptime zero-copy codec for packed structs |
| **IpcChannel** | `src/channel/ipc.zig` | Shared-memory IPC (create/open/publish/poll) |
| **UdpChannel** | `src/channel/udp.zig` | UDP unicast/multicast with framed send/recv |
| **NetworkChannel** | `src/channel/network.zig` | Reliable ordered UDP (NAK + flow control + fragmentation) |
| **Publisher** | `src/api/publisher.zig` | Typed pub API via WireCodec over IpcChannel |
| **Subscriber** | `src/api/subscriber.zig` | Typed sub API via WireCodec over IpcChannel |
| **Transport** | `src/api/transport.zig` | Channel manager, factory for publishers/subscribers |
| **SbeEncoder/Decoder** | `src/codec/sbe.zig` | SBE wire format engine with comptime schema definitions |
| **FIX Messages** | `src/codec/fix_messages.zig` | FIX/SBE market data messages (NewOrderSingle, ExecutionReport, etc.) |
| **DataHeaderFlyweight** | `src/protocol/flyweight.zig` | Aeron-compatible wire protocol flyweights (Data, SM, NAK, Setup, RTT, Error) |
| **BroadcastBuffer** | `src/core/broadcast.zig` | 1-to-N broadcast (BroadcastTransmitter, BroadcastReceiver, CopyBroadcastReceiver) |
| **IdleStrategy** | `src/core/idle_strategy.zig` | Idle strategies (BusySpin, Yielding, Sleeping, Backoff, NoOp) |
| **AgentRunner** | `src/core/agent.zig` | Agent pattern (AgentFn, AgentRunner, CompositeAgent, DutyCycleTracker) |
| **CounterSet** | `src/core/counters.zig` | Shared atomic counter system (Counter, CounterSet, GlobalCounters) |
| **CongestionControl** | `src/channel/congestion.zig` | AIMD congestion control with RTT estimation and NAK controller |
| **FlowControl** | `src/channel/flow_control.zig` | Flow control strategies (Min, Max, Tagged) with receiver tracking |
| **Archive** | `src/archive/archive.zig` | Segment-based message recording and replay |
| **Catalog** | `src/archive/catalog.zig` | Archive catalog with time/stream queries and disk persistence |
| **SparseIndex** | `src/archive/index.zig` | Sparse index for fast record lookup within segments |
| **Compressor** | `src/archive/compression.zig` | LZ4-style compression/decompression with framed format |
| **Sequencer** | `src/sequencer/sequencer.zig` | Atomic total-order sequence assignment |
| **RaftNode** | `src/cluster/raft.zig` | Raft consensus: election, replication, commit |
| **Cluster** | `src/cluster/cluster.zig` | High-level Raft cluster with state machine |
| **WriteAheadLog** | `src/cluster/wal.zig` | CRC32-validated WAL with crash recovery and truncation |
| **SnapshotManager** | `src/cluster/snapshot.zig` | Raft snapshots with CRC validation and old snapshot cleanup |
| **FFI** | `src/ffi/exports.zig` | C-ABI exports for cross-language integration |

## Code Examples

### IPC Publisher / Subscriber

```zig
const zigbolt = @import("zigbolt");

// Define a market data message (packed struct, comptime-validated)
const TickMessage = zigbolt.TickMessage;

// --- Publisher side ---
var pub_ch = try zigbolt.IpcChannel.create("/market-data", .{
    .term_length = 1 << 20,  // 1 MB
    .pre_fault = true,
});
defer pub_ch.deinit();

var publisher = zigbolt.Publisher(TickMessage).init(&pub_ch, 1);

const tick = TickMessage{
    .timestamp_ns = zigbolt.timestampNs(),
    .symbol_id = 42,
    .price = 15025_00,
    .volume = 100,
    .side = .bid,
};
try publisher.offer(&tick);

// --- Subscriber side ---
var sub_ch = try zigbolt.IpcChannel.open("/market-data", .{
    .term_length = 1 << 20,
});
defer sub_ch.deinit();

var subscriber = zigbolt.Subscriber(TickMessage).init(&sub_ch, 1);
_ = subscriber.poll(&handleTick, 100);

// The decoded pointer aliases the shared-memory frame (zero-copy),
// so the handler takes `*align(1) const T`.
fn handleTick(msg: *align(1) const TickMessage) void {
    // Zero-copy: msg points directly into shared memory
    _ = msg.price;
}
```

### UDP Channel

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

// Create a UDP channel bound to a local port
var ch = try zigbolt.UdpChannel.init(.{
    .bind_address = try std.net.Address.parseIp4("0.0.0.0", 9000),
    .remote_address = try std.net.Address.parseIp4("224.1.1.1", 9000),
    .multicast_group = .{ 224, 1, 1, 1 },
    .non_blocking = true,
});
defer ch.deinit();

// Send a framed message (FrameHeader + payload in one datagram)
try ch.sendFrame("hello", 42, null);

// Receive a framed message
var buf: [65536]u8 = undefined;
if (try ch.recvFrame(&buf)) |result| {
    // result.payload, result.msg_type_id, result.from
}
```

### Wire Codec

```zig
const zigbolt = @import("zigbolt");

const OrderMessage = zigbolt.OrderMessage;
const Codec = zigbolt.WireCodec(OrderMessage);

// Encode
const order = OrderMessage{
    .timestamp_ns = zigbolt.timestampNs(),
    .order_id = 12345,
    .symbol_id = 7,
    .price = 50000_00,
    .quantity = 250,
    .side = .buy,
    .order_type = .limit,
};
var buf: [Codec.wire_size]u8 = undefined;
Codec.encode(&order, &buf);

// Decode (zero-copy -- pointer into buf)
const decoded = Codec.decode(&buf);
_ = decoded.price;  // 50000_00

// Batch operations
var orders: [64]OrderMessage = undefined;
const count = Codec.batchDecode(&large_buf, &orders);
```

## Performance

### Benchmark Targets

| Benchmark | Metric | Target | Configuration |
|-----------|--------|--------|---------------|
| IPC Ping-Pong | p50 RTT | < 200 ns | 32-byte messages, 1 MB term |
| IPC Ping-Pong | p99 RTT | < 1,000 ns | 32-byte messages, 1 MB term |
| IPC Throughput | Messages/sec | > 50M msg/sec | 64-byte messages, 4 MB term |
| UDP RTT | p50 RTT | < 5 us | 32-byte messages, loopback |

### Running Benchmarks

```bash
# IPC round-trip latency (100K samples after 10K warmup)
./zig-out/bin/bench_ping_pong

# IPC throughput (10M messages)
./zig-out/bin/bench_throughput

# UDP loopback RTT (50K samples after 5K warmup)
./zig-out/bin/bench_udp_rtt
```

Results are printed with HDR histogram percentiles (p50, p90, p99, p99.9, p99.99, min, max).

## Comparison

| Feature | ZigBolt | Aeron | Chronicle Queue | ZeroMQ | LMAX Disruptor |
|---------|---------|-------|-----------------|--------|----------------|
| Language | Zig | Java/C++ | Java | C | Java |
| IPC Latency (p50) | < 200 ns | ~200 ns | ~1 us | ~10 us | ~100 ns |
| SBE Codec | Yes (native) | SBE (XML codegen) | Chronicle Wire | No | N/A |
| Zero-Copy Decode | Yes (comptime) | SBE codegen | Chronicle Wire | No | N/A |
| GC Pauses | None | JVM GC | JVM GC | None | JVM GC |
| Lock-Free Buffers | SPSC/MPSC | SPSC/MPMC | Appender | Lock-based | Ring buffer |
| Reliability | NAK-based | NAK-based | Replication | REQ/REP | N/A |
| Cluster Consensus | Raft | Raft (Aeron Cluster) | Enterprise Repl. | None | None |
| Wire Codec | WireCodec + SBE | SBE (XML codegen) | Chronicle Wire | Protobuf/etc | Custom |
| Flow Control | Min/Max/Tagged | Min/Max/Tagged | N/A | HWM | N/A |
| Broadcast Buffer | Yes (1-to-N) | Yes (1-to-N) | No | PUB/SUB | No |
| Archive/Replay | Segment-based | Archive | Chronicle Queue | None | None |
| Binary Size | ~100 KB | ~20 MB (JVM) | ~50 MB (JVM) | ~1 MB | ~10 MB (JVM) |
| Build Dependency | Zig compiler | JVM + Gradle | JVM + Maven | CMake | JVM + Gradle |

## FFI (C / Rust / Python)

ZigBolt exports a C-ABI shared library for cross-language integration.

### C Example

```c
#include <stdint.h>

// Opaque handles
void* zigbolt_transport_create(uint32_t term_length, uint8_t use_hugepages, uint8_t pre_fault);
void  zigbolt_transport_destroy(void* handle);

void* zigbolt_ipc_create(const char* name, uint32_t term_length);
void* zigbolt_ipc_open(const char* name, uint32_t term_length);
void  zigbolt_ipc_destroy(void* handle);

int32_t zigbolt_publish(void* handle, const uint8_t* data, uint32_t len, int32_t msg_type_id);

// Usage:
void* ch = zigbolt_ipc_create("/my-channel", 1 << 20);
uint8_t msg[] = {0x01, 0x02, 0x03};
zigbolt_publish(ch, msg, sizeof(msg), 42);
zigbolt_ipc_destroy(ch);
```

### Rust Example

```rust
extern "C" {
    fn zigbolt_ipc_create(name: *const i8, term_length: u32) -> *mut std::ffi::c_void;
    fn zigbolt_publish(handle: *mut std::ffi::c_void, data: *const u8, len: u32, msg_type_id: i32) -> i32;
    fn zigbolt_ipc_destroy(handle: *mut std::ffi::c_void);
}

fn main() {
    unsafe {
        let name = std::ffi::CString::new("/my-channel").unwrap();
        let ch = zigbolt_ipc_create(name.as_ptr(), 1 << 20);
        let data = [1u8, 2, 3];
        zigbolt_publish(ch, data.as_ptr(), data.len() as u32, 42);
        zigbolt_ipc_destroy(ch);
    }
}
```

### Python Example (ctypes)

```python
import ctypes

lib = ctypes.CDLL("./zig-out/lib/libzigbolt.so")  # or .dylib on macOS

lib.zigbolt_ipc_create.restype = ctypes.c_void_p
lib.zigbolt_publish.restype = ctypes.c_int32

ch = lib.zigbolt_ipc_create(b"/my-channel", 1 << 20)
data = (ctypes.c_uint8 * 3)(1, 2, 3)
lib.zigbolt_publish(ch, data, 3, 42)
lib.zigbolt_ipc_destroy(ch)
```

### FFI Functions Reference

| Function | Description |
|----------|-------------|
| `zigbolt_transport_create` | Create a Transport instance |
| `zigbolt_transport_destroy` | Destroy a Transport instance |
| `zigbolt_ipc_create` | Create an IPC channel (publisher side) |
| `zigbolt_ipc_open` | Open an existing IPC channel (subscriber side) |
| `zigbolt_ipc_destroy` | Close and destroy an IPC channel |
| `zigbolt_publish` | Publish a message (returns 0 on success) |
| `zigbolt_poll` | Poll for messages with a callback |
| `zigbolt_version_major/minor/patch` | Library version (0.2.1) |

## Project Structure

```
zigbolt/
  build.zig              # Build system
  src/
    root.zig             # Public API surface (all exports)
    platform/
      config.zig         # Platform constants, timestamps, alignment
      memory.zig         # Shared memory (mmap/shm_open)
    core/
      frame.zig          # FrameHeader (8 bytes), alignment helpers
      spsc.zig           # SPSC ring buffer
      mpsc.zig           # MPSC ring buffer (CAS)
      log_buffer.zig     # Triple-buffered log (term rotation)
      broadcast.zig      # 1-to-N broadcast buffer (transmitter/receiver)
      idle_strategy.zig  # Idle strategies (BusySpin, Yielding, Sleeping, Backoff)
      agent.zig          # Agent pattern (AgentRunner, CompositeAgent, DutyCycleTracker)
      counters.zig       # Shared atomic counter system (Counter, CounterSet, GlobalCounters)
    codec/
      wire.zig           # Comptime wire codec + TickMessage/OrderMessage
      sbe.zig            # SBE wire format engine (MessageHeader, GroupHeader, Encoder, Decoder)
      fix_messages.zig   # FIX/SBE market data messages (NewOrderSingle, ExecutionReport, etc.)
    protocol/
      flyweight.zig      # Aeron-compatible wire protocol flyweights (Data, SM, NAK, Setup, RTT, Error)
    channel/
      ipc.zig            # Shared-memory IPC channel
      udp.zig            # UDP unicast/multicast channel
      network.zig        # Reliable network channel
      reliability.zig    # NAK protocol, SendBuffer, RecvTracker, FlowControl
      fragment.zig       # Fragmentation / reassembly
      congestion.zig     # AIMD congestion control with RTT estimation
      flow_control.zig   # Flow control strategies (Min, Max, Tagged)
    api/
      publisher.zig      # Typed Publisher(T) and RawPublisher
      subscriber.zig     # Typed Subscriber(T) and RawSubscriber
      transport.zig      # Transport (channel manager)
    archive/
      segment.zig        # Segment file management
      archive.zig        # Record/replay engine
      catalog.zig        # Archive catalog (segment metadata, time/stream queries)
      index.zig          # Sparse index for fast record lookup
      compression.zig    # LZ4-style compression/decompression
    sequencer/
      sequencer.zig      # Sequencer, MultiStreamSequencer, SequenceIndex
    cluster/
      raft.zig           # Raft consensus node
      raft_log.zig       # Raft replicated log
      cluster.zig        # Cluster with state machine
      wal.zig            # Write-Ahead Log (CRC32-validated, crash recovery)
      snapshot.zig       # Raft snapshots (point-in-time state capture)
    ffi/
      exports.zig        # C-ABI function exports
  bench/
    ping_pong.zig             # IPC RTT benchmark
    throughput.zig            # IPC throughput benchmark
    udp_rtt.zig               # UDP RTT benchmark
    spsc_latency.zig          # SPSC ring buffer latency benchmark
    mpsc_latency.zig          # MPSC ring buffer latency benchmark
    codec_throughput.zig      # WireCodec encode/decode benchmark
    ipc_multisize.zig         # IPC latency across message sizes
    logbuffer_throughput.zig  # LogBuffer claim/commit/read benchmark
    run_all.zig               # Full suite runner (writes bench/results.json)
    hdr_histogram.zig         # HDR histogram for latency measurement
  frontend/              # Astro Starlight documentation site
    src/content/docs/    # Docs source (getting-started, architecture,
                         #   reference, examples, performance, changelog)
```

## License

MIT License. See [LICENSE](LICENSE) for details.

## Links

- [Reforms.AI](https://reforms.ai) -- AI-driven market reform research
- [Marketmaker.cc](https://marketmaker.cc) -- Market making tools and strategies
- [StockAPIs.com](https://stockapis.com) -- Real-time market data APIs
