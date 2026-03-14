---
title: Introduction
description: What is ZigBolt and why use it for HFT messaging
---

# ZigBolt

ZigBolt is an ultra-low-latency, lock-free messaging system written in pure Zig for high-frequency trading systems. Zero GC pauses, zero JVM safepoints, zero runtime overhead.

## Why ZigBolt?

Traditional HFT messaging systems are either:

- **Java-based** (Aeron, LMAX Disruptor, Chronicle Queue) — subject to GC pauses, JVM safepoints, and large runtime overhead
- **C/C++-based** (ZeroMQ, nanomsg) — complex build systems, memory safety issues, undefined behavior risks

ZigBolt combines the best of both worlds:

- **Zero overhead** like C — no GC, no runtime, direct hardware access
- **Safety** — comptime validation, no undefined behavior, clear error handling
- **Simplicity** — single file per module, no build system complexity, cross-compilation built in
- **Performance** — sub-200ns IPC, 100M+ msg/sec codec throughput

## Architecture

ZigBolt is organized into seven layers, each depending only on layers below it:

```
+================================================================+
|                     Application Layer                          |
|  Transport, Publisher(T), Subscriber(T), AgentRunner           |
+================================================================+
|                     Channel Layer                              |
|  IpcChannel (shm)    UdpChannel    NetworkChannel (reliable)   |
|  CongestionControl   FlowControl(Min/Max/Tagged)               |
+================================================================+
|                     Protocol Layer                             |
|  Reliability (NAK)   Fragmenter/Reassembler   NakController    |
|  DataHeaderFlyweight  StatusMessageFlyweight   NakFlyweight     |
+================================================================+
|                     Codec Layer                                |
|  WireCodec(T)   SbeEncoder/SbeDecoder   FIX Messages           |
+================================================================+
|                     Core Layer                                 |
|  SpscRingBuffer   MpscRingBuffer   LogBuffer   Sequencer       |
|  BroadcastBuffer (1-to-N)   CounterSet   GlobalCounters        |
+================================================================+
|                     Cluster / Archive Layer                    |
|  RaftNode   Cluster   WriteAheadLog   SnapshotManager          |
|  Archive   Catalog   SparseIndex   Compressor/Decompressor     |
+================================================================+
|                     Platform Layer                             |
|  config.zig (cache lines, timestamps)   memory.zig (shm/mmap) |
+================================================================+
```

## Module Overview

| Module | File | Description |
|--------|------|-------------|
| **SpscRingBuffer** | `src/core/spsc.zig` | Lock-free single-producer single-consumer ring buffer |
| **MpscRingBuffer** | `src/core/mpsc.zig` | Multi-producer single-consumer with CAS |
| **LogBuffer** | `src/core/log_buffer.zig` | Aeron-style term rotation buffer |
| **WireCodec** | `src/codec/wire.zig` | Comptime zero-copy codec for packed structs |
| **SbeEncoder/Decoder** | `src/codec/sbe.zig` | SBE wire format engine |
| **FIX Messages** | `src/codec/fix_messages.zig` | FIX/SBE market data messages |
| **IpcChannel** | `src/channel/ipc.zig` | Shared-memory IPC channel |
| **UdpChannel** | `src/channel/udp.zig` | UDP unicast/multicast |
| **NetworkChannel** | `src/channel/network.zig` | Reliable ordered UDP |
| **BroadcastBuffer** | `src/core/broadcast.zig` | 1-to-N fan-out for market data |
| **Archive** | `src/archive/archive.zig` | Segment-based message recording/replay |
| **RaftNode** | `src/cluster/raft.zig` | Raft consensus: election, replication |
| **Sequencer** | `src/sequencer/sequencer.zig` | Total-order sequence assignment |
| **FFI** | `src/ffi/exports.zig` | C-ABI exports for cross-language use |
