---
title: Architecture
description: ZigBolt internal architecture, module dependencies, and data flows
---

This document describes the internal architecture of ZigBolt, covering module
dependencies, data structures, memory layouts, threading model, and data flow.

## Layer Diagram

ZigBolt is organized into seven layers, each depending only on layers below it:

```
+================================================================+
|                     Application Layer                          |
|  Transport, Publisher(T), Subscriber(T), RawPublisher/Sub      |
|  AgentRunner, CompositeAgent, DutyCycleTracker                 |
+================================================================+
|                     Channel Layer                              |
|  IpcChannel (shm)    UdpChannel    NetworkChannel (reliable)   |
|  CongestionControl   FlowControl(Min/Max/Tagged)               |
+================================================================+
|                     Protocol Layer                             |
|  Reliability (NAK)   Fragmenter/Reassembler   NakController    |
|  DataHeaderFlyweight  StatusMessageFlyweight   NakFlyweight     |
|  SetupFlyweight   RttMeasurementFlyweight   ErrorFlyweight     |
+================================================================+
|                     Codec Layer                                |
|  WireCodec(T)   SbeEncoder/SbeDecoder   FIX Messages           |
|  FrameHeader   TickMessage   OrderMessage   Decimal64          |
+================================================================+
|                     Core Layer                                 |
|  SpscRingBuffer   MpscRingBuffer   LogBuffer   Sequencer       |
|  BroadcastBuffer (1-to-N)   CounterSet   GlobalCounters        |
|  IdleStrategy (BusySpin/Yielding/Sleeping/Backoff/NoOp)        |
+================================================================+
|                     Cluster / Archive Layer                    |
|  RaftNode   Cluster   WriteAheadLog   SnapshotManager          |
|  Archive   Catalog   SparseIndex   Compressor/Decompressor     |
+================================================================+
|                     Platform Layer                             |
|  config.zig (cache lines, timestamps)   memory.zig (shm/mmap) |
+================================================================+
```

## Module Dependency Graph

```
root.zig
  |
  +-- platform/config.zig          (constants, timestampNs)
  +-- platform/memory.zig          (SharedRegion, mmap/shm)
  |
  +-- core/frame.zig               (FrameHeader, alignment)
  +-- core/spsc.zig       <-- frame, config
  +-- core/mpsc.zig        <-- frame, config
  +-- core/log_buffer.zig  <-- frame, config
  |
  +-- codec/wire.zig               (WireCodec, TickMessage, OrderMessage)
  +-- codec/sbe.zig                (SbeEncoder, SbeDecoder, MessageHeader, GroupHeader, Decimal64)
  +-- codec/fix_messages.zig       (NewOrderSingle, ExecutionReport, MarketData, etc.) <-- sbe
  |
  +-- protocol/flyweight.zig       (DataHeader, StatusMessage, NAK, Setup, RTT, Error flyweights)
  |
  +-- core/broadcast.zig           (BroadcastTransmitter, BroadcastReceiver) <-- config
  +-- core/idle_strategy.zig       (BusySpin, Yielding, Sleeping, Backoff, NoOp)
  +-- core/agent.zig               (AgentFn, AgentRunner, CompositeAgent) <-- idle_strategy
  +-- core/counters.zig            (Counter, CounterSet, GlobalCounters)
  |
  +-- channel/ipc.zig     <-- memory, frame, config
  +-- channel/udp.zig     <-- frame, config
  +-- channel/reliability.zig <-- frame, config
  +-- channel/fragment.zig
  +-- channel/network.zig <-- udp, reliability, fragment
  +-- channel/congestion.zig       (CongestionControl, RttEstimator, NakController)
  +-- channel/flow_control.zig     (MinFlowControl, MaxFlowControl, TaggedFlowControl)
  |
  +-- api/publisher.zig   <-- ipc, wire
  +-- api/subscriber.zig  <-- ipc, wire
  +-- api/transport.zig   <-- ipc, publisher, subscriber
  |
  +-- archive/segment.zig
  +-- archive/archive.zig <-- segment
  +-- archive/catalog.zig          (Catalog, CatalogEntry)
  +-- archive/index.zig            (SparseIndex, IndexEntry)
  +-- archive/compression.zig      (Compressor, Decompressor, compressFrame/decompressFrame)
  |
  +-- sequencer/sequencer.zig
  |
  +-- cluster/raft_log.zig
  +-- cluster/raft.zig    <-- raft_log
  +-- cluster/cluster.zig <-- raft, raft_log
  +-- cluster/wal.zig              (WriteAheadLog, WalEntry, VoteState)
  +-- cluster/snapshot.zig         (SnapshotManager, SnapshotData)
  |
  +-- ffi/exports.zig     <-- zigbolt (root)
```

## Key Data Structures

### FrameHeader (8 bytes)

Every message in a ring buffer or log buffer is prefixed by this header:

```
Offset  Size  Field          Description
------  ----  -----          -----------
0       4     frame_length   i32: >0 data, <0 padding, =0 uncommitted
4       4     msg_type_id    i32: user-defined message type
```

The total frame size is `alignUp(8 + payload_len, 8)` -- always 8-byte aligned.

### SpscRingBuffer Memory Layout

```
                     Cache Line 0          Cache Line 1
                 +------------------+  +------------------+
                 | head (atomic u64)|  | tail (atomic u64)|
                 |   + padding      |  |   + padding      |
                 +------------------+  +------------------+
                 |                                        |
                 |          buffer[capacity]               |
                 |    (cache-line aligned, power of 2)     |
                 +----------------------------------------+

head and tail are on separate cache lines to prevent false sharing
between the producer (writes head) and consumer (writes tail).
```

- **head**: write position, modified only by producer, stored with `.release`
- **tail**: read position, modified only by consumer, stored with `.release`
- **mask**: `capacity - 1` (comptime constant, capacity must be power of 2)
- Wrap-around uses modular arithmetic: `pos & mask`

### MpscRingBuffer Memory Layout

Same structure as SPSC, but:
- **head** is advanced via CAS (compare-and-swap) for multiple producers
- **tail** is a plain `usize` (single consumer only)
- Two-phase commit: CAS claims space, then `frame_length` stored with `.release` to commit

### LogBuffer Memory Layout (Aeron-style)

```
+-------------------+-------------------+-------------------+
|    Term 0         |    Term 1         |    Term 2         |
|  (term_length)    |  (term_length)    |  (term_length)    |
+-------------------+-------------------+-------------------+

tail_position (atomic u64) -- absolute byte offset, wraps across terms
head_position (atomic u64) -- consumer read position

Term rotation: when a message doesn't fit in the current term,
a padding frame is inserted and tail advances to the next term.
Term index = (position / term_length) % 3
Term offset = position % term_length
```

The Claim API provides two-phase publishing:
1. `claim(length)` -- atomically reserves space, returns `Claim`
2. Write payload into `claim.term_buffer[claim.term_offset + 8 ..]`
3. `commit(claim, msg_type_id)` -- release-stores `frame_length` to make visible

### IPC Channel Shared Memory Layout

```
Offset     Size              Content
------     ----              -------
0          4096              Metadata (cache-line padded)
  +0       8                   magic: 0x5A49_4742_4F4C_5421 ("ZIGBOLT!")
  +8       4                   version: 1
  +12      4                   term_length
  +CL      8                   tail_position (atomic u64)
  +2*CL    8                   head_position (atomic u64)
4096       term_length       Term 0
4096+TL    term_length       Term 1
4096+2*TL  term_length       Term 2

Total size: 4096 + 3 * term_length
CL = cache_line_size (128 bytes on modern CPUs)
```

### NetworkHeader (Network Protocol)

```
Offset  Size  Field            Description
------  ----  -----            -----------
0       1     version          Protocol version (1)
1       1     header_type      data(0), nak(1), heartbeat(2), setup(3), teardown(4)
2       4     session_id       Publisher-subscriber pair identifier
6       4     stream_id        Topic/channel within session
10      8     sequence         Monotonically increasing per stream
18      4     payload_length   Bytes following this header
22      3     _reserved        Padding
```

### WireCodec Packed Message Layout

Messages must be `packed struct` with no pointers. Validated entirely at comptime.

**TickMessage (32 bytes)**:
```
Offset  Size  Field
------  ----  -----
0       8     timestamp_ns (u64)
8       4     symbol_id (u32)
12      8     price (i64)
20      8     volume (u64)
28      1     side (enum u8: bid=0, ask=1)
29      3     _padding (u24)
```

**OrderMessage (48 bytes)**:
```
Offset  Size  Field
------  ----  -----
0       8     timestamp_ns (u64)
8       8     order_id (u64)
16      4     symbol_id (u32)
20      8     price (i64)
28      8     quantity (u64)
36      1     side (enum u8: buy=0, sell=1)
37      1     order_type (enum u8: limit=0, market=1, cancel=2)
38      2     _padding (u16)
```

Wire size must be a multiple of 8 bytes. Encoding is a direct `@memcpy` of the
packed representation -- zero overhead.

## Threading Model

### IPC Channel (SPSC)

```
Process A (Publisher)         Shared Memory           Process B (Subscriber)
+-------------------+    +-------------------+    +-------------------+
| publish()         | -> | tail_position     | <- | poll()            |
| writes payload    |    | [Term Buffers]    |    | reads frames      |
| stores frame_len  |    | head_position     |    | advances head     |
| advances tail     |    +-------------------+    +-------------------+
+-------------------+

- Publisher writes payload, then release-stores frame_length
- Subscriber acquire-loads frame_length, reads payload, advances head
- No locks, no CAS -- pure acquire/release ordering
```

### MPSC Ring Buffer

```
Thread 1 (Producer)  Thread 2 (Producer)  Thread 3 (Consumer)
     |                    |                    |
     v                    v                    |
   CAS(head)            CAS(head)             |
     |                    |                    |
   write payload        write payload          |
     |                    |                    |
   release-store        release-store          |
   frame_length         frame_length           |
                                               v
                                         acquire-load
                                         frame_length
                                         read payload
                                         advance tail
```

### Network Channel

Single-threaded event loop:
1. `publish()` -- encode, fragment if needed, send via UDP
2. `poll()` -- receive UDP datagrams, track sequences, reassemble, deliver
3. NAK generation happens at the end of each poll cycle

### Raft Cluster

Each node runs a single-threaded tick loop:
1. Receive messages from peers
2. Handle via `handleMessage()` (state transitions, log replication)
3. `tick()` applies committed entries to the state machine
4. Heartbeats sent periodically by the leader

## Data Flow: Publish to Receive

### IPC Path (lowest latency)

```
Publisher.offer(&msg)
  |
  v
WireCodec.encode()          -- @memcpy packed struct to bytes
  |
  v
IpcChannel.publish()        -- write FrameHeader + payload into term buffer
  |                            release-store frame_length, advance tail
  v
  --- shared memory ---
  |
  v
IpcChannel.poll()           -- acquire-load tail, read frames
  |
  v
WireCodec.decode()          -- pointer cast into shared memory (zero-copy)
  |
  v
Subscriber handler(msg)     -- user callback with *const MsgType
```

Total copies: 1 (encode). Decode is zero-copy (pointer cast).

### Network Path (reliable UDP)

```
NetworkChannel.publish(data)
  |
  v
FlowControl.tryConsume()     -- check credit window
  |
  v
Fragmenter (if needed)       -- split into MTU-sized chunks
  |
  v
sendWithReliability()        -- assign sequence number
  |                             store copy in SendBuffer
  v                             prepend NetworkHeader
UdpChannel.send()            -- sendto() syscall
  |
  v
  --- network (UDP datagram) ---
  |
  v
UdpChannel.recv()            -- recvfrom() syscall
  |
  v
NetworkChannel.poll()        -- parse NetworkHeader
  |                             RecvTracker.recordReceived()
  v                             handle NAKs, heartbeats
Reassembler (if fragmented)  -- collect fragments, deliver complete
  |
  v
handler(data)                -- user callback
```

### Archive Path

```
Archive.record(stream_id, msg_type_id, data, timestamp_ns)
  |
  v
SegmentManager.write(Record)  -- append to current segment file
  |                              rotate segment when full
  v
[disk: /tmp/zigbolt/archive/segment_NNNN.dat]

Archive.replay(params, handler)
  |
  v
SegmentManager.openSegment()   -- memory-map segment file
  |
  v
Segment.readRecord()           -- sequential scan with offset tracking
  |                               optional stream_id filter
  v
handler(Record)                -- user callback per archived message
```

## Wire Protocol Flyweights

Aeron-compatible wire protocol frames. Each flyweight wraps a raw `[]u8` buffer
and provides typed accessor methods at fixed byte offsets (little-endian).

### DataHeaderFlyweight (32 bytes)

```
Offset  Size  Field           Description
------  ----  -----           -----------
0       4     frame_length    i32: total frame size including header
4       1     version         u8: protocol version
5       1     flags           u8: BEGIN(0x80), END(0x40), EOS(0x20)
6       2     frame_type      u16: FrameType enum (DATA=0x01)
8       4     term_offset     u32: offset within the term buffer
12      4     session_id      i32: publication session identifier
16      4     stream_id       i32: channel stream identifier
20      4     term_id         i32: term buffer identifier
24      8     reserved_value  i64: user-defined metadata
```

### StatusMessageFlyweight (36 bytes)

```
Offset  Size  Field                    Description
------  ----  -----                    -----------
0       8     [HeaderFlyweight]        Base header (frame_length, version, flags, type=SM)
8       4     session_id               i32
12      4     stream_id                i32
16      4     consumption_term_id      i32: term consumed up to
20      4     consumption_term_offset  i32: offset within consumption term
24      4     receiver_window_length   i32: advertised window (bytes)
28      8     receiver_id              i64: unique receiver identifier
```

### NakFlyweight (28 bytes)

```
Offset  Size  Field        Description
------  ----  -----        -----------
0       8     [Header]     Base header (type=NAK)
8       4     session_id   i32
12      4     stream_id    i32
16      4     term_id      i32: term containing missing data
20      4     term_offset  i32: start of missing range
24      4     nak_length   i32: length of missing range
```

### Other Flyweights

- **SetupFlyweight** (40 bytes) -- session establishment with term_length, MTU, TTL
- **RttMeasurementFlyweight** (40 bytes) -- echo_timestamp_ns + reception_delta_ns
- **ErrorFlyweight** (28+ bytes) -- error_code + variable-length error string

### Frame Types

```
PAD=0x00, DATA=0x01, NAK=0x02, SM=0x03, ERR=0x04, SETUP=0x05, RTTM=0x06, RES=0x07
```

## SBE Message Format

SBE (Simple Binary Encoding) messages follow the FIX Trading Community standard:

```
[MessageHeader: 8 bytes]
  block_length: u16     -- root block size in bytes
  template_id:  u16     -- message type ID
  schema_id:    u16     -- schema identifier
  version:      u16     -- schema version
[Root block: block_length bytes]
  Fixed fields in schema-defined order (zero-copy access)
[Groups: variable]
  [GroupHeader: 4 bytes] (block_length: u16, num_in_group: u16)
  [Entry x num_in_group: block_length bytes each]
  (groups may nest -- each entry can contain sub-groups)
[VarData: variable]
  [length: u32][data: length bytes]
```

SbeEncoder writes into caller-provided buffers with zero heap allocations.
SbeDecoder reads via zero-copy: `getBytes()` returns pointers directly into the
underlying buffer. Decimal64 represents fixed-point prices (mantissa only on wire,
exponent in schema).

## BroadcastBuffer Memory Layout

1-to-N fan-out buffer for market data distribution. One transmitter, many receivers.

```
+-------------------------------------------+---------------------------+
|        Buffer Region (capacity bytes)      |   Trailer (4 cache lines) |
|  [record][record][record]...               |   tail_intent (CL 0)     |
|                                            |   tail        (CL 1)     |
|  capacity must be a power of 2             |   latest      (CL 2)     |
+-------------------------------------------+---------------------------+

Record format:
  [i32 payload_length][i32 msg_type_id][payload...][padding to 8-byte alignment]

  payload_length: actual payload size (excluding header)
  msg_type_id:    user-defined type; 0 = padding record (skip on read)
```

- **BroadcastTransmitter**: writes with two-phase commit (tail_intent, then tail)
- **BroadcastReceiver**: tracks its own cursor; detects lapping via tail_intent
- **CopyBroadcastReceiver**: copies payload to scratch buffer for safe retention
- Max message size: `(capacity / 8) - 8` bytes

## Flow Control Architecture

Three flow control strategies matching Aeron's design:

```
                    Receiver Status Messages
                    (position + window)
Sender ─────────────────────────────────────────── Receivers
  |                                                  |
  v                                                  |
FlowControl.onStatusMessage()                        |
  |                                                  |
  +── MinFlowControl:  sender_limit = min(all positions + windows)
  |     Reliable multicast -- sender waits for slowest receiver
  |
  +── MaxFlowControl:  sender_limit = sender_position + window
  |     Best-effort -- sender never blocked, slow receivers lose data
  |
  +── TaggedFlowControl: sender_limit = min(tagged receivers only)
        Group-based -- risk checkers constrain, market data does not
```

Each strategy tracks up to 16 receivers in a fixed-size array (zero allocation
on hot path). Stale receivers are timed out after `receiver_timeout_ns`.

## Agent / IdleStrategy Threading Model

```
AgentRunner (dedicated thread)
  |
  v
  while (running) {
      work_count = agent.doWork()    // poll channels, process messages
      idle_strategy.idle(work_count) // back off when idle
  }

IdleStrategy state machine (BackoffIdleStrategy):
  NOT_IDLE ──(no work)──> SPINNING ──(max_spins)──> YIELDING ──(max_yields)──> PARKING
     ^                                                                            |
     |──────────────────────(work_count > 0)──────────────────────────────────────+

CompositeAgent: aggregates work from multiple sub-agents into a single runner.
DutyCycleTracker: measures cycle duration, max cycle time, work ratio.
```

## Congestion Control

AIMD (Additive Increase / Multiplicative Decrease) with RTT estimation:

```
Slow Start:     cwnd += MSS per ACK          (exponential growth)
Congestion Avoidance: cwnd += MSS*MSS/cwnd   (linear growth, ~1 MSS per RTT)
On Loss (NAK):  ssthresh = cwnd/2, cwnd = ssthresh
On Timeout:     ssthresh = cwnd/2, cwnd = min_window, re-enter slow start

RTT Estimator (RFC 6298 EWMA):
  SRTT   = 7/8 * SRTT + 1/8 * sample
  RTTVAR = 3/4 * RTTVAR + 1/4 * |SRTT - sample|
  RTO    = SRTT + max(G, 4 * RTTVAR)

NakController: exponential backoff for NAK timing (delay = base * 2^backoff)
```

## WAL Record Format

Write-Ahead Log for Raft consensus durability:

```
Offset  Size     Field          Description
------  ----     -----          -----------
0       4        record_length  u32: term(8) + index(8) + payload + crc(4)
4       8        term           u64: Raft term
12      8        index          u64: Raft log index
20      variable payload        Entry data
20+N    4        crc32          u32: CRC32 over term + index + payload
```

Total per-entry overhead: 24 bytes.

Sync policies: `every_entry` (safest), `every_n_entries` (batched), `explicit`.
Recovery: sequential scan, CRC validation, truncation of corrupt tail.

## Snapshot Format

Raft state snapshots for log compaction:

```
Offset  Size     Field                Description
------  ----     -----                -----------
0       4        magic                u32: 0x5A425350 ("ZBSP")
4       2        version              u16: 1
6       8        last_included_term   u64
14      8        last_included_index  u64
22      4        state_size           u32
26      variable state_data           Application state machine bytes
26+N    4        crc32                u32: CRC32 over header + state_data
```

File naming: `snapshot_{last_index}.zbsp`. SnapshotManager triggers after
a configurable number of committed entries and supports old snapshot cleanup.

## Data Flow: Broadcast Path

```
BroadcastTransmitter.transmit(msg_type_id, payload)
  |
  v
Calculate aligned record length
  |
  v
Check wrap-around:
  |-- fits: write record at current offset
  |-- wraps: insert padding record, write at offset 0
  |
  v
Two-phase commit:
  1. tail_intent_counter.store(new_tail, .release)
  2. Write RecordHeader + payload into buffer
  3. tail_counter.store(new_tail, .release)
  |
  v
BroadcastReceiver.receiveNext() [each receiver independently]
  |
  v
Check tail vs cursor:
  |-- cursor >= tail: return null (no new data)
  |-- lapped (tail > cursor + capacity): skip forward, increment lapped_count
  |
  v
Read RecordHeader:
  |-- padding (msg_type_id == 0): skip, loop
  |-- data: extract payload, advance cursor
  |
  v
Validate (tail_intent <= cursor + capacity): data not overwritten
  |
  v
Return Message { msg_type_id, payload }
```
