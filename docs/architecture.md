# ZigBolt Architecture

This document describes the internal architecture of ZigBolt, covering module
dependencies, data structures, memory layouts, threading model, and data flow.

## Layer Diagram

ZigBolt is organized into six layers, each depending only on layers below it:

```
+================================================================+
|                     Application Layer                          |
|  Transport, Publisher(T), Subscriber(T), RawPublisher/Sub      |
+================================================================+
|                     Channel Layer                              |
|  IpcChannel (shm)    UdpChannel    NetworkChannel (reliable)   |
+================================================================+
|                     Protocol Layer                             |
|  Reliability (NAK)   FlowControl   Fragmenter/Reassembler     |
+================================================================+
|                     Codec Layer                                |
|  WireCodec(T)   FrameHeader   TickMessage   OrderMessage       |
+================================================================+
|                     Core Layer                                 |
|  SpscRingBuffer   MpscRingBuffer   LogBuffer   Sequencer       |
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
  |
  +-- channel/ipc.zig     <-- memory, frame, config
  +-- channel/udp.zig     <-- frame, config
  +-- channel/reliability.zig <-- frame, config
  +-- channel/fragment.zig
  +-- channel/network.zig <-- udp, reliability, fragment
  |
  +-- api/publisher.zig   <-- ipc, wire
  +-- api/subscriber.zig  <-- ipc, wire
  +-- api/transport.zig   <-- ipc, publisher, subscriber
  |
  +-- archive/segment.zig
  +-- archive/archive.zig <-- segment
  |
  +-- sequencer/sequencer.zig
  |
  +-- cluster/raft_log.zig
  +-- cluster/raft.zig    <-- raft_log
  +-- cluster/cluster.zig <-- raft, raft_log
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
