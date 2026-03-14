---
title: API Reference
description: Complete API reference for all ZigBolt modules
---


All public types are exported from `src/root.zig` and accessible via
`@import("zigbolt")`.

---

## Table of Contents

- [Platform](#platform)
- [Core Data Structures](#core-data-structures)
- [Wire Codec](#wire-codec)
- [IPC Channel](#ipc-channel)
- [UDP Channel](#udp-channel)
- [Network Channel](#network-channel)
- [Reliability Protocol](#reliability-protocol)
- [Fragment Layer](#fragment-layer)
- [Publisher / Subscriber API](#publisher--subscriber-api)
- [Transport](#transport)
- [Archive](#archive)
- [Sequencer](#sequencer)
- [Cluster (Raft Consensus)](#cluster-raft-consensus)
- [Write-Ahead Log](#write-ahead-log)
- [Snapshots](#snapshots)
- [SBE Codec](#sbe-codec)
- [FIX Messages](#fix-messages)
- [Wire Protocol Flyweights](#wire-protocol-flyweights)
- [Broadcast Buffer](#broadcast-buffer)
- [Idle Strategies](#idle-strategies)
- [Agent Pattern](#agent-pattern)
- [Counters](#counters)
- [Congestion Control](#congestion-control)
- [Flow Control](#flow-control)
- [Archive Catalog](#archive-catalog)
- [Archive Index](#archive-index)
- [Compression](#compression)
- [FFI Exports](#ffi-exports)

---

## Platform

### `platform.config`

```zig
const cache_line_size: usize;        // 128 on modern CPUs
const page_size: usize;              // 4096
const is_linux: bool;
const is_macos: bool;
const supports_hugepages: bool;      // true on Linux
const supports_io_uring: bool;       // true on Linux
const frame_alignment: u32 = 8;
const default_term_length: usize;    // 1 << 20 (1 MB)
const default_ring_capacity: usize;  // 1 << 16 (64K)

fn timestampNs() u64;               // nanosecond timestamp
fn alignUp(size: u32, alignment: u32) u32;
```

### `platform.memory`

```zig
const SharedRegion = struct {
    base: [*]u8,
    size: usize,
    fn deinit(self: *SharedRegion) void;
};

const MemoryConfig = struct {
    use_hugepages: bool = false,
    pre_fault: bool = true,
};

fn createShared(name: [*:0]const u8, size: usize, config: MemoryConfig) !SharedRegion;
fn openShared(name: [*:0]const u8, size: usize) !SharedRegion;
fn prefault(region: SharedRegion) void;
```

---

## Core Data Structures

### `SpscRingBuffer(comptime capacity: usize)`

Lock-free single-producer single-consumer ring buffer. `capacity` must be a
power of 2.

```zig
const RB = zigbolt.SpscRingBuffer(1024);
var rb = RB.init();
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init() Self` | Create a zeroed ring buffer |
| `write` | `fn write(self: *Self, data: []const u8, msg_type_id: i32) bool` | Write a framed message. Returns `false` if full. |
| `read` | `fn read(self: *Self) ?ReadResult` | Read the next message. Returns `null` if empty. |

**ReadResult**:
```zig
pub const ReadResult = struct {
    data: []const u8,
    msg_type_id: i32,
};
```

### `MpscRingBuffer(comptime capacity: usize)`

Lock-free multi-producer single-consumer ring buffer using CAS.
`capacity` must be a power of 2.

```zig
const RB = zigbolt.MpscRingBuffer(1024);
var rb = RB.init();
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init() Self` | Create a zeroed ring buffer |
| `write` | `fn write(self: *Self, data: []const u8, msg_type_id: i32) bool` | Thread-safe write via CAS. Returns `false` if full. |
| `read` | `fn read(self: *Self) ?ReadResult` | Single-consumer read. Returns `null` if empty or uncommitted. |

### `LogBuffer(comptime cfg: LogBufferConfig)`

Aeron-style triple-buffered log with term rotation.

```zig
const Buf = zigbolt.LogBuffer(.{ .term_length = 1 << 20 });
var buf = Buf.init();
```

**LogBufferConfig**:
```zig
pub const LogBufferConfig = struct {
    term_length: usize = 1 << 20,  // must be power of 2
};
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init() Self` | Create a zeroed log buffer |
| `claim` | `fn claim(self: *Self, length: u32) ?Claim` | Claim space for a message. Returns `null` if consumer is too far behind. |
| `commit` | `fn commit(self: *Self, c: Claim, msg_type_id: i32) void` | Commit a claimed frame, making it visible to readers. |
| `read` | `fn read(self: *Self, handler: *const fn([]const u8, i32) void, limit: u32) u32` | Read committed frames, calling handler for each. Returns count. |

**Claim**:
```zig
pub const Claim = struct {
    term_buffer: [*]u8,
    term_offset: u32,
    length: u32,
    term_id: u32,
};
```

### `FrameHeader`

```zig
pub const FrameHeader = extern struct {
    frame_length: i32 = 0,   // >0: data, <0: padding, =0: uncommitted
    msg_type_id: i32 = 0,
    pub const SIZE: u32 = 8;
};
```

### Frame Helpers

```zig
fn alignedFrameLength(payload_length: u32) u32;
fn isPaddingFrame(frame_length: i32) bool;
fn isDataFrame(frame_length: i32) bool;
fn isUncommitted(frame_length: i32) bool;
const MAX_PAYLOAD_SIZE: u32 = 1 << 24;  // 16 MB
```

---

## Wire Codec

### `WireCodec(comptime T: type)`

Comptime-generated zero-copy codec for packed structs. `T` must be a `packed struct`
with no pointer or slice fields. Wire size must be a multiple of 8 bytes.

```zig
const Codec = zigbolt.WireCodec(zigbolt.TickMessage);
```

| Member | Type | Description |
|--------|------|-------------|
| `wire_size` | `usize` | Size of the wire representation in bytes |
| `Type` | `type` | The underlying message type |

| Method | Signature | Description |
|--------|-----------|-------------|
| `encode` | `fn encode(msg: *const T, buf: []u8) void` | Copy message bytes into buffer |
| `decode` | `fn decode(buf: []const u8) *align(1) const T` | Zero-copy: returns pointer into buffer |
| `decodeMut` | `fn decodeMut(buf: []u8) *align(1) T` | Mutable zero-copy decode |
| `batchDecode` | `fn batchDecode(buf: []const u8, out: []T) u32` | Decode multiple messages |
| `batchEncode` | `fn batchEncode(msgs: []const T, buf: []u8) u32` | Encode multiple messages |

### Built-in Message Types

**TickMessage** (32 bytes):
```zig
pub const TickMessage = packed struct {
    timestamp_ns: u64,
    symbol_id: u32,
    price: i64,
    volume: u64,
    side: enum(u8) { bid = 0, ask = 1 },
    _padding: u24 = 0,
};
```

**OrderMessage** (48 bytes):
```zig
pub const OrderMessage = packed struct {
    timestamp_ns: u64,
    order_id: u64,
    symbol_id: u32,
    price: i64,
    quantity: u64,
    side: enum(u8) { buy = 0, sell = 1 },
    order_type: enum(u8) { limit = 0, market = 1, cancel = 2 },
    _padding: u16 = 0,
};
```

---

## IPC Channel

### `IpcConfig`

```zig
pub const IpcConfig = struct {
    term_length: usize = default_term_length,  // power of 2
    use_hugepages: bool = false,               // Linux only
    pre_fault: bool = true,                    // pre-fault pages
};
```

### `IpcChannel`

Shared-memory IPC channel. SPSC: one publisher, one subscriber.

| Method | Signature | Description |
|--------|-----------|-------------|
| `create` | `fn create(name: [*:0]const u8, config: IpcConfig) !IpcChannel` | Create a new channel (publisher side) |
| `open` | `fn open(name: [*:0]const u8, config: IpcConfig) !IpcChannel` | Open an existing channel (subscriber side) |
| `publish` | `fn publish(self: *IpcChannel, data: []const u8, msg_type_id: i32) !void` | Publish a message |
| `poll` | `fn poll(self: *IpcChannel, handler: *const fn(ReadResult) void, limit: u32) u32` | Poll for messages. Returns count. |
| `deinit` | `fn deinit(self: *IpcChannel) void` | Close and release resources |

**ReadResult**:
```zig
pub const ReadResult = struct {
    data: []const u8,
    msg_type_id: i32,
};
```

**Errors**:
- `error.InvalidChannel` -- magic number mismatch on open
- `error.UnsupportedVersion` -- protocol version mismatch
- `error.MessageTooLarge` -- payload exceeds `MAX_PAYLOAD_SIZE`

---

## UDP Channel

### `UdpConfig`

```zig
pub const UdpConfig = struct {
    bind_address: std.net.Address,
    remote_address: ?std.net.Address = null,
    multicast_group: ?[4]u8 = null,
    send_buffer_size: u32 = 2 * 1024 * 1024,  // 2 MB
    recv_buffer_size: u32 = 2 * 1024 * 1024,  // 2 MB
    non_blocking: bool = true,
};
```

### `UdpChannel`

UDP unicast and multicast channel.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(config: UdpConfig) !UdpChannel` | Create and bind a UDP socket |
| `deinit` | `fn deinit(self: *UdpChannel) void` | Close the socket |
| `send` | `fn send(self: *UdpChannel, data: []const u8, dest: ?net.Address) !usize` | Send a raw datagram |
| `recv` | `fn recv(self: *UdpChannel, buf: []u8) !?RecvResult` | Receive a raw datagram (non-blocking) |
| `sendFrame` | `fn sendFrame(self: *UdpChannel, data: []const u8, msg_type_id: i32, dest: ?net.Address) !void` | Send a framed message (FrameHeader + payload) |
| `recvFrame` | `fn recvFrame(self: *UdpChannel, buf: []u8) !?FrameRecvResult` | Receive and parse a framed message |

**RecvResult**:
```zig
pub const RecvResult = struct {
    data: []const u8,
    from: std.net.Address,
};
```

**FrameRecvResult**:
```zig
pub const FrameRecvResult = struct {
    payload: []const u8,
    msg_type_id: i32,
    from: std.net.Address,
};
```

---

## Network Channel

### `NetworkConfig`

```zig
pub const NetworkConfig = struct {
    udp: UdpConfig,
    session_id: u32 = 1,
    stream_id: u32 = 1,
    send_buffer_capacity: usize = 4096,
    recv_window_size: u64 = 4096,
    flow_control_window: i64 = 4 * 1024 * 1024,  // 4 MB
    mtu: u32 = 1472,
    max_message_size: u32 = 1 << 20,
    heartbeat_interval_ns: u64 = 100_000_000,     // 100 ms
    nak_delay_ns: u64 = 1_000_000,                // 1 ms
};
```

### `NetworkChannel`

Reliable, ordered network channel. Combines UDP, NAK reliability, flow control,
and fragmentation.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, config: NetworkConfig) !NetworkChannel` | Initialize all sub-components |
| `deinit` | `fn deinit(self: *NetworkChannel) void` | Release all resources |
| `publish` | `fn publish(self: *NetworkChannel, data: []const u8, msg_type_id: i32) !void` | Publish with reliability and flow control |
| `poll` | `fn poll(self: *NetworkChannel, handler: *const fn([]const u8) void, limit: u32) !u32` | Poll for complete messages |

**Errors**:
- `error.BackPressured` -- flow control window exhausted

---

## Reliability Protocol

### `NetworkHeader`

```zig
pub const NetworkHeader = extern struct {
    version: u8 = 1,
    header_type: HeaderType,
    session_id: u32,
    stream_id: u32,
    sequence: u64,
    payload_length: u32,
    _reserved: [3]u8 = .{0, 0, 0},

    pub const HeaderType = enum(u8) { data, nak, heartbeat, setup, teardown };
    pub const SIZE: usize;
};
```

### `NakMessage`

```zig
pub const NakMessage = extern struct {
    session_id: u32,
    stream_id: u32,
    from_sequence: u64,
    count: u32,
    _padding: [4]u8,
};
```

### `SendBuffer`

Stores sent payloads for retransmission on NAK.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, capacity: usize) !SendBuffer` | Allocate entry ring |
| `deinit` | `fn deinit(self: *SendBuffer, allocator: Allocator) void` | Free all entries |
| `store` | `fn store(self: *SendBuffer, sequence: u64, data: []const u8, allocator: Allocator) !void` | Store a copy for retransmit |
| `get` | `fn get(self: *SendBuffer, sequence: u64) ?*SendEntry` | Look up by sequence |
| `release` | `fn release(self: *SendBuffer, up_to_sequence: u64) void` | Release acknowledged entries |

### `RecvTracker`

Bitmap-based gap detection.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, window_size: u64) !RecvTracker` | Allocate bitmap |
| `deinit` | `fn deinit(self: *RecvTracker) void` | Free bitmap |
| `recordReceived` | `fn recordReceived(self: *RecvTracker, sequence: u64) ?GapInfo` | Record a sequence, return gap if detected |
| `getMissing` | `fn getMissing(self: *RecvTracker, allocator: Allocator) ![]u64` | List all missing sequences in window |
| `slideWindow` | `fn slideWindow(self: *RecvTracker, new_base: u64) void` | Advance the window forward |

### `FlowControl`

Credit-based flow control.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(window_size: i64) FlowControl` | Initialize with credit window |
| `tryConsume` | `fn tryConsume(self: *FlowControl, bytes: usize) bool` | Atomically consume credits |
| `replenish` | `fn replenish(self: *FlowControl, bytes: usize) void` | Add credits back |
| `available` | `fn available(self: *FlowControl) i64` | Current available credits |

---

## Fragment Layer

### `Fragmenter`

Splits large messages into MTU-sized fragments.

### `Reassembler`

Collects fragments and delivers complete messages.

### `FragmentConfig`

```zig
pub const FragmentConfig = struct {
    mtu: u32 = 1472,
    max_message_size: u32 = 1 << 20,
};
```

---

## Publisher / Subscriber API

### `Publisher(comptime MsgType: type)`

Typed publisher using `WireCodec(MsgType)` over IPC.

```zig
var pub = zigbolt.Publisher(TickMessage).init(&channel, 1);
try pub.offer(&tick_msg);
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(channel: *IpcChannel, msg_type_id: i32) Self` | Bind to a channel |
| `offer` | `fn offer(self: *Self, msg: *const MsgType) !void` | Publish a typed message |
| `tryOffer` | `fn tryOffer(self: *Self, msg: *const MsgType) bool` | Non-blocking publish, returns false on back-pressure |
| `offerRaw` | `fn offerRaw(self: *Self, data: []const u8) !void` | Publish pre-encoded bytes |

### `RawPublisher`

Untyped publisher for raw byte messages.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(channel: *IpcChannel, msg_type_id: i32) RawPublisher` | Bind to a channel |
| `offer` | `fn offer(self: *RawPublisher, data: []const u8) !void` | Publish raw bytes |

### `Subscriber(comptime MsgType: type)`

Typed subscriber using `WireCodec(MsgType)` over IPC.

```zig
var sub = zigbolt.Subscriber(TickMessage).init(&channel, 1);
_ = sub.poll(&handleTick, 100);
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(channel: *IpcChannel, msg_type_id: i32) Self` | Bind to a channel |
| `poll` | `fn poll(self: *Self, handler: *const fn(*const MsgType) void, limit: u32) u32` | Poll and decode messages |
| `pollRaw` | `fn pollRaw(self: *Self, handler: *const fn(IpcChannel.ReadResult) void, limit: u32) u32` | Poll raw frames |

### `RawSubscriber`

Untyped subscriber for raw byte messages.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(channel: *IpcChannel) RawSubscriber` | Bind to a channel |
| `poll` | `fn poll(self: *RawSubscriber, handler: *const fn(IpcChannel.ReadResult) void, limit: u32) u32` | Poll raw frames |

---

## Transport

### `TransportConfig`

```zig
pub const TransportConfig = struct {
    term_length: usize = 1 << 20,
    use_hugepages: bool = false,
    pre_fault: bool = true,
};
```

### `Transport`

Main entry point. Manages IPC channels and creates typed publishers/subscribers.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, config: TransportConfig) Transport` | Create a transport |
| `deinit` | `fn deinit(self: *Transport) void` | Shut down all channels |
| `addPublication` | `fn addPublication(self, comptime MsgType, name: [:0]const u8, msg_type_id: i32) !Publisher(MsgType)` | Create a typed publisher |
| `addSubscription` | `fn addSubscription(self, comptime MsgType, name: [:0]const u8, msg_type_id: i32) !Subscriber(MsgType)` | Create a typed subscriber |
| `addRawPublication` | `fn addRawPublication(self, name: [:0]const u8, msg_type_id: i32) !RawPublisher` | Create a raw publisher |
| `addRawSubscription` | `fn addRawSubscription(self, name: [:0]const u8) !RawSubscriber` | Create a raw subscriber |

---

## Archive

### `ArchiveConfig`

```zig
pub const ArchiveConfig = struct {
    segment_size: usize = 256 * 1024 * 1024,     // 256 MB
    base_path: []const u8 = "/tmp/zigbolt/archive",
    sync_policy: SyncPolicy = .periodic,
    sync_interval_ms: u32 = 1000,
    compression: ?CompressionAlgo = null,

    pub const SyncPolicy = enum { none, periodic, every_segment };
    pub const CompressionAlgo = enum { lz4, zstd };
};
```

### `Archive`

Segment-based message recording and replay.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, config: ArchiveConfig) !Archive` | Initialize archive |
| `deinit` | `fn deinit(self: *Archive) void` | Release resources |
| `record` | `fn record(self: *Archive, stream_id: u32, msg_type_id: i32, data: []const u8, timestamp_ns: u64) !void` | Record a message |
| `replay` | `fn replay(self: *Archive, params: ReplayParams, handler: *const fn(Record) void) !u64` | Replay messages. Returns count. |
| `stats` | `fn stats(self: *const Archive) Stats` | Get archive statistics |

**ReplayParams**:
```zig
pub const ReplayParams = struct {
    stream_id: ?u32 = null,  // null = all streams
    from_segment: u64 = 0,
    from_offset: u64 = 0,
    limit: ?u64 = null,
};
```

**Stats**:
```zig
pub const Stats = struct {
    total_records: u64,
    total_bytes: u64,
    segment_count: u64,
};
```

---

## Sequencer

### `Sequencer`

Atomic total-order sequence assignment.

```zig
var seq = zigbolt.Sequencer.init(.{ .initial_sequence = 0 });
const event = seq.sequence(stream_id, payload);
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(config: SequencerConfig) Sequencer` | Initialize sequencer |
| `sequence` | `fn sequence(self: *Sequencer, stream_id: u32, payload: []const u8) SequencedEvent` | Assign next sequence number (thread-safe) |
| `peekNextSequence` | `fn peekNextSequence(self: *const Sequencer) u64` | Read next sequence without consuming |
| `reset` | `fn reset(self: *Sequencer, initial_sequence: u64) void` | Reset for testing/replay |

**SequencedEvent**:
```zig
pub const SequencedEvent = struct {
    sequence: u64,
    timestamp_ns: u64,
    stream_id: u32,
    payload: []const u8,
};
```

### `MultiStreamSequencer`

Merges multiple input streams into one globally ordered output.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(config: SequencerConfig) MultiStreamSequencer` | Initialize |
| `sequenceFrom` | `fn sequenceFrom(self, stream_id: u32, payload: []const u8) SequencedEvent` | Sequence from a specific stream |
| `getStreamStats` | `fn getStreamStats(self, stream_id: u32) StreamStats` | Per-stream statistics |
| `totalEvents` | `fn totalEvents(self) u64` | Total events across all streams |

### `SequenceIndex`

Maps sequence numbers to stream/offset for replay.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator) SequenceIndex` | Initialize |
| `deinit` | `fn deinit(self: *SequenceIndex) void` | Free memory |
| `add` | `fn add(self, entry: IndexEntry) !void` | Add an index entry |
| `lookup` | `fn lookup(self, seq: u64) ?IndexEntry` | Look up by sequence number |
| `rangeFrom` | `fn rangeFrom(self, from_sequence: u64) []const IndexEntry` | Get all entries from a sequence |

---

## Cluster (Raft Consensus)

### `RaftConfig`

```zig
pub const RaftConfig = struct {
    node_id: u32,
    peer_count: u32,
    election_timeout_min_ms: u32 = 150,
    election_timeout_max_ms: u32 = 300,
    heartbeat_interval_ms: u32 = 50,
};
```

### `RaftNode`

Full Raft consensus implementation.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, config: RaftConfig) !RaftNode` | Initialize as follower |
| `deinit` | `fn deinit(self: *RaftNode) void` | Free resources |
| `handleMessage` | `fn handleMessage(self, from: u32, msg: RaftMessage) ?MessageResponse` | Handle incoming Raft message |
| `startElection` | `fn startElection(self) RaftMessage` | Begin leader election |
| `propose` | `fn propose(self, data: []const u8) !u64` | Propose a log entry (leader only) |
| `createAppendEntries` | `fn createAppendEntries(self, peer_id: u32) AppendEntries` | Create replication message for peer |
| `createHeartbeat` | `fn createHeartbeat(self) AppendEntries` | Create empty heartbeat |
| `getApplicableEntries` | `fn getApplicableEntries(self) []const StoredEntry` | Get committed but unapplied entries |
| `markApplied` | `fn markApplied(self, up_to: u64) void` | Mark entries as applied |
| `updateCommitIndex` | `fn updateCommitIndex(self) void` | Recalculate commit index from match_index |

**NodeState**: `enum { follower, candidate, leader }`

**RaftMessage**:
```zig
pub const RaftMessage = union(enum) {
    request_vote: RequestVote,
    request_vote_response: RequestVoteResponse,
    append_entries: AppendEntries,
    append_entries_response: AppendEntriesResponse,
};
```

### `ClusterConfig`

```zig
pub const ClusterConfig = struct {
    node_id: u32,
    peer_count: u32,
    election_timeout_min_ms: u32 = 150,
    election_timeout_max_ms: u32 = 300,
    heartbeat_interval_ms: u32 = 50,
};
```

### `StateMachine`

User-implemented interface for applying committed entries.

```zig
pub const StateMachine = struct {
    apply_fn: *const fn (entry: []const u8) void,
    snapshot_fn: ?*const fn () []const u8 = null,
    restore_fn: ?*const fn (snapshot: []const u8) void = null,
};
```

### `Cluster`

High-level cluster that wraps RaftNode and a StateMachine.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, config: ClusterConfig, sm: ?StateMachine) !Cluster` | Initialize |
| `deinit` | `fn deinit(self: *Cluster) void` | Shut down |
| `propose` | `fn propose(self, data: []const u8) !u64` | Propose command (leader only) |
| `handleMessage` | `fn handleMessage(self, from: u32, msg: RaftMessage) ?MessageResponse` | Process message |
| `tick` | `fn tick(self: *Cluster) void` | Apply committed entries to state machine |
| `isLeader` | `fn isLeader(self) bool` | Check leadership |
| `getState` | `fn getState(self) NodeState` | Current Raft state |

---

## Write-Ahead Log

### `WriteAheadLog`

Persistent WAL for Raft consensus. Each entry is CRC32-validated on disk.

```zig
var wal = try WriteAheadLog.init(allocator, .{
    .path = "zigbolt_raft.wal",
    .sync_policy = .every_n_entries,
    .sync_interval = 100,
});
defer wal.deinit();
```

**WalConfig**:
```zig
pub const WalConfig = struct {
    path: []const u8 = "zigbolt_raft.wal",
    sync_policy: SyncPolicy = .every_n_entries,
    sync_interval: u32 = 100,
};
pub const SyncPolicy = enum { every_entry, every_n_entries, explicit };
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, config: WalConfig) !WriteAheadLog` | Create or open a WAL file |
| `deinit` | `fn deinit(self: *WriteAheadLog) void` | Sync and close |
| `append` | `fn append(self, term: u64, index: u64, data: []const u8) !void` | Append a CRC32-validated entry |
| `readEntry` | `fn readEntry(self, log_index: u64) !?WalEntry` | Read entry by log index |
| `truncateFrom` | `fn truncateFrom(self, from_index: u64) !void` | Remove entries >= from_index |
| `recover` | `fn recover(self) ![]WalEntry` | Scan file, rebuild index, return valid entries |
| `flush` | `fn flush(self) !void` | Force fsync to disk |
| `lastIndex` | `fn lastIndex(self) u64` | Last written log index |
| `lastTerm` | `fn lastTerm(self) u64` | Term of last entry |
| `entryCount` | `fn entryCount(self) u64` | Number of entries |

**WalEntry**:
```zig
pub const WalEntry = struct {
    term: u64,
    index: u64,
    data: []const u8,
};
```

### `VoteState`

Persistent Raft vote state (16-byte file).

| Method | Signature | Description |
|--------|-----------|-------------|
| `save` | `fn save(self: VoteState, path: []const u8) !void` | Atomically save to file |
| `load` | `fn load(path: []const u8) !?VoteState` | Load from file, null if missing |

---

## Snapshots

### `SnapshotManager`

Manages Raft snapshots on disk with CRC32 validation.

```zig
var mgr = SnapshotManager.init(allocator, .{
    .base_path = "/var/lib/zigbolt/snapshots",
    .snapshot_interval = 10000,
});
defer mgr.deinit();
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, config: SnapshotConfig) SnapshotManager` | Initialize |
| `deinit` | `fn deinit(self: *SnapshotManager) void` | Cleanup |
| `shouldSnapshot` | `fn shouldSnapshot(self) bool` | True if interval reached |
| `onEntryCommitted` | `fn onEntryCommitted(self) void` | Track committed entries |
| `takeSnapshot` | `fn takeSnapshot(self, last_term: u64, last_index: u64, state_data: []const u8) !void` | Write snapshot to disk |
| `loadLatestSnapshot` | `fn loadLatestSnapshot(self) !?SnapshotData` | Load newest snapshot |
| `getLatestMeta` | `fn getLatestMeta(self) ?SnapshotMeta` | Metadata without loading state |
| `cleanOldSnapshots` | `fn cleanOldSnapshots(self, keep_count: usize) !void` | Delete all but N newest |

**SnapshotData** (caller must call `deinit()`):
```zig
pub const SnapshotData = struct {
    last_included_term: u64,
    last_included_index: u64,
    data: []u8,
    allocator: std.mem.Allocator,
    pub fn deinit(self: *SnapshotData) void;
};
```

---

## SBE Codec

### `SbeEncoder`

Encodes SBE messages into caller-provided byte buffers. Zero heap allocations.

```zig
var buf: [4096]u8 = undefined;
var enc = SbeEncoder.init(&buf);
const hdr_pos = try enc.putMessageHeader(42, 1, 1);
try enc.putU64(timestamp);
try enc.putI64(price);
enc.finishHeader(hdr_pos);
const wire_bytes = buf[0..enc.encodedLength()];
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(buf: []u8) SbeEncoder` | Initialize over buffer |
| `encodedLength` | `fn encodedLength(self) usize` | Bytes written so far |
| `putMessageHeader` | `fn putMessageHeader(self, template_id: u16, schema_id: u16, version: u16) !usize` | Write 8-byte header, returns position for `finishHeader` |
| `finishHeader` | `fn finishHeader(self, header_pos: usize) void` | Patch block_length after root fields |
| `putU8`..`putU64` | `fn putU64(self, val: u64) !void` | Write unsigned integers |
| `putI8`..`putI64` | `fn putI64(self, val: i64) !void` | Write signed integers |
| `putF32`/`putF64` | `fn putF64(self, val: f64) !void` | Write floats |
| `putChar` | `fn putChar(self, val: u8) !void` | Write character |
| `putBytes` | `fn putBytes(self, data: []const u8) !void` | Write fixed-length bytes |
| `putEnum` | `fn putEnum(self, comptime E: type, val: E) !void` | Write enum as integer |
| `beginGroup` | `fn beginGroup(self, block_length: u16, count: u16) !void` | Write group header |
| `putVarData` | `fn putVarData(self, data: []const u8) !void` | Write [u32 len][data] |

### `SbeDecoder`

Zero-copy SBE decoder. Returns pointers directly into the underlying buffer.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(buf: []const u8) SbeDecoder` | Initialize over buffer |
| `position` | `fn position(self) usize` | Current read position |
| `remaining` | `fn remaining(self) usize` | Bytes left |
| `skip` | `fn skip(self, n: usize) !void` | Advance position |
| `getMessageHeader` | `fn getMessageHeader(self) !MessageHeader` | Read 8-byte header |
| `getGroupHeader` | `fn getGroupHeader(self) !GroupHeader` | Read 4-byte group header |
| `getU8`..`getU64` | `fn getU64(self) !u64` | Read unsigned integers |
| `getI8`..`getI64` | `fn getI64(self) !i64` | Read signed integers |
| `getF32`/`getF64` | `fn getF64(self) !f64` | Read floats |
| `getBytes` | `fn getBytes(self, comptime N: usize) !*const [N]u8` | Zero-copy fixed bytes |
| `getBytesSlice` | `fn getBytesSlice(self, n: usize) ![]const u8` | Zero-copy runtime-length bytes |
| `getEnum` | `fn getEnum(self, comptime E: type) !E` | Read enum |
| `getVarData` | `fn getVarData(self) ![]const u8` | Zero-copy variable-length data |

### `Decimal64`

Fixed-point decimal for financial prices. Only the mantissa is transmitted on the wire.

| Method | Signature | Description |
|--------|-----------|-------------|
| `fromFloat` | `fn fromFloat(val: f64, exp: i8) Decimal64` | Construct from float |
| `toFloat` | `fn toFloat(self) f64` | Convert to f64 |
| `isNull` | `fn isNull(self) bool` | Check null sentinel |
| `nullValue` | `fn nullValue() Decimal64` | Create null sentinel |

---

## FIX Messages

SBE-encoded FIX protocol messages in `src/codec/fix_messages.zig`.

### Enum Types

```zig
pub const Side = enum(u8) { buy = 1, sell = 2 };
pub const OrdType = enum(u8) { market = 1, limit = 2, stop = 3, stop_limit = 4 };
pub const TimeInForce = enum(u8) { day = 0, gtc = 1, ioc = 3, gtd = 6 };
pub const ExecType = enum(u8) { new = 0, fill = 1, partial_fill = 2, canceled = 4, rejected = 8 };
pub const OrdStatus = enum(u8) { new = 0, partially_filled = 1, filled = 2, canceled = 4, rejected = 8 };
pub const MDUpdateAction = enum(u8) { new = 0, change = 1, delete = 2 };
pub const MDEntryType = enum(u8) { bid = 0, offer = 1, trade = 2 };
```

### Fixed-Block Messages

| Message | Template ID | Block Size | Fields |
|---------|-------------|------------|--------|
| `NewOrderSingle` | 1 | 57 bytes | cl_ord_id, account, symbol, side, transact_time, order_qty, ord_type, price, stop_px, time_in_force |
| `ExecutionReport` | 2 | 89 bytes | order_id, cl_ord_id, exec_id, ord_status, exec_type, symbol, side, leaves_qty, cum_qty, avg_px, transact_time, text_len |
| `Heartbeat` | 5 | 16 bytes | test_req_id, timestamp_ns |
| `Logon` | 6 | 20 bytes | heart_bt_int, encrypt_method, reset_seq_num_flag, timestamp_ns |

### Group-Based Messages

| Message | Template ID | Description |
|---------|-------------|-------------|
| `MarketDataIncrementalRefresh` | 3 | MD entries group (action, type, symbol, price, size, etc.) |
| `MassQuote` | 4 | Quote sets group, each with nested quote entries |

Each group-based message provides an `encode()` method (returns `SbeEncoder` for streaming)
and a `decode()` method (returns `SbeDecoder` positioned after the root block).

---

## Wire Protocol Flyweights

Aeron-compatible flyweights in `src/protocol/flyweight.zig`. Each wraps a `[]u8` buffer.

### `DataHeaderFlyweight` (32 bytes)

| Method | Signature | Description |
|--------|-----------|-------------|
| `wrap` | `fn wrap(buf: []u8) DataHeaderFlyweight` | Wrap existing buffer |
| `init` | `fn init(buf: []u8) DataHeaderFlyweight` | Wrap and set type=DATA |
| `frameLength`/`setFrameLength` | `i32` | Total frame size |
| `flags`/`setFlags` | `u8` | BEGIN/END/EOS flags |
| `termOffset`/`setTermOffset` | `u32` | Offset in term |
| `sessionId`/`setSessionId` | `i32` | Session identifier |
| `streamId`/`setStreamId` | `i32` | Stream identifier |
| `termId`/`setTermId` | `i32` | Term identifier |
| `reservedValue`/`setReservedValue` | `i64` | User metadata |
| `payload` | `fn payload(self) []u8` | Payload region after header |
| `isBeginMessage`/`isEndMessage`/`isEndOfStream` | `bool` | Flag checks |

### `StatusMessageFlyweight` (36 bytes)

| Method | Signature | Description |
|--------|-----------|-------------|
| `sessionId`/`streamId` | `i32` | Identifiers |
| `consumptionTermId`/`consumptionTermOffset` | `i32` | Consumption position |
| `receiverWindowLength` | `i32` | Advertised window |
| `receiverId` | `i64` | Unique receiver ID |

### `NakFlyweight` (28 bytes)

| Method | Signature | Description |
|--------|-----------|-------------|
| `sessionId`/`streamId`/`termId` | `i32` | Identifiers |
| `termOffset` | `i32` | Start of missing range |
| `nakLength` | `i32` | Length of missing range |

### `SetupFlyweight` (40 bytes), `RttMeasurementFlyweight` (40 bytes), `ErrorFlyweight` (28+ bytes)

All follow the same pattern: `wrap(buf)`, `init(buf)`, typed getters/setters.

### Position Helpers

| Function | Signature | Description |
|----------|-----------|-------------|
| `computePosition` | `fn computePosition(term_offset, term_id, shift, initial_term_id) i64` | Absolute position from term addressing |
| `computeTermIdFromPosition` | `fn computeTermIdFromPosition(position, shift, initial_term_id) i32` | Term ID from position |
| `computeTermOffsetFromPosition` | `fn computeTermOffsetFromPosition(position, shift) i32` | Offset from position |

---

## Broadcast Buffer

### `BroadcastTransmitter`

Single-producer transmitter for 1-to-N messaging.

```zig
var buf: [1024 + TRAILER_LENGTH]u8 align(cache_line_size) = [_]u8{0} ** (1024 + TRAILER_LENGTH);
var tx = BroadcastTransmitter.init(&buf);
tx.transmit(42, "market data update");
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(buf: []u8) BroadcastTransmitter` | Initialize (capacity must be power of 2) |
| `transmit` | `fn transmit(self, msg_type_id: i32, msg: []const u8) void` | Transmit a message (always succeeds, old data overwritten) |
| `calculateMaxMessageLength` | `fn calculateMaxMessageLength(self) u32` | Max payload size: `(capacity / 8) - 8` |

### `BroadcastReceiver`

Per-consumer receiver. Each maintains its own cursor.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(buf: []const u8) BroadcastReceiver` | Join from current tail position |
| `receiveNext` | `fn receiveNext(self) ?Message` | Read next message, or null if none |
| `validate` | `fn validate(self) bool` | Check data not overwritten |
| `lappedCount` | `fn lappedCount(self) u64` | Times receiver was lapped |

### `CopyBroadcastReceiver`

Wrapper that copies payload to internal scratch buffer for safe retention.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(buf: []const u8) CopyBroadcastReceiver` | Initialize |
| `receiveNext` | `fn receiveNext(self) ?Message` | Receive with copy to scratch |
| `lappedCount` | `fn lappedCount(self) u64` | Times lapped |

---

## Idle Strategies

### `IdleStrategy`

Tagged union dispatching to concrete strategies via `idle(work_count)` and `reset()`.

```zig
var strategy = idle_strategy.backoff();
strategy.idle(0);  // no work -> back off
strategy.idle(1);  // work done -> reset to active
```

| Strategy | Latency | CPU | Description |
|----------|---------|-----|-------------|
| `BusySpinIdleStrategy` | Lowest | Highest | Hardware PAUSE instruction |
| `YieldingIdleStrategy` | Low | High | `Thread.yield()` |
| `SleepingIdleStrategy` | Medium | Low | `Thread.sleep(N)` |
| `BackoffIdleStrategy` | Adaptive | Adaptive | NOT_IDLE -> SPINNING -> YIELDING -> PARKING |
| `NoOpIdleStrategy` | N/A | N/A | Does nothing |

Convenience constructors: `busySpin()`, `yielding()`, `sleeping(ns)`, `backoff()`, `noOp()`.

---

## Agent Pattern

### `AgentFn`

Function-pointer-based agent interface for composable units of work.

```zig
pub const AgentFn = struct {
    doWorkFn: *const fn (ctx: *anyopaque) u32,   // returns work count
    onStartFn: ?*const fn (ctx: *anyopaque) void, // lifecycle start
    onCloseFn: ?*const fn (ctx: *anyopaque) void, // lifecycle close
    ctx: *anyopaque,
    name: []const u8,
};
```

### `AgentRunner`

Runs an agent on a dedicated thread with an idle strategy.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(agent: AgentFn, idle: IdleStrategy) AgentRunner` | Create runner |
| `start` | `fn start(self) !void` | Start agent on new thread |
| `stop` | `fn stop(self) void` | Stop agent and join thread |
| `isRunning` | `fn isRunning(self) bool` | Check if running |
| `errorCount` | `fn errorCount(self) u64` | Error counter |

### `CompositeAgent`

Combines multiple agents. Returns sum of work from all sub-agents.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(agents: []const AgentFn) CompositeAgent` | Create composite |
| `agentFn` | `fn agentFn(self) AgentFn` | Get AgentFn interface |

### `DutyCycleTracker`

Measures cycle performance for monitoring and tuning.

| Method | Signature | Description |
|--------|-----------|-------------|
| `cycleStart` | `fn cycleStart(self) void` | Record cycle start |
| `cycleEnd` | `fn cycleEnd(self, work_count: u32) void` | Record cycle end |
| `averageCycleNs` | `fn averageCycleNs(self) u64` | Recent cycle duration |
| `workRatio` | `fn workRatio(self) f64` | Ratio of busy vs idle cycles (0.0--1.0) |

---

## Counters

### `Counter`

Lightweight atomic i64 counter handle for hot-path instrumentation.

| Method | Signature | Description |
|--------|-----------|-------------|
| `increment` | `fn increment(self) void` | Atomic +1 (monotonic) |
| `incrementBy` | `fn incrementBy(self, n: i64) void` | Atomic +n |
| `decrement` | `fn decrement(self) void` | Atomic -1 |
| `get` | `fn get(self) i64` | Load (acquire) |
| `set` | `fn set(self, val: i64) void` | Store (release) |
| `getAndReset` | `fn getAndReset(self) i64` | Swap to 0 (acq_rel) |

### `CounterSet`

Fixed-capacity set of named atomic counters (max 64).

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init() CounterSet` | Zero-initialized set |
| `allocate` | `fn allocate(self, counter_type: CounterType, name: []const u8) ?Counter` | Allocate counter slot |
| `getByType` | `fn getByType(self, counter_type: CounterType) ?Counter` | Look up by type |
| `forEach` | `fn forEach(self, callback) void` | Iterate all active counters |
| `snapshot` | `fn snapshot(self, out: []CounterSnapshot) u32` | Copy all values |
| `resetAll` | `fn resetAll(self) void` | Reset all to zero |

### `GlobalCounters`

System-wide registry organized by subsystem (IPC, Network, Reliability, Archive, Cluster, Sequencer, System).

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init() GlobalCounters` | Empty counter sets |
| `initWithDefaults` | `fn initWithDefaults() GlobalCounters` | Pre-register all standard counters |
| `formatReport` | `fn formatReport(self, buf: []u8) []const u8` | Human-readable report |

---

## Congestion Control

### `CongestionControl`

AIMD congestion control with slow start and congestion avoidance phases.

```zig
var cc = CongestionControl.init(.{
    .initial_window = 64 * 1024,
    .max_window = 16 * 1024 * 1024,
    .min_window = 4 * 1024,
    .mss = 1460,
    .initial_ssthresh = 1024 * 1024,
});
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(cfg: CongestionConfig) CongestionControl` | Initialize |
| `onAck` | `fn onAck(self, bytes_acked: u64) void` | Window increase (slow start or CA) |
| `onLoss` | `fn onLoss(self) void` | Multiplicative decrease |
| `onTimeout` | `fn onTimeout(self) void` | Reset to min_window, re-enter slow start |
| `canSend` | `fn canSend(self, bytes: u64) bool` | Check window allows sending |
| `onSend` | `fn onSend(self, bytes: u64) void` | Record bytes in flight |
| `availableWindow` | `fn availableWindow(self) u64` | Bytes available in window |

### `RttEstimator`

RFC 6298 EWMA-based RTT estimation.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init() RttEstimator` | Initialize (1s initial RTO) |
| `update` | `fn update(self, rtt_ns: u64) void` | Record RTT sample |
| `retransmitTimeout` | `fn retransmitTimeout(self) u64` | Current RTO (ns) |
| `smoothedRtt` | `fn smoothedRtt(self) u64` | Current SRTT (ns) |

### `NakController`

Exponential backoff for NAK timing.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(config: NakConfig) NakController` | Initialize |
| `shouldSendNak` | `fn shouldSendNak(self, now_ns: u64) bool` | Check if enough time elapsed |
| `onNakSent` | `fn onNakSent(self, now_ns: u64) void` | Record NAK sent, increase backoff |
| `onGapFilled` | `fn onGapFilled(self) void` | Reset state for reuse |
| `isExhausted` | `fn isExhausted(self) bool` | Max retransmits exceeded |
| `currentDelay` | `fn currentDelay(self) u64` | Current delay with backoff (ns) |

---

## Flow Control

### `FlowControl`

Unified flow control dispatching to Min, Max, or Tagged strategy.

```zig
var fc = FlowControl.init(.{ .strategy = .min, .receiver_timeout_ns = 5_000_000_000 });
const new_limit = fc.onStatusMessage(status, sender_limit, initial_term_id, shift, now_ns);
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(cfg: FlowControlConfig) FlowControl` | Create from config |
| `onStatusMessage` | `fn onStatusMessage(self, status, sender_limit, initial_term_id, shift, now_ns) i64` | Process receiver status, return new sender limit |
| `onIdle` | `fn onIdle(self, now_ns, sender_limit, sender_position, is_eos) i64` | Remove stale receivers, return current limit |
| `hasRequiredReceivers` | `fn hasRequiredReceivers(self) bool` | Check for active receivers |

### `MinFlowControl`

Sender limit = minimum position across all active receivers. Guarantees no receiver left behind.

### `MaxFlowControl`

Sender always advances. No back-pressure. Suitable for market data where stale quotes are worthless.

### `TaggedFlowControl`

Only receivers matching `required_group_tag` constrain the sender. Untagged receivers are tracked but do not limit.

### `ReceiverStatus`

```zig
pub const ReceiverStatus = struct {
    session_id: i32,
    stream_id: i32,
    consumption_term_id: i32,
    consumption_term_offset: i32,
    receiver_window_length: i32,
    receiver_id: i64,
    timestamp_ns: u64,
};
```

---

## Archive Catalog

### `Catalog`

Tracks segment metadata with time-range and stream queries.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, base_path: []const u8) !Catalog` | Initialize |
| `deinit` | `fn deinit(self: *Catalog) void` | Free entries |
| `addEntry` | `fn addEntry(self, entry: CatalogEntry) !void` | Add segment metadata |
| `updateEntry` | `fn updateEntry(self, segment_id: u32, entry: CatalogEntry) !void` | Update existing |
| `getEntry` | `fn getEntry(self, segment_id: u32) ?CatalogEntry` | Look up by ID |
| `findByTimestamp` | `fn findByTimestamp(self, from_ns: u64, to_ns: u64) []const CatalogEntry` | Time range query |
| `findByStream` | `fn findByStream(self, stream_id: u32) ![]CatalogEntry` | Stream filter (caller frees) |
| `save` | `fn save(self) !void` | Persist to disk |
| `load` | `fn load(allocator: Allocator, path: []const u8) !Catalog` | Load from disk |
| `totalRecords` | `fn totalRecords(self) u64` | Sum of record counts |
| `totalBytes` | `fn totalBytes(self) u64` | Sum of payload bytes |
| `segmentCount` | `fn segmentCount(self) u32` | Number of segments |

**CatalogEntry** (56 bytes serialized):
```zig
pub const CatalogEntry = struct {
    segment_id: u32,
    start_offset: u64,
    end_offset: u64,
    start_timestamp_ns: u64,
    end_timestamp_ns: u64,
    stream_id: u32,
    record_count: u32,
    total_bytes: u64,
    closed: bool,
};
```

---

## Archive Index

### `SparseIndex`

Indexes every Nth record for fast binary-search lookup within segments.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator, segment_id: u32, interval: u32) SparseIndex` | Initialize |
| `deinit` | `fn deinit(self: *SparseIndex) void` | Free entries |
| `record` | `fn record(self, seq: u32, offset: u64, timestamp_ns: u64, stream_id: u32) !void` | Record an entry (indexes every Nth) |
| `findByTimestamp` | `fn findByTimestamp(self, timestamp_ns: u64) ?IndexEntry` | Binary search by timestamp |
| `findBySequence` | `fn findBySequence(self, record_seq: u32) ?IndexEntry` | Binary search by sequence |
| `save` | `fn save(self, base_path: []const u8) !void` | Save to disk |
| `load` | `fn load(allocator: Allocator, base_path: []const u8, segment_id: u32) !SparseIndex` | Load from disk |
| `rebuild` | `fn rebuild(allocator, segment_file, segment_id, interval) !SparseIndex` | Rebuild by scanning segment |

**IndexEntry** (24 bytes serialized):
```zig
pub const IndexEntry = struct {
    record_seq: u32,
    file_offset: u64,
    timestamp_ns: u64,
    stream_id: u32,
};
```

---

## Compression

### `Compressor`

LZ4-style compression with hash-table-based matching. 64 KB sliding window.

| Method | Signature | Description |
|--------|-----------|-------------|
| `init` | `fn init(allocator: Allocator) !Compressor` | Allocate hash table |
| `deinit` | `fn deinit(self, allocator: Allocator) void` | Free hash table |
| `compress` | `fn compress(self, src: []const u8, dst: []u8) !usize` | Compress into buffer, returns bytes written |
| `maxCompressedSize` | `fn maxCompressedSize(input_size: usize) usize` | Worst-case output size |

### `Decompressor`

| Method | Signature | Description |
|--------|-----------|-------------|
| `decompress` | `fn decompress(src: []const u8, dst: []u8) !usize` | Decompress, returns bytes written |

### Frame API

| Function | Signature | Description |
|----------|-----------|-------------|
| `compressFrame` | `fn compressFrame(allocator, src: []const u8) ![]u8` | Compress with 16-byte header + CRC32 |
| `decompressFrame` | `fn decompressFrame(allocator, frame_data: []const u8) ![]u8` | Decompress and validate checksum |

**CompressedFrame** (16-byte header):
```zig
pub const CompressedFrame = struct {
    magic: u32,           // 0x5A424C5A ("ZBLZ")
    original_size: u32,
    compressed_size: u32,
    checksum: u32,        // CRC32 of original data
};
```

---

## FFI Exports

C-ABI functions exported from `src/ffi/exports.zig`:

| Function | Signature | Description |
|----------|-----------|-------------|
| `zigbolt_transport_create` | `(term_length: u32, use_hugepages: u8, pre_fault: u8) ?*anyopaque` | Create transport |
| `zigbolt_transport_destroy` | `(handle: ?*anyopaque) void` | Destroy transport |
| `zigbolt_ipc_create` | `(name: ?[*:0]const u8, term_length: u32) ?*anyopaque` | Create IPC channel |
| `zigbolt_ipc_open` | `(name: ?[*:0]const u8, term_length: u32) ?*anyopaque` | Open IPC channel |
| `zigbolt_ipc_destroy` | `(handle: ?*anyopaque) void` | Destroy IPC channel |
| `zigbolt_publish` | `(handle: ?*anyopaque, data: ?[*]const u8, len: u32, msg_type_id: i32) i32` | Publish (0=success) |
| `zigbolt_poll` | `(handle: ?*anyopaque, callback: ?FragmentHandlerFn, limit: u32) u32` | Poll messages |
| `zigbolt_version_major` | `() u32` | Major version (0) |
| `zigbolt_version_minor` | `() u32` | Minor version (1) |
| `zigbolt_version_patch` | `() u32` | Patch version (0) |
