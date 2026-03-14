# ZigBolt API Reference

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
