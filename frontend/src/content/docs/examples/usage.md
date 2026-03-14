---
title: Usage Examples
description: Practical examples for every ZigBolt subsystem
---

Practical examples showing how to use each major ZigBolt subsystem.

---

## Table of Contents

- [IPC Publisher / Subscriber](#ipc-publisher--subscriber)
- [Market Data Streaming](#market-data-streaming)
- [UDP Networking](#udp-networking)
- [Wire Codec Usage](#wire-codec-usage)
- [Archive Record / Replay](#archive-record--replay)
- [Raft Cluster](#raft-cluster)
- [Sequencer (Total Ordering)](#sequencer-total-ordering)
- [Transport API (High-Level)](#transport-api-high-level)
- [Raw Publisher / Subscriber](#raw-publisher--subscriber)

---

## IPC Publisher / Subscriber

The lowest-latency path: shared memory IPC between processes.

### Publisher Process

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

const TickMessage = zigbolt.TickMessage;

pub fn main() !void {
    // Create a shared memory IPC channel
    var channel = try zigbolt.IpcChannel.create("/market-feed", .{
        .term_length = 1 << 20,  // 1 MB term buffers
        .pre_fault = true,        // pre-fault pages for deterministic latency
    });
    defer channel.deinit();

    // Create a typed publisher
    var publisher = zigbolt.Publisher(TickMessage).init(&channel, 1);

    // Publish market ticks
    var i: u64 = 0;
    while (i < 1_000_000) : (i += 1) {
        const tick = TickMessage{
            .timestamp_ns = zigbolt.timestampNs(),
            .symbol_id = 42,
            .price = 15025_00 + @as(i64, @intCast(i % 100)),
            .volume = 100,
            .side = if (i % 2 == 0) .bid else .ask,
        };

        try publisher.offer(&tick);
    }
}
```

### Subscriber Process

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

const TickMessage = zigbolt.TickMessage;

var tick_count: u64 = 0;

fn handleTick(msg: *const TickMessage) void {
    tick_count += 1;
    if (tick_count % 100_000 == 0) {
        const latency = zigbolt.timestampNs() - msg.timestamp_ns;
        std.debug.print("Tick #{d}: symbol={d} price={d} latency={d}ns\n", .{
            tick_count, msg.symbol_id, msg.price, latency,
        });
    }
}

pub fn main() !void {
    // Open the existing shared memory channel
    var channel = try zigbolt.IpcChannel.open("/market-feed", .{
        .term_length = 1 << 20,
    });
    defer channel.deinit();

    // Create a typed subscriber
    var subscriber = zigbolt.Subscriber(TickMessage).init(&channel, 1);

    // Poll loop
    while (true) {
        const count = subscriber.poll(&handleTick, 256);
        if (count == 0) {
            // No messages -- could yield, spin, or check a shutdown flag
            std.atomic.spinLoopHint();
        }
    }
}
```

---

## Market Data Streaming

Combining the wire codec with IPC for a realistic market data feed.

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

// Define a custom market data message
const L2Update = packed struct {
    timestamp_ns: u64,
    symbol_id: u32,
    bid_price: i64,
    ask_price: i64,
    bid_size: u32,
    ask_size: u32,
    _padding: u32 = 0,
    // Total: 40 bytes (multiple of 8)
};

const L2Codec = zigbolt.WireCodec(L2Update);

pub fn main() !void {
    var channel = try zigbolt.IpcChannel.create("/l2-feed", .{
        .term_length = 4 << 20,  // 4 MB for high-throughput
        .pre_fault = true,
    });
    defer channel.deinit();

    // Encode and publish directly
    const update = L2Update{
        .timestamp_ns = zigbolt.timestampNs(),
        .symbol_id = 1001,
        .bid_price = 50000_00,
        .ask_price = 50001_00,
        .bid_size = 500,
        .ask_size = 300,
    };

    var buf: [L2Codec.wire_size]u8 = undefined;
    L2Codec.encode(&update, &buf);
    try channel.publish(&buf, 2);  // msg_type_id = 2 for L2 updates

    // Batch encoding for burst scenarios
    var updates: [64]L2Update = undefined;
    for (&updates, 0..) |*u, i| {
        u.* = .{
            .timestamp_ns = zigbolt.timestampNs(),
            .symbol_id = @intCast(i),
            .bid_price = 10000_00,
            .ask_price = 10001_00,
            .bid_size = 100,
            .ask_size = 100,
        };
    }
    var batch_buf: [L2Codec.wire_size * 64]u8 = undefined;
    const encoded = L2Codec.batchEncode(&updates, &batch_buf);
    std.debug.print("Batch-encoded {d} L2 updates\n", .{encoded});
}
```

---

## UDP Networking

### Unicast Send/Receive

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

pub fn main() !void {
    // Sender: bind to an ephemeral port, target a known address
    var sender = try zigbolt.UdpChannel.init(.{
        .bind_address = try std.net.Address.parseIp4("0.0.0.0", 0),
        .remote_address = try std.net.Address.parseIp4("192.168.1.100", 9000),
        .non_blocking = true,
    });
    defer sender.deinit();

    // Send a raw datagram
    _ = try sender.send("hello network", null);

    // Send a framed message (FrameHeader + payload)
    try sender.sendFrame("order-new", 42, null);
}
```

### Multicast Group

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

pub fn main() !void {
    // Join a multicast group for market data distribution
    var receiver = try zigbolt.UdpChannel.init(.{
        .bind_address = try std.net.Address.parseIp4("0.0.0.0", 9000),
        .multicast_group = .{ 239, 1, 1, 1 },  // 239.1.1.1
        .recv_buffer_size = 8 * 1024 * 1024,     // 8 MB receive buffer
        .non_blocking = true,
    });
    defer receiver.deinit();

    var buf: [65536]u8 = undefined;

    // Poll for multicast datagrams
    while (true) {
        if (try receiver.recvFrame(&buf)) |result| {
            std.debug.print("Received type={d} len={d} from {}\n", .{
                result.msg_type_id,
                result.payload.len,
                result.from,
            });
        }
    }
}
```

### Reliable Network Channel

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

pub fn main() !void {
    var allocator = std.heap.page_allocator;

    // Create a reliable network channel with NAK-based retransmission
    var net_ch = try zigbolt.NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = try std.net.Address.parseIp4("0.0.0.0", 9000),
            .remote_address = try std.net.Address.parseIp4("192.168.1.100", 9000),
            .non_blocking = true,
        },
        .session_id = 1,
        .stream_id = 1,
        .flow_control_window = 8 * 1024 * 1024,  // 8 MB
        .mtu = 1472,
    });
    defer net_ch.deinit();

    // Publish -- handles fragmentation, reliability, flow control
    try net_ch.publish("important-order", 1);

    // Poll -- handles reassembly, NAK generation
    const count = try net_ch.poll(&handleMessage, 100);
    std.debug.print("Received {d} messages\n", .{count});
}

fn handleMessage(data: []const u8) void {
    std.debug.print("Message: {s}\n", .{data});
}
```

---

## Wire Codec Usage

### Defining Custom Messages

```zig
const zigbolt = @import("zigbolt");

// All fields must be fixed-size. No pointers, no slices.
// Total size must be a multiple of 8 bytes.
const TradeExecution = packed struct {
    timestamp_ns: u64,
    trade_id: u64,
    order_id: u64,
    symbol_id: u32,
    price: i64,
    quantity: u32,
    side: enum(u8) { buy = 0, sell = 1 },
    aggressor: enum(u8) { maker = 0, taker = 1 },
    _padding: u16 = 0,
    // Total: 48 bytes
};

const TradeCodec = zigbolt.WireCodec(TradeExecution);
// TradeCodec.wire_size == 48
```

### Encode / Decode

```zig
const zigbolt = @import("zigbolt");
const std = @import("std");

const TickMessage = zigbolt.TickMessage;
const Codec = zigbolt.WireCodec(TickMessage);

pub fn main() void {
    // Encode
    const tick = TickMessage{
        .timestamp_ns = 1_700_000_000_000_000_000,
        .symbol_id = 42,
        .price = 15025_00,
        .volume = 1000,
        .side = .ask,
    };

    var buf: [Codec.wire_size]u8 = undefined;
    Codec.encode(&tick, &buf);

    // Zero-copy decode: pointer directly into buf
    const decoded = Codec.decode(&buf);
    std.debug.print("Price: {d}\n", .{decoded.price});

    // Mutable decode: modify in-place
    const mut = Codec.decodeMut(&buf);
    mut.volume = 2000;

    // Verify mutation is visible through the buffer
    const check = Codec.decode(&buf);
    std.debug.assert(check.volume == 2000);
}
```

### Batch Operations

```zig
const zigbolt = @import("zigbolt");

const OrderMessage = zigbolt.OrderMessage;
const Codec = zigbolt.WireCodec(OrderMessage);

pub fn processBatch(wire_data: []const u8) void {
    var orders: [128]OrderMessage = undefined;

    // Decode up to 128 orders from a contiguous buffer
    const count = Codec.batchDecode(wire_data, &orders);

    for (orders[0..count]) |order| {
        // Process each order...
        _ = order.price;
    }
}

pub fn encodeBatch(orders: []const OrderMessage, out: []u8) u32 {
    return Codec.batchEncode(orders, out);
}
```

---

## Archive Record / Replay

### Recording Messages

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

pub fn main() !void {
    var archive = try zigbolt.archive.Archive.init(std.heap.page_allocator, .{
        .base_path = "/data/zigbolt/market-archive",
        .segment_size = 256 * 1024 * 1024,  // 256 MB segments
        .sync_policy = .periodic,
        .sync_interval_ms = 1000,
    });
    defer archive.deinit();

    // Record market data events
    try archive.record(
        1,                          // stream_id (e.g., 1 = equities)
        100,                        // msg_type_id
        "AAPL,150.25,1000,BID",    // payload
        zigbolt.timestampNs(),      // timestamp
    );

    try archive.record(
        2,                          // stream_id (e.g., 2 = futures)
        101,
        "ES,4500.50,10,ASK",
        zigbolt.timestampNs(),
    );

    // Check stats
    const s = archive.stats();
    std.debug.print("Records: {d}, Bytes: {d}, Segments: {d}\n", .{
        s.total_records, s.total_bytes, s.segment_count,
    });
}
```

### Replaying Messages

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

fn replayHandler(rec: zigbolt.archive.core.Record) void {
    std.debug.print("[{d}] stream={d} type={d}: {s}\n", .{
        rec.timestamp_ns,
        rec.stream_id,
        rec.msg_type_id,
        rec.payload,
    });
}

pub fn main() !void {
    var archive = try zigbolt.archive.Archive.init(std.heap.page_allocator, .{
        .base_path = "/data/zigbolt/market-archive",
    });
    defer archive.deinit();

    // Replay all messages
    const total = try archive.replay(.{}, &replayHandler);
    std.debug.print("Replayed {d} messages\n", .{total});

    // Replay only stream 1, starting from segment 0, limit 1000
    const filtered = try archive.replay(.{
        .stream_id = 1,
        .from_segment = 0,
        .from_offset = 0,
        .limit = 1000,
    }, &replayHandler);
    std.debug.print("Replayed {d} filtered messages\n", .{filtered});
}
```

---

## Raft Cluster

### Setting Up a 3-Node Cluster

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

const cluster = zigbolt.cluster;

var applied_commands: u32 = 0;

fn applyCommand(entry: []const u8) void {
    std.debug.print("Applying: {s}\n", .{entry});
    applied_commands += 1;
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Create a 3-node cluster (node 0)
    const sm = cluster.StateMachine{
        .apply_fn = &applyCommand,
    };

    var node = try cluster.Cluster.init(allocator, .{
        .node_id = 0,
        .peer_count = 2,   // 2 peers (3 nodes total)
        .election_timeout_min_ms = 150,
        .election_timeout_max_ms = 300,
        .heartbeat_interval_ms = 50,
    }, sm);
    defer node.deinit();

    std.debug.print("Node state: {}\n", .{node.getState()});
    std.debug.print("Is leader: {}\n", .{node.isLeader()});
}
```

### Leader Election and Log Replication

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

const cluster = zigbolt.cluster;

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    // Create 3 nodes
    var nodes: [3]cluster.Cluster = undefined;
    for (&nodes, 0..) |*n, i| {
        n.* = try cluster.Cluster.init(allocator, .{
            .node_id = @intCast(i),
            .peer_count = 2,
        }, null);
    }
    defer for (&nodes) |*n| n.deinit();

    // Node 0 starts an election
    const vote_request = nodes[0].node.node.startElection();

    // Nodes 1 and 2 receive the vote request and respond
    for (1..3) |i| {
        const response = nodes[i].handleMessage(0, vote_request);
        if (response) |resp| {
            // Send response back to node 0
            _ = nodes[0].handleMessage(@intCast(i), resp.msg);
        }
    }

    std.debug.print("Node 0 is leader: {}\n", .{nodes[0].isLeader()});

    // Leader proposes a command
    if (nodes[0].isLeader()) {
        const idx = try nodes[0].propose("set key=value");
        std.debug.print("Proposed at log index: {d}\n", .{idx});
    }

    // Simulate replication acknowledgment
    nodes[0].node.node.match_index[0] = 1;
    nodes[0].node.node.match_index[1] = 1;
    nodes[0].node.node.updateCommitIndex();

    // Tick to apply committed entries
    nodes[0].tick();
}
```

---

## Sequencer (Total Ordering)

### Single Sequencer

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

pub fn main() void {
    var seq = zigbolt.Sequencer.init(.{
        .initial_sequence = 0,
    });

    // Events from different streams all get global ordering
    const e1 = seq.sequence(0, "order-new");      // seq=0
    const e2 = seq.sequence(1, "market-data");     // seq=1
    const e3 = seq.sequence(0, "order-cancel");    // seq=2

    std.debug.print("Order: {d} -> {d} -> {d}\n", .{
        e1.sequence, e2.sequence, e3.sequence,
    });

    // Peek at next sequence without consuming
    std.debug.print("Next sequence: {d}\n", .{seq.peekNextSequence()});
}
```

### Multi-Stream Sequencer

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

pub fn main() void {
    var ms = zigbolt.MultiStreamSequencer.init(.{
        .initial_sequence = 0,
        .max_streams = 64,
    });

    // Stream 0: Order gateway
    _ = ms.sequenceFrom(0, "order-new-1");
    _ = ms.sequenceFrom(0, "order-new-2");

    // Stream 1: Market data
    _ = ms.sequenceFrom(1, "md-tick-1");

    // Stream 2: Risk engine
    _ = ms.sequenceFrom(2, "risk-check-1");

    // Per-stream statistics
    const order_stats = ms.getStreamStats(0);
    std.debug.print("Order stream: {d} events, last_seq={d}\n", .{
        order_stats.events_sequenced, order_stats.last_sequence,
    });

    std.debug.print("Total events: {d}, Active streams: {d}\n", .{
        ms.totalEvents(), ms.active_streams,
    });
}
```

### Sequence Index for Replay

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

pub fn main() !void {
    var index = zigbolt.SequenceIndex.init(std.heap.page_allocator);
    defer index.deinit();

    // Build index as events are sequenced
    try index.add(.{ .sequence = 0, .stream_id = 0, .timestamp_ns = 1000 });
    try index.add(.{ .sequence = 1, .stream_id = 1, .timestamp_ns = 2000 });
    try index.add(.{ .sequence = 2, .stream_id = 0, .timestamp_ns = 3000 });
    try index.add(.{ .sequence = 3, .stream_id = 2, .timestamp_ns = 4000 });

    // Look up a specific sequence
    if (index.lookup(2)) |entry| {
        std.debug.print("Seq 2: stream={d} ts={d}\n", .{
            entry.stream_id, entry.timestamp_ns,
        });
    }

    // Range query: all events from sequence 1 onward
    const range = index.rangeFrom(1);
    std.debug.print("Events from seq 1: {d} entries\n", .{range.len});
}
```

---

## Transport API (High-Level)

The Transport provides a managed, high-level interface.

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

const TickMessage = zigbolt.TickMessage;

pub fn main() !void {
    var transport = zigbolt.Transport.init(std.heap.page_allocator, .{
        .term_length = 1 << 20,
        .use_hugepages = false,
        .pre_fault = true,
    });
    defer transport.deinit();

    // Create a typed publisher (channel created automatically)
    var publisher = try transport.addPublication(
        TickMessage,
        "/prices",
        1,  // msg_type_id
    );

    // Create a raw publisher for generic data
    var raw_pub = try transport.addRawPublication("/events", 2);

    // Publish typed
    const tick = TickMessage{
        .timestamp_ns = zigbolt.timestampNs(),
        .symbol_id = 42,
        .price = 15025_00,
        .volume = 100,
        .side = .bid,
    };
    try publisher.offer(&tick);

    // Publish raw
    try raw_pub.offer("raw event data");
}
```

---

## Raw Publisher / Subscriber

For cases where you want to manage encoding yourself.

```zig
const std = @import("std");
const zigbolt = @import("zigbolt");

fn handleRaw(result: zigbolt.IpcChannel.ReadResult) void {
    std.debug.print("Received: type={d} len={d}\n", .{
        result.msg_type_id, result.data.len,
    });
}

pub fn main() !void {
    // Publisher
    var pub_ch = try zigbolt.IpcChannel.create("/raw-channel", .{
        .term_length = 1 << 20,
    });
    defer pub_ch.deinit();

    var publisher = zigbolt.RawPublisher.init(&pub_ch, 99);
    try publisher.offer("arbitrary bytes here");

    // Subscriber
    var subscriber = zigbolt.RawSubscriber.init(&pub_ch);
    _ = subscriber.poll(&handleRaw, 100);
}
```
