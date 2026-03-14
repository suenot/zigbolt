//! ZigBolt — Ultra-Low Latency Messaging System for HFT
//!
//! A high-performance messaging library designed as a direct competitor
//! to Aeron (Real Logic / Adaptive) for high-frequency trading.
//!
//! Key features:
//! - Zero-runtime-overhead (no GC, no JVM safepoints)
//! - Comptime-generated wire codecs (replaces SBE)
//! - Lock-free SPSC/MPSC ring buffers
//! - Shared memory IPC with sub-microsecond latency
//! - Native io_uring / DPDK / AF_XDP support (Linux)

// ── Platform ─────────────────────────────────────────────────
pub const platform = struct {
    pub const config = @import("platform/config.zig");
    pub const memory = @import("platform/memory.zig");
};

// ── Core Data Structures ─────────────────────────────────────
pub const SpscRingBuffer = @import("core/spsc.zig").SpscRingBuffer;
pub const MpscRingBuffer = @import("core/mpsc.zig").MpscRingBuffer;
pub const LogBuffer = @import("core/log_buffer.zig").LogBuffer;
pub const LogBufferConfig = @import("core/log_buffer.zig").LogBufferConfig;
pub const FrameHeader = @import("core/frame.zig").FrameHeader;
pub const frame = @import("core/frame.zig");
pub const broadcast = @import("core/broadcast.zig");
pub const BroadcastTransmitter = broadcast.BroadcastTransmitter;
pub const BroadcastReceiver = broadcast.BroadcastReceiver;
pub const CopyBroadcastReceiver = broadcast.CopyBroadcastReceiver;
pub const counters = @import("core/counters.zig");
pub const CounterSet = counters.CounterSet;
pub const CounterType = counters.CounterType;
pub const GlobalCounters = counters.GlobalCounters;
pub const idle_strategy = @import("core/idle_strategy.zig");
pub const IdleStrategy = idle_strategy.IdleStrategy;
pub const agent = @import("core/agent.zig");
pub const AgentFn = agent.AgentFn;
pub const AgentRunner = agent.AgentRunner;
pub const CompositeAgent = agent.CompositeAgent;
pub const DutyCycleTracker = agent.DutyCycleTracker;

// ── Wire Codec ───────────────────────────────────────────────
pub const WireCodec = @import("codec/wire.zig").WireCodec;
pub const TickMessage = @import("codec/wire.zig").TickMessage;
pub const OrderMessage = @import("codec/wire.zig").OrderMessage;

// ── SBE Codec ────────────────────────────────────────────────
pub const sbe = @import("codec/sbe.zig");
pub const fix = @import("codec/fix_messages.zig");

// ── IPC Channel ──────────────────────────────────────────────
pub const IpcChannel = @import("channel/ipc.zig").IpcChannel;
pub const IpcConfig = @import("channel/ipc.zig").IpcConfig;

// ── UDP Channel ─────────────────────────────────────────────
pub const UdpChannel = @import("channel/udp.zig").UdpChannel;
pub const UdpConfig = @import("channel/udp.zig").UdpConfig;
pub const NetworkChannel = @import("channel/network.zig").NetworkChannel;
pub const NetworkConfig = @import("channel/network.zig").NetworkConfig;

// ── Reliability Protocol ─────────────────────────────────────
pub const reliability = @import("channel/reliability.zig");
pub const NetworkHeader = reliability.NetworkHeader;
pub const NakMessage = reliability.NakMessage;
pub const SendBuffer = reliability.SendBuffer;
pub const RecvTracker = reliability.RecvTracker;
pub const FlowControl = reliability.FlowControl;

// ── Aeron Flow Control ──────────────────────────────────────
pub const aeron_flow_control = @import("channel/flow_control.zig");
pub const AeronFlowControl = aeron_flow_control.FlowControl;
pub const MinFlowControl = aeron_flow_control.MinFlowControl;
pub const MaxFlowControl = aeron_flow_control.MaxFlowControl;
pub const TaggedFlowControl = aeron_flow_control.TaggedFlowControl;
pub const ReceiverStatus = aeron_flow_control.ReceiverStatus;
pub const FlowControlConfig = aeron_flow_control.FlowControlConfig;

// ── Congestion Control ─────────────────────────────────────
pub const congestion = @import("channel/congestion.zig");
pub const CongestionControl = congestion.CongestionControl;
pub const CongestionConfig = congestion.CongestionConfig;
pub const RttEstimator = congestion.RttEstimator;
pub const NakController = congestion.NakController;

// ── Fragment Layer ───────────────────────────────────────────
pub const fragment = @import("channel/fragment.zig");
pub const Fragmenter = fragment.Fragmenter;
pub const Reassembler = fragment.Reassembler;
pub const FragmentConfig = fragment.FragmentConfig;
pub const FragmentHeader_ = fragment.FragmentHeader;

// ── Publisher / Subscriber API ───────────────────────────────
pub const Publisher = @import("api/publisher.zig").Publisher;
pub const RawPublisher = @import("api/publisher.zig").RawPublisher;
pub const Subscriber = @import("api/subscriber.zig").Subscriber;
pub const RawSubscriber = @import("api/subscriber.zig").RawSubscriber;
pub const Transport = @import("api/transport.zig").Transport;
pub const TransportConfig = @import("api/transport.zig").TransportConfig;

// ── Archive ──────────────────────────────────────────────────
pub const archive = struct {
    pub const segment = @import("archive/segment.zig");
    pub const core = @import("archive/archive.zig");
    pub const catalog = @import("archive/catalog.zig");
    pub const index = @import("archive/index.zig");
    pub const Segment = segment.Segment;
    pub const SegmentManager = segment.SegmentManager;
    pub const Archive = core.Archive;
    pub const ArchiveConfig = core.ArchiveConfig;
    pub const Catalog = catalog.Catalog;
    pub const CatalogEntry = catalog.CatalogEntry;
    pub const SparseIndex = index.SparseIndex;
    pub const IndexEntry = index.IndexEntry;
    pub const compression = @import("archive/compression.zig");
    pub const Compressor = compression.Compressor;
    pub const Decompressor = compression.Decompressor;
};

// ── Sequencer (Total Ordering) ───────────────────────────────
pub const sequencer_mod = @import("sequencer/sequencer.zig");
pub const Sequencer = sequencer_mod.Sequencer;
pub const MultiStreamSequencer = sequencer_mod.MultiStreamSequencer;
pub const SequenceIndex = sequencer_mod.SequenceIndex;
pub const SequencedEvent = sequencer_mod.SequencedEvent;

// ── Cluster (Raft Consensus) ────────────────────────────────
pub const cluster = struct {
    pub const RaftLog = @import("cluster/raft_log.zig").RaftLog;
    pub const LogEntry = @import("cluster/raft_log.zig").LogEntry;
    pub const RaftNode = @import("cluster/raft.zig").RaftNode;
    pub const RaftConfig = @import("cluster/raft.zig").RaftConfig;
    pub const RaftMessage = @import("cluster/raft.zig").RaftMessage;
    pub const NodeState = @import("cluster/raft.zig").NodeState;
    pub const Cluster = @import("cluster/cluster.zig").Cluster;
    pub const ClusterConfig = @import("cluster/cluster.zig").ClusterConfig;
    pub const StateMachine = @import("cluster/cluster.zig").StateMachine;
    pub const WriteAheadLog = @import("cluster/wal.zig").WriteAheadLog;
    pub const WalEntry = @import("cluster/wal.zig").WalEntry;
    pub const VoteState = @import("cluster/wal.zig").VoteState;
    pub const snapshot = @import("cluster/snapshot.zig");
    pub const SnapshotManager = snapshot.SnapshotManager;
    pub const SnapshotData = snapshot.SnapshotData;
};

// ── Protocol (Aeron-compatible wire flyweights) ──────────────
pub const protocol = struct {
    pub const flyweight = @import("protocol/flyweight.zig");
    pub const HeaderFlyweight = flyweight.HeaderFlyweight;
    pub const DataHeaderFlyweight = flyweight.DataHeaderFlyweight;
    pub const StatusMessageFlyweight = flyweight.StatusMessageFlyweight;
    pub const NakFlyweight = flyweight.NakFlyweight;
    pub const SetupFlyweight = flyweight.SetupFlyweight;
    pub const RttMeasurementFlyweight = flyweight.RttMeasurementFlyweight;
    pub const ErrorFlyweight = flyweight.ErrorFlyweight;
    pub const FrameType = flyweight.FrameType;
    pub const ErrorCode = flyweight.ErrorCode;
};

// ── Re-export key types for convenience ──────────────────────
pub const SharedRegion = platform.memory.SharedRegion;
pub const MemoryConfig = platform.memory.MemoryConfig;
pub const cache_line_size = platform.config.cache_line_size;
pub const timestampNs = platform.config.timestampNs;

// ── Version Info ─────────────────────────────────────────────
pub const version_major: u32 = 0;
pub const version_minor: u32 = 2;
pub const version_patch: u32 = 0;

// ── Tests ────────────────────────────────────────────────────
test {
    @import("std").testing.refAllDecls(@This());
}

test "version constants" {
    const testing = @import("std").testing;
    try testing.expectEqual(@as(u32, 0), version_major);
    try testing.expectEqual(@as(u32, 2), version_minor);
    try testing.expectEqual(@as(u32, 0), version_patch);
}
