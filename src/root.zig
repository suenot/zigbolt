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

// ── Wire Codec ───────────────────────────────────────────────
pub const WireCodec = @import("codec/wire.zig").WireCodec;
pub const TickMessage = @import("codec/wire.zig").TickMessage;
pub const OrderMessage = @import("codec/wire.zig").OrderMessage;

// ── IPC Channel ──────────────────────────────────────────────
pub const IpcChannel = @import("channel/ipc.zig").IpcChannel;
pub const IpcConfig = @import("channel/ipc.zig").IpcConfig;

// ── Publisher / Subscriber API ───────────────────────────────
pub const Publisher = @import("api/publisher.zig").Publisher;
pub const RawPublisher = @import("api/publisher.zig").RawPublisher;
pub const Subscriber = @import("api/subscriber.zig").Subscriber;
pub const RawSubscriber = @import("api/subscriber.zig").RawSubscriber;
pub const Transport = @import("api/transport.zig").Transport;
pub const TransportConfig = @import("api/transport.zig").TransportConfig;

// ── Re-export key types for convenience ──────────────────────
pub const SharedRegion = platform.memory.SharedRegion;
pub const MemoryConfig = platform.memory.MemoryConfig;
pub const cache_line_size = platform.config.cache_line_size;
pub const timestampNs = platform.config.timestampNs;

// ── Tests ────────────────────────────────────────────────────
test {
    @import("std").testing.refAllDecls(@This());
}
