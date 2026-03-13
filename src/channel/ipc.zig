const std = @import("std");
const config = @import("../platform/config.zig");
const memory = @import("../platform/memory.zig");
const frame = @import("../core/frame.zig");

/// IPC Channel configuration.
pub const IpcConfig = struct {
    /// Length of each term buffer in bytes (must be power of 2).
    term_length: usize = config.default_term_length,
    /// Use hugepages if available (Linux only).
    use_hugepages: bool = false,
    /// Pre-fault all pages on creation.
    pre_fault: bool = true,
};

/// Shared memory layout:
///
///   [Metadata (4KB, cache-line padded)]
///   [Term 0 (term_length bytes)]
///   [Term 1 (term_length bytes)]
///   [Term 2 (term_length bytes)]
const METADATA_SIZE: usize = 4096;

/// Metadata header stored at the start of shared memory.
const Metadata = extern struct {
    /// Magic number for validation.
    magic: u64 align(config.cache_line_size) = MAGIC,
    /// Version of the IPC protocol.
    version: u32 = 1,
    /// Term length in bytes.
    term_length: u32 = 0,

    /// Publisher tail position (absolute byte offset across all terms).
    tail_position: std.atomic.Value(u64) align(config.cache_line_size) = std.atomic.Value(u64).init(0),

    /// Subscriber head position (absolute byte offset, how far consumer has read).
    head_position: std.atomic.Value(u64) align(config.cache_line_size) = std.atomic.Value(u64).init(0),
};

const MAGIC: u64 = 0x5A49_4742_4F4C_5421; // "ZIGBOLT!"
const TERM_COUNT: usize = 3;

/// IPC Channel over shared memory.
/// Supports single-publisher / single-subscriber (SPSC) communication
/// with sub-microsecond latency.
pub const IpcChannel = struct {
    region: memory.SharedRegion,
    meta: *Metadata,
    term_base: [*]u8,
    term_length: usize,

    /// Create a new IPC channel (publisher side).
    pub fn create(name: [*:0]const u8, ipc_config: IpcConfig) !IpcChannel {
        const tl = ipc_config.term_length;
        const total_size = METADATA_SIZE + tl * TERM_COUNT;

        const region = try memory.createShared(name, total_size, .{
            .use_hugepages = ipc_config.use_hugepages,
            .pre_fault = ipc_config.pre_fault,
        });

        // Initialize metadata
        const meta: *Metadata = @ptrCast(@alignCast(region.base));
        meta.* = .{
            .term_length = @intCast(tl),
        };

        if (ipc_config.pre_fault) {
            memory.prefault(region);
        }

        return .{
            .region = region,
            .meta = meta,
            .term_base = region.base + METADATA_SIZE,
            .term_length = tl,
        };
    }

    /// Open an existing IPC channel (subscriber side).
    pub fn open(name: [*:0]const u8, ipc_config: IpcConfig) !IpcChannel {
        const tl = ipc_config.term_length;
        const total_size = METADATA_SIZE + tl * TERM_COUNT;

        const region = try memory.openShared(name, total_size);
        const meta: *Metadata = @ptrCast(@alignCast(region.base));

        if (meta.magic != MAGIC) return error.InvalidChannel;
        if (meta.version != 1) return error.UnsupportedVersion;

        return .{
            .region = region,
            .meta = meta,
            .term_base = region.base + METADATA_SIZE,
            .term_length = tl,
        };
    }

    /// Publish a message to the channel.
    pub fn publish(self: *IpcChannel, data: []const u8, msg_type_id: i32) !void {
        if (data.len > frame.MAX_PAYLOAD_SIZE) return error.MessageTooLarge;

        const payload_len: u32 = @intCast(data.len);
        const aligned_len = frame.alignedFrameLength(payload_len);
        const tl: u32 = @intCast(self.term_length);

        // Claim space
        const tail = self.meta.tail_position.load(.monotonic);
        const term_offset: u32 = @intCast(tail % self.term_length);

        // Check if frame fits in current term
        if (term_offset + aligned_len > tl) {
            // Need to insert padding and rotate to next term
            const remaining = tl - term_offset;
            if (remaining >= frame.FrameHeader.SIZE) {
                const term_idx = (tail / self.term_length) % TERM_COUNT;
                const buf = (self.term_base + term_idx * self.term_length + term_offset);
                const hdr: *frame.FrameHeader = @ptrCast(@alignCast(buf));
                hdr.frame_length = -@as(i32, @intCast(remaining));
                hdr.msg_type_id = 0;
            }

            // Advance to next term boundary
            const new_tail = tail + (tl - term_offset);
            self.meta.tail_position.store(new_tail, .release);
            return self.publish(data, msg_type_id);
        }

        // Write frame
        const term_idx = (tail / self.term_length) % TERM_COUNT;
        const buf = (self.term_base + term_idx * self.term_length + term_offset);

        // Write payload first (before header, for visibility ordering)
        const payload_ptr = buf + frame.FrameHeader.SIZE;
        @memcpy(payload_ptr[0..data.len], data);

        // Commit: write header with release semantics via atomic store
        const hdr_len_ptr: *std.atomic.Value(i32) = @ptrCast(@alignCast(buf));
        const hdr_type_ptr: *i32 = @ptrCast(@alignCast(buf + 4));
        hdr_type_ptr.* = msg_type_id;
        hdr_len_ptr.store(@intCast(payload_len), .release);

        // Advance tail
        self.meta.tail_position.store(tail + aligned_len, .release);
    }

    /// Read result from polling.
    pub const ReadResult = struct {
        data: []const u8,
        msg_type_id: i32,
    };

    /// Poll for available messages.
    /// Calls `handler` for each available frame, up to `limit` frames.
    /// Returns the number of frames read.
    pub fn poll(self: *IpcChannel, handler: *const fn (ReadResult) void, limit: u32) u32 {
        var count: u32 = 0;
        var head = self.meta.head_position.load(.monotonic);
        const tail = self.meta.tail_position.load(.acquire);

        while (count < limit and head < tail) {
            const term_offset: u32 = @intCast(head % self.term_length);
            const term_idx = (head / self.term_length) % TERM_COUNT;
            const buf = self.term_base + term_idx * self.term_length + term_offset;

            // Atomic load of frame_length for visibility
            const hdr_len_ptr: *std.atomic.Value(i32) = @ptrCast(@alignCast(buf));
            const fl = hdr_len_ptr.load(.acquire);

            if (frame.isUncommitted(fl)) {
                break;
            }

            if (frame.isPaddingFrame(fl)) {
                const tl_u32: u32 = @intCast(self.term_length);
                head += (tl_u32 - term_offset);
                continue;
            }

            // Data frame
            const payload_len: u32 = @intCast(fl);
            const payload = (buf + frame.FrameHeader.SIZE)[0..payload_len];
            const hdr_type_ptr: *const i32 = @ptrCast(@alignCast(buf + 4));

            handler(.{
                .data = payload,
                .msg_type_id = hdr_type_ptr.*,
            });

            head += frame.alignedFrameLength(payload_len);
            count += 1;
        }

        if (count > 0) {
            self.meta.head_position.store(head, .release);
        }

        return count;
    }

    /// Close the channel and release resources.
    pub fn deinit(self: *IpcChannel) void {
        self.region.deinit();
    }
};

// ── Tests ────────────────────────────────────────────────────
test "IpcChannel create and publish" {
    const name = "/zigbolt_test_ipc";

    var pub_ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer pub_ch.deinit();

    // Publish a message
    try pub_ch.publish("hello zigbolt", 42);

    // Poll it back
    const count = pub_ch.poll(&struct {
        fn handler(_: IpcChannel.ReadResult) void {}
    }.handler, 10);

    try std.testing.expect(count >= 1);
}
