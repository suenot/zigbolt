const std = @import("std");
const config = @import("../platform/config.zig");
const memory = @import("../platform/memory.zig");
const frame = @import("../core/frame.zig");

/// IPC Channel configuration.
pub const IpcConfig = struct {
    /// Length of each term buffer in bytes. Must be a power of two within
    /// [MIN_TERM_LENGTH, MAX_TERM_LENGTH]. Used by `create()`; `open()`
    /// always takes the authoritative term length from the channel metadata
    /// written by the creator.
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

/// Bounds for a valid term length. Restricting it to a power of two within
/// [MIN, MAX] keeps the modular index math exact, makes `tail %
/// term_length` division-by-zero impossible, and keeps every u32 offset
/// computation in publish()/poll() overflow-free (MAX fits u32 with room
/// for offset sums).
pub const MIN_TERM_LENGTH: usize = 4096;
pub const MAX_TERM_LENGTH: usize = 1 << 30; // 1 GiB

fn validateTermLength(term_length: usize) error{InvalidTermLength}!void {
    if (term_length < MIN_TERM_LENGTH or term_length > MAX_TERM_LENGTH) {
        return error.InvalidTermLength;
    }
    if (!std.math.isPowerOfTwo(term_length)) return error.InvalidTermLength;
}

/// SEC: head/tail live in the shared metadata header, which the peer
/// process can scribble on at will. They must be validated before being
/// used in any pointer arithmetic:
///   - reader never ahead of writer (head <= tail),
///   - in-flight bytes bounded by the whole log (tail - head <= 3 terms),
///   - both FRAME_ALIGNMENT-aligned, so a derived term_offset can never
///     land a 4-byte frame_length read in the last 7 bytes of a term
///     (or misalign the atomic header accesses).
fn positionsValid(term_length: usize, head: u64, tail: u64) bool {
    if (head > tail) return false;
    if (tail - head > @as(u64, TERM_COUNT) * term_length) return false;
    if (head % frame.FRAME_ALIGNMENT != 0) return false;
    if (tail % frame.FRAME_ALIGNMENT != 0) return false;
    return true;
}

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
        try validateTermLength(tl);
        const total_size = METADATA_SIZE + tl * TERM_COUNT;

        const region = try memory.createShared(name, total_size, .{
            .use_hugepages = ipc_config.use_hugepages,
            .pre_fault = ipc_config.pre_fault,
        });

        // Initialize metadata. The magic is published LAST with release
        // ordering, so a concurrently-opening subscriber that acquire-loads
        // a valid magic is guaranteed to also see version/term_length and
        // the zeroed positions (not stale pre-init bytes).
        const meta: *Metadata = @ptrCast(@alignCast(region.base));
        meta.* = .{
            .magic = 0,
            .term_length = @intCast(tl),
        };

        if (ipc_config.pre_fault) {
            memory.prefault(region);
        }

        @atomicStore(u64, &meta.magic, MAGIC, .release);

        return .{
            .region = region,
            .meta = meta,
            .term_base = region.base + METADATA_SIZE,
            .term_length = tl,
        };
    }

    /// Open an existing IPC channel (subscriber side).
    ///
    /// The creator's term_length stored in the channel metadata is
    /// authoritative: the mapping is sized from it and
    /// `ipc_config.term_length` is ignored. (Sizing from a caller-supplied
    /// hint would map past the end of the shm object — SIGBUS — when the
    /// hint is larger than the creator's value, or misframe every message
    /// when it is smaller.)
    pub fn open(name: [*:0]const u8, ipc_config: IpcConfig) !IpcChannel {
        _ = ipc_config;

        // Probe: map only the metadata header to learn the real term_length.
        const meta_tl: usize = blk: {
            var probe_region = try memory.openShared(name, METADATA_SIZE);
            defer probe_region.deinit();
            const probe: *const Metadata = @ptrCast(@alignCast(probe_region.base));

            // Acquire pairs with the creator's release-store of magic, so
            // the remaining metadata fields are fully initialized once the
            // magic matches.
            if (@atomicLoad(u64, &probe.magic, .acquire) != MAGIC) return error.InvalidChannel;
            if (probe.version != 1) return error.UnsupportedVersion;
            break :blk probe.term_length;
        };
        // The header is peer-writable: never size a mapping from an
        // unvalidated term_length.
        try validateTermLength(meta_tl);

        const total_size = METADATA_SIZE + meta_tl * TERM_COUNT;
        var region = try memory.openShared(name, total_size);
        errdefer region.deinit();

        const meta: *Metadata = @ptrCast(@alignCast(region.base));
        if (@atomicLoad(u64, &meta.magic, .acquire) != MAGIC) return error.InvalidChannel;
        if (meta.version != 1) return error.UnsupportedVersion;
        // Re-check through the final mapping: the value must not have
        // changed between the probe mapping and this one.
        if (meta.term_length != meta_tl) return error.TermLengthMismatch;

        return .{
            .region = region,
            .meta = meta,
            .term_base = region.base + METADATA_SIZE,
            .term_length = meta_tl,
        };
    }

    /// Publish a message to the channel.
    ///
    /// Returns `error.BackPressure` when the subscriber is too far behind:
    /// the writer never advances more than (TERM_COUNT - 1) terms past the
    /// reader, so the term currently being consumed is never overwritten.
    /// Returns `error.CorruptChannel` if the shared header holds positions
    /// that cannot be valid (the peer scribbled on it).
    pub fn publish(self: *IpcChannel, data: []const u8, msg_type_id: i32) !void {
        if (data.len > frame.MAX_PAYLOAD_SIZE) return error.MessageTooLarge;

        const payload_len: u32 = @intCast(data.len);
        const aligned_len = frame.alignedFrameLength(payload_len);
        const tl: u32 = @intCast(self.term_length);

        // Reject messages that can never fit in a single term.
        if (aligned_len > tl) return error.MessageTooLarge;

        // Claim space
        var tail = self.meta.tail_position.load(.monotonic);
        const head = self.meta.head_position.load(.acquire);

        // SEC: the shared header is peer-writable — validate before any
        // pointer arithmetic derives from it.
        if (!positionsValid(self.term_length, head, tail)) return error.CorruptChannel;

        var term_offset: u32 = @intCast(tail % self.term_length);

        // Overflow-safe rotation test: term_offset < tl always (modulo), so
        // `tl - term_offset` cannot underflow — unlike the additive form
        // `term_offset + aligned_len > tl`, which wraps for large values.
        const rotation: u32 = if (aligned_len > tl - term_offset) tl - term_offset else 0;

        // Back-pressure: total advance includes any rotation padding. The
        // subtraction is safe (head <= tail validated above).
        const advance: u64 = @as(u64, aligned_len) + rotation;
        if ((tail - head) + advance > @as(u64, TERM_COUNT - 1) * self.term_length) {
            return error.BackPressure;
        }

        // Frame does not fit in the current term: insert padding and rotate.
        if (rotation != 0) {
            if (rotation >= frame.FrameHeader.SIZE) {
                const term_idx = (tail / self.term_length) % TERM_COUNT;
                const buf = (self.term_base + term_idx * self.term_length + term_offset);
                // Commit the padding frame like a data frame: type first,
                // then frame_length with release, since the reader
                // acquire-loads frame_length on this same address.
                const pad_type_ptr: *i32 = @ptrCast(@alignCast(buf + 4));
                pad_type_ptr.* = 0;
                const pad_len_ptr: *std.atomic.Value(i32) = @ptrCast(@alignCast(buf));
                pad_len_ptr.store(-@as(i32, @intCast(rotation)), .release);
            }

            // Advance to next term boundary
            const new_tail = tail + rotation;
            self.meta.tail_position.store(new_tail, .release);
            tail = new_tail;
            term_offset = 0;
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
        const Dispatcher = struct {
            h: *const fn (ReadResult) void,
            inline fn deliver(d: @This(), result: ReadResult) void {
                d.h(result);
            }
        };
        return self.pollLoop(Dispatcher{ .h = handler }, limit);
    }

    /// Poll for available messages, passing an opaque `context` through to
    /// the handler. Identical semantics to `poll`; this variant lets callers
    /// carry runtime state (e.g. a user callback) without threadlocals or
    /// comptime closures.
    pub fn pollCtx(
        self: *IpcChannel,
        context: *anyopaque,
        handler: *const fn (context: *anyopaque, result: ReadResult) void,
        limit: u32,
    ) u32 {
        const Dispatcher = struct {
            ctx: *anyopaque,
            h: *const fn (*anyopaque, ReadResult) void,
            inline fn deliver(d: @This(), result: ReadResult) void {
                d.h(d.ctx, result);
            }
        };
        return self.pollLoop(Dispatcher{ .ctx = context, .h = handler }, limit);
    }

    /// Shared poll loop for `poll`/`pollCtx`. Inlined with a comptime
    /// dispatcher so both public entry points compile to the original
    /// hardened loop with no extra indirection.
    inline fn pollLoop(self: *IpcChannel, dispatcher: anytype, limit: u32) u32 {
        var count: u32 = 0;
        var head = self.meta.head_position.load(.monotonic);
        const tail = self.meta.tail_position.load(.acquire);

        // SEC: the shared header is peer-writable. Corrupt positions are
        // treated as "no data" — never used to derive a pointer.
        if (!positionsValid(self.term_length, head, tail)) return 0;

        const tl_u32: u32 = @intCast(self.term_length);

        while (count < limit and head < tail) {
            const term_offset: u32 = @intCast(head % self.term_length);
            const term_idx = (head / self.term_length) % TERM_COUNT;
            const buf = self.term_base + term_idx * self.term_length + term_offset;

            // SEC: the 4-byte frame_length read must lie fully inside the
            // term. head is FRAME_ALIGNMENT-aligned (validated above; every
            // advance below is a multiple of 8), so term_offset <= tl - 8
            // always holds — kept as a hard stop regardless.
            if (term_offset + frame.FrameHeader.SIZE > tl_u32) break;

            // Atomic load of frame_length for visibility
            const hdr_len_ptr: *std.atomic.Value(i32) = @ptrCast(@alignCast(buf));
            const fl = hdr_len_ptr.load(.acquire);

            if (frame.isUncommitted(fl)) {
                break;
            }

            if (frame.isPaddingFrame(fl)) {
                head += (tl_u32 - term_offset);
                continue;
            }

            // Data frame — frame_length came from shared memory: bound the
            // payload to the bytes remaining in this term. The guard above
            // guarantees term_offset + FrameHeader.SIZE <= tl_u32, so this
            // u32 subtraction cannot underflow. (The old order subtracted
            // first, and an attacker-positioned head near the term end made
            // the right side wrap to ~4 GiB, letting a forged frame_length
            // hand the consumer a slice reaching ~2 GiB past the mapping.)
            const payload_len: u32 = @intCast(fl);
            if (payload_len > tl_u32 - term_offset - frame.FrameHeader.SIZE) break;
            const payload = (buf + frame.FrameHeader.SIZE)[0..payload_len];
            const hdr_type_ptr: *const i32 = @ptrCast(@alignCast(buf + 4));

            dispatcher.deliver(.{
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

test "IpcChannel publish/poll roundtrip verifies data" {
    const name = "/zigbolt_test_ipc_rt";

    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    try ch.publish("roundtrip test", 99);

    const S = struct {
        var received_data: ?[]const u8 = null;
        var received_type: i32 = 0;
        fn handler(result: IpcChannel.ReadResult) void {
            received_data = result.data;
            received_type = result.msg_type_id;
        }
    };

    S.received_data = null;
    S.received_type = 0;

    const count = ch.poll(&S.handler, 10);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expect(S.received_data != null);
    try std.testing.expectEqualStrings("roundtrip test", S.received_data.?);
    try std.testing.expectEqual(@as(i32, 99), S.received_type);
}

test "IpcChannel pollCtx passes context through to handler" {
    const name = "/zigbolt_test_ipc_pollctx";

    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    try ch.publish("ctx one", 11);
    try ch.publish("ctx two", 22);

    const Ctx = struct {
        delivered: u32 = 0,
        bytes: usize = 0,
        type_sum: i32 = 0,

        fn dispatch(context: *anyopaque, result: IpcChannel.ReadResult) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.delivered += 1;
            self.bytes += result.data.len;
            self.type_sum += result.msg_type_id;
        }
    };

    var ctx = Ctx{};
    const count = ch.pollCtx(@ptrCast(&ctx), &Ctx.dispatch, 10);
    try std.testing.expectEqual(@as(u32, 2), count);
    try std.testing.expectEqual(@as(u32, 2), ctx.delivered);
    try std.testing.expectEqual(@as(usize, "ctx one".len + "ctx two".len), ctx.bytes);
    try std.testing.expectEqual(@as(i32, 33), ctx.type_sum);
}

test "IpcChannel multiple messages" {
    const name = "/zigbolt_test_ipc_multi";

    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    try ch.publish("msg1", 1);
    try ch.publish("msg2", 2);
    try ch.publish("msg3", 3);

    const S = struct {
        var msg_count: u32 = 0;
        fn handler(_: IpcChannel.ReadResult) void {
            msg_count += 1;
        }
    };

    S.msg_count = 0;
    const count = ch.poll(&S.handler, 10);
    try std.testing.expectEqual(@as(u32, 3), count);
    try std.testing.expectEqual(@as(u32, 3), S.msg_count);
}

test "IpcChannel empty poll returns zero" {
    const name = "/zigbolt_test_ipc_empty";

    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const count = ch.poll(&struct {
        fn handler(_: IpcChannel.ReadResult) void {}
    }.handler, 10);

    try std.testing.expectEqual(@as(u32, 0), count);
}

test "create rejects invalid term_length" {
    const name = "/zigbolt_test_ipc_badtl";

    // Zero would make every `tail % term_length` a division by zero.
    try std.testing.expectError(error.InvalidTermLength, IpcChannel.create(name, .{ .term_length = 0 }));
    // Non-power-of-two breaks term rotation / modular indexing.
    try std.testing.expectError(error.InvalidTermLength, IpcChannel.create(name, .{ .term_length = 4096 + 1024 }));
    // Below the documented minimum.
    try std.testing.expectError(error.InvalidTermLength, IpcChannel.create(name, .{ .term_length = 1024 }));
    // Above the documented maximum.
    try std.testing.expectError(error.InvalidTermLength, IpcChannel.create(name, .{ .term_length = MAX_TERM_LENGTH * 2 }));
}

test "open sizes mapping from creator metadata regardless of caller hint" {
    const name = "/zigbolt_test_ipc_meta";

    var pub_ch = try IpcChannel.create(name, .{ .term_length = 8192 });
    defer pub_ch.deinit();

    // Subscriber passes a stale/wrong hint — must neither SIGBUS (hint too
    // large would map past the shm object) nor misframe (hint too small).
    var sub_ch = try IpcChannel.open(name, .{ .term_length = 4096 });
    defer sub_ch.deinit();
    try std.testing.expectEqual(@as(usize, 8192), sub_ch.term_length);

    try pub_ch.publish("sized from meta", 7);

    const S = struct {
        var got: ?[]const u8 = null;
        var type_id: i32 = 0;
        fn handler(result: IpcChannel.ReadResult) void {
            got = result.data;
            type_id = result.msg_type_id;
        }
    };
    S.got = null;
    S.type_id = 0;

    const count = sub_ch.poll(&S.handler, 10);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqualStrings("sized from meta", S.got.?);
    try std.testing.expectEqual(@as(i32, 7), S.type_id);
}

test "open rejects corrupt metadata term_length" {
    const name = "/zigbolt_test_ipc_eviltl";

    var pub_ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer pub_ch.deinit();

    // Attacker rewrites term_length in the shared header.
    pub_ch.meta.term_length = 12345; // non-power-of-two
    try std.testing.expectError(error.InvalidTermLength, IpcChannel.open(name, .{}));

    pub_ch.meta.term_length = 0;
    try std.testing.expectError(error.InvalidTermLength, IpcChannel.open(name, .{}));

    pub_ch.meta.term_length = 1 << 31; // beyond MAX_TERM_LENGTH
    try std.testing.expectError(error.InvalidTermLength, IpcChannel.open(name, .{}));

    // Valid-looking power-of-two that the backing object cannot hold: the
    // object-size check must fire instead of mapping pages that SIGBUS.
    pub_ch.meta.term_length = 1 << 30;
    try std.testing.expectError(error.ShmTooSmall, IpcChannel.open(name, .{}));

    // Restored header opens fine.
    pub_ch.meta.term_length = 4096;
    var sub_ch = try IpcChannel.open(name, .{});
    sub_ch.deinit();
}

test "poll does not read out of bounds for attacker-set head near term end" {
    const name = "/zigbolt_test_ipc_oob";

    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const S = struct {
        var calls: u32 = 0;
        fn handler(_: IpcChannel.ReadResult) void {
            calls += 1;
        }
    };
    S.calls = 0;

    // Variant A: unaligned head with term_offset = 4092, inside the last 7
    // bytes of term 0, and a forged huge frame_length there. The old check
    // computed `4096 - 4092 - 8` in u32, which wrapped to ~4.29e9 and let
    // the forged length pass — handing the consumer a ~2 GiB slice that
    // reaches far past the mapping. Alignment validation must reject it.
    const forged: *align(1) i32 = @ptrCast(ch.term_base + 4092);
    forged.* = std.math.maxInt(i32) - 7;
    ch.meta.head_position.store(4092, .monotonic);
    ch.meta.tail_position.store(8192, .monotonic);
    try std.testing.expectEqual(@as(u32, 0), ch.poll(&S.handler, 10));
    try std.testing.expectEqual(@as(u32, 0), S.calls);

    // Variant B: aligned head at the last valid header slot (offset 4088)
    // with a forged huge frame_length: zero payload bytes remain in the
    // term, so the bound check must stop delivery.
    const forged2: *align(1) i32 = @ptrCast(ch.term_base + 4088);
    forged2.* = std.math.maxInt(i32) - 7;
    ch.meta.head_position.store(4088, .monotonic);
    ch.meta.tail_position.store(8192, .monotonic);
    try std.testing.expectEqual(@as(u32, 0), ch.poll(&S.handler, 10));
    try std.testing.expectEqual(@as(u32, 0), S.calls);
}

test "poll and publish reject corrupt head/tail positions" {
    const name = "/zigbolt_test_ipc_corrupt";

    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const S = struct {
        var calls: u32 = 0;
        fn handler(_: IpcChannel.ReadResult) void {
            calls += 1;
        }
    };
    S.calls = 0;

    // head > tail
    ch.meta.head_position.store(8192, .monotonic);
    ch.meta.tail_position.store(0, .monotonic);
    try std.testing.expectEqual(@as(u32, 0), ch.poll(&S.handler, 10));
    try std.testing.expectError(error.CorruptChannel, ch.publish("x", 1));

    // tail - head > TERM_COUNT * term_length
    ch.meta.head_position.store(0, .monotonic);
    ch.meta.tail_position.store(TERM_COUNT * 4096 + 8, .monotonic);
    try std.testing.expectEqual(@as(u32, 0), ch.poll(&S.handler, 10));
    try std.testing.expectError(error.CorruptChannel, ch.publish("x", 1));

    // Unaligned tail
    ch.meta.head_position.store(0, .monotonic);
    ch.meta.tail_position.store(12, .monotonic);
    try std.testing.expectEqual(@as(u32, 0), ch.poll(&S.handler, 10));
    try std.testing.expectError(error.CorruptChannel, ch.publish("x", 1));

    try std.testing.expectEqual(@as(u32, 0), S.calls);

    // Restored header works again.
    ch.meta.head_position.store(0, .monotonic);
    ch.meta.tail_position.store(0, .monotonic);
    try ch.publish("recovered", 5);
    try std.testing.expectEqual(@as(u32, 1), ch.poll(&S.handler, 10));
}

test "publish returns BackPressure instead of lapping a stalled reader" {
    const name = "/zigbolt_test_ipc_bp";

    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    // 1016-byte payload -> 1024-byte aligned frame, 4 frames per term.
    // Window = (TERM_COUNT - 1) * 4096 = 8192 -> exactly 8 frames fit
    // while the reader is stalled at head == 0.
    var payload: [1016]u8 = undefined;
    @memset(&payload, 0xAB);

    var i: usize = 0;
    while (i < 8) : (i += 1) {
        try ch.publish(&payload, 1);
    }

    // The 9th frame would enter the term the stalled reader still owns:
    // it must be rejected cleanly, not overwrite frames being read.
    try std.testing.expectError(error.BackPressure, ch.publish(&payload, 1));

    // Drain and verify nothing was corrupted, then publishing resumes.
    const S = struct {
        var n: u32 = 0;
        var intact = true;
        fn handler(result: IpcChannel.ReadResult) void {
            n += 1;
            if (result.data.len != 1016) intact = false;
            for (result.data) |b| {
                if (b != 0xAB) intact = false;
            }
        }
    };
    S.n = 0;
    S.intact = true;

    try std.testing.expectEqual(@as(u32, 8), ch.poll(&S.handler, 100));
    try std.testing.expectEqual(@as(u32, 8), S.n);
    try std.testing.expect(S.intact);
    try ch.publish(&payload, 1);
}
