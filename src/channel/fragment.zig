const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("../core/frame.zig");

/// Configuration for the fragmentation layer.
pub const FragmentConfig = struct {
    /// Maximum transmission unit in bytes.
    /// Default: 1500 (Ethernet) - 20 (IP) - 8 (UDP) = 1472
    mtu: u32 = 1472,
    /// Maximum message size (limits reassembly buffer).
    max_message_size: u32 = 1 << 20, // 1 MB
    /// Maximum number of concurrently in-flight (partially reassembled)
    /// messages. The oldest entry is evicted when the cap is reached.
    max_pending_messages: u32 = 64,
    /// Maximum total bytes across all pending reassembly buffers. Oldest
    /// entries are evicted to make room; a single message larger than this
    /// budget is rejected outright.
    max_pending_bytes: usize = 8 << 20, // 8 MB
    /// Pending entries older than this are evicted automatically on the
    /// processFragment path (a peer that never completes its messages must
    /// not pin reassembly buffers forever).
    reassembly_timeout_ns: u64 = 5 * std.time.ns_per_s,
    /// Maximum number of disjoint coverage ranges tracked per message.
    /// Bounds the bookkeeping memory a hostile peer can force with many
    /// tiny, non-adjacent fragments. An honest sender produces at most
    /// ceil(total_length / payload_per_fragment) ranges even under full
    /// reordering (adjacent ranges are merged).
    max_ranges_per_message: u32 = 4096,
};

/// Fragment header prepended to each fragment.
///
/// Layout (16 bytes total):
///   [0:4]   total_length:    u32 — total message length (same in all fragments)
///   [4:8]   fragment_offset: u32 — byte offset of this fragment within the message
///   [8:12]  message_id:      u32 — sender-assigned id grouping fragments of one message
///   [12:14] fragment_length: u16 — payload length of this fragment
///   [14]    flags:           u8  — begin/end flags
///   [15]    _reserved:       u8
pub const FragmentHeader = extern struct {
    /// Total message length (same in all fragments of a message).
    total_length: u32,
    /// Offset of this fragment within the message.
    fragment_offset: u32,
    /// Sender-assigned message id: all fragments of one message carry the
    /// same id so the receiver can reassemble interleaved/reordered
    /// messages on the same stream.
    message_id: u32 = 0,
    /// Length of this fragment's payload.
    fragment_length: u16,
    /// Flags.
    flags: Flags,
    _reserved: u8 = 0,

    pub const SIZE: usize = @sizeOf(FragmentHeader);

    pub const Flags = packed struct(u8) {
        /// Is this the first fragment?
        begin: bool = false,
        /// Is this the last fragment?
        end: bool = false,
        _padding: u6 = 0,
    };

    comptime {
        if (@sizeOf(FragmentHeader) != 16) @compileError("FragmentHeader must be 16 bytes");
    }
};

/// Splits a message into MTU-sized fragments.
pub const Fragmenter = struct {
    frag_config: FragmentConfig,

    pub fn init(frag_config: FragmentConfig) Fragmenter {
        return .{ .frag_config = frag_config };
    }

    /// Check if a message needs fragmentation.
    pub fn needsFragmentation(self: *const Fragmenter, message_len: usize) bool {
        return message_len > self.maxPayloadPerFragment();
    }

    /// Calculate maximum payload per fragment (MTU - fragment header).
    /// Saturates at 0 for degenerate MTUs and is capped at u16 max because
    /// `FragmentHeader.fragment_length` is a u16.
    pub fn maxPayloadPerFragment(self: *const Fragmenter) u32 {
        const budget = self.frag_config.mtu -| @as(u32, @intCast(FragmentHeader.SIZE));
        return @min(budget, std.math.maxInt(u16));
    }

    /// Fragment a message. Calls `emit` for each fragment.
    /// Each call to emit provides: fragment header + payload slice.
    pub fn fragment(
        self: *const Fragmenter,
        message: []const u8,
        message_id: u32,
        emit: *const fn (header: FragmentHeader, payload: []const u8) void,
    ) void {
        var iter = self.fragmentIterator(message, message_id);
        while (iter.next()) |frag| {
            emit(frag.header, frag.payload);
        }
    }

    /// Iterator-based fragmentation (alternative API).
    pub const FragmentIterator = struct {
        message: []const u8,
        offset: u32,
        max_payload: u32,
        total_length: u32,
        message_id: u32,

        pub const Fragment = struct {
            header: FragmentHeader,
            payload: []const u8,
        };

        pub fn next(self: *FragmentIterator) ?Fragment {
            // Degenerate MTU (<= header size): no forward progress is
            // possible — stop instead of looping forever.
            if (self.max_payload == 0) return null;
            if (self.offset >= self.total_length) return null;

            const remaining = self.total_length - self.offset;
            const payload_len: u32 = @min(remaining, self.max_payload);
            const is_first = self.offset == 0;
            const is_last = (self.offset + payload_len) == self.total_length;

            const header = FragmentHeader{
                .total_length = self.total_length,
                .fragment_offset = self.offset,
                .message_id = self.message_id,
                .fragment_length = @intCast(payload_len),
                .flags = .{
                    .begin = is_first,
                    .end = is_last,
                },
            };

            const payload = self.message[self.offset..][0..payload_len];
            self.offset += payload_len;

            return .{
                .header = header,
                .payload = payload,
            };
        }
    };

    pub fn fragmentIterator(self: *const Fragmenter, message: []const u8, message_id: u32) FragmentIterator {
        return .{
            .message = message,
            .offset = 0,
            .max_payload = self.maxPayloadPerFragment(),
            .total_length = @intCast(message.len),
            .message_id = message_id,
        };
    }
};

/// Reassembles fragments back into complete messages.
///
/// Hardened against hostile fragment streams:
/// - completion is tracked with an exact coverage range set (never a byte
///   counter), so duplicate/overlapping fragments can not fake completion
///   and the returned buffer never contains unwritten bytes;
/// - reassembly buffers are zeroed at allocation (defence in depth);
/// - the pending set is bounded in entry count and total bytes, with
///   oldest-first eviction and automatic stale-entry cleanup.
pub const Reassembler = struct {
    allocator: std.mem.Allocator,
    frag_config: FragmentConfig,
    /// In-progress reassembly buffers, keyed by caller-provided key
    /// (typically the message id within a session/stream).
    pending: std.AutoHashMap(u64, ReassemblyBuffer),
    /// Total bytes currently held by pending reassembly buffers.
    pending_bytes: usize,
    /// Monotonic time of the last automatic stale-entry sweep.
    last_autoclean_ns: u64,

    /// A received byte range [start, end) within the message.
    const Range = struct {
        start: u32,
        end: u32,
    };

    const ReassemblyBuffer = struct {
        buffer: []u8,
        total_length: u32,
        /// Disjoint, sorted (by start), merged-when-adjacent ranges of
        /// bytes already received.
        ranges: std.ArrayList(Range),
        /// Total bytes covered by `ranges`. Because ranges are disjoint and
        /// confined to [0, total_length), `covered == total_length` holds
        /// if and only if every byte of the message has been received
        /// exactly once.
        covered: u32,
        /// Creation time (monotonic). Deliberately NOT refreshed on later
        /// fragments: a message must complete within the reassembly
        /// timeout, so a trickle of bytes cannot pin an entry forever.
        timestamp_ns: u64,
    };

    pub fn init(allocator: std.mem.Allocator, frag_config: FragmentConfig) Reassembler {
        return .{
            .allocator = allocator,
            .frag_config = frag_config,
            .pending = std.AutoHashMap(u64, ReassemblyBuffer).init(allocator),
            .pending_bytes = 0,
            .last_autoclean_ns = 0,
        };
    }

    pub fn deinit(self: *Reassembler) void {
        var it = self.pending.valueIterator();
        while (it.next()) |buf| {
            self.allocator.free(buf.buffer);
            buf.ranges.deinit(self.allocator);
        }
        self.pending.deinit();
    }

    /// Process an incoming fragment. Returns the complete reassembled message
    /// when all fragments have been received, or null if reassembly is still
    /// in progress. The caller owns the returned slice and must free it.
    ///
    /// Duplicate, overlapping, out-of-range, and inconsistent fragments are
    /// rejected with an error; the caller should drop them (they come off
    /// an unauthenticated wire).
    pub fn processFragment(
        self: *Reassembler,
        key: u64,
        header: FragmentHeader,
        payload: []const u8,
    ) !?[]const u8 {
        // Validate fragment — every field is untrusted wire input.
        if (header.total_length > self.frag_config.max_message_size) {
            return error.MessageTooLarge;
        }
        if (payload.len != header.fragment_length) {
            return error.LengthMismatch;
        }
        if (@as(u64, header.fragment_offset) + payload.len > header.total_length) {
            return error.FragmentOutOfBounds;
        }

        // Automatic stale eviction (rate-limited sweep): entries older than
        // the reassembly timeout are dropped even if the caller never
        // invokes cleanStale() manually.
        const now = config.monotonicNs();
        const clean_interval = @max(self.frag_config.reassembly_timeout_ns / 4, 1);
        if (now -| self.last_autoclean_ns >= clean_interval) {
            self.last_autoclean_ns = now;
            self.cleanStaleAt(now, self.frag_config.reassembly_timeout_ns);
        }

        // Single-fragment fast path: only when this fragment IS the whole
        // message. A begin+end header with a short payload previously
        // allocated total_length but copied only payload.len — returning
        // uninitialized heap memory to the caller (info disclosure).
        // Anything inconsistent goes through the coverage path below.
        if (header.flags.begin and header.flags.end and
            header.fragment_offset == 0 and payload.len == header.total_length)
        {
            const result = try self.allocator.alloc(u8, payload.len);
            @memcpy(result, payload);
            return result;
        }

        // Zero-length fragments carry no coverage and would only pin
        // pending entries — reject them on the coverage path.
        if (payload.len == 0) return error.EmptyFragment;

        // Get or create the reassembly buffer. Creation enforces the
        // pending caps first (entry count and total buffered bytes).
        var entry = self.pending.getPtr(key);
        if (entry == null) {
            if (header.total_length > self.frag_config.max_pending_bytes) {
                return error.MessageTooLarge;
            }
            while (self.pending.count() >= self.frag_config.max_pending_messages or
                self.pending_bytes + header.total_length > self.frag_config.max_pending_bytes)
            {
                if (!self.evictOldest()) break;
            }

            // Zeroed allocation: even though completion requires full
            // coverage, never hold (or risk returning) uninitialized heap.
            const buffer = try self.allocator.alloc(u8, header.total_length);
            errdefer self.allocator.free(buffer);
            @memset(buffer, 0);
            try self.pending.put(key, .{
                .buffer = buffer,
                .total_length = header.total_length,
                .ranges = .empty,
                .covered = 0,
                .timestamp_ns = now,
            });
            self.pending_bytes += buffer.len;
            entry = self.pending.getPtr(key);
        }

        const buf = entry.?;

        // Validate total_length consistency across fragments of one message.
        if (buf.total_length != header.total_length) {
            return error.InconsistentTotalLength;
        }

        // Coverage tracking: insert [start, end) into the sorted disjoint
        // range set. Duplicates and overlaps are rejected outright — the
        // old byte counter double-counted them and could report a message
        // "complete" while gaps remained unwritten.
        const start: u32 = header.fragment_offset;
        const end: u32 = header.fragment_offset + header.fragment_length;

        const items = buf.ranges.items;
        var idx: usize = 0;
        while (idx < items.len and items[idx].start < start) : (idx += 1) {}
        if (idx > 0 and items[idx - 1].end > start) return error.OverlappingFragment;
        if (idx < items.len and items[idx].start < end) return error.OverlappingFragment;

        const left_adjacent = idx > 0 and items[idx - 1].end == start;
        const right_adjacent = idx < items.len and items[idx].start == end;
        if (left_adjacent and right_adjacent) {
            items[idx - 1].end = items[idx].end;
            _ = buf.ranges.orderedRemove(idx);
        } else if (left_adjacent) {
            items[idx - 1].end = end;
        } else if (right_adjacent) {
            items[idx].start = start;
        } else {
            if (items.len >= self.frag_config.max_ranges_per_message) {
                return error.TooManyFragments;
            }
            try buf.ranges.insert(self.allocator, idx, .{ .start = start, .end = end });
        }

        // Copy payload into the reassembly buffer (range accepted: the
        // destination slice is exactly this fragment's disjoint window).
        @memcpy(buf.buffer[start..end], payload);
        buf.covered += header.fragment_length;

        // Complete only when every byte in [0, total_length) is covered.
        if (buf.covered == buf.total_length) {
            const result = buf.buffer;
            buf.ranges.deinit(self.allocator);
            self.pending_bytes -= result.len;
            _ = self.pending.remove(key);
            return result;
        }

        return null;
    }

    /// Clean up stale reassembly buffers older than `max_age_ns` nanoseconds.
    /// Also invoked automatically from `processFragment` with the configured
    /// `reassembly_timeout_ns`.
    pub fn cleanStale(self: *Reassembler, max_age_ns: u64) void {
        self.cleanStaleAt(config.monotonicNs(), max_age_ns);
    }

    fn cleanStaleAt(self: *Reassembler, now: u64, max_age_ns: u64) void {
        // The pending set is small (bounded by max_pending_messages), so a
        // rescan per removal is cheap and avoids allocating a key list.
        while (true) {
            var stale_key: ?u64 = null;
            var it = self.pending.iterator();
            while (it.next()) |e| {
                if (now -| e.value_ptr.timestamp_ns > max_age_ns) {
                    stale_key = e.key_ptr.*;
                    break;
                }
            }
            self.removeEntry(stale_key orelse return);
        }
    }

    /// Evict the oldest pending entry. Returns false when there is nothing
    /// to evict.
    fn evictOldest(self: *Reassembler) bool {
        var oldest_key: ?u64 = null;
        var oldest_ts: u64 = std.math.maxInt(u64);
        var it = self.pending.iterator();
        while (it.next()) |e| {
            if (e.value_ptr.timestamp_ns <= oldest_ts) {
                oldest_ts = e.value_ptr.timestamp_ns;
                oldest_key = e.key_ptr.*;
            }
        }
        const key = oldest_key orelse return false;
        self.removeEntry(key);
        return true;
    }

    fn removeEntry(self: *Reassembler, key: u64) void {
        if (self.pending.fetchRemove(key)) |kv| {
            var value = kv.value;
            self.pending_bytes -= value.buffer.len;
            self.allocator.free(value.buffer);
            value.ranges.deinit(self.allocator);
        }
    }
};

// ── Tests ────────────────────────────────────────────────────

test "FragmentHeader size is 16 bytes" {
    try std.testing.expectEqual(@as(usize, 16), FragmentHeader.SIZE);
}

test "unfragmented message passes through" {
    const cfg = FragmentConfig{ .mtu = 1472 };
    const fragmenter = Fragmenter.init(cfg);

    // A small message that fits in a single fragment
    const msg = "hello zigbolt";
    try std.testing.expect(!fragmenter.needsFragmentation(msg.len));

    var iter = fragmenter.fragmentIterator(msg, 7);
    const frag = iter.next().?;

    try std.testing.expect(frag.header.flags.begin);
    try std.testing.expect(frag.header.flags.end);
    try std.testing.expectEqual(@as(u32, msg.len), frag.header.total_length);
    try std.testing.expectEqual(@as(u32, 0), frag.header.fragment_offset);
    try std.testing.expectEqual(@as(u32, 7), frag.header.message_id);
    try std.testing.expectEqualSlices(u8, msg, frag.payload);

    // No more fragments
    try std.testing.expect(iter.next() == null);
}

test "fragment and reassemble large message" {
    const allocator = std.testing.allocator;
    // Use a small MTU to force fragmentation: header (16) + payload (84) = 100
    const cfg = FragmentConfig{ .mtu = 100, .max_message_size = 1 << 20 };
    const fragmenter = Fragmenter.init(cfg);
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    // Create a message larger than one fragment's payload (84 bytes)
    const msg_len: usize = 300;
    var msg: [msg_len]u8 = undefined;
    for (&msg, 0..) |*b, i| {
        b.* = @intCast(i % 256);
    }

    try std.testing.expect(fragmenter.needsFragmentation(msg_len));

    const max_payload = fragmenter.maxPayloadPerFragment();
    try std.testing.expectEqual(@as(u32, 84), max_payload);

    // Fragment and reassemble
    var iter = fragmenter.fragmentIterator(&msg, 42);
    var fragment_count: usize = 0;
    var result: ?[]const u8 = null;

    while (iter.next()) |frag| {
        fragment_count += 1;
        const maybe_complete = try reassembler.processFragment(42, frag.header, frag.payload);
        if (maybe_complete) |complete| {
            result = complete;
        }
    }

    // 300 / 84 = 3 full + 1 partial = 4 fragments
    try std.testing.expectEqual(@as(usize, 4), fragment_count);

    // Reassembly should be complete
    const reassembled = result.?;
    defer allocator.free(reassembled);
    try std.testing.expectEqualSlices(u8, &msg, reassembled);

    // The completed message no longer occupies pending state.
    try std.testing.expectEqual(@as(u32, 0), reassembler.pending.count());
    try std.testing.expectEqual(@as(usize, 0), reassembler.pending_bytes);
}

test "FragmentIterator produces correct fragments" {
    const cfg = FragmentConfig{ .mtu = 100 }; // 84 bytes payload per fragment
    const fragmenter = Fragmenter.init(cfg);

    // 200 bytes = ceil(200/84) = 3 fragments: 84 + 84 + 32
    var msg: [200]u8 = undefined;
    @memset(&msg, 0xAB);

    var iter = fragmenter.fragmentIterator(&msg, 5);

    // First fragment
    const f1 = iter.next().?;
    try std.testing.expect(f1.header.flags.begin);
    try std.testing.expect(!f1.header.flags.end);
    try std.testing.expectEqual(@as(u32, 0), f1.header.fragment_offset);
    try std.testing.expectEqual(@as(u16, 84), f1.header.fragment_length);
    try std.testing.expectEqual(@as(usize, 84), f1.payload.len);
    try std.testing.expectEqual(@as(u32, 200), f1.header.total_length);
    try std.testing.expectEqual(@as(u32, 5), f1.header.message_id);

    // Second fragment
    const f2 = iter.next().?;
    try std.testing.expect(!f2.header.flags.begin);
    try std.testing.expect(!f2.header.flags.end);
    try std.testing.expectEqual(@as(u32, 84), f2.header.fragment_offset);
    try std.testing.expectEqual(@as(u16, 84), f2.header.fragment_length);

    // Third (last) fragment
    const f3 = iter.next().?;
    try std.testing.expect(!f3.header.flags.begin);
    try std.testing.expect(f3.header.flags.end);
    try std.testing.expectEqual(@as(u32, 168), f3.header.fragment_offset);
    try std.testing.expectEqual(@as(u16, 32), f3.header.fragment_length);
    try std.testing.expectEqual(@as(u32, 5), f3.header.message_id);

    // No more
    try std.testing.expect(iter.next() == null);
}

test "reassembler rejects oversized messages" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{ .mtu = 100, .max_message_size = 256 };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    const header = FragmentHeader{
        .total_length = 512, // exceeds max_message_size
        .fragment_offset = 0,
        .fragment_length = 84,
        .flags = .{ .begin = true },
    };

    var payload: [84]u8 = undefined;
    @memset(&payload, 0);

    const result = reassembler.processFragment(1, header, &payload);
    try std.testing.expectError(error.MessageTooLarge, result);
}

test "small message does not need fragmentation" {
    const fragmenter = Fragmenter.init(.{ .mtu = 1472, .max_message_size = 1 << 20 });
    try std.testing.expect(!fragmenter.needsFragmentation(10));
    try std.testing.expect(!fragmenter.needsFragmentation(0));
    try std.testing.expect(!fragmenter.needsFragmentation(1456)); // 1472 - 16 = 1456

    // At exactly max payload per fragment, no fragmentation needed
    const max_payload = fragmenter.maxPayloadPerFragment();
    try std.testing.expect(!fragmenter.needsFragmentation(max_payload));

    // One byte over needs fragmentation
    try std.testing.expect(fragmenter.needsFragmentation(max_payload + 1));
}

test "reassembler single-fragment fast path returns exactly the payload" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{ .mtu = 1472, .max_message_size = 1 << 20 };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    const msg = "hello world";
    const header = FragmentHeader{
        .total_length = msg.len,
        .fragment_offset = 0,
        .fragment_length = @intCast(msg.len),
        .flags = .{ .begin = true, .end = true },
    };

    const result = try reassembler.processFragment(1, header, msg);
    try std.testing.expect(result != null);
    defer allocator.free(result.?);
    // Byte-exact: same length, same content — no uninitialized tail.
    try std.testing.expectEqual(msg.len, result.?.len);
    try std.testing.expectEqualStrings(msg, result.?);
    // The fast path never touches pending state.
    try std.testing.expectEqual(@as(u32, 0), reassembler.pending.count());
}

test "reassembler does not return uninitialized tail for short begin+end fragment" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{ .mtu = 100, .max_message_size = 1 << 20 };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    // Hostile: claims begin+end but carries only 10 of 100 bytes. The old
    // fast path returned a 100-byte buffer with 90 uninitialized bytes
    // (heap info disclosure). It must go through coverage tracking and
    // stay incomplete instead.
    const header = FragmentHeader{
        .total_length = 100,
        .fragment_offset = 0,
        .fragment_length = 10,
        .flags = .{ .begin = true, .end = true },
    };
    const result = try reassembler.processFragment(5, header, "0123456789");
    try std.testing.expect(result == null);
    try std.testing.expectEqual(@as(u32, 1), reassembler.pending.count());
}

test "reassembler rejects fragment out of bounds" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{ .mtu = 100, .max_message_size = 1 << 20 };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    const header = FragmentHeader{
        .total_length = 10,
        .fragment_offset = 5,
        .fragment_length = 10,
        .flags = .{},
    };

    var payload: [10]u8 = undefined;
    @memset(&payload, 0);

    const result = reassembler.processFragment(1, header, &payload);
    try std.testing.expectError(error.FragmentOutOfBounds, result);
}

test "reassembler rejects duplicate and overlapping fragments, message still exact" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{ .mtu = 100, .max_message_size = 1 << 20 };
    const fragmenter = Fragmenter.init(cfg);
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    // 200 bytes -> 3 fragments: [0,84) [84,168) [168,200)
    var msg: [200]u8 = undefined;
    for (&msg, 0..) |*b, i| b.* = @intCast((i * 7 + 3) % 256);

    var frags: [3]Fragmenter.FragmentIterator.Fragment = undefined;
    var iter = fragmenter.fragmentIterator(&msg, 9);
    var n: usize = 0;
    while (iter.next()) |f| : (n += 1) {
        frags[n] = f;
    }
    try std.testing.expectEqual(@as(usize, 3), n);

    // First two fragments arrive out of order.
    try std.testing.expect((try reassembler.processFragment(9, frags[1].header, frags[1].payload)) == null);
    try std.testing.expect((try reassembler.processFragment(9, frags[0].header, frags[0].payload)) == null);

    // Exact duplicate: rejected — the old byte counter double-counted it
    // and "completed" the message with an unwritten 32-byte gap.
    try std.testing.expectError(
        error.OverlappingFragment,
        reassembler.processFragment(9, frags[1].header, frags[1].payload),
    );

    // Partially overlapping hostile fragment: rejected.
    const evil = FragmentHeader{
        .total_length = 200,
        .fragment_offset = 80,
        .fragment_length = 20,
        .flags = .{},
    };
    try std.testing.expectError(
        error.OverlappingFragment,
        reassembler.processFragment(9, evil, msg[80..100]),
    );

    // The genuine last fragment completes the message byte-exact.
    const done = (try reassembler.processFragment(9, frags[2].header, frags[2].payload)).?;
    defer allocator.free(done);
    try std.testing.expectEqualSlices(u8, &msg, done);
    try std.testing.expectEqual(@as(u32, 0), reassembler.pending.count());
    try std.testing.expectEqual(@as(usize, 0), reassembler.pending_bytes);
}

test "reassembler bounds pending messages under a flood of incomplete messages" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{
        .mtu = 100,
        .max_message_size = 1 << 20,
        .max_pending_messages = 8,
        .max_pending_bytes = 1 << 20,
    };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    var payload: [84]u8 = undefined;
    @memset(&payload, 0xEE);
    const header = FragmentHeader{
        .total_length = 200,
        .fragment_offset = 0,
        .fragment_length = 84,
        .flags = .{ .begin = true },
    };

    // 100 distinct message keys, none of which ever completes. Before the
    // cap, every key pinned a buffer forever (unbounded memory growth).
    var key: u64 = 0;
    while (key < 100) : (key += 1) {
        try std.testing.expect((try reassembler.processFragment(key, header, &payload)) == null);
    }

    try std.testing.expectEqual(@as(u32, 8), reassembler.pending.count());
    try std.testing.expectEqual(@as(usize, 8 * 200), reassembler.pending_bytes);
    // The newest message is always retained (older ones were evicted).
    try std.testing.expect(reassembler.pending.getPtr(99) != null);
}

test "reassembler bounds pending bytes and rejects messages over the byte cap" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{
        .mtu = 100,
        .max_message_size = 1 << 20,
        .max_pending_messages = 100,
        .max_pending_bytes = 1000,
    };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    var payload: [84]u8 = undefined;
    @memset(&payload, 0x11);
    const header = FragmentHeader{
        .total_length = 400,
        .fragment_offset = 0,
        .fragment_length = 84,
        .flags = .{ .begin = true },
    };

    var key: u64 = 0;
    while (key < 10) : (key += 1) {
        try std.testing.expect((try reassembler.processFragment(key, header, &payload)) == null);
        try std.testing.expect(reassembler.pending_bytes <= 1000);
    }
    // 400-byte buffers under a 1000-byte budget: at most 2 in flight.
    try std.testing.expectEqual(@as(u32, 2), reassembler.pending.count());

    // A single message bigger than the entire byte budget is rejected
    // outright (it could never be buffered).
    const huge = FragmentHeader{
        .total_length = 2000,
        .fragment_offset = 0,
        .fragment_length = 84,
        .flags = .{ .begin = true },
    };
    try std.testing.expectError(error.MessageTooLarge, reassembler.processFragment(777, huge, &payload));
}

test "reassembler auto-evicts stale incomplete messages" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{
        .mtu = 100,
        .max_message_size = 1 << 20,
        .reassembly_timeout_ns = 1,
    };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    var payload: [84]u8 = undefined;
    @memset(&payload, 0x22);
    const header = FragmentHeader{
        .total_length = 200,
        .fragment_offset = 0,
        .fragment_length = 84,
        .flags = .{ .begin = true },
    };

    _ = try reassembler.processFragment(1, header, &payload);
    try std.testing.expectEqual(@as(u32, 1), reassembler.pending.count());

    // Entry 1 ages past the 1 ns deadline. No manual cleanStale call: the
    // next processFragment must evict it automatically.
    std.Thread.sleep(2_000_000); // 2 ms
    _ = try reassembler.processFragment(2, header, &payload);
    try std.testing.expectEqual(@as(u32, 1), reassembler.pending.count());
    try std.testing.expect(reassembler.pending.getPtr(1) == null);
    try std.testing.expect(reassembler.pending.getPtr(2) != null);
}

test "reassembler caps coverage ranges per message" {
    const allocator = std.testing.allocator;
    const cfg = FragmentConfig{
        .mtu = 100,
        .max_message_size = 1 << 20,
        .max_ranges_per_message = 2,
    };
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    var payload: [10]u8 = undefined;
    @memset(&payload, 0x33);

    // Three tiny, mutually non-adjacent fragments of one message: the
    // third would need a third range and is rejected (bookkeeping memory
    // bound against hostile fragment dust).
    const fragAt = struct {
        fn make(offset: u32) FragmentHeader {
            return .{
                .total_length = 1000,
                .fragment_offset = offset,
                .fragment_length = 10,
                .flags = .{},
            };
        }
    }.make;

    try std.testing.expect((try reassembler.processFragment(3, fragAt(0), &payload)) == null);
    try std.testing.expect((try reassembler.processFragment(3, fragAt(100), &payload)) == null);
    try std.testing.expectError(error.TooManyFragments, reassembler.processFragment(3, fragAt(200), &payload));

    // An adjacent fragment merges into an existing range and is still
    // accepted (honest reordering is not penalised).
    try std.testing.expect((try reassembler.processFragment(3, fragAt(10), &payload)) == null);
}

test "maxPayloadPerFragment calculation" {
    const f1 = Fragmenter.init(.{ .mtu = 100 });
    try std.testing.expectEqual(@as(u32, 84), f1.maxPayloadPerFragment()); // 100 - 16

    const f2 = Fragmenter.init(.{ .mtu = 1472 });
    try std.testing.expectEqual(@as(u32, 1456), f2.maxPayloadPerFragment()); // 1472 - 16

    // Degenerate MTU saturates at 0 (and the iterator yields nothing
    // instead of looping forever).
    const f3 = Fragmenter.init(.{ .mtu = 8 });
    try std.testing.expectEqual(@as(u32, 0), f3.maxPayloadPerFragment());
    var iter = f3.fragmentIterator("some message", 0);
    try std.testing.expect(iter.next() == null);

    // Giant MTU is capped at the u16 fragment_length limit.
    const f4 = Fragmenter.init(.{ .mtu = 1 << 20 });
    try std.testing.expectEqual(@as(u32, std.math.maxInt(u16)), f4.maxPayloadPerFragment());
}
