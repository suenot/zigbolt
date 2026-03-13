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
};

/// Fragment header prepended to each fragment.
///
/// Layout (12 bytes total):
///   [0:4]  total_length:    u32 — total message length (same in all fragments)
///   [4:8]  fragment_offset: u32 — byte offset of this fragment within the message
///   [8:10] fragment_length: u16 — payload length of this fragment
///   [10]   flags:           u8  — begin/end flags
///   [11]   _reserved:       u8
pub const FragmentHeader = extern struct {
    /// Total message length (same in all fragments of a message).
    total_length: u32,
    /// Offset of this fragment within the message.
    fragment_offset: u32,
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
        if (@sizeOf(FragmentHeader) != 12) @compileError("FragmentHeader must be 12 bytes");
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
    pub fn maxPayloadPerFragment(self: *const Fragmenter) u32 {
        return self.frag_config.mtu - @as(u32, @intCast(FragmentHeader.SIZE));
    }

    /// Fragment a message. Calls `emit` for each fragment.
    /// Each call to emit provides: fragment header + payload slice.
    pub fn fragment(
        self: *const Fragmenter,
        message: []const u8,
        emit: *const fn (header: FragmentHeader, payload: []const u8) void,
    ) void {
        var iter = self.fragmentIterator(message);
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

        pub const Fragment = struct {
            header: FragmentHeader,
            payload: []const u8,
        };

        pub fn next(self: *FragmentIterator) ?Fragment {
            if (self.offset >= self.total_length) return null;

            const remaining = self.total_length - self.offset;
            const payload_len: u32 = @min(remaining, self.max_payload);
            const is_first = self.offset == 0;
            const is_last = (self.offset + payload_len) == self.total_length;

            const header = FragmentHeader{
                .total_length = self.total_length,
                .fragment_offset = self.offset,
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

    pub fn fragmentIterator(self: *const Fragmenter, message: []const u8) FragmentIterator {
        return .{
            .message = message,
            .offset = 0,
            .max_payload = self.maxPayloadPerFragment(),
            .total_length = @intCast(message.len),
        };
    }
};

/// Reassembles fragments back into complete messages.
pub const Reassembler = struct {
    allocator: std.mem.Allocator,
    frag_config: FragmentConfig,
    /// In-progress reassembly buffers, keyed by caller-provided key
    /// (typically session+stream+sequence).
    pending: std.AutoHashMap(u64, ReassemblyBuffer),

    const ReassemblyBuffer = struct {
        buffer: []u8,
        total_length: u32,
        received_bytes: u32,
        received_first: bool,
        received_last: bool,
        timestamp_ns: u64,
    };

    pub fn init(allocator: std.mem.Allocator, frag_config: FragmentConfig) Reassembler {
        return .{
            .allocator = allocator,
            .frag_config = frag_config,
            .pending = std.AutoHashMap(u64, ReassemblyBuffer).init(allocator),
        };
    }

    pub fn deinit(self: *Reassembler) void {
        var it = self.pending.valueIterator();
        while (it.next()) |buf| {
            self.allocator.free(buf.buffer);
        }
        self.pending.deinit();
    }

    /// Process an incoming fragment. Returns the complete reassembled message
    /// when all fragments have been received, or null if reassembly is still
    /// in progress. The caller owns the returned slice and must free it.
    pub fn processFragment(
        self: *Reassembler,
        key: u64,
        header: FragmentHeader,
        payload: []const u8,
    ) !?[]const u8 {
        // Validate fragment
        if (header.total_length > self.frag_config.max_message_size) {
            return error.MessageTooLarge;
        }
        if (header.fragment_length != @as(u16, @intCast(payload.len))) {
            return error.LengthMismatch;
        }
        if (@as(u64, header.fragment_offset) + payload.len > header.total_length) {
            return error.FragmentOutOfBounds;
        }

        // Single-fragment (unfragmented) message: fast path
        if (header.flags.begin and header.flags.end) {
            const result = try self.allocator.alloc(u8, header.total_length);
            @memcpy(result[0..payload.len], payload);
            return result;
        }

        // Get or create reassembly buffer
        const gop = try self.pending.getOrPut(key);
        if (!gop.found_existing) {
            const buffer = try self.allocator.alloc(u8, header.total_length);
            gop.value_ptr.* = .{
                .buffer = buffer,
                .total_length = header.total_length,
                .received_bytes = 0,
                .received_first = false,
                .received_last = false,
                .timestamp_ns = config.timestampNs(),
            };
        }

        const buf = gop.value_ptr;

        // Validate total_length consistency
        if (buf.total_length != header.total_length) {
            return error.InconsistentTotalLength;
        }

        // Copy payload into reassembly buffer
        @memcpy(buf.buffer[header.fragment_offset..][0..payload.len], payload);
        buf.received_bytes += @intCast(payload.len);

        if (header.flags.begin) buf.received_first = true;
        if (header.flags.end) buf.received_last = true;

        // Check if reassembly is complete
        if (buf.received_first and buf.received_last and buf.received_bytes == buf.total_length) {
            const result = buf.buffer;
            _ = self.pending.remove(key);
            return result;
        }

        return null;
    }

    /// Clean up stale reassembly buffers older than `max_age_ns` nanoseconds.
    pub fn cleanStale(self: *Reassembler, max_age_ns: u64) void {
        const now = config.timestampNs();

        // Collect keys to remove (cannot modify map while iterating)
        var keys_to_remove = std.ArrayList(u64).init(self.allocator);
        defer keys_to_remove.deinit();

        var it = self.pending.iterator();
        while (it.next()) |entry| {
            if (now -| entry.value_ptr.timestamp_ns > max_age_ns) {
                keys_to_remove.append(entry.key_ptr.*) catch continue;
            }
        }

        for (keys_to_remove.items) |key| {
            if (self.pending.fetchRemove(key)) |kv| {
                self.allocator.free(kv.value.buffer);
            }
        }
    }
};

// ── Tests ────────────────────────────────────────────────────

test "FragmentHeader size is 12 bytes" {
    try std.testing.expectEqual(@as(usize, 12), FragmentHeader.SIZE);
}

test "unfragmented message passes through" {
    const cfg = FragmentConfig{ .mtu = 1472 };
    const fragmenter = Fragmenter.init(cfg);

    // A small message that fits in a single fragment
    const msg = "hello zigbolt";
    try std.testing.expect(!fragmenter.needsFragmentation(msg.len));

    var iter = fragmenter.fragmentIterator(msg);
    const frag = iter.next().?;

    try std.testing.expect(frag.header.flags.begin);
    try std.testing.expect(frag.header.flags.end);
    try std.testing.expectEqual(@as(u32, msg.len), frag.header.total_length);
    try std.testing.expectEqual(@as(u32, 0), frag.header.fragment_offset);
    try std.testing.expectEqualSlices(u8, msg, frag.payload);

    // No more fragments
    try std.testing.expect(iter.next() == null);
}

test "fragment and reassemble large message" {
    const allocator = std.testing.allocator;
    // Use a small MTU to force fragmentation: header (12) + payload (88) = 100
    const cfg = FragmentConfig{ .mtu = 100, .max_message_size = 1 << 20 };
    const fragmenter = Fragmenter.init(cfg);
    var reassembler = Reassembler.init(allocator, cfg);
    defer reassembler.deinit();

    // Create a message larger than one fragment's payload (88 bytes)
    const msg_len: usize = 300;
    var msg: [msg_len]u8 = undefined;
    for (&msg, 0..) |*b, i| {
        b.* = @intCast(i % 256);
    }

    try std.testing.expect(fragmenter.needsFragmentation(msg_len));

    const max_payload = fragmenter.maxPayloadPerFragment();
    try std.testing.expectEqual(@as(u32, 88), max_payload);

    // Fragment and reassemble
    var iter = fragmenter.fragmentIterator(&msg);
    var fragment_count: usize = 0;
    var result: ?[]const u8 = null;

    while (iter.next()) |frag| {
        fragment_count += 1;
        const maybe_complete = try reassembler.processFragment(42, frag.header, frag.payload);
        if (maybe_complete) |complete| {
            result = complete;
        }
    }

    // 300 / 88 = 3 full + 1 partial = 4 fragments
    try std.testing.expectEqual(@as(usize, 4), fragment_count);

    // Reassembly should be complete
    const reassembled = result.?;
    defer allocator.free(reassembled);
    try std.testing.expectEqualSlices(u8, &msg, reassembled);
}

test "FragmentIterator produces correct fragments" {
    const cfg = FragmentConfig{ .mtu = 100 }; // 88 bytes payload per fragment
    const fragmenter = Fragmenter.init(cfg);

    // 200 bytes = ceil(200/88) = 3 fragments: 88 + 88 + 24
    var msg: [200]u8 = undefined;
    @memset(&msg, 0xAB);

    var iter = fragmenter.fragmentIterator(&msg);

    // First fragment
    const f1 = iter.next().?;
    try std.testing.expect(f1.header.flags.begin);
    try std.testing.expect(!f1.header.flags.end);
    try std.testing.expectEqual(@as(u32, 0), f1.header.fragment_offset);
    try std.testing.expectEqual(@as(u16, 88), f1.header.fragment_length);
    try std.testing.expectEqual(@as(usize, 88), f1.payload.len);
    try std.testing.expectEqual(@as(u32, 200), f1.header.total_length);

    // Second fragment
    const f2 = iter.next().?;
    try std.testing.expect(!f2.header.flags.begin);
    try std.testing.expect(!f2.header.flags.end);
    try std.testing.expectEqual(@as(u32, 88), f2.header.fragment_offset);
    try std.testing.expectEqual(@as(u16, 88), f2.header.fragment_length);

    // Third (last) fragment
    const f3 = iter.next().?;
    try std.testing.expect(!f3.header.flags.begin);
    try std.testing.expect(f3.header.flags.end);
    try std.testing.expectEqual(@as(u32, 176), f3.header.fragment_offset);
    try std.testing.expectEqual(@as(u16, 24), f3.header.fragment_length);

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
        .fragment_length = 88,
        .flags = .{ .begin = true },
    };

    var payload: [88]u8 = undefined;
    @memset(&payload, 0);

    const result = reassembler.processFragment(1, header, &payload);
    try std.testing.expectError(error.MessageTooLarge, result);
}
