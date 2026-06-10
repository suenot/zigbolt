const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("../core/frame.zig");
const UdpChannel = @import("udp.zig").UdpChannel;
const UdpConfig = @import("udp.zig").UdpConfig;
const reliability = @import("reliability.zig");
const congestion = @import("congestion.zig");
const frag = @import("fragment.zig");

/// Network channel configuration.
pub const NetworkConfig = struct {
    /// UDP transport config.
    udp: UdpConfig,
    /// Session ID for this channel.
    session_id: u32 = 1,
    /// Stream ID for this channel.
    stream_id: u32 = 1,
    /// Reliability settings.
    send_buffer_capacity: usize = 4096,
    recv_window_size: u64 = 4096,
    /// Flow control window (bytes).
    flow_control_window: i64 = 4 * 1024 * 1024, // 4 MB
    /// Fragmentation settings.
    mtu: u32 = 1472,
    max_message_size: u32 = 1 << 20,
    /// Heartbeat interval in nanoseconds.
    heartbeat_interval_ns: u64 = 100_000_000, // 100ms
    /// NAK delay in nanoseconds (wait before sending NAK to allow reordering).
    nak_delay_ns: u64 = 1_000_000, // 1ms
    /// Pin the expected peer address: when set, datagrams from any other
    /// source address are dropped. UDP source addresses are forgeable, so
    /// this filters off-path noise/abuse only — it is NOT authentication.
    expected_peer: ?std.net.Address = null,
    /// NAK-amplification defence: at most this many payload retransmissions
    /// are performed per `retransmit_interval_ns` window.
    max_retransmits_per_interval: u32 = 1024,
    /// Token-refill interval for the retransmission rate limit.
    retransmit_interval_ns: u64 = 10_000_000, // 10ms
};

/// A reliable, ordered network channel over UDP.
///
/// Combines:
/// - UDP unicast/multicast transport
/// - NAK-based reliability (retransmission on gaps)
/// - Credit-based flow control
/// - Fragmentation/reassembly for large messages
pub const NetworkChannel = struct {
    udp: UdpChannel,
    send_buf: reliability.SendBuffer,
    recv_tracker: reliability.RecvTracker,
    flow_control: reliability.FlowControl,
    fragmenter: frag.Fragmenter,
    reassembler: frag.Reassembler,
    config: NetworkConfig,
    next_sequence: u64,
    allocator: std.mem.Allocator,
    last_heartbeat_ns: u64,
    /// Pre-allocated scratch for missing-sequence scans (no allocation on
    /// the poll hot path).
    missing_buf: []u64,
    /// NAK timing/backoff so the same gap is not re-NAK'd every poll.
    nak_controller: congestion.NakController,
    /// Leading missing sequence the NAK backoff currently applies to.
    current_gap_from: ?u64,
    /// Remaining retransmission tokens in the current rate-limit window.
    retransmit_tokens: u32,
    /// Start of the current retransmission rate-limit window.
    retransmit_window_start_ns: u64,

    /// Initialize a network channel.
    pub fn init(allocator: std.mem.Allocator, net_config: NetworkConfig) !NetworkChannel {
        const missing_buf = try allocator.alloc(u64, @intCast(net_config.recv_window_size));
        errdefer allocator.free(missing_buf);
        return .{
            .udp = try UdpChannel.init(net_config.udp),
            .send_buf = try reliability.SendBuffer.init(allocator, net_config.send_buffer_capacity),
            .recv_tracker = try reliability.RecvTracker.init(allocator, net_config.recv_window_size),
            .flow_control = reliability.FlowControl.init(net_config.flow_control_window),
            .fragmenter = frag.Fragmenter.init(.{
                .mtu = net_config.mtu,
                .max_message_size = net_config.max_message_size,
            }),
            .reassembler = frag.Reassembler.init(allocator, .{
                .mtu = net_config.mtu,
                .max_message_size = net_config.max_message_size,
            }),
            .config = net_config,
            .next_sequence = 0,
            .allocator = allocator,
            .last_heartbeat_ns = 0,
            .missing_buf = missing_buf,
            .nak_controller = congestion.NakController.init(.{
                .base_delay_ns = net_config.nak_delay_ns,
            }),
            .current_gap_from = null,
            .retransmit_tokens = net_config.max_retransmits_per_interval,
            .retransmit_window_start_ns = 0,
        };
    }

    /// Shut down the channel.
    pub fn deinit(self: *NetworkChannel) void {
        self.udp.deinit();
        self.send_buf.deinit(self.allocator);
        self.recv_tracker.deinit();
        self.reassembler.deinit();
        self.allocator.free(self.missing_buf);
    }

    /// Publish a message over the network.
    /// Handles fragmentation, reliability tracking, and flow control.
    ///
    /// Flow control is credit-based on bytes in flight: credits are
    /// consumed per datagram in `sendWithReliability` and replenished when
    /// the receiver's heartbeat acknowledges delivery (or when an entry is
    /// evicted from the retransmit buffer), so a healthy channel keeps
    /// publishing indefinitely.
    pub fn publish(self: *NetworkChannel, data: []const u8, msg_type_id: i32) !void {
        _ = msg_type_id;

        if (self.fragmenter.needsFragmentation(data.len)) {
            // Fragment and send each piece
            var iter = self.fragmenter.fragmentIterator(data);
            while (iter.next()) |fragment| {
                try self.sendWithReliability(fragment.payload);
            }
        } else {
            try self.sendWithReliability(data);
        }
    }

    /// Poll for incoming messages.
    /// Handles reassembly, NAK generation, and heartbeat processing.
    /// Returns the number of complete messages delivered.
    pub fn poll(self: *NetworkChannel, handler: *const fn (data: []const u8) void, limit: u32) !u32 {
        // Align the recv buffer for safe header casting (NET-6, BUF-6).
        var buf: [65536]u8 align(@alignOf(reliability.NetworkHeader)) = undefined;
        var count: u32 = 0;

        while (count < limit) {
            const result = self.udp.recv(&buf) catch |err| switch (err) {
                error.WouldBlock => break,
                else => return err,
            };

            if (result) |recv| {
                // Optional source pinning: drop datagrams that do not come
                // from the configured peer (off-path noise filter only —
                // UDP sources are forgeable by an on-path attacker).
                if (self.config.expected_peer) |peer| {
                    if (!recv.from.eql(peer)) continue;
                }

                const payload = recv.data;
                if (payload.len < reliability.NetworkHeader.SIZE) continue;

                // Safe header read: copy into aligned local (H8 fix).
                var hdr_buf: [@sizeOf(reliability.NetworkHeader)]u8 align(@alignOf(reliability.NetworkHeader)) = undefined;
                @memcpy(&hdr_buf, payload[0..@sizeOf(reliability.NetworkHeader)]);
                const hdr: *const reliability.NetworkHeader = @ptrCast(&hdr_buf);

                // Filter on session/stream ID. NOTE: these travel in
                // cleartext and carry no authentication — they reject
                // mis-addressed/stale traffic, not a deliberate spoofer
                // (set `expected_peer` to additionally pin the source).
                if (hdr.session_id != self.config.session_id or hdr.stream_id != self.config.stream_id) continue;

                // NET-5: Validate protocol version.
                if (hdr.version != 1) continue;

                // The header-type byte comes straight off the wire: map it
                // through intToEnum so values 5..255 are a graceful drop,
                // not a "switch on corrupt value" panic (remote-crash DoS
                // in Debug builds). Never load the enum field directly.
                const raw_type = hdr_buf[@offsetOf(reliability.NetworkHeader, "header_type")];
                const header_type = std.meta.intToEnum(
                    reliability.NetworkHeader.HeaderType,
                    raw_type,
                ) catch continue;

                switch (header_type) {
                    .data => {
                        // NET-1 CRITICAL: Validate payload_length against actual packet size.
                        if (reliability.NetworkHeader.SIZE + hdr.payload_length > payload.len) continue;

                        // Track the sequence; drop duplicates/replays and
                        // out-of-window sequences (delivered at most once,
                        // in arrival order).
                        const rec = self.recv_tracker.recordReceived(hdr.sequence);
                        if (!rec.accepted) continue;

                        const msg_data = payload[reliability.NetworkHeader.SIZE..][0..hdr.payload_length];
                        handler(msg_data);
                        count += 1;
                    },
                    .nak => {
                        // Retransmit requested sequences
                        try self.handleNak(payload[reliability.NetworkHeader.SIZE..]);
                    },
                    .heartbeat => {
                        // Receiver status: `sequence` carries the peer's
                        // contiguous receive position. Release acked
                        // entries and return their bytes to flow control.
                        // Clamp to what we actually sent so a forged ack
                        // cannot release more than is outstanding.
                        const acked = @min(hdr.sequence, self.next_sequence);
                        const released = self.send_buf.release(acked);
                        if (released > 0) self.flow_control.replenish(released);
                    },
                    .setup, .teardown => {},
                }
            } else {
                break;
            }
        }

        // Check if we need to send NAKs for missing sequences
        self.maybeSendNaks();

        // Advertise our own receive progress to the sender.
        self.maybeSendHeartbeat();

        return count;
    }

    // ── Internal ─────────────────────────────────────────

    fn sendWithReliability(self: *NetworkChannel, data: []const u8) !void {
        // NET-4: Validate combined size fits in packet buffer.
        const hdr_size = @sizeOf(reliability.NetworkHeader);
        if (hdr_size + data.len > 65536) return error.MessageTooLarge;

        // Flow control: consume in-flight credits for this datagram. They
        // come back when the receiver's heartbeat acks delivery, or below
        // if the retransmit slot evicts an older (now unrecoverable) entry.
        if (!self.flow_control.tryConsume(data.len)) {
            return error.BackPressured;
        }

        const seq = self.next_sequence;
        self.next_sequence += 1;

        // Build network header
        var hdr = reliability.NetworkHeader{
            .header_type = .data,
            .session_id = self.config.session_id,
            .stream_id = self.config.stream_id,
            .sequence = seq,
            .payload_length = @intCast(data.len),
        };

        // Store for retransmission
        const evicted = self.send_buf.store(seq, data, self.allocator) catch |err| {
            // Credits must not leak if the copy cannot be stored.
            self.flow_control.replenish(data.len);
            return err;
        };
        // Bytes evicted from an overwritten ring slot can never be
        // retransmitted — stop counting them as in flight.
        if (evicted > 0) self.flow_control.replenish(evicted);

        // Concatenate and send (UDP doesn't support scatter-gather easily)
        var packet: [65536]u8 = undefined;
        const hdr_bytes = std.mem.asBytes(&hdr);
        @memcpy(packet[0..hdr_bytes.len], hdr_bytes);
        @memcpy(packet[hdr_bytes.len..][0..data.len], data);
        _ = try self.udp.send(packet[0 .. hdr_bytes.len + data.len], null);
    }

    fn handleNak(self: *NetworkChannel, nak_data: []const u8) !void {
        if (nak_data.len < @sizeOf(reliability.NakMessage)) return;

        // Safe unaligned read (NET-6 fix).
        var nak_buf: [@sizeOf(reliability.NakMessage)]u8 align(@alignOf(reliability.NakMessage)) = undefined;
        @memcpy(&nak_buf, nak_data[0..@sizeOf(reliability.NakMessage)]);
        const nak: *const reliability.NakMessage = @ptrCast(&nak_buf);

        // The inner NAK ids must match this channel too — the outer header
        // was validated, but a corrupt/forged body is dropped here.
        if (nak.session_id != self.config.session_id or nak.stream_id != self.config.stream_id) return;

        // Only sequences we have actually sent can be retransmitted. This
        // also bounds the range: a wire `from_sequence` near u64 max used
        // to overflow `from + count` and panic.
        if (nak.from_sequence >= self.next_sequence) return;

        // NAK-amplification defence: rate-limit retransmission work to
        // `max_retransmits_per_interval` payloads per interval, no matter
        // how many NAKs arrive.
        const now = config.monotonicNs();
        if (now -| self.retransmit_window_start_ns >= self.config.retransmit_interval_ns) {
            self.retransmit_window_start_ns = now;
            self.retransmit_tokens = self.config.max_retransmits_per_interval;
        }
        if (self.retransmit_tokens == 0) return;

        // NET-2: Cap NAK count to prevent amplification attacks.
        const max_nak_count: u64 = @min(self.config.send_buffer_capacity, 256);
        const capped_count = @min(@min(nak.count, max_nak_count), self.retransmit_tokens);

        var seq = nak.from_sequence;
        // Saturating add plus the live send range bound: cannot overflow.
        const end = @min(seq +| capped_count, self.next_sequence);
        while (seq < end) : (seq += 1) {
            if (self.send_buf.get(seq)) |entry| {
                // Retransmit. NOTE: retransmits go to the configured remote
                // (`send(.., null)`), never back to the datagram's source —
                // this prevents NAK-reflection to spoofed victims.
                var hdr = reliability.NetworkHeader{
                    .header_type = .data,
                    .session_id = self.config.session_id,
                    .stream_id = self.config.stream_id,
                    .sequence = seq,
                    .payload_length = @intCast(entry.data.len),
                };
                var packet: [65536]u8 = undefined;
                const hdr_bytes = std.mem.asBytes(&hdr);
                @memcpy(packet[0..hdr_bytes.len], hdr_bytes);
                @memcpy(packet[hdr_bytes.len..][0..entry.data.len], entry.data);
                _ = self.udp.send(packet[0 .. hdr_bytes.len + entry.data.len], null) catch {};
                // Saturating: a NAK flood must not overflow the u8 counter.
                entry.retransmit_count +|= 1;
                self.retransmit_tokens -= 1;
            }
        }
    }

    fn maybeSendNaks(self: *NetworkChannel) void {
        // Zero allocation: scan into the buffer pre-allocated at init.
        const missing_count = self.recv_tracker.getMissingInto(self.missing_buf);
        if (missing_count == 0) {
            // Gap (if any) was filled: reset the NAK backoff for the next one.
            if (self.current_gap_from != null) {
                self.current_gap_from = null;
                self.nak_controller.onGapFilled();
            }
            return;
        }
        const missing = self.missing_buf[0..missing_count];

        // A different leading gap is a new loss event: restart the backoff.
        if (self.current_gap_from == null or self.current_gap_from.? != missing[0]) {
            self.nak_controller.onGapFilled();
            self.current_gap_from = missing[0];
        }

        // Backoff gate: do not re-NAK the same gap every poll (NAK storm).
        const now = config.monotonicNs();
        if (!self.nak_controller.shouldSendNak(now)) return;

        // Build NAK message for first contiguous gap
        var nak = reliability.NakMessage{
            .session_id = self.config.session_id,
            .stream_id = self.config.stream_id,
            .from_sequence = missing[0],
            .count = 1,
        };

        // Count contiguous missing sequences
        for (missing[1..]) |seq| {
            if (seq == nak.from_sequence + nak.count) {
                nak.count += 1;
            } else {
                break;
            }
        }

        // Send NAK
        var hdr = reliability.NetworkHeader{
            .header_type = .nak,
            .session_id = self.config.session_id,
            .stream_id = self.config.stream_id,
            .sequence = 0,
            .payload_length = @intCast(@sizeOf(reliability.NakMessage)),
        };

        var packet: [256]u8 = undefined;
        const hdr_bytes = std.mem.asBytes(&hdr);
        const nak_bytes = std.mem.asBytes(&nak);
        @memcpy(packet[0..hdr_bytes.len], hdr_bytes);
        @memcpy(packet[hdr_bytes.len..][0..nak_bytes.len], nak_bytes);
        _ = self.udp.send(packet[0 .. hdr_bytes.len + nak_bytes.len], null) catch {};
        self.nak_controller.onNakSent(now);
    }

    /// Periodically advertise our contiguous receive position so the
    /// sender can release its retransmit buffer and replenish its
    /// flow-control credits.
    fn maybeSendHeartbeat(self: *NetworkChannel) void {
        if (self.udp.udp_config.remote_address == null) return;

        const now = config.monotonicNs();
        if (now -| self.last_heartbeat_ns < self.config.heartbeat_interval_ns) return;
        self.last_heartbeat_ns = now;

        var hdr = reliability.NetworkHeader{
            .header_type = .heartbeat,
            .session_id = self.config.session_id,
            .stream_id = self.config.stream_id,
            .sequence = self.recv_tracker.contiguousPosition(),
            .payload_length = 0,
        };
        _ = self.udp.send(std.mem.asBytes(&hdr), null) catch {};
    }
};

// ── Tests ────────────────────────────────────────────────────
const net = std.net;
const posix = std.posix;

// Handler state for poll() tests (the handler signature has no context
// pointer; tests run sequentially in-process and reset these first).
var g_delivered_count: u32 = 0;
var g_delivered_bytes: usize = 0;

fn testCountingHandler(data: []const u8) void {
    g_delivered_count += 1;
    g_delivered_bytes += data.len;
}

fn testNoopHandler(data: []const u8) void {
    _ = data;
}

fn boundAddress(fd: posix.socket_t) !net.Address {
    var bound_addr: posix.sockaddr align(4) = undefined;
    var bound_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    const rc = std.c.getsockname(fd, &bound_addr, &bound_len);
    if (rc != 0) return error.GetSockNameFailed;
    return net.Address.initPosix(&bound_addr);
}

fn buildDataPacket(buf: []u8, sequence: u64, payload: []const u8) []const u8 {
    var hdr = reliability.NetworkHeader{
        .header_type = .data,
        .session_id = 1,
        .stream_id = 1,
        .sequence = sequence,
        .payload_length = @intCast(payload.len),
    };
    const hdr_bytes = std.mem.asBytes(&hdr);
    @memcpy(buf[0..hdr_bytes.len], hdr_bytes);
    @memcpy(buf[hdr_bytes.len..][0..payload.len], payload);
    return buf[0 .. hdr_bytes.len + payload.len];
}

fn buildHeartbeatPacket(buf: []u8, ack_sequence: u64) []const u8 {
    var hdr = reliability.NetworkHeader{
        .header_type = .heartbeat,
        .session_id = 1,
        .stream_id = 1,
        .sequence = ack_sequence,
        .payload_length = 0,
    };
    const hdr_bytes = std.mem.asBytes(&hdr);
    @memcpy(buf[0..hdr_bytes.len], hdr_bytes);
    return buf[0..hdr_bytes.len];
}

fn buildNakPacket(buf: []u8, inner_session: u32, inner_stream: u32, from_sequence: u64, nak_count: u32) []const u8 {
    var hdr = reliability.NetworkHeader{
        .header_type = .nak,
        .session_id = 1,
        .stream_id = 1,
        .sequence = 0,
        .payload_length = @intCast(@sizeOf(reliability.NakMessage)),
    };
    var nak = reliability.NakMessage{
        .session_id = inner_session,
        .stream_id = inner_stream,
        .from_sequence = from_sequence,
        .count = nak_count,
    };
    const hdr_bytes = std.mem.asBytes(&hdr);
    const nak_bytes = std.mem.asBytes(&nak);
    @memcpy(buf[0..hdr_bytes.len], hdr_bytes);
    @memcpy(buf[hdr_bytes.len..][0..nak_bytes.len], nak_bytes);
    return buf[0 .. hdr_bytes.len + nak_bytes.len];
}

fn pollUntilDelivered(ch: *NetworkChannel, want: u32) !void {
    for (0..200) |_| {
        _ = try ch.poll(testCountingHandler, 16);
        if (g_delivered_count >= want) return;
        std.Thread.sleep(100_000); // 100 μs
    }
}

test "NetworkChannel init and deinit" {
    const allocator = std.testing.allocator;
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = bind_addr,
            .non_blocking = true,
        },
        .session_id = 1,
        .stream_id = 1,
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
        .flow_control_window = 1024 * 1024,
    });
    defer ch.deinit();

    try std.testing.expectEqual(@as(u64, 0), ch.next_sequence);
    try std.testing.expectEqual(@as(u32, 1), ch.config.session_id);
    try std.testing.expectEqual(@as(u32, 1), ch.config.stream_id);
}

test "NetworkChannel publish with flow control" {
    const allocator = std.testing.allocator;
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    // Discover actual bound port so sendWithReliability has a destination
    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = bind_addr,
            .non_blocking = true,
            .remote_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9999),
        },
        .session_id = 1,
        .stream_id = 1,
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
        .flow_control_window = 100,
    });
    defer ch.deinit();

    // First publish should succeed (under flow control window)
    try ch.publish("small", 1);
    try std.testing.expectEqual(@as(u64, 1), ch.next_sequence);

    // Publish should fail when flow control is exhausted
    const result = ch.publish(&[_]u8{0xAA} ** 200, 1);
    try std.testing.expectError(error.BackPressured, result);
}

test "NetworkChannel publish increments sequence" {
    const allocator = std.testing.allocator;
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = bind_addr,
            .non_blocking = true,
            .remote_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9999),
        },
        .session_id = 1,
        .stream_id = 1,
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
        .flow_control_window = 4 * 1024 * 1024,
    });
    defer ch.deinit();

    try ch.publish("msg1", 1);
    try ch.publish("msg2", 2);
    try ch.publish("msg3", 3);

    try std.testing.expectEqual(@as(u64, 3), ch.next_sequence);
}

test "NetworkChannel drops unknown header type instead of panicking" {
    const allocator = std.testing.allocator;
    g_delivered_count = 0;
    g_delivered_bytes = 0;

    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
            .non_blocking = true,
        },
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
    });
    defer ch.deinit();
    const ch_addr = try boundAddress(ch.udp.socket_fd);

    var attacker = try UdpChannel.init(.{
        .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
        .non_blocking = true,
    });
    defer attacker.deinit();

    // Datagram with a wire header_type byte of 200 (valid enum tags are
    // 0..4). Before the fix, poll() panicked with "switch on corrupt
    // value" — a guaranteed remote-crash DoS in Debug builds.
    var hostile_buf: [128]u8 = undefined;
    const hostile = buildDataPacket(&hostile_buf, 0, "boom!");
    hostile_buf[@offsetOf(reliability.NetworkHeader, "header_type")] = 200;
    _ = try attacker.send(hostile, ch_addr);

    // A valid datagram behind it must still be delivered.
    var valid_buf: [128]u8 = undefined;
    const valid = buildDataPacket(&valid_buf, 0, "ok");
    _ = try attacker.send(valid, ch_addr);

    try pollUntilDelivered(&ch, 1);
    try std.testing.expectEqual(@as(u32, 1), g_delivered_count);
    try std.testing.expectEqual(@as(usize, 2), g_delivered_bytes); // "ok" only
}

test "NetworkChannel survives NAK with from_sequence near u64 max and checks inner ids" {
    const allocator = std.testing.allocator;

    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
            .non_blocking = true,
            .remote_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9999),
        },
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
    });
    defer ch.deinit();
    const ch_addr = try boundAddress(ch.udp.socket_fd);

    var attacker = try UdpChannel.init(.{
        .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
        .non_blocking = true,
    });
    defer attacker.deinit();

    try ch.publish("msg1", 1);
    try ch.publish("msg2", 1);

    // 1) from_sequence = u64 max: `from + count` used to overflow & panic.
    var b1: [128]u8 = undefined;
    _ = try attacker.send(buildNakPacket(&b1, 1, 1, std.math.maxInt(u64), 5), ch_addr);
    // 2) Inner session id mismatch: must be ignored entirely.
    var b2: [128]u8 = undefined;
    _ = try attacker.send(buildNakPacket(&b2, 99, 1, 0, 1), ch_addr);
    // 3) Valid NAK with a huge count: capped, retransmits both messages.
    var b3: [128]u8 = undefined;
    _ = try attacker.send(buildNakPacket(&b3, 1, 1, 0, std.math.maxInt(u32)), ch_addr);

    // Poll until the valid NAK (sent last, FIFO on loopback) is processed.
    for (0..200) |_| {
        _ = try ch.poll(testNoopHandler, 16);
        if (ch.send_buf.get(0)) |e| {
            if (e.retransmit_count > 0) break;
        }
        std.Thread.sleep(100_000);
    }

    // Exactly one retransmission each: the mismatched-inner-id NAK (which
    // also targeted sequence 0) must NOT have triggered a retransmit.
    try std.testing.expectEqual(@as(u8, 1), ch.send_buf.get(0).?.retransmit_count);
    try std.testing.expectEqual(@as(u8, 1), ch.send_buf.get(1).?.retransmit_count);

    // Channel still functional.
    try ch.publish("msg3", 1);
    try std.testing.expectEqual(@as(u64, 3), ch.next_sequence);
}

test "NetworkChannel rejects DATA with sequence u64 max as out-of-window" {
    const allocator = std.testing.allocator;
    g_delivered_count = 0;
    g_delivered_bytes = 0;

    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
            .non_blocking = true,
        },
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
    });
    defer ch.deinit();
    const ch_addr = try boundAddress(ch.udp.socket_fd);

    var attacker = try UdpChannel.init(.{
        .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
        .non_blocking = true,
    });
    defer attacker.deinit();

    // Spoofed far-ahead sequence: used to overflow `sequence + 1` (panic)
    // and jump next_expected ~2^64 ahead, causing perpetual bogus NAKs.
    var evil_buf: [128]u8 = undefined;
    _ = try attacker.send(buildDataPacket(&evil_buf, std.math.maxInt(u64), "evil!"), ch_addr);

    var good_buf: [128]u8 = undefined;
    _ = try attacker.send(buildDataPacket(&good_buf, 0, "good"), ch_addr);

    try pollUntilDelivered(&ch, 1);
    try std.testing.expectEqual(@as(u32, 1), g_delivered_count);
    try std.testing.expectEqual(@as(usize, 4), g_delivered_bytes); // "good" only
    try std.testing.expectEqual(@as(u64, 1), ch.recv_tracker.next_expected);
}

test "NetworkChannel delivers duplicated DATA sequence only once" {
    const allocator = std.testing.allocator;
    g_delivered_count = 0;
    g_delivered_bytes = 0;

    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
            .non_blocking = true,
        },
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
    });
    defer ch.deinit();
    const ch_addr = try boundAddress(ch.udp.socket_fd);

    var attacker = try UdpChannel.init(.{
        .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
        .non_blocking = true,
    });
    defer attacker.deinit();

    var b0: [128]u8 = undefined;
    _ = try attacker.send(buildDataPacket(&b0, 0, "a"), ch_addr);
    // Replay of sequence 0: must be dropped (was delivered twice before).
    var b1: [128]u8 = undefined;
    _ = try attacker.send(buildDataPacket(&b1, 0, "a"), ch_addr);
    var b2: [128]u8 = undefined;
    _ = try attacker.send(buildDataPacket(&b2, 1, "b"), ch_addr);

    // Loopback is FIFO: once "b" (sent after the replay) is delivered, the
    // replay has necessarily been processed.
    try pollUntilDelivered(&ch, 2);
    try std.testing.expectEqual(@as(u32, 2), g_delivered_count);
    try std.testing.expectEqual(@as(usize, 2), g_delivered_bytes); // "a" + "b"
    try std.testing.expectEqual(@as(u64, 2), ch.recv_tracker.next_expected);
}

test "NetworkChannel flow control replenishes via heartbeat acks" {
    const allocator = std.testing.allocator;

    const window: i64 = 64;
    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
            .non_blocking = true,
            .remote_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9999),
        },
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
        .flow_control_window = window,
    });
    defer ch.deinit();
    const ch_addr = try boundAddress(ch.udp.socket_fd);

    var receiver = try UdpChannel.init(.{
        .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
        .non_blocking = true,
    });
    defer receiver.deinit();

    const msg = [_]u8{0xAB} ** 16; // 4 messages fill the 64-byte window

    // Publish 5 windows' worth of data (320 bytes through a 64-byte
    // window). Before the fix, credits were never replenished: after the
    // first window every publish returned BackPressured forever.
    var round: u32 = 0;
    while (round < 5) : (round += 1) {
        var i: u32 = 0;
        while (i < 4) : (i += 1) {
            try ch.publish(&msg, 1);
        }
        // Window exhausted.
        try std.testing.expectError(error.BackPressured, ch.publish(&msg, 1));
        try std.testing.expectEqual(@as(i64, 0), ch.flow_control.available());

        // Receiver acks everything sent so far via heartbeat.
        var hb_buf: [64]u8 = undefined;
        _ = try receiver.send(buildHeartbeatPacket(&hb_buf, ch.next_sequence), ch_addr);

        // Poll until the heartbeat is processed and credits return.
        for (0..200) |_| {
            _ = try ch.poll(testNoopHandler, 16);
            if (ch.flow_control.available() >= window) break;
            std.Thread.sleep(100_000);
        }
        try std.testing.expectEqual(window, ch.flow_control.available());
    }

    // 20 messages of 16 bytes went through a 64-byte window.
    try std.testing.expectEqual(@as(u64, 20), ch.next_sequence);
}

test "NetworkChannel expected_peer pins the source address" {
    const allocator = std.testing.allocator;
    g_delivered_count = 0;
    g_delivered_bytes = 0;

    var trusted = try UdpChannel.init(.{
        .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
        .non_blocking = true,
    });
    defer trusted.deinit();
    const trusted_addr = try boundAddress(trusted.socket_fd);

    var ch = try NetworkChannel.init(allocator, .{
        .udp = .{
            .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
            .non_blocking = true,
        },
        .send_buffer_capacity = 64,
        .recv_window_size = 64,
        .expected_peer = trusted_addr,
    });
    defer ch.deinit();
    const ch_addr = try boundAddress(ch.udp.socket_fd);

    var stranger = try UdpChannel.init(.{
        .bind_address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0),
        .non_blocking = true,
    });
    defer stranger.deinit();

    // Datagram from a non-pinned source: dropped.
    var s_buf: [128]u8 = undefined;
    _ = try stranger.send(buildDataPacket(&s_buf, 0, "zz"), ch_addr);
    // Datagram from the pinned peer: delivered.
    var t_buf: [128]u8 = undefined;
    _ = try trusted.send(buildDataPacket(&t_buf, 0, "y"), ch_addr);

    try pollUntilDelivered(&ch, 1);
    try std.testing.expectEqual(@as(u32, 1), g_delivered_count);
    try std.testing.expectEqual(@as(usize, 1), g_delivered_bytes); // "y" only
}
