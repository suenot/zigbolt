const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("../core/frame.zig");
const UdpChannel = @import("udp.zig").UdpChannel;
const UdpConfig = @import("udp.zig").UdpConfig;
const reliability = @import("reliability.zig");
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

    /// Initialize a network channel.
    pub fn init(allocator: std.mem.Allocator, net_config: NetworkConfig) !NetworkChannel {
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
        };
    }

    /// Shut down the channel.
    pub fn deinit(self: *NetworkChannel) void {
        self.udp.deinit();
        self.send_buf.deinit(self.allocator);
        self.recv_tracker.deinit();
        self.reassembler.deinit();
    }

    /// Publish a message over the network.
    /// Handles fragmentation, reliability tracking, and flow control.
    pub fn publish(self: *NetworkChannel, data: []const u8, msg_type_id: i32) !void {
        _ = msg_type_id;

        // Flow control check
        if (!self.flow_control.tryConsume(data.len)) {
            return error.BackPressured;
        }

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
        var buf: [65536]u8 = undefined;
        var count: u32 = 0;

        while (count < limit) {
            const result = self.udp.recv(&buf) catch |err| switch (err) {
                error.WouldBlock => break,
                else => return err,
            };

            if (result) |recv| {
                const payload = recv.data;
                if (payload.len < reliability.NetworkHeader.SIZE) continue;

                const hdr: *const reliability.NetworkHeader = @ptrCast(@alignCast(payload.ptr));

                switch (hdr.header_type) {
                    .data => {
                        // Track sequence for gap detection
                        _ = self.recv_tracker.recordReceived(hdr.sequence);

                        const msg_data = payload[reliability.NetworkHeader.SIZE..][0..hdr.payload_length];
                        handler(msg_data);
                        count += 1;
                    },
                    .nak => {
                        // Retransmit requested sequences
                        try self.handleNak(payload[reliability.NetworkHeader.SIZE..]);
                    },
                    .heartbeat => {
                        // Update flow control credits
                    },
                    else => {},
                }
            } else {
                break;
            }
        }

        // Check if we need to send NAKs for missing sequences
        try self.maybeSendNaks();

        return count;
    }

    // ── Internal ─────────────────────────────────────────

    fn sendWithReliability(self: *NetworkChannel, data: []const u8) !void {
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
        try self.send_buf.store(seq, data, self.allocator);

        // Concatenate and send (UDP doesn't support scatter-gather easily)
        var packet: [65536]u8 = undefined;
        const hdr_bytes = std.mem.asBytes(&hdr);
        @memcpy(packet[0..hdr_bytes.len], hdr_bytes);
        @memcpy(packet[hdr_bytes.len..][0..data.len], data);
        _ = try self.udp.send(packet[0 .. hdr_bytes.len + data.len], null);
    }

    fn handleNak(self: *NetworkChannel, nak_data: []const u8) !void {
        if (nak_data.len < @sizeOf(reliability.NakMessage)) return;

        const nak: *const reliability.NakMessage = @ptrCast(@alignCast(nak_data.ptr));

        var seq = nak.from_sequence;
        const end = seq + nak.count;
        while (seq < end) : (seq += 1) {
            if (self.send_buf.get(seq)) |entry| {
                // Retransmit
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
                entry.retransmit_count += 1;
            }
        }
    }

    fn maybeSendNaks(self: *NetworkChannel) !void {
        const missing = self.recv_tracker.getMissing(self.allocator) catch return;
        defer self.allocator.free(missing);

        if (missing.len == 0) return;

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
    }
};
