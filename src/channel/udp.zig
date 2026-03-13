const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("../core/frame.zig");

const posix = std.posix;
const net = std.net;
const builtin = @import("builtin");

// ── Platform-specific constants ──────────────────────────────
// IP_ADD_MEMBERSHIP is not exposed by Zig's std.posix for Unix targets.
const IP_ADD_MEMBERSHIP: u32 = if (config.is_linux) 35 else 12; // Linux=35, macOS/BSD=12

/// ip_mreq structure for multicast group membership (POSIX).
const IpMreq = extern struct {
    /// Multicast group address (network byte order).
    imr_multiaddr: [4]u8,
    /// Local interface address (network byte order). 0.0.0.0 = any.
    imr_interface: [4]u8,
};

/// Configuration for a UDP channel.
pub const UdpConfig = struct {
    bind_address: net.Address,
    remote_address: ?net.Address = null, // default destination for unicast send
    multicast_group: ?[4]u8 = null, // IPv4 multicast group to join
    send_buffer_size: u32 = 2 * 1024 * 1024, // 2 MB SO_SNDBUF
    recv_buffer_size: u32 = 2 * 1024 * 1024, // 2 MB SO_RCVBUF
    non_blocking: bool = true,
};

/// UDP channel supporting unicast and multicast communication.
///
/// Provides raw datagram send/recv as well as framed send/recv
/// (prepending a ZigBolt `FrameHeader` to each datagram).
pub const UdpChannel = struct {
    socket_fd: posix.socket_t,
    udp_config: UdpConfig,

    /// Result of a raw recv operation.
    pub const RecvResult = struct {
        data: []const u8,
        from: net.Address,
    };

    /// Result of a framed recv operation.
    pub const FrameRecvResult = struct {
        payload: []const u8,
        msg_type_id: i32,
        from: net.Address,
    };

    /// Create and initialize a UDP channel.
    ///
    /// Opens a UDP socket, configures buffer sizes, sets non-blocking mode,
    /// binds to the configured address, and optionally joins a multicast group.
    pub fn init(udp_config: UdpConfig) !UdpChannel {
        // Create UDP socket. Use NONBLOCK flag if requested (Zig handles
        // cross-platform via fcntl on macOS).
        const sock_type: u32 = posix.SOCK.DGRAM |
            (if (udp_config.non_blocking) @as(u32, posix.SOCK.NONBLOCK) else @as(u32, 0));

        const fd = try posix.socket(posix.AF.INET, sock_type, posix.IPPROTO.UDP);
        errdefer posix.close(fd);

        // SO_REUSEADDR — allow rapid rebind and multicast membership from
        // multiple processes.
        try setIntSockOpt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, 1);

        // Socket buffer sizes.
        try setIntSockOpt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, @as(i32, @intCast(udp_config.send_buffer_size)));
        try setIntSockOpt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, @as(i32, @intCast(udp_config.recv_buffer_size)));

        // Bind.
        try posix.bind(fd, &udp_config.bind_address.any, udp_config.bind_address.getOsSockLen());

        // Join multicast group if configured.
        if (udp_config.multicast_group) |group| {
            const mreq = IpMreq{
                .imr_multiaddr = group,
                .imr_interface = .{ 0, 0, 0, 0 }, // INADDR_ANY
            };
            try posix.setsockopt(
                fd,
                posix.IPPROTO.IP,
                IP_ADD_MEMBERSHIP,
                std.mem.asBytes(&mreq),
            );
        }

        return .{
            .socket_fd = fd,
            .udp_config = udp_config,
        };
    }

    /// Close the socket and release resources.
    pub fn deinit(self: *UdpChannel) void {
        posix.close(self.socket_fd);
    }

    /// Send a raw datagram.
    ///
    /// If `dest` is provided, the datagram is sent to that address (unicast or
    /// multicast). Otherwise, the channel's configured `remote_address` is used.
    /// Returns the number of bytes sent.
    pub fn send(self: *UdpChannel, data: []const u8, dest: ?net.Address) !usize {
        const target = dest orelse self.udp_config.remote_address orelse return error.DestinationRequired;
        return try posix.sendto(
            self.socket_fd,
            data,
            0, // flags
            &target.any,
            target.getOsSockLen(),
        );
    }

    /// Receive a raw datagram (non-blocking: returns `null` when no data is
    /// available).
    ///
    /// The caller provides `buf` which will be filled with the received data.
    /// On success the returned `RecvResult.data` is a slice into `buf`.
    pub fn recv(self: *UdpChannel, buf: []u8) !?RecvResult {
        var src_addr: posix.sockaddr align(4) = undefined;
        var addrlen: posix.socklen_t = @sizeOf(posix.sockaddr);

        const n = posix.recvfrom(
            self.socket_fd,
            buf,
            0,
            &src_addr,
            &addrlen,
        ) catch |err| {
            if (err == error.WouldBlock) return null;
            return err;
        };

        return .{
            .data = buf[0..n],
            .from = net.Address.initPosix(&src_addr),
        };
    }

    /// Send a framed message: prepends a `FrameHeader` (8 bytes) containing
    /// the payload length and message type ID before the payload bytes in a
    /// single datagram.
    pub fn sendFrame(self: *UdpChannel, data: []const u8, msg_type_id: i32, dest: ?net.Address) !void {
        if (data.len > frame.MAX_PAYLOAD_SIZE) return error.MessageTooLarge;

        // Build header + payload into a stack buffer.
        // Max UDP payload is ~64 KB, and MAX_PAYLOAD_SIZE is 16 MB which
        // exceeds that, but we cap at practical UDP limits.
        const header_size = frame.FrameHeader.SIZE;
        const total: usize = header_size + data.len;
        if (total > 65507) return error.MessageTooLarge; // UDP max payload

        var packet: [65507]u8 align(@alignOf(frame.FrameHeader)) = undefined;

        // Write frame header.
        const hdr: *frame.FrameHeader = @ptrCast(@alignCast(&packet));
        hdr.frame_length = @intCast(data.len);
        hdr.msg_type_id = msg_type_id;

        // Write payload.
        @memcpy(packet[header_size..][0..data.len], data);

        _ = try self.send(packet[0..total], dest);
    }

    /// Receive and parse a framed message.
    ///
    /// Returns `null` when no data is available (non-blocking). The returned
    /// `FrameRecvResult.payload` is a slice into the caller-provided `buf`
    /// (offset past the header).
    pub fn recvFrame(self: *UdpChannel, buf: []u8) !?FrameRecvResult {
        const result = try self.recv(buf) orelse return null;

        const header_size = frame.FrameHeader.SIZE;
        if (result.data.len < header_size) return error.InvalidFrame;

        const hdr: *const frame.FrameHeader = @ptrCast(@alignCast(result.data.ptr));

        if (!frame.isDataFrame(hdr.frame_length)) return error.InvalidFrame;

        const payload_len: u32 = @intCast(hdr.frame_length);
        if (header_size + payload_len > result.data.len) return error.InvalidFrame;

        return .{
            .payload = result.data[header_size..][0..payload_len],
            .msg_type_id = hdr.msg_type_id,
            .from = result.from,
        };
    }

    // ── Helpers ──────────────────────────────────────────────

    /// Set an integer-valued socket option.
    fn setIntSockOpt(fd: posix.socket_t, level: i32, optname: u32, value: i32) !void {
        try posix.setsockopt(fd, level, optname, std.mem.asBytes(&value));
    }
};

// ── Tests ────────────────────────────────────────────────────

test "UdpChannel loopback send/recv" {
    // Bind to an ephemeral port on localhost.
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
    });
    defer ch.deinit();

    // Discover the actual bound port.
    var bound_addr: posix.sockaddr align(4) = undefined;
    var bound_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    const rc = std.c.getsockname(ch.socket_fd, &bound_addr, &bound_len);
    if (rc != 0) return error.GetSockNameFailed;
    const actual_addr = net.Address.initPosix(&bound_addr);

    const msg = "hello zigbolt udp";
    _ = try ch.send(msg, actual_addr);

    // Non-blocking recv — retry a few times (loopback may need a moment).
    var buf: [256]u8 = undefined;
    var result: ?UdpChannel.RecvResult = null;
    for (0..100) |_| {
        result = try ch.recv(&buf);
        if (result != null) break;
        std.Thread.sleep(100_000); // 100 μs
    }

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings(msg, result.?.data);
}

test "UdpChannel framed send/recv" {
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
    });
    defer ch.deinit();

    // Discover actual port.
    var bound_addr: posix.sockaddr align(4) = undefined;
    var bound_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    const rc = std.c.getsockname(ch.socket_fd, &bound_addr, &bound_len);
    if (rc != 0) return error.GetSockNameFailed;
    const actual_addr = net.Address.initPosix(&bound_addr);

    const payload = "framed payload";
    const msg_type: i32 = 42;

    try ch.sendFrame(payload, msg_type, actual_addr);

    var buf: [512]u8 = undefined;
    var result: ?UdpChannel.FrameRecvResult = null;
    for (0..100) |_| {
        result = try ch.recvFrame(&buf);
        if (result != null) break;
        std.Thread.sleep(100_000); // 100 μs
    }

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings(payload, result.?.payload);
    try std.testing.expectEqual(msg_type, result.?.msg_type_id);
}

test "UdpChannel non-blocking recv returns null when no data" {
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
    });
    defer ch.deinit();

    var buf: [256]u8 = undefined;
    const result = try ch.recv(&buf);

    try std.testing.expect(result == null);
}
