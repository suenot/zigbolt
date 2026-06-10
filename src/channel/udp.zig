const std = @import("std");
const config = @import("../platform/config.zig");
const frame = @import("../core/frame.zig");

const posix = std.posix;
const net = std.net;
const builtin = @import("builtin");

// ── Platform-specific constants ──────────────────────────────
// These IP-level options are not exposed by Zig's std.posix for Unix targets.
const IP_ADD_MEMBERSHIP: u32 = if (config.is_linux) 35 else 12; // Linux=35, macOS/BSD=12
const IP_MULTICAST_TTL: u32 = if (config.is_linux) 33 else 10; // Linux=33, macOS/BSD=10
const IP_MULTICAST_LOOP: u32 = if (config.is_linux) 34 else 11; // Linux=34, macOS/BSD=11

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
    /// Outgoing multicast TTL (hop limit). `null` keeps the OS default (1,
    /// i.e. multicast does not leave the local subnet).
    multicast_ttl: ?u8 = null,
    /// Whether outgoing multicast is looped back to the local host.
    /// `null` keeps the OS default (enabled).
    multicast_loop: ?bool = null,
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

        // Socket buffer sizes. SO_SNDBUF/SO_RCVBUF take an int: clamp a
        // configured size above 2 GB instead of panicking in @intCast
        // (the kernel clamps to its own maximum anyway).
        const max_buf: u32 = std.math.maxInt(i32);
        try setIntSockOpt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, @as(i32, @intCast(@min(udp_config.send_buffer_size, max_buf))));
        try setIntSockOpt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, @as(i32, @intCast(@min(udp_config.recv_buffer_size, max_buf))));

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

        // Multicast TTL / loopback (config-driven; null preserves the OS
        // defaults of TTL=1 and loop enabled).
        if (udp_config.multicast_ttl) |ttl| {
            try setIpByteOpt(fd, IP_MULTICAST_TTL, ttl);
        }
        if (udp_config.multicast_loop) |loop| {
            try setIpByteOpt(fd, IP_MULTICAST_LOOP, if (loop) 1 else 0);
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

        // Note: std.posix.recvfrom retries EINTR internally (`.INTR =>
        // continue`), so a signal during recv cannot abort the poll loop
        // and `error.Interrupted` is not part of its error set.
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

        // The caller's buffer has no alignment guarantee: copy the header
        // into an aligned local before casting (an @alignCast on an
        // unaligned `buf` would panic in Debug / be UB in release).
        var hdr_buf: [@sizeOf(frame.FrameHeader)]u8 align(@alignOf(frame.FrameHeader)) = undefined;
        @memcpy(&hdr_buf, result.data[0..@sizeOf(frame.FrameHeader)]);
        const hdr: *const frame.FrameHeader = @ptrCast(&hdr_buf);

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

    /// Set a byte-valued IPPROTO_IP option (multicast TTL/loop).
    /// Linux reads these as int; macOS/BSD expect a single byte.
    fn setIpByteOpt(fd: posix.socket_t, optname: u32, value: u8) !void {
        if (config.is_linux) {
            try setIntSockOpt(fd, posix.IPPROTO.IP, optname, @as(i32, value));
        } else {
            try posix.setsockopt(fd, posix.IPPROTO.IP, optname, std.mem.asBytes(&value));
        }
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

test "UdpChannel recvFrame works on an unaligned buffer" {
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

    const payload = "unaligned recv";
    try ch.sendFrame(payload, 7, actual_addr);

    // Receive into a deliberately misaligned slice: the header lands on an
    // odd address. The old @ptrCast(@alignCast(...)) panicked here.
    var raw: [512]u8 align(8) = undefined;
    const unaligned = raw[1..];

    var result: ?UdpChannel.FrameRecvResult = null;
    for (0..100) |_| {
        result = try ch.recvFrame(unaligned);
        if (result != null) break;
        std.Thread.sleep(100_000); // 100 μs
    }

    try std.testing.expect(result != null);
    try std.testing.expectEqualStrings(payload, result.?.payload);
    try std.testing.expectEqual(@as(i32, 7), result.?.msg_type_id);
}

test "UdpChannel huge socket buffer sizes do not panic" {
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    // > 2 GB used to panic in the u32 -> i32 @intCast before reaching the
    // kernel. The clamped value may still be rejected by the OS — an error
    // is acceptable, a panic is not.
    var ch = UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
        .send_buffer_size = 3 * 1024 * 1024 * 1024,
        .recv_buffer_size = 3 * 1024 * 1024 * 1024,
    }) catch return;
    ch.deinit();
}

test "UdpChannel multicast TTL and loop options" {
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
        .multicast_ttl = 4,
        .multicast_loop = true,
    });
    defer ch.deinit();

    try std.testing.expect(ch.socket_fd >= 0);
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

test "UdpChannel socket creation and deinit" {
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
        .send_buffer_size = 1024 * 1024,
        .recv_buffer_size = 1024 * 1024,
    });
    defer ch.deinit();

    // Socket fd should be valid (non-negative)
    try std.testing.expect(ch.socket_fd >= 0);
}

test "UdpChannel multiple send/recv" {
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
    });
    defer ch.deinit();

    // Discover actual port
    var bound_addr: posix.sockaddr align(4) = undefined;
    var bound_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    const rc = std.c.getsockname(ch.socket_fd, &bound_addr, &bound_len);
    if (rc != 0) return error.GetSockNameFailed;
    const actual_addr = net.Address.initPosix(&bound_addr);

    // Send multiple messages
    _ = try ch.send("first", actual_addr);
    _ = try ch.send("second", actual_addr);

    var buf: [256]u8 = undefined;
    var count: u32 = 0;

    for (0..200) |_| {
        const result = try ch.recv(&buf);
        if (result != null) {
            count += 1;
            if (count >= 2) break;
        }
        std.Thread.sleep(50_000);
    }

    try std.testing.expectEqual(@as(u32, 2), count);
}

test "UdpChannel send requires destination" {
    const bind_addr = net.Address.initIp4(.{ 127, 0, 0, 1 }, 0);

    var ch = try UdpChannel.init(.{
        .bind_address = bind_addr,
        .non_blocking = true,
        .remote_address = null,
    });
    defer ch.deinit();

    // Sending without a destination and no remote_address should fail
    const result = ch.send("test", null);
    try std.testing.expectError(error.DestinationRequired, result);
}
