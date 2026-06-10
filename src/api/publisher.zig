const std = @import("std");
const IpcChannel = @import("../channel/ipc.zig").IpcChannel;
const WireCodec = @import("../codec/wire.zig").WireCodec;

/// A typed publisher that encodes messages via comptime WireCodec
/// and publishes them through an IPC channel.
pub fn Publisher(comptime MsgType: type) type {
    const Codec = WireCodec(MsgType);

    return struct {
        const Self = @This();

        channel: *IpcChannel,
        msg_type_id: i32,

        /// Create a publisher bound to a channel.
        pub fn init(channel: *IpcChannel, msg_type_id: i32) Self {
            return .{
                .channel = channel,
                .msg_type_id = msg_type_id,
            };
        }

        /// Publish a typed message: encoded via `WireCodec.encode` into a
        /// stack staging buffer, then committed to the shared-memory term
        /// by the channel. Propagates the channel's real error
        /// (`error.BackPressure`, `error.MessageTooLarge`,
        /// `error.CorruptChannel`).
        pub fn offer(self: *Self, msg: *const MsgType) !void {
            var buf: [Codec.wire_size]u8 = undefined;
            Codec.encode(msg, &buf);
            try self.channel.publish(&buf, self.msg_type_id);
        }

        /// Try to publish without blocking. Returns `false` only when the
        /// channel is back-pressured (`error.BackPressure`); other failures
        /// (`error.MessageTooLarge`, `error.CorruptChannel`) are surfaced
        /// as errors, not conflated with back-pressure.
        pub fn tryOffer(self: *Self, msg: *const MsgType) !bool {
            var buf: [Codec.wire_size]u8 = undefined;
            Codec.encode(msg, &buf);
            self.channel.publish(&buf, self.msg_type_id) catch |err| switch (err) {
                error.BackPressure => return false,
                else => return err,
            };
            return true;
        }

        /// Publish raw bytes (for pre-encoded messages).
        pub fn offerRaw(self: *Self, data: []const u8) !void {
            try self.channel.publish(data, self.msg_type_id);
        }
    };
}

/// An untyped publisher for raw byte messages.
pub const RawPublisher = struct {
    channel: *IpcChannel,
    msg_type_id: i32,

    pub fn init(channel: *IpcChannel, msg_type_id: i32) RawPublisher {
        return .{
            .channel = channel,
            .msg_type_id = msg_type_id,
        };
    }

    pub fn offer(self: *RawPublisher, data: []const u8) !void {
        try self.channel.publish(data, self.msg_type_id);
    }
};

// ── Tests ────────────────────────────────────────────────────

test "RawPublisher creation" {
    const name = "/zigbolt_test_pub_raw";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const pub_inst = RawPublisher.init(&ch, 42);
    try std.testing.expectEqual(@as(i32, 42), pub_inst.msg_type_id);
}

test "RawPublisher offer publishes data" {
    const name = "/zigbolt_test_pub_offer";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    var pub_inst = RawPublisher.init(&ch, 7);
    try pub_inst.offer("test payload");

    const S = struct {
        var received: bool = false;
        fn handler(result: IpcChannel.ReadResult) void {
            if (std.mem.eql(u8, result.data, "test payload")) {
                received = true;
            }
        }
    };

    S.received = false;
    const count = ch.poll(&S.handler, 10);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expect(S.received);
}

test "Publisher typed creation" {
    const TestMsg = packed struct {
        value: u64,
    };

    const name = "/zigbolt_test_pub_typed";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const pub_inst = Publisher(TestMsg).init(&ch, 10);
    try std.testing.expectEqual(@as(i32, 10), pub_inst.msg_type_id);
}

test "Publisher offer round-trips a Codec-encoded message" {
    const TickMessage = @import("../codec/wire.zig").TickMessage;
    const Codec = WireCodec(TickMessage);

    const name = "/zigbolt_test_pub_roundtrip";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    var pub_inst = Publisher(TickMessage).init(&ch, 21);

    const msg = TickMessage{
        .timestamp_ns = 1_700_000_000_000_000_000,
        .symbol_id = 42,
        .price = -123_456_789,
        .volume = 1000,
        .side = .ask,
    };
    try pub_inst.offer(&msg);

    const S = struct {
        var got: ?TickMessage = null;
        var type_id: i32 = 0;
        var len: usize = 0;
        fn handler(result: IpcChannel.ReadResult) void {
            len = result.data.len;
            type_id = result.msg_type_id;
            if (result.data.len >= Codec.wire_size) {
                got = Codec.decode(result.data).*;
            }
        }
    };
    S.got = null;

    try std.testing.expectEqual(@as(u32, 1), ch.poll(&S.handler, 10));
    try std.testing.expectEqual(Codec.wire_size, S.len);
    try std.testing.expectEqual(@as(i32, 21), S.type_id);
    const got = S.got.?;
    try std.testing.expectEqual(msg.timestamp_ns, got.timestamp_ns);
    try std.testing.expectEqual(msg.symbol_id, got.symbol_id);
    try std.testing.expectEqual(msg.price, got.price);
    try std.testing.expectEqual(msg.volume, got.volume);
    try std.testing.expectEqual(msg.side, got.side);
}

test "Publisher tryOffer returns false on back-pressure, true otherwise" {
    const TickMessage = @import("../codec/wire.zig").TickMessage;

    const name = "/zigbolt_test_pub_bp";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    var pub_inst = Publisher(TickMessage).init(&ch, 3);

    const msg = TickMessage{
        .timestamp_ns = 1,
        .symbol_id = 1,
        .price = 1,
        .volume = 1,
        .side = .bid,
    };

    // First offer must succeed.
    try std.testing.expect(try pub_inst.tryOffer(&msg));

    // With no consumer, the channel must eventually back-pressure and
    // tryOffer must report it as `false` (not an error). Frame = 40 bytes,
    // window = 2 * 4096 bytes -> well under 1000 iterations.
    var back_pressured = false;
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        if (!(try pub_inst.tryOffer(&msg))) {
            back_pressured = true;
            break;
        }
    }
    try std.testing.expect(back_pressured);
}
