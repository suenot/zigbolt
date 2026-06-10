const std = @import("std");
const IpcChannel = @import("../channel/ipc.zig").IpcChannel;
const WireCodec = @import("../codec/wire.zig").WireCodec;

/// A typed, zero-copy subscriber that decodes messages via comptime WireCodec
/// from an IPC channel.
pub fn Subscriber(comptime MsgType: type) type {
    const Codec = WireCodec(MsgType);

    return struct {
        const Self = @This();

        channel: *IpcChannel,
        msg_type_id: i32,

        /// Create a subscriber bound to a channel.
        pub fn init(channel: *IpcChannel, msg_type_id: i32) Self {
            return .{
                .channel = channel,
                .msg_type_id = msg_type_id,
            };
        }

        /// Poll for available messages and call `handler` for each decoded message.
        /// Only frames matching `msg_type_id` and at least `wire_size` bytes
        /// long are delivered; non-matching and short/garbage frames are
        /// consumed and skipped. Up to `limit` frames are consumed per call.
        /// The decoded pointer aliases the shared-memory frame (zero-copy)
        /// and is only valid for the duration of the handler call.
        /// Returns the number of messages delivered.
        pub fn poll(self: *Self, handler: *const fn (*align(1) const MsgType) void, limit: u32) u32 {
            var ctx = PollContext{
                .handler = handler,
                .msg_type_id = self.msg_type_id,
            };
            _ = self.channel.pollCtx(@ptrCast(&ctx), &dispatch, limit);
            return ctx.delivered;
        }

        const PollContext = struct {
            handler: *const fn (*align(1) const MsgType) void,
            msg_type_id: i32,
            delivered: u32 = 0,
        };

        fn dispatch(context: *anyopaque, result: IpcChannel.ReadResult) void {
            const ctx: *PollContext = @ptrCast(@alignCast(context));
            // Type filter: only frames tagged with this subscriber's id.
            if (result.msg_type_id != ctx.msg_type_id) return;
            // Guard the zero-copy decode against short frames.
            if (result.data.len < Codec.wire_size) return;
            ctx.handler(Codec.decode(result.data));
            ctx.delivered += 1;
        }

        /// Poll raw: calls handler with raw bytes for each frame.
        pub fn pollRaw(self: *Self, handler: *const fn (IpcChannel.ReadResult) void, limit: u32) u32 {
            return self.channel.poll(handler, limit);
        }
    };
}

/// An untyped subscriber for raw byte messages.
pub const RawSubscriber = struct {
    channel: *IpcChannel,

    pub fn init(channel: *IpcChannel) RawSubscriber {
        return .{ .channel = channel };
    }

    pub fn poll(self: *RawSubscriber, handler: *const fn (IpcChannel.ReadResult) void, limit: u32) u32 {
        return self.channel.poll(handler, limit);
    }
};

// ── Tests ────────────────────────────────────────────────────

test "RawSubscriber creation" {
    const name = "/zigbolt_test_sub_raw";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const sub = RawSubscriber.init(&ch);
    _ = sub;
}

test "RawSubscriber poll on empty channel" {
    const name = "/zigbolt_test_sub_empty";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    var sub = RawSubscriber.init(&ch);
    const count = sub.poll(&struct {
        fn handler(_: IpcChannel.ReadResult) void {}
    }.handler, 10);
    try std.testing.expectEqual(@as(u32, 0), count);
}

test "Subscriber typed creation" {
    const TestMsg = packed struct {
        x: u64,
    };

    const name = "/zigbolt_test_sub_typed";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const sub = Subscriber(TestMsg).init(&ch, 5);
    try std.testing.expectEqual(@as(i32, 5), sub.msg_type_id);
}

test "Subscriber(TickMessage) poll compiles, filters by type, and skips short frames" {
    const TickMessage = @import("../codec/wire.zig").TickMessage;
    const Codec = WireCodec(TickMessage);

    const name = "/zigbolt_test_sub_filter";
    var ch = try IpcChannel.create(name, .{ .term_length = 4096 });
    defer ch.deinit();

    const tick1 = TickMessage{
        .timestamp_ns = 1111,
        .symbol_id = 1,
        .price = 100,
        .volume = 10,
        .side = .bid,
    };
    const tick2 = TickMessage{
        .timestamp_ns = 2222,
        .symbol_id = 2,
        .price = -200,
        .volume = 20,
        .side = .ask,
    };

    var buf: [Codec.wire_size]u8 = undefined;

    // Matching type id.
    Codec.encode(&tick1, &buf);
    try ch.publish(&buf, 7);
    // DIFFERENT type id: well-formed but must be filtered out.
    Codec.encode(&tick2, &buf);
    try ch.publish(&buf, 9);
    // Matching type id again.
    Codec.encode(&tick2, &buf);
    try ch.publish(&buf, 7);
    // Short/garbage frame tagged with the matching type id: must not be
    // mis-decoded as a TickMessage.
    try ch.publish("garbage", 7);

    var sub = Subscriber(TickMessage).init(&ch, 7);

    const S = struct {
        var received: [4]TickMessage = undefined;
        var n: usize = 0;
        fn handler(msg: *align(1) const TickMessage) void {
            if (n < received.len) received[n] = msg.*;
            n += 1;
        }
    };
    S.n = 0;

    const delivered = sub.poll(&S.handler, 10);
    try std.testing.expectEqual(@as(u32, 2), delivered);
    try std.testing.expectEqual(@as(usize, 2), S.n);

    try std.testing.expectEqual(@as(u64, 1111), S.received[0].timestamp_ns);
    try std.testing.expectEqual(@as(u32, 1), S.received[0].symbol_id);
    try std.testing.expectEqual(@as(i64, 100), S.received[0].price);
    try std.testing.expectEqual(@as(u64, 10), S.received[0].volume);
    try std.testing.expect(S.received[0].side == .bid);
    try std.testing.expect(S.received[1].side == .ask);

    try std.testing.expectEqual(@as(u64, 2222), S.received[1].timestamp_ns);
    try std.testing.expectEqual(@as(u32, 2), S.received[1].symbol_id);
    try std.testing.expectEqual(@as(i64, -200), S.received[1].price);
    try std.testing.expectEqual(@as(u64, 20), S.received[1].volume);

    // All four frames (including the filtered/short ones) were consumed.
    try std.testing.expectEqual(@as(u32, 0), sub.poll(&S.handler, 10));
}
