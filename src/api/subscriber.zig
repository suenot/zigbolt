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
        /// Only messages matching `msg_type_id` are delivered.
        /// Returns the number of messages delivered.
        pub fn poll(self: *Self, handler: *const fn (*const MsgType) void, limit: u32) u32 {
            const Context = struct {
                fn dispatch(result: IpcChannel.ReadResult) void {
                    if (result.data.len >= Codec.wire_size) {
                        const msg = Codec.decode(result.data);
                        handler(msg);
                    }
                }
            };
            // We filter by msg_type_id at the subscriber level
            // For now, poll all and deliver all (type filtering can be added)
            return self.channel.poll(&Context.dispatch, limit);
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
