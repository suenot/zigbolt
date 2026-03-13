const std = @import("std");
const IpcChannel = @import("../channel/ipc.zig").IpcChannel;
const WireCodec = @import("../codec/wire.zig").WireCodec;

/// A typed, zero-copy publisher that encodes messages via comptime WireCodec
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

        /// Publish a typed message. Zero-copy: the message is serialized
        /// directly into the shared memory buffer via WireCodec.
        pub fn offer(self: *Self, msg: *const MsgType) !void {
            const bytes = std.mem.asBytes(msg);
            try self.channel.publish(bytes[0..Codec.wire_size], self.msg_type_id);
        }

        /// Try to publish without blocking. Returns false if back-pressured.
        pub fn tryOffer(self: *Self, msg: *const MsgType) bool {
            const bytes = std.mem.asBytes(msg);
            self.channel.publish(bytes[0..Codec.wire_size], self.msg_type_id) catch return false;
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
