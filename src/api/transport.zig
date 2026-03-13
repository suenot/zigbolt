const std = @import("std");
const IpcChannel = @import("../channel/ipc.zig").IpcChannel;
const ipc_mod = @import("../channel/ipc.zig");
const pub_mod = @import("publisher.zig");
const sub_mod = @import("subscriber.zig");

/// Transport configuration.
pub const TransportConfig = struct {
    /// Default term length for IPC channels.
    term_length: usize = 1 << 20, // 1 MB
    /// Use hugepages if available.
    use_hugepages: bool = false,
    /// Pre-fault pages on creation.
    pre_fault: bool = true,
};

/// Transport — the main entry point for ZigBolt messaging.
///
/// Manages IPC channels and provides factory methods for creating
/// typed Publishers and Subscribers.
pub const Transport = struct {
    config: TransportConfig,
    channels: std.StringHashMap(*IpcChannel),
    allocator: std.mem.Allocator,

    /// Initialize a new Transport instance.
    pub fn init(allocator: std.mem.Allocator, transport_config: TransportConfig) Transport {
        return .{
            .config = transport_config,
            .channels = std.StringHashMap(*IpcChannel).init(allocator),
            .allocator = allocator,
        };
    }

    /// Create or get a publication channel and return a typed Publisher.
    pub fn addPublication(
        self: *Transport,
        comptime MsgType: type,
        channel_name: [:0]const u8,
        msg_type_id: i32,
    ) !pub_mod.Publisher(MsgType) {
        const ch = try self.getOrCreateChannel(channel_name);
        return pub_mod.Publisher(MsgType).init(ch, msg_type_id);
    }

    /// Create or get a subscription channel and return a typed Subscriber.
    pub fn addSubscription(
        self: *Transport,
        comptime MsgType: type,
        channel_name: [:0]const u8,
        msg_type_id: i32,
    ) !sub_mod.Subscriber(MsgType) {
        const ch = try self.getOrOpenChannel(channel_name);
        return sub_mod.Subscriber(MsgType).init(ch, msg_type_id);
    }

    /// Create a raw (untyped) publisher.
    pub fn addRawPublication(
        self: *Transport,
        channel_name: [:0]const u8,
        msg_type_id: i32,
    ) !pub_mod.RawPublisher {
        const ch = try self.getOrCreateChannel(channel_name);
        return pub_mod.RawPublisher.init(ch, msg_type_id);
    }

    /// Create a raw (untyped) subscriber.
    pub fn addRawSubscription(
        self: *Transport,
        channel_name: [:0]const u8,
    ) !sub_mod.RawSubscriber {
        const ch = try self.getOrOpenChannel(channel_name);
        return sub_mod.RawSubscriber.init(ch);
    }

    /// Shut down the transport and release all resources.
    pub fn deinit(self: *Transport) void {
        var it = self.channels.valueIterator();
        while (it.next()) |ch_ptr| {
            ch_ptr.*.deinit();
            self.allocator.destroy(ch_ptr.*);
        }
        self.channels.deinit();
    }

    // ── Internal ─────────────────────────────────────────

    fn getOrCreateChannel(self: *Transport, name: [:0]const u8) !*IpcChannel {
        if (self.channels.get(name)) |ch| return ch;

        const ch = try self.allocator.create(IpcChannel);
        ch.* = try IpcChannel.create(name.ptr, .{
            .term_length = self.config.term_length,
            .use_hugepages = self.config.use_hugepages,
            .pre_fault = self.config.pre_fault,
        });
        try self.channels.put(name, ch);
        return ch;
    }

    fn getOrOpenChannel(self: *Transport, name: [:0]const u8) !*IpcChannel {
        if (self.channels.get(name)) |ch| return ch;

        const ch = try self.allocator.create(IpcChannel);
        ch.* = try IpcChannel.open(name.ptr, .{
            .term_length = self.config.term_length,
        });
        try self.channels.put(name, ch);
        return ch;
    }
};

// ── Tests ────────────────────────────────────────────────────
test "Transport basic lifecycle" {
    var transport = Transport.init(std.testing.allocator, .{ .term_length = 4096 });
    defer transport.deinit();

    // Just verify init/deinit works without channels
    try std.testing.expect(transport.channels.count() == 0);
}
