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

    /// Shut down the transport and release all resources,
    /// including the duplicated channel-name keys.
    pub fn deinit(self: *Transport) void {
        var it = self.channels.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.channels.deinit();
    }

    // ── Internal ─────────────────────────────────────────

    fn getOrCreateChannel(self: *Transport, name: [:0]const u8) !*IpcChannel {
        if (self.channels.get(name)) |ch| return ch;

        const ch = try self.allocator.create(IpcChannel);
        errdefer self.allocator.destroy(ch);

        ch.* = try IpcChannel.create(name.ptr, .{
            .term_length = self.config.term_length,
            .use_hugepages = self.config.use_hugepages,
            .pre_fault = self.config.pre_fault,
        });
        errdefer ch.deinit();

        try self.putOwnedKey(name, ch);
        return ch;
    }

    fn getOrOpenChannel(self: *Transport, name: [:0]const u8) !*IpcChannel {
        if (self.channels.get(name)) |ch| return ch;

        const ch = try self.allocator.create(IpcChannel);
        errdefer self.allocator.destroy(ch);

        ch.* = try IpcChannel.open(name.ptr, .{
            .term_length = self.config.term_length,
        });
        errdefer ch.deinit();

        try self.putOwnedKey(name, ch);
        return ch;
    }

    /// Insert `ch` under a copy of `name` owned by the transport allocator.
    /// The caller's `name` may be a temporary (e.g. heap-formatted); using
    /// it directly as the map key would leave a dangling key after the
    /// caller frees it (use-after-free on later get/deinit).
    fn putOwnedKey(self: *Transport, name: []const u8, ch: *IpcChannel) !void {
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        try self.channels.put(owned_name, ch);
    }
};

// ── Tests ────────────────────────────────────────────────────
test "Transport basic lifecycle" {
    var transport = Transport.init(std.testing.allocator, .{ .term_length = 4096 });
    defer transport.deinit();

    // Just verify init/deinit works without channels
    try std.testing.expect(transport.channels.count() == 0);
}

test "Transport creation with custom config" {
    var transport = Transport.init(std.testing.allocator, .{
        .term_length = 8192,
        .use_hugepages = false,
        .pre_fault = false,
    });
    defer transport.deinit();

    try std.testing.expectEqual(@as(usize, 8192), transport.config.term_length);
    try std.testing.expect(!transport.config.use_hugepages);
    try std.testing.expect(!transport.config.pre_fault);
}

test "Transport addRawPublication creates channel" {
    var transport = Transport.init(std.testing.allocator, .{ .term_length = 4096 });
    defer transport.deinit();

    var pub_inst = try transport.addRawPublication("/zigbolt_test_transport_pub", 42);
    try pub_inst.offer("hello from transport");

    try std.testing.expectEqual(@as(u32, 1), transport.channels.count());
}

test "Transport addRawPublication reuses channel" {
    var transport = Transport.init(std.testing.allocator, .{ .term_length = 4096 });
    defer transport.deinit();

    _ = try transport.addRawPublication("/zigbolt_test_transport_reuse", 1);
    _ = try transport.addRawPublication("/zigbolt_test_transport_reuse", 2);

    // Same channel name should reuse the same channel
    try std.testing.expectEqual(@as(u32, 1), transport.channels.count());
}

test "Transport owns its channel-name keys (temporary name survives free)" {
    var transport = Transport.init(std.testing.allocator, .{ .term_length = 4096 });
    defer transport.deinit();

    // Heap-formatted temporary name, freed right after registration.
    const tmp_name = try std.fmt.allocPrintSentinel(
        std.testing.allocator,
        "/zigbolt_test_transport_key_{d}",
        .{42},
        0,
    );
    var pub_inst = blk: {
        defer std.testing.allocator.free(tmp_name);
        break :blk try transport.addRawPublication(tmp_name, 1);
    };
    try pub_inst.offer("owned key");

    // Look the channel up via a fresh, equal string: must hit the same
    // entry (key was duped, not dangling).
    const lookup_name = try std.fmt.allocPrintSentinel(
        std.testing.allocator,
        "/zigbolt_test_transport_key_{d}",
        .{42},
        0,
    );
    defer std.testing.allocator.free(lookup_name);
    _ = try transport.addRawPublication(lookup_name, 2);
    try std.testing.expectEqual(@as(u32, 1), transport.channels.count());
    // deinit (via defer) must free the duped key — the testing allocator
    // fails the test on any leak or use-after-free.
}

test "Transport channel factory does not leak on open failure" {
    var transport = Transport.init(std.testing.allocator, .{ .term_length = 4096 });
    defer transport.deinit();

    // Opening a channel that no one created must fail cleanly: the
    // errdefer path frees the IpcChannel struct (the testing allocator
    // fails the test on a leak) and no map entry is left behind.
    try std.testing.expectError(
        error.ShmOpenFailed,
        transport.addRawSubscription("/zigbolt_test_transport_noexist"),
    );
    try std.testing.expectEqual(@as(u32, 0), transport.channels.count());
}
