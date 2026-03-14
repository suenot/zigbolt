const std = @import("std");

/// A sequenced event — every event receives a monotonically increasing sequence number.
pub const SequencedEvent = struct {
    sequence: u64,
    timestamp_ns: u64,
    stream_id: u32,
    payload: []const u8,
};

/// Sequencer configuration.
pub const SequencerConfig = struct {
    /// Initial sequence number.
    initial_sequence: u64 = 0,
    /// Maximum number of input streams.
    max_streams: u32 = 64,
};

/// Total ordering sequencer.
/// All incoming events from multiple streams are assigned a single,
/// monotonically increasing sequence number.
/// This guarantees that all consumers see events in the exact same order.
pub const Sequencer = struct {
    next_sequence: std.atomic.Value(u64),
    config: SequencerConfig,
    /// Atomically updated count of total events sequenced. Thread-safe.
    total_sequenced: std.atomic.Value(u64),

    pub fn init(config: SequencerConfig) Sequencer {
        return Sequencer{
            .next_sequence = std.atomic.Value(u64).init(config.initial_sequence),
            .config = config,
            .total_sequenced = std.atomic.Value(u64).init(0),
        };
    }

    /// Assign a sequence number to an event.
    /// Thread-safe (uses atomic fetchAdd).
    pub fn sequence(self: *Sequencer, stream_id: u32, payload: []const u8) SequencedEvent {
        const seq = self.next_sequence.fetchAdd(1, .monotonic);
        const ts = @as(u64, @intCast(std.time.nanoTimestamp()));
        _ = self.total_sequenced.fetchAdd(1, .monotonic);
        return SequencedEvent{
            .sequence = seq,
            .timestamp_ns = ts,
            .stream_id = stream_id,
            .payload = payload,
        };
    }

    /// Get the next sequence number that will be assigned (without consuming it).
    pub fn peekNextSequence(self: *const Sequencer) u64 {
        return self.next_sequence.load(.monotonic);
    }

    /// Reset the sequencer (for testing/replay).
    pub fn reset(self: *Sequencer, initial_sequence: u64) void {
        self.next_sequence.store(initial_sequence, .monotonic);
        self.total_sequenced.store(0, .monotonic);
    }
};

/// Multi-stream sequencer: merges N input streams into 1 sequenced output.
/// Uses round-robin with optional weighting.
///
/// Thread-safety: single-threaded only. The `stream_stats` and `active_streams`
/// fields are updated non-atomically for performance, assuming deterministic,
/// single-threaded access. The underlying `Sequencer.sequence()` is thread-safe
/// (atomic), but `sequenceFrom()` must be called from a single thread.
pub const MultiStreamSequencer = struct {
    sequencer: Sequencer,
    /// Per-stream statistics (single-threaded access only — not atomic).
    stream_stats: [64]StreamStats,
    /// Number of distinct streams that have produced at least one event
    /// (single-threaded access only — not atomic).
    active_streams: u32,

    pub const StreamStats = struct {
        events_sequenced: u64 = 0,
        last_sequence: u64 = 0,
    };

    pub fn init(config: SequencerConfig) MultiStreamSequencer {
        return MultiStreamSequencer{
            .sequencer = Sequencer.init(config),
            .stream_stats = [_]StreamStats{StreamStats{}} ** 64,
            .active_streams = 0,
        };
    }

    /// Sequence an event from a specific stream.
    pub fn sequenceFrom(self: *MultiStreamSequencer, stream_id: u32, payload: []const u8) SequencedEvent {
        std.debug.assert(stream_id < self.sequencer.config.max_streams);

        const event = self.sequencer.sequence(stream_id, payload);

        self.stream_stats[stream_id].events_sequenced += 1;
        self.stream_stats[stream_id].last_sequence = event.sequence;

        // Track active streams: a stream is active once it has at least one event.
        if (self.stream_stats[stream_id].events_sequenced == 1) {
            self.active_streams += 1;
        }

        return event;
    }

    /// Get statistics for a stream.
    pub fn getStreamStats(self: *const MultiStreamSequencer, stream_id: u32) StreamStats {
        std.debug.assert(stream_id < self.sequencer.config.max_streams);
        return self.stream_stats[stream_id];
    }

    /// Get total events across all streams.
    pub fn totalEvents(self: *const MultiStreamSequencer) u64 {
        var total: u64 = 0;
        for (&self.stream_stats) |stats| {
            total += stats.events_sequenced;
        }
        return total;
    }
};

/// SequenceIndex — maps sequence numbers to stream/offset for replay.
pub const SequenceIndex = struct {
    entries: std.ArrayList(IndexEntry),
    allocator: std.mem.Allocator,

    pub const IndexEntry = struct {
        sequence: u64,
        stream_id: u32,
        timestamp_ns: u64,
    };

    pub fn init(allocator: std.mem.Allocator) SequenceIndex {
        return SequenceIndex{
            .entries = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SequenceIndex) void {
        self.entries.deinit(self.allocator);
    }

    pub fn add(self: *SequenceIndex, entry: IndexEntry) !void {
        try self.entries.append(self.allocator, entry);
    }

    pub fn lookup(self: *const SequenceIndex, seq: u64) ?IndexEntry {
        for (self.entries.items) |entry| {
            if (entry.sequence == seq) {
                return entry;
            }
        }
        return null;
    }

    pub fn rangeFrom(self: *const SequenceIndex, from_sequence: u64) []const IndexEntry {
        for (self.entries.items, 0..) |entry, i| {
            if (entry.sequence >= from_sequence) {
                return self.entries.items[i..];
            }
        }
        return self.entries.items[0..0];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Sequencer: monotonically increasing sequence numbers" {
    var seq = Sequencer.init(.{ .initial_sequence = 0 });

    const e0 = seq.sequence(0, "order-new");
    const e1 = seq.sequence(0, "order-cancel");
    const e2 = seq.sequence(1, "trade-exec");
    const e3 = seq.sequence(2, "market-data");

    try std.testing.expectEqual(@as(u64, 0), e0.sequence);
    try std.testing.expectEqual(@as(u64, 1), e1.sequence);
    try std.testing.expectEqual(@as(u64, 2), e2.sequence);
    try std.testing.expectEqual(@as(u64, 3), e3.sequence);

    // Verify strictly increasing
    try std.testing.expect(e1.sequence > e0.sequence);
    try std.testing.expect(e2.sequence > e1.sequence);
    try std.testing.expect(e3.sequence > e2.sequence);

    // Verify stream IDs preserved
    try std.testing.expectEqual(@as(u32, 0), e0.stream_id);
    try std.testing.expectEqual(@as(u32, 1), e2.stream_id);
    try std.testing.expectEqual(@as(u32, 2), e3.stream_id);

    // Peek should return the next value to be assigned
    try std.testing.expectEqual(@as(u64, 4), seq.peekNextSequence());

    std.debug.print("Sequencer: 4 events sequenced, monotonically increasing OK\n", .{});
}

test "Sequencer: custom initial sequence" {
    var seq = Sequencer.init(.{ .initial_sequence = 1000 });

    const e0 = seq.sequence(0, "first");
    const e1 = seq.sequence(0, "second");

    try std.testing.expectEqual(@as(u64, 1000), e0.sequence);
    try std.testing.expectEqual(@as(u64, 1001), e1.sequence);
    try std.testing.expectEqual(@as(u64, 1002), seq.peekNextSequence());

    std.debug.print("Sequencer: custom initial_sequence=1000 OK\n", .{});
}

test "Sequencer: reset" {
    var seq = Sequencer.init(.{ .initial_sequence = 0 });

    _ = seq.sequence(0, "a");
    _ = seq.sequence(0, "b");
    _ = seq.sequence(0, "c");

    try std.testing.expectEqual(@as(u64, 3), seq.peekNextSequence());

    seq.reset(100);

    try std.testing.expectEqual(@as(u64, 100), seq.peekNextSequence());
    try std.testing.expectEqual(@as(u64, 0), seq.total_sequenced.load(.monotonic));

    const e = seq.sequence(0, "after-reset");
    try std.testing.expectEqual(@as(u64, 100), e.sequence);

    std.debug.print("Sequencer: reset to 100 OK\n", .{});
}

test "MultiStreamSequencer: multiple streams, global ordering" {
    var ms = MultiStreamSequencer.init(.{ .initial_sequence = 0, .max_streams = 64 });

    // Simulate events from 3 different streams (e.g. order gateway, market data, risk)
    const e0 = ms.sequenceFrom(0, "order-new");
    const e1 = ms.sequenceFrom(1, "md-update");
    const e2 = ms.sequenceFrom(2, "risk-check");
    const e3 = ms.sequenceFrom(0, "order-ack");
    const e4 = ms.sequenceFrom(1, "md-update-2");

    // All sequence numbers are globally ordered
    try std.testing.expectEqual(@as(u64, 0), e0.sequence);
    try std.testing.expectEqual(@as(u64, 1), e1.sequence);
    try std.testing.expectEqual(@as(u64, 2), e2.sequence);
    try std.testing.expectEqual(@as(u64, 3), e3.sequence);
    try std.testing.expectEqual(@as(u64, 4), e4.sequence);

    // Verify per-stream stats
    const stats0 = ms.getStreamStats(0);
    try std.testing.expectEqual(@as(u64, 2), stats0.events_sequenced);
    try std.testing.expectEqual(@as(u64, 3), stats0.last_sequence);

    const stats1 = ms.getStreamStats(1);
    try std.testing.expectEqual(@as(u64, 2), stats1.events_sequenced);
    try std.testing.expectEqual(@as(u64, 4), stats1.last_sequence);

    const stats2 = ms.getStreamStats(2);
    try std.testing.expectEqual(@as(u64, 1), stats2.events_sequenced);
    try std.testing.expectEqual(@as(u64, 2), stats2.last_sequence);

    // Total events
    try std.testing.expectEqual(@as(u64, 5), ms.totalEvents());

    // Active streams
    try std.testing.expectEqual(@as(u32, 3), ms.active_streams);

    std.debug.print("MultiStreamSequencer: 3 streams, 5 events, global ordering OK\n", .{});
}

test "SequenceIndex: add and lookup" {
    var index = SequenceIndex.init(std.testing.allocator);
    defer index.deinit();

    try index.add(.{ .sequence = 0, .stream_id = 0, .timestamp_ns = 100 });
    try index.add(.{ .sequence = 1, .stream_id = 1, .timestamp_ns = 200 });
    try index.add(.{ .sequence = 2, .stream_id = 0, .timestamp_ns = 300 });
    try index.add(.{ .sequence = 3, .stream_id = 2, .timestamp_ns = 400 });

    // Lookup existing
    const entry1 = index.lookup(1).?;
    try std.testing.expectEqual(@as(u64, 1), entry1.sequence);
    try std.testing.expectEqual(@as(u32, 1), entry1.stream_id);
    try std.testing.expectEqual(@as(u64, 200), entry1.timestamp_ns);

    // Lookup non-existing
    try std.testing.expect(index.lookup(99) == null);

    // Range query
    const range = index.rangeFrom(2);
    try std.testing.expectEqual(@as(usize, 2), range.len);
    try std.testing.expectEqual(@as(u64, 2), range[0].sequence);
    try std.testing.expectEqual(@as(u64, 3), range[1].sequence);

    // Range from beyond end
    const empty = index.rangeFrom(100);
    try std.testing.expectEqual(@as(usize, 0), empty.len);

    std.debug.print("SequenceIndex: add, lookup, rangeFrom OK\n", .{});
}

test "SequenceIndex: rangeFrom at start" {
    var index = SequenceIndex.init(std.testing.allocator);
    defer index.deinit();

    try index.add(.{ .sequence = 10, .stream_id = 0, .timestamp_ns = 100 });
    try index.add(.{ .sequence = 11, .stream_id = 1, .timestamp_ns = 200 });
    try index.add(.{ .sequence = 12, .stream_id = 2, .timestamp_ns = 300 });

    // Range from 0 should return everything (all sequences >= 0)
    const all = index.rangeFrom(0);
    try std.testing.expectEqual(@as(usize, 3), all.len);

    std.debug.print("SequenceIndex: rangeFrom(0) returns all entries OK\n", .{});
}
