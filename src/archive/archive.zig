const std = @import("std");
const segment = @import("segment.zig");

pub const Record = segment.Record;
pub const ReadRecord = segment.ReadRecord;

pub const ArchiveConfig = struct {
    segment_size: usize = 256 * 1024 * 1024,
    base_path: []const u8 = "/tmp/zigbolt/archive",
    sync_policy: SyncPolicy = .periodic,
    sync_interval_ms: u32 = 1000,
    compression: ?CompressionAlgo = null,

    pub const SyncPolicy = enum { none, periodic, every_segment };
    pub const CompressionAlgo = enum { lz4, zstd };
};

pub const Archive = struct {
    segments: segment.SegmentManager,
    config: ArchiveConfig,
    allocator: std.mem.Allocator,
    total_records: u64,
    total_bytes: u64,

    pub fn init(allocator: std.mem.Allocator, config: ArchiveConfig) !Archive {
        const seg_config = segment.SegmentConfig{
            .base_path = config.base_path,
            .segment_size = config.segment_size,
            .max_segments = null,
        };

        const mgr = try segment.SegmentManager.init(allocator, seg_config);

        return Archive{
            .segments = mgr,
            .config = config,
            .allocator = allocator,
            .total_records = 0,
            .total_bytes = 0,
        };
    }

    pub fn deinit(self: *Archive) void {
        self.segments.deinit();
    }

    /// Record a message into the archive.
    pub fn record(self: *Archive, stream_id: u32, msg_type_id: i32, data: []const u8, timestamp_ns: u64) !void {
        const rec = segment.Record{
            .timestamp_ns = timestamp_ns,
            .stream_id = stream_id,
            .msg_type_id = msg_type_id,
            .payload = data,
        };

        try self.segments.write(rec);

        self.total_records += 1;
        self.total_bytes += data.len;
    }

    /// Parameters for replaying archived messages.
    pub const ReplayParams = struct {
        stream_id: ?u32 = null, // null = all streams
        from_segment: u64 = 0,
        from_offset: u64 = 0,
        limit: ?u64 = null,
    };

    /// Replay messages from a specific position.
    /// Calls handler for each matching record. Returns the number of records replayed.
    pub fn replay(self: *Archive, params: ReplayParams, handler: *const fn (segment.Record) void) !u64 {
        var replayed: u64 = 0;
        var seg_id = params.from_segment;
        var buf: [64 * 1024]u8 = undefined; // 64 KB read buffer

        while (true) {
            // Try to open the segment; if it doesn't exist, we're done
            var seg = self.segments.openSegment(seg_id) catch |err| {
                if (err == error.FileNotFound) break;
                return err;
            };
            defer seg.close();

            var offset: u64 = if (seg_id == params.from_segment) params.from_offset else 0;

            while (true) {
                const result = try seg.readRecord(offset, &buf);
                if (result == null) break;
                const rr = result.?;

                // Filter by stream_id if specified
                if (params.stream_id == null or rr.record.stream_id == params.stream_id.?) {
                    handler(rr.record);
                    replayed += 1;

                    // Check limit
                    if (params.limit) |lim| {
                        if (replayed >= lim) return replayed;
                    }
                }

                offset = rr.next_offset;
            }

            seg_id += 1;
        }

        return replayed;
    }

    /// Archive statistics.
    pub const Stats = struct {
        total_records: u64,
        total_bytes: u64,
        segment_count: u64,
    };

    /// Get archive statistics.
    pub fn stats(self: *const Archive) Stats {
        return Stats{
            .total_records = self.total_records,
            .total_bytes = self.total_bytes,
            .segment_count = self.segments.next_segment_id,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

var test_replay_count: u64 = 0;
var test_replay_last_stream: u32 = 0;

fn testReplayHandler(rec: segment.Record) void {
    test_replay_count += 1;
    test_replay_last_stream = rec.stream_id;
}

test "Archive record and replay" {
    const test_path = "/tmp/zigbolt_test_archive";
    std.fs.cwd().deleteTree(test_path) catch {};

    var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 4096,
    });
    defer {
        archive.deinit();
        std.fs.cwd().deleteTree(test_path) catch {};
    }

    // Record several messages
    try archive.record(1, 100, "message one", 1000);
    try archive.record(2, 200, "message two", 2000);
    try archive.record(1, 101, "message three", 3000);

    // Replay all
    test_replay_count = 0;
    const count = try archive.replay(.{}, &testReplayHandler);
    try std.testing.expectEqual(@as(u64, 3), count);
    try std.testing.expectEqual(@as(u64, 3), test_replay_count);
}

test "Archive replay with stream filter" {
    const test_path = "/tmp/zigbolt_test_archive_filter";
    std.fs.cwd().deleteTree(test_path) catch {};

    var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 4096,
    });
    defer {
        archive.deinit();
        std.fs.cwd().deleteTree(test_path) catch {};
    }

    try archive.record(1, 10, "stream1-a", 100);
    try archive.record(2, 20, "stream2-a", 200);
    try archive.record(1, 11, "stream1-b", 300);
    try archive.record(3, 30, "stream3-a", 400);

    // Replay only stream 1
    test_replay_count = 0;
    const count = try archive.replay(.{ .stream_id = 1 }, &testReplayHandler);
    try std.testing.expectEqual(@as(u64, 2), count);
    try std.testing.expectEqual(@as(u64, 2), test_replay_count);
    try std.testing.expectEqual(@as(u32, 1), test_replay_last_stream);
}

test "Archive replay with limit" {
    const test_path = "/tmp/zigbolt_test_archive_limit";
    std.fs.cwd().deleteTree(test_path) catch {};

    var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 4096,
    });
    defer {
        archive.deinit();
        std.fs.cwd().deleteTree(test_path) catch {};
    }

    try archive.record(1, 10, "msg1", 100);
    try archive.record(1, 10, "msg2", 200);
    try archive.record(1, 10, "msg3", 300);

    // Replay with limit of 2
    test_replay_count = 0;
    const count = try archive.replay(.{ .limit = 2 }, &testReplayHandler);
    try std.testing.expectEqual(@as(u64, 2), count);
}

test "Archive stats" {
    const test_path = "/tmp/zigbolt_test_archive_stats";
    std.fs.cwd().deleteTree(test_path) catch {};

    var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 4096,
    });
    defer {
        archive.deinit();
        std.fs.cwd().deleteTree(test_path) catch {};
    }

    // Initially empty
    const s0 = archive.stats();
    try std.testing.expectEqual(@as(u64, 0), s0.total_records);
    try std.testing.expectEqual(@as(u64, 0), s0.total_bytes);

    try archive.record(1, 10, "hello", 100);
    try archive.record(2, 20, "world!!", 200);

    const s1 = archive.stats();
    try std.testing.expectEqual(@as(u64, 2), s1.total_records);
    try std.testing.expectEqual(@as(u64, 12), s1.total_bytes); // "hello" (5) + "world!!" (7)
    try std.testing.expectEqual(@as(u64, 1), s1.segment_count);
}
