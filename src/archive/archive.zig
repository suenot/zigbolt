const std = @import("std");
const segment = @import("segment.zig");

pub const Record = segment.Record;
pub const ReadRecord = segment.ReadRecord;

/// Archive configuration.
///
/// Note: the archive stores RAW (uncompressed) segments. The LZ4-style codec
/// in compression.zig is a standalone utility and is intentionally NOT wired
/// into the segment write path; a former `compression` config field that was
/// never honored has been removed rather than left as a decorative knob.
pub const ArchiveConfig = struct {
    segment_size: usize = 256 * 1024 * 1024,
    base_path: []const u8 = "/tmp/zigbolt/archive",
    /// Durability policy (wired):
    /// - .none: never fsync; the OS page cache decides when data hits disk.
    /// - .periodic: fsync the active segment at most once per
    ///   `sync_interval_ms`, checked on each record(). Also fsyncs the
    ///   outgoing segment on rotation and on deinit.
    /// - .every_segment: fsync each segment when it is rotated out and on
    ///   deinit.
    sync_policy: SyncPolicy = .periodic,
    sync_interval_ms: u32 = 1000,

    pub const SyncPolicy = enum { none, periodic, every_segment };
};

pub const Archive = struct {
    segments: segment.SegmentManager,
    config: ArchiveConfig,
    allocator: std.mem.Allocator,
    total_records: u64,
    total_bytes: u64,
    /// Last time (ms) the active segment was fsynced under `.periodic`.
    last_sync_ms: i64,

    pub fn init(allocator: std.mem.Allocator, config: ArchiveConfig) !Archive {
        const seg_config = segment.SegmentConfig{
            .base_path = config.base_path,
            .segment_size = config.segment_size,
            .max_segments = null,
            .sync_on_close = config.sync_policy != .none,
        };

        const mgr = try segment.SegmentManager.init(allocator, seg_config);

        var self = Archive{
            .segments = mgr,
            .config = config,
            .allocator = allocator,
            .total_records = 0,
            .total_bytes = 0,
            .last_sync_ms = std.time.milliTimestamp(),
        };

        // Stats survive restart: segment files persist, so recount them
        // instead of restarting from zero.
        self.recomputeStats();

        return self;
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

        // Honor the periodic durability policy.
        if (self.config.sync_policy == .periodic) {
            const now_ms = std.time.milliTimestamp();
            if (now_ms - self.last_sync_ms >= self.config.sync_interval_ms) {
                try self.segments.syncActive();
                self.last_sync_ms = now_ms;
            }
        }
    }

    /// Recompute total_records/total_bytes by scanning existing segment
    /// files (structural walk over length prefixes; a torn or invalid tail
    /// ends that segment's scan). Missing segment ids are skipped.
    fn recomputeStats(self: *Archive) void {
        self.total_records = 0;
        self.total_bytes = 0;

        var seg_id: u64 = 0;
        while (seg_id < self.segments.next_segment_id) : (seg_id += 1) {
            var seg = self.segments.openSegment(seg_id) catch continue;
            defer seg.close();

            const file_size = seg.file.getEndPos() catch continue;
            var offset: u64 = 0;
            while (offset < file_size) {
                seg.file.seekTo(offset) catch break;
                var len_buf: [4]u8 = undefined;
                const n = seg.file.readAll(&len_buf) catch break;
                if (n < 4) break;
                const total_record_len = std.mem.readInt(u32, &len_buf, .little);
                if (total_record_len < segment.record_overhead) break;
                const payload_len = total_record_len - segment.record_overhead;
                if (payload_len > segment.max_payload_size) break;
                const next = offset + @as(u64, segment.length_prefix_size) + @as(u64, total_record_len);
                if (next > file_size) break; // torn tail
                self.total_records += 1;
                self.total_bytes += payload_len;
                offset = next;
            }
        }
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
    ///
    /// Robustness:
    /// - The read buffer grows on demand up to segment.max_payload_size, so
    ///   any record that appendRecord accepted is replayable (a fixed 64 KB
    ///   buffer used to make every >64 KB record abort the whole replay).
    /// - A missing (e.g. retention-deleted) segment id is skipped; it no
    ///   longer hides all later segments.
    /// - A corrupt record stops replay of THAT segment only (records before
    ///   it are delivered); later segments still replay.
    pub fn replay(self: *Archive, params: ReplayParams, handler: *const fn (segment.Record) void) !u64 {
        var replayed: u64 = 0;
        var seg_id = params.from_segment;
        const end_seg_id = self.segments.next_segment_id;

        // Start small; grows geometrically up to max_payload_size as needed.
        var buf = try self.allocator.alloc(u8, 64 * 1024);
        defer self.allocator.free(buf);

        while (seg_id < end_seg_id) : (seg_id += 1) {
            // Skip missing segments (deleted by retention etc.) — later
            // segments must still be replayable.
            var seg = self.segments.openSegment(seg_id) catch |err| {
                if (err == error.FileNotFound) continue;
                return err;
            };
            defer seg.close();

            var offset: u64 = if (seg_id == params.from_segment) params.from_offset else 0;

            reading: while (true) {
                const result = seg.readRecord(offset, buf) catch |err| switch (err) {
                    error.BufferTooSmall => {
                        // Record is larger than the current buffer: grow
                        // (bounded — appendRecord enforces max_payload_size,
                        // and readRecord rejects bigger claims as corrupt).
                        if (buf.len >= segment.max_payload_size) return err;
                        buf = try self.allocator.realloc(buf, @min(buf.len * 2, segment.max_payload_size));
                        continue :reading;
                    },
                    // Corruption mid-segment: stop this segment cleanly and
                    // move on to the next one.
                    error.CorruptRecord => break :reading,
                    else => |e| return e,
                };
                if (result == null) break :reading;
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

var test_large_len: usize = 0;
var test_large_crc: u32 = 0;

fn testLargeHandler(rec: segment.Record) void {
    test_large_len = rec.payload.len;
    test_large_crc = std.hash.Crc32.hash(rec.payload);
}

test "Archive: >64KB record is recorded AND replayed intact" {
    const test_path = "/tmp/zigbolt_test_archive_large";
    std.fs.cwd().deleteTree(test_path) catch {};

    var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 1024 * 1024,
        .sync_policy = .none,
    });
    defer {
        archive.deinit();
        std.fs.cwd().deleteTree(test_path) catch {};
    }

    // 200 KB payload — larger than the old fixed 64 KB replay buffer, which
    // wrote it fine but then returned BufferTooSmall forever and aborted the
    // WHOLE replay via `try`.
    const big_len = 200 * 1024;
    const big = try std.testing.allocator.alloc(u8, big_len);
    defer std.testing.allocator.free(big);
    for (big, 0..) |*b, i| {
        b.* = @intCast((i *% 31 + 7) % 256);
    }
    const expected_crc = std.hash.Crc32.hash(big);

    try archive.record(1, 1, big, 1000);
    try archive.record(1, 2, "small-after-big", 2000);

    test_large_len = 0;
    test_large_crc = 0;
    test_replay_count = 0;
    const count = try archive.replay(.{ .limit = 1 }, &testLargeHandler);
    try std.testing.expectEqual(@as(u64, 1), count);
    try std.testing.expectEqual(big_len, test_large_len);
    try std.testing.expectEqual(expected_crc, test_large_crc);

    // Both records replay (the small one after the big one is not lost).
    const all = try archive.replay(.{}, &testReplayHandler);
    try std.testing.expectEqual(@as(u64, 2), all);
}

var test_streams_seen: [8]u32 = undefined;
var test_streams_count: usize = 0;

fn testStreamCollector(rec: segment.Record) void {
    if (test_streams_count < test_streams_seen.len) {
        test_streams_seen[test_streams_count] = rec.stream_id;
    }
    test_streams_count += 1;
}

test "Archive: corrupt record mid-archive stops that segment cleanly, later segments replay" {
    const test_path = "/tmp/zigbolt_test_archive_corrupt";
    std.fs.cwd().deleteTree(test_path) catch {};
    defer std.fs.cwd().deleteTree(test_path) catch {};

    var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 4096,
        .sync_policy = .none,
    });
    defer archive.deinit();

    // Segment 0: A, B, C (28 bytes each on disk: 24 header + 4 payload).
    try archive.record(1, 1, "aaaa", 100);
    try archive.record(2, 2, "bbbb", 200);
    try archive.record(3, 3, "cccc", 300);
    // Big record D forces rotation into segment 1.
    const filler = "D" ** 4000;
    try archive.record(4, 4, filler, 400);

    // Corrupt one payload byte of record B (mid-segment-0: C follows it).
    {
        const f = try std.fs.cwd().openFile(test_path ++ "/segment_0000000000.dat", .{ .mode = .read_write });
        defer f.close();
        try f.seekTo(28 + 24); // B starts at 28; payload after its 24-byte header
        try f.writeAll(&[_]u8{0xFF});
    }

    // Replay: A delivered; B corrupt => segment 0 stops (C sacrificed);
    // segment 1 still replays D. Before the fix this was a hard error that
    // aborted the whole replay.
    test_streams_count = 0;
    const count = try archive.replay(.{}, &testStreamCollector);
    try std.testing.expectEqual(@as(u64, 2), count);
    try std.testing.expectEqual(@as(u32, 1), test_streams_seen[0]); // A
    try std.testing.expectEqual(@as(u32, 4), test_streams_seen[1]); // D
}

test "Archive: missing segment is skipped, later segments still replay" {
    const test_path = "/tmp/zigbolt_test_archive_gap";
    std.fs.cwd().deleteTree(test_path) catch {};
    defer std.fs.cwd().deleteTree(test_path) catch {};

    var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 70, // one 40-byte-payload record (64 bytes) per segment
        .sync_policy = .none,
    });
    defer archive.deinit();

    const payload = "0123456789012345678901234567890123456789"; // 40 bytes
    try archive.record(1, 1, payload, 100); // segment 0
    try archive.record(2, 2, payload, 200); // segment 1
    try archive.record(3, 3, payload, 300); // segment 2

    // Simulate retention deleting the middle segment.
    try std.fs.cwd().deleteFile(test_path ++ "/segment_0000000001.dat");

    // Before the fix, replay stopped at the first FileNotFound and segment 2
    // was hidden forever.
    test_streams_count = 0;
    const count = try archive.replay(.{}, &testStreamCollector);
    try std.testing.expectEqual(@as(u64, 2), count);
    try std.testing.expectEqual(@as(u32, 1), test_streams_seen[0]);
    try std.testing.expectEqual(@as(u32, 3), test_streams_seen[1]);
}

test "Archive: stats recomputed from segment files after reopen" {
    const test_path = "/tmp/zigbolt_test_archive_reopen";
    std.fs.cwd().deleteTree(test_path) catch {};
    defer std.fs.cwd().deleteTree(test_path) catch {};

    {
        var archive = try Archive.init(std.testing.allocator, ArchiveConfig{
            .base_path = test_path,
            .segment_size = 4096,
            .sync_policy = .every_segment,
        });
        defer archive.deinit();

        try archive.record(1, 1, "hello", 100); // 5 bytes
        try archive.record(2, 2, "world!!", 200); // 7 bytes
        try archive.record(3, 3, "12345678901", 300); // 11 bytes
    }

    // Reopen: before the fix stats() reset to 0 although the files persist.
    var reopened = try Archive.init(std.testing.allocator, ArchiveConfig{
        .base_path = test_path,
        .segment_size = 4096,
    });
    defer reopened.deinit();

    const s = reopened.stats();
    try std.testing.expectEqual(@as(u64, 3), s.total_records);
    try std.testing.expectEqual(@as(u64, 23), s.total_bytes);
    try std.testing.expectEqual(@as(u64, 1), s.segment_count);

    // And the archive keeps counting correctly from there.
    try reopened.record(4, 4, "more", 400);
    try std.testing.expectEqual(@as(u64, 4), reopened.stats().total_records);
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
