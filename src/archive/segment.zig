const std = @import("std");

pub const SegmentConfig = struct {
    base_path: []const u8 = "/tmp/zigbolt/archive",
    segment_size: usize = 256 * 1024 * 1024, // 256 MB per segment
    max_segments: ?usize = null, // null = unlimited
    /// Fsync a segment file before closing it (on rotation and deinit).
    /// Wired from ArchiveConfig.sync_policy.
    sync_on_close: bool = false,
};

/// Maximum payload size for a single record (64 MB).
/// Enforced at write time so every written record is replayable with a
/// bounded buffer, and used at read time to reject garbage length prefixes
/// before they cause huge allocations.
pub const max_payload_size: usize = 64 * 1024 * 1024;

/// Header written before each record in a segment file.
/// Format: [u32 length][u32 crc][u64 timestamp_ns][u32 stream_id][i32 msg_type_id][payload...]
/// - `length` counts everything after the length prefix (crc + header + payload).
/// - `crc` is CRC32 over the length prefix bytes, the header fields, and the
///   payload — i.e. the whole record except the crc field itself.
pub const record_header_size: u32 = @sizeOf(u64) + @sizeOf(u32) + @sizeOf(i32); // 16 bytes
pub const length_prefix_size: u32 = @sizeOf(u32); // 4 bytes
pub const crc_size: u32 = @sizeOf(u32); // 4 bytes
/// Bytes counted by the length prefix beyond the payload (crc + header).
pub const record_overhead: u32 = crc_size + record_header_size; // 20 bytes
pub const full_header_size: u32 = length_prefix_size + crc_size + record_header_size; // 24 bytes

pub const Record = struct {
    timestamp_ns: u64,
    stream_id: u32,
    msg_type_id: i32,
    payload: []const u8,
};

pub const ReadRecord = struct {
    record: Record,
    next_offset: u64,
};

/// A single segment file containing recorded messages.
pub const Segment = struct {
    file: std.fs.File,
    id: u64,
    write_offset: u64,
    max_size: usize,

    pub fn create(dir: std.fs.Dir, id: u64, max_size: usize) !Segment {
        var name_buf: [32]u8 = undefined;
        const filename = segmentFilename(&name_buf, id);
        const file = try dir.createFile(filename, .{ .read = true, .truncate = true });
        return Segment{
            .file = file,
            .id = id,
            .write_offset = 0,
            .max_size = max_size,
        };
    }

    pub fn open(dir: std.fs.Dir, id: u64) !Segment {
        var name_buf: [32]u8 = undefined;
        const filename = segmentFilename(&name_buf, id);
        const file = try dir.openFile(filename, .{ .mode = .read_only });
        const file_size = try file.getEndPos();
        return Segment{
            .file = file,
            .id = id,
            .write_offset = file_size,
            .max_size = 0, // read-only, max_size not relevant
        };
    }

    pub fn close(self: *Segment) void {
        self.file.close();
    }

    /// Append a record to the segment.
    /// Record format: [u32 length][u32 crc][u64 timestamp_ns][u32 stream_id][i32 msg_type_id][payload]
    pub fn appendRecord(self: *Segment, record: Record) !void {
        // Guard the u32 cast and keep every written record replayable with a
        // bounded buffer.
        if (record.payload.len > max_payload_size) {
            return error.PayloadTooLarge;
        }
        const payload_len: u32 = @intCast(record.payload.len);
        const total_record_len: u32 = record_overhead + payload_len;
        const total_write_len: u64 = @as(u64, length_prefix_size) + total_record_len;

        // Check if this record would exceed segment size
        if (self.write_offset + total_write_len > self.max_size) {
            return error.SegmentFull;
        }

        // Build header in a buffer
        var header: [full_header_size]u8 = undefined;
        std.mem.writeInt(u32, header[0..4], total_record_len, .little);
        std.mem.writeInt(u64, header[8..16], record.timestamp_ns, .little);
        std.mem.writeInt(u32, header[16..20], record.stream_id, .little);
        std.mem.writeInt(i32, header[20..24], record.msg_type_id, .little);

        // CRC32 over everything except the crc field itself.
        var hasher = std.hash.Crc32.init();
        hasher.update(header[0..4]);
        hasher.update(header[8..24]);
        hasher.update(record.payload);
        std.mem.writeInt(u32, header[4..8], hasher.final(), .little);

        // Seek to write position and write header + payload
        try self.file.seekTo(self.write_offset);
        try self.file.writeAll(&header);
        try self.file.writeAll(record.payload);

        self.write_offset += total_write_len;
    }

    /// Read a record at the given offset.
    /// Returns null at end-of-segment, which includes a torn tail: a record
    /// whose bytes run past EOF, or whose CRC fails when it is the LAST
    /// record in the file (interrupted write). A CRC failure with more data
    /// after it is real corruption and returns error.CorruptRecord.
    pub fn readRecord(self: *Segment, offset: u64, buf: []u8) !?ReadRecord {
        const file_size = try self.file.getEndPos();
        if (offset >= file_size) {
            return null;
        }

        // Read the length prefix
        try self.file.seekTo(offset);
        var len_buf: [4]u8 = undefined;
        const len_bytes_read = try self.file.readAll(&len_buf);
        if (len_bytes_read < 4) {
            return null;
        }
        const total_record_len = std.mem.readInt(u32, &len_buf, .little);

        if (total_record_len < record_overhead) { // kcov-skip: evaluated on every readRecord; true branch covered by the undersized-length test; no own line record
            return error.CorruptRecord;
        }

        // A record extending past EOF is a torn tail — end of segment.
        if (offset + @as(u64, length_prefix_size) + @as(u64, total_record_len) > file_size) {
            return null;
        }

        const payload_len = total_record_len - record_overhead;
        if (payload_len > max_payload_size) {
            return error.CorruptRecord;
        }
        if (payload_len > buf.len) {
            return error.BufferTooSmall;
        }

        // Read the rest of the record (crc + header fields + payload)
        var meta_buf: [crc_size + record_header_size]u8 = undefined;
        const meta_bytes_read = try self.file.readAll(&meta_buf);
        if (meta_bytes_read < meta_buf.len) {
            return error.CorruptRecord; // kcov-skip: line 142 already verified prefix+crc+header+payload all fit within file_size, so the in-process read always fills meta_buf; a short read here needs the file truncated concurrently via another fd, which cannot be injected single-threaded
        }

        const stored_crc = std.mem.readInt(u32, meta_buf[0..4], .little);
        const timestamp_ns = std.mem.readInt(u64, meta_buf[4..12], .little);
        const stream_id = std.mem.readInt(u32, meta_buf[12..16], .little);
        const msg_type_id = std.mem.readInt(i32, meta_buf[16..20], .little);

        // Read payload
        if (payload_len > 0) { // kcov-skip: evaluated on every readRecord; both branches covered (zero-payload test); no own line record
            const payload_bytes_read = try self.file.readAll(buf[0..payload_len]);
            if (payload_bytes_read < payload_len) {
                return error.CorruptRecord;
            }
        }

        const next_offset = offset + @as(u64, length_prefix_size) + @as(u64, total_record_len);

        // Verify integrity.
        var hasher = std.hash.Crc32.init();
        hasher.update(&len_buf);
        hasher.update(meta_buf[4..]);
        hasher.update(buf[0..payload_len]);
        if (hasher.final() != stored_crc) {
            if (next_offset >= file_size) {
                // Last record in the file — torn tail, treat as end-of-segment.
                return null;
            }
            return error.CorruptRecord;
        }

        return ReadRecord{
            .record = Record{
                .timestamp_ns = timestamp_ns,
                .stream_id = stream_id,
                .msg_type_id = msg_type_id,
                .payload = buf[0..payload_len],
            },
            .next_offset = next_offset,
        };
    }

    pub fn isFull(self: *const Segment) bool {
        // Consider full if we can't fit even a minimal record (header + 1 byte payload)
        return self.write_offset + full_header_size + 1 > self.max_size;
    }

    fn segmentFilename(buf: *[32]u8, id: u64) []const u8 {
        return std.fmt.bufPrint(buf, "segment_{d:0>10}.dat", .{id}) catch unreachable;
    }
};

/// Manages a collection of segment files.
pub const SegmentManager = struct {
    config: SegmentConfig,
    dir: std.fs.Dir,
    active_segment: ?Segment,
    next_segment_id: u64,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: SegmentConfig) !SegmentManager {
        // Create or open the base directory. `.iterate = true` is required:
        // without it Linux opens the dir O_PATH and the scan below panics in
        // the std dir iterator (macOS happens to tolerate it).
        const dir = std.fs.cwd().makeOpenPath(config.base_path, .{ .iterate = true }) catch |err| {
            std.debug.print("Failed to open archive path '{s}': {}\n", .{ config.base_path, err });
            return err;
        };

        // Determine next segment ID by scanning existing files
        var next_id: u64 = 0;
        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind == .file) {
                // Parse segment_NNNNNNNNNN.dat
                if (std.mem.startsWith(u8, entry.name, "segment_") and std.mem.endsWith(u8, entry.name, ".dat")) {
                    const num_str = entry.name["segment_".len .. entry.name.len - ".dat".len];
                    const id = std.fmt.parseInt(u64, num_str, 10) catch continue;
                    if (id + 1 > next_id) { // kcov-skip: evaluated for every segment file in the reopen scan (reopen test); no own line record
                        next_id = id + 1;
                    }
                }
            }
        }

        return SegmentManager{
            .config = config,
            .dir = dir,
            .active_segment = null,
            .next_segment_id = next_id,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SegmentManager) void {
        if (self.active_segment) |*seg| {
            if (self.config.sync_on_close) {
                seg.file.sync() catch {};
            }
            seg.close();
            self.active_segment = null;
        }
        self.dir.close();
    }

    /// Fsync the active segment file (durability point for sync policies).
    pub fn syncActive(self: *SegmentManager) !void {
        if (self.active_segment) |*seg| {
            try seg.file.sync();
        }
    }

    /// Write a record, rotating segments as needed.
    pub fn write(self: *SegmentManager, record: Record) !void {
        // Ensure we have an active segment
        if (self.active_segment == null) {
            try self.rotateSegment();
        }

        // Try to write; if segment is full, rotate and retry
        if (self.active_segment) |*seg| {
            seg.appendRecord(record) catch |err| {
                if (err == error.SegmentFull) {
                    try self.rotateSegment();
                    if (self.active_segment) |*new_seg| {
                        try new_seg.appendRecord(record);
                    }
                } else {
                    return err;
                }
            };
        }
    }

    /// Open a segment for reading by ID.
    pub fn openSegment(self: *SegmentManager, id: u64) !Segment {
        _ = self.allocator;
        return Segment.open(self.dir, id);
    }

    fn rotateSegment(self: *SegmentManager) !void {
        // Check limits and create the NEW segment BEFORE closing the old one.
        // The previous order (close first, then create) left `active_segment`
        // pointing at a CLOSED file when the limit check or createFile failed:
        // the next write() would then append on a dead fd (EBADF) — or, if
        // the fd number had been reused, silently write records into an
        // unrelated file.
        if (self.config.max_segments) |max| {
            if (self.next_segment_id >= max) {
                return error.MaxSegmentsReached;
            }
        }

        const new_seg = try Segment.create(self.dir, self.next_segment_id, self.config.segment_size);

        // Only now retire the old segment; null it before close so no state
        // ever references a closed fd.
        if (self.active_segment) |*seg| {
            var old = seg.*;
            self.active_segment = null;
            if (self.config.sync_on_close) {
                old.file.sync() catch {};
            }
            old.close();
        }

        self.active_segment = new_seg;
        self.next_segment_id += 1;
    }
};

// =============================================================================
// Tests
// =============================================================================

test "Segment create, append, and read back" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const max_size: usize = 4096;
    var seg = try Segment.create(tmp_dir.dir, 0, max_size);
    defer seg.close();

    // Append a record
    const payload = "hello world";
    const rec = Record{
        .timestamp_ns = 1234567890,
        .stream_id = 42,
        .msg_type_id = 7,
        .payload = payload,
    };
    try seg.appendRecord(rec);

    // Read it back
    var buf: [1024]u8 = undefined;
    const result = try seg.readRecord(0, &buf);
    try std.testing.expect(result != null);
    const read_rec = result.?;
    try std.testing.expectEqual(@as(u64, 1234567890), read_rec.record.timestamp_ns);
    try std.testing.expectEqual(@as(u32, 42), read_rec.record.stream_id);
    try std.testing.expectEqual(@as(i32, 7), read_rec.record.msg_type_id);
    try std.testing.expectEqualSlices(u8, payload, read_rec.record.payload);

    // Reading past EOF should return null
    const result2 = try seg.readRecord(read_rec.next_offset, &buf);
    try std.testing.expect(result2 == null);
}

test "Segment multiple records" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 1, 4096);
    defer seg.close();

    // Write 3 records
    const messages = [_][]const u8{ "first", "second", "third" };
    for (messages, 0..) |msg, i| {
        try seg.appendRecord(Record{
            .timestamp_ns = @as(u64, @intCast(i)) * 1000,
            .stream_id = 1,
            .msg_type_id = @as(i32, @intCast(i)),
            .payload = msg,
        });
    }

    // Read all records back
    var buf: [1024]u8 = undefined;
    var offset: u64 = 0;
    var count: usize = 0;
    while (true) {
        const result = try seg.readRecord(offset, &buf);
        if (result == null) break;
        const rr = result.?;
        try std.testing.expectEqualSlices(u8, messages[count], rr.record.payload);
        offset = rr.next_offset;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 3), count);
}

test "Segment isFull" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    // Record layout: 4 (len) + 4 (crc) + 16 (header) = 24 bytes + payload.
    // A 14-byte-payload record takes 38 bytes; a minimal next record needs
    // full_header_size + 1 = 25 bytes.
    const tiny_size: usize = 63;
    var seg = try Segment.create(tmp_dir.dir, 2, tiny_size);
    defer seg.close();

    try std.testing.expect(!seg.isFull());

    // Write a small record: 24 bytes header + 14 bytes payload = 38 bytes.
    try seg.appendRecord(Record{
        .timestamp_ns = 0,
        .stream_id = 0,
        .msg_type_id = 0,
        .payload = "fill_segment!!",
    });

    // write_offset = 38. 38 + 25 = 63, which is NOT > 63 => not full yet.
    try std.testing.expect(!seg.isFull());

    // Write a 1-byte record (25 bytes). write_offset = 63.
    try seg.appendRecord(Record{
        .timestamp_ns = 0,
        .stream_id = 0,
        .msg_type_id = 0,
        .payload = "x",
    });

    // Now write_offset = 63. 63 + 25 = 88 > 63. Full!
    try std.testing.expect(seg.isFull());
}

test "Segment record CRC: corrupted tail record is end-of-segment, mid-file corruption is an error" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 3, 4096);
    defer seg.close();

    try seg.appendRecord(.{ .timestamp_ns = 1, .stream_id = 1, .msg_type_id = 1, .payload = "first-record" });
    try seg.appendRecord(.{ .timestamp_ns = 2, .stream_id = 1, .msg_type_id = 2, .payload = "second-record" });

    var buf: [1024]u8 = undefined;
    const r1 = (try seg.readRecord(0, &buf)).?;
    const second_offset = r1.next_offset;
    const r2 = (try seg.readRecord(second_offset, &buf)).?;
    const end_offset = r2.next_offset;

    // Corrupt one payload byte of the LAST record (simulates a torn/garbled tail).
    try seg.file.seekTo(end_offset - 1);
    try seg.file.writeAll(&[_]u8{0xFF});

    // First record still reads fine; the corrupt tail reads as end-of-segment.
    const again1 = (try seg.readRecord(0, &buf)).?;
    try std.testing.expectEqualSlices(u8, "first-record", again1.record.payload);
    try std.testing.expect((try seg.readRecord(second_offset, &buf)) == null);

    // Now corrupt the FIRST record (mid-file — more valid data follows it):
    // that is real corruption, not a torn tail, and must be a hard error.
    try seg.file.seekTo(second_offset - 1);
    try seg.file.writeAll(&[_]u8{0xAA});
    try std.testing.expectError(error.CorruptRecord, seg.readRecord(0, &buf));
}

test "Segment torn tail: truncated record is end-of-segment" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 4, 4096);
    defer seg.close();

    try seg.appendRecord(.{ .timestamp_ns = 1, .stream_id = 1, .msg_type_id = 1, .payload = "complete" });
    try seg.appendRecord(.{ .timestamp_ns = 2, .stream_id = 1, .msg_type_id = 2, .payload = "will-be-torn" });

    var buf: [256]u8 = undefined;
    const r1 = (try seg.readRecord(0, &buf)).?;

    // Truncate mid-way through the second record (simulates crash during write).
    try seg.file.setEndPos(r1.next_offset + 10);

    const again1 = (try seg.readRecord(0, &buf)).?;
    try std.testing.expectEqualSlices(u8, "complete", again1.record.payload);
    try std.testing.expect((try seg.readRecord(r1.next_offset, &buf)) == null);
}

test "Segment appendRecord rejects oversized payload" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 5, 1 << 40);
    defer seg.close();

    // Don't allocate 64MB+1 for real — a zero-length slice with a fake length
    // is enough to exercise the guard (the length check runs before any access).
    var fake: [0]u8 = .{};
    const oversized: []const u8 = fake[0..0].ptr[0 .. max_payload_size + 1];
    try std.testing.expectError(error.PayloadTooLarge, seg.appendRecord(.{
        .timestamp_ns = 0,
        .stream_id = 0,
        .msg_type_id = 0,
        .payload = oversized,
    }));
}

test "SegmentManager: failed rotation at max_segments leaves manager usable" {
    const test_path = "/tmp/zigbolt_test_segmgr_maxseg";
    std.fs.cwd().deleteTree(test_path) catch {};

    // Segment size fits exactly one 25-byte minimal record (4+4+16+1).
    var mgr = try SegmentManager.init(std.testing.allocator, SegmentConfig{
        .base_path = test_path,
        .segment_size = 25,
        .max_segments = 2,
    });
    defer {
        mgr.deinit();
        std.fs.cwd().deleteTree(test_path) catch {};
    }

    const rec = Record{ .timestamp_ns = 1, .stream_id = 1, .msg_type_id = 1, .payload = "a" };

    try mgr.write(rec); // segment 0
    try mgr.write(rec); // rotates to segment 1
    // Segment 1 full; rotation hits max_segments and must fail...
    try std.testing.expectError(error.MaxSegmentsReached, mgr.write(rec));
    // ...WITHOUT closing the active segment: before the fix the next write
    // went to a closed fd (EBADF) or a reused fd belonging to another file.
    try std.testing.expect(mgr.active_segment != null);
    try std.testing.expectError(error.MaxSegmentsReached, mgr.write(rec));

    // Both written records are still readable and intact.
    var buf: [128]u8 = undefined;
    var seg0 = try mgr.openSegment(0);
    defer seg0.close();
    const r0 = (try seg0.readRecord(0, &buf)).?;
    try std.testing.expectEqualSlices(u8, "a", r0.record.payload);

    var seg1 = try mgr.openSegment(1);
    defer seg1.close();
    const r1 = (try seg1.readRecord(0, &buf)).?;
    try std.testing.expectEqualSlices(u8, "a", r1.record.payload);
}

test "SegmentManager write and read" {
    const test_path = "/tmp/zigbolt_test_segmgr";
    std.fs.cwd().deleteTree(test_path) catch {};

    var mgr = try SegmentManager.init(std.testing.allocator, SegmentConfig{
        .base_path = test_path,
        .segment_size = 4096,
        .max_segments = null,
    });
    defer {
        mgr.deinit();
        std.fs.cwd().deleteTree(test_path) catch {};
    }

    // Write some records
    try mgr.write(Record{
        .timestamp_ns = 100,
        .stream_id = 1,
        .msg_type_id = 10,
        .payload = "record one",
    });
    try mgr.write(Record{
        .timestamp_ns = 200,
        .stream_id = 2,
        .msg_type_id = 20,
        .payload = "record two",
    });

    // Open segment 0 for reading
    var read_seg = try mgr.openSegment(0);
    defer read_seg.close();

    var buf: [1024]u8 = undefined;
    const r1 = try read_seg.readRecord(0, &buf);
    try std.testing.expect(r1 != null);
    try std.testing.expectEqualSlices(u8, "record one", r1.?.record.payload);

    const r2 = try read_seg.readRecord(r1.?.next_offset, &buf);
    try std.testing.expect(r2 != null);
    try std.testing.expectEqualSlices(u8, "record two", r2.?.record.payload);
}

test "readRecord flags an undersized length prefix as corrupt" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 0, 4096);
    defer seg.close();

    try seg.appendRecord(.{ .timestamp_ns = 1, .stream_id = 1, .msg_type_id = 1, .payload = "aaaa" });
    try seg.appendRecord(.{ .timestamp_ns = 2, .stream_id = 1, .msg_type_id = 2, .payload = "bbbb" });

    // Overwrite the SECOND record's length prefix with 5 (< record_overhead).
    const second_off: u64 = 4 + 20 + 4;
    try seg.file.seekTo(second_off); // kcov-skip: test line; the corruption it stages is asserted below; hit record oscillates between builds
    var bad: [4]u8 = undefined;
    std.mem.writeInt(u32, &bad, 5, .little);
    try seg.file.writeAll(&bad);

    var buf: [256]u8 = undefined;
    const first = try seg.readRecord(0, &buf);
    try std.testing.expectEqualSlices(u8, "aaaa", first.?.record.payload);
    try std.testing.expectError(error.CorruptRecord, seg.readRecord(second_off, &buf));
}

test "readRecord treats a partial length prefix as end-of-segment" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 0, 4096);
    defer seg.close();

    try seg.appendRecord(.{ .timestamp_ns = 1, .stream_id = 1, .msg_type_id = 1, .payload = "rec" });
    var buf: [256]u8 = undefined;
    const r1 = (try seg.readRecord(0, &buf)).?;

    // Leave only 2 bytes after the first record — fewer than the 4-byte length
    // prefix — as if the process crashed mid-prefix. The offset is still inside
    // the file, so the EOF guard does not fire; the short prefix read does.
    try seg.file.setEndPos(r1.next_offset + 2);
    try std.testing.expect((try seg.readRecord(r1.next_offset, &buf)) == null);
}

test "readRecord rejects a length prefix claiming an oversized payload" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 0, 1 << 40);
    defer seg.close();

    // Craft a length prefix whose decoded payload exceeds max_payload_size, then
    // sparsely extend the file so the record does not run past EOF. The
    // payload-size guard must reject it before any payload byte is touched.
    const bad_total: u32 = record_overhead + @as(u32, @intCast(max_payload_size)) + 1;
    var len_buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_buf, bad_total, .little);
    try seg.file.seekTo(0);
    try seg.file.writeAll(&len_buf);
    try seg.file.setEndPos(@as(u64, length_prefix_size) + bad_total);

    var buf: [64]u8 = undefined;
    try std.testing.expectError(error.CorruptRecord, seg.readRecord(0, &buf));
}

test "zero-length payload record roundtrips" {
    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    var seg = try Segment.create(tmp_dir.dir, 0, 4096);
    defer seg.close();

    try seg.appendRecord(.{ .timestamp_ns = 77, .stream_id = 9, .msg_type_id = -1, .payload = "" });

    var buf: [64]u8 = undefined;
    const rr = (try seg.readRecord(0, &buf)).?;
    try std.testing.expectEqual(@as(usize, 0), rr.record.payload.len);
    try std.testing.expectEqual(@as(u64, 77), rr.record.timestamp_ns);
    try std.testing.expectEqual(@as(i32, -1), rr.record.msg_type_id);
    try std.testing.expect((try seg.readRecord(rr.next_offset, &buf)) == null);
}

test "SegmentManager init fails cleanly when the base path is a file" {
    const p = "/tmp/zigbolt_test_seg_filepath";
    std.fs.cwd().deleteFile(p) catch {};
    defer std.fs.cwd().deleteFile(p) catch {};
    {
        const f = try std.fs.cwd().createFile(p, .{});
        f.close();
    }

    try std.testing.expect(std.meta.isError(SegmentManager.init(std.testing.allocator, .{
        .base_path = p,
        .segment_size = 4096,
    })));
}
