const std = @import("std");

pub const SegmentConfig = struct {
    base_path: []const u8 = "/tmp/zigbolt/archive",
    segment_size: usize = 256 * 1024 * 1024, // 256 MB per segment
    max_segments: ?usize = null, // null = unlimited
};

/// Header written before each record in a segment file.
/// Format: [u32 length][u64 timestamp_ns][u32 stream_id][i32 msg_type_id][payload...]
const record_header_size: u32 = @sizeOf(u64) + @sizeOf(u32) + @sizeOf(i32); // 16 bytes
const length_prefix_size: u32 = @sizeOf(u32); // 4 bytes
const full_header_size: u32 = length_prefix_size + record_header_size; // 20 bytes

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
    /// Record format: [u32 length][u64 timestamp_ns][u32 stream_id][i32 msg_type_id][payload]
    pub fn appendRecord(self: *Segment, record: Record) !void {
        const payload_len: u32 = @intCast(record.payload.len);
        const total_record_len: u32 = record_header_size + payload_len;
        const total_write_len: u64 = @as(u64, length_prefix_size) + total_record_len;

        // Check if this record would exceed segment size
        if (self.write_offset + total_write_len > self.max_size) {
            return error.SegmentFull;
        }

        // Build header in a buffer
        var header: [full_header_size]u8 = undefined;
        std.mem.writeInt(u32, header[0..4], total_record_len, .little);
        std.mem.writeInt(u64, header[4..12], record.timestamp_ns, .little);
        std.mem.writeInt(u32, header[12..16], record.stream_id, .little);
        std.mem.writeInt(i32, header[16..20], record.msg_type_id, .little);

        // Seek to write position and write header + payload
        try self.file.seekTo(self.write_offset);
        try self.file.writeAll(&header);
        try self.file.writeAll(record.payload);

        self.write_offset += total_write_len;
    }

    /// Read a record at the given offset. Returns null if at or past EOF.
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

        if (total_record_len < record_header_size) {
            return error.CorruptRecord;
        }

        const payload_len = total_record_len - record_header_size;
        if (payload_len > buf.len) {
            return error.BufferTooSmall;
        }

        // Read the rest of the record (header fields + payload)
        var header_buf: [record_header_size]u8 = undefined;
        const hdr_bytes_read = try self.file.readAll(&header_buf);
        if (hdr_bytes_read < record_header_size) {
            return error.CorruptRecord;
        }

        const timestamp_ns = std.mem.readInt(u64, header_buf[0..8], .little);
        const stream_id = std.mem.readInt(u32, header_buf[8..12], .little);
        const msg_type_id = std.mem.readInt(i32, header_buf[12..16], .little);

        // Read payload
        if (payload_len > 0) {
            const payload_bytes_read = try self.file.readAll(buf[0..payload_len]);
            if (payload_bytes_read < payload_len) {
                return error.CorruptRecord;
            }
        }

        const next_offset = offset + @as(u64, length_prefix_size) + @as(u64, total_record_len);

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
        // Create or open the base directory
        const dir = std.fs.cwd().makeOpenPath(config.base_path, .{}) catch |err| {
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
                    if (id + 1 > next_id) {
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
            seg.close();
            self.active_segment = null;
        }
        self.dir.close();
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
        // Close current segment if any
        if (self.active_segment) |*seg| {
            seg.close();
        }

        // Check max_segments limit
        if (self.config.max_segments) |max| {
            if (self.next_segment_id >= max) {
                return error.MaxSegmentsReached;
            }
        }

        const new_seg = try Segment.create(self.dir, self.next_segment_id, self.config.segment_size);
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

    // Very small segment: full_header_size (20) + 33 bytes payload = 53, plus next record needs 21 = 74
    // Set max to 55 so after one record it's full
    const tiny_size: usize = 55;
    var seg = try Segment.create(tmp_dir.dir, 2, tiny_size);
    defer seg.close();

    try std.testing.expect(!seg.isFull());

    // Write a small record: header (20) + 14 bytes payload = 34 bytes
    try seg.appendRecord(Record{
        .timestamp_ns = 0,
        .stream_id = 0,
        .msg_type_id = 0,
        .payload = "fill_segment!!",
    });

    // write_offset is now 34, remaining is 21, need 21 for minimal record => exactly fits
    // Actually 55 - 34 = 21, and full_header_size + 1 = 21, so not full (<=)
    // Let's just check the segment reports correctly based on remaining space
    // With 14 byte payload: write_offset = 34. full_header_size+1 = 21. 34+21=55, which is NOT > 55.
    // So it's not full yet. Let's write another tiny record.
    try seg.appendRecord(Record{
        .timestamp_ns = 0,
        .stream_id = 0,
        .msg_type_id = 0,
        .payload = "x",
    });

    // Now write_offset = 34 + 21 = 55. 55 + 21 = 76 > 55. Full!
    try std.testing.expect(seg.isFull());
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
