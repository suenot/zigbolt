const std = @import("std");
const builtin = @import("builtin");

/// Fsync a directory handle so a completed rename in it is durable.
fn syncDir(dir: std.fs.Dir) !void {
    if (builtin.os.tag == .windows) return;
    try std.posix.fsync(dir.fd);
}

/// A single entry in the sparse index, pointing to a record's location.
pub const IndexEntry = struct {
    /// Record sequence number within the segment.
    record_seq: u32,
    /// File offset of the record.
    file_offset: u64,
    /// Timestamp of the record.
    timestamp_ns: u64,
    /// Stream ID.
    stream_id: u32,

    /// Fixed serialized size on disk (bytes).
    /// Layout: [u32 record_seq][u64 file_offset][u64 timestamp_ns][u32 stream_id] = 24 bytes
    pub const SERIALIZED_SIZE: usize = 24;
};

/// Record header constants (must match segment.zig):
/// [u32 length][u32 crc][u64 timestamp_ns][u32 stream_id][i32 msg_type_id][payload]
const length_prefix_size: u32 = @sizeOf(u32);
const crc_size: u32 = @sizeOf(u32);
const record_header_size: u32 = @sizeOf(u64) + @sizeOf(u32) + @sizeOf(i32); // 16
const record_overhead: u32 = crc_size + record_header_size; // 20
const full_header_size: u32 = length_prefix_size + crc_size + record_header_size; // 24

/// Magic bytes for index file: "ZBIX" (ZigBolt IndeX).
const INDEX_MAGIC: u32 = 0x5A424958;
const INDEX_VERSION: u16 = 1;
const INDEX_HEADER_SIZE: usize = @sizeOf(u32) + @sizeOf(u16) + @sizeOf(u16) + @sizeOf(u32) + @sizeOf(u32);
// magic(4) + version(2) + reserved(2) + segment_id(4) + interval(4) = 16

/// Sparse index for fast lookup within a segment.
/// Indexes every Nth record, enabling binary search to find approximate
/// positions and then linear scan for exact matches.
pub const SparseIndex = struct {
    entries: std.ArrayListUnmanaged(IndexEntry),
    allocator: std.mem.Allocator,
    /// Index every Nth record.
    interval: u32,
    /// Segment ID this index belongs to.
    segment_id: u32,
    /// Records seen so far (used to decide when to index).
    records_seen: u32,

    pub fn init(allocator: std.mem.Allocator, segment_id: u32, interval: u32) SparseIndex {
        return SparseIndex{
            .entries = .{},
            .allocator = allocator,
            .interval = if (interval == 0) 1 else interval,
            .segment_id = segment_id,
            .records_seen = 0,
        };
    }

    pub fn deinit(self: *SparseIndex) void {
        self.entries.deinit(self.allocator);
    }

    /// Record a new entry. Only actually indexes every Nth record.
    pub fn record(self: *SparseIndex, record_seq: u32, file_offset: u64, timestamp_ns: u64, stream_id: u32) !void {
        if (self.records_seen % self.interval == 0) {
            try self.entries.append(self.allocator, IndexEntry{
                .record_seq = record_seq,
                .file_offset = file_offset,
                .timestamp_ns = timestamp_ns,
                .stream_id = stream_id,
            });
        }
        self.records_seen += 1;
    }

    /// Find a SCAN-START HINT for the given timestamp using binary search.
    ///
    /// Contract: returns the entry with the largest timestamp_ns <= the query
    /// when one exists. If ALL indexed entries are after the query, the FIRST
    /// entry is returned as the scan-start hint (for an index built from
    /// record 0 this is the start of the segment, which is the correct place
    /// to begin a forward scan). Returns null only for an empty index.
    /// Callers needing an exact "at or before" match must compare the
    /// returned entry's timestamp_ns against the query themselves.
    pub fn findByTimestamp(self: *const SparseIndex, timestamp_ns: u64) ?IndexEntry {
        const items = self.entries.items;
        if (items.len == 0) return null;

        // Binary search for the rightmost entry with timestamp_ns <= query.
        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (items[mid].timestamp_ns <= timestamp_ns) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        // lo is now the index of the first entry > timestamp_ns.
        if (lo == 0) {
            // All entries are after the query; return the first as nearest.
            return items[0];
        }
        return items[lo - 1];
    }

    /// Find a SCAN-START HINT for the given record sequence.
    /// Same contract as `findByTimestamp`: largest record_seq <= query when
    /// one exists, otherwise the first entry as a scan-start hint; null only
    /// for an empty index.
    pub fn findBySequence(self: *const SparseIndex, record_seq: u32) ?IndexEntry {
        const items = self.entries.items;
        if (items.len == 0) return null;

        var lo: usize = 0;
        var hi: usize = items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (items[mid].record_seq <= record_seq) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }

        if (lo == 0) {
            return items[0];
        }
        return items[lo - 1];
    }

    /// Save index to disk alongside segment file.
    /// File name: <base_path>/segment_NNNNNNNNNN.idx
    ///
    /// Crash-safe atomic write (same recipe as the cluster snapshot/WAL):
    /// write `<name>.tmp`, fsync it, rename over the final name, fsync the
    /// directory. A crash mid-save leaves either the old index or the new
    /// one — never a torn file.
    pub fn save(self: *const SparseIndex, base_path: []const u8) !void {
        var name_buf: [64]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "segment_{d:0>10}.idx", .{self.segment_id}) catch
            return error.PathTooLong;
        var tmp_name_buf: [64]u8 = undefined;
        const tmp_name = std.fmt.bufPrint(&tmp_name_buf, "segment_{d:0>10}.idx.tmp", .{self.segment_id}) catch
            return error.PathTooLong;

        if (self.entries.items.len > std.math.maxInt(u32)) {
            return error.TooManyEntries;
        }

        // Ensure parent directory exists.
        std.fs.cwd().makePath(base_path) catch {};
        var dir = try std.fs.cwd().openDir(base_path, .{});
        defer dir.close();

        // On any failure below, remove the temp file so it cannot linger.
        errdefer dir.deleteFile(tmp_name) catch {};

        // 1. Write the temp file and fsync it before rename.
        {
            const file = try dir.createFile(tmp_name, .{ .truncate = true });
            defer file.close();

            // Write header.
            var header: [INDEX_HEADER_SIZE]u8 = undefined;
            std.mem.writeInt(u32, header[0..4], INDEX_MAGIC, .little);
            std.mem.writeInt(u16, header[4..6], INDEX_VERSION, .little);
            std.mem.writeInt(u16, header[6..8], 0, .little); // reserved
            std.mem.writeInt(u32, header[8..12], self.segment_id, .little);
            std.mem.writeInt(u32, header[12..16], self.interval, .little);
            try file.writeAll(&header);

            // Write entry count as u32.
            var count_buf: [4]u8 = undefined;
            const entry_count: u32 = @intCast(self.entries.items.len);
            std.mem.writeInt(u32, &count_buf, entry_count, .little);
            try file.writeAll(&count_buf);

            // Write entries.
            var buf: [IndexEntry.SERIALIZED_SIZE]u8 = undefined;
            for (self.entries.items) |entry| {
                serializeIndexEntry(entry, &buf);
                try file.writeAll(&buf);
            }

            try file.sync();
        }

        // 2. Atomically replace any previous index file.
        try dir.rename(tmp_name, name);

        // 3. Make the rename itself durable.
        try syncDir(dir);
    }

    /// Load index from disk.
    pub fn load(allocator: std.mem.Allocator, base_path: []const u8, segment_id: u32) !SparseIndex {
        var path_buf: [300]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/segment_{d:0>10}.idx", .{ base_path, segment_id }) catch
            return error.PathTooLong;

        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
        defer file.close();

        // Read header.
        var header: [INDEX_HEADER_SIZE]u8 = undefined;
        const hdr_read = try file.readAll(&header);
        if (hdr_read < INDEX_HEADER_SIZE) return error.CorruptIndex;

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != INDEX_MAGIC) return error.InvalidMagic;

        const version = std.mem.readInt(u16, header[4..6], .little);
        if (version != INDEX_VERSION) return error.UnsupportedVersion;

        const stored_segment_id = std.mem.readInt(u32, header[8..12], .little);
        const interval = std.mem.readInt(u32, header[12..16], .little);

        // Read entry count.
        var count_buf: [4]u8 = undefined;
        const cnt_read = try file.readAll(&count_buf);
        if (cnt_read < 4) return error.CorruptIndex;
        const entry_count = std.mem.readInt(u32, &count_buf, .little);

        // Cap entry_count against the actual file size BEFORE allocating:
        // a corrupt count of 0xFFFFFFFF would otherwise drive
        // ensureTotalCapacity to a ~96 GB allocation.
        const file_size = try file.getEndPos();
        const data_bytes = file_size -| (INDEX_HEADER_SIZE + 4);
        const max_entries = data_bytes / IndexEntry.SERIALIZED_SIZE;
        if (entry_count > max_entries) return error.CorruptIndex;

        // Read entries.
        var entries = std.ArrayListUnmanaged(IndexEntry){};
        try entries.ensureTotalCapacity(allocator, entry_count);

        var buf: [IndexEntry.SERIALIZED_SIZE]u8 = undefined;
        for (0..entry_count) |_| {
            const bytes_read = try file.readAll(&buf);
            if (bytes_read < IndexEntry.SERIALIZED_SIZE) {
                entries.deinit(allocator);
                return error.CorruptIndex;
            }
            entries.appendAssumeCapacity(deserializeIndexEntry(&buf));
        }

        return SparseIndex{
            .entries = entries,
            .allocator = allocator,
            .interval = interval,
            .segment_id = stored_segment_id,
            .records_seen = if (entries.items.len > 0)
                entries.items[entries.items.len - 1].record_seq + 1
            else
                0,
        };
    }

    /// Rebuild index by scanning a segment file.
    pub fn rebuild(allocator: std.mem.Allocator, segment_file: std.fs.File, segment_id: u32, interval: u32) !SparseIndex {
        var idx = SparseIndex.init(allocator, segment_id, interval);
        errdefer idx.deinit();

        const file_size = try segment_file.getEndPos();
        var offset: u64 = 0;
        var seq: u32 = 0;

        while (offset < file_size) {
            // Read length prefix.
            try segment_file.seekTo(offset);
            var len_buf: [4]u8 = undefined;
            const len_read = try segment_file.readAll(&len_buf);
            if (len_read < 4) break;

            const total_record_len = std.mem.readInt(u32, &len_buf, .little);
            if (total_record_len < record_overhead) break;
            // Record running past EOF => torn tail; stop indexing here.
            if (offset + @as(u64, length_prefix_size) + @as(u64, total_record_len) > file_size) break;

            // Read crc + timestamp + stream_id from record header.
            var hdr_buf: [record_overhead]u8 = undefined;
            const hdr_read = try segment_file.readAll(&hdr_buf);
            if (hdr_read < record_overhead) break;

            const timestamp_ns = std.mem.readInt(u64, hdr_buf[4..12], .little);
            const stream_id = std.mem.readInt(u32, hdr_buf[12..16], .little);

            try idx.record(seq, offset, timestamp_ns, stream_id);

            offset += @as(u64, length_prefix_size) + @as(u64, total_record_len);
            seq += 1;
        }

        return idx;
    }

    fn serializeIndexEntry(entry: IndexEntry, buf: *[IndexEntry.SERIALIZED_SIZE]u8) void {
        std.mem.writeInt(u32, buf[0..4], entry.record_seq, .little);
        std.mem.writeInt(u64, buf[4..12], entry.file_offset, .little);
        std.mem.writeInt(u64, buf[12..20], entry.timestamp_ns, .little);
        std.mem.writeInt(u32, buf[20..24], entry.stream_id, .little);
    }

    fn deserializeIndexEntry(buf: *const [IndexEntry.SERIALIZED_SIZE]u8) IndexEntry {
        return IndexEntry{
            .record_seq = std.mem.readInt(u32, buf[0..4], .little),
            .file_offset = std.mem.readInt(u64, buf[4..12], .little),
            .timestamp_ns = std.mem.readInt(u64, buf[12..20], .little),
            .stream_id = std.mem.readInt(u32, buf[20..24], .little),
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

test "SparseIndex records every Nth entry" {
    var idx = SparseIndex.init(std.testing.allocator, 0, 4);
    defer idx.deinit();

    // Record 10 entries; only every 4th should be indexed (0, 4, 8).
    for (0..10) |i| {
        const seq: u32 = @intCast(i);
        try idx.record(seq, seq * 100, seq * 1000, 1);
    }

    try std.testing.expectEqual(@as(usize, 3), idx.entries.items.len);
    try std.testing.expectEqual(@as(u32, 0), idx.entries.items[0].record_seq);
    try std.testing.expectEqual(@as(u32, 4), idx.entries.items[1].record_seq);
    try std.testing.expectEqual(@as(u32, 8), idx.entries.items[2].record_seq);
}

test "SparseIndex findByTimestamp binary search" {
    var idx = SparseIndex.init(std.testing.allocator, 0, 1);
    defer idx.deinit();

    // Insert entries with timestamps 100, 200, 300, 400, 500.
    for (0..5) |i| {
        const seq: u32 = @intCast(i);
        const ts: u64 = (seq + 1) * 100;
        try idx.record(seq, seq * 50, ts, 1);
    }

    // Exact match.
    const r1 = idx.findByTimestamp(300).?;
    try std.testing.expectEqual(@as(u64, 300), r1.timestamp_ns);
    try std.testing.expectEqual(@as(u32, 2), r1.record_seq);

    // Between entries: 250 => should return entry at 200.
    const r2 = idx.findByTimestamp(250).?;
    try std.testing.expectEqual(@as(u64, 200), r2.timestamp_ns);

    // Before all entries: 50 => should return first entry (nearest).
    const r3 = idx.findByTimestamp(50).?;
    try std.testing.expectEqual(@as(u64, 100), r3.timestamp_ns);

    // After all entries: 999 => should return last entry.
    const r4 = idx.findByTimestamp(999).?;
    try std.testing.expectEqual(@as(u64, 500), r4.timestamp_ns);
}

test "SparseIndex findBySequence" {
    var idx = SparseIndex.init(std.testing.allocator, 0, 2);
    defer idx.deinit();

    // Records: 0, 2, 4, 6, 8 indexed.
    for (0..10) |i| {
        const seq: u32 = @intCast(i);
        try idx.record(seq, seq * 100, seq * 1000, 1);
    }

    // Find seq 5 => nearest indexed at-or-before is seq 4.
    const r1 = idx.findBySequence(5).?;
    try std.testing.expectEqual(@as(u32, 4), r1.record_seq);

    // Find seq 4 => exact match.
    const r2 = idx.findBySequence(4).?;
    try std.testing.expectEqual(@as(u32, 4), r2.record_seq);

    // Find seq 0 => exact match.
    const r3 = idx.findBySequence(0).?;
    try std.testing.expectEqual(@as(u32, 0), r3.record_seq);
}

test "SparseIndex save/load roundtrip" {
    const test_path = "/tmp/zigbolt_test_idx_disk";
    std.fs.cwd().deleteTree(test_path) catch {};
    defer std.fs.cwd().deleteTree(test_path) catch {};

    // Create and populate an index.
    {
        var idx = SparseIndex.init(std.testing.allocator, 7, 2);
        defer idx.deinit();

        for (0..8) |i| {
            const seq: u32 = @intCast(i);
            try idx.record(seq, seq * 64, (seq + 1) * 1_000_000, seq % 3);
        }

        try idx.save(test_path);
    }

    // Load it back.
    var loaded = try SparseIndex.load(std.testing.allocator, test_path, 7);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(u32, 7), loaded.segment_id);
    try std.testing.expectEqual(@as(u32, 2), loaded.interval);
    // Indexed: 0, 2, 4, 6 => 4 entries.
    try std.testing.expectEqual(@as(usize, 4), loaded.entries.items.len);
    try std.testing.expectEqual(@as(u32, 0), loaded.entries.items[0].record_seq);
    try std.testing.expectEqual(@as(u32, 6), loaded.entries.items[3].record_seq);
    try std.testing.expectEqual(@as(u64, 7_000_000), loaded.entries.items[3].timestamp_ns);
}

test "SparseIndex with interval=1 (full index)" {
    var idx = SparseIndex.init(std.testing.allocator, 0, 1);
    defer idx.deinit();

    for (0..5) |i| {
        const seq: u32 = @intCast(i);
        try idx.record(seq, seq * 32, seq * 500, 1);
    }

    // All 5 should be indexed.
    try std.testing.expectEqual(@as(usize, 5), idx.entries.items.len);
}

test "Empty index queries return null" {
    var idx = SparseIndex.init(std.testing.allocator, 0, 4);
    defer idx.deinit();

    try std.testing.expect(idx.findByTimestamp(100) == null);
    try std.testing.expect(idx.findBySequence(0) == null);
}

test "SparseIndex save is atomic: no temp file left, repeated saves survive reopen" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    {
        var idx = SparseIndex.init(std.testing.allocator, 3, 1);
        defer idx.deinit();
        try idx.record(0, 0, 111, 1);
        try idx.save(base);

        // Save again with more entries — overwrites via rename, never truncate-in-place.
        try idx.record(1, 40, 222, 1);
        try idx.save(base);
    }

    // No stale .tmp file may remain after a successful save.
    try std.testing.expectError(error.FileNotFound, tmp.dir.openFile("segment_0000000003.idx.tmp", .{}));

    var loaded = try SparseIndex.load(std.testing.allocator, base, 3);
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), loaded.entries.items.len);
    try std.testing.expectEqual(@as(u64, 222), loaded.entries.items[1].timestamp_ns);
}

test "SparseIndex load rejects corrupt entry_count without huge allocation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    {
        var idx = SparseIndex.init(std.testing.allocator, 9, 1);
        defer idx.deinit();
        try idx.record(0, 0, 100, 1);
        try idx.save(base);
    }

    // Corrupt the entry count field (4 bytes after the 16-byte header) to
    // 0xFFFFFFFF: would have driven ensureTotalCapacity to a ~96 GB alloc.
    {
        const f = try tmp.dir.openFile("segment_0000000009.idx", .{ .mode = .read_write });
        defer f.close();
        try f.seekTo(INDEX_HEADER_SIZE);
        try f.writeAll(&[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    }

    try std.testing.expectError(error.CorruptIndex, SparseIndex.load(std.testing.allocator, base, 9));
}
