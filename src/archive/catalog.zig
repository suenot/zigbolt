const std = @import("std");
const builtin = @import("builtin");

/// Magic bytes "ZBCT" (ZigBolt CaTalog) as little-endian u32.
const CATALOG_MAGIC: u32 = 0x5A424354;
/// Version 2: entry_count widened u16 -> u32 (a u16 count silently
/// truncated/panicked beyond 65535 segments) and a reserved padding field.
const CATALOG_VERSION: u16 = 2;

/// Fsync a directory handle so a completed rename in it is durable.
fn syncDir(dir: std.fs.Dir) !void {
    if (builtin.os.tag == .windows) return;
    try std.posix.fsync(dir.fd);
}

/// Metadata entry describing a single segment in the archive.
pub const CatalogEntry = struct {
    segment_id: u32,
    /// File offset of first record.
    start_offset: u64,
    /// File offset after last record.
    end_offset: u64,
    /// Timestamp of first record.
    start_timestamp_ns: u64,
    /// Timestamp of last record.
    end_timestamp_ns: u64,
    /// Primary stream_id. NOTE: 0 is a RESERVED sentinel meaning "mixed
    /// streams" — a mixed entry matches every findByStream query. Real stream
    /// ids written by producers must therefore start at 1; using 0 as a legal
    /// stream id would make its segments match every query.
    stream_id: u32,
    /// Number of records in segment.
    record_count: u32,
    /// Total bytes of payload data.
    total_bytes: u64,
    /// Is this segment closed (no more writes)?
    closed: bool,

    /// Fixed serialized size on disk (bytes).
    pub const SERIALIZED_SIZE: usize = 56;

    /// Serialize this entry into a 56-byte buffer.
    /// Layout (all little-endian):
    ///   [0..4]   segment_id   u32
    ///   [4..12]  start_offset u64
    ///   [12..20] end_offset   u64
    ///   [20..28] start_timestamp_ns u64
    ///   [28..36] end_timestamp_ns   u64
    ///   [36..40] stream_id    u32
    ///   [40..44] record_count u32
    ///   [44..52] total_bytes  u64
    ///   [52..53] closed       u8 (0 or 1)
    ///   [53..56] reserved     3 bytes (zero)
    pub fn serialize(self: CatalogEntry, buf: []u8) void {
        std.debug.assert(buf.len >= SERIALIZED_SIZE);
        std.mem.writeInt(u32, buf[0..4], self.segment_id, .little);
        std.mem.writeInt(u64, buf[4..12], self.start_offset, .little);
        std.mem.writeInt(u64, buf[12..20], self.end_offset, .little);
        std.mem.writeInt(u64, buf[20..28], self.start_timestamp_ns, .little);
        std.mem.writeInt(u64, buf[28..36], self.end_timestamp_ns, .little);
        std.mem.writeInt(u32, buf[36..40], self.stream_id, .little);
        std.mem.writeInt(u32, buf[40..44], self.record_count, .little);
        std.mem.writeInt(u64, buf[44..52], self.total_bytes, .little);
        buf[52] = if (self.closed) 1 else 0;
        buf[53] = 0;
        buf[54] = 0;
        buf[55] = 0;
    }

    /// Deserialize a CatalogEntry from a 56-byte buffer.
    pub fn deserialize(buf: []const u8) CatalogEntry {
        std.debug.assert(buf.len >= SERIALIZED_SIZE);
        return CatalogEntry{
            .segment_id = std.mem.readInt(u32, buf[0..4], .little),
            .start_offset = std.mem.readInt(u64, buf[4..12], .little),
            .end_offset = std.mem.readInt(u64, buf[12..20], .little),
            .start_timestamp_ns = std.mem.readInt(u64, buf[20..28], .little),
            .end_timestamp_ns = std.mem.readInt(u64, buf[28..36], .little),
            .stream_id = std.mem.readInt(u32, buf[36..40], .little),
            .record_count = std.mem.readInt(u32, buf[40..44], .little),
            .total_bytes = std.mem.readInt(u64, buf[44..52], .little),
            .closed = buf[52] != 0,
        };
    }
};

/// Tracks all segment metadata and supports time/stream queries.
pub const Catalog = struct {
    entries: std.ArrayListUnmanaged(CatalogEntry),
    allocator: std.mem.Allocator,
    /// Path to catalog file on disk.
    path: [256]u8,
    path_len: u16,

    /// magic(4) + version(2) + reserved(2) + entry_count(4) = 12
    const HEADER_SIZE: usize = @sizeOf(u32) + @sizeOf(u16) + @sizeOf(u16) + @sizeOf(u32);

    pub fn init(allocator: std.mem.Allocator, base_path: []const u8) !Catalog {
        var cat = Catalog{
            .entries = .{},
            .allocator = allocator,
            .path = undefined,
            .path_len = 0,
        };

        // Build path: base_path / catalog.zbct
        const suffix = "/catalog.zbct";
        if (base_path.len + suffix.len > 256) { // kcov-skip: evaluated on every init; true branch covered by the overlong-base-path test; no own line record
            return error.PathTooLong;
        }
        @memcpy(cat.path[0..base_path.len], base_path);
        @memcpy(cat.path[base_path.len .. base_path.len + suffix.len], suffix);
        cat.path_len = @intCast(base_path.len + suffix.len);

        return cat;
    }

    pub fn deinit(self: *Catalog) void {
        self.entries.deinit(self.allocator);
    }

    /// Add a new catalog entry.
    pub fn addEntry(self: *Catalog, entry: CatalogEntry) !void {
        try self.entries.append(self.allocator, entry);
    }

    /// Update an existing entry matching segment_id.
    pub fn updateEntry(self: *Catalog, segment_id: u32, entry: CatalogEntry) !void {
        for (self.entries.items) |*e| {
            if (e.segment_id == segment_id) {
                e.* = entry;
                return;
            }
        }
        return error.SegmentNotFound;
    }

    /// Find all segments whose timestamp range overlaps [from_ns, to_ns].
    pub fn findByTimestamp(self: *const Catalog, from_ns: u64, to_ns: u64) []const CatalogEntry {
        // Binary search for the first entry that could overlap.
        // An entry overlaps if entry.start_timestamp_ns <= to_ns AND entry.end_timestamp_ns >= from_ns.
        // Since entries are appended in segment order (monotonically increasing timestamps in
        // typical usage), we can scan to find the contiguous matching slice.
        var start: usize = 0;
        var end: usize = self.entries.items.len; // kcov-skip: runs on every findByTimestamp (range-query test); no own line record

        // Skip entries whose end_timestamp_ns < from_ns (entirely before range).
        while (start < end) {
            if (self.entries.items[start].end_timestamp_ns < from_ns) {
                start += 1;
            } else {
                break;
            }
        }

        // Trim entries whose start_timestamp_ns > to_ns (entirely after range).
        while (end > start) {
            if (self.entries.items[end - 1].start_timestamp_ns > to_ns) {
                end -= 1;
            } else {
                break;
            }
        }

        return self.entries.items[start..end];
    }

    /// Find segments by stream_id. Caller must free the returned slice.
    /// Entries whose stream_id is the reserved "mixed" sentinel (0) match
    /// every query — see the CatalogEntry.stream_id doc; producers must use
    /// stream ids >= 1.
    pub fn findByStream(self: *const Catalog, stream_id: u32) ![]CatalogEntry {
        // Count matching entries.
        var count: usize = 0;
        for (self.entries.items) |e| {
            if (e.stream_id == stream_id or e.stream_id == 0) {
                count += 1;
            }
        }

        if (count == 0) {
            return &[_]CatalogEntry{};
        }

        const result = try self.allocator.alloc(CatalogEntry, count);
        var idx: usize = 0;
        for (self.entries.items) |e| {
            if (e.stream_id == stream_id or e.stream_id == 0) {
                result[idx] = e;
                idx += 1;
            }
        }
        return result;
    }

    /// Get entry by segment_id, or null if not found.
    pub fn getEntry(self: *const Catalog, segment_id: u32) ?CatalogEntry {
        for (self.entries.items) |e| {
            if (e.segment_id == segment_id) {
                return e;
            }
        }
        return null;
    }

    /// Save catalog to disk.
    /// File format: [magic:u32][version:u16][reserved:u16][entry_count:u32][entries...]
    ///
    /// Crash-safe atomic write (same recipe as the cluster snapshot/WAL):
    /// write `catalog.zbct.tmp`, fsync it, rename over the final name, fsync
    /// the directory. A crash mid-save leaves either the old catalog or the
    /// new one — never a torn file (the previous truncate-in-place version
    /// lost ALL segment metadata if the process died mid-write).
    pub fn save(self: *const Catalog) !void {
        const path_slice = self.path[0..self.path_len];

        if (self.entries.items.len > std.math.maxInt(u32)) {
            return error.TooManyEntries;
        }

        // Ensure parent directory exists.
        if (std.mem.lastIndexOfScalar(u8, path_slice, '/')) |sep| {
            std.fs.cwd().makePath(path_slice[0..sep]) catch {};
        }

        var tmp_path_buf: [264]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&tmp_path_buf, "{s}.tmp", .{path_slice}) catch
            return error.PathTooLong;

        // On any failure below, remove the temp file so it cannot linger.
        errdefer std.fs.cwd().deleteFile(tmp_path) catch {};

        // 1. Write the temp file and fsync it before rename.
        {
            const file = try std.fs.cwd().createFile(tmp_path, .{ .truncate = true });
            defer file.close();

            // Write header.
            var header: [HEADER_SIZE]u8 = undefined;
            std.mem.writeInt(u32, header[0..4], CATALOG_MAGIC, .little);
            std.mem.writeInt(u16, header[4..6], CATALOG_VERSION, .little);
            std.mem.writeInt(u16, header[6..8], 0, .little); // reserved
            const entry_count: u32 = @intCast(self.entries.items.len);
            std.mem.writeInt(u32, header[8..12], entry_count, .little);
            try file.writeAll(&header);

            // Write entries.
            var buf: [CatalogEntry.SERIALIZED_SIZE]u8 = undefined;
            for (self.entries.items) |entry| {
                entry.serialize(&buf);
                try file.writeAll(&buf);
            }

            try file.sync();
        }

        // 2. Atomically replace the previous catalog.
        try std.fs.cwd().rename(tmp_path, path_slice);

        // 3. Make the rename itself durable.
        if (std.mem.lastIndexOfScalar(u8, path_slice, '/')) |sep| {
            // `.iterate = true` so Linux gives a real fd, not O_PATH — fsync
            // on an O_PATH fd hits unreachable (EBADF).
            var dir = try std.fs.cwd().openDir(path_slice[0..sep], .{ .iterate = true });
            defer dir.close();
            try syncDir(dir);
        }
    }

    /// Load catalog from disk.
    pub fn load(allocator: std.mem.Allocator, path: []const u8) !Catalog {
        const file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
        defer file.close();

        // Read header.
        var header: [HEADER_SIZE]u8 = undefined;
        const hdr_read = try file.readAll(&header);
        if (hdr_read < HEADER_SIZE) {
            return error.CorruptCatalog;
        }

        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != CATALOG_MAGIC) {
            return error.InvalidMagic;
        }

        const version = std.mem.readInt(u16, header[4..6], .little);
        if (version != CATALOG_VERSION) {
            return error.UnsupportedVersion;
        }

        const entry_count = std.mem.readInt(u32, header[8..12], .little);

        // Cap entry_count against the actual file size BEFORE allocating:
        // a corrupt count of 0xFFFFFFFF would otherwise drive
        // ensureTotalCapacity to a ~240 GB allocation.
        const file_size = try file.getEndPos();
        const data_bytes = file_size -| HEADER_SIZE;
        const max_entries = data_bytes / CatalogEntry.SERIALIZED_SIZE;
        if (entry_count > max_entries) {
            return error.CorruptCatalog;
        }

        // Read entries.
        var entries = std.ArrayListUnmanaged(CatalogEntry){};
        try entries.ensureTotalCapacity(allocator, entry_count);

        var buf: [CatalogEntry.SERIALIZED_SIZE]u8 = undefined;
        for (0..entry_count) |_| {
            const bytes_read = try file.readAll(&buf);
            if (bytes_read < CatalogEntry.SERIALIZED_SIZE) {
                entries.deinit(allocator); // kcov-skip: defensive: entry_count is pre-validated against file size, so a short read needs concurrent truncation, which cannot be staged in-process
                return error.CorruptCatalog; // kcov-skip: defensive: see line above — unreachable without concurrent file truncation
            }
            entries.appendAssumeCapacity(CatalogEntry.deserialize(&buf));
        }

        // Reconstruct the path field.
        var cat = Catalog{
            .entries = entries,
            .allocator = allocator,
            .path = undefined,
            .path_len = 0,
        };
        if (path.len > 256) {
            entries.deinit(allocator);
            return error.PathTooLong;
        }
        @memcpy(cat.path[0..path.len], path);
        cat.path_len = @intCast(path.len);

        return cat;
    }

    /// Total records across all segments.
    pub fn totalRecords(self: *const Catalog) u64 {
        var total: u64 = 0;
        for (self.entries.items) |e| {
            total += e.record_count;
        }
        return total;
    }

    /// Total bytes across all segments.
    pub fn totalBytes(self: *const Catalog) u64 {
        var total: u64 = 0;
        for (self.entries.items) |e| {
            total += e.total_bytes;
        }
        return total;
    }

    /// Number of segments.
    pub fn segmentCount(self: *const Catalog) u32 {
        return @intCast(self.entries.items.len);
    }
};

// =============================================================================
// Tests
// =============================================================================

test "CatalogEntry serialize/deserialize roundtrip" {
    const entry = CatalogEntry{
        .segment_id = 42,
        .start_offset = 0,
        .end_offset = 1024,
        .start_timestamp_ns = 1_000_000_000,
        .end_timestamp_ns = 2_000_000_000,
        .stream_id = 7,
        .record_count = 100,
        .total_bytes = 8192,
        .closed = true,
    };

    var buf: [CatalogEntry.SERIALIZED_SIZE]u8 = undefined;
    entry.serialize(&buf);
    const restored = CatalogEntry.deserialize(&buf);

    try std.testing.expectEqual(entry.segment_id, restored.segment_id);
    try std.testing.expectEqual(entry.start_offset, restored.start_offset);
    try std.testing.expectEqual(entry.end_offset, restored.end_offset);
    try std.testing.expectEqual(entry.start_timestamp_ns, restored.start_timestamp_ns);
    try std.testing.expectEqual(entry.end_timestamp_ns, restored.end_timestamp_ns);
    try std.testing.expectEqual(entry.stream_id, restored.stream_id);
    try std.testing.expectEqual(entry.record_count, restored.record_count);
    try std.testing.expectEqual(entry.total_bytes, restored.total_bytes);
    try std.testing.expectEqual(entry.closed, restored.closed);
}

test "Catalog addEntry and getEntry" {
    var cat = try Catalog.init(std.testing.allocator, "/tmp/zigbolt_test_catalog");
    defer cat.deinit();

    const e1 = CatalogEntry{
        .segment_id = 0,
        .start_offset = 0,
        .end_offset = 500,
        .start_timestamp_ns = 100,
        .end_timestamp_ns = 200,
        .stream_id = 1,
        .record_count = 10,
        .total_bytes = 400,
        .closed = false,
    };
    try cat.addEntry(e1);

    const found = cat.getEntry(0);
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u32, 0), found.?.segment_id);
    try std.testing.expectEqual(@as(u64, 500), found.?.end_offset);

    // Non-existent segment.
    try std.testing.expect(cat.getEntry(99) == null);
}

test "Catalog findByTimestamp range query" {
    var cat = try Catalog.init(std.testing.allocator, "/tmp/zigbolt_test_cat_ts");
    defer cat.deinit();

    // Segment 0: [100, 200]
    try cat.addEntry(.{
        .segment_id = 0,
        .start_offset = 0,
        .end_offset = 100,
        .start_timestamp_ns = 100,
        .end_timestamp_ns = 200,
        .stream_id = 1,
        .record_count = 5,
        .total_bytes = 50,
        .closed = true,
    });
    // Segment 1: [300, 400]
    try cat.addEntry(.{
        .segment_id = 1,
        .start_offset = 0,
        .end_offset = 200,
        .start_timestamp_ns = 300,
        .end_timestamp_ns = 400,
        .stream_id = 1,
        .record_count = 5,
        .total_bytes = 50,
        .closed = true,
    });
    // Segment 2: [500, 600]
    try cat.addEntry(.{
        .segment_id = 2,
        .start_offset = 0,
        .end_offset = 300,
        .start_timestamp_ns = 500,
        .end_timestamp_ns = 600,
        .stream_id = 2,
        .record_count = 5,
        .total_bytes = 50,
        .closed = true,
    });

    // Query [150, 350] should match segments 0 and 1.
    const result = cat.findByTimestamp(150, 350);
    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqual(@as(u32, 0), result[0].segment_id);
    try std.testing.expectEqual(@as(u32, 1), result[1].segment_id);

    // Query [450, 550] should match segment 2 only.
    const result2 = cat.findByTimestamp(450, 550);
    try std.testing.expectEqual(@as(usize, 1), result2.len);
    try std.testing.expectEqual(@as(u32, 2), result2[0].segment_id);

    // Query [700, 800] should match nothing.
    const result3 = cat.findByTimestamp(700, 800);
    try std.testing.expectEqual(@as(usize, 0), result3.len);
}

test "Catalog findByStream filtering" {
    var cat = try Catalog.init(std.testing.allocator, "/tmp/zigbolt_test_cat_stream");
    defer cat.deinit();

    try cat.addEntry(.{
        .segment_id = 0,
        .start_offset = 0,
        .end_offset = 100,
        .start_timestamp_ns = 100,
        .end_timestamp_ns = 200,
        .stream_id = 1,
        .record_count = 5,
        .total_bytes = 50,
        .closed = true,
    });
    try cat.addEntry(.{
        .segment_id = 1,
        .start_offset = 0,
        .end_offset = 200,
        .start_timestamp_ns = 300,
        .end_timestamp_ns = 400,
        .stream_id = 2,
        .record_count = 5,
        .total_bytes = 50,
        .closed = true,
    });
    try cat.addEntry(.{
        .segment_id = 2,
        .start_offset = 0,
        .end_offset = 300,
        .start_timestamp_ns = 500,
        .end_timestamp_ns = 600,
        .stream_id = 0, // mixed — matches any stream query
        .record_count = 5,
        .total_bytes = 50,
        .closed = true,
    });

    // Stream 1 => segments 0 and 2 (mixed).
    const r1 = try cat.findByStream(1);
    defer cat.allocator.free(r1);
    try std.testing.expectEqual(@as(usize, 2), r1.len);
    try std.testing.expectEqual(@as(u32, 0), r1[0].segment_id);
    try std.testing.expectEqual(@as(u32, 2), r1[1].segment_id);

    // Stream 2 => segments 1 and 2 (mixed).
    const r2 = try cat.findByStream(2);
    defer cat.allocator.free(r2);
    try std.testing.expectEqual(@as(usize, 2), r2.len);
}

test "Catalog save and load from disk" {
    const test_path = "/tmp/zigbolt_test_cat_disk";
    std.fs.cwd().deleteTree(test_path) catch {};
    defer std.fs.cwd().deleteTree(test_path) catch {};

    // Create and populate a catalog.
    {
        var cat = try Catalog.init(std.testing.allocator, test_path);
        defer cat.deinit();

        try cat.addEntry(.{
            .segment_id = 0,
            .start_offset = 0,
            .end_offset = 1024,
            .start_timestamp_ns = 1000,
            .end_timestamp_ns = 2000,
            .stream_id = 5,
            .record_count = 42,
            .total_bytes = 900,
            .closed = true,
        });
        try cat.addEntry(.{
            .segment_id = 1,
            .start_offset = 0,
            .end_offset = 2048,
            .start_timestamp_ns = 3000,
            .end_timestamp_ns = 4000,
            .stream_id = 5,
            .record_count = 37,
            .total_bytes = 1800,
            .closed = false,
        });

        try cat.save();
    }

    // Load it back.
    const catalog_path = test_path ++ "/catalog.zbct";
    var loaded = try Catalog.load(std.testing.allocator, catalog_path);
    defer loaded.deinit();

    try std.testing.expectEqual(@as(u32, 2), loaded.segmentCount());

    const e0 = loaded.getEntry(0).?;
    try std.testing.expectEqual(@as(u32, 0), e0.segment_id);
    try std.testing.expectEqual(@as(u64, 1024), e0.end_offset);
    try std.testing.expectEqual(@as(u32, 42), e0.record_count);
    try std.testing.expect(e0.closed);

    const e1 = loaded.getEntry(1).?;
    try std.testing.expectEqual(@as(u32, 1), e1.segment_id);
    try std.testing.expectEqual(@as(u64, 2048), e1.end_offset);
    try std.testing.expect(!e1.closed);
}

test "Catalog save is atomic: no temp file left, repeated saves survive reopen" {
    const test_path = "/tmp/zigbolt_test_cat_atomic";
    std.fs.cwd().deleteTree(test_path) catch {};
    defer std.fs.cwd().deleteTree(test_path) catch {};

    const entry_template = CatalogEntry{
        .segment_id = 0,
        .start_offset = 0,
        .end_offset = 100,
        .start_timestamp_ns = 1,
        .end_timestamp_ns = 2,
        .stream_id = 1,
        .record_count = 1,
        .total_bytes = 10,
        .closed = true,
    };

    {
        var cat = try Catalog.init(std.testing.allocator, test_path);
        defer cat.deinit();
        try cat.addEntry(entry_template);
        try cat.save();

        // Save again with more entries: the file is replaced via rename,
        // never truncated in place, so a crash can't tear it.
        var e2 = entry_template;
        e2.segment_id = 1;
        try cat.addEntry(e2);
        try cat.save();
    }

    // No stale .tmp file after a successful save.
    try std.testing.expectError(
        error.FileNotFound,
        std.fs.cwd().openFile(test_path ++ "/catalog.zbct.tmp", .{}),
    );

    var loaded = try Catalog.load(std.testing.allocator, test_path ++ "/catalog.zbct");
    defer loaded.deinit();
    try std.testing.expectEqual(@as(u32, 2), loaded.segmentCount());
    try std.testing.expect(loaded.getEntry(1) != null);
}

test "Catalog load rejects corrupt entry_count without huge allocation" {
    const test_path = "/tmp/zigbolt_test_cat_badcount";
    std.fs.cwd().deleteTree(test_path) catch {};
    defer std.fs.cwd().deleteTree(test_path) catch {};

    {
        var cat = try Catalog.init(std.testing.allocator, test_path);
        defer cat.deinit();
        try cat.addEntry(.{
            .segment_id = 0,
            .start_offset = 0,
            .end_offset = 100,
            .start_timestamp_ns = 1,
            .end_timestamp_ns = 2,
            .stream_id = 1,
            .record_count = 1,
            .total_bytes = 10,
            .closed = true,
        });
        try cat.save();
    }

    // Corrupt entry_count (header offset 8) to 0xFFFFFFFF: would have driven
    // ensureTotalCapacity to a ~240 GB allocation before the file-size cap.
    {
        const f = try std.fs.cwd().openFile(test_path ++ "/catalog.zbct", .{ .mode = .read_write });
        defer f.close();
        try f.seekTo(8);
        try f.writeAll(&[_]u8{ 0xFF, 0xFF, 0xFF, 0xFF });
    }

    try std.testing.expectError(
        error.CorruptCatalog,
        Catalog.load(std.testing.allocator, test_path ++ "/catalog.zbct"),
    );
}

test "Catalog totalRecords and totalBytes" {
    var cat = try Catalog.init(std.testing.allocator, "/tmp/zigbolt_test_cat_totals");
    defer cat.deinit();

    try cat.addEntry(.{
        .segment_id = 0,
        .start_offset = 0,
        .end_offset = 100,
        .start_timestamp_ns = 100,
        .end_timestamp_ns = 200,
        .stream_id = 1,
        .record_count = 10,
        .total_bytes = 500,
        .closed = true,
    });
    try cat.addEntry(.{
        .segment_id = 1,
        .start_offset = 0,
        .end_offset = 200,
        .start_timestamp_ns = 300,
        .end_timestamp_ns = 400,
        .stream_id = 1,
        .record_count = 20,
        .total_bytes = 1000,
        .closed = true,
    });

    try std.testing.expectEqual(@as(u64, 30), cat.totalRecords());
    try std.testing.expectEqual(@as(u64, 1500), cat.totalBytes());
}

test "Empty catalog" {
    var cat = try Catalog.init(std.testing.allocator, "/tmp/zigbolt_test_cat_empty");
    defer cat.deinit();

    try std.testing.expectEqual(@as(u32, 0), cat.segmentCount());
    try std.testing.expectEqual(@as(u64, 0), cat.totalRecords());
    try std.testing.expectEqual(@as(u64, 0), cat.totalBytes());
    try std.testing.expect(cat.getEntry(0) == null);

    const ts_result = cat.findByTimestamp(0, 1000);
    try std.testing.expectEqual(@as(usize, 0), ts_result.len);
}

test "Catalog init rejects an overlong base path" {
    const long = "/tmp/" ++ "x" ** 250;
    try std.testing.expectError(error.PathTooLong, Catalog.init(std.testing.allocator, long));
}

test "Catalog updateEntry replaces matching entry only" {
    var cat = try Catalog.init(std.testing.allocator, "/tmp/zigbolt_test_cat_upd");
    defer cat.deinit();

    try cat.addEntry(.{
        .segment_id = 1,
        .start_offset = 0,
        .end_offset = 100,
        .start_timestamp_ns = 10,
        .end_timestamp_ns = 20,
        .stream_id = 1,
        .record_count = 5,
        .total_bytes = 80,
        .closed = false,
    });

    var updated = cat.entries.items[0];
    updated.record_count = 9;
    updated.closed = true;
    try cat.updateEntry(1, updated);
    try std.testing.expectEqual(@as(u32, 9), cat.entries.items[0].record_count);
    try std.testing.expect(cat.entries.items[0].closed);
    try std.testing.expectEqual(@as(usize, 1), cat.entries.items.len);

    // Unknown segment_id: rejected, nothing changed.
    try std.testing.expectError(error.SegmentNotFound, cat.updateEntry(42, updated));
    try std.testing.expectEqual(@as(u32, 9), cat.entries.items[0].record_count);
}

test "Catalog load rejects malformed headers" {
    const dir_path = "/tmp/zigbolt_test_cat_bad";
    std.fs.cwd().deleteTree(dir_path) catch {};
    defer std.fs.cwd().deleteTree(dir_path) catch {};
    try std.fs.cwd().makePath(dir_path);

    // a) Shorter than the 12-byte header.
    {
        const f = try std.fs.cwd().createFile(dir_path ++ "/short.zbct", .{});
        defer f.close();
        try f.writeAll(&[_]u8{ 1, 2, 3, 4, 5 });
    }
    try std.testing.expectError(error.CorruptCatalog, Catalog.load(std.testing.allocator, dir_path ++ "/short.zbct"));

    // b) Wrong magic.
    {
        const f = try std.fs.cwd().createFile(dir_path ++ "/magic.zbct", .{});
        defer f.close();
        var hdr = [_]u8{0} ** 12;
        std.mem.writeInt(u32, hdr[0..4], 0xDEADBEEF, .little);
        try f.writeAll(&hdr);
    }
    try std.testing.expectError(error.InvalidMagic, Catalog.load(std.testing.allocator, dir_path ++ "/magic.zbct"));

    // c) Right magic, wrong version.
    {
        const f = try std.fs.cwd().createFile(dir_path ++ "/version.zbct", .{});
        defer f.close();
        var hdr = [_]u8{0} ** 12;
        std.mem.writeInt(u32, hdr[0..4], CATALOG_MAGIC, .little);
        std.mem.writeInt(u16, hdr[4..6], CATALOG_VERSION + 1, .little);
        try f.writeAll(&hdr);
    }
    try std.testing.expectError(error.UnsupportedVersion, Catalog.load(std.testing.allocator, dir_path ++ "/version.zbct"));
}

test "Catalog load rejects an overlong path after reading entries" {
    // A valid (empty) catalog at a path longer than the 256-byte path field.
    const deep = "/tmp/zigbolt_test_cat_long_" ++ "d" ** 230;
    std.fs.cwd().deleteTree(deep) catch {};
    defer std.fs.cwd().deleteTree(deep) catch {};
    try std.fs.cwd().makePath(deep);

    const file_path = deep ++ "/c.zbct";
    {
        const f = try std.fs.cwd().createFile(file_path, .{});
        defer f.close();
        var hdr = [_]u8{0} ** 12;
        std.mem.writeInt(u32, hdr[0..4], CATALOG_MAGIC, .little);
        std.mem.writeInt(u16, hdr[4..6], CATALOG_VERSION, .little);
        try f.writeAll(&hdr); // entry_count = 0
    }
    try std.testing.expect(file_path.len > 256);
    try std.testing.expectError(error.PathTooLong, Catalog.load(std.testing.allocator, file_path));
}

test "Catalog save removes the temp file when the rename fails" {
    const base = "/tmp/zigbolt_test_cat_dirclash";
    std.fs.cwd().deleteTree(base) catch {};
    defer std.fs.cwd().deleteTree(base) catch {};

    // Occupy the catalog path with a DIRECTORY so the rename must fail.
    try std.fs.cwd().makePath(base ++ "/catalog.zbct");

    var cat = try Catalog.init(std.testing.allocator, base);
    defer cat.deinit();
    try cat.addEntry(.{
        .segment_id = 1,
        .start_offset = 0,
        .end_offset = 10,
        .start_timestamp_ns = 1,
        .end_timestamp_ns = 2,
        .stream_id = 1,
        .record_count = 1,
        .total_bytes = 10,
        .closed = true,
    });
    try std.testing.expect(std.meta.isError(cat.save()));

    // The errdefer removed the temp file.
    try std.testing.expectError(error.FileNotFound, std.fs.cwd().openFile(base ++ "/catalog.zbct.tmp", .{}));
}
