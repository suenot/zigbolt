const std = @import("std");
const builtin = @import("builtin");

/// Magic bytes: "ZBSP" (ZigBolt SnaPshot).
const SNAPSHOT_MAGIC: u32 = 0x5A425350;
/// Current snapshot format version.
const SNAPSHOT_VERSION: u16 = 1;

/// Header size: magic(4) + version(2) + last_included_term(8) + last_included_index(8) + state_size(4) = 26 bytes.
const HEADER_SIZE: usize = 4 + 2 + 8 + 8 + 4;
/// CRC trailer size.
const CRC_SIZE: usize = 4;

/// Snapshot data returned by loadLatestSnapshot. Caller owns `data`.
pub const SnapshotData = struct {
    last_included_term: u64,
    last_included_index: u64,
    data: []u8,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *SnapshotData) void {
        self.allocator.free(self.data);
    }
};

/// Manages Raft snapshots on disk.
///
/// Snapshot file format:
///   [magic: u32 = 0x5A425350]      "ZBSP"
///   [version: u16 = 1]
///   [last_included_term: u64]
///   [last_included_index: u64]
///   [state_size: u32]
///   [state_data: state_size bytes]
///   [crc32: u32]                    CRC of everything except this field
pub const SnapshotManager = struct {
    /// Directory for snapshot files.
    base_path: [256]u8,
    base_path_len: u16,
    /// Trigger snapshot after this many committed entries since last snapshot.
    snapshot_interval: u64,
    /// Number of entries committed since last snapshot.
    entries_since_snapshot: u64,
    /// Last snapshot metadata.
    last_snapshot: ?SnapshotMeta,
    allocator: std.mem.Allocator,

    pub const SnapshotMeta = struct {
        last_included_term: u64,
        last_included_index: u64,
        file_size: u64,
        timestamp_ns: u64,
    };

    pub const SnapshotConfig = struct {
        base_path: []const u8 = ".",
        snapshot_interval: u64 = 10000,
    };

    pub fn init(allocator: std.mem.Allocator, config: SnapshotConfig) !SnapshotManager {
        var path_buf: [256]u8 = undefined;
        if (config.base_path.len > path_buf.len) {
            return error.PathTooLong;
        }
        const len: u16 = @intCast(config.base_path.len);
        @memcpy(path_buf[0..len], config.base_path);

        return .{
            .base_path = path_buf,
            .base_path_len = len,
            .snapshot_interval = config.snapshot_interval,
            .entries_since_snapshot = 0,
            .last_snapshot = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SnapshotManager) void {
        _ = self;
    }

    /// Returns true if enough entries have been committed to warrant a snapshot.
    pub fn shouldSnapshot(self: *const SnapshotManager) bool {
        return self.entries_since_snapshot >= self.snapshot_interval;
    }

    /// Called on each committed entry to track when a snapshot is needed.
    pub fn onEntryCommitted(self: *SnapshotManager) void {
        self.entries_since_snapshot += 1;
    }

    /// Write a snapshot to disk crash-safely.
    /// File name: snapshot_{last_index}.zbsp
    ///
    /// Atomic-write recipe: the snapshot is written to a `.tmp` file first,
    /// fsync'd, then renamed over the final name, then the directory is
    /// fsync'd. A crash at any point leaves either no snapshot (only a stale
    /// `.tmp`, which load/cleanup ignore/remove) or a complete valid one —
    /// never a torn `snapshot_N.zbsp`.
    pub fn takeSnapshot(self: *SnapshotManager, last_term: u64, last_index: u64, state_data: []const u8) !void {
        // Format final and temp file names.
        var name_buf: [128]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "snapshot_{d}.zbsp", .{last_index}) catch return error.NameTooLong;
        var tmp_name_buf: [128]u8 = undefined;
        const tmp_name = std.fmt.bufPrint(&tmp_name_buf, "snapshot_{d}.zbsp.tmp", .{last_index}) catch return error.NameTooLong;

        const base = self.base_path[0..self.base_path_len];

        // Build header.
        const state_size: u32 = @intCast(state_data.len);
        var header: [HEADER_SIZE]u8 = undefined;
        @memcpy(header[0..4], std.mem.asBytes(&SNAPSHOT_MAGIC));
        @memcpy(header[4..6], std.mem.asBytes(&SNAPSHOT_VERSION));
        @memcpy(header[6..14], std.mem.asBytes(&last_term));
        @memcpy(header[14..22], std.mem.asBytes(&last_index));
        @memcpy(header[22..26], std.mem.asBytes(&state_size));

        // Compute CRC over header + state_data.
        var hasher = std.hash.Crc32.init();
        hasher.update(&header);
        hasher.update(state_data);
        const crc = hasher.final();

        // `.iterate = true` so Linux gives a real fd, not O_PATH — the dir
        // fsync in syncDir() would otherwise hit unreachable (EBADF).
        var dir = try std.fs.cwd().openDir(base, .{ .iterate = true });
        defer dir.close();

        // On any failure below, remove the temp file so it cannot linger.
        errdefer dir.deleteFile(tmp_name) catch {};

        // 1. Write the temp file and fsync it before rename.
        {
            const file = try dir.createFile(tmp_name, .{});
            defer file.close();

            try file.writeAll(&header);
            if (state_data.len > 0) {
                try file.writeAll(state_data);
            }
            try file.writeAll(std.mem.asBytes(&crc));
            try file.sync();
        }

        // 2. Atomically publish the snapshot under its final name.
        try dir.rename(tmp_name, name);

        // 3. Make the rename itself durable.
        try syncDir(dir);

        // Get file size for metadata.
        const file_size: u64 = HEADER_SIZE + state_data.len + CRC_SIZE;

        // Update internal state.
        self.entries_since_snapshot = 0;
        self.last_snapshot = .{
            .last_included_term = last_term,
            .last_included_index = last_index,
            .file_size = file_size,
            .timestamp_ns = @intCast(std.time.nanoTimestamp()),
        };
    }

    /// Load the latest VALID snapshot from disk. Returns null if no snapshots exist.
    /// Candidates are tried from newest (highest index) to oldest; corrupt or
    /// torn files are skipped so a single bad write cannot brick recovery.
    /// If snapshots exist but none validate, the newest candidate's error is returned.
    /// Caller owns the returned SnapshotData and must call deinit().
    pub fn loadLatestSnapshot(self: *SnapshotManager) !?SnapshotData {
        const base = self.base_path[0..self.base_path_len];

        // Scan directory for snapshot files.
        var dir = std.fs.cwd().openDir(base, .{ .iterate = true }) catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };
        defer dir.close();

        var candidates = std.ArrayListUnmanaged(SnapshotFileEntry){};
        defer candidates.deinit(self.allocator);

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind != .file) continue;
            if (entry.name.len > 128) continue;
            // `.tmp` files (torn writes) never parse as snapshots and are ignored.
            const idx = parseSnapshotIndex(entry.name) orelse continue;
            var c: SnapshotFileEntry = .{ .index = idx, .name = undefined, .name_len = entry.name.len };
            @memcpy(c.name[0..entry.name.len], entry.name);
            try candidates.append(self.allocator, c);
        }

        if (candidates.items.len == 0) return null;

        // Sort by index descending: newest first.
        std.mem.sort(SnapshotFileEntry, candidates.items, {}, struct {
            fn newestFirst(_: void, a: SnapshotFileEntry, b: SnapshotFileEntry) bool {
                return a.index > b.index;
            }
        }.newestFirst);

        var first_err: ?SnapshotValidationError = null;
        for (candidates.items) |c| {
            var full_path_buf: [512]u8 = undefined;
            const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{ base, c.name[0..c.name_len] }) catch return error.NameTooLong;

            const snap = loadSnapshotFile(self.allocator, full_path) catch |err| switch (err) {
                error.InvalidSnapshot, error.CrcMismatch, error.UnsupportedVersion, error.FileNotFound => |e| {
                    // Corrupt/torn/vanished candidate — fall back to an older one.
                    if (first_err == null) first_err = e;
                    continue;
                },
                else => return err, // kcov-skip: OS I/O failure pass-through; cannot be injected in-process
            };
            return snap;
        }

        // Snapshots exist but none validate: surface the newest one's error so
        // the caller can distinguish "no snapshot" from "all snapshots corrupt".
        return first_err.?;
    }

    /// Return metadata of the latest snapshot without loading state data.
    pub fn getLatestMeta(self: *const SnapshotManager) ?SnapshotMeta {
        return self.last_snapshot; // kcov-skip: runs in the getLatestMeta test (result asserted); no own line record
    }

    /// Delete all but the N newest VALID snapshot files.
    ///
    /// Crash-safety rules:
    /// - CRC-invalid (torn/corrupt) files never count toward `keep_count` and
    ///   are deleted as garbage.
    /// - The newest VALID snapshot is never deleted (keep_count is clamped to >= 1).
    /// - Stale `.zbsp.tmp` files left by a crash mid-snapshot are removed.
    pub fn cleanOldSnapshots(self: *SnapshotManager, keep_count: usize) !void {
        const base = self.base_path[0..self.base_path_len];

        var dir = std.fs.cwd().openDir(base, .{ .iterate = true }) catch |err| {
            if (err == error.FileNotFound) return;
            return err;
        };
        defer dir.close();

        // Never delete the newest valid snapshot, even with keep_count == 0.
        const keep = @max(keep_count, 1);

        var entries = std.ArrayListUnmanaged(SnapshotFileEntry){};
        defer entries.deinit(self.allocator);
        var stale_tmps = std.ArrayListUnmanaged(SnapshotFileEntry){};
        defer stale_tmps.deinit(self.allocator);

        var iter = dir.iterate();
        while (try iter.next()) |de| {
            if (de.kind != .file) continue;
            if (de.name.len > 128) continue;
            var e: SnapshotFileEntry = .{ .index = 0, .name = undefined, .name_len = de.name.len };
            @memcpy(e.name[0..de.name.len], de.name);
            if (std.mem.endsWith(u8, de.name, ".zbsp.tmp")) {
                // Torn write left behind by a crash mid-takeSnapshot.
                try stale_tmps.append(self.allocator, e);
                continue;
            }
            e.index = parseSnapshotIndex(de.name) orelse continue;
            try entries.append(self.allocator, e);
        }

        // Remove stale temp files (collected first; deleting during iteration
        // can skip directory entries on some filesystems).
        for (stale_tmps.items) |e| {
            dir.deleteFile(e.name[0..e.name_len]) catch {};
        }

        // Sort by index descending: newest first.
        std.mem.sort(SnapshotFileEntry, entries.items, {}, struct {
            fn newestFirst(_: void, a: SnapshotFileEntry, b: SnapshotFileEntry) bool {
                return a.index > b.index;
            }
        }.newestFirst);

        // Keep the newest `keep` snapshots that validate; delete everything
        // else (older valid ones beyond `keep`, and all corrupt ones).
        var kept: usize = 0;
        for (entries.items) |e| {
            const name = e.name[0..e.name_len];
            if (kept < keep and validateSnapshotFile(dir, name)) {
                kept += 1;
            } else {
                dir.deleteFile(name) catch {};
            }
        }
    }

    // ── Internal helpers ─────────────────────────────────────

    /// Directory entry for a snapshot candidate file.
    const SnapshotFileEntry = struct {
        index: u64,
        name: [128]u8,
        name_len: usize,
    };

    /// Errors that mean "this particular snapshot file is unusable" (as
    /// opposed to environmental errors like OutOfMemory that must propagate).
    const SnapshotValidationError = error{
        InvalidSnapshot,
        CrcMismatch,
        UnsupportedVersion,
        FileNotFound,
    };

    /// Fsync a directory handle so a completed rename/unlink in it is durable.
    /// No-op on Windows where directory handles cannot be flushed.
    fn syncDir(dir: std.fs.Dir) !void {
        if (builtin.os.tag == .windows) return;
        try std.posix.fsync(dir.fd);
    }

    /// Streaming validation of a snapshot file (header + size + CRC) without
    /// allocating the state payload. Returns false for any torn/corrupt file.
    fn validateSnapshotFile(dir: std.fs.Dir, name: []const u8) bool {
        const file = dir.openFile(name, .{}) catch return false;
        defer file.close();

        const file_size = file.getEndPos() catch return false;
        if (file_size < HEADER_SIZE + CRC_SIZE) return false;

        var header: [HEADER_SIZE]u8 = undefined;
        const header_read = file.readAll(&header) catch return false;
        if (header_read < HEADER_SIZE) return false; // kcov-skip: defensive: file size was just checked >= header+crc; a short read needs concurrent truncation

        const magic = std.mem.bytesToValue(u32, header[0..4]);
        if (magic != SNAPSHOT_MAGIC) return false;
        const version = std.mem.bytesToValue(u16, header[4..6]);
        if (version != SNAPSHOT_VERSION) return false;

        const state_size = std.mem.bytesToValue(u32, header[22..26]);
        if (state_size != file_size - HEADER_SIZE - CRC_SIZE) return false;

        var hasher = std.hash.Crc32.init();
        hasher.update(&header);

        var remaining: u64 = state_size;
        var chunk: [4096]u8 = undefined;
        while (remaining > 0) {
            const want: usize = @intCast(@min(remaining, chunk.len));
            const got = file.readAll(chunk[0..want]) catch return false;
            if (got < want) return false;
            hasher.update(chunk[0..want]);
            remaining -= want;
        }

        var crc_buf: [CRC_SIZE]u8 = undefined;
        const crc_read = file.readAll(&crc_buf) catch return false;
        if (crc_read < CRC_SIZE) return false;
        const stored_crc = std.mem.bytesToValue(u32, &crc_buf);

        return stored_crc == hasher.final();
    }

    /// Parse "snapshot_{index}.zbsp" and return the index, or null.
    fn parseSnapshotIndex(name: []const u8) ?u64 {
        const prefix = "snapshot_";
        const suffix = ".zbsp";
        if (name.len <= prefix.len + suffix.len) return null;
        if (!std.mem.startsWith(u8, name, prefix)) return null;
        if (!std.mem.endsWith(u8, name, suffix)) return null;
        const num_str = name[prefix.len .. name.len - suffix.len];
        return std.fmt.parseInt(u64, num_str, 10) catch null;
    }

    /// Load and validate a snapshot file.
    fn loadSnapshotFile(allocator: std.mem.Allocator, path: []const u8) !SnapshotData {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        // Read header.
        var header: [HEADER_SIZE]u8 = undefined;
        const header_read = try file.readAll(&header);
        if (header_read < HEADER_SIZE) return error.InvalidSnapshot;

        // Validate magic.
        const magic = std.mem.bytesToValue(u32, header[0..4]);
        if (magic != SNAPSHOT_MAGIC) return error.InvalidSnapshot;

        // Validate version.
        const version = std.mem.bytesToValue(u16, header[4..6]);
        if (version != SNAPSHOT_VERSION) return error.UnsupportedVersion;

        const last_term = std.mem.bytesToValue(u64, header[6..14]);
        const last_index = std.mem.bytesToValue(u64, header[14..22]);
        const state_size = std.mem.bytesToValue(u32, header[22..26]);

        // Cap the untrusted state_size against the actual file size BEFORE
        // allocating, so a corrupt header cannot trigger a multi-GB alloc.
        const file_size = try file.getEndPos();
        if (file_size < HEADER_SIZE + CRC_SIZE) return error.InvalidSnapshot;
        if (state_size > file_size - HEADER_SIZE - CRC_SIZE) return error.InvalidSnapshot;

        // Read state data.
        const data = try allocator.alloc(u8, state_size);
        errdefer allocator.free(data);

        if (state_size > 0) {
            const data_read = try file.readAll(data);
            if (data_read < state_size) {
                return error.InvalidSnapshot;
            }
        }

        // Read CRC.
        var crc_buf: [CRC_SIZE]u8 = undefined;
        const crc_read = try file.readAll(&crc_buf);
        if (crc_read < CRC_SIZE) return error.InvalidSnapshot;
        const stored_crc = std.mem.bytesToValue(u32, &crc_buf);

        // Verify CRC over header + state_data.
        var hasher = std.hash.Crc32.init();
        hasher.update(&header);
        hasher.update(data);
        const computed_crc = hasher.final();

        if (stored_crc != computed_crc) {
            return error.CrcMismatch;
        }

        return .{
            .last_included_term = last_term,
            .last_included_index = last_index,
            .data = data,
            .allocator = allocator,
        };
    }
};

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

const TEST_DIR = "/tmp/zigbolt_test_snapshots";

fn ensureTestDir() !void {
    std.fs.cwd().makeDir(TEST_DIR) catch |err| {
        if (err != error.PathAlreadyExists) return err;
    };
}

fn cleanTestDir() void {
    std.fs.cwd().deleteTree(TEST_DIR) catch {};
}

test "Snapshot: takeSnapshot creates file with correct format" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    const state = "hello snapshot state";
    try mgr.takeSnapshot(3, 42, state);

    // Verify the file exists and has correct content.
    const path = TEST_DIR ++ "/snapshot_42.zbsp";
    const file = try std.fs.cwd().openFile(path, .{});
    defer file.close();

    var header: [HEADER_SIZE]u8 = undefined;
    const n = try file.readAll(&header);
    try testing.expectEqual(HEADER_SIZE, n);

    const magic = std.mem.bytesToValue(u32, header[0..4]);
    try testing.expectEqual(SNAPSHOT_MAGIC, magic);

    const version = std.mem.bytesToValue(u16, header[4..6]);
    try testing.expectEqual(SNAPSHOT_VERSION, version);

    const term = std.mem.bytesToValue(u64, header[6..14]);
    try testing.expectEqual(@as(u64, 3), term);

    const index = std.mem.bytesToValue(u64, header[14..22]);
    try testing.expectEqual(@as(u64, 42), index);

    const state_size = std.mem.bytesToValue(u32, header[22..26]);
    try testing.expectEqual(@as(u32, @intCast(state.len)), state_size);

    // Verify entries_since_snapshot is reset.
    try testing.expectEqual(@as(u64, 0), mgr.entries_since_snapshot);

    // Verify last_snapshot metadata.
    const meta = mgr.last_snapshot.?;
    try testing.expectEqual(@as(u64, 3), meta.last_included_term);
    try testing.expectEqual(@as(u64, 42), meta.last_included_index);
}

test "Snapshot: loadLatestSnapshot reads back correctly" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    const state = "test state data for roundtrip";
    try mgr.takeSnapshot(5, 100, state);

    var snap = (try mgr.loadLatestSnapshot()).?;
    defer snap.deinit();

    try testing.expectEqual(@as(u64, 5), snap.last_included_term);
    try testing.expectEqual(@as(u64, 100), snap.last_included_index);
    try testing.expectEqualStrings(state, snap.data);
}

test "Snapshot: CRC32 validation on corrupt snapshot" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    try mgr.takeSnapshot(1, 10, "some state");

    // Corrupt a byte in the state data region.
    const path = TEST_DIR ++ "/snapshot_10.zbsp";
    {
        const f = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer f.close();
        // State data starts at HEADER_SIZE (26). Flip a byte.
        var byte: [1]u8 = undefined;
        _ = try f.preadAll(&byte, HEADER_SIZE);
        byte[0] ^= 0xFF;
        try f.seekTo(HEADER_SIZE);
        try f.writeAll(&byte);
    }

    const result = mgr.loadLatestSnapshot();
    try testing.expectError(error.CrcMismatch, result);
}

test "Snapshot: shouldSnapshot triggers after interval" {
    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = ".",
        .snapshot_interval = 5,
    });
    defer mgr.deinit();

    try testing.expect(!mgr.shouldSnapshot());

    mgr.onEntryCommitted();
    mgr.onEntryCommitted();
    mgr.onEntryCommitted();
    mgr.onEntryCommitted();
    try testing.expect(!mgr.shouldSnapshot());

    mgr.onEntryCommitted(); // 5th
    try testing.expect(mgr.shouldSnapshot());

    mgr.onEntryCommitted(); // 6th — still true
    try testing.expect(mgr.shouldSnapshot());
}

test "Snapshot: multiple snapshots — loadLatest returns newest" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    try mgr.takeSnapshot(1, 10, "state at 10");
    try mgr.takeSnapshot(2, 50, "state at 50");
    try mgr.takeSnapshot(3, 30, "state at 30"); // index 30 < 50

    // Should return snapshot with highest index (50).
    var snap = (try mgr.loadLatestSnapshot()).?;
    defer snap.deinit();

    try testing.expectEqual(@as(u64, 2), snap.last_included_term);
    try testing.expectEqual(@as(u64, 50), snap.last_included_index);
    try testing.expectEqualStrings("state at 50", snap.data);
}

test "Snapshot: cleanOldSnapshots keeps only N newest" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    try mgr.takeSnapshot(1, 10, "s10");
    try mgr.takeSnapshot(2, 20, "s20");
    try mgr.takeSnapshot(3, 30, "s30");
    try mgr.takeSnapshot(4, 40, "s40");

    // Keep only 2 newest (30 and 40).
    try mgr.cleanOldSnapshots(2);

    // Verify that snapshot_10 and snapshot_20 are gone.
    const r10 = std.fs.cwd().openFile(TEST_DIR ++ "/snapshot_10.zbsp", .{});
    try testing.expectError(error.FileNotFound, r10);

    const r20 = std.fs.cwd().openFile(TEST_DIR ++ "/snapshot_20.zbsp", .{});
    try testing.expectError(error.FileNotFound, r20);

    // Verify that snapshot_30 and snapshot_40 still exist.
    {
        const f30 = try std.fs.cwd().openFile(TEST_DIR ++ "/snapshot_30.zbsp", .{});
        f30.close();
    }
    {
        const f40 = try std.fs.cwd().openFile(TEST_DIR ++ "/snapshot_40.zbsp", .{});
        f40.close();
    }
}

test "Snapshot: empty directory — loadLatest returns null" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    const result = try mgr.loadLatestSnapshot();
    try testing.expect(result == null);
}

test "Snapshot: corrupt newest falls back to older valid snapshot" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try tmp.dir.realpath(".", &path_buf);

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = base,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    try mgr.takeSnapshot(1, 10, "state at 10");
    try mgr.takeSnapshot(2, 20, "state at 20");

    // Simulate a torn/bit-rotted newest snapshot: flip a CRC byte on disk.
    {
        const f = try tmp.dir.openFile("snapshot_20.zbsp", .{ .mode = .read_write });
        defer f.close();
        const size = try f.getEndPos();
        var byte: [1]u8 = undefined;
        _ = try f.preadAll(&byte, size - 1);
        byte[0] ^= 0xFF;
        try f.seekTo(size - 1);
        try f.writeAll(&byte);
    }

    // Recovery must fall back to the older VALID snapshot, not error out.
    var snap = (try mgr.loadLatestSnapshot()).?;
    defer snap.deinit();
    try testing.expectEqual(@as(u64, 1), snap.last_included_term);
    try testing.expectEqual(@as(u64, 10), snap.last_included_index);
    try testing.expectEqualStrings("state at 10", snap.data);
}

test "Snapshot: stale .tmp from torn write is ignored by load and removed by cleanup" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try tmp.dir.realpath(".", &path_buf);

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = base,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    try mgr.takeSnapshot(1, 10, "good state");

    // Simulate a crash mid-takeSnapshot: only a partial temp file for index 99.
    try tmp.dir.writeFile(.{ .sub_path = "snapshot_99.zbsp.tmp", .data = "torn partial write" });

    // Load is unaffected by the temp file.
    var snap = (try mgr.loadLatestSnapshot()).?;
    defer snap.deinit();
    try testing.expectEqual(@as(u64, 10), snap.last_included_index);
    try testing.expectEqualStrings("good state", snap.data);

    // Cleanup removes the stale temp and keeps the valid snapshot.
    try mgr.cleanOldSnapshots(1);
    try testing.expectError(error.FileNotFound, tmp.dir.openFile("snapshot_99.zbsp.tmp", .{}));
    const f = try tmp.dir.openFile("snapshot_10.zbsp", .{});
    f.close();
}

test "Snapshot: cleanOldSnapshots keeps newest VALID, deletes the corrupt one" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try tmp.dir.realpath(".", &path_buf);

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = base,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    try mgr.takeSnapshot(1, 10, "good old");
    try mgr.takeSnapshot(2, 20, "torn new");

    // Corrupt the newest snapshot on disk.
    {
        const f = try tmp.dir.openFile("snapshot_20.zbsp", .{ .mode = .read_write });
        defer f.close();
        const size = try f.getEndPos();
        var byte: [1]u8 = undefined;
        _ = try f.preadAll(&byte, size - 1);
        byte[0] ^= 0xFF;
        try f.seekTo(size - 1);
        try f.writeAll(&byte);
    }

    // keep_count=1 must keep snapshot_10 (newest VALID), not snapshot_20 (corrupt).
    try mgr.cleanOldSnapshots(1);
    try testing.expectError(error.FileNotFound, tmp.dir.openFile("snapshot_20.zbsp", .{}));
    {
        const f = try tmp.dir.openFile("snapshot_10.zbsp", .{});
        f.close();
    }

    var snap = (try mgr.loadLatestSnapshot()).?;
    defer snap.deinit();
    try testing.expectEqualStrings("good old", snap.data);
}

test "Snapshot: corrupt state_size is rejected without huge allocation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const base = try tmp.dir.realpath(".", &path_buf);

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = base,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    // Craft a snapshot whose header claims ~4GB of state but holds 4 bytes.
    const term: u64 = 1;
    const index: u64 = 5;
    const huge_size: u32 = 0xFFFF_FFF0;
    var file_data: [HEADER_SIZE + 4]u8 = undefined;
    @memcpy(file_data[0..4], std.mem.asBytes(&SNAPSHOT_MAGIC));
    @memcpy(file_data[4..6], std.mem.asBytes(&SNAPSHOT_VERSION));
    @memcpy(file_data[6..14], std.mem.asBytes(&term));
    @memcpy(file_data[14..22], std.mem.asBytes(&index));
    @memcpy(file_data[22..26], std.mem.asBytes(&huge_size));
    @memset(file_data[HEADER_SIZE..], 0xAB);
    try tmp.dir.writeFile(.{ .sub_path = "snapshot_5.zbsp", .data = &file_data });

    // The size cap rejects it before any allocation can happen; since it is
    // the only candidate, the validation error surfaces to the caller.
    try testing.expectError(error.InvalidSnapshot, mgr.loadLatestSnapshot());
}

test "Snapshot: init rejects overlong base path" {
    const long_path = "x" ** 300;
    try testing.expectError(error.PathTooLong, SnapshotManager.init(testing.allocator, .{
        .base_path = long_path,
    }));
}

test "Snapshot: large state data (64KB)" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = try SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    // 64KB payload.
    const big_data = try testing.allocator.alloc(u8, 65536);
    defer testing.allocator.free(big_data);
    for (big_data, 0..) |*b, i| {
        b.* = @intCast(i % 256);
    }

    try mgr.takeSnapshot(7, 999, big_data);

    var snap = (try mgr.loadLatestSnapshot()).?;
    defer snap.deinit();

    try testing.expectEqual(@as(u64, 7), snap.last_included_term);
    try testing.expectEqual(@as(u64, 999), snap.last_included_index);
    try testing.expectEqual(@as(usize, 65536), snap.data.len);
    try testing.expectEqualSlices(u8, big_data, snap.data);
}

test "SnapshotManager getLatestMeta and missing-directory handling" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    var dir_buf: [512]u8 = undefined;
    const snap_dir = try std.fmt.bufPrint(&dir_buf, "{s}/snaps", .{base});
    try std.fs.cwd().makePath(snap_dir);

    var mgr = try SnapshotManager.init(std.testing.allocator, .{ .base_path = snap_dir });
    defer mgr.deinit();
    try std.testing.expect(mgr.getLatestMeta() == null);

    // With the directory deleted, load returns null and cleanup is a no-op.
    try std.fs.cwd().deleteTree(snap_dir);
    try std.testing.expect((try mgr.loadLatestSnapshot()) == null);
    try mgr.cleanOldSnapshots(2);

    // Restore the directory: a snapshot publishes metadata.
    try std.fs.cwd().makePath(snap_dir);
    try mgr.takeSnapshot(4, 9, "meta-state");
    const meta = mgr.getLatestMeta().?;
    try std.testing.expectEqual(@as(u64, 4), meta.last_included_term);
    try std.testing.expectEqual(@as(u64, 9), meta.last_included_index);
}

test "takeSnapshot removes the temp file when the rename fails" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    // Occupy the final snapshot name with a DIRECTORY.
    try tmp.dir.makeDir("snapshot_7.zbsp");

    var mgr = try SnapshotManager.init(std.testing.allocator, .{ .base_path = base });
    defer mgr.deinit();
    try std.testing.expect(std.meta.isError(mgr.takeSnapshot(1, 7, "doomed")));

    // The errdefer removed the temp file.
    try std.testing.expectError(error.FileNotFound, tmp.dir.openFile("snapshot_7.zbsp.tmp", .{}));
}

test "validateSnapshotFile rejects files shorter than header+crc" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    {
        const f = try tmp.dir.createFile("snapshot_1.zbsp", .{});
        defer f.close();
        try f.writeAll(&[_]u8{ 1, 2, 3 });
    }
    try std.testing.expect(!SnapshotManager.validateSnapshotFile(tmp.dir, "snapshot_1.zbsp"));
}
