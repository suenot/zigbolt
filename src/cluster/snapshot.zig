const std = @import("std");

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

    pub fn init(allocator: std.mem.Allocator, config: SnapshotConfig) SnapshotManager {
        var path_buf: [256]u8 = undefined;
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

    /// Write a snapshot to disk.
    /// File name: snapshot_{last_index}.zbsp
    pub fn takeSnapshot(self: *SnapshotManager, last_term: u64, last_index: u64, state_data: []const u8) !void {
        // Format file name.
        var name_buf: [128]u8 = undefined;
        const name = std.fmt.bufPrint(&name_buf, "snapshot_{d}.zbsp", .{last_index}) catch return error.NameTooLong;

        // Build full path.
        var full_path_buf: [512]u8 = undefined;
        const base = self.base_path[0..self.base_path_len];
        const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{ base, name }) catch return error.NameTooLong;

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

        // Write the file.
        const file = try std.fs.cwd().createFile(full_path, .{});
        defer file.close();

        try file.writeAll(&header);
        if (state_data.len > 0) {
            try file.writeAll(state_data);
        }
        try file.writeAll(std.mem.asBytes(&crc));

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

    /// Load the latest snapshot from disk. Returns null if no snapshots exist.
    /// Caller owns the returned SnapshotData and must call deinit().
    pub fn loadLatestSnapshot(self: *SnapshotManager) !?SnapshotData {
        const base = self.base_path[0..self.base_path_len];

        // Scan directory for snapshot files.
        var dir = std.fs.cwd().openDir(base, .{ .iterate = true }) catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };
        defer dir.close();

        var best_index: u64 = 0;
        var best_name: [128]u8 = undefined;
        var best_name_len: usize = 0;

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (entry.kind != .file) continue;
            const idx = parseSnapshotIndex(entry.name) orelse continue;
            if (idx > best_index) {
                best_index = idx;
                best_name_len = entry.name.len;
                @memcpy(best_name[0..entry.name.len], entry.name);
            }
        }

        if (best_index == 0) return null;

        // Build full path.
        var full_path_buf: [512]u8 = undefined;
        const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{ base, best_name[0..best_name_len] }) catch return error.NameTooLong;

        return try loadSnapshotFile(self.allocator, full_path);
    }

    /// Return metadata of the latest snapshot without loading state data.
    pub fn getLatestMeta(self: *const SnapshotManager) ?SnapshotMeta {
        return self.last_snapshot;
    }

    /// Delete all but the N newest snapshot files.
    pub fn cleanOldSnapshots(self: *SnapshotManager, keep_count: usize) !void {
        const base = self.base_path[0..self.base_path_len];

        var dir = std.fs.cwd().openDir(base, .{ .iterate = true }) catch |err| {
            if (err == error.FileNotFound) return;
            return err;
        };
        defer dir.close();

        // Collect snapshot indices and names.
        const Entry = struct {
            index: u64,
            name: [128]u8,
            name_len: usize,
        };

        var entries = std.ArrayListUnmanaged(Entry){};
        defer entries.deinit(self.allocator);

        var iter = dir.iterate();
        while (try iter.next()) |de| {
            if (de.kind != .file) continue;
            const idx = parseSnapshotIndex(de.name) orelse continue;
            var e: Entry = .{ .index = idx, .name = undefined, .name_len = de.name.len };
            @memcpy(e.name[0..de.name.len], de.name);
            try entries.append(self.allocator, e);
        }

        if (entries.items.len <= keep_count) return;

        // Sort by index ascending.
        std.mem.sort(Entry, entries.items, {}, struct {
            fn lessThan(_: void, a: Entry, b: Entry) bool {
                return a.index < b.index;
            }
        }.lessThan);

        // Delete all but the last keep_count.
        const to_delete = entries.items.len - keep_count;
        for (entries.items[0..to_delete]) |e| {
            dir.deleteFile(e.name[0..e.name_len]) catch {};
        }
    }

    // ── Internal helpers ─────────────────────────────────────

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

    var mgr = SnapshotManager.init(testing.allocator, .{
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

    var mgr = SnapshotManager.init(testing.allocator, .{
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

    var mgr = SnapshotManager.init(testing.allocator, .{
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
    var mgr = SnapshotManager.init(testing.allocator, .{
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

    var mgr = SnapshotManager.init(testing.allocator, .{
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

    var mgr = SnapshotManager.init(testing.allocator, .{
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

    var mgr = SnapshotManager.init(testing.allocator, .{
        .base_path = TEST_DIR,
        .snapshot_interval = 100,
    });
    defer mgr.deinit();

    const result = try mgr.loadLatestSnapshot();
    try testing.expect(result == null);
}

test "Snapshot: large state data (64KB)" {
    cleanTestDir();
    try ensureTestDir();
    defer cleanTestDir();

    var mgr = SnapshotManager.init(testing.allocator, .{
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
