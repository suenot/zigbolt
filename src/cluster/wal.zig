const std = @import("std");

/// Write-Ahead Log entry returned to callers.
pub const WalEntry = struct {
    term: u64,
    index: u64,
    data: []const u8,
};

/// Persistent Write-Ahead Log for Raft consensus.
///
/// Record format on disk:
///   [u32 record_length]  — bytes for term + index + payload + crc (excludes this field)
///   [u64 term]
///   [u64 index]
///   [payload bytes]
///   [u32 crc32]          — CRC32 over term + index + payload
///
/// Total per-entry overhead: 4 + 8 + 8 + 4 = 24 bytes.
pub const WriteAheadLog = struct {
    /// File handle for the WAL.
    file: std.fs.File,
    /// Current write position in the file.
    write_pos: u64,
    /// Sync policy.
    sync_policy: SyncPolicy,
    /// How many entries between automatic syncs (for `every_n_entries`).
    sync_interval: u32,
    /// Entries written since last sync.
    entries_since_sync: u32,
    /// Path for the WAL file.
    path: [256]u8,
    path_len: u16,
    /// In-memory index: maps log_index -> file_offset for fast lookup.
    index_map: std.ArrayListUnmanaged(IndexEntry),
    allocator: std.mem.Allocator,
    /// First log index in this WAL.
    first_index: u64,
    /// Last log index written.
    last_index: u64,

    pub const IndexEntry = struct {
        log_index: u64,
        file_offset: u64,
        term: u64,
    };

    pub const SyncPolicy = enum {
        /// fsync after every entry (safest, slowest).
        every_entry,
        /// fsync after N entries.
        every_n_entries,
        /// fsync only on explicit flush.
        explicit,
    };

    pub const WalConfig = struct {
        path: []const u8 = "zigbolt_raft.wal",
        sync_policy: SyncPolicy = .every_n_entries,
        sync_interval: u32 = 100,
    };

    /// Header size: record_length(4) + term(8) + index(8) = 20 bytes.
    const HEADER_SIZE: usize = 4 + 8 + 8;
    /// CRC trailer size.
    const CRC_SIZE: usize = 4;

    /// Create or open a WAL file. Does NOT run recovery; call `recover()` after init.
    pub fn init(allocator: std.mem.Allocator, config: WalConfig) !WriteAheadLog {
        var path_buf: [256]u8 = undefined;
        const len: u16 = @intCast(config.path.len);
        @memcpy(path_buf[0..len], config.path);

        // Try opening existing file first, create if it doesn't exist.
        const file = std.fs.cwd().createFile(config.path, .{
            .read = true,
            .truncate = false,
        }) catch |err| return err;

        // Seek to end to get write position.
        const end_pos = file.getEndPos() catch 0;

        return .{
            .file = file,
            .write_pos = end_pos,
            .sync_policy = config.sync_policy,
            .sync_interval = config.sync_interval,
            .entries_since_sync = 0,
            .path = path_buf,
            .path_len = len,
            .index_map = .empty,
            .allocator = allocator,
            .first_index = 0,
            .last_index = 0,
        };
    }

    /// Close the WAL file and free the in-memory index.
    pub fn deinit(self: *WriteAheadLog) void {
        self.file.sync() catch {};
        self.file.close();
        self.index_map.deinit(self.allocator);
    }

    /// Append an entry to the WAL on disk and update the in-memory index.
    pub fn append(self: *WriteAheadLog, term: u64, index: u64, data: []const u8) !void {
        // record_length = term(8) + index(8) + payload + crc(4)
        const payload_len = data.len;
        const record_length: u32 = @intCast(8 + 8 + payload_len + 4);

        // Compute CRC over term + index + payload.
        var hasher = std.hash.Crc32.init();
        hasher.update(std.mem.asBytes(&term));
        hasher.update(std.mem.asBytes(&index));
        hasher.update(data);
        const crc = hasher.final();

        // Build header buffer.
        var header: [HEADER_SIZE]u8 = undefined;
        @memcpy(header[0..4], std.mem.asBytes(&record_length));
        @memcpy(header[4..12], std.mem.asBytes(&term));
        @memcpy(header[12..20], std.mem.asBytes(&index));

        const crc_bytes = std.mem.asBytes(&crc);

        // Write at current position.
        const file_offset = self.write_pos;
        try self.file.seekTo(self.write_pos);
        try self.file.writeAll(&header);
        if (payload_len > 0) {
            try self.file.writeAll(data);
        }
        try self.file.writeAll(crc_bytes);

        self.write_pos += HEADER_SIZE + payload_len + CRC_SIZE;

        // Update in-memory index.
        try self.index_map.append(self.allocator, .{
            .log_index = index,
            .file_offset = file_offset,
            .term = term,
        });

        if (self.first_index == 0) {
            self.first_index = index;
        }
        self.last_index = index;

        // Apply sync policy.
        self.entries_since_sync += 1;
        switch (self.sync_policy) {
            .every_entry => try self.file.sync(),
            .every_n_entries => {
                if (self.entries_since_sync >= self.sync_interval) {
                    try self.file.sync();
                    self.entries_since_sync = 0;
                }
            },
            .explicit => {},
        }
    }

    /// Read an entry by its 1-based log index using the in-memory index.
    pub fn readEntry(self: *WriteAheadLog, log_index: u64) !?WalEntry {
        // Find offset in index_map.
        const ie = self.findIndexEntry(log_index) orelse return null;

        return try self.readEntryAt(ie.file_offset);
    }

    /// Truncate the WAL at the given index (inclusive) for Raft conflict resolution.
    /// Removes all entries with index >= from_index.
    pub fn truncateFrom(self: *WriteAheadLog, from_index: u64) !void {
        if (from_index == 0) return;

        // Find the file offset of the first entry to remove.
        var truncate_offset: ?u64 = null;
        var keep_count: usize = 0;
        for (self.index_map.items, 0..) |ie, i| {
            if (ie.log_index >= from_index) {
                truncate_offset = ie.file_offset;
                keep_count = i;
                break;
            }
        }

        if (truncate_offset) |offset| {
            // Truncate the file.
            try self.file.setEndPos(offset);
            self.write_pos = offset;

            // Shrink the in-memory index.
            self.index_map.shrinkRetainingCapacity(keep_count);

            // Update first/last index.
            if (self.index_map.items.len == 0) {
                self.first_index = 0;
                self.last_index = 0;
            } else {
                self.first_index = self.index_map.items[0].log_index;
                self.last_index = self.index_map.items[self.index_map.items.len - 1].log_index;
            }
        }
    }

    /// Scan the entire WAL file and rebuild the in-memory index.
    /// Returns all valid entries. Used at startup for crash recovery.
    /// Caller owns the returned slice and each entry's data; free with allocator.
    pub fn recover(self: *WriteAheadLog) ![]WalEntry {
        // Reset state.
        self.index_map.shrinkRetainingCapacity(0);
        self.first_index = 0;
        self.last_index = 0;

        const file_size = try self.file.getEndPos();
        if (file_size == 0) {
            self.write_pos = 0;
            return &.{};
        }

        var entries = std.ArrayListUnmanaged(WalEntry){};
        var pos: u64 = 0;

        while (pos + HEADER_SIZE + CRC_SIZE <= file_size) {
            // Read header.
            var header: [HEADER_SIZE]u8 = undefined;
            const header_read = try self.file.preadAll(&header, pos);
            if (header_read < HEADER_SIZE) break;

            const record_length = std.mem.bytesToValue(u32, header[0..4]);
            const term = std.mem.bytesToValue(u64, header[4..12]);
            const index = std.mem.bytesToValue(u64, header[12..20]);

            // Payload length = record_length - 8(term) - 8(index) - 4(crc)
            if (record_length < 20) break; // invalid
            const payload_len: usize = @intCast(record_length - 20);

            // Check we have enough data.
            const total_record_size = HEADER_SIZE + payload_len + CRC_SIZE;
            if (pos + total_record_size > file_size) break;

            // Read payload.
            const data = try self.allocator.alloc(u8, payload_len);
            errdefer self.allocator.free(data);

            if (payload_len > 0) {
                const data_read = try self.file.preadAll(data, pos + HEADER_SIZE);
                if (data_read < payload_len) {
                    self.allocator.free(data);
                    break;
                }
            }

            // Read and verify CRC.
            var crc_buf: [4]u8 = undefined;
            const crc_read = try self.file.preadAll(&crc_buf, pos + HEADER_SIZE + payload_len);
            if (crc_read < 4) {
                self.allocator.free(data);
                break;
            }
            const stored_crc = std.mem.bytesToValue(u32, &crc_buf);

            var hasher = std.hash.Crc32.init();
            hasher.update(std.mem.asBytes(&term));
            hasher.update(std.mem.asBytes(&index));
            hasher.update(data);
            const computed_crc = hasher.final();

            if (stored_crc != computed_crc) {
                // CRC mismatch — stop recovery here, truncate corrupt tail.
                self.allocator.free(data);
                break;
            }

            // Valid entry.
            try self.index_map.append(self.allocator, .{
                .log_index = index,
                .file_offset = pos,
                .term = term,
            });

            try entries.append(self.allocator, .{
                .term = term,
                .index = index,
                .data = data,
            });

            if (self.first_index == 0) {
                self.first_index = index;
            }
            self.last_index = index;

            pos += total_record_size;
        }

        // Set write position to end of last valid record (truncate any partial data).
        self.write_pos = pos;
        try self.file.setEndPos(pos);

        return entries.toOwnedSlice(self.allocator) catch |err| {
            // On allocation failure, free everything.
            for (entries.items) |e| {
                self.allocator.free(e.data);
            }
            entries.deinit(self.allocator);
            return err;
        };
    }

    /// Force an fsync to disk.
    pub fn flush(self: *WriteAheadLog) !void {
        try self.file.sync();
        self.entries_since_sync = 0;
    }

    /// Return the last written log index, or 0 if empty.
    pub fn lastIndex(self: *const WriteAheadLog) u64 {
        return self.last_index;
    }

    /// Return the term of the last entry, or 0 if empty.
    pub fn lastTerm(self: *const WriteAheadLog) u64 {
        if (self.index_map.items.len == 0) return 0;
        return self.index_map.items[self.index_map.items.len - 1].term;
    }

    /// Return the number of entries in the WAL.
    pub fn entryCount(self: *const WriteAheadLog) u64 {
        return @intCast(self.index_map.items.len);
    }

    // ── Internal helpers ─────────────────────────────────────

    fn findIndexEntry(self: *const WriteAheadLog, log_index: u64) ?IndexEntry {
        for (self.index_map.items) |ie| {
            if (ie.log_index == log_index) return ie;
        }
        return null;
    }

    fn readEntryAt(self: *WriteAheadLog, file_offset: u64) !WalEntry {
        // Read header.
        var header: [HEADER_SIZE]u8 = undefined;
        const header_read = try self.file.preadAll(&header, file_offset);
        if (header_read < HEADER_SIZE) return error.UnexpectedEof;

        const record_length = std.mem.bytesToValue(u32, header[0..4]);
        const term = std.mem.bytesToValue(u64, header[4..12]);
        const index = std.mem.bytesToValue(u64, header[12..20]);

        if (record_length < 20) return error.InvalidRecord;
        const payload_len: usize = @intCast(record_length - 20);

        // Read payload.
        const data = try self.allocator.alloc(u8, payload_len);
        errdefer self.allocator.free(data);

        if (payload_len > 0) {
            const data_read = try self.file.preadAll(data, file_offset + HEADER_SIZE);
            if (data_read < payload_len) return error.UnexpectedEof;
        }

        // Read and verify CRC.
        var crc_buf: [4]u8 = undefined;
        const crc_read = try self.file.preadAll(&crc_buf, file_offset + HEADER_SIZE + payload_len);
        if (crc_read < 4) return error.UnexpectedEof;
        const stored_crc = std.mem.bytesToValue(u32, &crc_buf);

        var hasher = std.hash.Crc32.init();
        hasher.update(std.mem.asBytes(&term));
        hasher.update(std.mem.asBytes(&index));
        hasher.update(data);
        const computed_crc = hasher.final();

        if (stored_crc != computed_crc) {
            self.allocator.free(data);
            return error.CrcMismatch;
        }

        return .{
            .term = term,
            .index = index,
            .data = data,
        };
    }
};

/// Persistent Raft vote state (current_term + voted_for).
/// Stored as a fixed-size 16-byte file.
pub const VoteState = struct {
    current_term: u64,
    voted_for: ?u32,

    pub const FILE_SIZE: usize = 16;

    /// Save vote state to a file atomically.
    pub fn save(self: VoteState, path: []const u8) !void {
        var buf: [FILE_SIZE]u8 = undefined;
        @memcpy(buf[0..8], std.mem.asBytes(&self.current_term));
        const vf: u32 = self.voted_for orelse 0xFFFFFFFF;
        @memcpy(buf[8..12], std.mem.asBytes(&vf));
        // 4 bytes padding (zeroed).
        @memset(buf[12..16], 0);

        try std.fs.cwd().writeFile(.{
            .sub_path = path,
            .data = &buf,
        });
    }

    /// Load vote state from a file. Returns null if file does not exist.
    pub fn load(path: []const u8) !?VoteState {
        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };
        defer file.close();

        var buf: [FILE_SIZE]u8 = undefined;
        const n = try file.readAll(&buf);
        if (n < FILE_SIZE) return null;

        const current_term = std.mem.bytesToValue(u64, buf[0..8]);
        const vf_raw = std.mem.bytesToValue(u32, buf[8..12]);
        const voted_for: ?u32 = if (vf_raw == 0xFFFFFFFF) null else vf_raw;

        return .{
            .current_term = current_term,
            .voted_for = voted_for,
        };
    }
};

/// Compute CRC32 of a byte slice (convenience wrapper).
pub fn crc32(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

// =============================================================================
// Tests
// =============================================================================

const testing = std.testing;

fn testWalPath(comptime suffix: []const u8) []const u8 {
    return "/tmp/zigbolt_test_wal_" ++ suffix ++ ".wal";
}

fn cleanupFile(path: []const u8) void {
    std.fs.cwd().deleteFile(path) catch {};
}

fn freeWalEntries(allocator: std.mem.Allocator, entries: []WalEntry) void {
    for (entries) |e| {
        allocator.free(e.data);
    }
    allocator.free(entries);
}

test "WAL: create and append single entry" {
    const path = testWalPath("single");
    cleanupFile(path);
    defer cleanupFile(path);

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .every_entry,
    });
    defer wal.deinit();

    try wal.append(1, 1, "hello");

    try testing.expectEqual(@as(u64, 1), wal.lastIndex());
    try testing.expectEqual(@as(u64, 1), wal.lastTerm());
    try testing.expectEqual(@as(u64, 1), wal.entryCount());

    const entry = (try wal.readEntry(1)).?;
    defer testing.allocator.free(entry.data);
    try testing.expectEqual(@as(u64, 1), entry.term);
    try testing.expectEqual(@as(u64, 1), entry.index);
    try testing.expectEqualStrings("hello", entry.data);
}

test "WAL: append multiple entries and read back" {
    const path = testWalPath("multi");
    cleanupFile(path);
    defer cleanupFile(path);

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .every_entry,
    });
    defer wal.deinit();

    try wal.append(1, 1, "alpha");
    try wal.append(1, 2, "beta");
    try wal.append(2, 3, "gamma");

    try testing.expectEqual(@as(u64, 3), wal.lastIndex());
    try testing.expectEqual(@as(u64, 2), wal.lastTerm());
    try testing.expectEqual(@as(u64, 3), wal.entryCount());

    const e2 = (try wal.readEntry(2)).?;
    defer testing.allocator.free(e2.data);
    try testing.expectEqualStrings("beta", e2.data);

    const e3 = (try wal.readEntry(3)).?;
    defer testing.allocator.free(e3.data);
    try testing.expectEqual(@as(u64, 2), e3.term);
    try testing.expectEqualStrings("gamma", e3.data);

    // Non-existent entry.
    const none = try wal.readEntry(99);
    try testing.expect(none == null);
}

test "WAL: recovery — write, close, reopen, recover all entries" {
    const path = testWalPath("recover");
    cleanupFile(path);
    defer cleanupFile(path);

    // Write phase.
    {
        var wal = try WriteAheadLog.init(testing.allocator, .{
            .path = path,
            .sync_policy = .every_entry,
        });
        defer wal.deinit();

        try wal.append(1, 1, "first");
        try wal.append(1, 2, "second");
        try wal.append(2, 3, "third");
    }

    // Recovery phase.
    {
        var wal = try WriteAheadLog.init(testing.allocator, .{
            .path = path,
            .sync_policy = .every_entry,
        });
        defer wal.deinit();

        const entries = try wal.recover();
        defer freeWalEntries(testing.allocator, entries);

        try testing.expectEqual(@as(usize, 3), entries.len);
        try testing.expectEqualStrings("first", entries[0].data);
        try testing.expectEqualStrings("second", entries[1].data);
        try testing.expectEqualStrings("third", entries[2].data);

        try testing.expectEqual(@as(u64, 1), wal.first_index);
        try testing.expectEqual(@as(u64, 3), wal.lastIndex());
        try testing.expectEqual(@as(u64, 2), wal.lastTerm());
    }
}

test "WAL: truncation — append 5, truncate from 3" {
    const path = testWalPath("trunc");
    cleanupFile(path);
    defer cleanupFile(path);

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .every_entry,
    });
    defer wal.deinit();

    try wal.append(1, 1, "a");
    try wal.append(1, 2, "b");
    try wal.append(2, 3, "c");
    try wal.append(2, 4, "d");
    try wal.append(3, 5, "e");

    try wal.truncateFrom(3);

    try testing.expectEqual(@as(u64, 2), wal.lastIndex());
    try testing.expectEqual(@as(u64, 1), wal.lastTerm());
    try testing.expectEqual(@as(u64, 2), wal.entryCount());

    // Entry 3 should not exist.
    const none = try wal.readEntry(3);
    try testing.expect(none == null);

    // Entries 1-2 should still be valid.
    const e1 = (try wal.readEntry(1)).?;
    defer testing.allocator.free(e1.data);
    try testing.expectEqualStrings("a", e1.data);
}

test "WAL: CRC32 validation — corrupt a byte" {
    const path = testWalPath("crc");
    cleanupFile(path);
    defer cleanupFile(path);

    {
        var wal = try WriteAheadLog.init(testing.allocator, .{
            .path = path,
            .sync_policy = .every_entry,
        });
        defer wal.deinit();
        try wal.append(1, 1, "valid_data");
    }

    // Corrupt a payload byte in the file.
    {
        const f = try std.fs.cwd().openFile(path, .{ .mode = .read_write });
        defer f.close();
        // Payload starts at offset 20 (4+8+8). Flip a byte there.
        var byte: [1]u8 = undefined;
        _ = try f.preadAll(&byte, 20);
        byte[0] ^= 0xFF;
        try f.seekTo(20);
        try f.writeAll(&byte);
    }

    // Recovery should return 0 entries due to CRC failure.
    {
        var wal = try WriteAheadLog.init(testing.allocator, .{
            .path = path,
            .sync_policy = .every_entry,
        });
        defer wal.deinit();

        const entries = try wal.recover();
        defer freeWalEntries(testing.allocator, entries);

        try testing.expectEqual(@as(usize, 0), entries.len);
    }
}

test "WAL: VoteState save and load roundtrip" {
    const path = "/tmp/zigbolt_test_vote.state";
    cleanupFile(path);
    defer cleanupFile(path);

    const vs1 = VoteState{ .current_term = 42, .voted_for = 7 };
    try vs1.save(path);

    const loaded = (try VoteState.load(path)).?;
    try testing.expectEqual(@as(u64, 42), loaded.current_term);
    try testing.expectEqual(@as(u32, 7), loaded.voted_for.?);

    // Test with null voted_for.
    const vs2 = VoteState{ .current_term = 99, .voted_for = null };
    try vs2.save(path);

    const loaded2 = (try VoteState.load(path)).?;
    try testing.expectEqual(@as(u64, 99), loaded2.current_term);
    try testing.expect(loaded2.voted_for == null);

    // Test load of non-existent file.
    const missing = try VoteState.load("/tmp/zigbolt_no_such_file.state");
    try testing.expect(missing == null);
}

test "WAL: explicit sync policy" {
    const path = testWalPath("explicit");
    cleanupFile(path);
    defer cleanupFile(path);

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .explicit,
    });
    defer wal.deinit();

    try wal.append(1, 1, "no-sync");
    try wal.append(1, 2, "still-no-sync");

    // Explicit flush.
    try wal.flush();

    try testing.expectEqual(@as(u64, 2), wal.entryCount());
    try testing.expectEqual(@as(u32, 0), wal.entries_since_sync);
}

test "WAL: empty file recovery" {
    const path = testWalPath("empty");
    cleanupFile(path);
    defer cleanupFile(path);

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .every_entry,
    });
    defer wal.deinit();

    const entries = try wal.recover();
    defer freeWalEntries(testing.allocator, entries);

    try testing.expectEqual(@as(usize, 0), entries.len);
    try testing.expectEqual(@as(u64, 0), wal.lastIndex());
    try testing.expectEqual(@as(u64, 0), wal.lastTerm());
}

test "WAL: append after recovery (continue from last index)" {
    const path = testWalPath("cont");
    cleanupFile(path);
    defer cleanupFile(path);

    // Write initial entries.
    {
        var wal = try WriteAheadLog.init(testing.allocator, .{
            .path = path,
            .sync_policy = .every_entry,
        });
        defer wal.deinit();

        try wal.append(1, 1, "one");
        try wal.append(1, 2, "two");
    }

    // Recover and continue appending.
    {
        var wal = try WriteAheadLog.init(testing.allocator, .{
            .path = path,
            .sync_policy = .every_entry,
        });
        defer wal.deinit();

        const entries = try wal.recover();
        defer freeWalEntries(testing.allocator, entries);

        try testing.expectEqual(@as(usize, 2), entries.len);
        try testing.expectEqual(@as(u64, 2), wal.lastIndex());

        // Append new entry continuing from where we left off.
        try wal.append(2, 3, "three");
        try testing.expectEqual(@as(u64, 3), wal.lastIndex());
        try testing.expectEqual(@as(u64, 3), wal.entryCount());

        const e3 = (try wal.readEntry(3)).?;
        defer testing.allocator.free(e3.data);
        try testing.expectEqualStrings("three", e3.data);
    }
}

test "WAL: large payload entry" {
    const path = testWalPath("large");
    cleanupFile(path);
    defer cleanupFile(path);

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .every_entry,
    });
    defer wal.deinit();

    // 64KB payload.
    const big_data = try testing.allocator.alloc(u8, 65536);
    defer testing.allocator.free(big_data);
    for (big_data, 0..) |*b, i| {
        b.* = @intCast(i % 256);
    }

    try wal.append(5, 1, big_data);

    const entry = (try wal.readEntry(1)).?;
    defer testing.allocator.free(entry.data);
    try testing.expectEqual(@as(u64, 5), entry.term);
    try testing.expectEqual(@as(usize, 65536), entry.data.len);
    try testing.expectEqualSlices(u8, big_data, entry.data);
}
