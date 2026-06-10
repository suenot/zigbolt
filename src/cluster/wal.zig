const std = @import("std");
const builtin = @import("builtin");

/// Fsync the directory containing `file_path` so renames/unlinks of entries
/// in it are durable. No-op on Windows where directory handles cannot be
/// flushed; on macOS plain fsync is used (F_FULLFSYNC would be stronger but
/// plain fsync is the portable baseline).
fn syncParentDir(file_path: []const u8) !void {
    if (builtin.os.tag == .windows) return;
    const dir_path = std.fs.path.dirname(file_path) orelse ".";
    // `.iterate = true` so Linux gives a real fd, not O_PATH — fsync on an
    // O_PATH fd hits unreachable (EBADF).
    var dir = try std.fs.cwd().openDir(dir_path, .{ .iterate = true });
    defer dir.close();
    try std.posix.fsync(dir.fd);
}

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

    /// Durability contract: an entry is only durable after `file.sync()` has
    /// run past it. A Raft node MUST NOT acknowledge (reply to AppendEntries /
    /// grant a vote referencing) an entry until it is durable — use
    /// `.every_entry`, or batch appends and call `sync()` before replying.
    pub const SyncPolicy = enum {
        /// fsync after every entry (safest, slowest). Use this — or an
        /// explicit `sync()` before acking — for the Raft consensus path.
        every_entry,
        /// fsync after N entries. NOT sufficient alone for Raft acks: entries
        /// between syncs sit in the page cache and are lost on power failure.
        every_n_entries,
        /// fsync only on explicit `sync()`/`flush()`. The caller owns the
        /// persist-before-reply ordering.
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
        if (config.path.len > path_buf.len) {
            return error.PathTooLong;
        }
        const len: u16 = @intCast(config.path.len);
        @memcpy(path_buf[0..len], config.path);

        // Try opening existing file first, create if it doesn't exist.
        const file = std.fs.cwd().createFile(config.path, .{
            .read = true,
            .truncate = false,
        }) catch |err| return err;
        errdefer file.close(); // kcov-skip: fires only if getEndPos fails on a freshly created file — not injectable in-process

        // Seek to end to get write position. Surface the error: silently
        // assuming position 0 would overwrite existing records.
        const end_pos = try file.getEndPos();

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
        const record_length: u32 = @intCast(8 + 8 + payload_len + 4); // kcov-skip: runs on every append (WAL tests throughout); no own line record

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
            .every_entry => {
                try self.file.sync();
                self.entries_since_sync = 0;
            },
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
            // Truncate the file and make the truncation durable immediately:
            // a crash after an un-fsynced ftruncate would resurrect the
            // conflicting (truncated) entries on recovery.
            try self.file.setEndPos(offset);
            try self.file.sync();
            try syncParentDir(self.path[0..self.path_len]);
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
        // Any error past this point (mid-loop allocation, tail truncation,
        // toOwnedSlice) must release every payload already collected — the
        // per-iteration errdefer below only covers the record being read.
        errdefer {
            for (entries.items) |e| self.allocator.free(e.data);
            entries.deinit(self.allocator);
        }
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
                    self.allocator.free(data); // kcov-skip: defensive: record extent is pre-validated against file size; a short read needs concurrent truncation
                    break;
                }
            }

            // Read and verify CRC.
            var crc_buf: [4]u8 = undefined;
            const crc_read = try self.file.preadAll(&crc_buf, pos + HEADER_SIZE + payload_len);
            if (crc_read < 4) {
                self.allocator.free(data); // kcov-skip: defensive: record extent is pre-validated against file size; a short read needs concurrent truncation
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
        if (pos < file_size) {
            try self.file.setEndPos(pos);
            // Make the corrupt-tail truncation durable.
            try self.file.sync();
        }

        // On failure the errdefer above frees the collected entries; on
        // success toOwnedSlice empties the list and hands ownership over.
        return entries.toOwnedSlice(self.allocator);
    }

    /// Force an fsync to disk. A Raft caller MUST invoke this (or use
    /// `.every_entry`) before acknowledging appended entries to a leader —
    /// persist-before-reply is required for Raft safety.
    pub fn sync(self: *WriteAheadLog) !void {
        try self.file.sync();
        self.entries_since_sync = 0;
    }

    /// Alias of `sync()` kept for existing callers.
    pub fn flush(self: *WriteAheadLog) !void {
        try self.sync();
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

        // Cap the untrusted record length against the actual file size BEFORE
        // allocating, so a corrupt header cannot trigger a multi-GB alloc.
        const file_size = try self.file.getEndPos();
        if (file_offset + HEADER_SIZE + payload_len + CRC_SIZE > file_size) {
            return error.InvalidRecord;
        }

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
            // The errdefer above frees `data` on this error return; freeing
            // here as well was a double free.
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
/// Stored as a fixed-size 16-byte file:
///   [u64 current_term][u32 voted_for][u32 crc32 over the first 12 bytes]
///
/// Durability matters here more than anywhere else: losing a granted vote and
/// restarting as "never voted" lets a node vote twice in the same term, which
/// can elect two leaders. save() is therefore atomic (temp + fsync + rename +
/// dir fsync) and load() treats a short/corrupt file as a FATAL error — the
/// caller must refuse to start rather than risk a double vote.
pub const VoteState = struct {
    current_term: u64,
    voted_for: ?u32,

    pub const FILE_SIZE: usize = 16;

    /// Save vote state to a file atomically and durably:
    /// write `path.tmp`, fsync it, rename over `path`, fsync the directory.
    /// A crash at any point leaves either the old vote record or the new one —
    /// never a torn or missing file.
    pub fn save(self: VoteState, path: []const u8) !void {
        var buf: [FILE_SIZE]u8 = undefined;
        @memcpy(buf[0..8], std.mem.asBytes(&self.current_term));
        const vf: u32 = self.voted_for orelse 0xFFFFFFFF;
        @memcpy(buf[8..12], std.mem.asBytes(&vf));
        const crc = std.hash.Crc32.hash(buf[0..12]);
        @memcpy(buf[12..16], std.mem.asBytes(&crc));

        var tmp_path_buf: [512]u8 = undefined;
        const tmp_path = std.fmt.bufPrint(&tmp_path_buf, "{s}.tmp", .{path}) catch return error.PathTooLong;

        // On any failure below, remove the temp file so it cannot linger.
        errdefer std.fs.cwd().deleteFile(tmp_path) catch {};

        // 1. Write the temp file and fsync it before rename.
        {
            const file = try std.fs.cwd().createFile(tmp_path, .{});
            defer file.close();
            try file.writeAll(&buf);
            try file.sync();
        }

        // 2. Atomically replace the previous vote record.
        try std.fs.cwd().rename(tmp_path, path);

        // 3. Make the rename itself durable.
        try syncParentDir(path);
    }

    /// Load vote state from a file.
    /// Returns null ONLY if the file does not exist (genuinely never voted).
    /// A short or CRC-invalid file returns error.CorruptVoteState: the caller
    /// MUST treat this as fatal and refuse to start, otherwise a node that
    /// already granted a vote could vote again in the same term.
    pub fn load(path: []const u8) !?VoteState {
        const file = std.fs.cwd().openFile(path, .{}) catch |err| {
            if (err == error.FileNotFound) return null;
            return err;
        };
        defer file.close();

        var buf: [FILE_SIZE]u8 = undefined;
        const n = try file.readAll(&buf);
        if (n < FILE_SIZE) return error.CorruptVoteState;

        const stored_crc = std.mem.bytesToValue(u32, buf[12..16]);
        const computed_crc = std.hash.Crc32.hash(buf[0..12]);
        if (stored_crc != computed_crc) return error.CorruptVoteState;

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

test "WAL: VoteState truncated or corrupt file is an error, not 'no vote'" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try tmp.dir.realpath(".", &dir_buf);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/vote.state", .{dir_path});

    const vs = VoteState{ .current_term = 7, .voted_for = 3 };
    try vs.save(path);

    // Atomic save leaves no temp file behind.
    try testing.expectError(error.FileNotFound, tmp.dir.openFile("vote.state.tmp", .{}));

    // Round-trip is intact (CRC verifies).
    const loaded = (try VoteState.load(path)).?;
    try testing.expectEqual(@as(u64, 7), loaded.current_term);
    try testing.expectEqual(@as(u32, 3), loaded.voted_for.?);

    // A truncated vote file (torn write) must be a hard ERROR — silently
    // treating it as "never voted" would allow a double vote after restart.
    {
        const f = try tmp.dir.openFile("vote.state", .{ .mode = .read_write });
        defer f.close();
        try f.setEndPos(10);
    }
    try testing.expectError(error.CorruptVoteState, VoteState.load(path));

    // Restore, then corrupt a payload byte: the CRC must catch it.
    try vs.save(path);
    {
        const f = try tmp.dir.openFile("vote.state", .{ .mode = .read_write });
        defer f.close();
        var byte: [1]u8 = undefined;
        _ = try f.preadAll(&byte, 3);
        byte[0] ^= 0xFF;
        try f.seekTo(3);
        try f.writeAll(&byte);
    }
    try testing.expectError(error.CorruptVoteState, VoteState.load(path));
}

test "WAL: sync() makes appended bytes durable before ack" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try tmp.dir.realpath(".", &dir_buf);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/sync_ack.wal", .{dir_path});

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .explicit,
    });
    defer wal.deinit();

    // Raft persist-before-reply: append, then sync() BEFORE acking the leader.
    try wal.append(1, 1, "hello");
    try wal.sync();
    try testing.expectEqual(@as(u32, 0), wal.entries_since_sync);

    // The full record (20-byte header + 5 payload + 4 CRC) is in the file.
    const f = try tmp.dir.openFile("sync_ack.wal", .{});
    defer f.close();
    try testing.expectEqual(@as(u64, 29), try f.getEndPos());
}

test "WAL: truncateFrom is durable — truncated entries do not resurrect on recovery" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try tmp.dir.realpath(".", &dir_buf);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/trunc_durable.wal", .{dir_path});

    // Append 5 entries, then resolve a Raft conflict by truncating from 3.
    {
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
    }

    // Simulated restart: the conflicting entries 3..5 must NOT come back.
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
        try testing.expectEqualStrings("a", entries[0].data);
        try testing.expectEqualStrings("b", entries[1].data);
        try testing.expect((try wal.readEntry(3)) == null);
    }
}

test "WAL: corrupt record_length is rejected without huge allocation" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = try tmp.dir.realpath(".", &dir_buf);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/corrupt_len.wal", .{dir_path});

    var wal = try WriteAheadLog.init(testing.allocator, .{
        .path = path,
        .sync_policy = .every_entry,
    });
    defer wal.deinit();

    try wal.append(1, 1, "x");

    // Corrupt the length field of the record at offset 0 to claim ~4GB.
    {
        const f = try tmp.dir.openFile("corrupt_len.wal", .{ .mode = .read_write });
        defer f.close();
        const huge: u32 = 0xFFFF_FF00;
        try f.seekTo(0);
        try f.writeAll(std.mem.asBytes(&huge));
    }

    // The size cap rejects the record before any allocation can happen.
    try testing.expectError(error.InvalidRecord, wal.readEntry(1));
}

test "WAL: init rejects overlong path" {
    const long_path = "x" ** 300;
    try testing.expectError(error.PathTooLong, WriteAheadLog.init(testing.allocator, .{
        .path = long_path,
    }));
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

test "WAL every_n_entries sync policy batches fsyncs" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/batched.wal", .{base});

    var wal = try WriteAheadLog.init(std.testing.allocator, .{
        .path = path,
        .sync_policy = .every_n_entries,
        .sync_interval = 2,
    });
    defer wal.deinit();

    try wal.append(1, 1, "a");
    try std.testing.expectEqual(@as(u32, 1), wal.entries_since_sync);
    // Second append reaches the interval: fsync runs and the counter resets.
    try wal.append(1, 2, "b");
    try std.testing.expectEqual(@as(u32, 0), wal.entries_since_sync);
}

test "WAL truncateFrom the first entry empties the log" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/trunc.wal", .{base});

    var wal = try WriteAheadLog.init(std.testing.allocator, .{ .path = path, .sync_policy = .explicit });
    defer wal.deinit();

    try wal.append(1, 1, "a");
    try wal.append(1, 2, "b");
    try wal.truncateFrom(1);

    try std.testing.expectEqual(@as(u64, 0), wal.first_index);
    try std.testing.expectEqual(@as(u64, 0), wal.last_index);
    try std.testing.expect((try wal.readEntry(1)) == null);
    try std.testing.expectEqual(@as(u64, 0), wal.write_pos);
}

test "WAL readEntry detects payload corruption without a double free" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/crc.wal", .{base});

    var wal = try WriteAheadLog.init(std.testing.allocator, .{ .path = path, .sync_policy = .explicit });
    defer wal.deinit();

    try wal.append(3, 1, "payload-x");

    // Flip one payload byte on disk (payload starts after the 20-byte header).
    try wal.file.pwriteAll(&[_]u8{0xFF}, WriteAheadLog.HEADER_SIZE);

    // Before the fix this error path freed `data` twice (explicit free +
    // errdefer) — heap corruption the testing allocator catches.
    try std.testing.expectError(error.CrcMismatch, wal.readEntry(1));

    // The WAL stays usable.
    try wal.append(3, 2, "ok");
    const e = (try wal.readEntry(2)).?;
    defer std.testing.allocator.free(e.data);
    try std.testing.expectEqualStrings("ok", e.data);
}

test "WAL recover survives allocation failure at every point" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/oom.wal", .{base});

    {
        var wal = try WriteAheadLog.init(std.testing.allocator, .{ .path = path, .sync_policy = .explicit });
        defer wal.deinit();
        try wal.append(1, 1, "first");
        try wal.append(1, 2, "second");
        try wal.sync();
    }

    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: std.mem.Allocator, wal_path: []const u8) !void {
            var wal = try WriteAheadLog.init(allocator, .{ .path = wal_path, .sync_policy = .explicit });
            defer wal.deinit();
            const entries = try wal.recover();
            defer {
                for (entries) |e| allocator.free(e.data);
                allocator.free(entries);
            }
            if (entries.len != 2) return error.TestUnexpectedResult;
        }
    }.run, .{path});
}

test "VoteState.save removes the temp file when the rename fails" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const base = try tmp.dir.realpathAlloc(std.testing.allocator, ".");
    defer std.testing.allocator.free(base);

    // Occupy the final vote path with a DIRECTORY so the rename must fail.
    try tmp.dir.makeDir("vote.bin");
    var path_buf: [512]u8 = undefined;
    const vote_path = try std.fmt.bufPrint(&path_buf, "{s}/vote.bin", .{base});

    const vs = VoteState{ .current_term = 3, .voted_for = 1 };
    try std.testing.expect(std.meta.isError(vs.save(vote_path)));

    // The errdefer removed the temp file.
    try std.testing.expectError(error.FileNotFound, tmp.dir.openFile("vote.bin.tmp", .{}));
}
