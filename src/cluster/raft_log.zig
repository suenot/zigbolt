const std = @import("std");

pub const LogEntry = struct {
    term: u64,
    index: u64,
    data: []const u8,
};

pub const RaftLog = struct {
    entries: std.ArrayListUnmanaged(StoredEntry),
    allocator: std.mem.Allocator,

    pub const StoredEntry = struct {
        term: u64,
        index: u64,
        data: []u8, // owned copy
    };

    pub fn init(allocator: std.mem.Allocator) RaftLog {
        return .{
            .entries = .empty,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *RaftLog) void {
        for (self.entries.items) |entry| {
            self.allocator.free(entry.data);
        }
        self.entries.deinit(self.allocator);
    }

    /// Append a new entry to the log. Returns the assigned index (1-based).
    pub fn append(self: *RaftLog, term: u64, data: []const u8) !u64 {
        const index = self.lastIndex() + 1;
        const owned_data = try self.allocator.dupe(u8, data);
        errdefer self.allocator.free(owned_data);
        try self.entries.append(self.allocator, .{
            .term = term,
            .index = index,
            .data = owned_data,
        });
        return index;
    }

    /// Get entry by 1-based index. Returns null if out of range.
    pub fn getEntry(self: *const RaftLog, index: u64) ?LogEntry {
        if (index == 0 or index > self.entries.items.len) return null;
        const stored = self.entries.items[index - 1];
        return .{
            .term = stored.term,
            .index = stored.index,
            .data = stored.data,
        };
    }

    /// Return the index of the last entry, or 0 if the log is empty.
    pub fn lastIndex(self: *const RaftLog) u64 {
        if (self.entries.items.len == 0) return 0;
        return self.entries.items[self.entries.items.len - 1].index;
    }

    /// Return the term of the last entry, or 0 if the log is empty.
    pub fn lastTerm(self: *const RaftLog) u64 {
        if (self.entries.items.len == 0) return 0;
        return self.entries.items[self.entries.items.len - 1].term;
    }

    /// Truncate all entries from the given index onward (inclusive).
    /// Used for conflict resolution during AppendEntries.
    pub fn truncateFrom(self: *RaftLog, index: u64) void {
        if (index == 0 or index > self.entries.items.len) return;
        // Free data for truncated entries
        const start = index - 1;
        for (self.entries.items[start..]) |entry| {
            self.allocator.free(entry.data);
        }
        self.entries.shrinkRetainingCapacity(start);
    }

    /// Return a slice of stored entries starting from the given 1-based index.
    /// Returns an empty slice if the index is beyond the log.
    pub fn entriesFrom(self: *const RaftLog, index: u64) []const StoredEntry {
        if (index == 0 or index > self.entries.items.len) return &.{};
        const start = index - 1;
        return self.entries.items[start..];
    }
};

// =============================================================================
// Tests
// =============================================================================

test "RaftLog: init and deinit empty log" {
    var log = RaftLog.init(std.testing.allocator);
    defer log.deinit();

    try std.testing.expectEqual(@as(u64, 0), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 0), log.lastTerm());
    try std.testing.expect(log.getEntry(1) == null);
}

test "RaftLog: append and retrieve entries" {
    var log = RaftLog.init(std.testing.allocator);
    defer log.deinit();

    const idx1 = try log.append(1, "cmd1");
    const idx2 = try log.append(1, "cmd2");
    const idx3 = try log.append(2, "cmd3");

    try std.testing.expectEqual(@as(u64, 1), idx1);
    try std.testing.expectEqual(@as(u64, 2), idx2);
    try std.testing.expectEqual(@as(u64, 3), idx3);

    try std.testing.expectEqual(@as(u64, 3), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 2), log.lastTerm());

    const entry1 = log.getEntry(1).?;
    try std.testing.expectEqual(@as(u64, 1), entry1.term);
    try std.testing.expectEqual(@as(u64, 1), entry1.index);
    try std.testing.expectEqualStrings("cmd1", entry1.data);

    const entry3 = log.getEntry(3).?;
    try std.testing.expectEqual(@as(u64, 2), entry3.term);
    try std.testing.expectEqualStrings("cmd3", entry3.data);
}

test "RaftLog: getEntry out of bounds returns null" {
    var log = RaftLog.init(std.testing.allocator);
    defer log.deinit();

    try std.testing.expect(log.getEntry(0) == null);
    try std.testing.expect(log.getEntry(1) == null);

    _ = try log.append(1, "a");
    try std.testing.expect(log.getEntry(2) == null);
}

test "RaftLog: truncateFrom removes entries" {
    var log = RaftLog.init(std.testing.allocator);
    defer log.deinit();

    _ = try log.append(1, "a");
    _ = try log.append(1, "b");
    _ = try log.append(2, "c");
    _ = try log.append(2, "d");

    log.truncateFrom(3); // remove entries at index 3 and 4

    try std.testing.expectEqual(@as(u64, 2), log.lastIndex());
    try std.testing.expectEqual(@as(u64, 1), log.lastTerm());
    try std.testing.expect(log.getEntry(3) == null);
}

test "RaftLog: entriesFrom returns correct slice" {
    var log = RaftLog.init(std.testing.allocator);
    defer log.deinit();

    _ = try log.append(1, "a");
    _ = try log.append(1, "b");
    _ = try log.append(2, "c");

    const from2 = log.entriesFrom(2);
    try std.testing.expectEqual(@as(usize, 2), from2.len);
    try std.testing.expectEqualStrings("b", from2[0].data);
    try std.testing.expectEqualStrings("c", from2[1].data);

    const from_beyond = log.entriesFrom(10);
    try std.testing.expectEqual(@as(usize, 0), from_beyond.len);
}

test "RaftLog: truncateFrom with out-of-bounds index is no-op" {
    var log = RaftLog.init(std.testing.allocator);
    defer log.deinit();

    _ = try log.append(1, "x");
    log.truncateFrom(0);
    try std.testing.expectEqual(@as(u64, 1), log.lastIndex());

    log.truncateFrom(5);
    try std.testing.expectEqual(@as(u64, 1), log.lastIndex());
}
