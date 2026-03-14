const std = @import("std");

/// LZ4-style compression for ZigBolt's archive system.
///
/// Uses a simplified token format for speed:
///   Literal run:  [0x00][length: u16 LE][literal bytes...]
///   Match:        [0x01][offset: u16 LE][length: u16 LE]
///   End marker:   [0x02]

const TOKEN_LITERAL: u8 = 0x00;
const TOKEN_MATCH: u8 = 0x01;
const TOKEN_END: u8 = 0x02;

/// Minimum match length to emit a match token.
const MIN_MATCH: usize = 4;

/// Maximum backreference distance (64 KB window).
const MAX_OFFSET: usize = 65535;

/// Maximum match length encodable in u16.
const MAX_MATCH_LEN: usize = 65535;

/// Hash table size — must be a power of two.
const HASH_TABLE_BITS: comptime_int = 14;
const HASH_TABLE_SIZE: usize = 1 << HASH_TABLE_BITS;
const HASH_TABLE_MASK: u32 = HASH_TABLE_SIZE - 1;

/// Compressed frame header placed before compressed data.
pub const CompressedFrame = struct {
    magic: u32 = FRAME_MAGIC,
    original_size: u32,
    compressed_size: u32,
    checksum: u32,

    pub const HEADER_SIZE: usize = 16;
};

const FRAME_MAGIC: u32 = 0x5A424C5A; // "ZBLZ"

/// Hash 4 bytes at the given position.
fn hash4(data: []const u8, pos: usize) u32 {
    const v = std.mem.readInt(u32, data[pos..][0..4], .little);
    return (v *% 2654435761) >> (32 - HASH_TABLE_BITS);
}

// ── Compressor ──────────────────────────────────────────────────────────

pub const Compressor = struct {
    /// Hash table mapping 4-byte hashes to positions in the source.
    hash_table: []u32,

    pub fn init(allocator: std.mem.Allocator) !Compressor {
        const table = try allocator.alloc(u32, HASH_TABLE_SIZE);
        @memset(table, 0);
        return Compressor{ .hash_table = table };
    }

    pub fn deinit(self: *Compressor, allocator: std.mem.Allocator) void {
        allocator.free(self.hash_table);
        self.hash_table = &.{};
    }

    /// Worst-case compressed output size for a given input size.
    /// Each byte could become a 1-byte literal run (3 bytes overhead per literal run + 1 byte)
    /// plus the end marker. In practice we batch literals so this is very conservative.
    pub fn maxCompressedSize(input_size: usize) usize {
        // Worst case: all literals emitted one run at a time.
        // One literal run: 1 (token) + 2 (length) + N (data).
        // We emit at most one run for the whole input, so:
        //   header_overhead = 3 bytes per run; at most input_size / 65535 + 1 runs.
        const num_runs = input_size / 65535 + 1;
        return input_size + num_runs * 3 + 1; // +1 for end marker
    }

    /// Compress `src` into `dst`. Returns the number of bytes written.
    /// `dst` must be at least `maxCompressedSize(src.len)` bytes long.
    pub fn compress(self: *Compressor, src: []const u8, dst: []u8) !usize {
        if (src.len == 0) {
            if (dst.len < 1) return error.OutputTooSmall;
            dst[0] = TOKEN_END;
            return 1;
        }

        // Reset hash table.
        @memset(self.hash_table, 0);

        var src_pos: usize = 0;
        var dst_pos: usize = 0;
        var literal_start: usize = 0;

        while (src_pos + 4 <= src.len) {
            const h = hash4(src, src_pos);
            const candidate = self.hash_table[h];
            self.hash_table[h] = @intCast(src_pos);

            // Check if we have a valid match.
            const cand = @as(usize, candidate);
            if (cand < src_pos and
                src_pos - cand <= MAX_OFFSET and
                std.mem.eql(u8, src[cand..][0..4], src[src_pos..][0..4]))
            {
                // Emit pending literals first.
                if (literal_start < src_pos) {
                    dst_pos = try emitLiterals(src, literal_start, src_pos - literal_start, dst, dst_pos);
                }

                // Extend the match forward.
                var match_len: usize = 4;
                const max_extend = @min(src.len - src_pos, MAX_MATCH_LEN);
                while (match_len < max_extend and src[cand + match_len] == src[src_pos + match_len]) {
                    match_len += 1;
                }

                // Emit match token.
                if (dst_pos + 5 > dst.len) return error.OutputTooSmall;
                dst[dst_pos] = TOKEN_MATCH;
                const offset: u16 = @intCast(src_pos - cand);
                std.mem.writeInt(u16, dst[dst_pos + 1 ..][0..2], offset, .little);
                std.mem.writeInt(u16, dst[dst_pos + 3 ..][0..2], @intCast(match_len), .little);
                dst_pos += 5;

                // Update hash table for positions inside the match (step by 1 for better compression).
                var i: usize = 1;
                while (i < match_len and src_pos + i + 4 <= src.len) : (i += 1) {
                    const mh = hash4(src, src_pos + i);
                    self.hash_table[mh] = @intCast(src_pos + i);
                }

                src_pos += match_len;
                literal_start = src_pos;
            } else {
                src_pos += 1;
            }
        }

        // Emit trailing literals (including last < 4 bytes).
        if (literal_start < src.len) {
            dst_pos = try emitLiterals(src, literal_start, src.len - literal_start, dst, dst_pos);
        }

        // End marker.
        if (dst_pos >= dst.len) return error.OutputTooSmall;
        dst[dst_pos] = TOKEN_END;
        dst_pos += 1;

        return dst_pos;
    }
};

/// Emit literal bytes from `src[start .. start+count]` into `dst` at `dst_pos`.
/// Splits into runs of at most 65535 bytes each.
fn emitLiterals(src: []const u8, start: usize, count: usize, dst: []u8, initial_dst_pos: usize) !usize {
    var remaining = count;
    var src_off = start;
    var dst_pos = initial_dst_pos;

    while (remaining > 0) {
        const run_len: u16 = @intCast(@min(remaining, 65535));
        if (dst_pos + 3 + run_len > dst.len) return error.OutputTooSmall;
        dst[dst_pos] = TOKEN_LITERAL;
        std.mem.writeInt(u16, dst[dst_pos + 1 ..][0..2], run_len, .little);
        dst_pos += 3;
        @memcpy(dst[dst_pos..][0..run_len], src[src_off..][0..run_len]);
        dst_pos += run_len;
        src_off += run_len;
        remaining -= run_len;
    }
    return dst_pos;
}

// ── Decompressor ────────────────────────────────────────────────────────

pub const Decompressor = struct {
    /// Decompress `src` into `dst`. Returns number of bytes written to `dst`.
    pub fn decompress(src: []const u8, dst: []u8) !usize {
        var src_pos: usize = 0;
        var dst_pos: usize = 0;

        while (src_pos < src.len) {
            const token = src[src_pos];
            src_pos += 1;

            switch (token) {
                TOKEN_LITERAL => {
                    if (src_pos + 2 > src.len) return error.MalformedInput;
                    const length = std.mem.readInt(u16, src[src_pos..][0..2], .little);
                    src_pos += 2;
                    if (src_pos + length > src.len) return error.MalformedInput;
                    if (dst_pos + length > dst.len) return error.OutputTooSmall;
                    @memcpy(dst[dst_pos..][0..length], src[src_pos..][0..length]);
                    src_pos += length;
                    dst_pos += length;
                },
                TOKEN_MATCH => {
                    if (src_pos + 4 > src.len) return error.MalformedInput;
                    const offset = std.mem.readInt(u16, src[src_pos..][0..2], .little);
                    const length = std.mem.readInt(u16, src[src_pos + 2 ..][0..2], .little);
                    src_pos += 4;
                    if (offset == 0 or offset > dst_pos) return error.MalformedInput;
                    if (dst_pos + length > dst.len) return error.OutputTooSmall;
                    const match_start = dst_pos - offset;
                    // Copy byte-by-byte to handle overlapping matches (e.g. run-length).
                    for (0..length) |i| {
                        dst[dst_pos + i] = dst[match_start + i];
                    }
                    dst_pos += length;
                },
                TOKEN_END => {
                    return dst_pos;
                },
                else => return error.MalformedInput,
            }
        }

        return error.MalformedInput; // Missing end marker.
    }
};

// ── CRC32 helper ────────────────────────────────────────────────────────

fn crc32(data: []const u8) u32 {
    return std.hash.Crc32.hash(data);
}

// ── Frame-level API ─────────────────────────────────────────────────────

/// Compress `src` into a framed buffer (header + compressed data).
/// Caller owns the returned slice and must free with `allocator`.
pub fn compressFrame(allocator: std.mem.Allocator, src: []const u8) ![]u8 {
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    const max_compressed = Compressor.maxCompressedSize(src.len);
    const tmp = try allocator.alloc(u8, max_compressed);
    defer allocator.free(tmp);

    const compressed_size = try compressor.compress(src, tmp);
    const checksum = crc32(src);

    const total = CompressedFrame.HEADER_SIZE + compressed_size;
    const result = try allocator.alloc(u8, total);

    // Write header.
    std.mem.writeInt(u32, result[0..4], FRAME_MAGIC, .little);
    std.mem.writeInt(u32, result[4..8], @intCast(src.len), .little);
    std.mem.writeInt(u32, result[8..12], @intCast(compressed_size), .little);
    std.mem.writeInt(u32, result[12..16], checksum, .little);

    // Write compressed payload.
    @memcpy(result[CompressedFrame.HEADER_SIZE..][0..compressed_size], tmp[0..compressed_size]);

    return result;
}

/// Decompress a framed buffer. Validates magic and checksum.
/// Caller owns the returned slice and must free with `allocator`.
pub fn decompressFrame(allocator: std.mem.Allocator, frame_data: []const u8) ![]u8 {
    if (frame_data.len < CompressedFrame.HEADER_SIZE) return error.MalformedInput;

    const magic = std.mem.readInt(u32, frame_data[0..4], .little);
    if (magic != FRAME_MAGIC) return error.InvalidMagic;

    const original_size = std.mem.readInt(u32, frame_data[4..8], .little);
    const compressed_size = std.mem.readInt(u32, frame_data[8..12], .little);
    const expected_checksum = std.mem.readInt(u32, frame_data[12..16], .little);

    if (frame_data.len < CompressedFrame.HEADER_SIZE + compressed_size) return error.MalformedInput;

    const compressed = frame_data[CompressedFrame.HEADER_SIZE..][0..compressed_size];
    const result = try allocator.alloc(u8, original_size);
    errdefer allocator.free(result);

    const written = try Decompressor.decompress(compressed, result);
    if (written != original_size) return error.SizeMismatch;

    const actual_checksum = crc32(result[0..written]);
    if (actual_checksum != expected_checksum) return error.ChecksumMismatch;

    return result;
}

// ── Tests ───────────────────────────────────────────────────────────────

test "compress and decompress roundtrip — small data" {
    const allocator = std.testing.allocator;
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    const src = "Hello, ZigBolt compression!";
    var dst: [Compressor.maxCompressedSize(src.len)]u8 = undefined;
    const compressed_len = try compressor.compress(src, &dst);

    var decompressed: [src.len]u8 = undefined;
    const decompressed_len = try Decompressor.decompress(dst[0..compressed_len], &decompressed);

    try std.testing.expectEqual(src.len, decompressed_len);
    try std.testing.expectEqualSlices(u8, src, decompressed[0..decompressed_len]);
}

test "compress and decompress roundtrip — repetitive data (high ratio)" {
    const allocator = std.testing.allocator;
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    // Highly repetitive: 1024 bytes of the same 16-byte pattern.
    var src: [1024]u8 = undefined;
    for (0..src.len) |i| {
        src[i] = @intCast(i % 16);
    }

    var dst: [Compressor.maxCompressedSize(1024)]u8 = undefined;
    const compressed_len = try compressor.compress(&src, &dst);

    var decompressed: [1024]u8 = undefined;
    const decompressed_len = try Decompressor.decompress(dst[0..compressed_len], &decompressed);

    try std.testing.expectEqual(src.len, decompressed_len);
    try std.testing.expectEqualSlices(u8, &src, decompressed[0..decompressed_len]);

    // Compressed size should be significantly smaller.
    try std.testing.expect(compressed_len < src.len / 2);
}

test "compress and decompress roundtrip — random data (low ratio)" {
    const allocator = std.testing.allocator;
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    // Pseudo-random data — poor compression expected.
    var src: [512]u8 = undefined;
    var rng = std.Random.DefaultPrng.init(42);
    const random = rng.random();
    for (&src) |*b| {
        b.* = random.int(u8);
    }

    var dst: [Compressor.maxCompressedSize(512)]u8 = undefined;
    const compressed_len = try compressor.compress(&src, &dst);

    var decompressed: [512]u8 = undefined;
    const decompressed_len = try Decompressor.decompress(dst[0..compressed_len], &decompressed);

    try std.testing.expectEqual(src.len, decompressed_len);
    try std.testing.expectEqualSlices(u8, &src, decompressed[0..decompressed_len]);
}

test "incompressible data handling" {
    const allocator = std.testing.allocator;
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    // Unique bytes — output may be larger than input.
    var src: [256]u8 = undefined;
    for (0..256) |i| {
        src[i] = @intCast(i);
    }

    var dst: [Compressor.maxCompressedSize(256)]u8 = undefined;
    const compressed_len = try compressor.compress(&src, &dst);

    var decompressed: [256]u8 = undefined;
    const decompressed_len = try Decompressor.decompress(dst[0..compressed_len], &decompressed);

    try std.testing.expectEqual(src.len, decompressed_len);
    try std.testing.expectEqualSlices(u8, &src, decompressed[0..decompressed_len]);
}

test "empty input" {
    const allocator = std.testing.allocator;
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    const src: []const u8 = "";
    var dst: [16]u8 = undefined;
    const compressed_len = try compressor.compress(src, &dst);

    try std.testing.expectEqual(@as(usize, 1), compressed_len);
    try std.testing.expectEqual(TOKEN_END, dst[0]);

    var decompressed: [1]u8 = undefined;
    const decompressed_len = try Decompressor.decompress(dst[0..compressed_len], decompressed[0..0]);
    try std.testing.expectEqual(@as(usize, 0), decompressed_len);
}

test "frame compress and decompress roundtrip" {
    const allocator = std.testing.allocator;

    const src = "ZigBolt frame compression test — repeated repeated repeated repeated!";
    const frame = try compressFrame(allocator, src);
    defer allocator.free(frame);

    const decompressed = try decompressFrame(allocator, frame);
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, src, decompressed);
}

test "frame checksum validation" {
    const allocator = std.testing.allocator;

    const src = "Checksum test data";
    const frame = try compressFrame(allocator, src);
    defer allocator.free(frame);

    // Corrupt the compressed payload.
    if (frame.len > CompressedFrame.HEADER_SIZE) {
        frame[CompressedFrame.HEADER_SIZE] ^= 0xFF;
    }

    const result = decompressFrame(allocator, frame);
    // Should fail with either a decompression error or checksum mismatch.
    try std.testing.expect(if (result) |buf| blk: {
        allocator.free(buf);
        break :blk false;
    } else |_| true);
}

test "large data — 64KB" {
    const allocator = std.testing.allocator;
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    const size = 64 * 1024;
    const src = try allocator.alloc(u8, size);
    defer allocator.free(src);

    // Fill with somewhat compressible pattern.
    for (0..size) |i| {
        src[i] = @intCast((i *% 7 + i / 256) % 256);
    }

    const max_out = Compressor.maxCompressedSize(size);
    const dst = try allocator.alloc(u8, max_out);
    defer allocator.free(dst);

    const compressed_len = try compressor.compress(src, dst);

    const decompressed = try allocator.alloc(u8, size);
    defer allocator.free(decompressed);

    const decompressed_len = try Decompressor.decompress(dst[0..compressed_len], decompressed);

    try std.testing.expectEqual(size, decompressed_len);
    try std.testing.expectEqualSlices(u8, src, decompressed[0..decompressed_len]);
}

test "compression ratio — repetitive data should compress well" {
    const allocator = std.testing.allocator;
    var compressor = try Compressor.init(allocator);
    defer compressor.deinit(allocator);

    // 4 KB of a repeating 8-byte pattern.
    const size = 4096;
    var src: [size]u8 = undefined;
    const pattern = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE };
    for (0..size) |i| {
        src[i] = pattern[i % pattern.len];
    }

    var dst: [Compressor.maxCompressedSize(size)]u8 = undefined;
    const compressed_len = try compressor.compress(&src, &dst);

    // Expect at least 4:1 ratio for highly repetitive data.
    try std.testing.expect(compressed_len * 4 < src.len);
}
