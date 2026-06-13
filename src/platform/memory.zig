const std = @import("std");
const builtin = @import("builtin");
const config = @import("config.zig");
const posix = std.posix;

/// Thin wrapper around the C library mlock(2) syscall.
fn mlockMemory(addr: [*]const u8, len: usize) bool {
    const c = struct {
        extern "c" fn mlock(addr: ?*const anyopaque, len: usize) c_int;
    };
    return c.mlock(@ptrCast(addr), len) == 0;
}

/// shm_open(2) with the mode actually delivered to libc.
///
/// Darwin declares `int shm_open(const char *, int, ...)` — VARIADIC. On
/// aarch64-darwin variadic arguments are passed on the stack, but
/// `std.c.shm_open`'s fixed-arity prototype passes the mode in a register
/// that libc never reads, so segments were created with garbage permissions
/// (and could subsequently fail to re-open with EACCES). Call through a
/// correct variadic prototype on Darwin; elsewhere shm_open is fixed-arity.
fn shmOpen(name: [*:0]const u8, oflag: c_int, mode: std.c.mode_t) c_int {
    if (comptime builtin.os.tag.isDarwin()) {
        const darwin = struct {
            extern "c" fn shm_open(name: [*:0]const u8, oflag: c_int, ...) c_int;
        };
        // C default argument promotion: mode_t (u16) promotes to int.
        return darwin.shm_open(name, oflag, @as(c_int, @intCast(mode)));
    }
    return std.c.shm_open(name, oflag, mode);
}

/// Maximum length of a shared-memory object name (excluding the NUL).
/// POSIX bounds shm names by NAME_MAX (255); macOS enforces a stricter
/// PSHMNAMLEN (31) at the syscall level, which shm_open reports itself.
pub const max_shm_name_len: usize = 255;

/// A region of shared or anonymous memory-mapped memory.
pub const SharedRegion = struct {
    base: [*]align(std.heap.page_size_min) u8,
    len: usize,
    fd: ?posix.fd_t,
    /// Owned copy of the shm name. Only set by `createShared` (the owner),
    /// so `deinit()` unlinks the segment; `openShared`/`createAnonymous`
    /// leave it empty. Owning a copy — instead of borrowing the caller's
    /// pointer — prevents a use-after-free unlink when bindings pass a
    /// transient C string that is freed right after creation.
    name_buf: [max_shm_name_len:0]u8 = @splat(0),
    name_len: usize = 0,

    /// Get a typed, aligned pointer within the region at byte offset.
    pub fn ptrAt(self: SharedRegion, comptime T: type, byte_offset: usize) *align(config.cache_line_size) T {
        const ptr = self.base + byte_offset;
        return @ptrCast(@alignCast(ptr));
    }

    /// Get a slice of the region starting at byte offset.
    pub fn sliceAt(self: SharedRegion, byte_offset: usize, len: usize) []u8 {
        return (self.base + byte_offset)[0..len];
    }

    /// Destroy the shared region: unmap and, if this region owns the name
    /// (creator side), unlink the shm object.
    pub fn deinit(self: *SharedRegion) void {
        const slice = self.base[0..self.len];
        posix.munmap(slice);

        if (self.fd) |fd| {
            posix.close(fd);
        }
        if (self.name_len > 0) {
            _ = std.c.shm_unlink(&self.name_buf);
        }
    }
};

/// Memory configuration for shared region creation.
pub const MemoryConfig = struct {
    /// Use hugepages for anonymous regions (Linux MAP_HUGETLB only;
    /// best-effort with fallback to normal pages). Explicit no-op for
    /// named shm regions — MAP_HUGETLB requires anonymous or hugetlbfs
    /// mappings and cannot back a POSIX shm object — and on macOS, which
    /// exposes no hugepage mmap flag.
    use_hugepages: bool = false,
    /// Pre-fault pages to avoid page faults on the hot path.
    pre_fault: bool = true,
    /// Lock pages in RAM (prevent swapping).
    mlock: bool = true,
};

/// Create a named shared memory region accessible by multiple processes.
///
/// `name` must be non-empty, start with '/', and fit `max_shm_name_len`
/// (POSIX shm naming rules); otherwise `error.InvalidName` is returned.
pub fn createShared(name: [*:0]const u8, size: usize, mem_config: MemoryConfig) !SharedRegion {
    const name_slice = std.mem.span(name);
    if (name_slice.len == 0 or name_slice.len > max_shm_name_len) return error.InvalidName;
    if (name_slice[0] != '/') return error.InvalidName;

    // Pre-unlink to avoid EACCES on macOS when a stale segment with
    // different permissions exists from a previous crashed process.
    _ = std.c.shm_unlink(name);

    // O_EXCL after the unlink: if another process (re)created the name in
    // the window (squat attempt), fail instead of silently adopting and
    // truncating an object we did not create.
    const fd = shmOpen(name, @bitCast(std.c.O{ .ACCMODE = .RDWR, .CREAT = true, .EXCL = true }), 0o600);
    if (fd < 0) {
        return if (posix.errno(fd) == .EXIST) error.ShmExists else error.ShmOpenFailed;
    }

    const posix_fd: posix.fd_t = @intCast(fd);
    errdefer posix.close(posix_fd);
    // Don't leave a half-initialized named object linked on failure.
    errdefer _ = std.c.shm_unlink(name);

    // Ensure correct permissions (umask may strip bits on some platforms).
    // Best-effort, but surface unexpected failures instead of dropping them
    // silently: the 0600 mode is what keeps other users off the segment.
    // macOS does not support fchmod on shm descriptors (EINVAL) — there the
    // 0600 mode passed to shm_open(O_CREAT|O_EXCL) is already authoritative.
    if (std.c.fchmod(posix_fd, 0o600) != 0) { // kcov-skip: evaluated on every createShared; on the Linux runner fchmod succeeds so only the false branch exists; no own line record
        const err: posix.E = @enumFromInt(std.c._errno().*); // kcov-skip: Darwin-only: fchmod on shm fds fails (EINVAL) only on macOS; cannot execute on the Linux coverage runner
        if (err != .INVAL) { // kcov-skip: Darwin-only: see line above
            std.log.warn("fchmod(0o600) failed for shm '{s}': {s}", .{ name_slice, @tagName(err) });
        }
    }

    // Set size
    posix.ftruncate(posix_fd, @intCast(size)) catch return error.FtruncateFailed;

    // mmap
    const prot = posix.PROT.READ | posix.PROT.WRITE;
    const base = try posix.mmap(null, size, prot, .{ .TYPE = .SHARED }, posix_fd, 0);

    var region = SharedRegion{
        .base = base.ptr,
        .len = size,
        .fd = posix_fd,
        .name_len = name_slice.len,
    };
    // Own a copy of the name; name_buf is pre-zeroed so the copy is always
    // NUL-terminated (name_slice.len <= max_shm_name_len was checked above).
    @memcpy(region.name_buf[0..name_slice.len], name_slice);

    // Apply memory configuration
    if (mem_config.pre_fault) {
        prefault(region);
    }

    if (mem_config.mlock) {
        // Lock pages in RAM to prevent swapping. Best-effort: ignore failures
        // (e.g. unprivileged processes may lack RLIMIT_MEMLOCK allowance).
        _ = mlockMemory(base.ptr, size);
    }

    return region;
}

/// Open an existing named shared memory region.
///
/// Verifies the object is at least `size` bytes before mapping: touching a
/// page of a mapping that extends past the object's end raises SIGBUS, so
/// an undersized (or attacker-shrunk) object must be rejected up front.
pub fn openShared(name: [*:0]const u8, size: usize) !SharedRegion {
    const fd = shmOpen(name, @bitCast(std.c.O{ .ACCMODE = .RDWR }), 0);
    if (fd < 0) return error.ShmOpenFailed;

    const posix_fd: posix.fd_t = @intCast(fd);
    errdefer posix.close(posix_fd);

    const st = posix.fstat(posix_fd) catch return error.ShmOpenFailed;
    if (st.size < 0 or @as(u64, @intCast(st.size)) < size) return error.ShmTooSmall;

    const prot = posix.PROT.READ | posix.PROT.WRITE;
    const base = try posix.mmap(null, size, prot, .{ .TYPE = .SHARED }, posix_fd, 0);

    return .{
        .base = base.ptr, // kcov-skip: runs on every successful openShared (ipc + shm tests); literal field store folded, no own line record
        .len = size,
        .fd = posix_fd,
        // name_len stays 0: don't unlink on close — we didn't create it.
    };
}

/// Create an anonymous (process-private) memory region.
pub fn createAnonymous(size: usize, mem_config: MemoryConfig) !SharedRegion {
    const prot = posix.PROT.READ | posix.PROT.WRITE;

    const base = blk: {
        // MAP_HUGETLB is Linux-only and only valid for anonymous/hugetlbfs
        // mappings. Best-effort: if the hugepage pool is empty or the size
        // is not hugepage-aligned, fall back to normal pages.
        if (builtin.os.tag == .linux and mem_config.use_hugepages) {
            if (posix.mmap(null, size, prot, .{ .TYPE = .PRIVATE, .ANONYMOUS = true, .HUGETLB = true }, -1, 0)) |slice| {
                break :blk slice;
            } else |_| {}
        }
        break :blk try posix.mmap(null, size, prot, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);
    };

    const region = SharedRegion{
        .base = base.ptr,
        .len = size,
        .fd = null,
    };

    if (mem_config.pre_fault) {
        prefault(region);
    }

    if (mem_config.mlock) {
        _ = mlockMemory(base.ptr, size);
    }

    return region;
}

/// Pre-fault all pages in a region so the hot path takes no page faults.
///
/// Performs a volatile read-modify-write of one byte per page: a plain read
/// is not enough for private (COW) mappings, where the first hot-path WRITE
/// would still take a copy-on-write fault. The write stores back the value
/// just read, so existing contents (e.g. an already-initialized metadata
/// header) are preserved.
pub fn prefault(region: SharedRegion) void {
    const slice = region.base[0..region.len];
    var i: usize = 0;
    while (i < slice.len) : (i += config.page_size) {
        const byte: *volatile u8 = @ptrCast(&slice[i]);
        byte.* = byte.*;
    } // kcov-skip: prefault loop back-edge; loop iterates in every prefaulting test; attributed to the loop body
}

// ── Tests ────────────────────────────────────────────────────

// Read one byte through a volatile pointer. A plain `region.base[i]` load of
// the *last* byte of a page-aligned mapping can be widened by the ReleaseFast
// optimizer into a multi-byte load that runs past the mapping into the
// unmapped guard page and faults (SIGSEGV). A volatile load is exact-width and
// stays in bounds — the same reason prefault() touches pages through a
// *volatile u8.
fn readByteVolatile(base: [*]const u8, i: usize) u8 {
    const p: *const volatile u8 = @ptrCast(&base[i]);
    return p.*;
}

test "SharedRegion create and close" {
    const name = "/zigbolt_test_shm_create";
    var region = try createShared(name, 4096, .{});
    defer region.deinit();

    try std.testing.expect(region.len == 4096);
    try std.testing.expect(region.fd != null);
    try std.testing.expect(region.name_len > 0);

    // Should be able to write to the region
    region.base[0] = 0xAA;
    region.base[4095] = 0xBB;
    try std.testing.expectEqual(@as(u8, 0xAA), region.base[0]);
    try std.testing.expectEqual(@as(u8, 0xBB), region.base[4095]);
}

test "SharedRegion multiple create and close" {
    // Test that we can create, write, close, and re-create shared regions.
    const name = "/zigbolt_test_shm_multi";
    _ = std.c.shm_unlink(name);

    {
        var region = try createShared(name, 4096, .{});
        region.base[0] = 0x42;
        try std.testing.expectEqual(@as(u8, 0x42), region.base[0]);
        region.deinit();
    }

    // Should be able to re-create after close
    {
        var region = try createShared(name, 4096, .{});
        defer region.deinit();
        region.base[0] = 0xBB;
        try std.testing.expectEqual(@as(u8, 0xBB), region.base[0]);
    }
}

test "SharedRegion ptrAt and sliceAt" {
    const name = "/zigbolt_test_shm_ptr";
    var region = try createShared(name, 4096, .{});
    defer region.deinit();

    const slice = region.sliceAt(0, 16);
    try std.testing.expectEqual(@as(usize, 16), slice.len);

    // Write through slice
    @memset(slice, 0xCD);
    try std.testing.expectEqual(@as(u8, 0xCD), region.base[0]);
    try std.testing.expectEqual(@as(u8, 0xCD), region.base[15]);
}

test "createAnonymous region" {
    var region = try createAnonymous(8192, .{});
    defer region.deinit();

    try std.testing.expectEqual(@as(usize, 8192), region.len);
    try std.testing.expect(region.fd == null);
    try std.testing.expectEqual(@as(usize, 0), region.name_len);

    // Should be writable
    region.base[0] = 0x11;
    region.base[8191] = 0x22;
    try std.testing.expectEqual(@as(u8, 0x11), readByteVolatile(region.base, 0));
    try std.testing.expectEqual(@as(u8, 0x22), readByteVolatile(region.base, 8191));
}

test "prefault does not crash" {
    const name = "/zigbolt_test_shm_prefault";
    var region = try createShared(name, 16384, .{});
    defer region.deinit();

    // Should not crash
    prefault(region);
}

test "prefault preserves existing contents" {
    var region = try createAnonymous(16384, .{ .pre_fault = false, .mlock = false });
    defer region.deinit();

    // Initialize page-start bytes (the ones prefault touches), then prefault.
    region.base[0] = 0xA5;
    region.base[region.len - 1] = 0x5A;
    prefault(region);

    try std.testing.expectEqual(@as(u8, 0xA5), readByteVolatile(region.base, 0));
    try std.testing.expectEqual(@as(u8, 0x5A), readByteVolatile(region.base, region.len - 1));
}

test "createShared rejects invalid names" {
    // Empty name
    try std.testing.expectError(error.InvalidName, createShared("", 4096, .{}));
    // Missing leading '/'
    try std.testing.expectError(error.InvalidName, createShared("no_leading_slash", 4096, .{}));
    // Longer than the owned-name buffer
    const too_long = "/" ++ "a" ** max_shm_name_len;
    try std.testing.expectError(error.InvalidName, createShared(too_long, 4096, .{}));
}

test "SharedRegion owns a copy of the shm name" {
    const shm_name = "/zigbolt_test_shm_own";

    // Build the name in a mutable buffer to simulate a transient C string
    // from a binding (freed/reused right after create returns).
    var caller_buf = [_]u8{0} ** 64; // kcov-skip: test local; the passing owned-name test uses it; hit record oscillates between builds
    @memcpy(caller_buf[0..shm_name.len], shm_name);

    var region = try createShared(@ptrCast(&caller_buf), 4096, .{});
    try std.testing.expectEqual(shm_name.len, region.name_len);

    // Clobber the caller's buffer — deinit must unlink from its OWN copy,
    // not through the (now garbage) caller pointer.
    @memset(&caller_buf, 0xFF);
    region.deinit();

    // The segment must actually be gone: the unlink hit the right name.
    try std.testing.expectError(error.ShmOpenFailed, openShared(shm_name, 4096));
}

test "openShared rejects undersized objects" {
    const name = "/zigbolt_test_shm_small";
    var region = try createShared(name, 4096, .{});
    defer region.deinit();

    // Asking for more than the object holds must fail instead of mapping
    // pages that would SIGBUS on first touch.
    try std.testing.expectError(error.ShmTooSmall, openShared(name, 1 << 20));

    // Exact size still opens fine.
    var opened = try openShared(name, 4096);
    defer opened.deinit();
    try std.testing.expectEqual(@as(usize, 0), opened.name_len);
}

test "createShared fails cleanly on an unmappable size and leaves no object" {
    const name = "/zigbolt_test_shm_huge";
    // 2^62 bytes: ftruncate or mmap must fail; the errdefers close the fd
    // and unlink the half-created object.
    try std.testing.expect(std.meta.isError(createShared(name, 1 << 62, .{})));

    // The name is reusable immediately — nothing was left linked.
    var region = try createShared(name, 4096, .{});
    region.deinit();
}

test "createShared surfaces OS-level name rejections" {
    // An embedded slash is invalid on Linux (EINVAL); macOS may accept it.
    if (createShared("/zigbolt/extra_slash", 4096, .{})) |r| {
        var region = r; // kcov-skip: Darwin-only success arm: Linux rejects the embedded slash with EINVAL, so this never executes on the Linux coverage runner
        region.deinit();
    } else |err| {
        try std.testing.expect(err == error.ShmOpenFailed or err == error.ShmExists);
    }

    // Over macOS's PSHMNAMLEN (31); fine on Linux (NAME_MAX).
    const long_name = "/zigbolt_shm_name_longer_than_31_chars_total";
    if (createShared(long_name, 4096, .{})) |r| {
        var region = r;
        region.deinit();
    } else |err| {
        try std.testing.expect(err == error.ShmOpenFailed or err == error.ShmExists);
    }
}

test "createAnonymous honors a hugepage request with fallback" {
    // On Linux this attempts MAP_HUGETLB first and falls back to normal
    // pages when no hugepages are reserved; on macOS the branch compiles out.
    var region = try createAnonymous(2 * 1024 * 1024, .{ .use_hugepages = true });
    defer region.deinit();

    region.base[0] = 0x77;
    region.base[region.len - 1] = 0x88;
    try std.testing.expectEqual(@as(u8, 0x77), readByteVolatile(region.base, 0));
    try std.testing.expectEqual(@as(u8, 0x88), readByteVolatile(region.base, region.len - 1));
}
