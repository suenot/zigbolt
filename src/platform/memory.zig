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

/// A region of shared or anonymous memory-mapped memory.
pub const SharedRegion = struct {
    base: [*]align(std.heap.page_size_min) u8,
    len: usize,
    fd: ?posix.fd_t,
    name: ?[*:0]const u8,

    /// Get a typed, aligned pointer within the region at byte offset.
    pub fn ptrAt(self: SharedRegion, comptime T: type, byte_offset: usize) *align(config.cache_line_size) T {
        const ptr = self.base + byte_offset;
        return @ptrCast(@alignCast(ptr));
    }

    /// Get a slice of the region starting at byte offset.
    pub fn sliceAt(self: SharedRegion, byte_offset: usize, len: usize) []u8 {
        return (self.base + byte_offset)[0..len];
    }

    /// Destroy the shared region: unmap and optionally unlink.
    pub fn deinit(self: *SharedRegion) void {
        const slice = self.base[0..self.len];
        posix.munmap(slice);

        if (self.fd) |fd| {
            posix.close(fd);
        }
        if (self.name) |name| {
            _ = std.c.shm_unlink(name);
        }
    }
};

/// Memory configuration for shared region creation.
pub const MemoryConfig = struct {
    /// Use hugepages (Linux only, ignored on macOS).
    use_hugepages: bool = false,
    /// Pre-fault pages to avoid page faults on the hot path.
    pre_fault: bool = true,
    /// Lock pages in RAM (prevent swapping).
    mlock: bool = true,
};

/// Create a named shared memory region accessible by multiple processes.
pub fn createShared(name: [*:0]const u8, size: usize, mem_config: MemoryConfig) !SharedRegion {
    // Pre-unlink to avoid EACCES on macOS when a stale segment with
    // different permissions exists from a previous crashed process.
    _ = std.c.shm_unlink(name);

    const fd = std.c.shm_open(name, @bitCast(std.c.O{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true }), 0o600);
    if (fd < 0) return error.ShmOpenFailed;

    const posix_fd: posix.fd_t = @intCast(fd);
    errdefer posix.close(posix_fd);

    // Ensure correct permissions (umask may strip bits on some platforms).
    // Use raw C fchmod since Zig's posix.fchmod may not be available.
    const fchmod_rc = std.c.fchmod(posix_fd, 0o600);
    _ = fchmod_rc;

    // Set size
    posix.ftruncate(posix_fd, @intCast(size)) catch return error.FtruncateFailed;

    // mmap
    const prot = posix.PROT.READ | posix.PROT.WRITE;
    const base = try posix.mmap(null, size, prot, .{ .TYPE = .SHARED }, posix_fd, 0);

    const region = SharedRegion{
        .base = base.ptr,
        .len = size,
        .fd = posix_fd,
        .name = name,
    };

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
pub fn openShared(name: [*:0]const u8, size: usize) !SharedRegion {
    const fd = std.c.shm_open(name, @bitCast(std.c.O{ .ACCMODE = .RDWR }), 0o600);
    if (fd < 0) return error.ShmOpenFailed;

    const posix_fd: posix.fd_t = @intCast(fd);
    errdefer posix.close(posix_fd);

    const prot = posix.PROT.READ | posix.PROT.WRITE;
    const base = try posix.mmap(null, size, prot, .{ .TYPE = .SHARED }, posix_fd, 0);

    return .{
        .base = base.ptr,
        .len = size,
        .fd = posix_fd,
        .name = null, // Don't unlink on close — we didn't create it
    };
}

/// Create an anonymous (process-private) memory region.
pub fn createAnonymous(size: usize, mem_config: MemoryConfig) !SharedRegion {
    const prot = posix.PROT.READ | posix.PROT.WRITE;
    const base = try posix.mmap(null, size, prot, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);

    const region = SharedRegion{
        .base = base.ptr,
        .len = size,
        .fd = null,
        .name = null,
    };

    if (mem_config.pre_fault) {
        prefault(region);
    }

    if (mem_config.mlock) {
        _ = mlockMemory(base.ptr, size);
    }

    return region;
}

/// Pre-fault all pages in a region to avoid page faults on the hot path.
pub fn prefault(region: SharedRegion) void {
    const slice = region.base[0..region.len];
    // Touch each page to force the OS to allocate physical pages
    var i: usize = 0;
    while (i < slice.len) : (i += config.page_size) {
        // Volatile read to prevent optimization
        _ = @as(*volatile u8, @ptrCast(&slice[i])).*;
    }
}

// ── Tests ────────────────────────────────────────────────────

test "SharedRegion create and close" {
    const name = "/zigbolt_test_shm_create";
    var region = try createShared(name, 4096, .{});
    defer region.deinit();

    try std.testing.expect(region.len == 4096);
    try std.testing.expect(region.fd != null);
    try std.testing.expect(region.name != null);

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
    try std.testing.expect(region.name == null);

    // Should be writable
    region.base[0] = 0x11;
    region.base[8191] = 0x22;
    try std.testing.expectEqual(@as(u8, 0x11), region.base[0]);
    try std.testing.expectEqual(@as(u8, 0x22), region.base[8191]);
}

test "prefault does not crash" {
    const name = "/zigbolt_test_shm_prefault";
    var region = try createShared(name, 16384, .{});
    defer region.deinit();

    // Should not crash
    prefault(region);
}
