const std = @import("std");
const config = @import("config.zig");
const posix = std.posix;

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
    _ = mem_config;
    const fd = std.c.shm_open(name, @bitCast(std.c.O{ .ACCMODE = .RDWR, .CREAT = true, .TRUNC = true }), 0o600);
    if (fd < 0) return error.ShmOpenFailed;

    const posix_fd: posix.fd_t = @intCast(fd);
    errdefer posix.close(posix_fd);

    // Set size
    posix.ftruncate(posix_fd, @intCast(size)) catch return error.FtruncateFailed;

    // mmap
    const prot = posix.PROT.READ | posix.PROT.WRITE;
    const base = try posix.mmap(null, size, prot, .{ .TYPE = .SHARED }, posix_fd, 0);

    return .{
        .base = base.ptr,
        .len = size,
        .fd = posix_fd,
        .name = name,
    };
}

/// Open an existing named shared memory region.
pub fn openShared(name: [*:0]const u8, size: usize) !SharedRegion {
    const fd = std.c.shm_open(name, @bitCast(std.c.O{ .ACCMODE = .RDWR }), 0);
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
    _ = mem_config;
    const prot = posix.PROT.READ | posix.PROT.WRITE;
    const base = try posix.mmap(null, size, prot, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);

    return .{
        .base = base.ptr,
        .len = size,
        .fd = null,
        .name = null,
    };
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
