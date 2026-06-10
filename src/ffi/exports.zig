const std = @import("std");
const zigbolt = @import("zigbolt");

// ── Transport ────────────────────────────────────────────────

pub export fn zigbolt_transport_create(term_length: u32, use_hugepages: u8, pre_fault: u8) ?*anyopaque {
    const transport = std.heap.c_allocator.create(zigbolt.Transport) catch return null;
    transport.* = zigbolt.Transport.init(std.heap.c_allocator, .{
        .term_length = @intCast(term_length),
        .use_hugepages = use_hugepages != 0,
        .pre_fault = pre_fault != 0,
    });
    return @ptrCast(transport);
}

pub export fn zigbolt_transport_destroy(handle: ?*anyopaque) void {
    if (handle) |h| {
        const transport: *zigbolt.Transport = @ptrCast(@alignCast(h));
        transport.deinit();
        std.heap.c_allocator.destroy(transport);
    }
}

// ── IPC Channel ──────────────────────────────────────────────

pub export fn zigbolt_ipc_create(name: ?[*:0]const u8, term_length: u32) ?*anyopaque {
    const n = name orelse return null;
    const ch = std.heap.c_allocator.create(zigbolt.IpcChannel) catch return null;
    ch.* = zigbolt.IpcChannel.create(n, .{
        .term_length = @intCast(term_length),
    }) catch {
        std.heap.c_allocator.destroy(ch);
        return null;
    };
    return @ptrCast(ch);
}

pub export fn zigbolt_ipc_open(name: ?[*:0]const u8, term_length: u32) ?*anyopaque {
    const n = name orelse return null;
    const ch = std.heap.c_allocator.create(zigbolt.IpcChannel) catch return null;
    ch.* = zigbolt.IpcChannel.open(n, .{
        .term_length = @intCast(term_length),
    }) catch {
        std.heap.c_allocator.destroy(ch);
        return null;
    };
    return @ptrCast(ch);
}

pub export fn zigbolt_ipc_destroy(handle: ?*anyopaque) void {
    if (handle) |h| {
        const ch: *zigbolt.IpcChannel = @ptrCast(@alignCast(h));
        ch.deinit();
        std.heap.c_allocator.destroy(ch);
    }
}

/// Publish a message to an IPC channel.
/// Returns 0 on success, negative on error.
pub export fn zigbolt_publish(handle: ?*anyopaque, data: ?[*]const u8, len: u32, msg_type_id: i32) i32 {
    const h = handle orelse return -1;
    const d = data orelse return -2;
    const ch: *zigbolt.IpcChannel = @ptrCast(@alignCast(h));
    ch.publish(d[0..len], msg_type_id) catch return -3;
    return 0;
}

/// Fragment handler callback type for C FFI.
pub const FragmentHandlerFn = *const fn (data: [*]const u8, len: u32, msg_type_id: i32) callconv(.c) void;

/// Thread-local storage to pass the C callback into the Zig handler,
/// since Zig comptime closures cannot capture runtime values.
threadlocal var current_callback: ?FragmentHandlerFn = null;

/// Poll for messages from an IPC channel.
/// Returns the number of messages read.
pub export fn zigbolt_poll(handle: ?*anyopaque, callback: ?FragmentHandlerFn, limit: u32) u32 {
    const h = handle orelse return 0;
    const cb = callback orelse return 0;
    const ch: *zigbolt.IpcChannel = @ptrCast(@alignCast(h));

    current_callback = cb;
    defer current_callback = null;

    const Handler = struct {
        fn dispatch(result: zigbolt.IpcChannel.ReadResult) void {
            if (current_callback) |ffi_cb| {
                ffi_cb(result.data.ptr, @intCast(result.data.len), result.msg_type_id);
            }
        }
    };
    return ch.poll(&Handler.dispatch, limit);
}

// ── Version Info ─────────────────────────────────────────────

pub export fn zigbolt_version_major() u32 {
    return zigbolt.version_major;
}

pub export fn zigbolt_version_minor() u32 {
    return zigbolt.version_minor;
}

pub export fn zigbolt_version_patch() u32 {
    return zigbolt.version_patch;
}
