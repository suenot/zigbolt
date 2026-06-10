const std = @import("std");
const zigbolt = @import("zigbolt");

// ── Tagged Handles ───────────────────────────────────────────
//
// Every pointer returned across the C boundary is a tagged wrapper, not a
// bare object pointer. publish/poll/*_destroy validate the tag before
// touching the wrapped object, so a Transport handle passed where an IPC
// handle is expected — or a stale or bogus pointer — is rejected with an
// error code instead of reinterpreting unrelated memory (CWE-843). The
// void* ABI is unchanged: callers still see opaque handles.

/// Live-handle marker ("ZBFH" — ZigBolt FFI Handle).
const HANDLE_MAGIC: u32 = 0x5A42_4648;

/// Poisoned marker written on destroy BEFORE the memory is freed, so a
/// double-destroy or a publish/poll after destroy fails the magic check and
/// is safely rejected (CWE-415/416). Best-effort: a freed-and-reallocated
/// block cannot be fully protected, but this catches the common double call.
const HANDLE_POISON: u32 = 0xDEAD_5A42;

const HandleKind = enum(u16) {
    transport = 1,
    ipc_channel = 2,
};

/// Tagged FFI handle: a fixed header (magic + kind) followed by the wrapped
/// object. The object is stored as raw bytes because Transport/IpcChannel
/// are not extern-compatible; the extern layout guarantees the header sits
/// at offset 0 with an identical layout for every kind, so the tag of a
/// wrong-kind handle is still read correctly during validation.
fn TaggedHandle(comptime T: type, comptime kind_tag: HandleKind) type {
    return extern struct {
        magic: u32 = HANDLE_MAGIC,
        kind: u16 = @intFromEnum(kind_tag),
        _pad: u16 = 0,
        obj_bytes: [@sizeOf(T)]u8 align(@alignOf(T)) = undefined,

        const Self = @This();

        fn obj(self: *Self) *T {
            return @ptrCast(&self.obj_bytes);
        }
    };
}

const TransportHandle = TaggedHandle(zigbolt.Transport, .transport);
const IpcHandle = TaggedHandle(zigbolt.IpcChannel, .ipc_channel);

/// Validate an incoming C handle. Returns null unless `handle` is properly
/// aligned AND carries a live magic AND the expected kind — never panics
/// and never dereferences a wrong-typed object. The explicit alignment
/// check runs first so the @alignCast below cannot trip a safety panic on a
/// misaligned bogus pointer (a panic unwinding through an export fn is UB
/// for the C caller, CWE-248). `kind` is compared as a raw integer: loading
/// arbitrary bytes as an enum would itself be checked illegal behavior.
fn checkHandle(comptime W: type, comptime kind_tag: HandleKind, handle: *anyopaque) ?*W {
    if (@intFromPtr(handle) % @alignOf(W) != 0) return null;
    const w: *W = @ptrCast(@alignCast(handle));
    if (w.magic != HANDLE_MAGIC) return null;
    if (w.kind != @intFromEnum(kind_tag)) return null;
    return w;
}

// ── Transport ────────────────────────────────────────────────

pub export fn zigbolt_transport_create(term_length: u32, use_hugepages: u8, pre_fault: u8) ?*anyopaque {
    const w = std.heap.c_allocator.create(TransportHandle) catch return null;
    w.* = .{};
    // term_length widens u32 -> usize (cannot overflow); Transport.init is
    // infallible, so no error can escape this export.
    w.obj().* = zigbolt.Transport.init(std.heap.c_allocator, .{
        .term_length = term_length,
        .use_hugepages = use_hugepages != 0,
        .pre_fault = pre_fault != 0,
    });
    return @ptrCast(w);
}

pub export fn zigbolt_transport_destroy(handle: ?*anyopaque) void {
    const h = handle orelse return;
    const w = checkHandle(TransportHandle, .transport, h) orelse return;
    // Poison before deinit/free so a concurrent-ish or repeated destroy is
    // rejected by the magic check instead of double-freeing.
    w.magic = HANDLE_POISON;
    w.obj().deinit();
    std.heap.c_allocator.destroy(w);
}

// ── IPC Channel ──────────────────────────────────────────────

pub export fn zigbolt_ipc_create(name: ?[*:0]const u8, term_length: u32) ?*anyopaque {
    const n = name orelse return null;
    const w = std.heap.c_allocator.create(IpcHandle) catch return null;
    w.* = .{};
    // term_length is validated inside IpcChannel.create (power of two
    // within [MIN, MAX]); any invalid value (and any shm/name failure)
    // surfaces as an error mapped to NULL here — never a panic across the
    // C boundary.
    w.obj().* = zigbolt.IpcChannel.create(n, .{
        .term_length = term_length,
    }) catch {
        std.heap.c_allocator.destroy(w);
        return null;
    };
    return @ptrCast(w);
}

pub export fn zigbolt_ipc_open(name: ?[*:0]const u8, term_length: u32) ?*anyopaque {
    const n = name orelse return null;
    const w = std.heap.c_allocator.create(IpcHandle) catch return null;
    w.* = .{};
    w.obj().* = zigbolt.IpcChannel.open(n, .{
        .term_length = term_length,
    }) catch {
        std.heap.c_allocator.destroy(w);
        return null;
    };
    return @ptrCast(w);
}

pub export fn zigbolt_ipc_destroy(handle: ?*anyopaque) void {
    const h = handle orelse return;
    const w = checkHandle(IpcHandle, .ipc_channel, h) orelse return;
    // Poison before deinit/free (see zigbolt_transport_destroy).
    w.magic = HANDLE_POISON;
    w.obj().deinit();
    std.heap.c_allocator.destroy(w);
}

/// Publish a message to an IPC channel.
///
/// The caller owns the correctness of (data, len): `data` must point to at
/// least `len` readable bytes for the duration of the call — the library
/// cannot verify a foreign buffer. Returns 0 on success, negative on error:
///   -1  handle is null, already destroyed, or not an IPC channel handle
///   -2  data is null
///   -3  publish failed (message too large, back-pressure, corrupt channel)
pub export fn zigbolt_publish(handle: ?*anyopaque, data: ?[*]const u8, len: u32, msg_type_id: i32) i32 {
    const h = handle orelse return -1;
    const d = data orelse return -2;
    const w = checkHandle(IpcHandle, .ipc_channel, h) orelse return -1;
    // Early bound: the channel enforces this too (error.MessageTooLarge),
    // but rejecting here avoids even forming an oversized slice.
    if (len > zigbolt.frame.MAX_PAYLOAD_SIZE) return -3;
    w.obj().publish(d[0..len], msg_type_id) catch return -3;
    return 0;
}

/// Fragment handler callback type for C FFI.
pub const FragmentHandlerFn = *const fn (data: [*]const u8, len: u32, msg_type_id: i32) callconv(.c) void;

/// Poll for messages from an IPC channel, invoking `callback` per frame.
/// Returns the number of messages read (0 for a null/invalid handle or a
/// null callback).
///
/// The C callback travels through `IpcChannel.pollCtx` as a per-call
/// context (a pointer to a stack slot holding it), so reentrancy is safe:
/// a callback may itself call zigbolt_poll and every active poll keeps its
/// own callback — no threadlocal state to clobber, no silently consumed
/// frames (CWE-662).
pub export fn zigbolt_poll(handle: ?*anyopaque, callback: ?FragmentHandlerFn, limit: u32) u32 {
    const h = handle orelse return 0;
    const cb = callback orelse return 0;
    const w = checkHandle(IpcHandle, .ipc_channel, h) orelse return 0;

    const Dispatch = struct {
        fn dispatch(context: *anyopaque, result: zigbolt.IpcChannel.ReadResult) void {
            const slot: *const FragmentHandlerFn = @ptrCast(@alignCast(context));
            // result.data.len is bounded by the channel's poll loop to at
            // most term_length (<= 1 GiB), so the u32 cast cannot panic.
            slot.*(result.data.ptr, @intCast(result.data.len), result.msg_type_id);
        }
    };

    var cb_slot: FragmentHandlerFn = cb;
    return w.obj().pollCtx(@ptrCast(&cb_slot), &Dispatch.dispatch, limit);
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

// ── Tests ────────────────────────────────────────────────────

const testing = std.testing;

test {
    // Force semantic analysis of every export in this file, so `zig build
    // test` type-checks the whole FFI surface even where no test calls it.
    testing.refAllDecls(@This());
}

test "ffi ipc create/publish/poll roundtrip" {
    const h = zigbolt_ipc_create("/zigbolt_test_ffi_rt", 4096) orelse return error.TestUnexpectedResult;
    defer zigbolt_ipc_destroy(h);

    try testing.expectEqual(@as(i32, 0), zigbolt_publish(h, "ffi roundtrip", "ffi roundtrip".len, 7));

    const S = struct {
        var count: u32 = 0;
        var last_type: i32 = 0;
        var matches: bool = false;
        fn cb(data: [*]const u8, len: u32, msg_type_id: i32) callconv(.c) void {
            count += 1;
            last_type = msg_type_id;
            matches = std.mem.eql(u8, data[0..len], "ffi roundtrip");
        }
    };
    S.count = 0;
    S.last_type = 0;
    S.matches = false;

    try testing.expectEqual(@as(u32, 1), zigbolt_poll(h, &S.cb, 10));
    try testing.expectEqual(@as(u32, 1), S.count);
    try testing.expectEqual(@as(i32, 7), S.last_type);
    try testing.expect(S.matches);
}

test "ffi publish/poll reject a transport handle (type confusion)" {
    const t = zigbolt_transport_create(4096, 0, 0) orelse return error.TestUnexpectedResult;
    defer zigbolt_transport_destroy(t);

    // A Transport handle must never be reinterpreted as an IpcChannel:
    // the kind check rejects it with an error code instead of derefing.
    try testing.expectEqual(@as(i32, -1), zigbolt_publish(t, "x", 1, 1));

    const S = struct {
        var calls: u32 = 0;
        fn cb(_: [*]const u8, _: u32, _: i32) callconv(.c) void {
            calls += 1;
        }
    };
    S.calls = 0;
    try testing.expectEqual(@as(u32, 0), zigbolt_poll(t, &S.cb, 10));
    try testing.expectEqual(@as(u32, 0), S.calls);

    // The wrong-kind destructor is a no-op: the handle stays live (still
    // rejected by publish as wrong-kind, then freed by the deferred
    // zigbolt_transport_destroy without a double-free).
    zigbolt_ipc_destroy(t);
    try testing.expectEqual(@as(i32, -1), zigbolt_publish(t, "x", 1, 1));

    // And the reverse direction: an IPC handle is not a Transport.
    const h = zigbolt_ipc_create("/zigbolt_test_ffi_kind", 4096) orelse return error.TestUnexpectedResult;
    defer zigbolt_ipc_destroy(h);
    zigbolt_transport_destroy(h); // must be a no-op
    try testing.expectEqual(@as(i32, 0), zigbolt_publish(h, "still alive", "still alive".len, 1));
}

test "ffi double destroy is a safe no-op" {
    const h = zigbolt_ipc_create("/zigbolt_test_ffi_dd", 4096) orelse return error.TestUnexpectedResult;
    zigbolt_ipc_destroy(h);
    // Magic was poisoned before the free: the second destroy and any
    // post-destroy use fail the magic check (best-effort: detection reads
    // the freed block, which is reliable for the common immediate retry).
    zigbolt_ipc_destroy(h);
    try testing.expectEqual(@as(i32, -1), zigbolt_publish(h, "x", 1, 1));

    const t = zigbolt_transport_create(4096, 0, 0) orelse return error.TestUnexpectedResult;
    zigbolt_transport_destroy(t);
    zigbolt_transport_destroy(t);
}

test "ffi bogus and misaligned pointers are rejected without a crash" {
    // Non-null pointer to caller-controlled memory that is not a handle:
    // the magic check must reject it on every entry point.
    var bogus: u64 align(8) = 0x1122_3344_5566_7788;
    const p: *anyopaque = @ptrCast(&bogus);

    const S = struct {
        var calls: u32 = 0;
        fn cb(_: [*]const u8, _: u32, _: i32) callconv(.c) void {
            calls += 1;
        }
    };
    S.calls = 0;

    try testing.expectEqual(@as(i32, -1), zigbolt_publish(p, "x", 1, 1));
    try testing.expectEqual(@as(u32, 0), zigbolt_poll(p, &S.cb, 10));
    zigbolt_ipc_destroy(p);
    zigbolt_transport_destroy(p);
    try testing.expectEqual(@as(u32, 0), S.calls);
    // The bogus memory was never written through.
    try testing.expectEqual(@as(u64, 0x1122_3344_5566_7788), bogus);

    // Misaligned pointer: the alignment pre-check must reject it before
    // any @alignCast could trip a safety panic.
    var buf: [16]u8 align(8) = @splat(0);
    const misaligned: *anyopaque = @ptrCast(&buf[1]);
    try testing.expectEqual(@as(i32, -1), zigbolt_publish(misaligned, "x", 1, 1));
    try testing.expectEqual(@as(u32, 0), zigbolt_poll(misaligned, &S.cb, 10));
    zigbolt_ipc_destroy(misaligned);
    zigbolt_transport_destroy(misaligned);
    try testing.expectEqual(@as(u32, 0), S.calls);
}

test "ffi reentrant poll delivers all messages from both channels" {
    const outer = zigbolt_ipc_create("/zigbolt_test_ffi_reent_a", 4096) orelse return error.TestUnexpectedResult;
    defer zigbolt_ipc_destroy(outer);
    const inner = zigbolt_ipc_create("/zigbolt_test_ffi_reent_b", 4096) orelse return error.TestUnexpectedResult;
    defer zigbolt_ipc_destroy(inner);

    try testing.expectEqual(@as(i32, 0), zigbolt_publish(outer, "o1", 2, 1));
    try testing.expectEqual(@as(i32, 0), zigbolt_publish(outer, "o2", 2, 1));
    try testing.expectEqual(@as(i32, 0), zigbolt_publish(inner, "i1", 2, 2));
    try testing.expectEqual(@as(i32, 0), zigbolt_publish(inner, "i2", 2, 2));

    const S = struct {
        var inner_handle: ?*anyopaque = null;
        var outer_delivered: u32 = 0;
        var inner_delivered: u32 = 0;

        fn outerCb(_: [*]const u8, _: u32, _: i32) callconv(.c) void {
            outer_delivered += 1;
            // Reentrant poll from inside a callback. With the old
            // threadlocal scheme, the inner call's cleanup nulled the
            // shared callback slot and the outer poll then consumed its
            // remaining frames without delivering them (silent loss).
            if (inner_delivered == 0) {
                _ = zigbolt_poll(inner_handle, &innerCb, 10);
            }
        }
        fn innerCb(_: [*]const u8, _: u32, _: i32) callconv(.c) void {
            inner_delivered += 1;
        }
    };
    S.inner_handle = inner;
    S.outer_delivered = 0;
    S.inner_delivered = 0;

    try testing.expectEqual(@as(u32, 2), zigbolt_poll(outer, &S.outerCb, 10));
    try testing.expectEqual(@as(u32, 2), S.outer_delivered);
    try testing.expectEqual(@as(u32, 2), S.inner_delivered);
}

test "ffi invalid inputs return null/error codes, never panic" {
    // Invalid term_length values are rejected by IpcChannel validation and
    // mapped to NULL (the old code's unchecked path could reach a panic).
    try testing.expectEqual(@as(?*anyopaque, null), zigbolt_ipc_create("/zigbolt_test_ffi_badtl", 0));
    try testing.expectEqual(@as(?*anyopaque, null), zigbolt_ipc_create("/zigbolt_test_ffi_badtl", 12345));
    try testing.expectEqual(@as(?*anyopaque, null), zigbolt_ipc_create(null, 4096));
    try testing.expectEqual(@as(?*anyopaque, null), zigbolt_ipc_open(null, 4096));
    try testing.expectEqual(@as(?*anyopaque, null), zigbolt_ipc_open("/zigbolt_test_ffi_noexist", 4096));

    const h = zigbolt_ipc_create("/zigbolt_test_ffi_badlen", 4096) orelse return error.TestUnexpectedResult;
    defer zigbolt_ipc_destroy(h);

    // Null arguments keep their documented codes.
    try testing.expectEqual(@as(i32, -1), zigbolt_publish(null, "x", 1, 1));
    try testing.expectEqual(@as(i32, -2), zigbolt_publish(h, null, 1, 1));
    try testing.expectEqual(@as(u32, 0), zigbolt_poll(null, null, 10));
    try testing.expectEqual(@as(u32, 0), zigbolt_poll(h, null, 10));
    zigbolt_ipc_destroy(null);
    zigbolt_transport_destroy(null);

    // Oversized len is rejected up front (-3) without touching `data`.
    try testing.expectEqual(@as(i32, -3), zigbolt_publish(h, "x", zigbolt.frame.MAX_PAYLOAD_SIZE + 1, 1));
    // Larger than a term but within payload bounds: channel rejects -> -3.
    try testing.expectEqual(@as(i32, -3), zigbolt_publish(h, "x", 8192, 1));
}

test "ffi version exports match root.zig constants" {
    try testing.expectEqual(zigbolt.version_major, zigbolt_version_major());
    try testing.expectEqual(zigbolt.version_minor, zigbolt_version_minor());
    try testing.expectEqual(zigbolt.version_patch, zigbolt_version_patch());
    try testing.expectEqual(@as(u32, 0), zigbolt_version_major());
    try testing.expectEqual(@as(u32, 2), zigbolt_version_minor());
    try testing.expectEqual(@as(u32, 1), zigbolt_version_patch());
}
