const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // ── Library ──────────────────────────────────────────────
    const lib_mod = b.addModule("zigbolt", .{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });

    // ── FFI Library (C ABI, shared + static) ────────────────
    const ffi_mod = b.createModule(.{
        .root_source_file = b.path("src/ffi/exports.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
    });
    ffi_mod.addImport("zigbolt", lib_mod);

    const shared_lib = b.addLibrary(.{
        .name = "zigbolt",
        .linkage = .dynamic,
        .root_module = ffi_mod,
    });
    b.installArtifact(shared_lib);

    const static_lib = b.addLibrary(.{
        .name = "zigbolt",
        .linkage = .static,
        .root_module = ffi_mod,
    });
    b.installArtifact(static_lib);

    // ── Tests ────────────────────────────────────────────────
    // Run all tests via root.zig which uses refAllDecls to discover them.
    const test_step = b.step("test", "Run all unit tests");

    const test_mod = b.createModule(.{
        .root_source_file = b.path("src/root.zig"),
        .target = target,
        .optimize = optimize,
    });
    const t = b.addTest(.{
        .root_module = test_mod,
    });
    const run_t = b.addRunArtifact(t);
    test_step.dependOn(&run_t.step);

    // The C-ABI surface (src/ffi/exports.zig) is the root file of ffi_mod
    // and a file may belong to only one module per compilation, so it
    // cannot be pulled into the root test module via @import. Type-check
    // and run its tests as a dedicated test artifact instead.
    const ffi_tests = b.addTest(.{
        .root_module = ffi_mod,
    });
    const run_ffi_tests = b.addRunArtifact(ffi_tests);
    test_step.dependOn(&run_ffi_tests.step);

    // ── Benchmarks ───────────────────────────────────────────
    const bench_step = b.step("bench", "Run benchmarks");

    // Helper to register a benchmark executable.
    const bench_names = [_]struct { name: []const u8, file: []const u8 }{
        .{ .name = "bench_ping_pong", .file = "bench/ping_pong.zig" },
        .{ .name = "bench_throughput", .file = "bench/throughput.zig" },
        .{ .name = "bench_udp_rtt", .file = "bench/udp_rtt.zig" },
        .{ .name = "bench_spsc_latency", .file = "bench/spsc_latency.zig" },
        .{ .name = "bench_mpsc_latency", .file = "bench/mpsc_latency.zig" },
        .{ .name = "bench_codec_throughput", .file = "bench/codec_throughput.zig" },
        .{ .name = "bench_ipc_multisize", .file = "bench/ipc_multisize.zig" },
        .{ .name = "bench_logbuffer", .file = "bench/logbuffer_throughput.zig" },
        .{ .name = "bench_run_all", .file = "bench/run_all.zig" },
    };

    for (bench_names) |entry| {
        const mod = b.createModule(.{
            .root_source_file = b.path(entry.file),
            .target = target,
            .optimize = .ReleaseFast,
        });
        mod.addImport("zigbolt", lib_mod);
        const exe = b.addExecutable(.{
            .name = entry.name,
            .root_module = mod,
        });
        b.installArtifact(exe);
    }

    // `zig build bench` runs the full suite
    const run_all_mod = b.createModule(.{
        .root_source_file = b.path("bench/run_all.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    run_all_mod.addImport("zigbolt", lib_mod);
    const run_all_exe = b.addExecutable(.{
        .name = "bench_suite",
        .root_module = run_all_mod,
    });
    const run_all = b.addRunArtifact(run_all_exe);
    bench_step.dependOn(&run_all.step);
}
