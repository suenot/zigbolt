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

    // ── Benchmarks ───────────────────────────────────────────
    const bench_step = b.step("bench", "Run benchmarks");

    const ping_pong_mod = b.createModule(.{
        .root_source_file = b.path("bench/ping_pong.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    ping_pong_mod.addImport("zigbolt", lib_mod);
    const ping_pong = b.addExecutable(.{
        .name = "bench_ping_pong",
        .root_module = ping_pong_mod,
    });
    b.installArtifact(ping_pong);

    const run_bench = b.addRunArtifact(ping_pong);
    bench_step.dependOn(&run_bench.step);

    const throughput_mod = b.createModule(.{
        .root_source_file = b.path("bench/throughput.zig"),
        .target = target,
        .optimize = .ReleaseFast,
    });
    throughput_mod.addImport("zigbolt", lib_mod);
    const throughput = b.addExecutable(.{
        .name = "bench_throughput",
        .root_module = throughput_mod,
    });
    b.installArtifact(throughput);
}
