# ZigBolt Code Review

Perform comprehensive code quality review for ZigBolt (Zig 0.15.1 HFT messaging system).

## Instructions

1. **Architecture Review**
   - Verify module organization follows ZigBolt layered architecture (platform → core → codec → channel → api)
   - Check that all public API exports in `src/root.zig` are correct and complete
   - Verify separation of concerns between layers

2. **Zig Code Quality**
   - Check for proper use of `comptime` vs runtime code
   - Verify correct atomic ordering (`.acquire`/`.release`/`.monotonic`)
   - Check cache-line alignment (`std.atomic.cache_line`) on hot-path structures
   - Look for unnecessary allocations on hot paths
   - Verify proper error handling with Zig error unions
   - Check `defer`/`errdefer` patterns for resource cleanup

3. **Lock-Free Correctness**
   - Verify SPSC/MPSC ring buffer memory ordering
   - Check for ABA problems in CAS loops
   - Verify publish/subscribe visibility guarantees
   - Review LogBuffer term rotation safety

4. **Performance Review**
   - Identify false sharing (structs with mixed read/write fields in same cache line)
   - Check for branch misprediction hotspots
   - Verify zero-copy paths (no unnecessary memcpy)
   - Review alignment of hot-path data structures

5. **Safety & Robustness**
   - Check for potential integer overflow in sequence counters
   - Verify bounds checking on buffer accesses
   - Review shared memory cleanup (shm_unlink, munmap)
   - Check for resource leaks in error paths

6. **Platform Compatibility**
   - Verify macOS/Linux abstraction in `platform/` module
   - Check conditional compilation for platform-specific features
   - Verify no hardcoded Linux-only syscalls on macOS paths

7. **Wire Codec Safety**
   - Verify packed struct alignment guarantees
   - Check endianness assumptions
   - Review zero-copy decode safety (alignment, bounds)

8. **Report**
   - Categorize findings: Critical / High / Medium / Low
   - Provide file paths and line numbers
   - Suggest specific fixes with code examples
