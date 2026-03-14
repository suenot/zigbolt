# ZigBolt Performance Audit

Audit ZigBolt for HFT-grade performance. Target: sub-100ns IPC, 50M+ msg/sec throughput.

## Instructions

1. **Hot Path Analysis**
   - Trace the publish → poll critical path in IPC channel
   - Identify every memory access, atomic operation, and branch
   - Count cache line touches per operation
   - Verify zero-allocation on hot paths

2. **Cache Efficiency**
   - Check all hot-path structs for cache-line alignment
   - Identify false sharing between publisher/subscriber fields
   - Verify padding prevents cross-cache-line access
   - Check struct field ordering for access pattern locality

3. **Atomic Operations**
   - Verify minimum necessary ordering (prefer `.monotonic` where safe)
   - Check for unnecessary atomic operations
   - Review CAS retry loops for livelock potential
   - Verify no hidden atomic operations in standard library calls

4. **Memory Access Patterns**
   - Verify sequential access in ring buffers (hardware prefetch friendly)
   - Check for pointer chasing in critical paths
   - Review mmap configuration (hugepages, pre-fault, mlock)
   - Verify buffer alignment for SIMD operations

5. **Branch Prediction**
   - Identify unpredictable branches on hot paths
   - Check for `if`/`switch` on hot paths that could be eliminated
   - Verify comptime resolution where possible

6. **Benchmark Validation**
   - Run `zig build bench` and analyze results
   - Compare IPC RTT against Aeron target (< 100ns p50)
   - Measure throughput against target (50M+ msg/sec)
   - Check UDP RTT against target (< 5μs with io_uring)

7. **Optimization Opportunities**
   - Suggest specific optimizations with expected impact
   - Prioritize by latency impact on critical path
   - Provide before/after code examples
