# ZigBolt Test Generator

Generate comprehensive tests for ZigBolt modules. Target: `$ARGUMENTS` (or all under-tested modules if empty).

## Instructions

1. **Coverage Analysis**
   - Read all source files in `src/` and identify test blocks
   - List modules with their current test count
   - Identify modules with insufficient coverage (< 3 tests per module)
   - Prioritize: core data structures > channel layer > API > cluster

2. **Test Categories for Each Module**
   - **Happy path**: Normal operation with valid inputs
   - **Edge cases**: Empty buffers, max capacity, zero-length messages
   - **Error conditions**: Full buffers, invalid inputs, resource exhaustion
   - **Concurrency**: Multi-threaded access patterns (SPSC/MPSC)
   - **Boundary**: Power-of-2 boundaries, wrap-around, overflow

3. **Zig Test Patterns**
   - Use `test "descriptive name" { ... }` blocks
   - Use `std.testing.expect*` assertions
   - Use `std.testing.allocator` for leak detection
   - Defer cleanup: `defer obj.deinit();`
   - Use `@import("std").testing.refAllDecls(@This())` in module root for discovery

4. **Module-Specific Test Focus**
   - **SPSC**: push/pop, full buffer, empty buffer, wrap-around, capacity
   - **MPSC**: concurrent push, CAS contention, consumer drain
   - **LogBuffer**: claim/commit, term rotation, padding frames, concurrent access
   - **WireCodec**: encode/decode roundtrip, batch ops, alignment validation
   - **IPC Channel**: publish/poll, multi-message, shared memory lifecycle
   - **UDP Channel**: send/recv, non-blocking, multicast config
   - **Reliability**: NAK generation, retransmission, flow control credits
   - **Fragment**: fragmentation/reassembly roundtrip, max message size
   - **Network**: publish/poll with reliability, NAK handling
   - **Archive**: record/replay, segment rotation, filtering
   - **Raft**: election, log replication, commit advancement, term handling
   - **Sequencer**: monotonic ordering, multi-stream merge

5. **Test Quality**
   - Each test must have a clear assertion (not just "doesn't crash")
   - Test names describe the behavior being verified
   - Tests are independent and don't depend on execution order
   - Use `errdefer` for debug output on test failure

6. **Integration Tests**
   - Publisher → Subscriber roundtrip
   - IPC Channel end-to-end with shared memory
   - Archive record → replay consistency
   - Raft leader election with 3+ nodes

7. **Output**
   - Add tests directly to existing source files (in `test` blocks)
   - Run `zig build test` to verify all tests pass
   - Report total test count before and after
