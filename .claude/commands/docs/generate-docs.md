# ZigBolt Documentation Generator

Generate comprehensive project documentation.

## Instructions

1. **README.md** (project root)
   - Project title, badges, and one-line description
   - Feature highlights (sub-100ns IPC, zero-copy, comptime codecs, etc.)
   - Architecture diagram (ASCII art)
   - Quick start: build, test, benchmark
   - Module overview table
   - Performance benchmarks table
   - Comparison with Aeron/Chronicle/ZeroMQ
   - API usage examples (Publisher/Subscriber, IPC, UDP)
   - FFI examples (Rust, Python, C)
   - Requirements (Zig 0.15.1, Linux/macOS)
   - License (MIT)

2. **docs/architecture.md**
   - Layered architecture explanation
   - Data flow diagrams
   - Module dependency graph
   - Memory layout of key structures (FrameHeader, LogBuffer, ring buffers)
   - Threading model

3. **docs/api-reference.md**
   - All public types and functions from `src/root.zig`
   - Constructor signatures, parameters, return types
   - Usage examples for each major API
   - Error types and when they occur

4. **docs/benchmarks.md**
   - Benchmark methodology
   - Results table (IPC RTT, throughput, UDP RTT)
   - How to run benchmarks
   - Comparison with Aeron targets
   - Hardware/OS requirements for best results

5. **docs/examples.md**
   - IPC ping-pong example
   - Market data publisher/subscriber
   - UDP multicast example
   - Archive record and replay
   - Raft cluster setup
   - FFI from Rust/Python

6. **docs/internals.md**
   - Lock-free ring buffer design
   - LogBuffer triple-buffering
   - NAK-based reliability protocol
   - Raft consensus implementation details
   - Wire codec comptime generation

7. **Format Rules**
   - Use GitHub-flavored markdown
   - Include code blocks with `zig` syntax highlighting
   - Keep each doc focused and under 500 lines
   - Cross-reference between docs with relative links
