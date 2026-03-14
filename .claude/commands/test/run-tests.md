# ZigBolt Test Runner

Run all tests and report results.

## Instructions

1. **Run Unit Tests**
   - Execute `zig build test 2>&1` and capture output
   - Parse test results for pass/fail counts
   - Report any compilation errors first

2. **Run Benchmarks** (if requested via $ARGUMENTS containing "bench")
   - Execute `zig build bench 2>&1`
   - Report RTT percentiles and throughput numbers

3. **Coverage Analysis**
   - Count test blocks in each source file
   - Report modules with < 3 tests as "under-tested"
   - Suggest what additional tests to write

4. **Report**
   - Total tests: passed / failed / skipped
   - Module-by-module breakdown
   - Under-tested modules list
   - Any warnings from compiler
