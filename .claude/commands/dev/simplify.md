# ZigBolt Code Simplify

Review changed code for reuse, quality, and efficiency, then fix any issues found.

## Instructions

1. **Check Recent Changes**
   - Run `git diff HEAD` to see uncommitted changes
   - Run `git diff HEAD~1` to see last commit changes
   - Identify all modified/added files

2. **Simplification Review**
   - Look for duplicated code that could be extracted
   - Identify over-engineered abstractions
   - Check for unnecessary indirection
   - Verify error handling is minimal but correct
   - Remove dead code, unused imports, unused variables

3. **Zig-Specific Quality**
   - Replace `var` with `const` where variable is never mutated
   - Use `@as()` casts only when necessary
   - Prefer `errdefer` over manual cleanup
   - Use `comptime` for compile-time-known values
   - Prefer slices over pointer+length pairs

4. **Fix Issues**
   - Apply simplifications directly
   - Run `zig build test` to verify nothing broke
   - Report what was changed and why
