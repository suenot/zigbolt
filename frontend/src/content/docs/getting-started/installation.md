---
title: Installation
description: How to install and configure ZigBolt
---

## Requirements

| Requirement | Version |
|-------------|---------|
| Zig compiler | 0.15.1+ |
| OS | Linux (x86_64, aarch64) or macOS (aarch64) |

ZigBolt has zero external dependencies. Everything is implemented in pure Zig.

## Building from Source

```bash
git clone https://github.com/suenot/zigbolt.git
cd zigbolt
zig build
```

### Build Targets

```bash
zig build              # Build all targets
zig build test         # Run all unit tests (199 tests)
zig build bench        # Build all benchmarks
```

### Build Options

ZigBolt builds with `-OReleaseFast` for benchmarks to enable maximum optimization.

## Using as a Zig Package

Add ZigBolt as a dependency in your `build.zig.zon`:

```zig
.{
    .name = "my-trading-system",
    .version = "0.1.0",
    .dependencies = .{
        .zigbolt = .{
            .url = "https://github.com/suenot/zigbolt/archive/v0.2.0.tar.gz",
            // .hash = "...",
        },
    },
}
```

Then in your `build.zig`:

```zig
const zigbolt = b.dependency("zigbolt", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("zigbolt", zigbolt.module("zigbolt"));
```

## FFI (C / Rust / Python)

Build the shared library:

```bash
zig build
# Output: zig-out/lib/libzigbolt.so (Linux) or libzigbolt.dylib (macOS)
```

### C

```c
#include <stdint.h>

// Declare ZigBolt C-ABI functions
extern void* zigbolt_ipc_create(const char* name, uint32_t term_length);
extern int32_t zigbolt_publish(void* handle, const uint8_t* data, uint32_t len, int32_t msg_type_id);
extern void zigbolt_ipc_destroy(void* handle);

int main() {
    void* ch = zigbolt_ipc_create("/my-channel", 1 << 20);
    uint8_t data[] = {1, 2, 3, 4};
    zigbolt_publish(ch, data, sizeof(data), 1);
    zigbolt_ipc_destroy(ch);
    return 0;
}
```

### Python

```python
import ctypes

lib = ctypes.CDLL("./zig-out/lib/libzigbolt.dylib")

lib.zigbolt_version_major.restype = ctypes.c_uint32
lib.zigbolt_version_minor.restype = ctypes.c_uint32
lib.zigbolt_version_patch.restype = ctypes.c_uint32

print(f"ZigBolt v{lib.zigbolt_version_major()}.{lib.zigbolt_version_minor()}.{lib.zigbolt_version_patch()}")
```

## Platform Notes

### Linux
- Shared memory via `shm_open` + `mmap`
- Best performance with `isolcpus` and `nohz_full` kernel parameters
- io_uring support planned for future versions

### macOS
- Shared memory via `shm_open` + `mmap`
- Hugepages not available (gracefully falls back to regular pages)
- Cache line size: 128 bytes (Apple Silicon)
