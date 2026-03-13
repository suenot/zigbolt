# ZigBolt: Ultra-Low Latency Messaging System for HFT

## Спецификация проекта v0.1 — Reforms.AI / Marketmaker.cc

---

## Executive Summary

ZigBolt — система передачи сообщений на Zig, спроектированная как прямой конкурент Aeron (Real Logic / Adaptive) для high-frequency trading. Ключевое конкурентное преимущество: zero-runtime-overhead (нет GC, нет JVM safepoints), comptime-генерируемые кодеки (замена SBE), нативная поддержка io_uring и kernel bypass (DPDK/AF_XDP), предсказуемая наносекундная латентность без tail latency спайков.

### Почему Zig, а не C/C++/Rust?

| Критерий | Zig | C/C++ | Rust | Java (Aeron) |
|---|---|---|---|---|
| GC/Runtime overhead | Нет | Нет | Нет | JVM safepoints, GC (даже off-heap) |
| Comptime codegen | Нативный | Макросы/шаблоны | proc macros | Нет |
| C interop (DPDK/liburing) | Trivial (@cImport) | Нативный | FFI/bindgen | JNI overhead |
| SIMD | @Vector, встроенный | Intrinsics/asm | packed_simd (unstable) | Vectorization hints |
| Cross-compilation | Встроенная | CMake/Makefile hell | cargo target | N/A |
| Bounds checking | ReleaseFast=off | Нет (UB) | Всегда (unsafe opt-out) | Всегда |
| Build time | Секунды | Минуты (C++) | Минуты | Секунды (compile), но JVM startup |
| Hidden control flow | Нет | Exceptions, implicit casts | Паники в unwrap | Exceptions everywhere |

**Zig даёт уникальную комбинацию:** производительность C + безопасность при разработке + comptime метапрограммирование (генерация wire-format кодеков, lookup tables, protocol state machines на этапе компиляции) + trivial интеграция с существующими C-библиотеками (DPDK, liburing, ef_vi).

---

## Часть 1: Анализ Aeron — что нужно побить

### 1.1 Aeron Transport

**Архитектура:** Media Driver (отдельный процесс или embedded) + Client API + Publication/Subscription модель. Коммуникация между клиентом и Media Driver — через shared memory (mmap файлы в /dev/shm). Данные передаются через Log Buffers (3 term buffers по дефолту, каждый от 64KB до 1GB). Управление — через cnc.dat (Command and Control файл).

**Ключевые структуры данных:**
- **ManyToOneRingBuffer** (MPSC) — клиенты → Media Driver commands
- **BroadcastTransmitter/Receiver** — Media Driver → клиенты responses  
- **Log Buffers** — triple-buffered append-only log для данных (term A → term B → term C → rotate)
- **Position counters** — атомарные счётчики для координации publisher/subscriber позиций

**Производительность (заявленная):**
- IPC (shared memory): RTT ~0.25 мкс (250 нс)
- UDP unicast: RTT ~10 мкс на bare metal
- Cloud (AWS): <100 мкс
- Throughput: >20M msg/sec на пике
- Aeron Premium (kernel bypass): P99 = 39 мкс, P99.9 = 43 мкс

**Слабости Aeron Transport:**
1. **JVM dependency** — даже с off-heap memory, JVM safepoints (GuaranteedSafepointInterval=300000 — костыль), JIT warm-up, class loading. C-драйвер существует, но менее feature-complete
2. **Media Driver overhead** — отдельный процесс = лишний hop через shared memory. Embedded mode привязывает к JVM
3. **Фиксированная архитектура log buffers** — 3 term buffers, размер фиксирован при создании. Нет адаптивного управления памятью
4. **UDP-only для сети** — нет нативной поддержки io_uring, DPDK, AF_XDP. Aeron Premium (closed source) добавляет kernel bypass, но это платная опция
5. **SBE — отдельная зависимость** — XML schemas, Java codegen, отдельный build step. Нет интеграции в язык
6. **Нет zero-copy networking** — данные копируются из сокета в log buffer. io_uring ZC Rx (Linux 6.15+) мог бы это решить

### 1.2 Aeron Archive

**Что делает:** запись message streams на диск для replay. Subscriber подписывается на publication → Archive записывает все сообщения → позже можно replay с любой позиции.

**Архитектура:** Archive процесс подписывается на потоки через Aeron Transport, записывает в segmented files на диск. Replay — создаёт новую publication из записанных данных.

**Слабости:**
1. Файловый I/O через стандартные Java NIO / POSIX calls — нет io_uring для async disk I/O
2. Нет компрессии на лету
3. Нет нативной интеграции с columnar formats (Parquet, Arrow)
4. Replay через сеть — лишний hop, когда данные на локальном диске

### 1.3 Aeron Cluster

**Что делает:** fault-tolerant replicated state machine на базе Raft consensus. Leader принимает commands, реплицирует на followers через Aeron Transport, committed entries применяются к state machine.

**Характеристики:**
- Automatic leader election и failover (<1 сек)
- Strongly consistent (linearizable reads через leader)
- Hot system upgrades
- Throughput: "millions of messages per second" с Raft consensus

**Слабости:**
1. Raft consensus добавляет минимум 1 RTT на каждый committed message — это фундаментально
2. Leader bottleneck — весь throughput через один node
3. JVM garbage collection на leader = latency spike для всего кластера
4. Нет multi-Raft (несколько независимых Raft-групп для шардирования)

### 1.4 Aeron Sequencer (Новый, 2025)

**Что делает:** единый strongly consistent replicated log, оптимизированный для capital markets. Позволяет нескольким независимым SMR-приложениям подключаться к одному логу. Total ordering всех событий.

**Архитектура:**
- Построен поверх Aeron Cluster
- Distributed log, реплицированный на несколько машин
- Multiple readers — несколько приложений читают один лог
- Decoupled teams — команды работают независимо в рамках единого ordering

**Слабости:**
1. Все слабости Aeron Cluster наследуются
2. Closed-source (коммерческий продукт Adaptive)
3. Single-log bottleneck — все сообщения через один sequencer
4. Нет нативной поддержки sharding по инструментам/стратегиям

---

## Часть 2: Архитектура ZigBolt

### 2.1 Обзор продуктовой линейки

```
┌─────────────────────────────────────────────────────────────────┐
│                        ZigBolt Suite                            │
├─────────────┬───────────────┬──────────────┬───────────────────┤
│  Transport  │   Archive     │   Cluster    │    Sequencer      │
│  (Core)     │   (Persist)   │   (HA)       │    (Total Order)  │
├─────────────┴───────────────┴──────────────┴───────────────────┤
│                     Wire Format (comptime SBE)                  │
├─────────────────────────────────────────────────────────────────┤
│            IO Layer: io_uring │ DPDK │ AF_XDP │ POSIX           │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 ZigBolt Transport — ядро системы

#### 2.2.1 Философия: No Media Driver

Aeron вынуждает использовать Media Driver как промежуточный процесс. ZigBolt встраивается прямо в приложение — zero overhead, zero extra hops.

```
┌──────────────────────────────────────────────────────────┐
│  Aeron: App → shm → Media Driver → shm → socket → NIC  │
│  ZigBolt: App → ring buffer → io_uring → NIC            │
└──────────────────────────────────────────────────────────┘
```

#### 2.2.2 Архитектура Transport

```
┌─────────────────────────────────────────────────────────┐
│                    Application                           │
│  pub = transport.publisher("btc_ticks", .{});           │
│  pub.offer(msg);                                        │
├─────────────────────────────────────────────────────────┤
│                  ZigBolt Client API                       │
│  Publisher<T> / Subscriber<T> / ExclusivePublisher<T>   │
│  comptime-typed: pub.offer(TickMsg{...})                │
├─────────────────────────────────────────────────────────┤
│                  Channel Layer                            │
│  IPC (shared memory) │ UDP unicast │ UDP multicast       │
│  io_uring │ DPDK │ AF_XDP │ RDMA                        │
├─────────────────────────────────────────────────────────┤
│               Log Buffer Manager                         │
│  SPSC/MPSC ring buffers в shared memory                 │
│  Lock-free, cache-line aligned, pre-faulted hugepages   │
├─────────────────────────────────────────────────────────┤
│          Reliability Layer (optional)                     │
│  NAK-based retransmission │ Flow control │ Ordering     │
└─────────────────────────────────────────────────────────┘
```

#### 2.2.3 Log Buffer Design

```zig
// Comptime-параметризованный Log Buffer
pub fn LogBuffer(comptime config: LogBufferConfig) type {
    return struct {
        const Self = @This();
        
        // Triple-buffered terms (как Aeron, но с comptime-размерами)
        terms: [3]*align(std.mem.page_size) [config.term_length]u8,
        
        // Metadata в отдельной cache line
        meta: *align(cache_line_size) LogMetadata,
        
        // Position counters — каждый в своей cache line для false sharing prevention
        pub_pos: *align(cache_line_size) Atomic(i64),
        sub_pos: *align(cache_line_size) Atomic(i64),
        
        pub fn claim(self: *Self, length: u32) ?Claim {
            // Lock-free CAS на tail position
            // Zero-copy: возвращает slice прямо в term buffer
            // Приложение пишет данные прямо в shared memory
        }
        
        pub fn commit(self: *Self, claim: Claim) void {
            // Атомарный store length в frame header
            // Subscriber увидит новое сообщение
        }
    };
}
```

**Отличия от Aeron:**
1. **comptime-параметризация** — размеры буферов, alignment, количество terms — всё решается на этапе компиляции. Нет runtime конфигурации = нет branch mispredictions
2. **Hugepage support** — pre-faulted 2MB/1GB hugepages для минимизации TLB misses
3. **Cache-line isolation** — каждый position counter в своей cache line (64 или 128 байт в зависимости от CPU). `comptime` определяет правильный размер через `@cachelineSize()`
4. **SPSC fast path** — если publisher один (ExclusivePublisher), CAS заменяется на простой store. Comptime выбирает реализацию

#### 2.2.4 IPC (Shared Memory) Channel

```zig
pub const IpcChannel = struct {
    // mmap-based shared memory через /dev/shm
    // Publisher и Subscriber маппят один и тот же файл
    
    shm_fd: std.posix.fd_t,
    base_addr: [*]align(std.mem.page_size) u8,
    log_buffer: LogBuffer(.{ .term_length = 1 << 20 }), // 1MB terms
    
    pub fn publish(self: *IpcChannel, data: []const u8) !void {
        // 1. Claim space in log buffer (CAS or store)
        // 2. Memcpy data into claimed region
        // 3. Commit (store frame length)
        // Весь путь: ~50-100 нс для мелких сообщений
    }
    
    pub fn poll(self: *IpcChannel, handler: FragmentHandler, limit: u32) u32 {
        // 1. Read subscriber position
        // 2. Check if new data available (compare with pub position)
        // 3. Call handler for each complete frame
        // 4. Update subscriber position
        // Zero-copy: handler получает slice прямо в shared memory
    }
};
```

#### 2.2.5 Network Channel — io_uring backend

```zig
pub const IoUringChannel = struct {
    ring: std.os.linux.IoUring,
    
    // Pre-registered buffers для zero-copy TX/RX
    registered_buffers: []align(std.mem.page_size) [buffer_size]u8,
    
    // Completion ring polling — без syscalls в steady state
    pub fn poll_completions(self: *IoUringChannel) u32 {
        // io_uring CQ polling — userspace only, zero syscalls
        // IORING_SETUP_SQPOLL: kernel thread polls SQ
        // Результат: send/recv без единого context switch
    }
    
    pub fn send(self: *IoUringChannel, data: []const u8, addr: std.net.Address) !void {
        // IORING_OP_SEND_ZC — zero-copy send (Linux 6.0+)
        // Данные отправляются прямо из pre-registered buffer
        // Нет копирования в kernel socket buffer
    }
    
    pub fn recv(self: *IoUringChannel) ![]const u8 {
        // IORING_OP_RECV с registered buffers
        // Linux 6.15+: io_uring ZC Rx — данные прямо в userspace memory
        // NIC DMA → registered buffer → application
        // Нет копирования из kernel buffer
    }
};
```

#### 2.2.6 DPDK Backend (kernel bypass)

```zig
// Trivial C interop через @cImport
const dpdk = @cImport({
    @cInclude("rte_eal.h");
    @cInclude("rte_ethdev.h");
    @cInclude("rte_mbuf.h");
    @cInclude("rte_ring.h");
});

pub const DpdkChannel = struct {
    port_id: u16,
    tx_ring: *dpdk.rte_ring,
    rx_ring: *dpdk.rte_ring,
    mempool: *dpdk.rte_mempool,
    
    pub fn init(config: DpdkConfig) !DpdkChannel {
        // EAL init, port setup, queue setup
        // Hugepage memory allocation через DPDK mempool
        // RSS (Receive Side Scaling) для multi-queue
    }
    
    pub fn send_batch(self: *DpdkChannel, mbufs: []**dpdk.rte_mbuf) u16 {
        // rte_eth_tx_burst — batch send, no syscalls
        // Latency: sub-microsecond (NIC DMA → wire)
    }
    
    pub fn recv_batch(self: *DpdkChannel, mbufs: []**dpdk.rte_mbuf) u16 {
        // rte_eth_rx_burst — polling mode, no interrupts
        // Packets arrive directly into hugepage memory
    }
};
```

#### 2.2.7 AF_XDP Backend (kernel-integrated bypass)

```zig
// AF_XDP: performance close to DPDK, but keeps kernel networking tools
pub const AfXdpChannel = struct {
    xsk: *xdp.xsk_socket,
    umem: *xdp.xsk_umem,
    
    // UMEM: shared memory region between kernel and userspace
    // Frames pre-allocated in hugepages
    fill_ring: xdp.xsk_ring_prod, // app → kernel: "here are empty buffers"
    comp_ring: xdp.xsk_ring_cons, // kernel → app: "these buffers were sent"
    rx_ring: xdp.xsk_ring_cons,   // kernel → app: "received data here"
    tx_ring: xdp.xsk_ring_prod,   // app → kernel: "send this data"
    
    // Преимущество: не забирает NIC у kernel (в отличие от DPDK)
    // ping, ip, tcpdump — всё работает
};
```

### 2.3 Comptime Wire Format — замена SBE

**Ключевая инновация:** вместо XML schema → Java codegen → generated code, ZigBolt генерирует кодеки на этапе компиляции из Zig struct definitions.

```zig
// Определение сообщения — обычный Zig struct
pub const TickMessage = packed struct {
    timestamp_ns: u64,    // 8 bytes, offset 0
    symbol_id: u32,       // 4 bytes, offset 8
    price: i64,           // 8 bytes, offset 12 (fixed-point, 8 decimals)
    volume: u64,          // 8 bytes, offset 20
    side: enum(u8) { bid = 0, ask = 1 }, // 1 byte, offset 28
    _padding: [3]u8 = .{0} ** 3,         // 3 bytes, alignment to 32
    // Total: 32 bytes, cache-line friendly
};

// Comptime-генерируемый кодек
pub fn WireCodec(comptime T: type) type {
    return struct {
        const wire_size = @sizeOf(T);
        
        // Compile-time validation
        comptime {
            // Проверяем что struct packed (no padding surprises)
            if (@typeInfo(T) != .@"struct" or !@typeInfo(T).@"struct".layout != .@"packed")
                @compileError("WireCodec requires packed struct");
            
            // Проверяем alignment
            if (wire_size % 8 != 0)
                @compileError("Wire size must be 8-byte aligned");
        }
        
        // Zero-copy encode: просто @ptrCast на buffer
        pub inline fn encode(msg: *const T, buf: []u8) void {
            @memcpy(buf[0..wire_size], std.mem.asBytes(msg));
        }
        
        // Zero-copy decode: flyweight прямо на buffer
        pub inline fn decode(buf: []const u8) *const T {
            return @ptrCast(@alignCast(buf.ptr));
        }
        
        // SIMD batch decode для market data
        pub fn decode_batch(buf: []const u8, out: []T) u32 {
            const count = @min(buf.len / wire_size, out.len);
            // @Vector-based SIMD memcpy для batch processing
            // На AVX-512: 64 байта за такт = 2 сообщения за такт
            @memcpy(
                std.mem.sliceAsBytes(out[0..count]),
                buf[0..count * wire_size],
            );
            return @intCast(count);
        }
    };
}

// Использование — нулевой overhead
const codec = WireCodec(TickMessage);
var msg = codec.decode(buffer[offset..]);
// msg.price — прямой доступ, без десериализации
```

**Сравнение с SBE:**

| Аспект | SBE (Aeron) | ZigBolt comptime |
|---|---|---|
| Schema язык | XML | Zig structs (нативный код) |
| Codegen | Java tool → generated Java/C++ | comptime → inlined machine code |
| Build step | Отдельный (sbe-tool) | Нет (часть компиляции) |
| Runtime overhead | Flyweight pattern (virtual dispatch) | Zero (inlined @ptrCast) |
| Validation | Runtime checks | Compile-time @compileError |
| Variable-length fields | В конце сообщения | Zig slices с comptime layout |
| Batch decode | Ручной | Автоматический SIMD |
| Endianness | Configurable | comptime native endian + @byteSwap |

### 2.4 ZigBolt Archive

```zig
pub const Archive = struct {
    // Подписывается на Transport publications
    // Записывает на диск через io_uring (async, zero-copy)
    
    ring: std.os.linux.IoUring,
    segments: SegmentManager,
    index: SegmentIndex,
    
    pub const Config = struct {
        segment_size: usize = 256 * 1024 * 1024, // 256MB segments
        sync_policy: enum { none, periodic, every_segment } = .periodic,
        sync_interval_ms: u32 = 1000,
        compression: ?CompressionAlgo = null, // lz4, zstd
        max_disk_usage: ?usize = null,
    };
    
    // Запись через io_uring — async, не блокирует hot path
    pub fn record(self: *Archive, stream_id: u32) !void {
        // Подписывается на stream через Transport
        // Каждый fragment → io_uring WRITE (batched)
        // fsync через io_uring FSYNC (periodic)
    }
    
    // Replay: создаёт Publication из записанных данных
    pub fn replay(self: *Archive, params: ReplayParams) !ReplaySession {
        // Sequential read через io_uring READV
        // Prefetch следующих segments
        // Zero-copy: mmap segment → publish через IPC
    }
    
    // Экспорт в Parquet для аналитики
    pub fn export_parquet(self: *Archive, stream_id: u32, path: []const u8) !void {
        // Columnar conversion: row-based archive → Parquet columns
        // Интеграция с вашим StockAPIs Parquet pipeline
    }
    
    // Экспорт в QuestDB через ILP (InfluxDB Line Protocol)
    pub fn stream_to_questdb(self: *Archive, stream_id: u32, endpoint: []const u8) !void {
        // Replay → ILP encode → TCP send to QuestDB
    }
};
```

**Отличия от Aeron Archive:**
1. **io_uring для disk I/O** — async write/fsync без блокировки. Aeron использует синхронный Java NIO
2. **Встроенная компрессия** — LZ4 (low latency) или ZSTD (high ratio) на лету
3. **Parquet/QuestDB экспорт** — нативная интеграция с вашим data pipeline
4. **Segment prefetch** — io_uring readahead для replay без stall'ов

### 2.5 ZigBolt Cluster

```zig
pub const Cluster = struct {
    // Raft consensus implementation на Zig
    // Оптимизирован для trading: batching, pre-vote, pipeline replication
    
    pub const Config = struct {
        node_id: u32,
        peers: []const PeerConfig,
        election_timeout_ms: struct { min: u32 = 150, max: u32 = 300 },
        heartbeat_interval_ms: u32 = 50,
        
        // Trading-specific optimizations
        batch_commit: bool = true,       // Batch multiple entries per consensus round
        pipeline_replication: bool = true, // Don't wait for prev entry ACK before sending next
        pre_vote: bool = true,            // Prevent disruption from partitioned nodes
        read_index: bool = true,          // Linearizable reads without log entry
    };
    
    // State Machine interface — пользователь реализует бизнес-логику
    pub const StateMachine = struct {
        // Вызывается для каждого committed entry
        applyFn: *const fn (entry: []const u8) void,
        
        // Snapshot для быстрого recovery
        snapshotFn: *const fn () []const u8,
        restoreFn: *const fn (snapshot: []const u8) void,
    };
    
    // Leader получает команду от клиента
    pub fn propose(self: *Cluster, data: []const u8) !ProposeFuture {
        // 1. Append to leader's log
        // 2. Replicate to followers via Transport (parallel)
        // 3. Wait for majority ACK
        // 4. Commit & apply to state machine
        // 5. Respond to client
    }
    
    // Multi-Raft: несколько независимых Raft-групп
    pub const MultiRaft = struct {
        groups: std.AutoHashMap(u32, *Cluster),
        
        // Шардирование по symbol_id:
        // BTC trades → Raft group 0
        // ETH trades → Raft group 1
        // Каждая группа — независимый consensus
        pub fn route(self: *MultiRaft, symbol_id: u32) *Cluster {
            return self.groups.get(symbol_id % self.groups.count()) orelse unreachable;
        }
    };
};
```

**Отличия от Aeron Cluster:**
1. **Нет JVM** — предсказуемая латентность, нет GC spikes
2. **Multi-Raft** — шардирование по инструментам. BTC и ETH не конкурируют за один consensus round
3. **Pipeline replication** — не ждём ACK предыдущего entry перед отправкой следующего. Увеличивает throughput в 3-5x при стабильной сети
4. **Pre-vote** — партitioned node не вызовет ненужные election'ы при reconnect
5. **Read Index** — linearizable reads без записи в лог (для read-heavy workloads типа query state)

### 2.6 ZigBolt Sequencer

```zig
pub const Sequencer = struct {
    // Total ordering для capital markets
    // Все входящие события получают monotonic sequence number
    // Все consumers видят события в одинаковом порядке
    
    cluster: *Cluster,
    sequence: Atomic(u64),
    
    // Gateway отправляет событие → Sequencer штампует sequence → broadcast
    pub fn sequence_event(self: *Sequencer, event: []const u8) !SequencedEvent {
        const seq = self.sequence.fetchAdd(1, .monotonic);
        
        // Wrap event with sequence header
        var sequenced = SequencedEvent{
            .sequence = seq,
            .timestamp_ns = rdtsc(), // hardware timestamp
            .payload = event,
        };
        
        // Replicate through Raft for durability
        try self.cluster.propose(sequenced.encode());
        
        return sequenced;
    }
    
    // Multi-stream sequencer: total order across multiple input streams
    pub const MultiStreamSequencer = struct {
        // N input streams → 1 sequenced output
        // Fair scheduling: round-robin с весами
        // Deterministic replay: один и тот же input → один и тот же output
        
        input_streams: []const StreamConfig,
        output: *LogBuffer(.{}),
        
        pub fn run(self: *MultiStreamSequencer) !void {
            // Busy-poll all input streams
            // Stamp with sequence number
            // Write to output log
            // Single-threaded: deterministic, no synchronization
        }
    };
};
```

**Отличия от Aeron Sequencer:**
1. **Open source** — Aeron Sequencer коммерческий
2. **Multi-Raft sharding** — несколько sequencer'ов для разных market segments
3. **Hardware timestamps** — rdtsc для наносекундной точности (vs System.nanoTime() в Java)
4. **Deterministic replay** — один и тот же input гарантированно даёт один и тот же output (для backtesting)

---

## Часть 3: Системное программирование — детали

### 3.1 Memory Management

```zig
pub const MemoryConfig = struct {
    // Hugepages: 2MB или 1GB
    hugepage_size: enum { @"2MB", @"1GB" } = .@"2MB",
    
    // Pre-fault: все страницы загружаются в RAM при создании
    pre_fault: bool = true,
    
    // NUMA-aware allocation
    numa_node: ?u32 = null,
    
    // mlock: prevent swapping
    mlock: bool = true,
};

pub fn allocateSharedMemory(config: MemoryConfig, size: usize) !SharedRegion {
    // 1. Создаём файл в /dev/hugepages/ или /dev/shm/
    // 2. mmap с MAP_HUGETLB | MAP_LOCKED | MAP_POPULATE
    // 3. Если NUMA — mbind() к конкретному node
    // 4. Pre-fault: memset для избежания page faults на hot path
    // 5. madvise(MADV_DONTFORK) — не копировать при fork
}
```

### 3.2 CPU Affinity & Isolation

```zig
pub const ThreadConfig = struct {
    // Привязка к конкретному CPU core
    cpu_affinity: ?u32 = null,
    
    // Приоритет: SCHED_FIFO для реального времени
    scheduling: enum { normal, fifo, rr } = .fifo,
    priority: u32 = 90,
    
    // Idle strategy
    idle: enum {
        busy_spin,          // 100% CPU, минимальная латентность
        yield,              // sched_yield между polls
        back_off,           // exponential backoff (spin → yield → park)
        sleeping,           // epoll_wait/io_uring_wait
    } = .busy_spin,
};

// Рекомендуемая конфигурация ядер для co-location:
// Core 0: OS + interrupts (isolcpus=1-7)
// Core 1: ZigBolt Transport IO (send/recv)
// Core 2: Market Data processing
// Core 3: Strategy engine
// Core 4: Order management
// Core 5: Archive (disk IO)
// Core 6-7: spare / monitoring
```

### 3.3 Lock-Free Data Structures

```zig
// SPSC Ring Buffer — основной примитив
pub fn SpscRingBuffer(comptime T: type, comptime capacity: usize) type {
    // capacity must be power of 2 (comptime enforced)
    comptime {
        if (capacity & (capacity - 1) != 0)
            @compileError("capacity must be power of 2");
    }
    
    return struct {
        const mask = capacity - 1;
        
        buffer: [capacity]T align(cache_line_size) = undefined,
        head: Atomic(usize) align(cache_line_size) = .init(0), // writer
        tail: Atomic(usize) align(cache_line_size) = .init(0), // reader
        // head и tail в разных cache lines — нет false sharing
        
        pub fn push(self: *@This(), item: T) bool {
            const h = self.head.load(.monotonic);
            const next = (h + 1) & mask;
            if (next == self.tail.load(.acquire)) return false; // full
            self.buffer[h] = item;
            self.head.store(next, .release);
            return true;
        }
        
        pub fn pop(self: *@This()) ?T {
            const t = self.tail.load(.monotonic);
            if (t == self.head.load(.acquire)) return null; // empty
            const item = self.buffer[t];
            self.tail.store((t + 1) & mask, .release);
            return item;
        }
    };
}

// MPSC Ring Buffer — для multiple publishers
pub fn MpscRingBuffer(comptime T: type, comptime capacity: usize) type {
    return struct {
        // CAS-based multi-producer with single consumer
        // Используется для command channel (клиенты → transport)
        
        pub fn push(self: *@This(), item: T) bool {
            while (true) {
                const h = self.head.load(.monotonic);
                const next = (h + 1) & mask;
                if (next == self.tail.load(.acquire)) return false;
                if (self.head.cmpxchgWeak(h, next, .release, .monotonic)) |_| {
                    continue; // CAS failed, retry
                }
                self.buffer[h] = item;
                return true;
            }
        }
    };
}
```

### 3.4 Reliability Protocol (UDP)

```zig
pub const ReliabilityProtocol = struct {
    // NAK-based reliable multicast (как Aeron)
    // + улучшения для trading workloads
    
    // Frame header layout (16 bytes):
    // [0:4]   frame_length: u32
    // [4:8]   session_id: u32
    // [8:12]  stream_id: u32
    // [12:16] term_offset: u32
    
    pub const FrameHeader = packed struct {
        frame_length: u32,
        session_id: u32,
        stream_id: u32,
        term_offset: u32,
    };
    
    // Loss detection: receiver tracks gaps in term_offset
    // NAK: receiver sends back [stream_id, term_id, term_offset, length]
    // Retransmission: sender re-reads from log buffer (zero-copy)
    
    // Flow control: credit-based
    // Receiver grants credits (bytes it can accept)
    // Publisher backs off when credits exhausted (returns BACK_PRESSURED)
    
    // Congestion control: не TCP-style AIMD
    // Static window для co-location (известная bandwidth)
    // Adaptive window для cloud/WAN
};
```

---

## Часть 4: Benchmarking Target

### 4.1 Целевые показатели

| Метрика | Aeron (Java, open source) | Aeron Premium | ZigBolt Target |
|---|---|---|---|
| IPC RTT (p50) | 250 нс | ~200 нс | **<100 нс** |
| IPC RTT (p99) | ~1 мкс | ~500 нс | **<200 нс** |
| IPC RTT (p99.9) | ~5 мкс | ~1 мкс | **<500 нс** |
| UDP RTT (p50) | ~10 мкс | ~5 мкс | **<3 мкс** (io_uring) |
| UDP RTT (DPDK, p50) | N/A (Premium) | ~2 мкс | **<1 мкс** |
| Throughput (msg/sec) | 20M+ | 20M+ | **50M+** |
| Startup time | 2-5 сек (JVM) | 2-5 сек | **<10 мс** |
| Binary size | ~50MB (JVM + JARs) | ~50MB | **<2 MB** |
| Memory footprint | ~200MB (JVM heap) | ~200MB | **<10 MB** (+ buffers) |

### 4.2 Benchmark Suite

```zig
// Встроенный в проект benchmark framework
pub const Benchmark = struct {
    // HDR Histogram для accurate percentile measurement
    histogram: HdrHistogram,
    
    pub fn pingPong(config: BenchConfig) !BenchResult {
        // Publisher sends → Subscriber echoes → measure RTT
        // 100K messages, 32 bytes each
        // Warm-up: 10K messages (discarded)
        // Results: p50, p99, p99.9, p99.99, max
    }
    
    pub fn throughput(config: BenchConfig) !BenchResult {
        // Streaming publisher → subscriber measures rate
        // 10M+ messages
        // Results: msg/sec, bytes/sec, CPU utilization
    }
    
    pub fn jitter(config: BenchConfig) !BenchResult {
        // Measure inter-message arrival time variance
        // Critical для HFT: jitter > latency
    }
};
```

---

## Часть 5: Интеграция с вашим стеком

### 5.1 StockAPIs.com Integration

```
100+ бирж ──WS──▶ Collector Fleet (Rust)
                        │
              ZigBolt Transport (IPC)   ← замена Kafka hot path
                        │
            ┌───────────┼───────────┐
            ▼           ▼           ▼
    Redis (hot)   QuestDB (ts)   ZigBolt Archive → Parquet (cold)
            │           │
            └─────┬─────┘
                  ▼
         gRPC API + Centrifugo WS
```

### 5.2 Marketmaker.cc Scalping Terminal

```
Exchange ──WS/FIX──▶ Gateway (Rust + ZigBolt FFI)
                          │
                    ZigBolt IPC (shared memory, <100 нс)
                          │
                    Strategy Engine (Zig/Rust)
                          │
                    ZigBolt IPC
                          │
                    Order Router → Exchange
                          │
                    ZigBolt Archive → QuestDB (post-trade)
```

### 5.3 Rust/Python FFI

```zig
// C ABI export для Rust/Python интеграции
export fn zigbolt_transport_create(config: *const TransportConfig) ?*Transport {
    return Transport.init(config.*) catch null;
}

export fn zigbolt_publish(pub_handle: *Publisher, data: [*]const u8, len: usize) i32 {
    return pub_handle.offer(data[0..len]) catch return -1;
}

export fn zigbolt_poll(sub_handle: *Subscriber, handler: FragmentHandlerFn, limit: u32) u32 {
    return sub_handle.poll(handler, limit);
}
```

Rust usage:
```rust
// В Rust: extern "C" binding
extern "C" {
    fn zigbolt_transport_create(config: *const TransportConfig) -> *mut Transport;
    fn zigbolt_publish(pub_h: *mut Publisher, data: *const u8, len: usize) -> i32;
}
```

Python usage (через ctypes или cffi):
```python
import ctypes
lib = ctypes.CDLL("libzigbolt.so")
transport = lib.zigbolt_transport_create(config)
```

---

## Часть 6: Development Roadmap

### Phase 1: Transport Core (2-3 месяца)
- [ ] SPSC/MPSC Ring Buffers
- [ ] Log Buffer (triple-buffered terms)
- [ ] IPC Channel (shared memory)
- [ ] Comptime Wire Codec
- [ ] Basic Publisher/Subscriber API
- [ ] Ping-pong benchmark
- **Target:** IPC RTT < 200 нс, beat Aeron open-source

### Phase 2: Network Layer (2-3 месяца)
- [ ] UDP unicast/multicast
- [ ] io_uring backend
- [ ] NAK-based reliability
- [ ] Flow control
- [ ] Fragmentation/reassembly
- **Target:** UDP RTT < 5 мкс (io_uring), throughput > 20M msg/sec

### Phase 3: Archive (1-2 месяца)
- [ ] Segment-based recording (io_uring writes)
- [ ] Replay with position tracking
- [ ] Compression (LZ4)
- [ ] Parquet export
- [ ] QuestDB ILP streaming
- **Target:** Record at line rate, replay без stalls

### Phase 4: Cluster (2-3 месяца)
- [ ] Raft leader election
- [ ] Log replication
- [ ] State Machine interface
- [ ] Snapshot/restore
- [ ] Multi-Raft
- **Target:** Committed entry latency < 100 мкс (same-rack)

### Phase 5: Sequencer + Kernel Bypass (2-3 месяца)
- [ ] Total ordering sequencer
- [ ] DPDK backend
- [ ] AF_XDP backend
- [ ] Multi-stream sequencer
- **Target:** Sequenced event latency < 50 мкс, DPDK RTT < 1 мкс

### Phase 6: Production Hardening (ongoing)
- [ ] Fault injection testing
- [ ] Chaos engineering framework
- [ ] Monitoring/metrics (Prometheus compatible)
- [ ] Documentation + examples
- [ ] Rust/Python/C bindings
- [ ] Package manager (Zig package + C shared library)

---

## Часть 7: Naming & Branding

### Варианты имени

| Имя | Почему | Домен |
|---|---|---|
| **ZigBolt** | Zig + молния (speed) | zigbolt.io |
| **NanoMQ** | Наносекунды + MQ | nanomq.dev (занят) |
| **FlashWire** | Flash trading + wire protocol | flashwire.io |
| **LightLink** | Speed of light + link | lightlink.dev |
| **VoltStream** | Voltage/energy + streaming | voltstream.io |
| **PicoTransport** | Picosecond ambition | picotransport.dev |

---

## Часть 8: Конкурентный ландшафт

| Система | Язык | IPC Latency | Network | Kernel Bypass | License |
|---|---|---|---|---|---|
| **Aeron** | Java/C | 250 нс | UDP | Premium ($$) | Apache 2.0 |
| **Chronicle Queue** | Java | 300 нс | TCP only | Нет | LGPL/Commercial |
| **ZeroMQ** | C | 1-5 мкс | TCP/IPC | Нет | LGPL |
| **LMAX Disruptor** | Java | 50-100 нс | N/A (IPC only) | N/A | Apache 2.0 |
| **DPDK** | C | N/A | UDP/raw | Да (native) | BSD |
| **ZigBolt** | Zig | **<100 нс** | UDP/io_uring/DPDK | **Да (native)** | MIT |

**Уникальная позиция ZigBolt:** единственная система, которая комбинирует:
1. Sub-100ns IPC (уровень LMAX Disruptor)
2. Reliable UDP networking (уровень Aeron)
3. Native kernel bypass (уровень DPDK)
4. Comptime wire codecs (лучше SBE)
5. Нет JVM/GC overhead
6. Cluster/Sequencer для HA
7. Всё это в одном unified package

---

## Приложение A: Ключевые Linux System Calls

| Syscall | Зачем |
|---|---|
| `mmap(MAP_HUGETLB \| MAP_LOCKED)` | Shared memory с hugepages |
| `mbind(MPOL_BIND)` | NUMA-aware memory |
| `sched_setaffinity` | CPU pinning |
| `sched_setscheduler(SCHED_FIFO)` | Real-time priority |
| `io_uring_setup/enter/register` | Async I/O |
| `mlockall(MCL_CURRENT \| MCL_FUTURE)` | Prevent swapping |
| `madvise(MADV_HUGEPAGE)` | Transparent hugepages |
| `perf_event_open` | Hardware performance counters |
| `rdtsc` (inline asm) | Nanosecond timestamps |

## Приложение B: Hardware Requirements

### Минимум (development/testing)
- Linux x86_64 (kernel 5.15+)
- 4 cores, 16GB RAM
- NVMe SSD

### Рекомендуемый (production)
- Linux x86_64 (kernel 6.1+ LTS, лучше 6.6+)
- 8+ dedicated cores (isolcpus)
- 64GB+ RAM, hugepages configured
- NVMe SSD (Samsung 990 Pro или лучше)
- 10Gbps NIC (Intel E810 / Mellanox ConnectX-6)

### Co-location (HFT)
- Linux x86_64 (kernel 6.6+, RT patch)
- 16+ cores, turbo boost disabled (стабильная частота)
- 128GB+ RAM, 1GB hugepages
- Solarflare X2522 / Xilinx Alveo (ef_vi support)
- FPGA NIC для hardware timestamping
- Direct connection to exchange matching engine

## Приложение C: Лицензирование

MIT License — максимальная свобода для коммерческого использования. Отсутствие copyleft позволяет интеграцию в проприетарные trading системы.

---

*Спецификация подготовлена для Reforms.AI / Marketmaker.cc / StockAPIs.com*
*Версия: 0.1-draft*
*Дата: 2026-03-11*
