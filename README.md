# lsm-engine-c
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/lsm-engine-c/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/lsm-engine-c/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/lsm-engine-c) [![Stars](https://img.shields.io/github/stars/sohamshewani/lsm-engine-c?style=flat)](https://github.com/sohamshewani/lsm-engine-c)

## Executive Summary & Problem Space

`lsm-engine-c` is a high-throughput, crash-resilient, embedded Log-Structured Merge-tree (LSM) key-value storage engine engineered in C99. Modern enterprise infrastructure—ranging from distributed transactional metadata stores to real-time analytics stream processors—demands storage engines capable of sustaining high write-amplification bounds while bypassing the random-I/O performance cliffs inherent to traditional B-Trees. 

Traditional in-place update architectures bottleneck on disk seek latencies, thread contention, and synchronous page flushing. `lsm-engine-c` resolves these bottlenecks by converting random disk writes into sequential append-only operations. It decouples high-frequency memory mutations from persistent storage through asynchronous background compaction pipelines, strictly enforced memory barriers, and lock-free staging memtables. Designed for bare-metal performance with zero garbage collection pauses and deterministic memory footprints, `lsm-engine-c` serves as a foundational storage kernel for systems requiring predictable sub-millisecond latencies under high concurrency.

## Structural ASCII Architecture / Dataflow Diagram

The system architecture partitions operations across high-speed volatile staging structures and immutable persistent tiers, managed via strict synchronization boundaries.

```
                              +---------------------------------------+
                              |              Client Thread            |
                              +---------------------------------------+
                                        |                   |
                                   [Put(k,v)]           [Get(k)]
                                        |                   |
                                        v                   v
+---------------------------------------------------------------------------------------+
|                                  lsm_engine (Core)                                    |
|                                                                                       |
|   +--------------------------+                         +--------------------------+   |
|   |   Write-Ahead Log (WAL)  |                         |    Active Memtable       |   |
|   |  (src/lsm_wal.c)         |                         |  (src/lsm_memtable.c)    |   |
|   |  Append-only persistence |                         |  Concurrent SkipList     |   |
|   +--------------------------+                         +--------------------------+   |
|                                                                     |                 |
|                                                            [Memtable Threshold Reached|
|                                                                     |                 |
|                                                                     v                 |
|                                                        +--------------------------+   |
|                                                        |   Immutable Memtable     |   |
|                                                        +--------------------------+   |
|                                                                     |                 |
|                                                       [Background Flush Worker]       |
+---------------------------------------------------------------------|-----------------+
                                                                      |
                                                                      v
                                                         +--------------------------+
                                                         |      SSTable Tier 0      |
                                                         |  (src/lsm_sstable.c)     |
                                                         |  Immutable Data Blocks   |
                                                         +--------------------------+
                                                                      |
                                                     [Background Compaction Thread]
                                                     (src/lsm_compaction.c)
                                                                      |
                                                                      v
                                                         +--------------------------+
                                                         |      SSTable Tier N      |
                                                         |  Leveled Merged Sorting  |
                                                         +--------------------------+
```

## Concurrency Model & Memory Invariants

`lsm-engine-c` employs a sophisticated concurrency model designed to maximize multi-core throughput while guaranteeing strict serializability of mutations and snapshot isolation for reads.

*   **Thread Safety & Lock Contention Prevention:** Write operations bypass heavy global locks by staging updates directly into a thread-safe active memtable protected by fine-grained read-write latches and atomic state pointers. Read operations utilize lock-free pointer acquisition paths, traversing immutable snapshots without blocking incoming write streams.
*   **Memory Ownership & Lifetimes:** Explicit lifecycle boundaries govern all data structures. Once a memtable reaches its capacity threshold, its ownership pointer is atomically swapped to an immutable status, transferring responsibility entirely to the background flush pipeline. Callers never retain ownership of keys or values passed into write interfaces; payloads are deep-copied into arenas managed by the active memtable.
*   **Atomic Primitives & Memory Barriers:** C11 atomic operations (`stdatomic.h`) are utilized for state transitions, sequence number generation, and compaction handoffs. Explicit memory barriers (`memory_order_acquire` / `memory_order_release`) guarantee visibility of memtable payloads across thread boundaries prior to WAL fsync completion.

## Performance Characteristics & Algorithmic Complexity Table

The engine balances write ingestion speeds against read amplification profiles across its architectural strata.

| Operation | Average Case Complexity | Worst Case Complexity | Latency Target (p99) | Bottleneck / Mitigation |
| :--- | :--- | :--- | :--- | :--- |
| **Point Read (`Get`)** | $O(1)$ amortized | $O(\log N)$ | $< 250\mu s$ | Bloom filter checks / Cache locality |
| **Write (`Put`/`Delete`)** | $O(1)$ amortized | $O(\log N)$ | $< 50\mu s$ | Sequential WAL append / Memtable insertion |
| **Range Scan** | $O(\log N + K)$ | $O(N)$ | $< 1ms$ (per 100 keys) | Iterator multi-way merge heap |
| **Compaction (Tiered)** | $O(N)$ background | $O(N \log N)$ | Background execution | I/O throttling / Dual-buffer streaming |

## Security Architecture & Threat Model

Designed to operate within hardened enterprise environments, `lsm-engine-c` incorporates strict defensive programming paradigms:

*   **Input Boundaries & Sanitization:** All externally supplied keys and values undergo rigorous bounds checking (`include/lsm_types.h`) to prevent integer overflows during dynamic allocation sizing and offset arithmetic.
*   **Memory Corruption Protections:** Buffer overruns are mitigated through explicit size-enforced memory copies (`memcpy_s` patterns and strict bounds validation). Raw pointer arithmetic within SSTable parsers is bound by immutable file size limits to prevent out-of-bounds reading.
*   **Threat Vectors & Mitigations:** 
    *   *Denial of Service (Disk Exhaustion):* Controlled via strict memtable ceilings and compaction backpressure signaling.
    *   *Crash Inconsistency:* Mitigated via synchronous WAL journaling (`src/lsm_wal.c`) with checksum verification on every block header.
    *   *Side-Channel / Memory Leaks:* Comprehensive resource tracking ensures zero memory leaks during abrupt engine teardowns or panic conditions.

## Build, Verification & Benchmarking Recipes

The project utilizes a deterministic POSIX-compliant Makefile with strict warning flags (`-Wall -Wextra -Werror -std=c99 -O3 -pthread`).

### 1. Compile the Library and Binaries
```bash
make clean && make all
```

### 2. Execute Invariant Verification Tests
Run the rigorous test suite validating concurrency correctness, crash recovery invariants, and compaction stability:
```bash
make test
```
*Alternatively, execute the compiled test binary directly:*
```bash
./tests/test_invariants
```

### 3. Run Micro-benchmarks
Measure throughput, latency distribution, and IOPS characteristics under simulated heavy workloads:
```bash
make bench
```
*Direct execution option:*
```bash
./benchmarks/bench_engine --threads=8 --ops=1000000