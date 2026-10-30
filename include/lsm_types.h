#ifndef LSM_TYPES_H
#define LSM_TYPES_H

#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

/* -------------------------------------------------------------------------
 * Error Codes & Status
 * ------------------------------------------------------------------------- */
typedef enum {
    LSM_OK = 0,
    LSM_ERR_IO = -1,
    LSM_ERR_CORRUPTION = -2,
    LSM_ERR_NOT_FOUND = -3,
    LSM_ERR_INVALID_ARGUMENT = -4,
    LSM_ERR_OUT_OF_MEMORY = -5,
    LSM_ERR_BUSY = -6,
    LSM_ERR_IO_OVERFLOW = -7
} lsm_status_t;

/* -------------------------------------------------------------------------
 * Basic Slices and Identifiers
 * ------------------------------------------------------------------------- */
typedef struct {
    const char *data;
    size_t size;
} lsm_slice_t;

typedef struct {
    char *data;
    size_t size;
} lsm_mutable_slice_t;

typedef uint64_t lsm_lsn_t;
typedef uint64_t lsm_timestamp_t;
typedef uint64_t lsm_sequence_t;

/* -------------------------------------------------------------------------
 * WAL (Write-Ahead Log) Frame Layout
 * 16-byte fixed header: [CRC32:4] [Payload_Len:4] [LSN:8] [Payload]
 * ------------------------------------------------------------------------- */
#define LSM_WAL_HEADER_SIZE 16

typedef struct __attribute__((__packed__)) {
    uint32_t crc32;
    uint32_t payload_len;
    uint64_t lsn;
} lsm_wal_header_t;

/* -------------------------------------------------------------------------
 * MemTable Structures (Lock-free Skip-List with Hazard Pointers)
 * ------------------------------------------------------------------------- */
#define LSM_SKIPLIST_MAX_HEIGHT 32

typedef struct lsm_memtable_node {
    lsm_lsn_t lsn;
    uint8_t type; /* 0 = Put, 1 = Delete */
    uint16_t height;
    _Atomic(uint32_t) ref_count;
    /* Hazard pointer atomic next pointers per level */
    _Atomic(struct lsm_memtable_node *) forward[LSM_SKIPLIST_MAX_HEIGHT];
    /* Flexible array member for contiguous key-value storage */
    uint32_t key_len;
    uint32_t val_len;
    char kv_data[];
} lsm_memtable_node_t;

typedef struct {
    _Atomic(lsm_memtable_node_t *) head;
    _Atomic(uint32_t) height;
    size_t byte_size;
    _Atomic(uint64_t) count;
} lsm_memtable_t;

/* Hazard Pointer Record for Thread-Safe Lock-Free Reads */
typedef struct lsm_hazmat_record {
    _Atomic(lsm_memtable_node_t *) protected_node;
    _Atomic(bool) active;
    struct lsm_hazmat_record *next;
} lsm_hazmat_record_t;

typedef struct {
    _Atomic(lsm_hazmat_record_t *) head;
} lsm_hazmat_domain_t;

/* -------------------------------------------------------------------------
 * SSTable (Sorted String Table) Metadata & Indexing
 * ------------------------------------------------------------------------- */
#define LSM_SSTABLE_MAGIC 0x4C534D535354544CULL /* "LSM_SSTL" */
#define LSM_SSTABLE_FOOTER_SIZE 48

typedef struct {
    uint64_t index_offset;
    uint64_t index_size;
    uint64_t bloom_offset;
    uint64_t bloom_size;
    uint32_t checksum;
    uint64_t magic;
} __attribute__((__packed__)) lsm_sstable_footer_t;

typedef struct {
    lsm_slice_t key;
    uint64_t block_offset;
    uint64_t block_size;
} lsm_index_entry_t;

typedef struct {
    uint32_t num_entries;
    lsm_index_entry_t *entries;
} lsm_index_block_t;

/* Dynamic Block-Level Bloom Filter */
typedef struct {
    uint32_t *bitset;
    uint32_t bitset_size_bytes;
    uint32_t num_hashes;
    uint32_t num_keys;
} lsm_bloom_filter_t;

/* -------------------------------------------------------------------------
 * Arena Allocator (Massive Huge-Pages Backed Bump Allocator)
 * ------------------------------------------------------------------------- */
typedef struct lsm_arena_chunk {
    char *memory;
    size_t capacity;
    size_t offset;
    struct lsm_arena_chunk *next;
} lsm_arena_chunk_t;

typedef struct {
    size_t chunk_size;
    lsm_arena_chunk_t *current_chunk;
    pthread_spinlock_t lock;
    size_t total_allocated;
} lsm_arena_t;

/* -------------------------------------------------------------------------
 * Zero-Copy Block Cache (LRU using mmap reference-counted blocks)
 * ------------------------------------------------------------------------- */
typedef struct lsm_cache_entry {
    char *data;
    size_t size;
    uint64_t file_id;
    uint64_t block_offset;
    _Atomic(int32_t) ref_count;
    bool compressed;
    struct lsm_cache_entry *prev;
    struct lsm_cache_entry *next;
} lsm_cache_entry_t;

typedef struct {
    size_t capacity_bytes;
    size_t current_bytes;
    size_t num_entries;
    lsm_cache_entry_t *lru_head;
    lsm_cache_entry_t *lru_tail;
    pthread_rwlock_t rwlock;
} lsm_block_cache_t;

/* -------------------------------------------------------------------------
 * Compaction & Level Metadata Structures
 * ------------------------------------------------------------------------- */
#define LSM_MAX_LEVELS 7

typedef struct {
    uint64_t file_id;
    lsm_slice_t smallest_key;
    lsm_slice_t largest_key;
    uint64_t file_size;
    int fd;
    char *filename;
} lsm_sstable_meta_t;

typedef struct {
    int num_files;
    lsm_sstable_meta_t **files;
    pthread_rwlock_t rwlock;
} lsm_level_t;

typedef struct {
    lsm_level_t levels[LSM_MAX_LEVELS];
    uint64_t next_file_id;
    pthread_rwlock_t manifest_lock;
} lsm_version_set_t;

/* -------------------------------------------------------------------------
 * Engine Core Handle
 * ------------------------------------------------------------------------- */
typedef struct {
    char *db_path;
    bool is_open;
    lsm_memtable_t *memtable;
    lsm_memtable_t *imm_memtable; /* Immutable memtable undergoing flush */
    lsm_hazmat_domain_t hazard_domain;
    lsm_arena_t arena;
    lsm_block_cache_t block_cache;
    lsm_version_set_t version_set;
    
    int wal_fd;
    char *wal_path;
    lsm_lsn_t current_lsn;
    pthread_mutex_t wal_mutex;
    
    pthread_t background_flush_thread;
    pthread_t background_compaction_thread;
    bool shutdown_requested;
    pthread_cond_t background_cond;
    pthread_mutex_t background_mutex;
} lsm_engine_t;

#ifdef __cplusplus
}
#endif

#endif /* LSM_TYPES_H */