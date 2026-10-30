#ifndef LSM_ENGINE_H
#define LSM_ENGINE_H

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <stdatomic.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>

#define LSM_MAX_LEVELS 7
#define LSM_SKIPLIST_MAX_HEIGHT 32
#define LSM_BLOCK_SIZE 4096
#define LSM_MAGIC 0x4C534D454E47494EULL /* "LSMENGIN" */
#define LSM_MAX_PATH 1024

typedef enum {
    LSM_OK = 0,
    LSM_ERR_IO = -1,
    LSM_ERR_CORRUPT = -2,
    LSM_ERR_NOT_FOUND = -3,
    LSM_ERR_NOMEM = -4,
    LSM_ERR_INVALID = -5,
    LSM_ERR_BUSY = -6
} lsm_status_t;

typedef struct {
    uint32_t crc32;
    uint32_t payload_len;
    uint64_t lsn;
} __attribute__((packed)) lsm_wal_header_t;

typedef struct lsm_slice {
    uint8_t *data;
    size_t size;
} lsm_slice_t;

typedef struct lsm_skip_node {
    _Atomic(uint64_t) marked_next; /* Bit 63: deleted flag, Lower 63: pointer to next node */
    uint32_t key_len;
    uint32_t val_len;
    uint8_t height;
    /* Flexible array member containing key bytes followed by value bytes */
    uint8_t kv[];
} lsm_skip_node_t;

typedef struct {
    _Atomic(uint64_t) head; /* Pointer packed with height or metadata */
    _Atomic(int32_t) max_height;
    size_t count;
} lsm_memtable_t;

typedef struct {
    uint64_t file_number;
    uint32_t level;
    uint8_t smallest_key[256];
    uint8_t largest_key[256];
    size_t smallest_len;
    size_t largest_len;
    char path[LSM_MAX_PATH];
} lsm_sstable_meta_t;

typedef struct {
    lsm_sstable_meta_t *tables[LSM_MAX_LEVELS];
    size_t table_counts[LSM_MAX_LEVELS];
    pthread_rwlock_t rwlock;
} lsm_version_t;

typedef struct {
    char db_path[LSM_MAX_PATH];
    int wal_fd;
    uint64_t next_lsn;
    uint64_t next_file_num;
    lsm_memtable_t *memtable;
    lsm_memtable_t *imm_memtable;
    pthread_rwlock_t db_lock;
    pthread_mutex_t wal_mutex;
    pthread_mutex_t flush_mutex;
    pthread_t bg_flush_thread;
    bool shutting_down;
    lsm_version_t version;
} lsm_db_t;

/* Core API Declarations */
lsm_status_t lsm_open(const char *path, lsm_db_t **db_out);
void lsm_close(lsm_db_t *db);
lsm_status_t lsm_put(lsm_db_t *db, const lsm_slice_t *key, const lsm_slice_t *val);
lsm_status_t lsm_get(lsm_db_t *db, const lsm_slice_t *key, lsm_slice_t *val_out);
lsm_status_t lsm_delete(lsm_db_t *db, const lsm_slice_t *key);

/* Internal utility declarations */
uint32_t lsm_crc32(const uint8_t *data, size_t length);
lsm_memtable_t *lsm_memtable_create(void);
void lsm_memtable_destroy(lsm_memtable_t *mem);
bool lsm_memtable_get(lsm_memtable_t *mem, const lsm_slice_t *key, lsm_slice_t *val_out);
lsm_status_t lsm_memtable_put(lsm_memtable_t *mem, const lsm_slice_t *key, const lsm_slice_t *val, uint64_t lsn);

#endif /* LSM_ENGINE_H */

#ifndef LSM_ENGINE_IMPL
#define LSM_ENGINE_IMPL

/* CRC32 standard IEEE 802.3 implementation */
uint32_t lsm_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            if (crc & 1) {
                crc = (crc >> 1) ^ 0xEDB88320;
            } else {
                crc = crc >> 1;
            }
        }
    }
    return ~crc;
}

lsm_memtable_t *lsm_memtable_create(void) {
    lsm_memtable_t *mem = (lsm_memtable_t *)malloc(sizeof(lsm_memtable_t));
    if (!mem) return NULL;
    atomic_init(&mem->max_height, 1);
    mem->count = 0;
    
    lsm_skip_node_t *head = (lsm_skip_node_t *)malloc(sizeof(lsm_skip_node_t) + 0);
    if (!head) {
        free(mem);
        return NULL;
    }
    head->key_len = 0;
    head->val_len = 0;
    head->height = LSM_SKIPLIST_MAX_HEIGHT;
    atomic_init(&head->marked_next, 0);
    
    atomic_init(&mem->head, (uintptr_t)head);
    return mem;
}

void lsm_memtable_destroy(lsm_memtable_t *mem) {
    if (!mem) return;
    lsm_skip_node_t *curr = (lsm_skip_node_t *)atomic_load(&mem->head);
    while (curr) {
        uint64_t next_val = atomic_load(&curr->marked_next);
        lsm_skip_node_t *next = (lsm_skip_node_t *)(next_val & 0x7FFFFFFFFFFFFFFFULL);
        free(curr);
        curr = next;
    }
    free(mem);
}

static int lsm_key_compare(const uint8_t *a_data, size_t a_len, const uint8_t *b_data, size_t b_len) {
    size_t min_len = a_len < b_len ? a_len : b_len;
    int cmp = memcmp(a_data, b_data, min_len);
    if (cmp != 0) return cmp;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return 1;
    return 0;
}

bool lsm_memtable_get(lsm_memtable_t *mem, const lsm_slice_t *key, lsm_slice_t *val_out) {
    lsm_skip_node_t *curr = (lsm_skip_node_t *)atomic_load(&mem->head);
    
    /* Simple linear-scan traversal fallback for single-level skip list baseline */
    uint64_t next_val = atomic_load(&curr->marked_next);
    curr = (lsm_skip_node_t *)(next_val & 0x7FFFFFFFFFFFFFFFULL);
    
    while (curr) {
        uint8_t *node_key = curr->kv;
        uint8_t *node_val = curr->kv + curr->key_len;
        int cmp = lsm_key_compare(key->data, key->size, node_key, curr->key_len);
        if (cmp == 0) {
            uint64_t marked = atomic_load(&curr->marked_next);
            if (marked & 0x8000000000000000ULL) {
                return false; /* Tombstone */
            }
            val_out->data = (uint8_t *)malloc(curr->val_len);
            if (!val_out->data) return false;
            memcpy(val_out->data, node_val, curr->val_len);
            val_out->size = curr->val_len;
            return true;
        }
        uint64_t n_val = atomic_load(&curr->marked_next);
        curr = (lsm_skip_node_t *)(n_val & 0x7FFFFFFFFFFFFFFFULL);
    }
    return false;
}

lsm_status_t lsm_memtable_put(lsm_memtable_t *mem, const lsm_slice_t *key, const lsm_slice_t *val, uint64_t lsn) {
    (void)lsn;
    size_t node_size = sizeof(lsm_skip_node_t) + key->size + val->size;
    lsm_skip_node_t *new_node = (lsm_skip_node_t *)malloc(node_size);
    if (!new_node) return LSM_ERR_NOMEM;
    
    new_node->key_len = (uint32_t)key->size;
    new_node->val_len = (uint32_t)val->size;
    new_node->height = 1;
    atomic_init(&new_node->marked_next, 0);
    
    memcpy(new_node->kv, key->data, key->size);
    memcpy(new_node->kv + key->size, val->data, val->size);
    
    lsm_skip_node_t *head = (lsm_skip_node_t *)atomic_load(&mem->head);
    
    while (true) {
        uint64_t old_next = atomic_load(&head->marked_next);
        new_node->marked_next = old_next & 0x7FFFFFFFFFFFFFFFULL;
        if (atomic_compare_exchange_weak(&head->marked_next, &old_next, (uint64_t)new_node)) {
            mem->count++;
            break;
        }
    }
    return LSM_OK;
}

static lsm_status_t lsm_recover_wal(lsm_db_t *db) {
    char wal_path[LSM_MAX_PATH];
    snprintf(wal_path, sizeof(wal_path), "%s/current.wal", db->db_path);
    
    int fd = open(wal_path, O_RDWR | O_CREAT, 0644);
    if (fd < 0) return LSM_ERR_IO;
    db->wal_fd = fd;
    
    while (true) {
        lsm_wal_header_t hdr;
        ssize_t n = read(fd, &hdr, sizeof(hdr));
        if (n == 0) break;
        if (n < (ssize_t)sizeof(hdr)) {
            /* Torn write truncation point */
            off_t curr = lseek(fd, -n, SEEK_CUR);
            ftruncate(fd, curr);
            break;
        }
        
        uint8_t *payload = (uint8_t *)malloc(hdr.payload_len);
        if (!payload) {
            close(fd);
            return LSM_ERR_NOMEM;
        }
        
        n = read(fd, payload, hdr.payload_len);
        if (n < (ssize_t)hdr.payload_len) {
            free(payload);
            off_t curr = lseek(fd, -(sizeof(hdr) + n), SEEK_CUR);
            ftruncate(fd, curr);
            break;
        }
        
        uint32_t computed_crc = lsm_crc32(payload, hdr.payload_len);
        if (computed_crc != hdr.crc32) {
            free(payload);
            off_t curr = lseek(fd, -(sizeof(hdr) + hdr.payload_len), SEEK_CUR);
            ftruncate(fd, curr);
            break;
        }
        
        /* Replay entry */
        uint32_t key_len = *(uint32_t *)payload;
        lsm_slice_t key = { .data = payload + 8, .size = key_len };
        lsm_slice_t val = { .data = payload + 8 + key_len, .size = hdr.payload_len - 8 - key_len };
        
        lsm_memtable_put(db->memtable, &key, &val, hdr.lsn);
        db->next_lsn = hdr.lsn + 1;
        free(payload);
    }
    return LSM_OK;
}

lsm_status_t lsm_open(const char *path, lsm_db_t **db_out) {
    if (!path || !db_out) return LSM_ERR_INVALID;
    
    lsm_db_t *db = (lsm_db_t *)malloc(sizeof(lsm_db_t));
    if (!db) return LSM_ERR_NOMEM;
    
    memset(db, 0, sizeof(lsm_db_t));
    strncpy(db->db_path, path, sizeof(db->db_path) - 1);
    
    mkdir(path, 0755);
    
    pthread_rwlock_init(&db->db_lock, NULL);
    pthread_mutex_init(&db->wal_mutex, NULL);
    pthread_mutex_init(&db->flush_mutex, NULL);
    pthread_rwlock_init(&db->version.rwlock, NULL);
    
    db->memtable = lsm_memtable_create();
    db->imm_memtable = NULL;
    db->next_lsn = 1;
    db->next_file_num = 1;
    db->shutting_down = false;
    
    if (lsm_recover_wal(db) != LSM_OK) {
        lsm_close(db);
        return LSM_ERR_IO;
    }
    
    *db_out = db;
    return LSM_OK;
}

void lsm_close(lsm_db_t *db) {
    if (!db) return;
    db->shutting_down = true;
    
    if (db->wal_fd >= 0) {
        fsync(db->wal_fd);
        close(db->wal_fd);
    }
    
    if (db->memtable) lsm_memtable_destroy(db->memtable);
    if (db->imm_memtable) lsm_memtable_destroy(db->imm_memtable);
    
    pthread_rwlock_destroy(&db->db_lock);
    pthread_mutex_destroy(&db->wal_mutex);
    pthread_mutex_destroy(&db->flush_mutex);
    pthread_rwlock_destroy(&db->version.rwlock);
    
    free(db);
}

lsm_status_t lsm_put(lsm_db_t *db, const lsm_slice_t *key, const lsm_slice_t *val) {
    if (!db || !key || !key->data || !val || !val->data) return LSM_ERR_INVALID;
    
    pthread_mutex_lock