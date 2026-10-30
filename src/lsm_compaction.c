#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/mman.h>

#define LSM_MAX_PATH 1024
#define LSM_MAX_LEVELS 7
#define LSM_BLOCK_SIZE 4096
#define LSM_MAGIC 0x4C534D4454424C53ULL // "LSMDTBLS"

typedef enum {
    LSM_OK = 0,
    LSM_ERROR_IO = -1,
    LSM_ERROR_NOMEM = -2,
    LSM_ERROR_CORRUPTION = -3,
    LSM_ERROR_INVALID = -4
} lsm_status_t;

typedef struct {
    uint32_t crc32;
    uint32_t payload_len;
    uint64_t lsn;
} __attribute__((packed)) lsm_wal_header_t;

typedef struct {
    uint8_t *data;
    size_t size;
    size_t capacity;
} lsm_buffer_t;

typedef struct {
    char key[256];
    uint16_t key_len;
    uint64_t file_size;
    uint64_t index_offset;
    uint64_t seq_num;
    bool is_tombstone;
} lsm_sstable_meta_t;

typedef struct {
    lsm_sstable_meta_t *tables;
    size_t count;
    size_t capacity;
} lsm_level_t;

typedef struct {
    lsm_level_t levels[LSM_MAX_LEVELS];
    pthread_rwlock_t rwlock;
    char base_dir[LSM_MAX_PATH];
} lsm_version_t;

typedef struct {
    int level;
    lsm_sstable_meta_t *inputs;
    size_t input_count;
    char staging_path[LSM_MAX_PATH];
    char final_path[LSM_MAX_PATH];
} lsm_compaction_job_t;

typedef struct {
    char key[256];
    uint16_t key_len;
    uint8_t *value;
    uint16_t val_len;
    uint64_t seq_num;
    bool is_tombstone;
} lsm_entry_t;

static uint32_t lsm_crc32(const uint8_t *data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }
    return ~crc;
}

static int lsm_entry_compare(const void *a, const void *b) {
    const lsm_entry_t *ea = (const lsm_entry_t *)a;
    const lsm_entry_t *eb = (const lsm_entry_t *)b;
    size_t min_len = ea->key_len < eb->key_len ? ea->key_len : eb->key_len;
    int cmp = memcmp(ea->key, eb->key, min_len);
    if (cmp != 0) return cmp;
    if (ea->key_len < eb->key_len) return -1;
    if (ea->key_len > eb->key_len) return 1;
    if (ea->seq_num > eb->seq_num) return -1;
    if (ea->seq_num < eb->seq_num) return 1;
    return 0;
}

static lsm_status_t lsm_read_sstable_entries(const char *path, lsm_entry_t **out_entries, size_t *out_count) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) return LSM_ERROR_IO;

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return LSM_ERROR_IO;
    }

    if (st.st_size < 32) {
        close(fd);
        return LSM_ERROR_CORRUPTION;
    }

    void *map = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (map == MAP_FAILED) return LSM_ERROR_IO;

    uint64_t magic;
    memcpy(&magic, (uint8_t *)map + st.st_size - sizeof(uint64_t), sizeof(uint64_t));
    if (magic != LSM_MAGIC) {
        munmap(map, st.st_size);
        return LSM_ERROR_CORRUPTION;
    }

    uint64_t index_offset;
    memcpy(&index_offset, (uint8_t *)map + st.st_size - sizeof(uint64_t) - sizeof(uint64_t), sizeof(uint64_t));

    uint8_t *ptr = (uint8_t *)map + index_offset;
    uint32_t entry_count;
    memcpy(&entry_count, ptr, sizeof(uint32_t));
    ptr += sizeof(uint32_t);

    lsm_entry_t *entries = malloc(entry_count * sizeof(lsm_entry_t));
    if (!entries) {
        munmap(map, st.st_size);
        return LSM_ERROR_NOMEM;
    }

    for (size_t i = 0; i < entry_count; i++) {
        uint32_t k_len, v_len;
        uint64_t seq;
        uint8_t tomb;
        
        memcpy(&k_len, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
        memcpy(&v_len, ptr, sizeof(uint32_t)); ptr += sizeof(uint32_t);
        memcpy(&seq, ptr, sizeof(uint64_t)); ptr += sizeof(uint64_t);
        memcpy(&tomb, ptr, sizeof(uint8_t)); ptr += sizeof(uint8_t);

        if (k_len > 255) {
            free(entries);
            munmap(map, st.st_size);
            return LSM_ERROR_CORRUPTION;
        }

        memcpy(entries[i].key, ptr, k_len);
        entries[i].key[k_len] = '\0';
        entries[i].key_len = (uint16_t)k_len;
        ptr += k_len;

        entries[i].value = malloc(v_len);
        if (!entries[i].value) {
            for (size_t j = 0; j < i; j++) free(entries[j].value);
            free(entries);
            munmap(map, st.st_size);
            return LSM_ERROR_NOMEM;
        }
        memcpy(entries[i].value, ptr, v_len);
        entries[i].val_len = (uint16_t)v_len;
        ptr += v_len;

        entries[i].seq_num = seq;
        entries[i].is_tombstone = (tomb != 0);
    }

    munmap(map, st.st_size);
    *out_entries = entries;
    *out_count = entry_count;
    return LSM_OK;
}

static lsm_status_t lsm_write_sstable(const char *path, const lsm_entry_t *entries, size_t count, lsm_sstable_meta_t *meta) {
    char tmp_path[LSM_MAX_PATH];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);

    int fd = open(tmp_path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return LSM_ERROR_IO;

    lsm_buffer_t block_buf;
    block_buf.size = 0;
    block_buf.capacity = LSM_BLOCK_SIZE;
    block_buf.data = malloc(block_buf.capacity);
    if (!block_buf.data) {
        close(fd);
        return LSM_ERROR_NOMEM;
    }

    size_t current_offset = 0;
    lsm_buffer_t index_buf;
    index_buf.size = 0;
    index_buf.capacity = 4096;
    index_buf.data = malloc(index_buf.capacity);
    if (!index_buf.data) {
        free(block_buf.data);
        close(fd);
        return LSM_ERROR_NOMEM;
    }

    uint32_t count32 = (uint32_t)count;
    if (index_buf.size + sizeof(uint32_t) > index_buf.capacity) {
        index_buf.capacity *= 2;
        uint8_t *new_ptr = realloc(index_buf.data, index_buf.capacity);
        if (!new_ptr) {
            free(block_buf.data);
            free(index_buf.data);
            close(fd);
            return LSM_ERROR_NOMEM;
        }
        index_buf.data = new_ptr;
    }
    memcpy(index_buf.data + index_buf.size, &count32, sizeof(uint32_t));
    index_buf.size += sizeof(uint32_t);

    for (size_t i = 0; i < count; i++) {
        uint32_t k_len = entries[i].key_len;
        uint32_t v_len = entries[i].val_len;
        uint64_t seq = entries[i].seq_num;
        uint8_t tomb = entries[i].is_tombstone ? 1 : 0;

        size_t needed = sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint8_t) + k_len + v_len;
        
        if (block_buf.size + needed > LSM_BLOCK_SIZE) {
            if (write(fd, block_buf.data, block_buf.size) != (ssize_t)block_buf.size) {
                free(block_buf.data);
                free(index_buf.data);
                close(fd);
                unlink(tmp_path);
                return LSM_ERROR_IO;
            }
            current_offset += block_buf.size;
            block_buf.size = 0;
        }

        uint8_t *dst = block_buf.data + block_buf.size;
        memcpy(dst, &k_len, sizeof(uint32_t)); dst += sizeof(uint32_t);
        memcpy(dst, &v_len, sizeof(uint32_t)); dst += sizeof(uint32_t);
        memcpy(dst, &seq, sizeof(uint64_t)); dst += sizeof(uint64_t);
        memcpy(dst, &tomb, sizeof(uint8_t)); dst += sizeof(uint8_t);
        memcpy(dst, entries[i].key, k_len); dst += k_len;
        memcpy(dst, entries[i].value, v_len);
        
        block_buf.size += needed;

        size_t index_needed = sizeof(uint32_t) * 2 + sizeof(uint64_t) + sizeof(uint8_t) + k_len + v_len;
        if (index_buf.size + index_needed > index_buf.capacity) {
            index_buf.capacity = index_buf.capacity * 2 + index_needed;
            uint8_t *new_ptr = realloc(index_buf.data, index_buf.capacity);
            if (!new_ptr) {
                free(block_buf.data);
                free(index_buf.data);
                close(fd);
                unlink(tmp_path);
                return LSM_ERROR_NOMEM;
            }
            index_buf.data = new_ptr;
        }

        uint8_t *idx_dst = index_buf.data + index_buf.size;
        memcpy(idx_dst, &k_len, sizeof(uint32_t)); idx_dst += sizeof(uint32_t);
        memcpy(idx_dst, &v_len, sizeof(uint32_t)); idx_dst += sizeof(uint32_t);
        memcpy(idx_dst, &seq, sizeof(uint64_t)); idx_dst += sizeof(uint64_t);
        memcpy(idx_dst, &tomb, sizeof(uint8_t)); idx_dst += sizeof(uint8_t);
        memcpy(idx_dst, entries[i].key, k_len); idx_dst += k_len;
        memcpy(idx_dst, entries[i].value, v_len);
        index_buf.size += index_needed;
    }

    if (block_buf.size > 0) {
        if (write(fd, block_buf.data, block_buf.size) != (ssize_t)block_buf.size) {
            free(block_buf.data);
            free(index_buf.data);
            close(fd);
            unlink(tmp_path);
            return LSM_ERROR_IO;
        }
        current_offset += block_buf.size;
    }

    uint64_t index_offset = current_offset;
    if (write(fd, index_buf.data, index_buf.size) != (ssize_t)index_buf.size) {
        free(block_buf.data);
        free(index_buf.data);
        close(fd);
        unlink(tmp_path);
        return LSM_ERROR_IO;
    }

    uint64_t footer[2];
    footer[0] = index_offset;
    footer[1] = LSM_MAGIC;
    if (write(fd, footer, sizeof(footer)) != (ssize_t)sizeof(footer)) {
        free(block_buf.data);
        free(index_buf.data);
        close(fd);
        unlink(tmp_path);
        return LSM_ERROR_IO;
    }

    if (fsync(fd) < 0) {
        free(block_buf.data);
        free(index_buf.data);
        close(fd);
        unlink(tmp_path);
        return LSM_ERROR_IO;
    }

    close(fd);
    if (rename(tmp_path, path) < 0) {
        free(block_buf.data);
        free(index_buf.data);
        unlink(tmp_path);
        return LSM_ERROR_IO;
    }

    free(block_buf.data);
    free(index_buf.data);

    memset(meta, 0, sizeof(lsm_sstable_meta_t));
    if (count > 0) {
        memcpy(meta->key, entries[0].key, entries[0].key_len);
        meta->key_len = entries[0].key_len;
        meta->seq_num = entries[count - 1].seq_num;
    }
    struct stat st;
    if (stat(path, &st) == 0) {
        meta->file_size = st.st_size;
    }
    meta->index_offset = index_offset;
    meta->is_tombstone = false;

    return LSM_OK;
}

static int lsm_merge_iterators(lsm_entry_t **iter_arrays, size_t *iter_counts, size_t num_iters, lsm_entry_t **out_entries, size_t *out_count) {
    size_t *indices = calloc(