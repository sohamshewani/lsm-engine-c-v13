#include "lsm_sstable.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <pthread.h>

#define SSTABLE_MAGIC 0x4C534D53535424CULL // "LSMSSPBL"
#define FOOTER_SIZE 48
#define BLOCK_SIZE 4096

// MurmurHash3 32-bit implementation for Bloom filters
static uint32_t rotl32(uint32_t x, int8_t r) {
    return (x << r) | (x >> (32 - r));
}

static uint32_t fmix32(uint32_t h) {
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae3d;
    h ^= h >> 16;
    return h;
}

static uint32_t murmurhash3(const void *key, int len, uint32_t seed) {
    const uint8_t *data = (const uint8_t *)key;
    const int nblocks = len / 4;
    uint32_t h1 = seed;
    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);
    for (int i = -nblocks; i; i++) {
        uint32_t k1 = blocks[i];
        k1 *= c1;
        k1 = rotl32(k1, 15);
        k1 *= c2;

        h1 ^= k1;
        h1 = rotl32(h1, 13);
        h1 = h1 * 5 + 0xe6546b64;
    }

    const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= tail[2] << 16;
        case 2: k1 ^= tail[1] << 8;
        case 1: k1 ^= tail[0];
            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;
            h1 ^= k1;
    };

    h1 ^= len;
    return fmix32(h1);
}

// CRC32 IEEE implementation
static uint32_t crc32_compute(const uint8_t *data, size_t length) {
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

struct SSTableBuilder {
    int fd;
    char *path;
    uint64_t current_offset;
    uint32_t block_size;
    
    // Current data block buffer
    uint8_t *block_buffer;
    uint32_t block_offset;
    uint32_t block_key_count;
    
    // Index entries
    struct IndexEntry *index_entries;
    size_t index_count;
    size_t index_capacity;
    
    // Bloom filter tracking
    uint8_t *bloom_bits;
    size_t bloom_byte_size;
    uint32_t total_keys;
    
    // Last key for sorting validation
    char *last_key;
    size_t last_key_len;
};

struct SSTableReader {
    int fd;
    char *path;
    size_t file_size;
    void *mmap_region;
    
    // Footer metadata
    uint64_t index_offset;
    uint64_t index_size;
    uint64_t bloom_offset;
    uint64_t bloom_size;
    
    // In-memory sparse index
    struct IndexEntry *index_entries;
    size_t index_count;
    
    // Bloom filter
    uint8_t *bloom_bits;
    size_t bloom_byte_size;
    
    pthread_rwlock_t rwlock;
};

struct SSTableIterator {
    SSTableReader *reader;
    size_t current_index;
    uint8_t *current_block_data;
    uint32_t current_block_size;
    uint32_t block_cursor;
    bool valid;
    
    char *current_key;
    size_t current_key_len;
    char *current_value;
    size_t current_value_len;
};

SSTableBuilder* sstable_builder_create(const char *path, uint32_t block_size) {
    SSTableBuilder *builder = (SSTableBuilder*)malloc(sizeof(SSTableBuilder));
    if (!builder) return NULL;

    builder->fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (builder->fd < 0) {
        free(builder);
        return NULL;
    }

    builder->path = strdup(path);
    builder->current_offset = 0;
    builder->block_size = block_size > 0 ? block_size : BLOCK_SIZE;

    builder->block_buffer = (uint8_t*)malloc(builder->block_size);
    builder->block_offset = 0;
    builder->block_key_count = 0;

    builder->index_capacity = 1024;
    builder->index_entries = (struct IndexEntry*)malloc(builder->index_capacity * sizeof(struct IndexEntry));
    builder->index_count = 0;

    builder->bloom_byte_size = 16384; // Initial size, resized or fixed
    builder->bloom_bits = (uint8_t*)calloc(1, builder->bloom_byte_size);
    builder->total_keys = 0;

    builder->last_key = NULL;
    builder->last_key_len = 0;

    return builder;
}

static void sstable_flush_block(SSTableBuilder *builder) {
    if (builder->block_offset == 0) return;

    // Write block data
    ssize_t written = write(builder->fd, builder->block_buffer, builder->block_offset);
    if (written != (ssize_t)builder->block_offset) {
        perror("Failed to write SSTable block");
        return;
    }

    // Add to index: First key of the block points to builder->current_offset
    // Extract first key from block buffer
    uint32_t key_len;
    memcpy(&key_len, builder->block_buffer, sizeof(uint32_t));
    char *first_key = (char*)malloc(key_len);
    memcpy(first_key, builder->block_buffer + sizeof(uint32_t), key_len);

    if (builder->index_count >= builder->index_capacity) {
        builder->index_capacity *= 2;
        builder->index_entries = (struct IndexEntry*)realloc(builder->index_entries, builder->index_capacity * sizeof(struct IndexEntry));
    }

    builder->index_entries[builder->index_count].offset = builder->current_offset;
    builder->index_entries[builder->index_count].size = builder->block_offset;
    builder->index_entries[builder->index_count].key = first_key;
    builder->index_entries[builder->index_count].key_len = key_len;
    builder->index_count++;

    builder->current_offset += builder->block_offset;
    builder->block_offset = 0;
    builder->block_key_count = 0;
}

int sstable_builder_add(SSTableBuilder *builder, const char *key, size_t key_len, const char *value, size_t value_len) {
    // Enforce LSM Monotonicity: keys must be strictly sorted
    if (builder->last_key) {
        int cmp = memcmp(builder->last_key, key, builder->last_key_len < key_len ? builder->last_key_len : key_len);
        if (cmp > 0 || (cmp == 0 && builder->last_key_len >= key_len)) {
            return -1; // Out of order key
        }
        free(builder->last_key);
    }
    builder->last_key = (char*)malloc(key_len);
    memcpy(builder->last_key, key, key_len);
    builder->last_key_len = key_len;

    // Format: [key_len:4][val_len:4][key:key_len][val:val_len]
    uint32_t needed = sizeof(uint32_t) * 2 + key_len + value_len;
    if (builder->block_offset + needed > builder->block_size && builder->block_offset > 0) {
        sstable_flush_block(builder);
    }

    uint32_t k_len = (uint32_t)key_len;
    uint32_t v_len = (uint32_t)value_len;

    memcpy(builder->block_buffer + builder->block_offset, &k_len, sizeof(uint32_t));
    builder->block_offset += sizeof(uint32_t);

    memcpy(builder->block_buffer + builder->block_offset, &v_len, sizeof(uint32_t));
    builder->block_offset += sizeof(uint32_t);

    memcpy(builder->block_buffer + builder->block_offset, key, key_len);
    builder->block_offset += key_len;

    memcpy(builder->block_buffer + builder->block_offset, value, value_len);
    builder->block_offset += value_len;

    builder->block_key_count++;

    // Update Bloom Filter
    uint32_t h = murmurhash3(key, key_len, 0xbc9f1d35);
    size_t bit_idx = h % (builder->bloom_byte_size * 8);
    builder->bloom_bits[bit_idx / 8] |= (1 << (bit_idx % 8));
    builder->total_keys++;

    return 0;
}

int sstable_builder_finalize(SSTableBuilder *builder) {
    // Flush remaining block
    if (builder->block_offset > 0) {
        sstable_flush_block(builder);
    }

    // Write Index Block
    uint64_t index_offset = builder->current_offset;
    uint32_t idx_count = (uint32_t)builder->index_count;
    if (write(builder->fd, &idx_count, sizeof(uint32_t)) != sizeof(uint32_t)) return -1;
    uint64_t index_bytes_written = sizeof(uint32_t);

    for (size_t i = 0; i < builder->index_count; i++) {
        uint32_t k_len = (uint32_t)builder->index_entries[i].key_len;
        if (write(builder->fd, &builder->index_entries[i].offset, sizeof(uint64_t)) != sizeof(uint64_t)) return -1;
        if (write(builder->fd, &builder->index_entries[i].size, sizeof(uint32_t)) != sizeof(uint32_t)) return -1;
        if (write(builder->fd, &k_len, sizeof(uint32_t)) != sizeof(uint32_t)) return -1;
        if (write(builder->fd, builder->index_entries[i].key, k_len) != (ssize_t)k_len) return -1;

        index_bytes_written += sizeof(uint64_t) + sizeof(uint32_t) + sizeof(uint32_t) + k_len;
    }
    uint64_t index_size = index_bytes_written;
    builder->current_offset += index_size;

    // Write Bloom Filter Block
    uint64_t bloom_offset = builder->current_offset;
    uint32_t b_size = (uint32_t)builder->bloom_byte_size;
    if (write(builder->fd, &b_size, sizeof(uint32_t)) != sizeof(uint32_t)) return -1;
    if (write(builder->fd, builder->bloom_bits, builder->bloom_byte_size) != (ssize_t)builder->bloom_byte_size) return -1;
    uint64_t bloom_size = sizeof(uint32_t) + builder->bloom_byte_size;
    builder->current_offset += bloom_size;

    // Write Footer
    uint8_t footer[FOOTER_SIZE];
    memset(footer, 0, FOOTER_SIZE);

    uint64_t magic = SSTABLE_MAGIC;
    memcpy(footer + 0, &magic, 8);
    memcpy(footer + 8, &index_offset, 8);
    memcpy(footer + 16, &index_size, 8);
    memcpy(footer + 24, &bloom_offset, 8);
    memcpy(footer + 32, &bloom_size, 8);

    uint32_t footer_crc = crc32_compute(footer, 40);
    memcpy(footer + 40, &footer_crc, 4);

    if (write(builder->fd, footer, FOOTER_SIZE) != FOOTER_SIZE) return -1;

    fsync(builder->fd);
    return 0;
}

void sstable_builder_destroy(SSTableBuilder *builder) {
    if (!builder) return;
    if (builder->fd >= 0) close(builder->fd);
    if (builder->path) {
        free(builder->path);
    }
    if (builder->block_buffer) free(builder->block_buffer);
    if (builder->last_key) free(builder->last_key);
    if (builder->bloom_bits) free(builder->bloom_bits);
    for (size_t i = 0; i < builder->index_count; i++) {
        free(builder->index_entries[i].key);
    }
    if (builder->index_entries) free(builder->index_entries);
    free(builder);
}

SSTableReader* sstable_reader_open(const char *path) {
    SSTableReader *reader = (SSTableReader*)malloc(sizeof(SSTableReader));
    if (!reader) return NULL;

    reader->path = strdup(path);
    reader->fd = open(path, O_RDONLY);
    if (reader->fd < 0) {
        free(reader->path);
        free(reader);
        return NULL;
    }

    struct stat st;
    if (fstat(reader->fd, &st) < 0) {
        close(reader->fd);
        free(reader->path);
        free(reader);
        return NULL;
    }
    reader->file_size = st.st_size;

    reader->mmap_region = mmap(NULL, reader->file_size, PROT_READ, MAP_SHARED, reader->fd, 0);
    if (reader->mmap_region == MAP_FAILED) {
        close(reader->fd);
        free(reader->path);
        free(reader);
        return NULL;
    }

    // Read and validate footer
    if (