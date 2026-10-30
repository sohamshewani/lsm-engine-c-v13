#include "lsm_wal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>

#define WAL_MAGIC_HEADER 0x4C534D5730303031ULL /* "LSMW0001" */

/* Standard CRC32 IEEE implementation */
static uint32_t calculate_crc32(const uint8_t *data, size_t length) {
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

struct lsm_wal {
    int fd;
    char filepath[1024];
    uint64_t next_lsn;
    pthread_mutex_t mutex;
    bool sync_on_write;
};

lsm_wal_t* lsm_wal_open(const char *filepath, bool sync_on_write) {
    if (!filepath) {
        errno = EINVAL;
        return NULL;
    }

    lsm_wal_t *wal = (lsm_wal_t *)malloc(sizeof(lsm_wal_t));
    if (!wal) {
        return NULL;
    }

    strncpy(wal->filepath, filepath, sizeof(wal->filepath) - 1);
    wal->filepath[sizeof(wal->filepath) - 1] = '\0';
    wal->next_lsn = 1;
    wal->sync_on_write = sync_on_write;

    if (pthread_mutex_init(&wal->mutex, NULL) != 0) {
        free(wal);
        return NULL;
    }

    int flags = O_RDWR | O_CREAT;
    if (sync_on_write) {
        #ifdef O_SYNC
        flags |= O_SYNC;
        #endif
    }

    wal->fd = open(wal->filepath, flags, 0644);
    if (wal->fd < 0) {
        pthread_mutex_destroy(&wal->mutex);
        free(wal);
        return NULL;
    }

    return wal;
}

int lsm_wal_append(lsm_wal_t *wal, const uint8_t *payload, uint32_t payload_len, uint64_t *out_lsn) {
    if (!wal || !payload || payload_len == 0) {
        return -EINVAL;
    }

    if (pthread_mutex_lock(&wal->mutex) != 0) {
        return -EINVAL;
    }

    uint64_t lsn = wal->next_lsn++;

    wal_header_t header;
    header.payload_len = payload_len;
    header.lsn = lsn;
    
    // Calculate CRC32 over the payload
    header.crc32 = calculate_crc32(payload, payload_len);

    // Write header
    ssize_t written = write(wal->fd, &header, sizeof(wal_header_t));
    if (written != (ssize_t)sizeof(wal_header_t)) {
        pthread_mutex_unlock(&wal->mutex);
        return -EIO;
    }

    // Write payload
    written = write(wal->fd, payload, payload_len);
    if (written != (ssize_t)payload_len) {
        pthread_mutex_unlock(&wal->mutex);
        return -EIO;
    }

    if (!wal->sync_on_write) {
        if (fsync(wal->fd) < 0) {
            pthread_mutex_unlock(&wal->mutex);
            return -EIO;
        }
    }

    if (out_lsn) {
        *out_lsn = lsn;
    }

    pthread_mutex_unlock(&wal->mutex);
    return 0;
}

int lsm_wal_recover(const char *filepath, void (*callback)(uint64_t lsn, const uint8_t *payload, uint32_t payload_len, void *user_data), void *user_data) {
    if (!filepath || !callback) {
        return -EINVAL;
    }

    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        if (errno == ENOENT) {
            return 0; // File does not exist, nothing to recover
        }
        return -errno;
    }

    while (1) {
off_t current_offset = lseek(fd, 0, SEEK_CUR);
        wal_header_t header;
        ssize_t n = read(fd, &header, sizeof(wal_header_t));
        if (n == 0) {
            // EOF reached cleanly
            break;
        }
        if (n < 0) {
            close(fd);
            return -EIO;
        }
        if (n < (ssize_t)sizeof(wal_header_t)) {
            // Torn write at EOF, truncate and stop recovery
            ftruncate(fd, current_offset);
            break;
        }

        if (header.payload_len == 0 || header.payload_len > 1024 * 1024 * 64) {
            // Unreasonable payload size, treat as corruption/torn write
            ftruncate(fd, current_offset);
            break;
        }

        uint8_t *payload = (uint8_t *)malloc(header.payload_len);
        if (!payload) {
            close(fd);
            return -ENOMEM;
        }

        n = read(fd, payload, header.payload_len);
        if (n < 0 || (size_t)n != header.payload_len) {
            // Torn payload write, truncate and stop
            free(payload);
            ftruncate(fd, current_offset);
            break;
        }

        uint32_t computed_crc = calculate_crc32(payload, header.payload_len);
        if (computed_crc != header.crc32) {
            // Checksum mismatch, corruption detected, truncate and stop
            free(payload);
            ftruncate(fd, current_offset);
            break;
        }

        // Valid frame, deliver to recovery callback
        callback(header.lsn, payload, header.payload_len, user_data);
        free(payload);
    }

    close(fd);
    return 0;
}

int lsm_wal_sync(lsm_wal_t *wal) {
    if (!wal) {
        return -EINVAL;
    }

    if (pthread_mutex_lock(&wal->mutex) != 0) {
        return -EINVAL;
    }

    int ret = 0;
    if (fsync(wal->fd) < 0) {
        ret = -EIO;
    }

    pthread_mutex_unlock(&wal->mutex);
    return ret;
}

void lsm_wal_close(lsm_wal_t *wal) {
    if (!wal) {
        return;
    }

    pthread_mutex_lock(&wal->mutex);
    if (wal->fd >= 0) {
        fsync(wal->fd);
        close(wal->fd);
        wal->fd = -1;
    }
    pthread_mutex_unlock(&wal->mutex);

    pthread_mutex_destroy(&wal->mutex);
    free(wal);
}