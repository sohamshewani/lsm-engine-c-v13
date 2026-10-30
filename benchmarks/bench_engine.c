#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include "lsm.h"

#define NUM_OPERATIONS 500000
#define WARMUP_OPERATIONS 50000
#define KEY_SIZE 16
#define VAL_SIZE 100
#define DB_PATH "bench_lsm.db"

// Helper to get time in nanoseconds
static inline uint64_t get_time_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

// Comparison function for qsort to calculate percentiles
static int compare_uint64(const void *a, const void *b) {
    uint64_t arg1 = *(const uint64_t *)a;
    uint64_t arg2 = *(const uint64_t *)b;
    if (arg1 < arg2) return -1;
    if (arg1 > arg2) return 1;
    return 0;
}

int main(void) {
    // Remove any leftover database files
    remove(DB_PATH);

    lsm_db *db = NULL;
    int rc = lsm_open(&db, DB_PATH);
    if (rc != LSM_OK || !db) {
        fprintf(stderr, "Failed to open LSM database: %d\n", rc);
        return 1;
    }

    uint64_t *latencies = malloc(NUM_OPERATIONS * sizeof(uint64_t));
    if (!latencies) {
        fprintf(stderr, "Memory allocation failed\n");
        lsm_close(db);
        return 1;
    }

    char key[KEY_SIZE];
    char val[VAL_SIZE];
    memset(val, 'x', VAL_SIZE);

    // ---------------------------------------------------------
    // WARMUP PHASE (PUT)
    // ---------------------------------------------------------
    for (int i = 0; i < WARMUP_OPERATIONS; i++) {
        int klen = snprintf(key, KEY_SIZE, "key%012d", i);
        lsm_put(db, key, klen, val, VAL_SIZE);
    }

    // ---------------------------------------------------------
    // BENCHMARK PHASE: PUT (INSERT / UPDATE)
    // ---------------------------------------------------------
    uint64_t start_total = get_time_ns();
    
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        int klen = snprintf(key, KEY_SIZE, "key%012d", i);
        
        uint64_t start_op = get_time_ns();
        rc = lsm_put(db, key, klen, val, VAL_SIZE);
        uint64_t end_op = get_time_ns();

        if (rc != LSM_OK) {
            fprintf(stderr, "lsm_put failed at op %d: %d\n", i, rc);
        }

        latencies[i] = end_op - start_op;
    }
    
    uint64_t end_total = get_time_ns();
    uint64_t total_time_ns = end_total - start_total;

    qsort(latencies, NUM_OPERATIONS, sizeof(uint64_t), compare_uint64);

    double put_ops_sec = ((double)NUM_OPERATIONS * 1000000000.0) / (double)total_time_ns;
    uint64_t put_p50 = latencies[(int)(NUM_OPERATIONS * 0.50)];
    uint64_t put_p99 = latencies[(int)(NUM_OPERATIONS * 0.99)];

    // ---------------------------------------------------------
    // BENCHMARK PHASE: GET (READ)
    // ---------------------------------------------------------
    start_total = get_time_ns();

    for (int i = 0; i < NUM_OPERATIONS; i++) {
        int klen = snprintf(key, KEY_SIZE, "key%012d", i);
        
        char *out_val = NULL;
        int out_val_len = 0;

        uint64_t start_op = get_time_ns();
        rc = lsm_get(db, key, klen, &out_val, &out_val_len);
        uint64_t end_op = get_time_ns();

        if (rc == LSM_OK && out_val) {
            free(out_val);
        }

        latencies[i] = end_op - start_op;
    }

    end_total = get_time_ns();
    uint64_t get_total_time_ns = end_total - start_total;

    qsort(latencies, NUM_OPERATIONS, sizeof(uint64_t), compare_uint64);

    double get_ops_sec = ((double)NUM_OPERATIONS * 1000000000.0) / (double)get_total_time_ns;
    uint64_t get_p50 = latencies[(int)(NUM_OPERATIONS * 0.50)];
    uint64_t get_p99 = latencies[(int)(NUM_OPERATIONS * 0.99)];

    // ---------------------------------------------------------
    // FORMATTED OUTPUT
    // ---------------------------------------------------------
    printf("\n");
    printf("=========================================================================\n");
    printf("               LSM-ENGINE-C PERFORMANCE BENCHMARK REPORT                 \n");
    printf("=========================================================================\n");
    printf(" Operations Count : %d\n", NUM_OPERATIONS);
    printf(" Key Size         : %d bytes\n", KEY_SIZE);
    printf(" Value Size       : %d bytes\n", VAL_SIZE);
    printf("-------------------------------------------------------------------------\n");
    printf(" Operation | Throughput (ops/sec) |       P50 Latency |       P99 Latency \n");
    printf("-------------------------------------------------------------------------\n");
    printf(" PUT       | %20.2f | %15.2f us | %15.2f us \n", 
           put_ops_sec, (double)put_p50 / 1000.0, (double)put_p99 / 1000.0);
    printf(" GET       | %20.2f | %15.2f us | %15.2f us \n", 
           get_ops_sec, (double)get_p50 / 1000.0, (double)get_p99 / 1000.0);
    printf("========================================================================-\n");
    printf("\n");

    // Cleanup
    free(latencies);
    lsm_close(db);
    remove(DB_PATH);

    return 0;
}