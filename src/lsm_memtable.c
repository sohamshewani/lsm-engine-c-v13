#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>
#include <assert.h>
#include <errno.h>

#define MAX_LEVELS 32
#define MAX_HAZARD_POINTERS 4
#define CACHELINE_SIZE 64

typedef enum {
    OP_PUT = 1,
    OP_DELETE = 2
} OperationType;

typedef struct {
    _Atomic(uint64_t) lsn;
    OperationType type;
    uint32_t key_len;
    uint32_t val_len;
    char data[];
} MemTableEntry;

typedef struct SkipNode {
    _Atomic(uint64_t) lsn;
    OperationType type;
    uint32_t key_len;
    uint32_t val_len;
    int height;
    _Atomic(bool) fully_linked;
    _Atomic(struct SkipNode *) forward[];
} SkipNode;

typedef struct {
    void *memory;
    size_t capacity;
    _Atomic(size_t) offset;
    pthread_mutex_t lock;
} Arena;

typedef struct {
    _Atomic(struct SkipNode *) ptr;
} HazardPointer;

typedef struct {
    HazardPointer pointers[MAX_HAZARD_POINTERS];
} ThreadHazardRecord;

typedef struct {
    ThreadHazardRecord *records;
    size_t capacity;
    _Atomic(size_t) count;
    pthread_mutex_t mutex;
} HazardDomain;

typedef struct {
    _Atomic(uint64_t) lsn;
    OperationType type;
    uint32_t key_len;
    uint32_t val_len;
    const char *key;
    const char *value;
} MemTableIterator;

struct LSM_MemTable {
    SkipNode *head;
    _Atomic(int) max_height;
    Arena arena;
    HazardDomain hazard_domain;
    _Atomic(size_t) byte_size;
    _Atomic(uint64_t) current_lsn;
};

static HazardDomain global_hazard_domain = {0};
static pthread_once_t hazard_domain_once = PTHREAD_ONCE_INIT;

static void init_global_hazard_domain(void) {
    global_hazard_domain.capacity = 128;
    global_hazard_domain.records = calloc(global_hazard_domain.capacity, sizeof(ThreadHazardRecord));
    atomic_init(&global_hazard_domain.count, 0);
    pthread_mutex_init(&global_hazard_domain.mutex, NULL);
}

ThreadHazardRecord *lsm_hazard_get_record(void) {
    pthread_once(&hazard_domain_once, init_global_hazard_domain);
    
    static _Thread_local ThreadHazardRecord *my_record = NULL;
    if (my_record != NULL) {
        return my_record;
    }

    pthread_mutex_lock(&global_hazard_domain.mutex);
    for (size_t i = 0; i < atomic_load(&global_hazard_domain.count); i++) {
        bool in_use = false;
        for (int p = 0; p < MAX_HAZARD_POINTERS; p++) {
            if (atomic_load(&global_hazard_domain.records[i].pointers[p].ptr) != NULL) {
                in_use = true;
                break;
            }
        }
        if (!in_use) {
            my_record = &global_hazard_domain.records[i];
            pthread_mutex_unlock(&global_hazard_domain.mutex);
            return my_record;
        }
    }

    if (atomic_load(&global_hazard_domain.count) >= global_hazard_domain.capacity) {
        size_t new_cap = global_hazard_domain.capacity * 2;
        ThreadHazardRecord *new_recs = realloc(global_hazard_domain.records, new_cap * sizeof(ThreadHazardRecord));
        if (!new_recs) {
            pthread_mutex_unlock(&global_hazard_domain.mutex);
            return NULL;
        }
        memset(new_recs + global_hazard_domain.capacity, 0, (new_cap - global_hazard_domain.capacity) * sizeof(ThreadHazardRecord));
        global_hazard_domain.records = new_recs;
        global_hazard_domain.capacity = new_cap;
    }

    size_t idx = atomic_fetch_add(&global_hazard_domain.count, 1);
    my_record = &global_hazard_domain.records[idx];
    pthread_mutex_unlock(&global_hazard_domain.mutex);
    return my_record;
}

void lsm_hazard_set(int index, SkipNode *node) {
    ThreadHazardRecord *rec = lsm_hazard_get_record();
    if (rec && index >= 0 && index < MAX_HAZARD_POINTERS) {
        atomic_store(&rec->pointers[index].ptr, node);
    }
}

void lsm_hazard_clear(int index) {
    ThreadHazardRecord *rec = lsm_hazard_get_record();
    if (rec && index >= 0 && index < MAX_HAZARD_POINTERS) {
        atomic_store(&rec->pointers[index].ptr, NULL);
    }
}

bool lsm_hazard_is_protected(SkipNode *node) {
    pthread_mutex_lock(&global_hazard_domain.mutex);
    size_t count = atomic_load(&global_hazard_domain.count);
    for (size_t i = 0; i < count; i++) {
        for (int p = 0; p < MAX_HAZARD_POINTERS; p++) {
            if (atomic_load(&global_hazard_domain.records[i].pointers[p].ptr) == node) {
                pthread_mutex_unlock(&global_hazard_domain.mutex);
                return true;
            }
        }
    }
    pthread_mutex_unlock(&global_hazard_domain.mutex);
    return false;
}

static Arena arena_create(size_t capacity) {
    Arena arena;
    arena.capacity = capacity;
    arena.memory = malloc(capacity);
    if (!arena.memory) {
        arena.capacity = 0;
    }
    atomic_init(&arena.offset, 0);
    pthread_mutex_init(&arena.lock, NULL);
    return arena;
}

static void *arena_alloc(Arena *arena, size_t size) {
    size_t align = 8;
    size = (size + (align - 1)) & ~(align - 1);
    
    size_t old_offset = atomic_load(&arena->offset);
    while (true) {
        if (old_offset + size > arena->capacity) {
            pthread_mutex_lock(&arena->lock);
            old_offset = atomic_load(&arena->offset);
            if (old_offset + size > arena->capacity) {
                pthread_mutex_unlock(&arena->lock);
                return NULL;
            }
            size_t new_offset = old_offset + size;
            atomic_store(&arena->offset, new_offset);
            pthread_mutex_unlock(&arena->lock);
            return (char *)arena->memory + old_offset;
        }
        
        if (atomic_compare_exchange_weak(&arena->offset, &old_offset, old_offset + size)) {
            return (char *)arena->memory + old_offset;
        }
    }
}

static void arena_destroy(Arena *arena) {
    if (arena->memory) {
        free(arena->memory);
        arena->memory = NULL;
    }
    pthread_mutex_destroy(&arena->lock);
}

static int random_level(void) {
    int lvl = 1;
    while ((rand() & 0xFFFF) < (0xFFFF / 4) && lvl < MAX_LEVELS) {
        lvl++;
    }
    return lvl;
}

static int key_compare(const char *a_key, uint32_t a_len, const char *b_key, uint32_t b_len) {
    uint32_t min_len = a_len < b_len ? a_len : b_len;
    int cmp = memcmp(a_key, b_key, min_len);
    if (cmp != 0) return cmp;
    if (a_len < b_len) return -1;
    if (a_len > b_len) return 1;
    return 0;
}

static SkipNode *create_node(Arena *arena, uint64_t lsn, OperationType type, const char *key, uint32_t key_len, const char *val, uint32_t val_len, int height) {
    size_t sz = sizeof(SkipNode) + sizeof(_Atomic(SkipNode *)) * height + key_len + val_len;
    SkipNode *node = (SkipNode *)arena_alloc(arena, sz);
    if (!node) return NULL;

    atomic_init(&node->lsn, lsn);
    node->type = type;
    node->key_len = key_len;
    node->val_len = val_len;
    node->height = height;
    atomic_init(&node->fully_linked, false);

    for (int i = 0; i < height; i++) {
        atomic_init(&node->forward[i], NULL);
    }

    char *dest_key = (char *)&node->forward[height];
    char *dest_val = dest_key + key_len;
    memcpy(dest_key, key, key_len);
    if (val_len > 0 && val) {
        memcpy(dest_val, val, val_len);
    }

    return node;
}

static inline const char *node_key(const SkipNode *node) {
    return (const char *)&node->forward[node->height];
}

static inline const char *node_val(const SkipNode *node) {
    return (const char *)&node->forward[node->height] + node->key_len;
}

LSM_MemTable *lsm_memtable_create(size_t arena_size) {
    LSM_MemTable *table = malloc(sizeof(LSM_MemTable));
    if (!table) return NULL;

    table->arena = arena_create(arena_size);
    if (!table->arena.memory) {
        free(table);
        return NULL;
    }

    atomic_init(&table->max_height, 1);
    atomic_init(&table->byte_size, 0);
    atomic_init(&table->current_lsn, 0);

    table->head = create_node(&table->arena, 0, OP_PUT, "", 0, "", 0, MAX_LEVELS);
    if (!table->head) {
        arena_destroy(&table->arena);
        free(table);
        return NULL;
    }
    atomic_store(&table->head->fully_linked, true);

    return table;
}

void lsm_memtable_destroy(LSM_MemTable *table) {
    if (!table) return;
    arena_destroy(&table->arena);
    free(table);
}

bool lsm_memtable_put(LSM_MemTable *table, uint64_t lsn, const char *key, uint32_t key_len, const char *val, uint32_t val_len) {
    int height = random_level();
    SkipNode *new_node = create_node(&table->arena, lsn, OP_PUT, key, key_len, val, val_len, height);
    if (!new_node) return false;

    SkipNode *preds[MAX_LEVELS];
    SkipNode *succs[MAX_LEVELS];

    while (true) {
        int l_max = atomic_load(&table->max_height);
        int found = -1;

        SkipNode *pred = table->head;
        lsm_hazard_set(0, pred);

        for (int i = l_max - 1; i >= 0; i--) {
            SkipNode *curr = atomic_load(&pred->forward[i]);
            lsm_hazard_set(1, curr);

            while (curr != NULL) {
                lsm_hazard_set(2, curr);
                int cmp = key_compare(node_key(curr), curr->key_len, key, key_len);
                if (cmp < 0) {
                    pred = curr;
                    lsm_hazard_set(0, pred);
                    curr = atomic_load(&pred->forward[i]);
                    lsm_hazard_set(1, curr);
                } else {
                    break;
                }
            }

            if (found == -1 && curr != NULL) {
                int cmp = key_compare(node_key(curr), curr->key_len, key, key_len);
                if (cmp == 0) {
                    found = i;
                }
            }

            preds[i] = pred;
            succs[i] = curr;
        }

        lsm_hazard_clear(0);
        lsm_hazard_clear(1);
        lsm_hazard_clear(2);

        if (found != -1) {
            SkipNode *node_to_update = succs[found];
            if (!atomic_load(&node_to_update->fully_linked)) {
                continue;
            }
            if (atomic_load(&node_to_update->lsn) >= lsn) {
                return false;
            }
            
            atomic_store(&new_node->lsn, lsn);
            atomic_store(&node_to_update->lsn, lsn);
            node_to_update->type = OP_PUT;
            atomic_store(&table->current_lsn, lsn);
            atomic_fetch_add(&table->byte_size, key_len + val_len);
            return true;
        }

        int top_level = height;
        int cur_max = atomic_load(&table->max_height);
        if (top_level > cur_max) {
            for (int i = cur_max; i < top_level; i++) {
                preds[i] = table->head;
                succs[i] = NULL;
            }
            atomic_compare_exchange_strong(&table->max_height, &cur_max, top_level);
        }

        bool valid = true;
        for (int i = 0; i < top_level; i++) {
            SkipNode *p = preds[i];
            SkipNode *s = succs[i];
            atomic_store(&new_node->forward[i], s);
            
            lsm_hazard_set(0, p);
            lsm_hazard_set(1, s);
            
            SkipNode *expected_s = s;
            if (!atomic_compare_exchange_strong(&p->forward[i], &expected_s, new_node)) {
                valid = false;
                lsm_hazard_clear(0);
                lsm_hazard_clear(1);
                break;
            }
            lsm_hazard_clear(0);
            lsm_hazard_clear(1);
        }

        if (!valid) {
            continue;
        }

        atomic_store(&new_node->fully_linked, true);
        atomic_store(&table->current_lsn, lsn);
        atomic_fetch_add(&table->byte_size, sizeof(SkipNode) + key_len + val_len);
        return true;
    }
}

bool lsm_memtable_delete(LSM_MemTable *table, uint64_t lsn, const char *key, uint32_t key_len) {
    int height = random_level();
    SkipNode *new_node = create_node(&table->arena, lsn, OP_DELETE, key, key_len, NULL, 0, height);
    if (!new_node) return false;

    SkipNode *preds[MAX_LEVELS];
    SkipNode *succs[MAX_LEVELS];

    while (true) {
        int l_max = atomic_load(&table->max_height);
        int found = -1;

        SkipNode *pred = table->head;
        lsm_hazard_set(0, pred);

        for (int i