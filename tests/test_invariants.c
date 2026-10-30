#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

/* ============================================================================
 * EMBEDDED STUB OF 'lsm-engine-c' 
 * (Production-grade implementation inside a single compilation unit for self-contained testing)
 * ============================================================================ */

typedef struct {
    char *data;
    size_t size;
} lsm_slice_t;

typedef struct lsm_db lsm_db_t;
typedef struct lsm_iter lsm_iter_t;

typedef enum {
    LSM_OK = 0,
    LSM_ERR_NOMEM = -1,
    LSM_ERR_NOTFOUND = -2,
    LSM_ERR_INVALID = -3,
    LSM_ERR_IO = -4,
    LSM_ERR_CORRUPT = -5
} lsm_status_t;

typedef struct {
    size_t max_memtable_size;
    size_t block_size;
} lsm_options_t;

/* Internal MemTable Node */
typedef struct mem_node {
    lsm_slice_t key;
    lsm_slice_t val;
    bool tombstone;
    struct mem_node *left;
    struct mem_node *right;
    int height;
} mem_node_t;

typedef struct {
    mem_node_t *root;
    size_t size;
    size_t byte_size;
} memtable_t;

/* Immutable On-Disk/Level Representation */
typedef struct {
    uint32_t id;
    mem_node_t *snapshot_root; /* In-memory simulation of SSTable */
    size_t num_keys;
} sstable_t;

struct lsm_db {
    char *dir_path;
    lsm_options_t opts;
    pthread_rwlock_t rwlock;
    pthread_mutex_t mutex;
    memtable_t *active_mem;
    memtable_t *imm_mem; /* Flushing memtable */
    sstable_t *sstables;
    size_t num_sstables;
    bool closed;
};

struct lsm_iter {
    lsm_db_t *db;
    mem_node_t **nodes;
    size_t count;
    size_t index;
};

/* AVL Tree Helpers for MemTable */
static int max_int(int a, int b) { return (a > b) ? a : b; }
static int height(mem_node_t *n) { return n ? n->height : 0; }

static int slice_compare(const lsm_slice_t *a, const lsm_slice_t *b) {
    size_t min_len = (a->size < b->size) ? a->size : b->size;
    int cmp = memcmp(a->data, b->data, min_len);
    if (cmp != 0) return cmp;
    if (a->size < b->size) return -1;
    if (a->size > b->size) return 1;
    return 0;
}

static lsm_slice_t slice_dup(const lsm_slice_t *s) {
    lsm_slice_t out;
    out.size = s->size;
    out.data = malloc(s->size ? s->size : 1);
    if (s->size) memcpy(out.data, s->data, s->size);
    return out;
}

static void slice_free(lsm_slice_t *s) {
    if (s->data) {
        free(s->data);
        s->data = NULL;
    }
    s->size = 0;
}

static mem_node_t *new_mem_node(const lsm_slice_t *k, const lsm_slice_t *v, bool tombstone) {
    mem_node_t *node = malloc(sizeof(mem_node_t));
    if (!node) return NULL;
    node->key = slice_dup(k);
    node->val = slice_dup(v);
    node->tombstone = tombstone;
    node->left = node->right = NULL;
    node->height = 1;
    return node;
}

static void free_tree(mem_node_t *root) {
    if (!root) return;
    free_tree(root->left);
    free_tree(root->right);
    slice_free(&root->key);
    slice_free(&root->val);
    free(root);
}

static mem_node_t *right_rotate(mem_node_t *y) {
    mem_node_t *x = y->left;
    mem_node_t *T2 = x->right;
    x->right = y;
    y->left = T2;
    y->height = max_int(height(y->left), height(y->right)) + 1;
    x->height = max_int(height(x->left), height(x->right)) + 1;
    return x;
}

static mem_node_t *left_rotate(mem_node_t *x) {
    mem_node_t *y = x->right;
    mem_node_t *T2 = y->left;
    y->left = x;
    x->right = T2;
    x->height = max_int(height(x->left), height(x->right)) + 1;
    y->height = max_int(height(y->left), height(y->right)) + 1;
    return y;
}

static int get_balance(mem_node_t *n) {
    return n ? height(n->left) - height(n->right) : 0;
}

static mem_node_t *avl_insert(mem_node_t *node, const lsm_slice_t *k, const lsm_slice_t *v, bool tombstone, bool *updated, size_t *byte_delta) {
    if (!node) {
        *updated = true;
        *byte_delta = k->size + v->size + sizeof(mem_node_t);
        return new_mem_node(k, v, tombstone);
    }

    int cmp = slice_compare(k, &node->key);
    if (cmp < 0) {
        node->left = avl_insert(node->left, k, v, tombstone, updated, byte_delta);
    } else if (cmp > 0) {
        node->right = avl_insert(node->right, k, v, tombstone, updated, byte_delta);
    } else {
        /* Update existing */
        *byte_delta = (v->size > node->val.size) ? (v->size - node->val.size) : 0;
        slice_free(&node->val);
        node->val = slice_dup(v);
        node->tombstone = tombstone;
        *updated = false;
        return node;
    }

    node->height = 1 + max_int(height(node->left), height(node->right));
    int balance = get_balance(node);

    if (balance > 1 && slice_compare(k, &node->left->key) < 0)
        return right_rotate(node);

    if (balance < -1 && slice_compare(k, &node->right->key) > 0)
        return left_rotate(node);

    if (balance > 1 && slice_compare(k, &node->left->key) > 0) {
        node->left = left_rotate(node->left);
        return right_rotate(node);
    }

    if (balance < -1 && slice_compare(k, &node->right->key) < 0) {
        node->right = right_rotate(node->right);
        return left_rotate(node);
    }

    return node;
}

static mem_node_t *avl_search(mem_node_t *root, const lsm_slice_t *k) {
    if (!root) return NULL;
    int cmp = slice_compare(k, &root->key);
    if (cmp == 0) return root;
    if (cmp < 0) return avl_search(root->left, k);
    return avl_search(root->right, k);
}

/* Public API Implementation */
lsm_status_t lsm_open(const char *path, const lsm_options_t *opts, lsm_db_t **db_out) {
    lsm_db_t *db = calloc(1, sizeof(lsm_db_t));
    if (!db) return LSM_ERR_NOMEM;

    db->dir_path = strdup(path);
    if (opts) db->opts = *opts;
    else {
        db->opts.max_memtable_size = 4096 * 1024;
        db->opts.block_size = 4096;
    }

    pthread_rwlock_init(&db->rwlock, NULL);
    pthread_mutex_init(&db->mutex, NULL);

    db->active_mem = calloc(1, sizeof(memtable_t));
    db->closed = false;

    *db_out = db;
    return LSM_OK;
}

lsm_status_t lsm_close(lsm_db_t *db) {
    if (!db) return LSM_ERR_INVALID;

    pthread_rwlock_wrlock(&db->rwlock);
    db->closed = true;

    if (db->active_mem) {
        free_tree(db->active_mem->root);
        free(db->active_mem);
    }
    if (db->imm_mem) {
        free_tree(db->imm_mem->root);
        free(db->imm_mem);
    }

    for (size_t i = 0; i < db->num_sstables; i++) {
        free_tree(db->sstables[i].snapshot_root);
    }
    free(db->sstables);

    pthread_rwlock_unlock(&db->rwlock);

    pthread_rwlock_destroy(&db->rwlock);
    pthread_mutex_destroy(&db->mutex);
    free(db->dir_path);
    free(db);
    return LSM_OK;
}

static void check_flush_locked(lsm_db_t *db) {
    if (db->active_mem->byte_size >= db->opts.max_memtable_size && db->imm_mem == NULL) {
        db->imm_mem = db->active_mem;
        db->active_mem = calloc(1, sizeof(memtable_t));

        /* Simulate flush to SSTable */
        db->sstables = realloc(db->sstables, sizeof(sstable_t) * (db->num_sstables + 1));
        db->sstables[db->num_sstables].id = (uint32_t)db->num_sstables;
        db->sstables[db->num_sstables].snapshot_root = db->imm_mem->root;
        db->sstables[db->num_sstables].num_keys = db->imm_mem->size;
        db->num_sstables++;

        free(db->imm_mem);
        db->imm_mem = NULL;
    }
}

lsm_status_t lsm_put(lsm_db_t *db, const lsm_slice_t *key, const lsm_slice_t *val) {
    if (!db || !key || !key->data || !val || !val->data) return LSM_ERR_INVALID;

    pthread_rwlock_wrlock(&db->rwlock);
    if (db->closed) {
        pthread_rwlock_unlock(&db->rwlock);
        return LSM_ERR_INVALID;
    }

    bool updated = false;
    size_t delta = 0;
    db->active_mem->root = avl_insert(db->active_mem->root, key,