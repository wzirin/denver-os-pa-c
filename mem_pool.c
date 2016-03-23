/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    if(pool_store != NULL) {
        return ALLOC_CALLED_AGAIN;
    }
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate
    else {
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        pool_store = calloc(pool_store_capacity, sizeof(pool_mgr_pt));
        if(pool_store != NULL) {
            return ALLOC_OK;
        }
        else {
            return ALLOC_FAIL;
        }
    }
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    if(pool_store == NULL) {
        return ALLOC_CALLED_AGAIN;
    }
    // make sure all pool managers have been deallocated
    for(int i = 0; i < pool_store_size; i++) {
        if(pool_store[i] != NULL) {
            return ALLOC_NOT_FREED;
        }
    }
    // can free the pool store array
    free(pool_store);
    // update static variables
    pool_store = NULL;
    pool_store_capacity = 0;
    pool_store_size = 0;
    return ALLOC_OK;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    assert(pool_store != NULL);
    // expand the pool store, if necessary
    _mem_resize_pool_store();
    // allocate a new mem pool mgr
    pool_mgr_pt memPoolMgr = malloc(sizeof(pool_mgr_t));
    // check success, on error return null
    if(memPoolMgr == NULL) {
        return NULL;
    }
    // allocate a new memory pool
    memPoolMgr->pool.mem = malloc(size);
    // check success, on error deallocate mgr and return null
    if(memPoolMgr->pool.mem == NULL) {
        free(memPoolMgr);
        return NULL;
    }
    // allocate a new node heap
    memPoolMgr->node_heap = calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    // check success, on error deallocate mgr/pool and return null
    if(memPoolMgr->node_heap == NULL) {
        free(memPoolMgr->pool.mem);
        free(memPoolMgr);
        return NULL;
    }
    // allocate a new gap index
    memPoolMgr->gap_ix = calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    memPoolMgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
    // check success, on error deallocate mgr/pool/heap and return null
    if(memPoolMgr->gap_ix == NULL) {
        free(memPoolMgr->node_heap);
        free(memPoolMgr->pool.mem);
        free(memPoolMgr);
        return NULL;
    }
    // assign all the pointers and update meta data:
    //   initialize top node of node heap
    memPoolMgr->node_heap[0].alloc_record.mem = memPoolMgr->pool.mem;
    memPoolMgr->node_heap[0].alloc_record.size = size;
    memPoolMgr->node_heap[0].allocated = 0;
    memPoolMgr->node_heap[0].used = 1;
    memPoolMgr->node_heap[0].next = NULL;
    memPoolMgr->node_heap[0].prev = NULL;
    //   initialize top node of gap index
    memPoolMgr->gap_ix[0].node = &memPoolMgr->node_heap[0];
    memPoolMgr->gap_ix[0].size = size;
    //   initialize pool mgr
    memPoolMgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    memPoolMgr->used_nodes = 1;
    memPoolMgr->pool.total_size = size;
    memPoolMgr->pool.alloc_size = 0;
    memPoolMgr->pool.num_allocs = 0;
    memPoolMgr->pool.num_gaps = 1;
    memPoolMgr->pool.policy = policy;
    //   link pool mgr to pool store
    pool_store[pool_store_size] = memPoolMgr;
    pool_store_size++;
    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) memPoolMgr;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt memPoolMgr = (pool_mgr_pt) pool;
    // check if this pool is allocated
    if(memPoolMgr->pool.alloc_size != 0) {
        return ALLOC_NOT_FREED;
    }
    // check if pool has only one gap
    if(memPoolMgr->pool.num_gaps != 1) {
        return ALLOC_NOT_FREED;
    }
    // check if it has zero allocations
    if(memPoolMgr->pool.num_allocs != 0) {
        return ALLOC_NOT_FREED;
    }
    // free memory pool
    free(memPoolMgr->pool.mem);
    // free node heap
    free(memPoolMgr->node_heap);
    // free gap index
    free(memPoolMgr->gap_ix);
    // find mgr in pool store and set to null
    // note: don't decrement pool_store_size, because it only grows
    for(int i = 0; i < pool_store_size; i++) {
        if(pool_store[i] == memPoolMgr) {
            pool_store[i] = NULL;
        }
    }
    // free mgr
    free(memPoolMgr);
    return ALLOC_OK;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt memPoolMgr = (pool_mgr_pt) pool;
    // check if any gaps, return null if none
    if(memPoolMgr->pool.num_gaps == 0) {
        return NULL;
    }
    // expand heap node, if necessary, quit on error
    assert(_mem_resize_node_heap(memPoolMgr) == ALLOC_OK);
    // check used nodes fewer than total nodes, quit on error
    assert(memPoolMgr->total_nodes > memPoolMgr->used_nodes);
    // get a node for allocation:
    node_pt node = NULL;
    // if FIRST_FIT, then find the first sufficient node in the node heap
    if(memPoolMgr->pool.policy == FIRST_FIT) {
        node = &memPoolMgr->node_heap[0];
        int i = 1;
        while(i == 1) {
            if(node->allocated == 0 && node->alloc_record.size >= size) {
                i = 0;
            }
            else {
                node = node->next;
            }
        }
    }
    // if BEST_FIT, then find the first sufficient node in the gap index
    else if(memPoolMgr->pool.policy == BEST_FIT) {
        for(int i = 0; i < memPoolMgr->pool.num_gaps; i++) {
            if(memPoolMgr->gap_ix[i].size >= size) {
                node = memPoolMgr->gap_ix[i].node;
            }
        }
    }
    // check if node found
    assert(node != NULL);
    // update metadata (num_allocs, alloc_size)
    memPoolMgr->pool.num_allocs++;
    memPoolMgr->pool.alloc_size += size;
    // calculate the size of the remaining gap, if any
    int diff = node->alloc_record.size - size;
    // remove node from gap index
    _mem_remove_from_gap_ix(memPoolMgr, size, node);
    // convert gap_node to an allocation node of given size
    node->allocated = 1;
    node->alloc_record.size = size;
    // adjust node heap:
    //   if remaining gap, need a new node
    if(diff > 0) {
        node_pt newNode = NULL;
        for(int i = 0; i < memPoolMgr->total_nodes; i++) {
            if(memPoolMgr->node_heap[i].used == 0) {
                newNode = &memPoolMgr->node_heap[i];
            }
        }
        assert(newNode != NULL);
        newNode->allocated = 0;
        newNode->used = 1;
        newNode->alloc_record.size = diff;
        newNode->alloc_record.mem = node->alloc_record.mem + size;

        memPoolMgr->used_nodes++;

        newNode->next = node->next;
        if(newNode->next != NULL) {
            newNode->next->prev = newNode;
        }
        node->next = newNode;
        newNode->prev = node;

        if(_mem_add_to_gap_ix(memPoolMgr, diff, newNode) != ALLOC_OK) {
            return NULL;
        }
    }
    //   find an unused one in the node heap
    //   make sure one was found
    //   initialize it to a gap node
    //   update metadata (used_nodes)
    //   update linked list (new node right after the node for allocation)
    //   add to gap index
    //   check if successful
    // return allocation record by casting the node to (alloc_pt)

    return (alloc_pt) node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt memPoolMgr = (pool_mgr_pt) pool;
    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt) alloc;
    // find the node in the node heap
    int index = -1;
    for(int i = 0; i < memPoolMgr->total_nodes; i++) {
        if(node == &memPoolMgr->node_heap[i]) {
            index = i;
        }
    }
    // this is node-to-delete
    // make sure it's found
    assert(index != -1);
    // convert to gap node
    node->allocated = 0;
    // update metadata (num_allocs, alloc_size)
    memPoolMgr->pool.num_allocs--;
    memPoolMgr->pool.alloc_size -= node->alloc_record.size;
    // if the next node in the list is also a gap, merge into node-to-delete
    if(node->next != NULL) {
        if(node->next->allocated == 0) {
            size_t mergeSize = node->next->alloc_record.size;
            node->alloc_record.size += mergeSize;
            int a = _mem_remove_from_gap_ix(memPoolMgr, mergeSize, node->next);
            if(a != ALLOC_OK) {
                node->alloc_record.size -= mergeSize;
                return ALLOC_FAIL;
            }
            else {
                node->next->used = 0;
                memPoolMgr->used_nodes--;
                if(node->next->next != NULL) {
                    node->next->next->prev = node;
                    node->next = node->next->next;
                }
                else {
                    node->next = NULL;
                }
            }
        }
    }
    //   remove the next node from gap index
    //   check success
    //   add the size to the node-to-delete
    //   update node as unused
    //   update metadata (used nodes)
    //   update linked list:
    /*
                    if (next->next) {
                        next->next->prev = node_to_del;
                        node_to_del->next = next->next;
                    } else {
                        node_to_del->next = NULL;
                    }
                    next->next = NULL;
                    next->prev = NULL;
     */

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    //   remove the previous node from gap index
    //   check success
    //   add the size of node-to-delete to the previous
    //   update node-to-delete as unused
    //   update metadata (used_nodes)
    //   update linked list
    /*
                    if (node_to_del->next) {
                        prev->next = node_to_del->next;
                        node_to_del->next->prev = prev;
                    } else {
                        prev->next = NULL;
                    }
                    node_to_del->next = NULL;
                    node_to_del->prev = NULL;
     */
    if(node->prev != NULL && node->prev->allocated == 0) {
        size_t mergeSize = node->prev->alloc_record.size;
        node->alloc_record.size += mergeSize;
        if(_mem_remove_from_gap_ix(memPoolMgr, mergeSize, node->prev) != ALLOC_OK) {
            node->alloc_record.size -= mergeSize;
            return ALLOC_FAIL;
        }
        else {
            node->prev->used = 0;
            memPoolMgr->used_nodes--;
            if(node->prev->prev != NULL) {
                node->prev->prev->next = node;
                node->prev = node->prev->prev;
            }
            else {
                node->prev = NULL;
            }
        }
    }
    _mem_add_to_gap_ix(memPoolMgr, node->alloc_record.size, node);
    //   change the node to add to the previous node!
    // add the resulting node to the gap index
    // check success

    return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt memPoolMgr = (pool_mgr_pt) pool;
    // allocate the segments array with size == used_nodes
    // check successful
    pool_segment_pt segmentArray = calloc(memPoolMgr->used_nodes, sizeof(pool_segment_pt));
    assert(segmentArray != NULL);
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
    node_pt node = &memPoolMgr->node_heap[0];
    int i = 0;
    while(node != NULL) {
        segmentArray[i].allocated = node->allocated;
        segmentArray[i].size = node->alloc_record.size;
        node = node->next;
        i++;
    }
    *segments = segmentArray;
    *num_segments = memPoolMgr->used_nodes;
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    if(((float) pool_store_size / pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR) {
        pool_store_capacity = pool_store_capacity * MEM_POOL_STORE_EXPAND_FACTOR;
        pool_store = (pool_mgr_pt*) realloc(pool_store, sizeof(pool_mgr_pt) * pool_store_capacity);
        if(pool_store == NULL) {
            pool_store_capacity = pool_store_capacity / MEM_POOL_STORE_EXPAND_FACTOR;
            return ALLOC_FAIL;
        }
        else {
            return ALLOC_OK;
        }
    }
    else {
        return ALLOC_OK;
    }
    // don't forget to update capacity variables
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // see above
    if(((float) pool_mgr->used_nodes / pool_mgr->total_nodes) / MEM_NODE_HEAP_FILL_FACTOR) {
        pool_mgr->total_nodes *= MEM_NODE_HEAP_EXPAND_FACTOR;
        pool_mgr->node_heap = realloc(pool_mgr->node_heap, sizeof(node_pt) * pool_mgr->total_nodes);
        if(pool_mgr->node_heap == NULL) {
            pool_mgr->total_nodes = pool_mgr->total_nodes / MEM_NODE_HEAP_EXPAND_FACTOR;
            return ALLOC_FAIL;
        }
        else {
            return ALLOC_OK;
        }
    }
    else {
        return ALLOC_OK;
    }
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // see above
    if(((float) pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity) > MEM_GAP_IX_FILL_FACTOR) {
        pool_mgr->gap_ix_capacity *= MEM_GAP_IX_EXPAND_FACTOR;
        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, sizeof(gap_pt) * pool_mgr->gap_ix_capacity);
        if(pool_mgr->gap_ix == NULL) {
            pool_mgr->gap_ix_capacity /= MEM_GAP_IX_EXPAND_FACTOR;
            return ALLOC_FAIL;
        }
        else {
            return ALLOC_OK;
        }
    }
    else {
        return ALLOC_OK;
    }
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);
    // add the entry at the end
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps++;
    // sort the gap index (call the function)
    assert(_mem_sort_gap_ix(pool_mgr) == ALLOC_OK);
    // check success

    return ALLOC_OK;
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
    int index = -1;
    for(int i = 0; i < pool_mgr->pool.num_gaps; i++) {
        if(pool_mgr->gap_ix[i].node == node) {
            index = i;
        }
    }
    if(index == -1) {
        return ALLOC_FAIL;
    }
    else {
        for(int i = index; i < pool_mgr->pool.num_gaps - 1; i++) {
            pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i + 1];
        }
    }
    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps--;
    // zero out the element at position num_gaps!
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = 0;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    //    if the size of the current entry is less than the previous (u - 1)
    //    or if the sizes are the same but the current entry points to a
    //    node with a lower address of pool allocation address (mem)
    //       swap them (by copying) (remember to use a temporary variable)
    int start = pool_mgr->pool.num_gaps - 1;
    int point = start;
    for(int i = 0; i < start; i++) {
        if(pool_mgr->gap_ix[i].size > pool_mgr->gap_ix[start].size) {
            point = i;
        }
    }
    gap_t gap1 = pool_mgr->gap_ix[start];
    gap_t gap2 = pool_mgr->gap_ix[point];
    for(int i = point; i < start + 1; i++) {
        gap2 = pool_mgr->gap_ix[i];
        pool_mgr->gap_ix[i] = gap1;
        gap1 = gap2;
    }

    return ALLOC_OK;
}


