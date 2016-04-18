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
static node_pt mergeGaps(pool_mgr_pt poolManager, node_pt node, node_pt nextNode);



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
        return ALLOC_OK;
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
            mem_pool_close((pool_pt) pool_store[i]);
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
    //printf("mem_pool_open\n");
    // make sure there the pool store is allocated
    if(pool_store == NULL) {
        return NULL;
    }
    // expand the pool store, if necessary
    _mem_resize_pool_store();
    // allocate a new mem pool mgr
    pool_mgr_pt memPoolMgr = (pool_mgr_pt) malloc(sizeof(pool_mgr_t));
    // check success, on error return null
    if(memPoolMgr == NULL) {
        return NULL;
    }
    // allocate a new memory pool
    memPoolMgr->pool.mem = (char*) malloc(size);
    // check success, on error deallocate mgr and return null
    if(memPoolMgr->pool.mem == NULL) {
        free(memPoolMgr);
        return NULL;
    }
    // allocate a new node heap
    memPoolMgr->node_heap = (node_pt) calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    // check success, on error deallocate mgr/pool and return null
    if(memPoolMgr->node_heap == NULL) {
        free(memPoolMgr->pool.mem);
        free(memPoolMgr);
        return NULL;
    }
    // allocate a new gap index
    memPoolMgr->gap_ix = (gap_pt) calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
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
    memPoolMgr->gap_ix[0].node = memPoolMgr->node_heap;
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
    //printf("mem_pool_close\n");
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
    //printf("mem_new_alloc\n");
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
        node_pt currentNode= memPoolMgr->node_heap;
        while(currentNode != NULL) {
            if(currentNode->allocated == 0 && currentNode->alloc_record.size >= size && currentNode->used == 1) {
                node = currentNode;
                currentNode = NULL;
            }
            else {
                currentNode = currentNode->next;
            }
        }
    }
    // if BEST_FIT, then find the first sufficient node in the gap index
    else if(memPoolMgr->pool.policy == BEST_FIT) {
        for(int i = 0; i < memPoolMgr->gap_ix_capacity && node == NULL; i++) {
            if(memPoolMgr->gap_ix[i].size >= size) {
                node = memPoolMgr->gap_ix[i].node;
                if(i < memPoolMgr->gap_ix_capacity - 1 && memPoolMgr->gap_ix[i + 1].size == node->alloc_record.size) {
                    node_pt currentNode = memPoolMgr->node_heap;
                    while(currentNode != NULL) {
                        if(currentNode->used = 1 && currentNode->allocated == 0 && currentNode->alloc_record.size == node->alloc_record.size) {
                            node = currentNode;
                            currentNode = NULL;
                        }
                        else {
                            currentNode = currentNode->next;
                        }
                    }
                }
            }
        }
    }
    // check if node found
    if(node == NULL) {
        return NULL;
    }
    // update metadata (num_allocs, alloc_size)
    memPoolMgr->pool.num_allocs++;
    memPoolMgr->pool.alloc_size += size;
    // calculate the size of the remaining gap, if any
    int diff = node->alloc_record.size - size;
    if(diff == 0) {
        node->allocated = 1;
        assert(_mem_remove_from_gap_ix(memPoolMgr, node->alloc_record.size, node) == ALLOC_OK);
    }
    else {
        node_pt newNode = NULL;
        for(int i = 0; i < memPoolMgr->total_nodes; i++) {
            if(memPoolMgr->node_heap[i].used == 0) {
                newNode = &memPoolMgr->node_heap[i];
                break;
            }
        }
        assert(newNode != NULL);
        node->allocated = 1;
        node->alloc_record.size = size;
        _mem_remove_from_gap_ix(memPoolMgr, node->alloc_record.size, node);
        newNode->used = 1;
        newNode->alloc_record.mem = node->alloc_record.mem + size;
        newNode->alloc_record.size = diff;
        _mem_add_to_gap_ix(memPoolMgr, diff, newNode);
        newNode->next = node->next;
        if(newNode->next != NULL) {
            newNode->next->prev = newNode;
        }
        node->next = newNode;
        newNode->prev = node;
    }
    if(diff > 0) {
        memPoolMgr->used_nodes++;
    }

    // remove node from gap index
    // convert gap_node to an allocation node of given size
    // adjust node heap:
    //   if remaining gap, need a new node
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
    //printf("mem_del_alloc\n");
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt memPoolMgr = (pool_mgr_pt) pool;
    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt) alloc;
    // find the node in the node heap
    int index = -1;/*
    for(int i = 0; i < memPoolMgr->total_nodes; i++) {
        if(node->alloc_record.mem == memPoolMgr->node_heap[i].alloc_record.mem) {
            index = i;
        }
    }*/
    node_pt currentNode = memPoolMgr->node_heap;
    while(currentNode != NULL && currentNode->used == 1) {
        if(node->alloc_record.mem == currentNode->alloc_record.mem) {
            node = currentNode;
            currentNode = NULL;
            index = 0;
        }
        else {
            currentNode = currentNode->next;
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
    node_pt finalNode = node;
    _mem_add_to_gap_ix(memPoolMgr, finalNode->alloc_record.size, finalNode);
    if(node->next != NULL && node->next->used == 1 && node->next->allocated == 0) {
        finalNode = mergeGaps(memPoolMgr, node, node->next);
    }
    if(finalNode->prev != NULL && finalNode->prev->allocated == 0) {
        finalNode = mergeGaps(memPoolMgr, finalNode->prev, finalNode);
    }
    // if the next node in the list is also a gap, merge into node-to-delete
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
    //   change the node to add to the previous node!
    // add the resulting node to the gap index
    // check success

    return ALLOC_OK;
}
static node_pt mergeGaps(pool_mgr_pt poolManager, node_pt node, node_pt nextNode) {
    assert(node->allocated == 0);
    assert(nextNode->allocated == 0);
    _mem_remove_from_gap_ix(poolManager, nextNode->alloc_record.size, nextNode);
    _mem_remove_from_gap_ix(poolManager, node->alloc_record.size, node);
    node->alloc_record.size += nextNode->alloc_record.size;
    nextNode->alloc_record.size = 0;
    nextNode->alloc_record.mem = NULL;
    nextNode->used = 0;
    poolManager->used_nodes--;
    if(nextNode->next != NULL) {
        node->next = nextNode->next;
        nextNode->next->prev = node;
    }
    else {
        node->next = NULL;
    }
    nextNode->next = NULL;
    nextNode->prev = NULL;
    _mem_add_to_gap_ix(poolManager, node->alloc_record.size, node);
    return node;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    //printf("mem_inspect_pool\n");
    // get the mgr from the pool
    pool_mgr_pt memPoolMgr = (pool_mgr_pt) pool;
    // allocate the segments array with size == used_nodes
    // check successful
    pool_segment_pt segmentArray = calloc(memPoolMgr->used_nodes, sizeof(pool_segment_t));
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
    node_pt node = memPoolMgr->node_heap;
    int i = 0;
    while(node != NULL && node->used == 1) {
        segmentArray[i].allocated = node->allocated;
        segmentArray[i].size = node->alloc_record.size;
        node = node->next;
        i++;
    }
    *segments = segmentArray;
    *num_segments = memPoolMgr->used_nodes;
    //printf("leaving mem_inspect_pool\n");
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
        pool_store = (pool_mgr_pt*) realloc(pool_store, sizeof(pool_mgr_pt) * pool_store_capacity * MEM_EXPAND_FACTOR);
        if(pool_store == NULL) {
            return ALLOC_FAIL;
        }
        else {
            pool_store_capacity *= MEM_EXPAND_FACTOR;
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
    if(((float) pool_mgr->used_nodes / pool_mgr->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR) {
        pool_mgr->node_heap = realloc(pool_mgr->node_heap, sizeof(node_pt) * pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR);
        if(pool_mgr->node_heap == NULL) {
            return ALLOC_FAIL;
        }
        else {
            pool_mgr->total_nodes *= MEM_NODE_HEAP_EXPAND_FACTOR;
            return ALLOC_OK;
        }
    }
    else {
        return ALLOC_OK;
    }
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    //printf("_mem_resize_gap_ix\n");
    // see above
    /*if(((float) pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity) > MEM_GAP_IX_FILL_FACTOR) {
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
    }*/
    int arrayFull = 0;
    for(int i = 0; i < pool_mgr->gap_ix_capacity; i++) {
        if(pool_mgr->gap_ix[i].node == NULL) {
            arrayFull = 1;
            break;
        }
    }
    if(arrayFull == 1) {
        return ALLOC_FAIL;
    }
    else {
        return ALLOC_OK;
    }
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {
    //printf("_mem_add_to_gap_ix\n");

    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);
    // add the entry at the end
    for(int i = 0; i < pool_mgr->gap_ix_capacity; i++) {
        if(pool_mgr->gap_ix[i].size == 0) {
            pool_mgr->gap_ix[i].size = size;
            pool_mgr->gap_ix[i].node = node;
            i = pool_mgr->gap_ix_capacity;
        }
    }
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
    //printf("_mem_remove_from_gap_ix\n");
    // find the position of the node in the gap index
    int index = -1;
    for(int i = 0; i < pool_mgr->pool.num_gaps; i++) {
        if(pool_mgr->gap_ix[i].node == node) {
            index = i;
        }
    }
    if(index == -1) {
        //printf("_mem_remove_from_gap_ix fail\n");
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
    //printf("_mem_sort_gap_ix\n");
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    //    if the size of the current entry is less than the previous (u - 1)
    //    or if the sizes are the same but the current entry points to a
    //    node with a lower address of pool allocation address (mem)
    //       swap them (by copying) (remember to use a temporary variable)
    /*int start = pool_mgr->pool.num_gaps - 1;
    int point = start;
    for(int i = 0; i < start; i++) {
        if(pool_mgr->gap_ix[i].size > pool_mgr->gap_ix[start].size) {
            printf("%d > %d\n", (int) pool_mgr->gap_ix[i].size, (int) pool_mgr->gap_ix[start].size);
            point = i;
        }
        else {
            printf("%d < %d\n", (int) pool_mgr->gap_ix[i].size, (int) pool_mgr->gap_ix[start].size);
        }
    }
    gap_t gap1 = pool_mgr->gap_ix[start];
    gap_t gap2 = pool_mgr->gap_ix[point];
    for(int i = point; i < start + 1; i++) {
        gap2 = pool_mgr->gap_ix[i];
        pool_mgr->gap_ix[i] = gap1;
        gap1 = gap2;
    }*/
    for(int i = pool_mgr->pool.num_gaps - 1; i > 0; i--) {
        for(int j = 0; j < i; j++) {
            if(pool_mgr->gap_ix[j].size < pool_mgr->gap_ix[j + 1].size) {
                size_t tempSize = pool_mgr->gap_ix[j].size;
                node_pt tempNode = pool_mgr->gap_ix[j].node;
                pool_mgr->gap_ix[j].size = pool_mgr->gap_ix[j + 1].size;
                pool_mgr->gap_ix[j].node = pool_mgr->gap_ix[j + 1].node;
                pool_mgr->gap_ix[j + 1].size = tempSize;
                pool_mgr->gap_ix[j + 1].node = tempNode;
            }
        }
    }

    for(int i = 0; i < pool_mgr->pool.num_gaps; i++) {
        printf("%d\n", (int) pool_mgr->gap_ix[i].size);
    }
    return ALLOC_OK;
}


