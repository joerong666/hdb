
/*
 * Borrowed from Nginx, Copyright (C) Igor Sysoev
 */


#ifndef __PALLOC_H__
#define __PALLOC_H__

#include <stdint.h>

/*
 * POOL_MAX_ALLOC_FROM_POOL should be (pagesize - 1), i.e. 4095 on x86.
 * On Windows NT it decreases a number of locked pages in a kernel.
 */

#define align_mem(d, a)     (((d) + (a - 1)) & ~(a - 1))
#define align_ptr(p, a)                                                   \
    (uint8_t *) (((uintptr_t) (p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))

#define POOL_MAX_ALLOC_FROM_POOL  (4096 - 1)

#define POOL_DEFAULT_POOL_SIZE    (16 * 1024)

#define POOL_ALIGNMENT       16
#define POOL_MIN_POOL_SIZE                                                     \
    align_mem((sizeof(pool_t) + 2 * sizeof(pool_large_t)),            \
              POOL_ALIGNMENT)


typedef void (*pool_cleanup_pt)(void *data);

typedef struct pool_cleanup 
{
    pool_cleanup_pt      handler;
    void                 *data;
    struct pool_cleanup  *next;
}pool_cleanup_t;


typedef struct pool_large 
{
    struct pool_large     *next;
    void                  *alloc;
}pool_large_t;


typedef struct pool pool_t;

typedef struct pool_data
{
    uint8_t               *last;
    uint8_t               *end;
    pool_t                *next;
    uint32_t              failed;
} pool_data_t;


struct pool 
{
    pool_data_t      d;
    size_t           max;
    pool_t           *current;
    pool_large_t     *large;
    pool_cleanup_t   *cleanup;
};

pool_t *pool_create(size_t size);
void pool_destroy(pool_t *pool);
void pool_reset(pool_t *pool);

void *palloc(pool_t *pool, size_t size);
void *pnalloc(pool_t *pool, size_t size);
void *pcalloc(pool_t *pool, size_t size);
void *pmemalign(pool_t *pool, size_t size, size_t alignment);
void *prealloc(pool_t *pool, void *p, size_t old_size, size_t new_size);
int32_t pfree(pool_t *pool, void *p);


pool_cleanup_t *pool_cleanup_add(pool_t *p, size_t size);

#endif /* _POOL_PALLOC_H_INCLUDED_ */
