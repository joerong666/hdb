/*
 * Copyright (C) Igor Sysoev
 */

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "palloc.h"

#ifndef USE_ZMALLOC
static void *palloc_block(pool_t *pool, size_t size);
static void *palloc_large(pool_t *pool, size_t size);


pool_t *
pool_create(size_t size)
{
    pool_t  *p;

	size = (size>POOL_MIN_POOL_SIZE)?size:POOL_MIN_POOL_SIZE;

    if(posix_memalign((void **)(void *)&p, (size_t)POOL_ALIGNMENT, size))
    {
        return NULL;
    }

    p->d.last = (uint8_t *) p + sizeof(pool_t);
    p->d.end = (uint8_t *) p + size;
    p->d.next = NULL;
    p->d.failed = 0;

    size = size - sizeof(pool_t);
    p->max = (size < POOL_MAX_ALLOC_FROM_POOL) ? size : POOL_MAX_ALLOC_FROM_POOL;

    p->current = p;
    p->large = NULL;
    p->cleanup = NULL;
    return p;
}


void
pool_destroy(pool_t *pool)
{
    pool_t          *p, *n;
    pool_large_t    *l;
    pool_cleanup_t  *c;

    for (c = pool->cleanup; c; c = c->next) 
    {
        if (c->handler) 
        {
            c->handler(c->data);
        }
    }

    for (l = pool->large; l; l = l->next) 
    {

        if (l->alloc) 
        {
            free(l->alloc);
        }
    }

    for (p = pool, n = pool->d.next; /* void */; p = n, n = n->d.next) 
    {
        free(p);

        if (n == NULL) 
        {
            break;
        }
    }
}


/*reset the pool to have only one block*/
void
pool_reset(pool_t *pool)
{
    pool_t        *p;
	pool_t        *next;

    pool_large_t  *l;

    for (l = pool->large; l; l = l->next) 
    {
        if (l->alloc) 
        {
            free(l->alloc);
        }
    }

    pool->large = NULL;

    p = pool;

    /*Modify by hhq,only the first block offset pool_t*/
    p->d.last = (uint8_t *) p + sizeof(pool_t);    

    for (next = p->d.next; next ; ) 
    {
		p = next;
		next = next->d.next;
        free(p);
    }
	
	pool->d.next    = NULL;
	pool->d.failed  = 0;
	pool->current   = pool;
}


void *
palloc(pool_t *pool, size_t size)
{
    uint8_t      *m;
    pool_t  *p;

    if (size <= pool->max) 
    {

        p = pool->current;

        do 
        {
            m = align_ptr(p->d.last, sizeof(unsigned long));

            if ((size_t) (p->d.end - m) >= size) 
            {
                p->d.last = m + size;

                return m;
            }

            p = p->d.next;

        }while (p);

        return palloc_block(pool, size);
    }

    return palloc_large(pool, size);
}


void *
pnalloc(pool_t *pool, size_t size)
{
    uint8_t      *m;
    pool_t  *p;

    if (size <= pool->max) 
    {

        p = pool->current;

        do 
        {
            m = p->d.last;

            if ((size_t) (p->d.end - m) >= size) 
            {
                p->d.last = m + size;

                return m;
            }

            p = p->d.next;

        } while (p);

        return palloc_block(pool, size);
    }

    return palloc_large(pool, size);
}


static void *
palloc_block(pool_t *pool, size_t size)
{
    uint8_t      *m;
    size_t       psize;
    pool_t  *p, *new, *current;

    psize = (size_t) (pool->d.end - (uint8_t *) pool);

    if(posix_memalign((void **)(void *)&m, POOL_ALIGNMENT, psize))
    {
        return NULL;
    }

    new = (pool_t *) m;

    new->d.end = m + psize;
    new->d.next = NULL;
    new->d.failed = 0;

    m += sizeof(pool_data_t);
    m = align_ptr(m, sizeof(unsigned long));
    new->d.last = m + size;

    current = pool->current;

    for (p = current; p->d.next; p = p->d.next) 
    {
        if (p->d.failed++ > 4) 
        {
            current = p->d.next;
        }
    }

    p->d.next = new;

    pool->current = current ? current : new;

    return m;
}


static void *
palloc_large(pool_t *pool, size_t size)
{
    void              *p;
    uint32_t         n;
    pool_large_t  *large;

    p = malloc(size);
    if (p == NULL) 
    {
        return NULL;
    }

    n = 0;

    for (large = pool->large; large; large = large->next) 
    {
        if (large->alloc == NULL) 
        {
            large->alloc = p;
            return p;
        }

        if (n++ > 3) 
        {
            break;
        }
    }

    large = palloc(pool, sizeof(pool_large_t));
    if (large == NULL) 
    {
        free(p);
        return NULL;
    }

    large->alloc = p;
    large->next = pool->large;
    pool->large = large;

    return p;
}


void *
pmemalign(pool_t *pool, size_t size, size_t alignment)
{
    void              *p;
    pool_large_t  *large;

    if(posix_memalign(&p, alignment, size))
    {
        return NULL;
    }

    large = palloc(pool, sizeof(pool_large_t));
    if (large == NULL) 
    {
        free(p);
        return NULL;
    }

    large->alloc = p;
    large->next = pool->large;
    pool->large = large;

    return p;
}


int32_t
pfree(pool_t *pool, void *p)
{
    pool_large_t  *l;

    for (l = pool->large; l; l = l->next) 
    {
        if (p == l->alloc) 
        {
            free(l->alloc);
            l->alloc = NULL;

            return 0;
        }
    }

    return 0;
}


void *
pcalloc(pool_t *pool, size_t size)
{
    void *p;

    p = palloc(pool, size);
    if (p) 
    {
        memset(p,0,size);
    }

    return p;
}


pool_cleanup_t *
pool_cleanup_add(pool_t *p, size_t size)
{
    pool_cleanup_t  *c;

    c = palloc(p, sizeof(pool_cleanup_t));
    if (c == NULL) 
    {
        return NULL;
    }

    if (size) 
    {
        c->data = palloc(p, size);
        if (c->data == NULL) 
        {
            return NULL;
        }

    } 
    else 
    {
        c->data = NULL;
    }

    c->handler = NULL;
    c->next = p->cleanup;

    p->cleanup = c;

    return c;
}

void *prealloc(pool_t *pool, void *p, size_t old_size, size_t new_size) 
{ 
    void *new; 

    if (p == NULL) { 
        return palloc(pool, new_size); 
    } 

    if (new_size == 0) { 
        if ((u_char *) p + old_size == pool->d.last) { 
           pool->d.last = p; 
        } else { 
           pfree(pool, p); 
        } 

        return NULL; 
    } 

    if ((u_char *) p + old_size == pool->d.last 
        && (u_char *) p + new_size <= pool->d.end) 
    { 
        pool->d.last = (u_char *) p + new_size; 
        return p; 
    } 

    new = palloc(pool, new_size); 
    if (new == NULL) { 
        return NULL; 
    } 

    memcpy(new, p, old_size); 

    pfree(pool, p); 

    return new; 
} 

#else 
#include "my_malloc.h"

#define MY_Malloc my_malloc
#define MY_Calloc my_calloc
#define MY_Free my_free

typedef struct pnode_s pnode_t;

struct pnode_s {
    pnode_t *next;
    void *data;
};

typedef struct ompool_s {
    pool_t  mp;
    pnode_t *head;
    pnode_t *tail;
} ompool_t;

pool_t *pool_create(size_t size)
{
    (void)size;
    ompool_t *opool = MY_Calloc(sizeof(*opool));
    return &opool->mp;
}

void pool_reset(pool_t *pool)
{
    pnode_t *n, *t;
    ompool_t *opool = (ompool_t *)pool;

    n = opool->head;
    while (n) {
        t = n->next;
        MY_Free(n->data);
        MY_Free(n);
        n = t;
    }

    opool->head = opool->tail = NULL;
}

void pool_destroy(pool_t *pool)
{
    pool_reset(pool);
    
    MY_Free((ompool_t *)pool);
}

static void *alloc_i(pool_t *pool, size_t size, int zero)
{
    pnode_t *n;
    ompool_t *opool = (ompool_t *)pool;

    n = MY_Calloc(sizeof(*n));
    if (n == NULL) return NULL;

    if (zero) {
        n->data = MY_Calloc(size);
    } else {
        n->data = MY_Malloc(size);
    }

    if (n->data == NULL) {
        MY_Free(n);
        return NULL;
    }

    if (opool->head == NULL) {
        opool->head = opool->tail = n;
    } else {
        opool->tail->next = n;
        opool->tail = n;
    }

    return n->data;
}

void *palloc(pool_t *pool, size_t size)
{
    return alloc_i(pool, size, 0);
}

void *pcalloc(pool_t *pool, size_t size)
{
    return alloc_i(pool, size, 1);
}

int32_t pfree(pool_t *pool, void *p)
{
    (void)pool;
    (void)p;

    return 0;
}

#endif

