
/*
 * 
 */
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "palloc.h"

#include "buf.h"

buf_t *
chain_create_temp_buf(pool_t *pool, size_t size)
{
    buf_t *b;
    b = calloc_buf(pool);
    if (b == NULL) 
    {
        return NULL;
    }

    b->start = palloc(pool, size);
    if (b->start == NULL) 
    {
        return NULL;
    }

    /*
     * set by calloc_buf():
     *
     *     b->file_pos = 0;
     *     b->file_last = 0;
     *     b->file = NULL;
     *     b->shadow = NULL;
     *     b->tag = 0;
     *     and flags
     */

    b->pos = b->start;
    b->last = b->start;
    b->end = b->last + size;

    return b;
}


chain_t *
chain_alloc_link(pool_t *pool)
{
    chain_t  *cl;

    cl = palloc(pool, sizeof(chain_t));
    if (cl == NULL) 
    {
        return NULL;
    }

    cl->next = NULL;
    return cl;
}

chain_t *chain_alloc_buflink(pool_t *pool)
{
    chain_t  *cl;

    cl = pcalloc(pool, sizeof(chain_t));
    if (cl == NULL) 
    {
        return NULL;
    }
    cl->buf = pcalloc(pool, sizeof(buf_t));
    if (cl->buf == NULL)
    {
        return NULL;
    }

    cl->next = NULL;
    return cl;
}

chain_t *
chain_create_chain_of_buf(pool_t *pool, size_t size)
{
    chain_t *chain;
    
    chain = chain_alloc_link(pool);
    if(chain == NULL)
    {
        return NULL;
    }
    else
    {
        chain->buf = chain_create_temp_buf(pool, size);
        chain->next = NULL;
        if( chain->buf == NULL )
        {
            return NULL;
        }
    }
    return chain;
}

chain_t *
chain_create_chain_of_bufs(pool_t *pool, bufs_t *bufs)
{
    uint8_t       *p;
    int32_t     i;
    buf_t    *b;
    chain_t  *chain, *cl, **ll;

    p = palloc(pool, bufs->num * bufs->size);
    if (p == NULL) {
        return NULL;
    }

    ll = &chain;

    for (i = 0; i < bufs->num; i++) 
    {

        b = calloc_buf(pool);
        if (b == NULL) 
        {
            return NULL;
        }

        /*
         * set by calloc_buf():
         *
         *     b->file_pos = 0;
         *     b->file_last = 0;
         *     b->file = NULL;
         *     b->shadow = NULL;
         *     b->tag = 0;
         *     and flags
         *
         */

        b->pos = p;
        b->last = p;

        b->start = p;
        p += bufs->size;
        b->end = p;

        cl = chain_alloc_link(pool);
        if (cl == NULL) 
        {
            return NULL;
        }

        cl->buf = b;
        *ll = cl;
        ll = &cl->next;
    }

    *ll = NULL;

    return chain;
}


int32_t
chain_add_copy(pool_t *pool, chain_t **chain, chain_t *in)
{
    chain_t  *cl, **ll;

    ll = chain;

    for (cl = *chain; cl; cl = cl->next) 
    {
        ll = &cl->next;
    }

    while (in) {
        cl = chain_alloc_link(pool);
        if (cl == NULL) 
        {
            return 1;
        }

        cl->buf = in->buf;
        *ll = cl;
        ll = &cl->next;
        in = in->next;
    }

    *ll = NULL;

    return 0;
}


chain_t *
chain_get_free_buf(pool_t *p, chain_t **free)
{
    chain_t  *cl;

    if (*free) 
    {
        cl = *free;
        *free = cl->next;
        cl->next = NULL;
        return cl;
    }

    cl = chain_alloc_link(p);
    if (cl == NULL) 
    {
        return NULL;
    }

    cl->buf = calloc_buf(p);
    if (cl->buf == NULL) 
    {
        return NULL;
    }

    cl->next = NULL;

    return cl;
}

int32_t
chain_append(chain_t **chain, chain_t *in)
{
    if (chain == 0 || in == 0)
    {
        return 1;
    }
    chain_t **p = chain;

    while (*p)
    {
        p = &(*p)->next;
    }
    *p = in;
    return 0;
}

chain_t *chain_clone(pool_t *pool, chain_t *in)
{
    chain_t *ret = 0;
    chain_t **ptr = &ret;
    chain_t *tmp = 0;

    while (in && in->buf)
    {
        tmp = chain_alloc_buflink(pool);
        if (tmp == 0)
        {
            return 0;
        }
        memcpy(tmp->buf, in->buf, sizeof(buf_t));
        *ptr = tmp;
        ptr = &tmp->next;

        in = in->next;
    }

    return ret;
}

void chain_reset(chain_t *in)
{
    while (in && in->buf)
    {
        in->buf->pos = in->buf->start;
        in = in->next;
    }
}

int32_t chain_get_len(const chain_t *in)
{
    int32_t len = 0;

    while (in && in->buf)
    {
        len += in->buf->end - in->buf->start;
        in = in->next;
    }

    return len;
}

int32_t
chain_del_last(chain_t **chain)
{
    if (chain == 0)
    {
        return -1;
    }
    chain_t **p = chain;

    while (*p)
    {
		if ((*p)->next == NULL) 
		{
			*p = NULL;
			break;		
		}
        p = &(*p)->next;
    }
    return 0;
}

int32_t
get_chain_len(chain_t *chain)
{
	int32_t chain_count = 0;
	while(chain != NULL)
	{
		chain_count++;
		chain = chain->next;
	}
	return chain_count;
}
