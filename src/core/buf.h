
/*
 * 
 */


#ifndef __BUF_H__
#define __BUF_H__


typedef struct buf
{
    uint8_t          *pos;           /* current data pos*/
    uint8_t          *last;          /* data end pos */
    uint8_t          *start;         /* start of buffer */
    uint8_t          *end;           /* end of buffer */
}buf_t;


typedef struct chain 
{
    buf_t         *buf;
    struct chain  *next;
}chain_t;


typedef struct bufs
{
    int32_t    num;
    size_t     size;
} bufs_t;


#define buf_size(b) (b->last - b->pos)
#define alloc_buf(pool)  palloc(pool, sizeof(buf_t))
#define calloc_buf(pool) pcalloc(pool, sizeof(buf_t))

buf_t *chain_create_temp_buf(pool_t *pool, size_t size);
chain_t *chain_create_chain_of_buf(pool_t *pool, size_t size);
chain_t *chain_create_chain_of_bufs(pool_t *pool, bufs_t *bufs);
chain_t *chain_alloc_link(pool_t *pool);
chain_t *chain_alloc_buflink(pool_t *pool);
int32_t chain_add_copy(pool_t *pool, chain_t **chain, chain_t *in);
int32_t chain_append(chain_t **chain, chain_t *in);
chain_t *chain_clone(pool_t *pool, chain_t *in);
void chain_reset(chain_t *in);
int32_t chain_get_len(const chain_t *in);
chain_t *chain_get_free_buf(pool_t *p, chain_t **free);
int32_t chain_del_last(chain_t **chain);
#endif /* __BUF_H__ */
