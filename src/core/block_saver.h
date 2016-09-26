#ifndef _CORE_BLOCK_SAVER_H_
#define _CORE_BLOCK_SAVER_H_

#define T blksaver_t
typedef struct blksaver_s T;

typedef struct {
    void *buf;
    struct list_head page_node;
} blkpage_t;

struct blksaver_s {
    /* must be the first field */
    obj_t  super;

    int fd;
    int blktype;
    int blksize;
    int meta_size;
    int blkcnt;

    struct list_head page_list;
    hdr_block_t *hdr;

    /* public fields */
    struct blksaver_pri *pri;

    /* methods */
    void  (*destroy)(T *thiz);

    int   (*save_item)(T *thiz, fkv_t *item);   
    int   (*flush)(T *thiz);
};

T *blksaver_create(pool_t *mpool);

T *val_blksaver_create(pool_t *mpool);
T *leaf_blksaver_create(pool_t *mpool);
T *index_blksaver_create(pool_t *mpool);
T *filter_blksaver_create(pool_t *mpool);

#undef T
#endif

