#ifndef _CORE_BTREE_SAVER_H_
#define _CORE_BTREE_SAVER_H_

#define T btsaver_t
typedef struct btsaver_s T;

struct btsaver_s {
    /* must be the first field */
    obj_t  super;

    int fd;
    int flag;
    char *file;
    conf_t *conf;
    hdr_block_t *hdr;

    /* public fields */
    struct btsaver_pri *pri;

    /* methods */
    int   (*init)(T *thiz, btree_t *base);
    void  (*destroy)(T *thiz);

    int   (*start)(T *thiz);
    int   (*save_kv)(T *thiz, mkv_t *kv);   
    int   (*save_fkv)(T *thiz, fkv_t *fkv);   
    int   (*flush)(T *thiz);
    void  (*finish)(T *thiz);
};

T *btsaver_create(pool_t *mpool);

#undef T
#endif

