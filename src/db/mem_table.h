#ifndef _HDB_MEM_TABLE_H_
#define _HDB_MEM_TABLE_H_

#include "htable.h"

#define T mtb_t
typedef struct mtb_s T;
typedef struct mtbset_s mtbset_t;

struct mtb_s {
    /* must be the first field */
    obj_t  super;

    /* public fields */
    char file[G_MEM_MID];
    int fd;
    int flag;
    rwlock_t lock;
    conf_t *conf;
    htable_t *model;
    struct list_head mnode;

    struct mtb_pri *pri;

    /* methods */
    int   (*init)(T *thiz);
    void  (*destroy)(T *thiz);
    void  (*clean)(T *thiz);

    int   (*push)(T *thiz, mkv_t *kv);
    int   (*push_unsafe)(T *thiz, mkv_t *kv);
    int   (*find)(T *thiz, const mkey_t *k, mval_t *v);
    int   (*exist)(T *thiz, const mkey_t *k, uint64_t ver);
    int   (*full)(T *thiz);
    int   (*restore)(T *thiz);
    int   (*write_ready)(T *thiz);
    int   (*write_bin)(T *thiz);
    int   (*empty)(T *thiz);
    int   (*mem_limit)(T *thiz);

    int   (*flush)(T *thiz);
    void  (*flush_wait)(T *thiz);
    void  (*flush_notify)(T *thiz);
};

struct mtbset_s {
    /* must be the first field */
    obj_t  super;

    int mlist_len;
    rwlock_t lock;
    struct list_head mlist;
    struct mtbset_pri *pri;

    /* methods */
    void    (*destroy)(mtbset_t *thiz);

    int     (*len)(mtbset_t *thiz);
    int     (*empty)(mtbset_t *thiz);
    int     (*push)(mtbset_t *thiz, T *item);
    int     (*trypush)(mtbset_t *thiz, T *item);
    int     (*find)(mtbset_t *thiz, const mkey_t *k, mval_t *v);
    int     (*exist)(mtbset_t *thiz, const mkey_t *k, mtb_t *until_pos, uint64_t ver);
    mtb_t  *(*pop)(mtbset_t *thiz);
    mtb_t  *(*top)(mtbset_t *thiz);
    mtb_t  *(*tail)(mtbset_t *thiz);

    int     (*flush)(mtbset_t *thiz);
    void    (*flush_wait)(mtbset_t *thiz);
    void    (*flush_notify)(mtbset_t *thiz);
};

T *mtb_create(pool_t *mpool);
mtbset_t *mtbset_create(pool_t *mpool);

#undef T
#endif

