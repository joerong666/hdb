#ifndef _CORE_BTREE_H_
#define _CORE_BTREE_H_

#include "htable.h"

#define T btree_t
typedef struct btree_s T;
typedef struct btriter_s btriter_t;
typedef int (*KCMP)(mkey_t *o, mkey_t *n);
typedef int (*KFILTER)(void *arg, fkv_t *fkv);
typedef int (*BTITERCMP)(mkey_t *data, mkey_t *start, mkey_t *stop);

struct btree_s {
    /* must be the first field */
    obj_t  super;

    int rfd;
    int wfd;
    rwlock_t lock;
    char *file;
    conf_t *conf;
    hdr_block_t *hdr;
    
    /* public fields */
    struct btree_pri *pri;

    /* methods */
    void  (*destroy)(T *thiz);

    int   (*store)(T *thiz, htable_t *htb);   
    int   (*restore)(T *thiz);
    int   (*merge_start)(T *thiz, int (*filter)(fkv_t *));   
    int   (*merge)(T *thiz, fkv_t *nkv);   
    int   (*merge_flush)(T *thiz);   
    int   (*merge_hdr)(T *thiz);   
    int   (*merge_fin)(T *thiz);   
    off_t (*find_in_index)(T *thiz, off_t ioff, mkey_t *key, KCMP cmp);
    int   (*find)(T *thiz, mkey_t *key, mval_t *v);
    int   (*exist)(T *thiz, mkey_t *key, uint64_t ver);
    int   (*range_cmp)(T *thiz, T *other);
    int   (*krange_cmp)(T *thiz, mkey_t *k);
    int   (*pkrange_cmp)(T *thiz, mkey_t *k);
    int   (*split)(T *thiz, T *part1, T *part2);
    int   (*shrink)(T *thiz, T *newer);
    int   (*invalid)(T *thiz);

    btriter_t *(*get_iter)(T *thiz, mkey_t *start, mkey_t *stop, BTITERCMP cmp);
};

struct btriter_s {
    obj_t  super;

    T *container;
    mkey_t *start;
    mkey_t *stop;
    BTITERCMP cmp;

    struct btriter_pri *pri;

    int   (*init)(btriter_t *it);
    void  (*destroy)(btriter_t *it);

    int   (*has_next)(btriter_t *it);
    int   (*next)(btriter_t *it);
    int   (*get)(btriter_t *it, fkv_t **fkv);
    int   (*get_next)(btriter_t *it, fkv_t **fkv);
};

T *btree_create(pool_t *mpool);
btriter_t *btriter_create(pool_t *mpool);

#undef T
#endif

