#ifndef _HDB_IMPL_H_
#define _HDB_IMPL_H_

#include "db_com_def.h"
#include "mem_table.h"
#include "file_table.h"

#define T dbimpl_t
typedef struct dbimpl_s T;
typedef struct dbit_impl_s dbit_impl_t;

struct dbimpl_s {
    /* must be the first field */
    obj_t  super;

    /* public fields */
    conf_t *conf;
    ftbset_t **fsets;
    struct dbimpl_pri *pri;

    /* methods */
    void  (*destroy)(T *thiz);

    int   (*open)(T *thiz, const char *path, const char *conf_str);
    int   (*config)(T *thiz, const char *conf_str);   
    int   (*prepare)(T *thiz);   
    int   (*put)(T *thiz, mkv_t *kv);   
    int   (*del)(T *thiz, mkey_t *k);   
    int   (*get)(T *thiz, mkey_t *k, mval_t *v);   
    int   (*mput)(T *thiz, mkv_t *kvs, size_t cnt);   
    int   (*mdel)(T *thiz, mkey_t *keys, size_t cnt);   
    int   (*pdel)(T *thiz, mkey_t *prefix);   
    int   (*exist)(T *thiz, mkey_t *k);   
    int   (*recover)(T *thiz);
    int   (*repaire)(T *thiz);
    int   (*flush)(T *thiz);   
    int   (*checkpoint)(T *thiz);   

    void  (*close)(T *thiz);   

    dbit_impl_t *(*get_iter)(T *thiz, mkey_t *start, mkey_t *stop);   

#if 0   /* TODO!! */
    char        *(*get_conf)(T *thiz);   
#endif
};


struct dbit_impl_s {
    obj_t  super;

    uint32_t flag;
    uint64_t version;
    T *container;
    mtb_t *mmtb;
    mtbset_t *imq;
    mkey_t start;
    mkey_t stop;
    struct dbit_impl_pri *pri;

    int   (*init)(struct dbit_impl_s *it);
    /*
     * 1: success
     * 0: finished
     * -1: error
     */
    int   (*next)(struct dbit_impl_s *it, mkv_t *kv);
    void  (*destroy)(struct dbit_impl_s *it);
};

T *dbimpl_create(pool_t *mpool);
dbit_impl_t *dbit_impl_create(pool_t *mpool);

#undef T
#endif

