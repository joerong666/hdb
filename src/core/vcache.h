#ifndef _CORE_VCACHE_H_
#define _CORE_VCACHE_H_

#define T vcache_t
typedef struct vcache_s T;

struct vcache_s {
    /* must be the first field */
    obj_t  super;

    int cap;
    rwlock_t lock;

    struct vcache_pri *pri;

    /* methods */
    int   (*init)(T *thiz);
    void  (*destroy)(T *thiz);
    void  (*release)(T *thiz);

    int   (*push)(T *thiz, char *id, void *data);   
    void *(*get)(T *thiz, char *id);
};

T *vcache_create(pool_t *mpool);

#undef T
#endif
