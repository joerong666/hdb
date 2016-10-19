#ifndef _CORE_VCACHE_H_
#define _CORE_VCACHE_H_

#define T vcache_t
typedef struct vcache_s T;

struct vcache_s {
    /* must be the first field */
    obj_t  super;

    int cap;
    int size;
    rwlock_t lock;

    struct vcache_pri *pri;

    /* methods */
    int   (*init)(T *thiz);
    void  (*destroy)(T *thiz);

    int   (*push)(T *thiz, uint32_t id, void *data);   
    void *(*get)(T *thiz, uint32_t id);
};

T *vcache_create(pool_t *mpool);

#undef T
#endif
