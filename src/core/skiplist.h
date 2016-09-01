#ifndef __CORE_SKIPLIST_H__
#define __CORE_SKIPLIST_H__

#define T skiplist_t
typedef struct skiplist_s T;
typedef struct skiter_s   skiter_t;

/* @n: new data
 * @o: old data
 */
typedef int   (*SKCMP)(const void *n, const void *o);

/* how to handle collision
 * @n: new data
 * @o: old data
 * @ret: return the target data which will assign to element's data
 */
typedef void *(*SKCOLLIDE)(void *n, void *o);

typedef int (*SKITERCMP)(void *data, void *start, void *stop);

typedef struct skl_node_s {
    int lv;
    void *data;

    struct skl_lv {
        struct skl_node_s *fw;
    } lvs[1];
} skl_node;

struct skiplist_s {
    /* must be the first field */
    obj_t  super;

    int lv;
    int maxlv;
    rwlock_t lock;
    skl_node *head;

    /* public fields */
    SKCMP cmp;
    SKCOLLIDE collide;
    void (*dfree_func)(void *d);

    struct skiplist_pri *pri;

    /* methods */
    int   (*init)(T *thiz);
    void  (*destroy)(T *thiz);
    void  (*destroy_data)(T *thiz);

    /* if existed, return old data 
     * @ret:  0: push succ, @out_data is set null
     *       -1: push fail
     *        1: exist, @out_data placed the old data
     */
    int   (*push)(T *thiz, void *in_data, void **out_data);   
    int   (*push_unsafe)(T *thiz, void *in_data, void **out_data);   
    void *(*find)(T *thiz, const void *data);
    int   (*exist)(T *thiz, const void *data);
    int   (*empty)(T *thiz);

    skiter_t *(*get_iter)(T *thiz, void *start, void *stop, SKITERCMP cmp);
};

struct skiter_s {
    obj_t  super;

    T *container;
    void *start;
    void *stop;
    SKITERCMP cmp;
    struct skiter_pri *pri;

    int   (*init)(skiter_t *it);
    int   (*has_next)(skiter_t *it);
    int   (*next)(skiter_t *it);
    int   (*get)(skiter_t *it, void **out_data);
    int   (*get_next)(skiter_t *it, void **out_data);
    void  (*destroy)(skiter_t *it);
};

T *skiplist_create(pool_t *mpool);
skiter_t *skiter_create(pool_t *mpool);

#undef T
#endif

