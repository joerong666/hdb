#include "inc.h"
#include "skiplist.h"
#include "htable.h"

#define T   htable_t

#define MAXCAP 10
#define SKLEVEL 10

struct htable_pri {
};
 
static int push_i(T *thiz, size_t pos, void *in_data, void **out_data, int safe)  
{
    int r = 0;
    skiplist_t *skl;

    skl = thiz->tbs[pos];

    if (safe) {
        r = skl->push(skl, in_data, out_data);
    } else {
        r = skl->push_unsafe(skl, in_data, out_data);
    }

    return r;
}

static int push(T *thiz, void *in_data, void **out_data)  
{
    int r;
    size_t pos = thiz->hfunc(in_data) % thiz->cap;  

    r = push_i(thiz, pos, in_data, out_data, 1);
    return r;
}  

static int push_unsafe(T *thiz, void *in_data, void **out_data)  
{
    int r;
    size_t pos = thiz->hfunc(in_data) % thiz->cap;  

    r = push_i(thiz, pos, in_data, out_data, 0);
    return r;
}  

static void *find_i(T *thiz, int pos, const void *data)  
{
    skiplist_t *skl = thiz->tbs[pos];

    return skl->find(skl, data);
}

static int exist_i(T *thiz, int pos, const void *data)  
{
    skiplist_t *skl = thiz->tbs[pos];

    return skl->exist(skl, data);
}

static void *find(T *thiz, const void *data)
{  
    void *r = NULL;
    int pos = thiz->hfunc(data) % thiz->cap;  

    r = find_i(thiz, pos, data);
    return r;
}

static int exist(T *thiz, const void *data)
{  
    int pos = thiz->hfunc(data) % thiz->cap;  
    int r = exist_i(thiz, pos, data);

    return r;
}

static int empty(T *thiz)
{
    int i;
    skiplist_t *sk;

    for (i = 0; i < thiz->cap; i++) {
        sk = thiz->tbs[i];
        if (!sk->empty(sk)) return 0;
    }

    return 1;
}

static htiter_t *get_iter(T *thiz, void *start, void *stop, HTITERCMP cmp)
{
    htiter_t *it;
    
    it = htiter_create(NULL);
    it->container = thiz;

    it->start = start;
    it->stop = stop;
    it->cmp = cmp;

    it->init(it);

    return it;
}

/****************************************
** basic function
*****************************************/
static int init(T *thiz)
{
    if (thiz->cap <= 0 || thiz->cap > MAXCAP) thiz->cap = MAXCAP;

    thiz->tbs = PCALLOC(SUPER->mpool, thiz->cap * sizeof(*thiz->tbs));
    if (thiz->tbs == NULL) return -1;

    int i;
    for (i = 0; i < thiz->cap; i++) {
        skiplist_t *skl = skiplist_create(SUPER->mpool);
        skl->maxlv = SKLEVEL;
        skl->cmp = thiz->cmp;
        skl->dfree_func = thiz->dfree_func;
        skl->init(skl);

        thiz->tbs[i] = skl;
    }

    return 0;
}

static void destroy_data(T *thiz)
{
    int i;
    skiplist_t *s;

    for (i = 0; i < thiz->cap; i++) {
        if (thiz->tbs[i] == NULL) continue;

        s = thiz->tbs[i];
        s->destroy_data(s);
    }
}

static void destroy(T *thiz)
{
    del_obj(thiz);
}

static int _init(T *thiz)
{
    thiz->cmp = NULL;
    thiz->hfunc = NULL;

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(destroy_data);
    ADD_METHOD(push);
    ADD_METHOD(push_unsafe);
    ADD_METHOD(find);
    ADD_METHOD(exist);
    ADD_METHOD(empty);
    ADD_METHOD(get_iter);

    return 0;
}

T *htable_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    _init(thiz);

    return thiz;           
}


