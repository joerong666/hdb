#include    "inc.h"
#include    "skiplist.h"

#define T   skiter_t

struct skiter_pri {
    skl_node *cur_pos;    
};

static skl_node *search_first(T *thiz)
{
	int i, c;
	skl_node *x, *fw;
    skiplist_t *skl = thiz->container;

    if (thiz->start == NULL && thiz->stop == NULL) return skl->head;
    if (thiz->cmp == NULL) return skl->head;

	x = skl->head;
	for (i = skl->lv - 1; i >= 0; i--) {
#if 0
		while (x->lvs[i].fw && (c = thiz->cmp(x->lvs[i].fw->data, thiz->start, thiz->stop)) < 0) {
			x = x->lvs[i].fw;
		}
#else
        fw = x->lvs[i].fw;

		while (fw && (c = thiz->cmp(fw->data, thiz->start, thiz->stop)) < 0) {
			x = fw;
            fw = fw->lvs[i].fw;
		}
#endif
    }

    return x;
}

static int has_next(skiter_t *thiz)
{
    int r;
    skl_node *x, *fw;


    if (SELF->cur_pos == NULL || SELF->cur_pos->lvs[0].fw == NULL) return 0;
    if (thiz->start == NULL && thiz->stop == NULL) return 1;
    if (thiz->cmp == NULL) return 1;

    x = SELF->cur_pos;
    if (x == thiz->container->head) {
        x = search_first(thiz);
    }

    SELF->cur_pos = x;
    fw = x->lvs[0].fw;
    if (fw == NULL) return 0;

    r = thiz->cmp(fw->data, thiz->start, thiz->stop);
    return (r == 0);
}

static int next(skiter_t *thiz)
{
    SELF->cur_pos = SELF->cur_pos->lvs[0].fw;

    return 0;
}

static int get(skiter_t *thiz, void **out_data)
{
    *out_data = SELF->cur_pos->data;

    return 1;
}

static int get_next(skiter_t *thiz, void **out_data)
{
    *out_data = SELF->cur_pos->lvs[0].fw->data;

    return 1;
}

static void  destroy(skiter_t *thiz)
{
    RWUNLOCK(&thiz->container->lock);
    del_obj(thiz);
}

static int init(skiter_t *thiz)
{
    /* unlock in destroy */
    RWLOCK_READ(&thiz->container->lock);

    SELF->cur_pos = thiz->container->head;
    return 0;
}

skiter_t *skiter_create(pool_t *mpool)
{
    skiter_t *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(has_next);
    ADD_METHOD(next);
    ADD_METHOD(get);
    ADD_METHOD(get_next);

    return thiz;
}

