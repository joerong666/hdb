#include "inc.h"
#include "htable.h"

#define T   htiter_t

struct htiter_pri {
    int flag;
    int next_flag;
    int sub_itscnt;

    skiter_t *cur_sit;
};

static int has_next(htiter_t *thiz)
{
    int r = 0, i;
    skiter_t *sit;
    skiter_t *cur_sit = SELF->cur_sit;

    if (SELF->next_flag) return 1;

    if (cur_sit != NULL && cur_sit->has_next(cur_sit)) {
        r = 1;
    } else {
        cur_sit = NULL;

        for (i = 0; i < SELF->sub_itscnt; i++) {
            sit = thiz->sub_its[i];

            if (sit->has_next(sit)) {
                r = 1;
                cur_sit = sit;
                break;
            }
        }

        SELF->cur_sit = cur_sit;
        if (cur_sit == NULL) return 0;
    }

    SELF->next_flag = r;
    return r;
}

static int next(htiter_t *thiz)
{
    int r, i;
    skiter_t *sit;
    skiter_t *cur_sit = SELF->cur_sit;
    void *cur_d, *var_d;

    SELF->next_flag = 0;

    /* select the smallest as current sit */
    for (i = 0; i < SELF->sub_itscnt; i++) {
        sit = thiz->sub_its[i];

        if (sit == cur_sit) continue;
        if (!sit->has_next(sit)) continue;

        cur_sit->get_next(cur_sit, &cur_d);
        sit->get_next(sit, &var_d);

        r = thiz->container->cmp(cur_d, var_d);
        if (r <= 0) continue;

        cur_sit = sit;
    }

    cur_sit->next(cur_sit);
    SELF->cur_sit = cur_sit;

    return 0;
}

static int get(htiter_t *thiz, void **out_data)
{
    skiter_t *cur_sit = SELF->cur_sit;

    return cur_sit->get(cur_sit, out_data);
}

static int init(htiter_t *thiz)
{
    int i, j;
    skiplist_t *s;
    skiter_t *sit;

    thiz->sub_its = (skiter_t **)PALLOC(SUPER->mpool, sizeof(void *) * thiz->container->cap);

    for (i = 0, j = 0; i < thiz->container->cap; i++) {
        s = thiz->container->tbs[i];
        sit = s->get_iter(s, thiz->start, thiz->stop, thiz->cmp);

        if (sit->has_next(sit)) {
            thiz->sub_its[j++] = sit;
        } else {
            sit->destroy(sit);
        }
    }

    SELF->sub_itscnt = j;

    return 0;
}

static void  destroy(htiter_t *thiz)
{
    int i;
    skiter_t *sit;

    for (i = 0; i < SELF->sub_itscnt; i++) {
        sit = thiz->sub_its[i];
        sit->destroy(sit);
    }

    del_obj(thiz);
}

htiter_t *htiter_create(pool_t *mpool)
{
    htiter_t *thiz;

    thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(has_next);
    ADD_METHOD(get);
    ADD_METHOD(next);

    return thiz;
}

