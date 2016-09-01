#include "hdb_inc.h"
#include "mem_table.h"

#define T mtbset_t
#define BIN_HEADER_SIZE 64

struct mtbset_pri {
};

/******************************************
 *  for mtbset
 ******************************************/
static void destroy(T *thiz)
{
    mtb_t *it, *saver;

    list_for_each_entry_safe(it, saver, &thiz->mlist, mnode) {
        MY_LIST_DEL(&it->mnode, &thiz->mlist_len);
        it->destroy(it);
    }

    del_obj(thiz);
}

static int  len(T *thiz)
{
    return thiz->mlist_len;
}

static int  empty(T *thiz)
{
    int r;

    RWLOCK_READ(&thiz->lock);
    r = list_empty(&thiz->mlist);
    RWUNLOCK(&thiz->lock);

    return r;
}

static int  push(T *thiz, mtb_t *item)
{
    RWLOCK_WRITE(&thiz->lock);
    MY_LIST_ADD(&item->mnode, &thiz->mlist, &thiz->mlist_len);
    RWUNLOCK(&thiz->lock);

    return 0;
}

static int  trypush(T *thiz, mtb_t *item)
{
    int r;
    r = RWLOCK_TRY_WRITE(&thiz->lock);
    if (r != 0) return r;

    MY_LIST_ADD(&item->mnode, &thiz->mlist, &thiz->mlist_len);
    RWUNLOCK(&thiz->lock);

    return 0;
}

static mtb_t   *top(T *thiz)
{
    mtb_t *item;

    RWLOCK_READ(&thiz->lock);
    if (list_empty(&thiz->mlist)) item = NULL;
    else item = list_last_entry(&thiz->mlist, typeof(*item), mnode);
    RWUNLOCK(&thiz->lock);

    return item;
}

static mtb_t   *tail(T *thiz)
{
    mtb_t *item;

    RWLOCK_READ(&thiz->lock);
    if (list_empty(&thiz->mlist)) item = NULL;
    else item = list_first_entry(&thiz->mlist, typeof(*item), mnode);
    RWUNLOCK(&thiz->lock);

    return item;
}

static int  find(T *thiz, const mkey_t *k, mval_t *v)
{
    int r = 0;
    mtb_t *it;

    RWLOCK_READ(&thiz->lock);
    list_for_each_entry(it, &thiz->mlist, mnode) {
        r = it->find(it, k, v);
        if (r) break;
    }
    RWUNLOCK(&thiz->lock);

    return r;
}

static int exist(T *thiz, const mkey_t *k, mtb_t *until_pos)
{
    int r = 0;
    mtb_t *it;

    RWLOCK_READ(&thiz->lock);
    list_for_each_entry(it, &thiz->mlist, mnode) {
        if (it == until_pos) break;

        r = it->exist(it, k);
        if (r) break;
    }
    RWUNLOCK(&thiz->lock);

    return r;
}

static mtb_t   *pop(T *thiz)
{
    mtb_t *item = NULL;

    RWLOCK_WRITE(&thiz->lock);
    if (!list_empty(&thiz->mlist)) {
        item = list_last_entry(&thiz->mlist, typeof(*item), mnode);
        MY_LIST_DEL(&item->mnode, &thiz->mlist_len);
    }
    RWUNLOCK(&thiz->lock);

    return item;
}

static int _init(T *thiz)
{
    RWLOCK_INIT(&thiz->lock);
    INIT_LIST_HEAD(&thiz->mlist);

    ADD_METHOD(destroy);
    ADD_METHOD(len    );
    ADD_METHOD(empty  );
    ADD_METHOD(push   );
    ADD_METHOD(trypush);
    ADD_METHOD(find   );
    ADD_METHOD(exist  );
    ADD_METHOD(pop    );
    ADD_METHOD(top    );
    ADD_METHOD(tail   );

    return 0;
}

mtbset_t *mtbset_create(pool_t *mpool)
{
    mtbset_t *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    _init(thiz);

    return thiz;           
}
