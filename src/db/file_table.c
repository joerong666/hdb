#include "hdb_inc.h"
#include "file_table.h"

#define T ftb_t

struct ftb_pri {
};

static int   store(T *thiz, mtb_t *mtb)
{
    return thiz->model->store(thiz->model, mtb->model);
}

static int restore(T *thiz)
{
    int r;

    r = thiz->model->restore(thiz->model);

#if 0
    fkv_t *fkv;
    btriter_t *trit;

    trit = btriter_create(NULL);
    trit->container = thiz->model;
    trit->fd = trit->container->rfd;
    trit->init(trit);

    INFO("iterate %s", thiz->file);
    while (trit->has_next(trit)) {
        trit->get(trit, &fkv);
        INFO("ks=%d, vs=%d, %.*s: %.*s", fkv->kv->k.len, fkv->kv->v.len, 
                fkv->kv->k.len, fkv->kv->k.data, fkv->kv->v.len, fkv->kv->v.data);
        trit->next(trit);
    }

    trit->destroy(trit);

#endif

    return r;
}

static int   find(T *thiz, mkey_t *k, mval_t *v)
{
    return thiz->model->find(thiz->model, k, v);
}

static int   exist(T *thiz, mkey_t *k, uint64_t ver)
{
    return thiz->model->exist(thiz->model, k, ver);
}

static void search_overlap(T *thiz, ftbset_t *fset, struct list_head *ovr)
{
    int r;
    ftb_t *it;

    if (list_empty(&fset->flist)) return;

    it = list_first_entry(&fset->flist, typeof(*it), fnode);
    r = thiz->model->range_cmp(thiz->model, it->model);
    if (r < 0) return;

    it = list_last_entry(&fset->flist, typeof(*it), fnode);
    r = thiz->model->range_cmp(thiz->model, it->model);
    if (r > 0) return;

    list_for_each_entry(it, &fset->flist, fnode) {
        r = thiz->model->range_cmp(thiz->model, it->model);
        if (r == 0) {
            list_add_tail(&it->cnode, ovr);
        }
    }
}

static void backup_file(T *thiz)
{
    backup(thiz->conf, thiz->file);
}

static void clean(T *thiz)
{
    recycle(thiz->conf, thiz->file);
}

/****************************************
** basic function 
*****************************************/
static int init(T *thiz)
{
    btree_t *m = btree_create(SUPER->mpool);
    m->file = thiz->file;
    m->conf = thiz->conf;

    m->init(m);
    thiz->model = m;

    return 0;
}

static void destroy(T *thiz)
{
    if (thiz->model) thiz->model->destroy(thiz->model);

    del_obj(thiz);
}

static int _init(T *thiz)
{
    RWLOCK_INIT(&thiz->lock);

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(clean);
    ADD_METHOD(find);
    ADD_METHOD(exist);
    ADD_METHOD(store);
    ADD_METHOD(restore);
    ADD_METHOD(search_overlap);

    thiz->backup = backup_file;

    return 0;
}

T *ftb_create(pool_t *mpool)
{
     T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
     if (thiz == NULL) return NULL;

     SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

     if (_init(thiz) != 0) {
         del_obj(thiz);     
         return NULL;       
     }                      

     return thiz;           
}
