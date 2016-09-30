#include "hdb_inc.h"
#include "file_table.h"

#define T ftbset_t
#define FTB_FLG_ON_CPCT 1

struct ftbset_pri {
};

static int  len(T *thiz)
{
    int r;

    RWLOCK_READ(&thiz->lock);
    r = thiz->flist_len;
    RWUNLOCK(&thiz->lock);

    return r;
}

static int find_ins_pos(T *thiz, ftb_t *ftb, ftb_t **pos)
{
    int r = -1;
    ftb_t *it;

    /* if not sorted, just append */
    *pos = NULL;

    RWLOCK_READ(&thiz->lock);
    if (thiz->flag & FSET_FLG_ORDERED) {
        list_for_each_entry(it, &thiz->flist, fnode) {
            r = it->model->range_cmp(it->model, ftb->model);
            if (r == 0) {
                ERROR("%s krange overlap, cannot insert %s", it->file, ftb->file); 
                break;
            }

            if (r > 0) {
                *pos = it;
                break;
            }
        }
    }

    RWUNLOCK(&thiz->lock);

    /* zero means krange overlap */
    if (r != 0) return 0;

    return -1;
}

static int  push(T *thiz, ftb_t *item)
{
    int r;
    ftb_t *larger;

    r = find_ins_pos(thiz, item, &larger);
    if (r != 0) return -1;

    RWLOCK_WRITE(&thiz->lock);
    if (list_empty(&thiz->flist) || larger == NULL) {
        if (thiz->flag & FSET_FLG_ORDERED) {
            MY_LIST_ADD_TAIL(&item->fnode, &thiz->flist, &thiz->flist_len);
        } else {
            /* latest place after head */
            MY_LIST_ADD(&item->fnode, &thiz->flist, &thiz->flist_len);
        }
    } else {
        item->fnode.next = &larger->fnode;
        item->fnode.prev = larger->fnode.prev;
        larger->fnode.prev->next = &item->fnode;
        larger->fnode.prev = &item->fnode;

        thiz->flist_len++;
    }
    RWUNLOCK(&thiz->lock);

    return 0;
}

static int  find(T *thiz, mkey_t *k, mval_t *v)
{
    int r = RC_NOT_FOUND;
    ftb_t *it;

    RWLOCK_READ(&thiz->lock);
    list_for_each_entry(it, &thiz->flist, fnode) {
        r = it->find(it, k, v);
        if (r == RC_FOUND || r == RC_ERR) break;
    }
    RWUNLOCK(&thiz->lock);

    return r;
}

static int  exist(T *thiz, mkey_t *k, ftb_t *until_pos, uint64_t ver)
{
    int r = 0;
    ftb_t *it;

    list_for_each_entry(it, &thiz->flist, fnode) {
        if (it == until_pos) break;

        r = it->exist(it, k, ver);
        if (r) break;
    }

    return r;
}

static ftb_t *top(T *thiz)
{
    ftb_t *ftb;

    RWLOCK_READ(&thiz->lock);
    if (list_empty(&thiz->flist)) ftb = NULL;
    else ftb = list_last_entry(&thiz->flist, typeof(*ftb), fnode);
    RWUNLOCK(&thiz->lock);

    return ftb;
}

static ftb_t *tail(T *thiz)
{
    ftb_t *ftb;

    RWLOCK_READ(&thiz->lock);
    if (list_empty(&thiz->flist)) ftb = NULL;
    else ftb = list_first_entry(&thiz->flist, typeof(*ftb), fnode);
    RWUNLOCK(&thiz->lock);

    return ftb;
}

static ftb_t *search_cpct_tb(T *thiz, int cpct_type)
{
    ftb_t *ftb = NULL, *it = NULL, *it2 = NULL;
    hdr_block_t *hdr;
    conf_t *cnf;

    RWLOCK_WRITE(&thiz->lock);

    if (cpct_type == CPCT_SPLIT) {
        list_for_each_entry(it, &thiz->flist, fnode) {
            RWLOCK_READ(&it->model->lock);
            hdr = it->model->hdr;
            cnf = it->conf;

            if (hdr->fend_off > (cnf->ftb_size * 2)) {
                ftb = it;
            }
            RWUNLOCK(&it->model->lock);

            if (ftb != NULL) break;
        }

        goto _out;
    }
 
    if (cpct_type == CPCT_MAJOR) {
        /* select oldest ftb for compaction */
        it = list_last_entry(&thiz->flist, typeof(*ftb), fnode);

        if (it->flag & FTB_FLG_ON_CPCT) it = NULL;
        else it->flag |= FTB_FLG_ON_CPCT;

        ftb = it;
        goto _out;
    }

    if (cpct_type == CPCT_AJACENT) {
        list_for_each_entry(it, &thiz->flist, fnode) {
            if (list_is_last(&it->fnode, &thiz->flist)) continue;
            
            it2 = list_first_entry(&it->fnode, typeof(*ftb), fnode);
            RWLOCK_READ(&it->model->lock);
            RWLOCK_READ(&it2->model->lock);
            size_t sz = it->model->hdr->fend_off + it2->model->hdr->fend_off;

            if (sz < it->conf->ftb_size) {
                ftb = it;
            }
            RWUNLOCK(&it2->model->lock);
            RWUNLOCK(&it->model->lock);

            if (ftb != NULL) break;
        }

        goto _out;
    }

    if (cpct_type == CPCT_SHRINK) {
        list_for_each_entry(it, &thiz->flist, fnode) {
            RWLOCK_READ(&it->model->lock);
            hdr = it->model->hdr;
            cnf = it->conf;

            if (hdr->cpct_cnt > cnf->cpct_cnt) {
                ftb = it;
            }
            RWUNLOCK(&it->model->lock);

            if (ftb != NULL) break;
        }

        goto _out;
    }
 
 _out:
    RWUNLOCK(&thiz->lock);
    return ftb;
}

static void  destroy(T *thiz)
{
    ftb_t *it, *save;

    list_for_each_entry_safe(it, save, &thiz->flist, fnode) {
        MY_LIST_DEL(&it->fnode, &thiz->flist_len);
        it->destroy(it);
    }

    del_obj(thiz);
}

T *ftbset_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));
    RWLOCK_INIT(&thiz->lock);
    INIT_LIST_HEAD(&thiz->flist);

    ADD_METHOD(destroy       ); 
    ADD_METHOD(len           );
    ADD_METHOD(push          );
    ADD_METHOD(find          );
    ADD_METHOD(exist         );
    ADD_METHOD(top           );
    ADD_METHOD(tail          );
    ADD_METHOD(find_ins_pos   );
    ADD_METHOD(search_cpct_tb);

    return thiz;           
}

