#include "hdb_inc.h"
#include "conf.h"
#include "btree_aux.h"
#include "btree.h"
#include "mem_table.h"
#include "file_table.h"

#define T compactor_t

static int cpct_major(compactor_t *thiz);
static int cpct_L0(compactor_t *thiz);
static int cpct_remote(compactor_t *thiz);
static int cpct_split(compactor_t *thiz);

typedef int (*CPCT_EXECUTOR)(compactor_t *thiz);
static CPCT_EXECUTOR g_cpct_exec[CPCT_MAX] = {
    [CPCT_MAJOR]    = cpct_major,
    [CPCT_L0]       = cpct_L0 ,
    [CPCT_REMOTE]   = cpct_remote,
    [CPCT_SPLIT]    = cpct_split,
};

static void cpct_destroy(compactor_t *thiz)
{
    del_obj(thiz);
}

static void compact(compactor_t *thiz)
{
    g_cpct_exec[thiz->type](thiz);
}

compactor_t *compactor_create(pool_t *mpool)
{
     compactor_t *thiz = new_obj(mpool, sizeof(*thiz));
     if (thiz == NULL) return NULL;

     thiz->destroy = cpct_destroy;
     thiz->compact = compact;

     return thiz;           
}

static int upgrade_fname(char *src, char *dst)
{
    int lv;
    uint64_t fnum;
    char *b;

    strcpy(dst, src);
    b = basename(dst);
    sscanf(b, "%ju_%d", &fnum, &lv);
    lv++;

    /* "b - 1" point to base file name */
    sprintf(b - 1, "/%"PRIu64"_%d"DB_DATA_EXT, fnum, lv);

    return 0;
}

static int do_upgrade(compactor_t *thiz, ftb_t *ftb)
{
    int r = 0;
    char fname[G_MEM_MID];
    ftbset_t *sfset = thiz->src_fset;
    ftbset_t *dfset = thiz->dst_fset;
    ftb_t *larger;

    dfset->find_ins_pos(dfset, ftb, &larger);

    upgrade_fname(ftb->file, fname);
    INFO("upgrade %s to %s", ftb->file, fname);

    RWLOCK_WRITE(&ftb->lock);
    r = rename(ftb->file, fname);
    if (r == -1) {
        ERROR("rename %s to %s, errno=%d", ftb->file, fname, errno);
        RWUNLOCK(&ftb->lock);
        return -1;
    }

    strcpy(ftb->file, fname);
    RWUNLOCK(&ftb->lock);

    RWLOCK_WRITE(&sfset->lock);
    RWLOCK_WRITE(&dfset->lock);

    MY_LIST_DEL(&ftb->fnode, &sfset->flist_len);
    
    if (list_empty(&dfset->flist) || larger == NULL) {
        MY_LIST_ADD_TAIL(&ftb->fnode, &dfset->flist, &dfset->flist_len);
    } else {
        ftb->fnode.next = &larger->fnode;
        ftb->fnode.prev = larger->fnode.prev;
        larger->fnode.prev->next = &ftb->fnode;
        larger->fnode.prev = &ftb->fnode;

        dfset->flist_len++;
    }

    RWUNLOCK(&dfset->lock);
    RWUNLOCK(&sfset->lock);

    return 0;
}

static void cleanup_after_major(compactor_t *thiz, struct list_head *ovr)
{
    ftb_t *it, *save;

    /* del first entry, which is from level-0 */
    it = list_first_entry(ovr, typeof(*it), cnode);
    list_del(&it->cnode);
    MY_LIST_DEL(&it->fnode, &thiz->src_fset->flist_len);
    it->clean(it);
    it->destroy(it);

#if 1   /* drop invalid file */
    list_for_each_entry_safe(it, save, ovr, cnode) {
        if (it->model->invalid(it->model)) {
            PROMPT("drop invalid file %s", it->file);
            list_del(&it->cnode);
            MY_LIST_DEL(&it->fnode, &thiz->dst_fset->flist_len);
            it->clean(it);
            it->destroy(it);
        }
    }
#endif
}

static int do_major(compactor_t *thiz, ftb_t *ftb, struct list_head *ovr)
{
    int r = 0;
    ftb_t *it;
    fkv_t *fkv;
    btriter_t *trit;

    trit = ftb->model->get_iter(ftb->model, NULL, NULL, NULL);

    list_for_each_entry(it, ovr, cnode) {
        r = it->model->merge_start(it->model, merge_filter);
        if (r != 0) goto _out;
    }
           
    it = list_first_entry(ovr, typeof(*it), cnode);

    while (trit->has_next(trit)) {
        r = trit->get_next(trit, &fkv);
        if (r != 0) goto _out;

        extract_fkey(fkv);
        extract_fval(trit->container->rfd, fkv);

        list_for_each_entry_from(it, ovr, cnode) {
            /* it is the larger or the last */
            if (list_is_last(&it->cnode, ovr)) break;

            r = it->model->krange_cmp(it->model, &fkv->kv->k);
            if (r >= 0) break;
        }

        it->model->merge(it->model, fkv);

        trit->next(trit);
    }

    list_for_each_entry(it, ovr, cnode) {
        r = it->model->merge_flush(it->model);
        if (r != 0) goto _out;
    }

    r = 0;

_out:
    RWLOCK_WRITE(&thiz->src_fset->lock);
    RWLOCK_WRITE(&thiz->dst_fset->lock);

    trit->destroy(trit);
    list_for_each_entry(it, ovr, cnode) {
        it->model->merge_hdr(it->model);
        it->model->merge_fin(it->model);
    }

    if (r == 0) {
        /* add source ftable for cleaning */
        list_add(&ftb->cnode, ovr);

        cleanup_after_major(thiz, ovr);
    }

    RWUNLOCK(&thiz->dst_fset->lock);
    RWUNLOCK(&thiz->src_fset->lock);

    return r;
}

static int cpct_major(compactor_t *thiz)
{
    int r;
    ftb_t *ftb;
    ftbset_t *dfset = thiz->dst_fset;
    struct list_head ovr;

    INIT_LIST_HEAD(&ovr);

    ftb = thiz->src_ftb;
    ftb->search_overlap(ftb, dfset, &ovr);

    if (list_empty(&ovr)) {
        r = do_upgrade(thiz, ftb);
        return r;
    }

    r = do_major(thiz, ftb, &ovr);
    return r;
}

static int cpct_L0(compactor_t *thiz)
{
    /* TODO!! */
    return -1;
}

static int cpct_remote(compactor_t *thiz)
{
    /* TODO!! */
    return -1;
}

static int cpct_split(compactor_t *thiz)
{
    int r = 0, lv;
    uint64_t fnum;
    char fname[G_MEM_MID], *bname;
    ftb_t *tb0, *tb1 = NULL, *tb2 = NULL;
    btree_t *m;
    ftbset_t *sfset = thiz->src_fset;

    tb0 = thiz->src_ftb;

    strcpy(fname, thiz->src_ftb->file);
    bname = basename(fname);
    sscanf(bname, "%ju_%d", &fnum, &lv);

    tb1= ftb_create(NULL);
    tb1->conf = thiz->conf;
    TMP_FIRST_PARTIAL(tb1->file, thiz->conf, bname);
    tb1->init(tb1);

    strcpy(fname, tb0->file);
    bname = basename(fname);

    tb2= ftb_create(NULL);
    tb2->conf = thiz->conf;
    TMP_SECOND_PARTIAL(tb2->file, thiz->conf, bname);
    tb2->init(tb2);

    INFO("splitting %s into %"PRIu64", %"PRIu64, tb0->file, thiz->nfnum1, thiz->nfnum2);

    m = thiz->src_ftb->model;
    r = m->split(m, tb1->model, tb2->model);
    if (r != 0) goto _out;

    tb1->fnode.next = &tb2->fnode;
    tb2->fnode.prev = &tb1->fnode;

    RWLOCK_WRITE(&sfset->lock);

    tb1->fnode.prev = tb0->fnode.prev;
    tb2->fnode.next = tb0->fnode.next;

    tb1->fnode.prev->next = &tb1->fnode;
    tb2->fnode.next->prev = &tb2->fnode;

    sfset->flist_len++;

    /* rename file */
    DATA_FILE(fname, thiz->conf, thiz->nfnum1, lv);
    if (rename(tb1->file, fname) == -1) {
        r = -1;
        ERROR("rename %s to %s, errno=%d", tb1->file, fname, errno);
    } else {
        strcpy(tb1->file, fname);
    }
    
    DATA_FILE(fname, thiz->conf, thiz->nfnum2, lv);
    if (rename(tb2->file, fname) == -1) {
        r = -1;
        ERROR("rename %s to %s, errno=%d", tb2->file, fname, errno);
    } else {
        strcpy(tb2->file, fname);
    }

    if (r == 0) {
        if (tb1->model->invalid(tb1->model)) {
            PROMPT("drop invalid file %s", tb1->file);
            MY_LIST_DEL(&tb1->fnode, &sfset->flist_len);
            tb1->clean(tb1);
            tb1->destroy(tb1);
        }

        if (tb2->model->invalid(tb2->model)) {
            PROMPT("drop invalid file %s", tb2->file);
            MY_LIST_DEL(&tb2->fnode, &sfset->flist_len);
            tb2->clean(tb2);
            tb2->destroy(tb2);
        }
    }
    RWUNLOCK(&sfset->lock);

 
_out:
    if (r != 0) {
        if (tb1) {
            tb1->clean(tb1);
            tb1->destroy(tb1);
        }

        if (tb2) {
            tb2->clean(tb2);
        }
    } else {
        tb0->clean(tb0);
        tb0->destroy(tb0);
    }

    return r;
}

