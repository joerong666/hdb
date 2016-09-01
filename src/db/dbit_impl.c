#include "hdb_inc.h"
#include "btree_aux.h"
#include "mem_table.h"
#include "file_table.h"
#include "dbimpl.h"

#define T dbit_impl_t

enum lock_flag_e {
    LOCK_IMQ = 1,
    LOCK_L0 = (1 << 1),
    LOCK_Ln = (1 << 2),
};

struct dbit_impl_pri {
    int lock_flag;

    mkv_t mstart;
    mkv_t mstop;

    mkey_t *startptr;
    mkey_t *stopptr;
    mkv_t  *mstartptr;
    mkv_t  *mstopptr;

    mtb_t *cur_mtb;
    ftb_t *cur_ftb;

    htiter_t *miter;
    btriter_t *fiter;

    ftbset_t *fq_L0;
    ftbset_t *fq_Ln;

    BTITERCMP btcmp;
    SKITERCMP htcmp;
};

static int prefix_cmp(mkey_t *target, mkey_t *start, mkey_t *stop)
{
    (void)stop;

    if (start == NULL || start->data == NULL) return 0;

    if (target->len < start->len) return -1;

    return strncmp(target->data, start->data, start->len);
}

static int range_cmp(mkey_t *target, mkey_t *start, mkey_t *stop)
{
    int r;

    if ((start == NULL || start->data == NULL) 
            && (stop == NULL || stop->data == NULL)) return 0;

    if (start == NULL || start->data == NULL) {
        r = key_cmp(target, stop);
        if (r > 0) return r;
        else return 0;
    }

    if (stop == NULL || stop->data == NULL) {
        r = key_cmp(target, start);
        if (r < 0) return r;
        else return 0;
    }

    r = key_cmp(target, start);
    if (r < 0) return r;

    r = key_cmp(target, stop);
    if (r > 0) return r;

    return 0;
}

static int ht_prefix_cmp(void *target, void *start, void *stop)
{
    mkv_t *t = (mkv_t *)target;
    mkv_t *b = (mkv_t *)start;
    mkv_t *e = (mkv_t *)stop;

    mkey_t *bk = b ? (&b->k) : NULL;
    mkey_t *ek = e ? (&e->k) : NULL;

    return prefix_cmp(&t->k, bk, ek);
}

static int ht_range_cmp(void *target, void *start, void *stop)
{
    mkv_t *t = (mkv_t *)target;
    mkv_t *b = (mkv_t *)start;
    mkv_t *e = (mkv_t *)stop;

    mkey_t *bk = b ? (&b->k) : NULL;
    mkey_t *ek = e ? (&e->k) : NULL;

    return range_cmp(&t->k, bk, ek);
}

static int   extract_mkey(T *thiz, mkv_t *dst, mkv_t *src)
{
    (void)thiz;

    dst->type = src->type;
    dst->vcrc = src->vcrc;
    dst->off = src->off;
    dst->seq = src->seq;

    dst->k.len = src->k.len;
    dst->k.data = MY_Malloc(dst->k.len);

    memcpy(dst->k.data, src->k.data, dst->k.len);

    return 0;
}

static int   extract_mval(T *thiz, mkv_t *dst, mkv_t *src)
{
    int r = 0, fd;

    dst->v.len = src->v.len;
    dst->v.data = MY_Malloc(dst->v.len);

    if (dst->type & KV_VTP_BINOFF) {
#if 1
        fd = SELF->cur_mtb->fd;
#else
        if (thiz->flag & IT_MMTB) {
            fd = thiz->mmtb->fd;
        } else {
            fd = SELF->mtb->fd;
        }
#endif
        r = read_bin_val(fd, dst); 
        if (r == -1) goto _out;

    } else {
        memcpy(dst->v.data, src->v.data, dst->v.len);
    }

_out:
    if (r == -1) {
        MY_Free(dst->v.data);
        return -1;
    }

    return 0;
}

static int check_mkexist(T *thiz, mkey_t *k, mtb_t *tb)
{
    int r;
    mtb_t *mtb;
    mtbset_t *mq;

    mtb = thiz->mmtb;
    r = mtb->exist(mtb, k);
    if (r) return r;

    mq = thiz->imq;
    r = mq->exist(mq, k, tb);
    if (r) return r;

    return 0;
}

static int check_fkexist(T *thiz, fkv_t *fkv, ftb_t *tb)
{
    int r;
    mtb_t *mtb;
    mtbset_t *mq;
    ftbset_t *fq;

    mkey_t k;
    char data[G_KSIZE_LIMIT];

    if (fkv->kshare.len > 0) {
        memcpy(data, fkv->kshare.data, fkv->kshare.len);
        memcpy(data + fkv->kshare.len, fkv->kdelt.data, fkv->kdelt.len);

        k.data = data;
        k.len = fkv->kshare.len + fkv->kdelt.len;
    } else {
        k.data = fkv->kdelt.data;
        k.len = fkv->kdelt.len;
    }

    mtb = thiz->mmtb;
    r = mtb->exist(mtb, &k);
    if (r) return r;

    mq = thiz->imq;
    r = mq->exist(mq, &k, NULL);
    if (r) return r;

    fq = SELF->fq_L0;
    r = fq->exist(fq, &k, tb);
    if (r) return r;

    return 0;
}

static int mkvflt(T *thiz, mkv_t *kv)
{
    conf_t *cnf = thiz->container->conf;
    return cnf->mkvflt(cnf, kv);
}

static int fkvflt(T *thiz, fkv_t *fkv)
{
    conf_t *cnf = thiz->container->conf;
    return cnf->fkvflt(cnf, fkv);
}

static int it_mmt(T *thiz, mkv_t *kv)
{
    int r;
    mkv_t *tkv;
    mtb_t *tb = thiz->mmtb;
    htiter_t *iter = SELF->miter;

    if (!(thiz->flag & IT_BEG)) {
        thiz->flag |= IT_BEG;

        iter = tb->model->get_iter(tb->model, SELF->mstartptr, SELF->mstopptr, SELF->htcmp);

        SELF->cur_mtb = tb;
        SELF->miter = iter;

        PROMPT("iterate %s", tb->file);
    }

    r = 0;
    while (iter->has_next(iter)) {
        iter->next(iter);
        iter->get(iter, (void **)&tkv);

        if (mkvflt(thiz, tkv)) continue;

        r = 1;
        break;
    }

    if (r) {
        extract_mkey(thiz, kv, tkv);
        if (!(thiz->flag & IT_ONLY_KEY)) {
            extract_mval(thiz, kv, tkv);
        }
    } else {
        iter->destroy(iter);
        thiz->flag &= ~IT_BEG;
    }

    return r;
}

static int it_imt(T *thiz, mkv_t *kv)
{
    int r;
    mkv_t *tkv;
    mtb_t *tb = SELF->cur_mtb;
    htiter_t *iter = SELF->miter;

    if (!(thiz->flag & IT_BEG)) {
        if (list_empty(&thiz->imq->mlist)) return 0;

        thiz->flag |= IT_BEG;

        tb = list_first_entry(&thiz->imq->mlist, mtb_t, mnode);
        iter = tb->model->get_iter(tb->model, SELF->mstartptr, SELF->mstopptr, SELF->htcmp);

        SELF->cur_mtb = tb;
        SELF->miter = iter;

        PROMPT("iterate %s", tb->file);
    }

    r = 0;
    do {
        while (iter->has_next(iter)) {
            iter->next(iter);
            iter->get(iter, (void **)&tkv);

            if (mkvflt(thiz, tkv)) continue;
            if (check_mkexist(thiz, &tkv->k, tb)) continue;

            r = 1;
            break;
        }

        if (r) {
            extract_mkey(thiz, kv, tkv);
            if (!(thiz->flag & IT_ONLY_KEY)) {
                extract_mval(thiz, kv, tkv);
            }
            break;
        } else {
            iter->destroy(iter);

            if (list_is_last(&tb->mnode, &thiz->imq->mlist)) {
                thiz->flag &= ~IT_BEG;
                break;
            }

            tb = list_first_entry(&tb->mnode, mtb_t, mnode);
            iter = tb->model->get_iter(tb->model, SELF->mstartptr, SELF->mstopptr, SELF->htcmp);

            SELF->cur_mtb = tb;
            SELF->miter = iter;

            PROMPT("iterate %s", tb->file);
        }
    } while(1);

    return r;
}

static int it_file(T *thiz, int lv, mkv_t *kv)
{
    int r;
    fkv_t *tkv;
    ftb_t *tb = SELF->cur_ftb;
    btriter_t *iter = SELF->fiter;
    ftbset_t *fq = lv > 0 ? SELF->fq_Ln : SELF->fq_L0;

    if (!(thiz->flag & IT_BEG)) {
        thiz->flag |= IT_BEG;

        if (lv > 0) SELF->lock_flag |= LOCK_Ln;
        else SELF->lock_flag |= LOCK_L0;

        RWLOCK_READ(&fq->lock);

        if (list_empty(&fq->flist)) return 0;

        tb = list_first_entry(&fq->flist, ftb_t, fnode);
        iter = tb->model->get_iter(tb->model, SELF->startptr, SELF->stopptr, SELF->btcmp);

        SELF->cur_ftb = tb;
        SELF->fiter = iter;

        PROMPT("iterate %s", tb->file);
    }

    r = 0;
    do {
        while (iter->has_next(iter)) {
            iter->get_next(iter, &tkv);
            iter->next(iter);

            if (fkvflt(thiz, tkv)) continue;
            if (check_fkexist(thiz, tkv, tb)) continue;

            r = 1;
            break;
        }

        if (r) {
            extract_fkey(tkv);
            if (!(thiz->flag & IT_ONLY_KEY)) {
                extract_fval(iter->container->rfd, tkv);
            }

            memcpy(kv, tkv->kv, sizeof(mkv_t));
            break;
        } else {
            iter->destroy(iter);

            if (list_is_last(&tb->fnode, &fq->flist)) {
                thiz->flag &= ~IT_BEG;
                return 0;
            }

            tb = list_first_entry(&tb->fnode, ftb_t, fnode);
            iter = tb->model->get_iter(tb->model, SELF->startptr, SELF->stopptr, SELF->btcmp);

            SELF->cur_ftb = tb;
            SELF->fiter = iter;

            PROMPT("iterate %s", tb->file);
        }
    } while(1);

    return r;
}

static int next(T *thiz, mkv_t *kv)
{
    int r;

    if (thiz->flag & IT_FIN) {
        PROMPT("iterate finished");
        goto _out;
    }

    if (thiz->flag & IT_MMTB) {
        r = it_mmt(thiz, kv);
        if (r == 1) goto _out;

        thiz->flag &= ~IT_BEG;
        thiz->flag &= ~IT_MMTB;
        thiz->flag |= IT_IMTB;

        PROMPT("iterate mmtable finished");
    }
 
    if (thiz->flag & IT_IMTB) {
        r = it_imt(thiz, kv);
        if (r == 1) goto _out;

        thiz->flag &= ~IT_BEG;
        thiz->flag &= ~IT_IMTB;
        thiz->flag |= IT_L0;

        PROMPT("iterate imtable finished");
    }
        
    if (thiz->flag & IT_L0) {
        r = it_file(thiz, 0, kv);
        if(r == 1) goto _out;

        thiz->flag &= ~IT_BEG;
        thiz->flag &= ~IT_L0;
        thiz->flag |= IT_Ln;

        PROMPT("iterate L0 finished");
    }

    if (thiz->flag & IT_Ln) {
        r = it_file(thiz, 1, kv);
        if(r == 1) goto _out;

        thiz->flag &= ~IT_BEG;
        thiz->flag &= ~IT_Ln;
        thiz->flag |= IT_FIN;

        PROMPT("iterate Ln finished");
    }
    
_out:
    return (!(thiz->flag & IT_FIN));
}

/**********************************
 * basic function
 **********************************/
static int init(T *thiz)
{
    int r = 0;

    SELF->fq_L0 = thiz->container->fsets[0];
    SELF->fq_Ln = thiz->container->fsets[1];

    if (thiz->flag & IT_FETCH_RANGE) {
        SELF->btcmp = range_cmp;
        SELF->htcmp = ht_range_cmp;
    } else {
        SELF->btcmp = prefix_cmp;
        SELF->htcmp = ht_prefix_cmp;
    }

    if (thiz->start.len > 0 && thiz->start.data != NULL) {
        SELF->mstart.k.len  = thiz->start.len;
        SELF->mstart.k.data = thiz->start.data;

        SELF->mstartptr = &SELF->mstart;
        SELF->startptr  = &thiz->start;
    }

    if (thiz->stop.len > 0 && thiz->stop.data != NULL) {
        SELF->mstop.k.len  = thiz->stop.len;
        SELF->mstop.k.data = thiz->stop.data;

        SELF->mstopptr = &SELF->mstop;
        SELF->stopptr  = &thiz->stop;
    }

    RWLOCK_READ(&thiz->imq->lock);

    return r;
}

static void destroy(T *thiz)
{
    RWUNLOCK(&thiz->imq->lock);

    if (SELF->lock_flag & LOCK_L0) {
        SELF->lock_flag &= ~LOCK_L0;
        RWUNLOCK(&SELF->fq_L0->lock);
    }

    if (SELF->lock_flag & LOCK_Ln) {
        SELF->lock_flag &= ~LOCK_Ln;
        RWUNLOCK(&SELF->fq_Ln->lock);
    }

    del_obj(thiz);
}

static int _init(T *thiz)
{
    thiz->flag |= IT_MMTB;

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(next);

    return 0;
}

T *dbit_impl_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    _init(thiz);

    return thiz;           
}


