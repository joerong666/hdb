#include "dbimpl_pri.h"
#include "recover.h"
#include "db_threads.h"
#include "operation.h"

#define T dbimpl_t

mtb_t *HI_PREFIX(get_mmtb)(T *thiz)
{
    mtb_t *t;

    RWLOCK_READ(&SELF->lock);
    t = SELF->mmtb;
    RWUNLOCK(&SELF->lock);

    return t;
}

void HI_PREFIX(set_mmtb)(T *thiz, mtb_t *tb)
{
    RWLOCK_READ(&SELF->lock);
    SELF->mmtb = tb;
    RWUNLOCK(&SELF->lock);
}

int HI_PREFIX(prepare_mtb)(T *thiz)
{
    mtb_t *m;

    m = mtb_create(NULL);

    BIN_FILE(m->file, thiz->conf, HI_PREFIX(next_fnum)(thiz));
    m->conf = thiz->conf;
    m->init(m);

    HI_PREFIX(set_mmtb)(thiz, m);

    return 0;
}

dbit_impl_t *HI_PREFIX(get_iter)(T *thiz, mkey_t *start, mkey_t *stop)
{
    dbit_impl_t *it;
    
    it = dbit_impl_create(NULL);
    it->container = thiz;

    if (start != NULL) {
        DEBUG("it start=%.*s", start->len, start->data);
        it->start.len = start->len;
        it->start.data = PALLOC(it->super.mpool, start->len);
        memcpy(it->start.data, start->data, start->len);
    }

    if (stop != NULL) {
        DEBUG("it stop=%.*s", start->len, start->data);
        it->stop.len = stop->len;
        it->stop.data = PALLOC(it->super.mpool, stop->len);
        memcpy(it->stop.data, stop->data, stop->len);
    }

    it->mmtb = HI_PREFIX(get_mmtb)(thiz);

    it->version = HI_PREFIX(get_seq)(thiz);
    it->imq = SELF->imq;
    it->init(it);

    return it;
}

static void load_protect(T *thiz)
{
    int len;

    pthread_mutex_lock(&SELF->prot_mtx);
    while(1) {
        len = SELF->imq->len(SELF->imq);
        if (len > thiz->conf->imq_limit) {
            ERROR("len of imq exceed %d, block push!", thiz->conf->imq_limit);
            pthread_cond_wait(&SELF->prot_cond, &SELF->prot_mtx);
        } else {
            break;
        }
    }
    pthread_mutex_unlock(&SELF->prot_mtx);
}

static int   switch_mtb(T *thiz)
{
    int r = 0;
    mtb_t *m;

    m = SELF->mmtb;

    if (m->full(m)) {
        load_protect(thiz);

        r = SELF->imq->trypush(SELF->imq, m);
        if (r == 0) {
            HI_PREFIX(prepare_mtb)(thiz);
        } else {
            INFO("mmtb full, but imq'lock not ready");
        }

        HI_PREFIX(notify)(thiz, NTF_MTB_DUMP);
    }

    return 0;
}

static int   put_i(T *thiz, mkv_t *kv)
{
    int r;

    if (kv->k.len >= G_KSIZE_LIMIT) {
        ERROR("klen=%u exceed %d", kv->k.len, G_KSIZE_LIMIT);
        return -1;
    }

    r = switch_mtb(thiz);
    if (r != 0) return -1;

    kv->seq = HI_PREFIX(get_seq)(thiz);

    r = SELF->mmtb->push(SELF->mmtb, kv);
    if (r != 0) return -1;

    HI_PREFIX(notify)(thiz, NTF_WRITE_BIN);
    return 0;
}

int   HI_PREFIX(put)(T *thiz, mkv_t *kv)
{
    DBWR_STATS_INCR(put);

    kv->type = 0;
    return put_i(thiz, kv); 
}

int   HI_PREFIX(del)(T *thiz, mkey_t *k)
{
    mkv_t kv;

    DBWR_STATS_INCR(del);
    memset(&kv, 0x0, sizeof(kv));
    kv.type = KV_OP_DEL;
    memcpy(&kv.k, k, sizeof(mkey_t));
    
    return put_i(thiz, &kv); 
}

static int   mput_i(T *thiz, mkv_t *kvs, size_t cnt)
{
    int r, i;
    uint64_t seq;

    r = switch_mtb(thiz);
    if (r != 0) return r;

    for (i = 0; i < (int)cnt; i++) {
        if (kvs[i].k.len >= G_KSIZE_LIMIT) {
            ERROR("klen=%u too big", kvs[i].k.len);
            return -1;
        }
    }

    seq = HI_PREFIX(get_seq)(thiz);

    RWLOCK_WRITE(&SELF->mmtb->lock);
    for (i = 0; i < (int)cnt; i++) {
        kvs[i].seq = seq;
        r = SELF->mmtb->push_unsafe(SELF->mmtb, &kvs[i]);
        if (r != 0) break;
    }
    RWUNLOCK(&SELF->mmtb->lock);

    if (r != 0) return -1;

    HI_PREFIX(notify)(thiz, NTF_WRITE_BIN);
    return 0;
}

int   HI_PREFIX(mput)(T *thiz, mkv_t *kvs, size_t cnt)
{
    size_t i;

    DBWR_STATS_INCR(mput);
    for (i = 0; i < cnt; i++) {
        kvs[i].type = 0;
    }

    return mput_i(thiz, kvs, cnt);
}

int   HI_PREFIX(mdel)(T *thiz, mkey_t *keys, size_t cnt)
{
    size_t i;
    mkv_t kvs[cnt];

    DBWR_STATS_INCR(mdel);
    memset(kvs, 0x0, sizeof(kvs));

    for (i = 0; i < cnt; i++) {
        kvs[i].type = KV_OP_DEL;
        memcpy(&kvs[i].k, &keys[i], sizeof(mkey_t));
    }

    return mput_i(thiz, kvs, cnt);
}

dbit_impl_t *HI_PREFIX(pget)(T *thiz, mkey_t *prefix)
{
    dbit_impl_t *it;

    DBRD_STATS_INCR(pget);
    it = thiz->get_iter(thiz, prefix, NULL);

    return it;
}

int   HI_PREFIX(pdel)(T *thiz, mkey_t *prefix)
{
    int r = 0;
    uint64_t seq;
    dbit_impl_t *it;
    mkv_t kv;

    DBWR_STATS_INCR(pdel);

    r = switch_mtb(thiz);
    if (r != 0) return r;

    seq = HI_PREFIX(get_seq)(thiz);

    RWLOCK_WRITE(&SELF->mmtb->lock);

    it = thiz->get_iter(thiz, prefix, NULL);
    it->flag |= (IT_UNSAFE | IT_ONLY_KEY);

    while(it->next(it, &kv)) {
        kv.type = KV_OP_DEL;
        kv.seq = seq;
        r = SELF->mmtb->push_unsafe(SELF->mmtb, &kv);
        if (r != 0) break;
    }

    it->destroy(it);
    RWUNLOCK(&SELF->mmtb->lock);

    return r;
}

static int   find_Ln(T *thiz, mkey_t *k, mval_t *v)
{
    int r = RC_NOT_FOUND, i;
    ftbset_t *fset;

    for (i = 0; i < thiz->conf->db_level; i++) {
        fset = thiz->fsets[i];
        r = fset->find(fset, k, v);
        if (r == RC_FOUND || r == RC_ERR) goto _out;
    }

_out:
    return r;
}

int   HI_PREFIX(get)(T *thiz, mkey_t *k, mval_t *v)
{
    int r;
    mtb_t *m;

    DBRD_STATS_INCR(get);
    m = HI_PREFIX(get_mmtb)(thiz);
    r = m->find(m, k, v);
    if (r != RC_NOT_FOUND) goto _out;

    r = SELF->imq->find(SELF->imq, k, v);
    if (r != RC_NOT_FOUND) goto _out;

    r = find_Ln(thiz, k, v);
    if (r != RC_NOT_FOUND) goto _out;

_out:
    if (r == RC_FOUND) return 0;

    return -1;
}

int HI_PREFIX(flush)(T *thiz)
{
    int r = 0;
    mtb_t *m;

    if (SELF->thpool != NULL) {
        m = HI_PREFIX(get_mmtb)(thiz);
        HI_PREFIX(notify)(thiz, NTF_FLUSH_DB);
        SELF->imq->flush_wait(SELF->imq);
        m->flush_wait(m);
    }

    return r;
}

int HI_PREFIX(checkpoint)(T *thiz)
{
    int r = 0;

    PROMPT("notify checkpoint");
    r = HI_PREFIX(flush)(thiz);
    if (r != 0) return -1;

    if (SELF->mmtb->empty(SELF->mmtb)) return 0;

    r = SELF->imq->push(SELF->imq, SELF->mmtb);
    if (r != 0) return -1;

    r = HI_PREFIX(prepare_mtb)(thiz);
    if (r != 0) return -1;

    HI_PREFIX(notify)(thiz, NTF_MTB_DUMP);

    return r;
}

