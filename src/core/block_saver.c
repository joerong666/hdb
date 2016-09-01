#include "inc.h"
#include "coding.h"
#include "btree_aux.h"
#include "block_saver.h"

#define T blksaver_t
#define META_SIZE 64

#define APPEND_KEY_CHAIN(_fkv) do {                             \
    nchain = chain_alloc_buflink(SUPER->mpool);                 \
    if (_fkv->kv->type & KV_KTP_FROM_FILE) {                    \
        nchain->buf->start = _fkv->kv->k.data;                  \
    } else {                                                    \
        nchain->buf->start = NULL;                              \
    }                                                           \
    _fkv->kshare.data = _fkv->kv->k.data;                       \
    _fkv->kdelt.data = _fkv->kv->k.data + _fkv->kshare.len;     \
    nchain->buf->pos = _fkv->kdelt.data;                        \
    nchain->buf->last = _fkv->kdelt.data + _fkv->kdelt.len;     \
    chain_append(&SELF->tchain, nchain);                        \
    SELF->tchain = nchain;                                      \
} while (0)

/* just append for free, no data to write */
#define APPEND_VAL_CHAIN(_fkv) do {                             \
    nchain = chain_alloc_buflink(SUPER->mpool);                 \
    nchain->buf->start = _fkv->kv->v.data;                      \
    nchain->buf->pos = _fkv->kv->v.data;                        \
    nchain->buf->last = _fkv->kv->v.data;                       \
    chain_append(&SELF->tchain, nchain);                        \
    SELF->tchain = nchain;                                      \
} while (0)

/* just append for write, no need free */
#define APPEND_BUF_CHAIN(_buf, _len) do {                       \
    nchain = chain_alloc_buflink(SUPER->mpool);                 \
    nchain->buf->start = NULL;                                  \
    nchain->buf->pos = _buf;                                    \
    nchain->buf->last = _buf + (_len);                          \
    chain_append(&SELF->tchain, nchain);                        \
    SELF->tchain = nchain;                                      \
} while (0)

#define VAL_OFF() (thiz->blksize - SELF->left - BTR_BLK_TAILER_SIZE)
#define DATA_BEG_OFF() (thiz->meta_size - BTR_BLK_TAILER_SIZE)
#define DATA_END_OFF() (thiz->blksize - BTR_BLK_TAILER_SIZE)

#define FLG_INIT 0
#define FLG_FIRST_INS 1

struct blksaver_pri {
    int flag;
    int key_cnt;
    size_t left;
    uint32_t vblkoff;

    mkey_t last_k;
    mkey_t share_k;

    chain_t *chain;  /* head of block buf chain */
    chain_t *tchain; /* tail of block buf chain */
};

/****************************************
** function declaration
*****************************************/
static void  destroy(T *thiz);
static int flush(T *thiz);

static int  _init(T *thiz);

/****************************************
** public function
*****************************************/
T *blksaver_create(pool_t *mpool)
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

/****************************************
** private function
*****************************************/
static int _init(T *thiz)
{
    ADD_METHOD(destroy);
    ADD_METHOD(flush);

    return 0;
}

static void destroy(T *thiz)
{
    del_obj(thiz);
}

static int seri_blk_meta(T *thiz, char *blkbuf)
{
    char *p = blkbuf + 4;

    *p = thiz->blktype & 0xFF;
    p++;

    switch (thiz->blktype) {
        case BTR_LEAF_BLK:
        case BTR_INDEX_BLK:
            p = enc_fix16(p, SELF->key_cnt);
            break;
        case BTR_VAL_BLK:
            /* anything to do ? */
            break;
        case BTR_FILTER_BLK:
            break;
    }

    ASSERT((p - blkbuf) <= DATA_BEG_OFF());

    p = blkbuf + DATA_END_OFF();
    *p = thiz->blktype & 0xFF;

    return 0;
}

static int save_block(T *thiz)
{
    int r, sz, tsz, free_flag = 0;
    chain_t *tchain;
    char *blkbuf = NULL, *p;

    if (SELF->chain == NULL) {
        INFO("chain is null, nothing to write!!");
        return 0;
    }

    tsz = thiz->meta_size;
    r = MY_Memalign((void *)&blkbuf, getpagesize(), thiz->blksize);
    if (r != 0) {
        ERROR("alloc block, errno=%d", r);
        return -1;
    }

    memset(blkbuf, 0x0, thiz->blksize);

    r = seri_blk_meta(thiz, blkbuf);
    if (r != 0) goto _out;

    p = blkbuf + DATA_BEG_OFF();
    tchain = SELF->chain;

    while (tchain != NULL) {
        /* need free later */
        if (tchain->buf != NULL && tchain->buf->start != NULL) free_flag = 1;

        if (tchain->buf == NULL || tchain->buf->last == tchain->buf->pos) {
            tchain = tchain->next;
            continue;
        }

        sz = tchain->buf->last - tchain->buf->pos;
        tsz += sz;
        ASSERT(tsz <= thiz->blksize);

        memcpy(p, tchain->buf->pos, sz);

        DEBUG("saving item, size=%d: %.*s", sz, sz, p);
        p += sz;

        tchain = tchain->next;
    }

    r = wrap_block_crc(blkbuf, thiz->blksize);
    if (r == -1) goto _out;

    r = io_write(thiz->fd, blkbuf, thiz->blksize);
    if (r == -1) goto _out;

    thiz->blkcnt++;

_out:
    MY_Free(blkbuf);

    /* memories of key, value within each block are freed here */
    tchain = SELF->chain;
    while (free_flag == 1 && tchain != NULL) {
        if (tchain->buf == NULL || tchain->buf->start == NULL) {
            tchain = tchain->next;
            continue;
        }

        MY_Free(tchain->buf->start);
        tchain = tchain->next;
    }
 
    if (r >= 0) {
        r = 0;
    }
    
    SELF->chain = NULL;
    return r;
}

static int new_block(T *thiz)
{
    int r = 0;

    SELF->chain = chain_alloc_link(SUPER->mpool);
    SELF->chain->buf = NULL;
    SELF->tchain = SELF->chain;

    SELF->left = thiz->blksize - thiz->meta_size;

    return r;
}

static void save_beg_key(T *thiz, fkv_t *item)
{
    if (thiz->blktype != BTR_LEAF_BLK) return;

    if (SELF->flag & FLG_FIRST_INS) goto _out;

    SELF->flag |= FLG_FIRST_INS;

    thiz->hdr->beg.len = item->kv->k.len;
    thiz->hdr->beg.data = PALLOC(SUPER->mpool, item->kv->k.len);
    memcpy(thiz->hdr->beg.data, item->kv->k.data, item->kv->k.len);

_out:
    memcpy(&SELF->last_k, &item->kv->k, sizeof(mkey_t));
}

static void save_end_key(T *thiz)
{
    if (thiz->blktype != BTR_LEAF_BLK) return;

    thiz->hdr->end.len = SELF->last_k.len;
    thiz->hdr->end.data = PALLOC(SUPER->mpool, SELF->last_k.len);
    memcpy(thiz->hdr->end.data, SELF->last_k.data, SELF->last_k.len);
}

static int flush(T *thiz)
{
    int r;

    save_end_key(thiz);

    DEBUG("flush last block");
    r = save_block(thiz);

    return r;
}

static int save_val_item(T *thiz, fkv_t *item)
{
    int r;
    size_t vlen;
    uint8_t *v;
    chain_t *nchain;

    if (SELF->left == 0) {
        if (SELF->chain != NULL) {
            r = save_block(thiz);
            if (r == -1) return r;
        }

        new_block(thiz);
        SELF->vblkoff = thiz->hdr->fend_off + thiz->blkcnt * thiz->blksize;
    }

    ASSERT(SELF->left > 0);

    item->blkoff = SELF->vblkoff;
    item->voff = VAL_OFF();

    v = item->kv->v.data;
    vlen = item->kv->v.len;

    item->kv->vcrc = calc_crc16(v, vlen);

    while (SELF->left < vlen) {
        APPEND_BUF_CHAIN(v, SELF->left);

        r = save_block(thiz);
        if (r == -1) return r;

        v += SELF->left;
        vlen -= SELF->left;

        new_block(thiz);
        SELF->vblkoff = thiz->hdr->fend_off + thiz->blkcnt * thiz->blksize;
    }

    ASSERT(SELF->left > 0);

    if (SELF->left >= vlen && vlen > 0) {
        APPEND_BUF_CHAIN(v, vlen);

        SELF->left -= vlen;
    }

    if (item->kv->type & KV_VTP_FROM_FILE) {
        /* just add for freeing later */
        APPEND_VAL_CHAIN(item);
    }

    return 0;
}

static int save_index_item(T *thiz, fkv_t *item)
{
    int r = 0, item_size;
    chain_t *nchain;
    char *kmeta, *m, *pt;

    if (SELF->key_cnt == 0) memcpy(&SELF->share_k, &item->kv->k, sizeof(mkey_t));

    item_size = seri_keyitem_size(item, &SELF->share_k, &item->kv->k);
    
    if (SELF->left == 0 || (int)SELF->left < item_size) { /* begin from new block */
        if (SELF->chain != NULL) {
            r = save_block(thiz);
            if (r == -1) return r;
        }

        new_block(thiz);

        SELF->key_cnt = 0;
        memcpy(&SELF->share_k, &item->kv->k, sizeof(mkey_t));

        /* re-calculate*/
        item_size = seri_keyitem_size(item, &SELF->share_k, &item->kv->k);
    }

    kmeta = PALLOC(SUPER->mpool, META_SIZE);

    if (item->blktype != BTR_INDEX_BLK && !(item->kv->type & KV_OP_DEL)) {
        m = seri_kmeta(kmeta, META_SIZE, item);
        pt = move_to_key_pos(item->blktype, kmeta);
        APPEND_BUF_CHAIN((uint8_t *)kmeta, pt - kmeta);
    } else {
        pt = m = seri_kmeta(kmeta, META_SIZE, item);
        APPEND_BUF_CHAIN((uint8_t *)kmeta, m - kmeta);
    }

    APPEND_KEY_CHAIN(item);

    /* add vlen chain */
    if (pt != m) {
        APPEND_BUF_CHAIN((uint8_t *)pt, m - pt);
    }

    SELF->left -= item_size;
    SELF->key_cnt++;

    save_beg_key(thiz, item);

    return r;
}

static int save_filter_item(T *thiz, fkv_t *item)
{
    /* TODO!! */
    return 0;
}

blksaver_t *val_blksaver_create(pool_t *mpool)
{
    blksaver_t *thiz;

    thiz = blksaver_create(mpool);

    thiz->meta_size = BTR_VAL_BLK_MSIZE;
    thiz->blksize = BTR_VAL_BLK_SIZE;
    thiz->blktype = BTR_VAL_BLK;
    thiz->save_item = save_val_item;

    return thiz;
}

blksaver_t *index_blksaver_create(pool_t *mpool)
{
    blksaver_t *thiz;

    thiz = blksaver_create(mpool);

    thiz->blktype = BTR_INDEX_BLK;
    thiz->meta_size = BTR_INDEX_BLK_MSIZE;
    thiz->blksize = BTR_INDEX_BLK_SIZE;
    thiz->save_item = save_index_item;

    return thiz;
}

blksaver_t *leaf_blksaver_create(pool_t *mpool)
{
    blksaver_t *thiz;

    thiz = index_blksaver_create(mpool);
    thiz->blktype = BTR_LEAF_BLK;
    thiz->meta_size = BTR_LEAF_BLK_MSIZE;

    return thiz;
}

blksaver_t *filter_blksaver_create(pool_t *mpool)
{
    blksaver_t *thiz;

    thiz = blksaver_create(mpool);

    thiz->meta_size = BTR_FILTER_BLK_MSIZE;
    thiz->blksize = BTR_FILTER_BLK_SIZE;
    thiz->blktype = BTR_FILTER_BLK;
    thiz->save_item = save_filter_item;

    return thiz;
}


