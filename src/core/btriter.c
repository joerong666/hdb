#include "hdb_inc.h"
#include "coding.h"
#include "btree_aux.h"
#include "btree.h"

#define T btriter_t

#define ITER_BEG 1

struct btriter_pri {
    uint8_t flag;
    uint16_t kcnt;
    uint16_t kidx;
    uint32_t leaf;
    uint32_t max_leaf;
    uint32_t bfsize;
    int next_flag;

    char *kshare;
    char *buf;
    fkv_t *cur_item;
    fkv_t *next_item;
};

static off_t search_first_leaf(T *thiz)
{
    int i;
    off_t off;
    hdr_block_t *hdr;
    btree_t * container = thiz->container;

    if (thiz->start == NULL) return 0;

    hdr = container->hdr;
    off = (hdr->node_cnt - 1) * BTR_INDEX_BLK_SIZE; /* root off */

    for (i = 0; i < hdr->tree_heigh; i++) {
        off = container->find_in_index(container, off, thiz->start, NULL);
        if (off <= 0) return 0;

        off = off - (off_t)(hdr->leaf_off);
    }

    return off;
}

static int seek_first(T *thiz, off_t off, fkv_t *it)
{
    int r = -1, i;
    char *blkbuf, *p, *ps, *pd, kdata[G_KSIZE_LIMIT];
    mkey_t k;

    k.data = kdata;
    blkbuf = thiz->container->hdr->map_buf + off;

    p = blkbuf + BTR_BLK_TAILER_SIZE;
    SELF->kcnt = dec_fix16(p, NULL);

    p = ps = blkbuf + BTR_LEAF_BLK_MSIZE - BTR_BLK_TAILER_SIZE;
    ps = move_to_key_pos(BTR_LEAF_BLK, ps);

    it->blktype = BTR_LEAF_BLK;
    it->kshare.data = SELF->kshare = ps;
    
    for (i = 0; i < SELF->kcnt; i++) {
        SELF->kidx = i;
        SELF->buf = p;

        pd = move_to_key_pos(it->blktype, p);
        it->kdelt.data = pd;

        p = deseri_kmeta(it, p);
        SELF->bfsize = p - SELF->buf;

        if (thiz->start == NULL && thiz->stop == NULL) return 1;
        if (thiz->cmp == NULL) return 1;

        if (it->kshare.len > 0) memcpy(k.data, it->kshare.data, it->kshare.len);
        memcpy(k.data + it->kshare.len, it->kdelt.data, it->kdelt.len);

        k.len = it->kshare.len + it->kdelt.len;

        r = thiz->cmp(&k, thiz->start, thiz->stop);
        if (r >= 0) break;
    }

    if (r < 0 && SELF->kidx + 1 == SELF->kcnt) return 0;
    return (r <= 0);
}

static int seek_next(T *thiz)
{
    int r;
    char *p, *pd, kdata[G_KSIZE_LIMIT];
    mkey_t k;

    k.data = kdata;
    fkv_t *it = SELF->next_item;

    if (SELF->kidx == SELF->kcnt) {
        if (SELF->leaf + BTR_LEAF_BLK_SIZE >= SELF->max_leaf) return 0;

        SELF->leaf += BTR_LEAF_BLK_SIZE;
        r = seek_first(thiz, SELF->leaf, SELF->next_item);
        return r;
    }

    p = SELF->buf;
    it->kshare.data = SELF->kshare;

    pd = move_to_key_pos(it->blktype, p);
    it->kdelt.data = pd;

    p = deseri_kmeta(it, p);
    SELF->bfsize = p - SELF->buf;

    if (thiz->start == NULL && thiz->stop == NULL) return 1;
    if (thiz->cmp == NULL) return 1;

    if (it->kshare.len > 0) memcpy(k.data, it->kshare.data, it->kshare.len);
    memcpy(k.data + it->kshare.len, pd, it->kdelt.len);

    k.len = it->kshare.len + it->kdelt.len;

    r = thiz->cmp(&k, thiz->start, thiz->stop);
    return (r == 0);
}

static int has_next(T *thiz)
{
    int r;

    if (SELF->next_flag) return 1;

    if (SELF->flag & IT_BEG) {
        if (thiz->start != NULL) {
            r = thiz->container->krange_cmp(thiz->container, thiz->start);
            if (r < 0) return 0;
        }

        if (thiz->stop != NULL) {
            r = thiz->container->krange_cmp(thiz->container, thiz->stop);
            if (r > 0) return 0;
        }

        SELF->flag &= ~IT_BEG;

        SELF->leaf = search_first_leaf(thiz);
        r = seek_first(thiz, SELF->leaf, SELF->next_item);

        SELF->next_flag = r;
        return r;
    }

    r = seek_next(thiz); 
    SELF->next_flag = r;

    return r;
}

static fkv_t *gen_item(T *thiz)
{
    fkv_t *item;

    UNUSED(thiz);
    item = MY_Calloc(sizeof(fkv_t) + sizeof(mkv_t));
    item->kv = (mkv_t *)((char *)item + sizeof(fkv_t));
    item->blktype = BTR_LEAF_BLK;

    return item;
}

static int next(T *thiz)
{
    MY_Free(SELF->cur_item);

    SELF->next_flag = 0;
    SELF->cur_item = SELF->next_item;
    SELF->buf += SELF->bfsize;
    SELF->kidx++;

    SELF->next_item = gen_item(thiz);

    return 0;
}

static int get(T *thiz, fkv_t **fkv)
{
    *fkv = SELF->cur_item;
    return 0;
}

static int get_next(T *thiz, fkv_t **fkv)
{
    *fkv = SELF->next_item;
    return 0;
}

/****************************************
** basic function
*****************************************/
static int init(T *thiz)
{
    SELF->flag |= IT_BEG;
    SELF->max_leaf = BTR_LEAF_BLK_SIZE * thiz->container->hdr->leaf_cnt;
    SELF->next_item = gen_item(thiz);

    return 0;
}

static void destroy(T *thiz)
{
    MY_Free(SELF->cur_item);
    MY_Free(SELF->next_item);

    del_obj(thiz);
}

T *btriter_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
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


