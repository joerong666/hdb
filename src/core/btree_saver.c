#include "inc.h"
#include "btree_aux.h"
#include "conf.h"
#include "btree.h"
#include "block_saver.h"
#include "btree_saver.h"

#define T btsaver_t
#define DATA_BEG_OFF(_ms) (_ms - BTR_BLK_TAILER_SIZE)

typedef struct fnode_s {
    int fd;
    blksaver_t *sv;
    struct fnode_s *next;
} fnode_t;

typedef struct flist_s {
    fnode_t *head;
    fnode_t *tail;
} flist_t;

struct btsaver_pri {
    off_t off;   /* start to write for this saver */

    flist_t flist;
    blksaver_t *dsv;    /* data saver */
    blksaver_t *lsv;    /* leaf saver */
};

static void append_file(T *thiz, fnode_t *fn)
{
    flist_t *list = &SELF->flist;

    if (list->head == NULL) {
        list->head = fn;
        list->tail = fn;
    } else {
        list->tail->next = fn;
        list->tail = fn;
    }
}

static fnode_t *gen_file(T *thiz)
{
    fnode_t *fn = PALLOC(SUPER->mpool, sizeof(*fn));
    fn->next = NULL;

    fn->fd = thiz->fd;
    return fn;
}

static blksaver_t *gen_blksaver(T *thiz, blksaver_t *(*create_saver)(pool_t *))
{
    fnode_t *fn;
    blksaver_t *sv;

    fn = gen_file(thiz);
    if (fn == NULL) return NULL;

    sv = create_saver(NULL);
    sv->fd = fn->fd;
    sv->hdr = thiz->hdr;
    fn->sv = sv;

    append_file(thiz, fn);

    return sv;
}

static blksaver_t *create_val_saver(T *thiz)
{
    int r;
    blksaver_t *sv;

    r = lseek(thiz->fd, thiz->hdr->fend_off, SEEK_SET);
    if (r == -1) {
        ERROR("lseek, errno=%d, off=%"PRIu32, errno, thiz->hdr->fend_off);
        return NULL;
    }

    sv = val_blksaver_create(NULL);
    sv->fd = thiz->fd;
    sv->hdr = thiz->hdr;

    return sv;
}

static blksaver_t *create_leaf_saver(T *thiz)
{
    blksaver_t *sv;

    sv = gen_blksaver(thiz, leaf_blksaver_create);

    return sv;
}

static blksaver_t *create_index_saver(T *thiz)
{
    blksaver_t *sv;

    sv = gen_blksaver(thiz, index_blksaver_create);

    return sv;
}

static int sync_pages(T *thiz, blksaver_t *sv)
{
    ssize_t r;
    blkpage_t *bp;

    list_for_each_entry(bp, &sv->page_list, page_node) {
        if (sv->blktype == BTR_LEAF_BLK ||
            sv->blktype == BTR_INDEX_BLK) thiz->hdr->node_cnt++;
        if (sv->blktype == BTR_LEAF_BLK) thiz->hdr->leaf_cnt++;

        r = io_write(sv->fd, bp->buf, sv->blksize);
        if (r == -1) return -1;
    }

    return 0;
}

static int save_index(T *thiz)
{
    int r, blksize, off, i;
    char *blkbuf = NULL, *bp;
    fnode_t *fn;
    blkpage_t *bpg, *bpt;
    blksaver_t *osv = NULL, *nsv = NULL;
    
    /* move to leaf offset */
    off = thiz->hdr->leaf_off;
    blksize = BTR_INDEX_BLK_SIZE;

    /* read from leaf node file to construct internal node files */
    for (i = 1; ; i++) {
        fn = SELF->flist.tail;
        osv = fn->sv;

        /* new index file will append to flist */
        nsv = create_index_saver(thiz);

        list_for_each_entry(bpg, &osv->page_list, page_node) {
            blkbuf = bpg->buf;
            bp = blkbuf + DATA_BEG_OFF(osv->meta_size);

            mkv_t kv;
            fkv_t fkv;

            memset(&kv, 0x00, sizeof(mkv_t));
            memset(&fkv, 0x00, sizeof(fkv_t));

            /* current block info */
            fkv.kv = &kv;
            fkv.blktype = osv->blktype;

            /* deserialize the first kv */
            if (deseri_kname(&fkv, bp, bp) == NULL) {
                r = -1;
                goto _out;
            }

            /* new block info */
            fkv.blktype = BTR_INDEX_BLK;
            fkv.blkoff = off; /* point to child blkoff */
            off +=  blksize;

            r = nsv->save_item(nsv, &fkv);
            if (r != 0) goto _out;
        }

        list_for_each_entry_safe(bpg, bpt, &osv->page_list, page_node) {
            MY_AlignFree(bpg->buf);
            list_del(&bpg->page_node);
        }

        if (osv != SELF->lsv) {
            osv->destroy(osv);
            osv = NULL;
        }

        nsv->flush(nsv);
        r = sync_pages(thiz, nsv);
        if (r != 0) goto _out;

        if (nsv->blkcnt <= 1) break; /* the root */
    }

    thiz->hdr->tree_heigh = i;

_out:   
    if (osv && osv != SELF->lsv) osv->destroy(osv);
    if (nsv) nsv->destroy(nsv);

    if (r == -1) {
        ERROR("io error, errno=%d", errno);
    } else {
        r = 0;
    }

    return r;
}

static int save_hdr(T *thiz)
{
    ssize_t r = 0;
    char *blkbuf = NULL;

    if (thiz->hdr->key_cnt == 0) return 0;

    r = lseek(thiz->fd, 0, SEEK_CUR);
    thiz->hdr->fend_off = r + BTR_HEADER_BLK_SIZE;

    blkbuf = MY_Memalign(BTR_HEADER_BLK_SIZE);
    if (blkbuf == NULL) return -1;

    seri_hdr(blkbuf, thiz->hdr);
    r = io_write(thiz->fd, blkbuf, BTR_HEADER_BLK_SIZE);
    if (r == -1) goto _out;

    /* sync before update header block */
    r = fsync(thiz->fd);
    if (r == -1) goto _out;

    lseek(thiz->fd, 0, SEEK_SET);
    r = io_write(thiz->fd, blkbuf, BTR_HEADER_BLK_SIZE);
    if (r == -1) goto _out;

    lseek(thiz->fd, thiz->hdr->fend_off, SEEK_SET);

_out:
    if (blkbuf) MY_AlignFree(blkbuf);

    if (r == -1) {
        ERROR("io error, errno=%d", errno);
    } else {
        r = 0;
    }

    return r;
}

static int save_kv(T *thiz, mkv_t *kv)
{
    int r;
    fkv_t fkv;

    memset(&fkv, 0x00, sizeof(fkv_t));
    fkv.kv = kv;

    if (!(kv->type & KV_OP_DEL)) {
        r = SELF->dsv->save_item(SELF->dsv, &fkv);
        if (r != 0) return r;
    }

    r = SELF->lsv->save_item(SELF->lsv, &fkv);
    if (r == 0) thiz->hdr->key_cnt++;

    return r;
}

static int save_fkv(T *thiz, fkv_t *fkv)
{
    int r;
    blksaver_t *dsv, *lsv;

    dsv = SELF->dsv;
    lsv = SELF->lsv;

    if (!(fkv->kv->type & KV_MERGE_KEEP_OLD) && !(fkv->kv->type & KV_OP_DEL)) {
        r = dsv->save_item(dsv, fkv);
        if (r != 0) return r;
    }

    r = lsv->save_item(lsv, fkv);
    if (r == 0) thiz->hdr->key_cnt++;

    return r;
}

static int start(T *thiz)
{
    SELF->dsv = create_val_saver(thiz);
    SELF->lsv = create_leaf_saver(thiz);

    if (SELF->dsv == NULL || SELF->lsv == NULL) return -1;

    return 0;
}

static int flush(T *thiz)
{
    ssize_t r = 0;
    blksaver_t *dsv, *lsv;

    dsv = SELF->dsv;
    lsv = SELF->lsv;

    r = dsv->flush(dsv);
    if (r != 0) goto _out;

    r = lsv->flush(lsv);
    if (r != 0) goto _out;

    r = lseek(thiz->fd, 0, SEEK_CUR);
    thiz->hdr->leaf_off = r;

    r = sync_pages(thiz, lsv);
    if (r != 0) goto _out;

    r = save_index(thiz);
    if (r != 0) goto _out;

    r = save_hdr(thiz);
    if (r != 0) goto _out;

_out:
    return r;
}

static void finish(T *thiz)
{
    blksaver_t *dsv, *lsv;

    dsv = SELF->dsv;
    lsv = SELF->lsv;

    dsv->destroy(dsv);
    lsv->destroy(lsv);
}

/****************************************
** basic function
*****************************************/
static int init(T *thiz, btree_t *base)
{
    thiz->fd = base->wfd;
    thiz->file = base->file;
    thiz->conf = base->conf;
#if 1
    thiz->hdr->fend_off = base->hdr->fend_off;
    thiz->hdr->cpct_cnt = base->hdr->cpct_cnt + 1;
    thiz->hdr->version = DB_FILE_VERSION;
    thiz->hdr->blktype = BTR_HEADER_BLK;
    strncpy(thiz->hdr->magic, DB_MAGIC_NUM, sizeof(thiz->hdr->magic));
#else
    memcpy(thiz->hdr, base->hdr, sizeof(hdr_block_t));
#endif

    return 0;
}

static void destroy(T *thiz)
{
    del_obj(thiz);
}

static int _init(T *thiz)
{
    thiz->hdr = PCALLOC(SUPER->mpool, sizeof(hdr_block_t));

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(start);
    ADD_METHOD(save_kv);
    ADD_METHOD(save_fkv);
    ADD_METHOD(flush);
    ADD_METHOD(finish);

    return 0;
}

T *btsaver_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    _init(thiz);

    return thiz;           
}

