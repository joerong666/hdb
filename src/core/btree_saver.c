#include "inc.h"
#include "btree_aux.h"
#include "conf.h"
#include "block_saver.h"
#include "btree_saver.h"

#if 0
$ date -d '2016-01-01' +%s
1451577600
#endif
#define BASE_TIMESTAMP 1451577600

#define T btsaver_t
#define DATA_BEG_OFF(_ms) (_ms - BTR_BLK_TAILER_SIZE)

typedef struct fnode_s {
    char fname[G_MEM_MID];
    int fd;
    int blktype;
    int blksize;
    int meta_size;
    struct fnode_s *next;
} fnode_t;

typedef struct flist_s {
    int len;
    fnode_t *head;
    fnode_t *tail;
} flist_t;

struct btsaver_pri {
    off_t off;   /* start to write for this saver */

    flist_t flist;
    blksaver_t *dsv;    /* data saver */
    blksaver_t *lsv;    /* leaf saver */
};

/****************************************
** function declaration
*****************************************/
static int   init(T *thiz);
static void  destroy(T *thiz);
static int   start(T *thiz);
static int   save_kv(T *thiz, mkv_t *kv);
static int   save_fkv(T *thiz, fkv_t *fkv);
static int   flush(T *thiz);
static void  finish(T *thiz);

static int  _init(T *thiz);

/****************************************
** public function
*****************************************/
T *btsaver_create(pool_t *mpool)
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
    thiz->hdr = PCALLOC(SUPER->mpool, sizeof(hdr_block_t));

    thiz->hdr->version = DB_MAJOR_VER;
    thiz->hdr->blktype = BTR_HEADER_BLK;
    strncpy(thiz->hdr->magic, DB_MAGIC_NUM, sizeof(thiz->hdr->magic));

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(start);
    ADD_METHOD(save_kv);
    ADD_METHOD(save_fkv);
    ADD_METHOD(flush);
    ADD_METHOD(finish);

    return 0;
}

static int init(T *thiz)
{
    int r;
    struct stat st;

    r = fstat(thiz->fd, &st);
    if (r == -1) return -1;

    if (st.st_size == 0) {
        thiz->hdr->fend_off = BTR_HEADER_BLK_SIZE;
    } else {
        thiz->hdr->fend_off = st.st_size;
    }

    return 0;
}

static void destroy(T *thiz)
{
    del_obj(thiz);
}

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

    list->len++;
}

static fnode_t *gen_file(T *thiz)
{
    fnode_t *fn = PALLOC(SUPER->mpool, sizeof(*fn));
    fn->next = NULL;

    fn->fd = open_tmp_file(thiz->conf, fn->fname);
    if (fn->fd == -1) fn = NULL;

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

    append_file(thiz, fn);
    fn->blksize = sv->blksize;
    fn->meta_size = sv->meta_size;
    fn->blktype = sv->blktype;

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

static int save_index(T *thiz)
{
    int r, fd, blksize, off, i;
    char *blkbuf = NULL, *bp;
    fnode_t *fn;
    blksaver_t *sv = NULL;
    struct stat st;
    
#if 0 /* if no val block, lseek may return 0, so use SEEK_CUR instead */
    r = lseek(SELF->dsv->fd, 0, SEEK_END);
#else
    r = lseek(SELF->dsv->fd, 0, SEEK_CUR);
#endif
    if (r == -1) goto _out;

    /* move to leaf offset */
    off = r;
    thiz->hdr->leaf_off = off;

    blksize = BTR_INDEX_BLK_SIZE;
    r = MY_Memalign((void *)&blkbuf, getpagesize(), blksize);
    if (r != 0) {
        ERROR("alloc block, errno=%d", r);
        return -1;
    }

    /* read from leaf node file to construct internal node files */
    for (i = 1; ; i++) {
        fn = SELF->flist.tail;
        fd = fn->fd;

        r = fstat(fd, &st);
        if (r == -1) goto _out;

        /* new index file will append to flist */
        sv = create_index_saver(thiz);
        lseek(fd, 0, SEEK_SET);

        while ((r = io_read(fd, blkbuf, blksize)) == blksize) {
            thiz->hdr->node_cnt++;
            if (i == 1) thiz->hdr->leaf_cnt++;

            bp = blkbuf + DATA_BEG_OFF(fn->meta_size);

            mkv_t kv;
            fkv_t fkv;

            memset(&kv, 0x00, sizeof(mkv_t));
            memset(&fkv, 0x00, sizeof(fkv_t));

            /* current block info */
            fkv.kv = &kv;
            fkv.blktype = fn->blktype;

            /* deserialize the first kv */
            if (deseri_kname(&fkv, bp, bp) == NULL) {
                r = -1;
                goto _out;
            }

            /* new block info */
            fkv.blktype = BTR_INDEX_BLK;
            fkv.blkoff = off; /* point to child blkoff */
            off +=  blksize;

            r = sv->save_item(sv, &fkv);
            if (r != 0) goto _out;
        }

        if (r == -1) goto _out;
        if (r != 0) {
            ERROR("%d not equal blksize, errno=%d", r, errno);
            r = -1;
            goto _out;
        }

        sv->flush(sv);
        if (sv->blkcnt <= 1) break; /* the root */

        sv->destroy(sv);
        sv = NULL;
    }

    thiz->hdr->node_cnt++;
    thiz->hdr->tree_heigh = i;

_out:   
    if (blkbuf) MY_Free(blkbuf);
    if (sv) sv->destroy(sv);

    if (r == -1) {
        ERROR("io error, errno=%d", errno);
    } else {
        r = 0;
    }

    return r;
}

static int join_file(T *thiz, fnode_t *fn)
{
    ssize_t r;
    char *blkbuf = NULL;

    r = MY_Memalign((void *)&blkbuf, getpagesize(), fn->blksize);
    if (r != 0) {
        ERROR("alloc block, errno=%zd", r);
        return -1;
    }

    while ((r = io_read(fn->fd, blkbuf, fn->blksize)) == fn->blksize) {
        r = io_write(thiz->fd, blkbuf, fn->blksize);
        if (r != fn->blksize) goto _out;
    }

_out:
    if (r != 0) {
        ERROR("io_size=%zd, errno=%d", r, errno);
        r = -1;
    }

    MY_Free(blkbuf);
    return r;
}

static int join_files(T *thiz)
{
    ssize_t r = 0;
    fnode_t *fn;
    struct stat st;

#if 0 /* if no val block, lseek may return 0, so use SEEK_CUR instead */
    r = lseek(thiz->fd, 0, SEEK_END);
#else
    r = lseek(thiz->fd, 0, SEEK_CUR);
#endif
    if (r == -1) goto _out;

    thiz->hdr->fend_off = r;
    fn = SELF->flist.head;

    while (fn) {
        r = lseek(fn->fd, 0, SEEK_SET);
        if (r == -1) goto _out;

        r = join_file(thiz, fn);
        if (r != 0) goto _out;

        fstat(fn->fd, &st);
        thiz->hdr->fend_off += st.st_size;

        fn = fn->next;
    }

    thiz->hdr->fend_off += BTR_HEADER_BLK_SIZE;

_out:
    if (r == -1) {
        ERROR("io error, errno=%d", errno);
    }

    return r;
}

static int save_hdr(T *thiz)
{
    int r = 0;
    char *blkbuf = NULL;

    if (thiz->hdr->key_cnt == 0) return 0;

    r = MY_Memalign((void *)&blkbuf, getpagesize(), BTR_HEADER_BLK_SIZE);
    if (r != 0) {
        ERROR("alloc block, errno=%d", r);
        return -1;
    }

    seri_hdr(blkbuf, thiz->hdr);
    r = io_write(thiz->fd, blkbuf, BTR_HEADER_BLK_SIZE);
    if (r == -1) goto _out;

    /* sync before update header block */
    r = fsync(thiz->fd);
    if (r == -1) goto _out;

    lseek(thiz->fd, 0, SEEK_SET);
    r = io_write(thiz->fd, blkbuf, BTR_HEADER_BLK_SIZE);
    if (r == -1) goto _out;

_out:
    if (blkbuf) MY_Free(blkbuf);

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
    int r = 0;
    blksaver_t *dsv, *lsv;

    dsv = SELF->dsv;
    lsv = SELF->lsv;

    r = dsv->flush(dsv);
    if (r != 0) goto _out;

    r = lsv->flush(lsv);
    if (r != 0) goto _out;

    r = save_index(thiz);
    if (r != 0) goto _out;

    r = join_files(thiz);
    if (r != 0) goto _out;

    r = save_hdr(thiz);
    if (r != 0) goto _out;

_out:
    return r;
}

static void finish(T *thiz)
{
    blksaver_t *dsv, *lsv;
    fnode_t *fn;

    dsv = SELF->dsv;
    lsv = SELF->lsv;

    dsv->destroy(dsv);
    lsv->destroy(lsv);

    fn = SELF->flist.head;
    while (fn != NULL) {
        close(fn->fd);
        remove(fn->fname);

        fn = fn->next;
    }
}

