#include "inc.h"
#include "coding.h"
#include "btree_aux.h"
#include "conf.h"
#include "htable.h"
#include "btree.h"
#include "btree_saver.h"

#define T btree_t
#define HDRBLK_KRANGE_OFF 20
#define MG_STATE_NORMAL  0
#define MG_STATE_INVALID 1
#define LOCK (&thiz->lock)

struct btree_pri {
    int mg_state;

    int (*mg_filter)(fkv_t *);
    btsaver_t *mg_sv;
    btriter_t *mg_iter;
};

static int range_cmp(T *thiz, T *other)
{
    int r;
    size_t len;
    hdr_block_t *h1, *h2;

    h1 = thiz->hdr;
    h2 = other->hdr;

    len = h1->end.len <= h2->beg.len ? h1->end.len : h2->beg.len;
    r = memcmp(h1->end.data, h2->beg.data, len);
    if (r < 0) return -1;
    if (r == 0 && h1->end.len < h2->beg.len) return -1;

    len = h1->beg.len <= h2->end.len ? h1->beg.len : h2->end.len;
    r = memcmp(h1->beg.data, h2->end.data, len);
    if (r > 0) return +1;
    if (r == 0 && h1->beg.len > h2->end.len) return +1;

    return 0;
}

static int krange_cmp(T *thiz, mkey_t *k)
{
    int r;
    size_t len;
    hdr_block_t *h;

    h = thiz->hdr;

    len = h->end.len <= k->len ? h->end.len : k->len;
    r = memcmp(h->end.data, k->data, len);
    if (r < 0) return -1;
    if (r == 0 && h->end.len < k->len) return -1;

    len = h->beg.len <= k->len ? h->beg.len : k->len;
    r = memcmp(h->beg.data, k->data, len);
    if (r > 0) return +1;
    if (r == 0 && h->beg.len > k->len) return +1;

    return 0;
}

static int pkrange_cmp(T *thiz, mkey_t *k)
{
    int r;
    size_t len;
    hdr_block_t *h;

    h = thiz->hdr;

    len = h->end.len <= k->len ? h->end.len : k->len;
    r = memcmp(h->end.data, k->data, len);
    if (r < 0) return -1;
    if (r == 0 && h->end.len < k->len) return -1;

    len = h->beg.len <= k->len ? h->beg.len : k->len;
    r = memcmp(h->beg.data, k->data, len);
    if (r > 0) return +1;

    return 0;
}

static int update_hdr(T *thiz, hdr_block_t *hdr)
{
    int r = 0;
    size_t hdr_off, pz = getpagesize();
    char *buf;
    hdr_block_t *h;

    RWLOCK_WRITE(LOCK);
    SELF->mg_state = MG_STATE_NORMAL;
    if (hdr->key_cnt == 0) {
        INFO("%s no key valid, no need update_hdr!!", thiz->file);
        SELF->mg_state = MG_STATE_INVALID;
        goto _out;
    }

    buf = thiz->hdr->map_buf;
    if (buf != NULL) {
        r = munmap(buf, thiz->hdr->map_size);
        if (r == -1) {
            ERROR("munmap errno=%d", errno);
            goto _out;
        }

        buf = thiz->hdr->map_buf = NULL;
    }

    if (buf == NULL) {  /* re-mmap */
        if (hdr->leaf_off < pz) {
            hdr->map_off = 0;
        } else {
            hdr->map_off = hdr->leaf_off;
        }

        hdr->map_size = hdr->fend_off - hdr->map_off;
        buf = mmap(NULL, hdr->map_size, PROT_READ, MAP_PRIVATE, thiz->rfd, hdr->map_off);

        if (buf == MAP_FAILED) {
            ERROR("mmap errno=%d", errno);
            r = -1;
            goto _out;
        }

        madvise(buf, hdr->map_size, MADV_WILLNEED);
    }

    memcpy(thiz->hdr, hdr, sizeof(*hdr));
    thiz->hdr->map_buf = buf;

    h = thiz->hdr;

    hdr_off = h->fend_off - h->map_off - BTR_HEADER_BLK_SIZE;
    h->beg.data = (buf + hdr_off + HDRBLK_KRANGE_OFF + 1);
    h->end.data = (h->beg.data + h->beg.len + 1);

    INFO("update %s\n"
        "krange: %.*s~%.*s\n" 
        "key_cnt: %"PRIu32"\n"
        , thiz->file 
        , thiz->hdr->beg.len, thiz->hdr->beg.data 
        , thiz->hdr->end.len, thiz->hdr->end.data
        , thiz->hdr->key_cnt
        );
_out:
    RWUNLOCK(LOCK);

    return r;
}

static int prepare_file(T *thiz)
{
    if (thiz->wfd == -1) {
        thiz->wfd = open(thiz->file, O_CREAT | O_RDWR, 0644);

        if (thiz->wfd == -1) {
            ERROR("open %s, errno=%d", thiz->file, errno);
            return -1;
        }
    }

    if (thiz->rfd == -1) {
        thiz->rfd = open(thiz->file, O_CREAT | O_RDONLY, 0644);
        if (thiz->rfd == -1) {
            ERROR("open %s, errno=%d", thiz->file, errno);
            return -1;
        }
    }

    return 0;
}

static int store(T *thiz, htable_t *htb)
{
    ssize_t r = 0;
    btsaver_t *sv;
    htiter_t *iter;

    r = prepare_file(thiz);
    if (r != 0) return -1;

    iter = htb->get_iter(htb, NULL, NULL, NULL);

    sv = btsaver_create(NULL);
    sv->init(sv, thiz);
    sv->start(sv);

    while (iter->has_next(iter)) {
        mkv_t *kv;

        iter->next(iter);
        iter->get(iter, (void **)&kv);

        DEBUG("iter key %.*s", (int)kv->k.len, kv->k.data);

        r = sv->save_kv(sv, kv);
        if (r != 0) goto _out;
    }

    r = sv->flush(sv);
    if (r != 0) goto _out;

    r = update_hdr(thiz, sv->hdr);
_out:
    if (r != 0) r = -1;

    iter->destroy(iter);
    sv->finish(sv);
    sv->destroy(sv);

    return r;
}

static int diagnosis(T *thiz)
{
    int r = 0, r1, r2, blktype = 0;
    struct stat st;
    char *hdr_buf = NULL, *tail_buf = NULL;

    hdr_buf = MY_Memalign(BTR_HEADER_BLK_SIZE);
    tail_buf = MY_Memalign(BTR_HEADER_BLK_SIZE);

    if (hdr_buf == NULL || tail_buf == NULL) {
        r = -1;
        goto _out;
    }

    r1 = io_pread(thiz->wfd, hdr_buf, BTR_HEADER_BLK_SIZE, 0);
    if (r1 == -1 || r1 != BTR_HEADER_BLK_SIZE) {
        ERROR("pread errno=%d, r=%d", errno, BTR_HEADER_BLK_SIZE);
        r = -1;
        goto _out;
    }

    /* read tailer block */
    fstat(thiz->wfd, &st);
    r2 = io_pread(thiz->wfd, tail_buf, BTR_HEADER_BLK_SIZE, st.st_size - BTR_HEADER_BLK_SIZE);
    if (r2 == -1 || r2 != BTR_HEADER_BLK_SIZE) {
        ERROR("pread errno=%d, r=%d", errno, r2);
        r = -1;
        goto _out;
    }

    r1 = r2 = 0;

    blktype = *(hdr_buf + 4);
    if (blktype != BTR_HEADER_BLK) {
        ERROR("header not a real BTR_HEADER_BLK, blktype=%d", blktype);
        r1 = -1;
    }

    blktype = *(tail_buf + 4);
    if (blktype != BTR_HEADER_BLK) {
        ERROR("tailer not a real BTR_HEADER_BLK, blktype=%d", blktype);
        r2 = -1;
    }

    if (r1 == -1 && r2 == -1) {
        r = -1;
        goto _out;
    }

    if (r1 == 0) {
        r1 = blk_crc32_check(hdr_buf, BTR_HEADER_BLK_SIZE);
        /* return if header block is correct, regardless of tailer block */
        if (r1 == 0) goto _out;
        else {
            ERROR("header block crc error");
        }
    }

    r2 = blk_crc32_check(tail_buf, BTR_HEADER_BLK_SIZE);
    if (r2 != 0) {
        ERROR("tailer block crc error");
        r = -1;
        goto _out;
    }

    PROMPT("repair header block");
    r2 = io_pwrite(thiz->wfd, tail_buf, BTR_HEADER_BLK_SIZE, 0);
    if (r2 == -1 || r2 != BTR_HEADER_BLK_SIZE) {
        ERROR("repair header block error");
        r = -1;
        goto _out;
    }

_out:
    if (hdr_buf) MY_AlignFree(hdr_buf);
    if (tail_buf) MY_AlignFree(tail_buf);

    return r;
}

static int read_hdr(T *thiz, hdr_block_t *hdr)
{
    int r;
    char blkbuf[BTR_HEADER_BLK_SIZE];

    r = io_pread(thiz->rfd, blkbuf, sizeof(blkbuf), 0);
    if (r == -1) {
        ERROR("pread errno=%d", errno);
        return -1;
    }

    r = deseri_hdr(hdr, blkbuf);
    if (r != 0) return -1;

    return 0;
}

static int fit_file(T *thiz, hdr_block_t *hdr)
{
    int r = 0;
    struct stat st;
    char *hdr_buf = NULL, *tail_buf = NULL;

    fstat(thiz->wfd, &st);
    if (hdr->fend_off == st.st_size) goto _out;
    if (hdr->fend_off > st.st_size) {
        ERROR("offset=%"PRIu32" bigger than fsize=%zu", hdr->fend_off, st.st_size);
        r = -1;
        goto _out;
    }

    PROMPT("repair tailer block");
    r = ftruncate(thiz->wfd, hdr->fend_off);
    if (r != 0) {
        ERROR("ftruncate errno=%d", errno);
        r = -1;
        goto _out;
    }

    hdr_buf = MY_Memalign(BTR_HEADER_BLK_SIZE);
    tail_buf = MY_Memalign(BTR_HEADER_BLK_SIZE);

    io_pread(thiz->wfd, hdr_buf, BTR_HEADER_BLK_SIZE, 0);
    io_pread(thiz->wfd, tail_buf, BTR_HEADER_BLK_SIZE, hdr->fend_off - BTR_HEADER_BLK_SIZE);

    r = memcmp(hdr_buf, tail_buf, BTR_HEADER_BLK_SIZE);
    if (r != 0) {
        ERROR("header still not euqal tailer");
        r = -1;
    }

    MY_AlignFree(hdr_buf);
    MY_AlignFree(tail_buf);

_out:
    fsync(thiz->wfd);
    return r;

}

static int restore(T *thiz)
{
    int r;
    hdr_block_t hdr;

    INFO("restoring %s", thiz->file);

    r = prepare_file(thiz);
    if (r != 0) return -1;

    r = diagnosis(thiz);
    if (r != 0) return -1;

    r = read_hdr(thiz, &hdr);
    if (r != 0) return -1;

    r = fit_file(thiz, &hdr);
    if (r != 0) return -1;

    r = update_hdr(thiz, &hdr);
    if (r != 0) return -1;

    return 0;
}

static btriter_t *get_iter(T *thiz, mkey_t *start, mkey_t *stop, BTITERCMP cmp)
{
    btriter_t *iter = btriter_create(NULL);
    iter->container = thiz;
    iter->start = start;
    iter->stop  = stop;
    iter->cmp   = cmp;

    iter->init(iter);

    return iter;
}

static int merge_start(T *thiz, int (*filter)(fkv_t *))
{
    btsaver_t *sv;
    btriter_t *iter;

    sv = btsaver_create(NULL);
    sv->init(sv, thiz);
    sv->start(sv);

    iter = thiz->get_iter(thiz, NULL, NULL, NULL);

    SELF->mg_sv = sv;
    SELF->mg_iter = iter;
    SELF->mg_filter = filter;

    return 0;
}

static int merge(T *thiz, fkv_t *nkv)
{
    int r = 0, skip = 0;
    fkv_t *okv;
    btsaver_t *sv = SELF->mg_sv;
    btriter_t *iter = SELF->mg_iter;

    while (iter->has_next(iter)) {
        r = iter->get_next(iter, &okv);
        if (r != 0) goto _out;

        r = fkv_cmp(okv, nkv);

        if (r > 0) break;
        if (r == 0) {
            if (okv->kv->seq < nkv->kv->seq) {
                iter->next(iter);
                break;
            } else skip = 1;
        }

        if (SELF->mg_filter && SELF->mg_filter(okv)) {
            iter->next(iter);
            continue;
        }

        extract_fkey(okv);
        okv->kv->type |= KV_MERGE_KEEP_OLD;

        r = sv->save_fkv(sv, okv);
        if (r != 0) {
            MY_Free(okv->kv->k.data);
            MY_Free(okv->kv->v.data);
            r = -1;
            goto _out;
        }

        iter->next(iter);

        if (skip) break;
    }

    if (!skip && SELF->mg_filter && SELF->mg_filter(nkv)) {
        skip = 1;
    }

    r = 0;
    if (skip || (r = sv->save_fkv(sv, nkv)) != 0) {
        MY_Free(nkv->kv->k.data);
        MY_Free(nkv->kv->v.data);
    }

_out:
    if (r > 0) r = 0;

    return r;
}

static int merge_flush(T *thiz)
{
    int r = 0;
    fkv_t *okv;
    btsaver_t *sv = SELF->mg_sv;
    btriter_t *iter = SELF->mg_iter;

    while (iter->has_next(iter)) {
        r = iter->get_next(iter, &okv);
        if (r != 0) break;

        if (SELF->mg_filter && SELF->mg_filter(okv)) {
            iter->next(iter);
            continue;
        }

        extract_fkey(okv);
        okv->kv->type |= KV_MERGE_KEEP_OLD;

        r = sv->save_fkv(sv, okv);
        if (r != 0) {
            MY_Free(okv->kv->k.data);
            MY_Free(okv->kv->v.data);
            r = -1;
            break;
        }

        iter->next(iter);
    }

    if (r != 0) goto _out;

    r = sv->flush(sv);
    if (r != 0) goto _out;

_out:
    return r;
}

static int merge_hdr(T *thiz)
{
    int r = 0;

    r = update_hdr(thiz, SELF->mg_sv->hdr);

    return r;
}

static int merge_fin(T *thiz)
{
    int r = 0;
    btsaver_t *sv = SELF->mg_sv;
    btriter_t *iter = SELF->mg_iter;

    /* release read-lock first */
    iter->destroy(iter);

    sv->finish(sv);
    sv->destroy(sv);

    return r;
}

static int invalid(T *thiz)
{
    int r = 1;

    RWLOCK_READ(LOCK);
    if (thiz->hdr->node_cnt == 0) {
        DEBUG("%s empty", thiz->file);
        goto _out;
    }

    if (SELF->mg_state == MG_STATE_INVALID) {
        DEBUG("%s all keys invalid", thiz->file);
        goto _out;
    }

    r = 0;
_out:
    RWUNLOCK(LOCK);

    return r;
}

/*
* @ret: 
*  0 => not found
*  >0 => child offset
*/
off_t find_in_index(T *thiz, off_t ioff, mkey_t *target, KCMP cmp)
{
    int r, type, key_cnt, i;
    off_t toff, child_off = 0;
    uint16_t share_ks, delt_ks;
    char *blkbuf, *pb, *p, kdata[G_KSIZE_LIMIT];
    mkey_t k;
    KCMP kcmp = cmp ? cmp : key_cmp;

    blkbuf = thiz->hdr->map_buf + ioff;
    k.data = kdata;
    
    p = blkbuf + BTR_BLK_TAILER_SIZE;
    key_cnt = dec_fix16(p, NULL);

    p = pb = blkbuf + BTR_INDEX_BLK_MSIZE - BTR_BLK_TAILER_SIZE;
    pb = move_to_key_pos(BTR_INDEX_BLK, pb);

    ASSERT(key_cnt > 0);

    for (i = 0; i < key_cnt; i++) {
        type = *p++;

        toff = child_off;
        child_off = 0;

        child_off = dec_fix32(p, &p);

        share_ks = *p++;
        delt_ks = *p++;

        k.len = share_ks + delt_ks;

        if (share_ks > 0) memcpy(k.data, pb, share_ks);
        memcpy(k.data + share_ks, p, delt_ks);

        p += delt_ks;

        r = kcmp(&k, target);

        if (r == 0) return child_off;
        if (r > 0) return toff;
    }

    return child_off;
}

static int find_in_leaf(T *thiz, off_t ioff, mkey_t *target, fkv_t *fkv, KCMP cmp)
{
    int r = -1, key_cnt, i;
    char *blkbuf, *pb, *p, *pt, kdata[G_KSIZE_LIMIT];
    mkey_t k;
    KCMP kcmp = cmp ? cmp : key_cmp;

    blkbuf = thiz->hdr->map_buf + ioff;
    k.data = kdata;
    fkv->blktype = BTR_LEAF_BLK;
    
    p = blkbuf + BTR_BLK_TAILER_SIZE;
    key_cnt = dec_fix16(p, NULL);

    p = pb = blkbuf + BTR_LEAF_BLK_MSIZE - BTR_BLK_TAILER_SIZE;
    pb = move_to_key_pos(BTR_LEAF_BLK, pb);

    ASSERT(key_cnt > 0);

    for (i = 0; i < key_cnt; i++) {
        pt = move_to_key_pos(fkv->blktype, p);
        p = deseri_kmeta(fkv, p);

        if (fkv->kshare.len > 0) memcpy(k.data, pb, fkv->kshare.len);
        memcpy(k.data + fkv->kshare.len, pt, fkv->kdelt.len);

        k.len = fkv->kshare.len + fkv->kdelt.len;

        r = kcmp(&k, target);
        if (r >= 0) return r;
    }

    return -1;
}

static int find_i(T *thiz, mkey_t *key, mval_t *v, mkv_t *tkv)
{
    int r, i;
    hdr_block_t *hdr;
    off_t off;
    fkv_t fkv;

    fkv.kv = tkv;

    RWLOCK_READ(LOCK);

    r = krange_cmp(thiz, key);
    if (r != 0) {
        r = RC_NOT_FOUND;
        goto _out;
    }

    hdr = thiz->hdr;
    off = (hdr->node_cnt - 1) * BTR_INDEX_BLK_SIZE; /* root off */

    for (i = 0; i < hdr->tree_heigh; i++) {
        off = find_in_index(thiz, off, key, NULL);
        if (off <= 0) {
            r = RC_NOT_FOUND;
            goto _out;
        }

        off = off - (off_t)(hdr->leaf_off);
    }

    r = find_in_leaf(thiz, off, key, &fkv, NULL);
    if (r != 0) {
        r = RC_NOT_FOUND;
        goto _out;
    }

    if (v != NULL) {
        v->len = tkv->v.len;
        v->data = MY_Malloc(v->len);

        r = read_val(thiz->rfd, fkv.blkoff, fkv.voff, v);
        if (r != 0) {
            r = RC_ERR;
            MY_Free(v->data);
            goto _out;
        }

        tkv->v.data = v->data;
        check_fval(thiz->rfd, tkv);
    }

    r = RC_FOUND;
    DEBUG("hit %.*s in %s", (int)key->len, key->data, thiz->file);

_out:
    RWUNLOCK(LOCK);

    return r;
}

static int find(T *thiz, mkey_t *key, mval_t *v)
{
    mkv_t tkv;
    return find_i(thiz, key, v, &tkv);
}

static int exist(T *thiz, mkey_t *key, uint64_t ver)
{
    int r;
    mkv_t tkv;

    r = find_i(thiz, key, NULL, &tkv);
    if (r == RC_NOT_FOUND) return 0;
    if (ver == 0 || tkv.seq <= ver) return RC_FOUND;

    return RC_NOT_FOUND;
}

static int split(T *thiz, T *part1, T *part2)
{
    ssize_t r = 0;
    size_t bytes = 0, i = 0, kleft = 0;
    fkv_t *fkv;
    btriter_t *trit;
    btsaver_t *sv1, *sv2;

    r = prepare_file(part1);
    if (r != 0) return -1;

    r = prepare_file(part2);
    if (r != 0) return -1;

    sv1 = btsaver_create(NULL);
    sv1->init(sv1, part1);
    sv1->start(sv1);

    sv2 = btsaver_create(NULL);
    sv2->init(sv2, part2);
    sv2->start(sv2);

    trit = btriter_create(NULL);
    trit->container = thiz;
    trit->init(trit);
   
    while (trit->has_next(trit)) {
        i++;

        r = trit->get_next(trit, &fkv);
        if (r != 0) break;

        if (merge_filter(fkv)) {
            continue;
        }

        extract_fkey(fkv);
        extract_fval(thiz->rfd, fkv);

        bytes += fkv->kv->k.len + fkv->kv->v.len;

        if (bytes > thiz->conf->ftb_size && kleft == 0) {
            kleft = thiz->hdr->key_cnt - i;
        }

        if (kleft > thiz->conf->ftb_min_kcnt) {
            r = sv2->save_fkv(sv2, fkv);
        } else {
            r = sv1->save_fkv(sv1, fkv);
        }

        if (r != 0) break;
        trit->next(trit);
    }

    trit->destroy(trit);
    if (r != 0) goto _out;

    r = sv1->flush(sv1);
    if (r != 0) goto _out;

    r = sv2->flush(sv2);
    if (r != 0) goto _out;

    r = update_hdr(part1, sv1->hdr);
    if (r != 0) goto _out;

    r = update_hdr(part2, sv2->hdr);
    if (r != 0) goto _out;

_out:
    if (r != 0) r = -1;

    sv1->finish(sv1);
    sv1->destroy(sv1);

    sv2->finish(sv2);
    sv2->destroy(sv2);

    return r;
}

static int shrink(T *thiz, T *newer)
{
    ssize_t r = 0;
    fkv_t *fkv;
    btriter_t *trit;
    btsaver_t *sv1;

    r = prepare_file(newer);
    if (r != 0) return -1;

    sv1 = btsaver_create(NULL);
    sv1->init(sv1, newer);
    sv1->start(sv1);

    trit = btriter_create(NULL);
    trit->container = thiz;
    trit->init(trit);
   
    while (trit->has_next(trit)) {
        r = trit->get_next(trit, &fkv);
        if (r != 0) break;

        if (merge_filter(fkv)) {
            continue;
        }

        extract_fkey(fkv);
        extract_fval(thiz->rfd, fkv);

        r = sv1->save_fkv(sv1, fkv);
        if (r != 0) break;

        trit->next(trit);
    }

    trit->destroy(trit);
    if (r != 0) goto _out;

    r = sv1->flush(sv1);
    if (r != 0) goto _out;

    r = update_hdr(newer, sv1->hdr);
    if (r != 0) goto _out;

_out:
    if (r != 0) r = -1;

    sv1->finish(sv1);
    sv1->destroy(sv1);

    return r;
}
/****************************************
** basic function 
*****************************************/
static void destroy(btree_t *thiz)
{
    if (thiz->hdr != NULL && thiz->hdr->map_buf != NULL) {
        munmap(thiz->hdr->map_buf, thiz->hdr->map_size);
    }

    close(thiz->wfd);
    close(thiz->rfd);

    del_obj(thiz);
}

static int _init(T *thiz)
{
    RWLOCK_INIT(LOCK);

    thiz->hdr = PCALLOC(SUPER->mpool, sizeof(hdr_block_t));
#if 1
    thiz->hdr->fend_off = BTR_HEADER_BLK_SIZE;
#else
    thiz->hdr->fend_off = BTR_HEADER_BLK_SIZE;
    thiz->hdr->version = DB_FILE_VERSION;
    thiz->hdr->blktype = BTR_HEADER_BLK;
    strncpy(thiz->hdr->magic, DB_MAGIC_NUM, sizeof(thiz->hdr->magic));
#endif

    thiz->rfd = -1;
    thiz->wfd = -1;

    ADD_METHOD(destroy);
    ADD_METHOD(store);
    ADD_METHOD(restore);
    ADD_METHOD(merge_start);
    ADD_METHOD(merge);
    ADD_METHOD(merge_flush);
    ADD_METHOD(merge_hdr);
    ADD_METHOD(merge_fin);
    ADD_METHOD(find);
    ADD_METHOD(exist);
    ADD_METHOD(find_in_index);
    ADD_METHOD(range_cmp);
    ADD_METHOD(krange_cmp);
    ADD_METHOD(pkrange_cmp);
    ADD_METHOD(split);
    ADD_METHOD(shrink);
    ADD_METHOD(invalid);
    ADD_METHOD(get_iter);

    return 0;
}

T *btree_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    _init(thiz);

    return thiz;           
}
