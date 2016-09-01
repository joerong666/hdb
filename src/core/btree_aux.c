#include "inc.h"
#include "coding.h"
#include "btree_aux.h"

#define LEAF_ADD_KEY_POS() (19)
#define LEAF_ADD_KLEN_POS() (LEAF_ADD_KEY_POS() - 2)

#define LEAF_DEL_KEY_POS() (11)
#define LEAF_DEL_KLEN_POS() (LEAF_DEL_KEY_POS() - 2)

#define INDEX_KEY_POS() (7) 
#define INDEX_KLEN_POS() (INDEX_KEY_POS() - 2)

size_t io_read(int fd, void *buf, size_t count)
{
    /* FIXME!! check errno=EINTR */
    return read(fd, buf, count);
}

ssize_t io_write(int fd, const void *buf, size_t count)
{
    /* FIXME!! check errno=EINTR */
    return write(fd, buf, count);
}

size_t io_pread(int fd, void *buf, size_t count, off_t offset)
{
    /* FIXME!! check errno=EINTR */
    return pread(fd, buf, count, offset);
}

ssize_t io_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
    /* FIXME!! check errno=EINTR */
    return pwrite(fd, buf, count, offset);
}

static int seek_prefix(uint8_t *s1, size_t len1, uint8_t *s2, size_t len2)
{
    /* TODO!! */
    return 0;
}

static int calc_share_size(mkey_t *share, mkey_t *delt)
{
    if (share == delt || share->data == delt->data) return 0;

    int len = seek_prefix(delt->data, delt->len, share->data, share->len);
    if (len < 0) len = 0;

    return len;
}

int seri_keyitem_size(fkv_t *fkv, mkey_t *share, mkey_t *delt)
{
    int r = 0;
    int slen = calc_share_size(share, delt);

    if (slen > 0) {
        fkv->kv->type |= KV_KTP_DELT;
    } else {
        fkv->kv->type &= ~KV_KTP_DELT;
    }
 
    fkv->kshare.len = slen;
    fkv->kdelt.len = delt->len - slen;
    fkv->kdelt.data = delt->data + slen;

    if (fkv->blktype == BTR_INDEX_BLK) {
        r = INDEX_KEY_POS() + fkv->kdelt.len;
    } else {
        if (fkv->kv->type & KV_OP_DEL) {
            r = LEAF_DEL_KEY_POS() + fkv->kdelt.len;
        } else {
            r = LEAF_ADD_KEY_POS() + fkv->kdelt.len + _encode_varint(fkv->kv->v.len, NULL);
        }
    }

    return r;
}

char *move_to_klen_pos(int blktype, char *src)
{
    int type;
    char *p = src;

    if (blktype == BTR_INDEX_BLK) {
        p += INDEX_KLEN_POS();
    } else {
        type = *p;
        if (type & KV_OP_DEL) {
            p += LEAF_DEL_KLEN_POS();
        } else {
            p += LEAF_ADD_KLEN_POS();
        }
    }

    return p;
}

char *move_to_key_pos(int blktype, char *src)
{
    int type;
    char *p = src;

    if (blktype == BTR_INDEX_BLK) {
        p += INDEX_KEY_POS();
    } else {
        type = *p;
        if (type & KV_OP_DEL) {
            p += LEAF_DEL_KEY_POS();
        } else {
            p += LEAF_ADD_KEY_POS();
        }
    }

    return p;
}

char *deseri_kmeta(fkv_t *fkv, char *src)
{
    uint16_t type;
    char *p = src;

    type = *p++;
    fkv->kv->type = type;

    if (fkv->blktype == BTR_INDEX_BLK) {
        fkv->blkoff = dec_fix32(p, &p);
    } else {
        fkv->kv->seq = dec_fix64(p, &p);

        if (!(type & KV_OP_DEL)) {
            fkv->kv->vcrc   = dec_fix16(p, &p);
            fkv->blkoff     = dec_fix32(p, &p);
            fkv->voff       = dec_fix16(p, &p);
        }
    }

    if (type & KV_KTP_DELT) {
        fkv->kshare.len = *p++;
        fkv->kdelt.len = *p++;
    } else {
        ASSERT(*p == 0);

        fkv->kshare.len = *p++;
        fkv->kdelt.len = *p++;
    }

    fkv->kv->k.len = fkv->kshare.len + fkv->kdelt.len;
    p += fkv->kdelt.len;

    if (fkv->blktype != BTR_INDEX_BLK && !(type & KV_OP_DEL)) {
        fkv->kv->v.len  = dec_varint(p, &p);
    }

    return p;
}

char *deseri_kname(fkv_t *fkv, uint8_t *share, uint8_t *delt)
{
    int type, rslen, rdlen, len;
    uint8_t *tshare = share;
    uint8_t *tdelt = delt;
    uint8_t *k;

    tshare = move_to_key_pos(fkv->blktype, share);
    tdelt = move_to_klen_pos(fkv->blktype, delt);

    type = *delt;
    rslen = *tdelt++;
    rdlen = *tdelt++;

    if (!(type & KV_KTP_DELT)) {
        ASSERT(rslen == 0);
    }

    len = rslen + rdlen;
    k = MY_Malloc(len);
    fkv->kv->type |= KV_KTP_FROM_FILE;

    if (rslen > 0) {
        memcpy(k, tshare, rslen); 
    }

    fkv->kshare.data = k;
    fkv->kshare.len = rslen;

    memcpy((char *)(k + rslen), tdelt, rdlen); 
    fkv->kdelt.data = k + rslen;
    fkv->kdelt.len = rdlen;

    fkv->kv->k.data = k;
    fkv->kv->k.len = len;

    tdelt += rdlen;

    if (fkv->blktype != BTR_INDEX_BLK && !(type & KV_OP_DEL)) {
        dec_varint(tdelt, &tdelt);
    }

    return tdelt;
}

char *deseri_key(fkv_t *fkv, char *share, char *delt)
{
    char *r;

    deseri_kmeta(fkv, delt);
    r = deseri_kname(fkv, share, delt);

    return r;
}

char *seri_kmeta(char *dst, size_t len, fkv_t *fkv)
{
    int type = 0;
    char *p = dst;


    if (fkv->blktype == BTR_INDEX_BLK) {
        if (fkv->kv->type & KV_KTP_DELT) type |= KV_KTP_DELT;
        if (fkv->kv->type & KV_OP_DEL) type |= KV_OP_DEL;

        *p++ = type & 0xFF;
        p = enc_fix32(p, fkv->blkoff);
    } else {
        if (fkv->kv->type & KV_OP_DEL) type |= KV_OP_DEL;
        if (fkv->kv->type & KV_KTP_DELT) type |= KV_KTP_DELT;

        *p++ = type & 0xFF;

        p = enc_fix64(p, fkv->kv->seq);

        if (!(fkv->kv->type & KV_OP_DEL)) {
            p = enc_fix16(p, fkv->kv->vcrc);
            p = enc_fix32(p, fkv->blkoff);
            p = enc_fix16(p, fkv->voff);
        }
    }

    if (fkv->kv->type & KV_KTP_DELT) {
        *p++ = fkv->kshare.len & 0xFF;
        *p++ = fkv->kdelt.len & 0xFF;
    } else {
        fkv->kdelt.len = fkv->kv->k.len;
        *p++ = 0 & 0xFF;
        *p++ = fkv->kv->k.len & 0xFF;
    }

    if (fkv->blktype != BTR_INDEX_BLK && !(fkv->kv->type & KV_OP_DEL)) {
        p = enc_varint(p, fkv->kv->v.len);
    }

    ASSERT(p - dst <= len);
    return p;
}

int seri_hdr(char *blkbuf, hdr_block_t *hdr) 
{
    char *hp = blkbuf;
    char *tp = blkbuf + BTR_HEADER_BLK_SIZE;

    memset(hp, 0x00, BTR_HEADER_BLK_SIZE);

    /* crc32 calculate later */
    hp += 4;

    *hp++ = BTR_HEADER_BLK & 0xFF;

    memcpy(hp, hdr->magic, 8);
    hp += 8;

    hp = enc_fix16(hp, hdr->version);
    hp = enc_fix32(hp, hdr->key_cnt);

    *hp++ = hdr->tree_heigh & 0xFF;
    *hp++ = hdr->cpct_cnt & 0xFF;
    *hp++ = hdr->beg.len & 0xFF;

    memcpy(hp, hdr->beg.data, hdr->beg.len);
    hp += hdr->beg.len;

    *hp++ = hdr->end.len & 0xFF;

    memcpy(hp, hdr->end.data, hdr->end.len);
    hp += hdr->end.len;

    hp = enc_varint(hp, hdr->fend_off);
    hp = enc_varint(hp, hdr->leaf_off);
    hp = enc_varint(hp, hdr->node_cnt);
    hp = enc_varint(hp, hdr->leaf_cnt);

#if 0  /* TODO!! serialize other fields */
    hp = enc_varint(hp, hdr->filter_off);

    *hp = hdr->filter_cnt & 0xFF;
    hp++;
#endif
    
    tp -= BTR_BLK_TAILER_SIZE;
    *tp++ = hdr->blktype & 0xFF;

    wrap_block_crc(blkbuf, BTR_HEADER_BLK_SIZE);

    return 0;
}

int deseri_hdr(hdr_block_t *hdr, char *blkbuf)
{
    char *hp = blkbuf;

    hdr->blktype = *(blkbuf + 4);

    if (hdr->blktype != BTR_HEADER_BLK) {
        ERROR("not a real BTR_HEADER_BLK, blktype=%d", hdr->blktype);
        return -1;
    }

    hp += BTR_BLK_TAILER_SIZE;

    memcpy(hdr->magic, hp, 8);
    hp += 8;

    hdr->version = dec_fix16(hp, &hp);
    hdr->key_cnt = dec_fix32(hp, &hp);

    hdr->tree_heigh = *hp++;
    hdr->cpct_cnt = *hp++;

    hdr->beg.len = *hp++;

    hdr->beg.data = (uint8_t *)hp;
    hp += hdr->beg.len;

    hdr->end.len = *hp++;

    hdr->end.data = (uint8_t *)hp;
    hp += hdr->end.len;

    hdr->fend_off = dec_varint(hp, &hp);
    hdr->leaf_off = dec_varint(hp, &hp);
    hdr->node_cnt = dec_varint(hp, &hp);
    hdr->leaf_cnt = dec_varint(hp, &hp);

#if 0  /* TODO!! deserialize other fields */
    hdr->filter_off = dec_varint(hp, &hp);

    hdr->filter_cnt = *hp++;
#endif
 
    return 0;
}

int read_val(int fd, uint32_t blkoff, uint32_t voff, mval_t *v)
{
    ssize_t r = 0;
    uint8_t *dp = v->data;
    ssize_t left = v->len, sz = 0;
    uint32_t blksize = BTR_VAL_BLK_SIZE;

    do {
        sz = blksize - voff - BTR_BLK_TAILER_SIZE;

        if (left <= sz) {
            r = pread(fd, dp, left, blkoff + voff);
            if (r != left) goto _out;

            left = 0;
        } else {
            r = pread(fd, dp, sz, blkoff + voff);
            if (r != sz) goto _out;

            left -= sz;
            dp += sz;

            blkoff += blksize;
            voff = BTR_VAL_BLK_MSIZE - BTR_BLK_TAILER_SIZE;
        }
    } while (left > 0);

    /* ASSERT(left == 0) */
    if (left != 0) r = -1;

_out:
    if (r == -1) {
        ERROR("pread errno=%d", errno);
        return -1;
    }

    return 0;
}

int wrap_block_crc(char *blkbuf, size_t blksize)
{
    uint32_t crc;

    /* serialize head/tail crc32 */
    crc = calc_crc32(blkbuf + 4, blksize - 8);
    enc_fix32(blkbuf, crc);
    enc_fix32(blkbuf + blksize - 4, crc);

    return 0;
}

int blk_crc32_check(char *blkbuf, size_t blksize)
{
    uint32_t crc = 0, crc2;

    if (memcmp(blkbuf, blkbuf + blksize - 4, 4) != 0) {
        ERROR("CRC not equal");
        return -1;
    }

    crc = dec_fix32(blkbuf, NULL);
    crc2 = calc_crc32(blkbuf + 4, blksize - 8);

    if (crc != crc2) {
        ERROR("CRC check fail!!");
        return -1;
    }

    return 0;
}

size_t bin_kv_size(mkv_t *kv)
{
    size_t total = 18 + kv->k.len;

    if (!(kv->type & KV_OP_DEL)) {
        total += _encode_varint(kv->v.len, NULL) + kv->v.len;
    }

    return total;
}

size_t seri_bin_kv(char *buf, size_t total, mkv_t *kv)
{
    char *p = buf;
    uint32_t crc;
    int type = 0;

    p += 4;

    /* serialize bin_rec_size */
    p = enc_fix32(p, total - 8);

    p = enc_fix64(p, kv->seq);

    if (kv->type & KV_OP_DEL) type |= KV_OP_DEL;
    if (kv->type & KV_KTP_DELT) type |= KV_KTP_DELT;

    *p++ = type & 0xFF;
    *p++ = kv->k.len & 0xFF;

    memcpy(p, kv->k.data, kv->k.len);
    p += kv->k.len;

    if (!(kv->type & KV_OP_DEL)) {
        p = enc_varint(p, kv->v.len);

        memcpy(p, kv->v.data, kv->v.len);
        p += kv->v.len;
    }

    crc = calc_crc32(buf + 4, total - 4);

    p = buf;
    p = enc_fix32(p, crc);

    return total;
}

#define FLG_BIN_EOF 0
ssize_t deseri_bin_kv(char *buf, off_t xoff, size_t len, mkv_t *kv)
{
    uint32_t crc, crc2, sz;
    char *ps, *p;

    p = ps = buf + xoff;

    if (len <= 8) {
        ERROR("binlog err, len=%zd", len);
        return FLG_BIN_EOF;
    }

    crc = dec_fix32(p, &p);
    sz  = dec_fix32(p, &p);

    if (len < sz + 4) {
        ERROR("binlog err, len=%zd, sz=%d", len, sz);
        return FLG_BIN_EOF;
    }

    crc2 = calc_crc32(ps + 4, sz + 4);
    if (crc != crc2) {
        ERROR("binlog crc not equal");
        return -1;
    }

    kv->off = xoff;
    kv->seq = dec_fix64(p, &p);

    kv->type = *p++;
    kv->k.len = *p++;

    kv->k.data = MY_Malloc(kv->k.len);
    memcpy(kv->k.data, p, kv->k.len);
    p += kv->k.len;

    if (!(kv->type & KV_OP_DEL)) {
        kv->v.len = dec_varint(p, &p);

        kv->v.data = NULL;
        if (kv->v.len > 0) {
            if (kv->v.len < G_BIN_VAL_SIZE) {
                kv->v.data = MY_Malloc(kv->v.len);
                memcpy(kv->v.data, p, kv->v.len);
            } else {
                kv->type |= KV_VTP_BINOFF;
            }
        }

        p += kv->v.len;
    }
    
    return (p - ps);
}

ssize_t read_bin_val(int fd, mkv_t *kv)
{
    ssize_t r = io_pread(fd, kv->v.data, kv->v.len, kv->off + bin_kv_size(kv) - kv->v.len);
    if (r == -1) {
        FATAL("read bin errno=%d, please restart for recover", errno);
    }

    return r;
}

int key_cmp(mkey_t *k1, mkey_t *k2)
{
    int r;
    size_t len;

    len = k1->len < k2->len ? k1->len : k2->len;

    r = strncmp(k1->data, k2->data, len);
    if (r != 0) return r;
    if (k1->len == k2->len) return 0;
    if (k1->len < k2->len) return -1;
    if (k1->len > k2->len) return +1;
    
    return 0;
}

int mkv_cmp(mkv_t *kv1, mkv_t *kv2)
{
    return key_cmp(&kv1->k, &kv2->k);
}

int fkv_cmp(fkv_t *kv1, fkv_t *kv2)
{
    char k1[G_KSIZE_LIMIT], k2[G_KSIZE_LIMIT];
    mkey_t mk1, mk2;

    mk1.len = kv1->kshare.len + kv1->kdelt.len;
    mk1.data = k1;

    mk2.len = kv2->kshare.len + kv2->kdelt.len;
    mk2.data = k2;

    if (kv1->kshare.len > 0) {
        memcpy(k1, kv1->kshare.data, kv1->kshare.len);
    }
    memcpy(k1 + kv1->kshare.len, kv1->kdelt.data, kv1->kdelt.len);

    if (kv2->kshare.len > 0) {
        memcpy(k2, kv2->kshare.data, kv2->kshare.len);
    }
    memcpy(k2 + kv2->kshare.len, kv2->kdelt.data, kv2->kdelt.len);

    return key_cmp(&mk1, &mk2);
}

int extract_fkey(fkv_t *fkv)
{
    char *k = MY_Malloc(fkv->kv->k.len);

    if (fkv->kshare.len > 0) {
        memcpy(k, fkv->kshare.data, fkv->kshare.len); 
    }

    memcpy((char *)(k + fkv->kshare.len), fkv->kdelt.data, fkv->kdelt.len); 
    fkv->kv->k.data = k;

    fkv->kv->type |= KV_KTP_FROM_FILE;

    return 0;
}

static int get_fname(int fd, char *path, size_t sz)
{
    int r;
    char fp[256];
    snprintf(fp, sizeof(fp), "/proc/%d/fd/%d", getpid(), fd);
    r = readlink(fp, path, sz);
    if (r == -1) {
        ERROR("readlink %s, errno=%d", fp, errno);
    } else {
        path[r] = '\0';
    }

    return r;
}

void check_fval(int fd, mkv_t *kv)
{
    uint16_t crc = calc_crc16(kv->v.data, kv->v.len);
    if (kv->vcrc != crc) {
        char fp[256];
        get_fname(fd, fp, sizeof(fp));
        FATAL("%s corrupt!!", fp);
        ASSERT(kv->vcrc == crc);
    }
}

int extract_fval(int fd, fkv_t *fkv)
{
    int r;
    if (fkv->kv->type & KV_OP_DEL) {
        ASSERT(fkv->kv->v.len == 0);
        return 0;
    }

    fkv->kv->v.data = MY_Malloc(fkv->kv->v.len);
    r = read_val(fd, fkv->blkoff, fkv->voff, &fkv->kv->v);
    if (r != 0) {
        MY_Free(fkv->kv->v.data);
        return -1;
    }

    fkv->kv->type |= KV_VTP_FROM_FILE;

    check_fval(fd, fkv->kv);

    return 0;
}

int merge_filter(fkv_t *fkv)
{
    if (fkv->kv->type & KV_OP_DEL) return 1;

    return 0;
}

int gen_tmp_file(conf_t *cnf, char *fname)
{
    RWLOCK_WRITE(&cnf->lock);

    while (1) {
        cnf->tmp_fnum++;
        TMP_FILE(fname, cnf, cnf->tmp_fnum);

        if (access(fname, F_OK) == 0) {
            INFO("%s existed, re-gen", fname);
            continue;
        }

        break;
    }
    
    RWUNLOCK(&cnf->lock);
    return 0;
}

int open_tmp_file(conf_t *cnf, char *fname)
{
    int fd = -1;

    gen_tmp_file(cnf, fname);
    
    fd = open(fname, O_CREAT | O_EXCL | O_RDWR | O_DIRECT, 0644);
    if (fd == -1) {
        ERROR("open %s, errno=%d", fname, errno);
    }

    return fd;
}

void backup(conf_t *cnf, char *file)
{
    char dname[G_MEM_MID], bname[G_MEM_MID], *b;

    AB_PATH_BAK(dname, cnf);
    
    strcpy(bname, file);
    b = basename(bname);

    sprintf(dname + strlen(dname), "/%s", b);

    INFO("backup %s to %s", file, dname);
    rename(file, dname);
}

void recycle(conf_t *cnf, char *file)
{
#if 1 
    remove(file);
#else  /* TODO!! just rename for IO performance */    
    char dname[G_MEM_MID], bname[G_MEM_MID], *b;
    struct stat st;

    stat(file, &st);
    if (st.st_size == 0) {
        INFO("%s is empty, remove it", file);
        remove(file);
    }

    AB_PATH_RECYCLE(dname, cnf);
    
    strcpy(bname, file);
    b = basename(bname);

    sprintf(dname + strlen(dname), "/%s", b);

    INFO("recycle %s to %s", file, dname);
    rename(file, dname);
#endif
}
