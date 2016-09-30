#include "hdb_inc.h"
#include "mem_table.h"

#define T mtb_t

enum mt_e {
    L_FLUSH_FIN = 1,
};

typedef struct mtkv_s {
    mkv_t *kv;
    struct mtkv_s *next;
} mtkv_t;

typedef struct mtkvlist_s {
    int len;
    size_t bytes;

    mtkv_t *head;
    mtkv_t *tail;
    mtkv_t *last;
    pool_t *mp;
} mtkvlist_t;

struct mtb_pri {
    int flag;
    uint32_t mtb_size;
    uint32_t bin_size;

    pthread_mutex_t flush_mtx;
    pthread_cond_t  flush_cond;

    mtkvlist_t *mm_kvlist;
    mtkvlist_t *im_kvlist;
};


/****************************************
** function declaration
*****************************************/
static int full(T *thiz)
{
    int r = 0;

    if (SELF->mtb_size >= thiz->conf->mtb_size
        || SELF->bin_size >= thiz->conf->bin_size) r = 1;

    return r;
}

static mtkvlist_t *kvlist_create(pool_t *mp)
{
    mtkvlist_t *p;

    if (mp == NULL) {
        mp = POOL_CREATE(G_MPOOL_SIZE);
    }

    p = PCALLOC(mp, sizeof(*p));
    p->mp = mp;

    return p;
}

static int kvlist_add(mtkvlist_t *list, mkv_t *kv)
{
    int r = RC_OK;
    mtkv_t *mtkv;

    if (list->head == NULL) {
        list->head = PALLOC(list->mp, sizeof(*list->head));
        list->head->kv = kv;
        list->head->next = NULL;

        list->tail = list->last = list->head;
        list->len++;
        list->bytes += kv->k.len;

        goto _out;
    }

    mtkv = PALLOC(list->mp, sizeof(*mtkv));
    mtkv->kv = kv;
    mtkv->next = NULL;

    list->tail->next = mtkv;
    list->tail = mtkv;
    list->len++;
    list->bytes += kv->k.len;

_out:
    return r;
}

static void change_mmkv_status(T *thiz)
{
    mtkvlist_t *list;
    mtkv_t *it;

    list = SELF->mm_kvlist;
    it = list->head;

    while (it) {
        it->kv->type &= ~KV_IN_MM_CACHE;
        it->kv->type |= KV_IN_IM_CACHE;
        it = it->next;
    }
}

static int push_i(T *thiz, mkv_t *nkv, int safe)
{
    int r;
    mkv_t *okv, *kv;
    kver_t *kver;

    /* nkv is a stack variable, change to heap */
    kv = MY_Calloc(sizeof(*kv));
    memcpy(kv, nkv, sizeof(*kv));

    r = thiz->model->push(thiz->model, kv, (void **)&okv);
    if (r == -1) {
        MY_Free(kv);
        return r;
    }

    ASSERT( r == 0 || r == 1);

    if (safe) {
        RWLOCK_WRITE(&thiz->lock);
    }

    if (r == 1) { /* exist */
        /* record history version */
        kver = PCALLOC(SUPER->mpool, sizeof(*kver));
        kver->ver = okv->seq;
        kver->next = okv->kver_list;
        kv->kver_list = kver;
                    
        if (okv->type & (KV_IN_MM_CACHE | KV_IN_IM_CACHE)) {
            okv->type |= KV_DEPRECATED;
            if (!(okv->type & KV_OP_DEL)) {
                SELF->mtb_size -= okv->v.len;
            }
        } else {
            MY_Free(okv->k.data);

            if (!(okv->type & KV_OP_DEL)) {
                SELF->mtb_size -= okv->v.len;
                MY_Free(okv->v.data);
            }

            MY_Free(okv);
        }
    } else {
        SELF->mtb_size += kv->k.len;
    }

    SELF->bin_size += kv->k.len;
    if (!(kv->type & KV_OP_DEL)) {
        SELF->bin_size += kv->v.len;
        SELF->mtb_size += kv->v.len;
        SELF->mm_kvlist->bytes += kv->v.len;
    }

    kv->type |= KV_IN_MM_CACHE;
    kvlist_add(SELF->mm_kvlist, kv);

    if (safe) {
        RWUNLOCK(&thiz->lock);
    }

    return 0;
}

static int push(T *thiz, mkv_t *kv)
{
    return push_i(thiz, kv, 1);
}

static int push_unsafe(T *thiz, mkv_t *kv)
{
    return push_i(thiz, kv, 0);
}

static int write_ready(T *thiz)
{
    int r = 0;

    RWLOCK_READ(&thiz->lock);
    if (SELF->im_kvlist != NULL) {
        r = 1;
    } else if (SELF->mm_kvlist != NULL 
            && SELF->mm_kvlist->bytes > thiz->conf->batch_size) {
        r = 1;
    }
    RWUNLOCK(&thiz->lock);

    return r;
}

static void after_write(T *thiz)
{
    mtkv_t *it;

    RWLOCK_WRITE(&thiz->lock);

    it = SELF->im_kvlist->head;
    while (it) {
        if (it->kv->type & KV_DEPRECATED) {
            MY_Free(it->kv->k.data);

            if (!(it->kv->type & KV_OP_DEL)) {
                MY_Free(it->kv->v.data);
            }

            MY_Free(it->kv);
            it = it->next;
            continue;
        }

        it->kv->type &= ~KV_DEPRECATED;
        it->kv->type &= ~KV_IN_MM_CACHE;
        it->kv->type &= ~KV_IN_IM_CACHE;
        it = it->next;
    }

    POOL_DESTROY(SELF->im_kvlist->mp);
    SELF->im_kvlist = NULL;

    RWUNLOCK(&thiz->lock);
}

static int switch_kvlist(T *thiz)
{
    if (SELF->im_kvlist == NULL) {
        RWLOCK_WRITE(&thiz->lock);
        change_mmkv_status(thiz);

        SELF->im_kvlist = SELF->mm_kvlist;
        SELF->mm_kvlist = kvlist_create(NULL);
        RWUNLOCK(&thiz->lock);
    }

    return 0;
}

static int write_bin(T *thiz)
{
    ssize_t r = 0;
    size_t sz = 0, kvsz, batch;
    uint32_t off;
    char *buf = NULL, *bigbuf;
    mtkvlist_t *list;
    mtkv_t *it;

    r = switch_kvlist(thiz);
    if (r != 0) return -1;

    list = SELF->im_kvlist;
    it = list->head;
    batch = (32 << 10);
    buf = MY_Malloc(batch);
    
    off = lseek(thiz->fd, 0, SEEK_CUR);

    while (it) {
        if (it->kv->type & KV_DEPRECATED) goto _next;

        kvsz = bin_kv_size(it->kv);
        off += kvsz;

        if (kvsz > batch) {
            if (sz > 0) {
                r = io_write(thiz->fd, buf, sz);
                if (r == -1) goto _out;

                sz = 0;
            }

            bigbuf = MY_Malloc(kvsz);
            seri_bin_kv(bigbuf, kvsz, it->kv);
            r = io_write(thiz->fd, buf, kvsz);

            MY_Free(bigbuf);

            if (r == -1) goto _out;
            else goto _next;
        }

        if ((sz + kvsz) > batch) {
            r = io_write(thiz->fd, buf, sz);
            if (r == -1) goto _out;

            sz = 0;
        }

        seri_bin_kv(buf + sz, kvsz, it->kv);
        sz += kvsz;

_next:
        it = it->next;
    }

    if (sz > 0) {
        r = io_write(thiz->fd, buf, sz);
        if (r == -1) goto _out;

        sz = 0;
    }

    after_write(thiz);

_out:
    MY_Free(buf);
    if (r != -1) r = 0;
    
    return r;
}

static int flush(T *thiz)
{
    int r = 0;

    if (SELF->im_kvlist != NULL) {
        r = write_bin(thiz);
        if (r != 0) goto _out;
    }

    if (SELF->mm_kvlist != NULL && SELF->mm_kvlist->head != NULL) {
        r = write_bin(thiz);
        if (r != 0) goto _out;
    }

_out:
    return r;
}

static void flush_wait(T *thiz)
{
    pthread_mutex_lock(&SELF->flush_mtx);
    while(1) {
        if (SELF->flag & L_FLUSH_FIN) break;
        pthread_cond_wait(&SELF->flush_cond, &SELF->flush_mtx);
    }
    SELF->flag &= ~L_FLUSH_FIN;
    pthread_mutex_unlock(&SELF->flush_mtx);
}

static void flush_notify(T *thiz)
{
    pthread_mutex_lock(&SELF->flush_mtx);
    SELF->flag |= L_FLUSH_FIN;
    pthread_cond_signal(&SELF->flush_cond);
    pthread_mutex_unlock(&SELF->flush_mtx);
}

#define FLG_BIN_EOF 0
#define FLG_BIN_OK 1

static int bin_read(char *buf, int len, int *off, mkv_t *kv)
{
    int r, xoff, xlen;
    mkv_t tkv;

    memset(&tkv, 0x0, sizeof(mkv_t));

    xlen = len;
    xoff = *off;

    if (xoff >= len) {
        if (xoff > len) {
            ERROR("offset exceeds flen, off=%d, flen=%d", xoff, len);
        }

        return FLG_BIN_EOF;
    }

    while (1) {
        r = deseri_bin_kv(buf, xoff, xlen, &tkv);
        if (r == FLG_BIN_EOF) return r;
        else if (r == RC_ERR) {
            xoff++;
            xlen--;
            continue;
        }
        
        memcpy(kv, &tkv, sizeof(mkv_t));
        xoff += r;

        break;
    }

    *off = xoff;
    return FLG_BIN_OK;
}   

static int restore_bin_hdr(T *thiz, char *buf)
{
    /* TODO!! */
    UNUSED(thiz);
    UNUSED(buf);
    return 0;
}

static int restore(T *thiz)
{
    int r, off = 0;
    char *buf = NULL;
    mkv_t *kv, *okv;
    struct stat st;

    INFO("restoring %s", thiz->file);

    fstat(thiz->fd, &st);
    if (st.st_size <= BIN_HEADER_SIZE) {
        ERROR("%s too small, size=%zd", thiz->file, st.st_size);
        return -1;
    }

    buf = mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, thiz->fd, 0);
    if (buf == NULL) {
        ERROR("mmap %s, erro=%d", thiz->file, errno);
        r = -1;
        goto _out;
    }

    r = restore_bin_hdr(thiz, buf);
    if (r != 0) goto _out;

    off += BIN_HEADER_SIZE;

    while(1) {
        kv = MY_Calloc(sizeof(*kv));

        r = bin_read(buf, st.st_size, &off, kv);
        if (r == RC_ERR) {
            r = -1;
            goto _out;
        } else if (r == FLG_BIN_EOF) break;

        r = thiz->model->push_unsafe(thiz->model, kv, (void **)&okv);
        if (r == -1) goto _out;

        ASSERT( r == 0 || r == 1);

        if (r == 1) {
            MY_Free(okv->k.data);
            if (okv->type & KV_OP_DEL) {
                SELF->mtb_size -= okv->v.len;
            } else {
                SELF->mtb_size += kv->v.len;
                SELF->mtb_size -= okv->v.len;
                MY_Free(okv->v.data);
            }
            MY_Free(okv);
        } else {
            SELF->mtb_size += kv->k.len;
            if (!(kv->type & KV_OP_DEL)) {
                SELF->mtb_size += kv->v.len;
            }
        }
    }

_out:
    if (buf) munmap(buf, st.st_size);
    SELF->bin_size = lseek(thiz->fd, 0, SEEK_END);

    return r;
}

static int   find(T *thiz, const mkey_t *k, mval_t *v)
{
    mkv_t kv, *tkv;

    v->data = NULL;
    memcpy(&kv.k, k, sizeof(*k));

    RWLOCK_READ(&thiz->lock);

    tkv = thiz->model->find(thiz->model, &kv);
    if (tkv == NULL) goto _out;
    if (tkv->type & KV_OP_DEL) {
        tkv = NULL;
        goto _out;
    }
    
    v->len = tkv->v.len;
    v->data = MY_Malloc(v->len);
    
    memcpy(v->data, tkv->v.data, v->len);
    
_out:

    RWUNLOCK(&thiz->lock);

    if (v->data == NULL) return RC_NOT_FOUND;
    
    return RC_FOUND;
}

static int   exist(T *thiz, const mkey_t *k, uint64_t ver)
{
    int r = 0;
    mkv_t kv, *tkv;
    kver_t *kver;

    memcpy(&kv.k, k, sizeof(*k));

    tkv = thiz->model->find(thiz->model, &kv);
    if (tkv != NULL) {
        if (ver == 0 || tkv->seq <= ver) r = 1;
        else {
            kver = tkv->kver_list;
            while(kver) {
                if (kver->ver <= ver) {
                    r = 1;
                    break;
                }

                kver = kver->next;
            }
        }
    }
   
    return r;
}

static int empty(T *thiz)
{
    return thiz->model->empty(thiz->model);
}

static unsigned int JSHash(char* str, unsigned int len)  
{  
   unsigned int hash = 1315423911;  
   unsigned int i    = 0;  
  
   for(i=0; i<len; str++, i++) {   
      hash ^= ((hash<<5) + toupper(*str) + (hash>>2));  
   }   
   return hash;  
}  

static unsigned int htfunc(const void *data)  
{
    mkv_t *kv = (mkv_t *)data;
    return JSHash(kv->k.data, kv->k.len);
}

/****************************************
** basic function
*****************************************/
static void free_kv(void *d)
{
    mkv_t *kv = (mkv_t *)d;

    MY_Free(kv->k.data);

    if (!(kv->type & KV_OP_DEL)) {
        MY_Free(kv->v.data);
    }

    MY_Free(kv);
}

static int store_bin_hdr(T *thiz)
{
#if 0
    char *buf = MY_Malloc(BIN_HEADER_SIZE);
    char *p = buf;

    p += 4; /* crc32 */

    memcpy(p, DB_MAGIC_NUM, strlen(DB_MAGIC_NUM));
    p += strlen(DB_MAGIC_NUM);

    *p++ = DB_VERSION & 0xFF;
#endif
    
    if (lseek(thiz->fd, BIN_HEADER_SIZE, SEEK_SET) == -1) {
        ERROR("lseek %s, errno=%d", thiz->file, errno);
        return -1;
    }

    return 0;
}

static int init(T *thiz)
{
    htable_t *m;

    thiz->fd = open(thiz->file, O_CREAT | O_RDWR, 0644);
    if (thiz->fd == -1) {
        ERROR("open %s, errno = %d", thiz->file, errno);
        return -1;
    }

    store_bin_hdr(thiz);

    m = htable_create(SUPER->mpool);
    m->cmp = (HTCMP)mkv_cmp;
    m->hfunc = htfunc;
    m->dfree_func = free_kv;
    m->fd = thiz->fd;

    if (m->init(m) != 0) return -1;

    thiz->model = m;
    SELF->mm_kvlist = kvlist_create(NULL);

    return 0;
}

static void clean(T *thiz)
{
    recycle(thiz->conf, thiz->file);
}

static void destroy(T *thiz)
{
    thiz->model->destroy_data(thiz->model);

    if (thiz->fd >= 0) close(thiz->fd);

    if (SELF->mm_kvlist) POOL_DESTROY(SELF->mm_kvlist->mp);

    del_obj(thiz);
}

static int _init(T *thiz)
{
    RWLOCK_INIT(&thiz->lock);

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(clean);
    ADD_METHOD(push);
    ADD_METHOD(push_unsafe);
    ADD_METHOD(find);
    ADD_METHOD(exist);
    ADD_METHOD(full);
    ADD_METHOD(restore);
    ADD_METHOD(write_ready);
    ADD_METHOD(empty);
    ADD_METHOD(write_bin);
    ADD_METHOD(flush);
    ADD_METHOD(flush_wait);
    ADD_METHOD(flush_notify);

    return 0;
}

T *mtb_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    _init(thiz);

    return thiz;           
}


