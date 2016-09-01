#include "hdb_inc.h"
#include "conf.h"

#define T conf_t

static int mkvflt(T *thiz, mkv_t *kv)
{
    if (kv->type & KV_OP_DEL) return 1;
    if (thiz->vflt && thiz->vflt(kv->v.data, kv->v.len, NULL)) return 1;

    char k[G_KSIZE_LIMIT];
    memcpy(k, kv->k.data, kv->k.len);
    k[kv->k.len] = '\0';

    if (thiz->kflt && thiz->kflt(k)) return 1;
    
    return 0;
}

static int fkvflt(T *thiz, fkv_t *fkv)
{
    mkv_t *kv = fkv->kv;

    if (kv->type & KV_OP_DEL) return 1;
    if (thiz->vflt && thiz->vflt(kv->v.data, kv->v.len, NULL)) return 1;

    char k[G_KSIZE_LIMIT];
    if (fkv->kshare.data != NULL && fkv->kshare.len > 0) {
        memcpy(k, fkv->kshare.data, fkv->kshare.len);
    }

    memcpy(k + fkv->kshare.len, fkv->kdelt.data, fkv->kdelt.len);
    k[fkv->kshare.len + fkv->kdelt.len] = '\0';

    if (thiz->kflt && thiz->kflt(k)) return 1;
    
    return 0;
}

static int init(T *thiz)
{
#if 1
    thiz->mtb_size = (32 << 20);
    thiz->ftb_size = (256 << 20);
    thiz->bin_size = (256 << 20);
    thiz->kv_fsize = (2 << 20);
    thiz->kv_bsize = G_BIN_VAL_SIZE;
    thiz->batch_size = (32 << 10);
    thiz->db_level = DB_MAX_LEVEL;
    thiz->imq_limit = 10;
#elif 1
    thiz->mtb_size = (2 << 20);
    thiz->ftb_size = (2 << 20);
    thiz->bin_size = (4 << 20);
    thiz->kv_fsize = (2 << 20);
    thiz->kv_bsize = G_BIN_VAL_SIZE;
    thiz->batch_size = (3 << 10);
    thiz->db_level = DB_MAX_LEVEL;
    thiz->imq_limit = 2;
#elif 0
    thiz->mtb_size = (100);
    thiz->ftb_size = (2 << 20);
    thiz->bin_size = (1024);
    thiz->kv_fsize = (2 << 20);
    thiz->kv_bsize = G_BIN_VAL_SIZE;
    thiz->batch_size = (10);
    thiz->db_level = DB_MAX_LEVEL;
    thiz->imq_limit = 1;
#endif

    return 0;
}

static int parse(T *thiz, const char *conf_str)
{
    /* TODO!! */
    return 0;
}

/****************************************
** basic function
*****************************************/
static void destroy(T *thiz)
{
    del_obj(thiz);
}

static int _init(T *thiz)
{
    RWLOCK_INIT(&thiz->lock);
    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(parse);
    ADD_METHOD(mkvflt);
    ADD_METHOD(fkvflt);

    return 0;
}

T *conf_create(pool_t *mpool)
{
     T *thiz = new_obj(mpool, sizeof(*thiz));
     if (thiz == NULL) return NULL;

     _init(thiz);

     return thiz;           
}
