#include "hdb_inc.h"
#include "conf.h"

#define T conf_t

static int mkeyflt(T *thiz, mkv_t *kv)
{
    if (kv->type & KV_OP_DEL) return 1;
    if (thiz->kflt && thiz->kflt(kv->k.data, kv->k.len)) {
        INFO("kflitered k=%.*s", kv->k.len, kv->k.data);
        return 1;
    }
    
    return 0;
}

static int mvalflt(T *thiz, mkv_t *kv)
{
    if (thiz->vflt && thiz->vflt(kv->v.data, kv->v.len, NULL)) {
        INFO("vflitered k=%.*s", kv->k.len, kv->k.data);
        return 1;
    }

    return 0;
}

static int fkeyflt(T *thiz, fkv_t *fkv)
{
    mkv_t *kv = fkv->kv;
    int ks;

    if (kv->type & KV_OP_DEL) return 1;

    char k[G_KSIZE_LIMIT];
    if (fkv->kshare.data != NULL && fkv->kshare.len > 0) {
        memcpy(k, fkv->kshare.data, fkv->kshare.len);
    }

    memcpy(k + fkv->kshare.len, fkv->kdelt.data, fkv->kdelt.len);
    ks = fkv->kshare.len + fkv->kdelt.len;
    k[ks] = '\0';

    if (thiz->kflt && thiz->kflt(k, ks)) {
        INFO("kflitered k=%.*s", ks, k);
        return 1;
    }
    
    return 0;
}

static int fvalflt(T *thiz, fkv_t *fkv)
{
    mkv_t *kv = fkv->kv;

    if (thiz->vflt && thiz->vflt(kv->v.data, kv->v.len, NULL)) {
        INFO("vflitered kshare=%.*s, kdelt=%.*s", 
                fkv->kshare.len, fkv->kshare.data,
                fkv->kdelt.len, fkv->kdelt.data);
        return 1;
    }

    return 0;
}

static int init(T *thiz)
{
#if 1
    thiz->batch_size = (32 << 10);
    thiz->bin_size = (256 << 20);
    thiz->mtb_size = (32 << 20);
    thiz->ftb_size = (128 << 20);
    thiz->db_level = DB_MAX_LEVEL;
    thiz->imq_limit = 10;

    thiz->ftb_min_kcnt = 10240;
    /* shrink if compact amount of file bigger than this */
    thiz->cpct_cnt = 1; 
#elif 1
    thiz->batch_size = (3 << 10);
    thiz->bin_size = (8 << 20);
    thiz->mtb_size = (2 << 20);
    thiz->ftb_size = (4 << 20);
    thiz->db_level = DB_MAX_LEVEL;
    thiz->imq_limit = 2;

    thiz->ftb_min_kcnt = 100;
    thiz->cpct_cnt = 1;
#elif 0
    thiz->batch_size = (10);
    thiz->bin_size = (1024);
    thiz->mtb_size = (100);
    thiz->ftb_size = (2 << 20);
    thiz->db_level = DB_MAX_LEVEL;
    thiz->imq_limit = 1;

    thiz->ftb_min_kcnt = 100;
    thiz->cpct_cnt = 1;
#endif

    return 0;
}

static int parse(T *thiz, const char *conf_str)
{
    /* TODO!! */
    UNUSED(thiz);
    UNUSED(conf_str);
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
    ADD_METHOD(mkeyflt);
    ADD_METHOD(mvalflt);
    ADD_METHOD(fkeyflt);
    ADD_METHOD(fvalflt);

    return 0;
}

T *conf_create(pool_t *mpool)
{
     T *thiz = new_obj(mpool, sizeof(*thiz));
     if (thiz == NULL) return NULL;

     _init(thiz);

     return thiz;           
}
