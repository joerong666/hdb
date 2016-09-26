#include "hdb_inc.h"
#include "dbimpl.h"
#include "db.h"

struct db_s {
    /* must be the first field */
    obj_t  super;

    db_attr_t attr;
    dbimpl_t *dbimpl;
};

struct iter_s {
    obj_t super;
    dbit_impl_t *itimpl;
};

static db_t *db_create()
{
    db_t *thiz = new_obj(NULL, sizeof(*thiz));
    thiz->dbimpl = dbimpl_create(NULL);

    return thiz;           
}

static void db_destroy(db_t *db)
{
    if (db == NULL) return;

    if (db->dbimpl != NULL) db->dbimpl->destroy(db->dbimpl);

    del_obj(db);
}

static iter_t *iter_create()
{
    iter_t *it = new_obj(NULL, sizeof(*it));

    return it;           
}

static void iter_destroy(iter_t *it)
{
    if (it == NULL) return;

    if (it->itimpl != NULL) it->itimpl->destroy(it->itimpl);

    del_obj(it);
}

int dbe_version(char *path)
{
    int r = DBVER_UNKNOWN, fd = -1, empty = 1;
    char p[256];
    DIR *d = NULL;
    struct dirent *ent;

    if (access(path, F_OK) != 0) {
        PROMPT("%s not exist", path);
        r = DBVER_EMPTY;
        goto _out;
    }

    snprintf(p, sizeof(p), "%s/CURRENT", path);
    if (access(p, F_OK) == 0) {
        PROMPT("CURRENT file exist, may be leveldb");
        goto _out;
    }

    snprintf(p, sizeof(p), "%s/MANIFEST", path);
    if (access(p, F_OK) == 0) {
        PROMPT("MANIFEST file exist, consider as hidb_v%d", DBVER_V1);
        r = DBVER_V1;
        goto _out;
    }

    snprintf(p, sizeof(p), "%s/MANIFEST-v2", path);
    if (access(p, F_OK) == 0) {
        PROMPT("MANIFEST-v2 file exist, consider as hidb_v%d", DBVER_V1);
        r = DBVER_V1;
        goto _out;
    }

    if ((d = opendir(path)) == NULL) {
        ERROR("opendir %s, errno=%d", path, errno);
        goto _out;
    }

    errno = 0;
    while((ent = readdir(d)) != NULL) {
        if (fnmatch("*.bin", ent->d_name, 0) == 0 || 
            fnmatch("*.sst", ent->d_name, 0) == 0) {
            PROMPT("*.bin or *.sst exist, consider as hidb_v%d", DBVER_V1);
            r = DBVER_V1;
            goto _out;
        }

        if (empty && strcmp(ent->d_name, ".") != 0 
                  && strcmp(ent->d_name, "..") != 0) empty = 0;
    }

    if (empty) {
        PROMPT("consider as empty db");
        r = DBVER_EMPTY;
        goto _out;
    }

    snprintf(p, sizeof(p), "%s/"DB_DIR_DATA, path);
    if (access(p, F_OK) != 0) goto _out;

    snprintf(p, sizeof(p), "%s/"DB_DIR_BIN, path);
    if (access(p, F_OK) != 0) goto _out;

    r = DB_MARK_ID;
    PROMPT("consider as hidb_v%d", DB_MARK_ID);

_out:
    if (d) closedir(d);
    if (fd >= 0) close(fd);

    return r;
}

db_t *HIDB2(db_open)(const char *path, const db_attr_t *attr)
{
    int r;
    db_t *db;

    db = db_create();
    if (db == NULL) {
        r = -1;
        goto _out;
    }

    r = db->dbimpl->open(db->dbimpl, path, NULL);
    if (r != 0) {
        r = -1;
        goto _out;
    }

    DBATTR_INIT(&db->attr);
    if(attr != NULL) memcpy(&db->attr, attr, sizeof(db_attr_t));

_out:
    if (r != 0) {
        db_destroy(db);
        return NULL;
    }

    return db;
}

int HIDB2(db_run)(db_t *db)
{
    return db->dbimpl->prepare(db->dbimpl);
}

int HIDB2(db_flush)(db_t *db)
{
    return db->dbimpl->flush(db->dbimpl);
}

int HIDB2(db_checkpoint)(db_t *db)
{
    return db->dbimpl->checkpoint(db->dbimpl);
}

void HIDB2(db_close)(db_t *db)
{
    db->dbimpl->close(db->dbimpl);

#if 1 /* for fast quit, skip destroy */
    db_destroy(db);
#endif
}

void HIDB2(db_print_state)(db_t *db)
{
    /* TODO!! */
    UNUSED(db);
}

int HIDB2(db_put)(db_t *db, char *key, uint16_t key_size, char *val, uint32_t val_size)
{
    mkv_t kv;

    memset(&kv, 0x00, sizeof(kv));

    kv.k.data = key;
    kv.k.len = key_size;
    
    kv.v.data = val;
    kv.v.len = val_size;

    DEBUG("put k=%.*s, vs=%d", key_size, key, val_size);
    return db->dbimpl->put(db->dbimpl, &kv);
}

int HIDB2(db_del)(db_t *db, char *key, uint16_t key_size)
{
    mkey_t k;

    k.len = key_size;
    k.data = MY_Malloc(key_size);
    memcpy(k.data, key, key_size);
    
    DEBUG("del k=%.*s", key_size, key);
    return db->dbimpl->del(db->dbimpl, &k);
}

int HIDB2(db_get)(db_t *db, char *key, uint16_t key_size, 
			char **val, uint32_t *val_size, void *(*alloc_func)(size_t))
{
    int r;

    mkey_t k;
    mval_t v;

    UNUSED(alloc_func);

    k.data = key;
    k.len = key_size;

    r = db->dbimpl->get(db->dbimpl, &k, &v);
    if (r != 0) return 1;

    *val = v.data;
    *val_size = v.len;

    return 0;
}

int HIDB2(db_mput)(db_t *db, kvec_t *vec, size_t cnt)
{
    size_t i;
    mkv_t kvs[cnt];
    
    memset(kvs, 0x00, sizeof(kvs));

    for (i = 0; i < cnt; i++) {
        DEBUG("mput k=%.*s, vs=%d", vec[i].ks, vec[i].k, vec[i].vs);
        kvs[i].k.data = vec[i].k;
        kvs[i].k.len = vec[i].ks;
        kvs[i].v.data = vec[i].v;
        kvs[i].v.len = vec[i].vs;
    }

    MY_Free(vec);

    return db->dbimpl->mput(db->dbimpl, kvs, cnt);
}

int HIDB2(db_mdel)(db_t *db, kvec_t *vec, size_t cnt)
{
    size_t i;
    mkey_t keys[cnt];
    
    for (i = 0; i < cnt; i++) {
        DEBUG("mdel k=%.*s", vec[i].ks, vec[i].k);
        keys[i].data = vec[i].k;
        keys[i].len = vec[i].ks;
    }

    MY_Free(vec);

    return db->dbimpl->mdel(db->dbimpl, keys, cnt);
}

static iter_t *get_iter(db_t *db, mkey_t *start, mkey_t *stop)
{
    iter_t *it;
    dbit_impl_t *itimpl;

    it = iter_create();
    itimpl = db->dbimpl->get_iter(db->dbimpl, start, stop);
    it->itimpl = itimpl;

    return it;
}

iter_t *HIDB2(db_pget)(db_t *db, char *kprefix, size_t ks)
{
    mkey_t k;
    k.data = kprefix;
    k.len = (uint8_t)ks;

    DEBUG("pget k=%.*s", ks, kprefix);
    return get_iter(db, &k, NULL);
}

int HIDB2(db_pdel)(db_t *db, char *kprefix, size_t ks)
{
    mkey_t pk;
    pk.data = kprefix;
    pk.len = ks;

    DEBUG("pdel k=%.*s", ks, kprefix);
    return db->dbimpl->pdel(db->dbimpl, &pk);
}

int HIDB2(db_set_rulefilter)(db_t *db, DB_KFILTER flt)
{
    db->attr.keyfilter = flt;
    db->dbimpl->conf->kflt = flt;
    return 0;
}

int HIDB2(db_set_valfilter)(db_t *db, DB_VFILTER flt)
{
    db->attr.valfilter = flt;
    db->dbimpl->conf->vflt = flt;
    return 0;
}

int HIDB2(db_get_stats)(db_t *db, db_stats_t *stats)
{
    /* TODO!! */
    UNUSED(db);
    UNUSED(stats);
    return 0;
}

iter_t *HIDB2(db_create_it)(db_t *db, int level)
{
    UNUSED(level);
    
    return get_iter(db, NULL, NULL);
}

void HIDB2(db_destroy_it)(iter_t *it)
{
    iter_destroy(it);
}

#define IT_FIN 1
int HIDB2(db_iter)(iter_t *it, char **k, int *ks, char **v, int *vs, void *(*alloc_func)(size_t))
{
    UNUSED(alloc_func);

    int r = 0;
    mkv_t kv;
    dbit_impl_t *itimpl = it->itimpl;

    r = itimpl->next(itimpl, &kv);
    if (r != 1) return IT_FIN;

    *k = kv.k.data;
    *ks = kv.k.len;

    *v = kv.v.data;
    *vs = kv.v.len;

    return 0;
}

int HIDB2(db_clear)(db_t *db)
{
    /* TODO!! */
    UNUSED(db);
    return 0;
}

int HIDB2(db_load_dir)(db_t *db, const char *dir)
{
    /* TODO!! */
    UNUSED(db);
    UNUSED(dir);
    return 0;
}

/* begin global transaction */
int HIDB2(db_gtx_begin)(db_t *db)
{
    /* TODO!! */
    UNUSED(db);
    return 0;
}

/* commit global transaction */
int HIDB2(db_gtx_comit)(db_t *db)
{
    /* TODO!! */
    UNUSED(db);
    return 0;
}

#define FUNC_DEPRECATED() \
    ERROR("%s has been deprecated!!", __func__);

int HIDB2(db_get_attr)(db_t *db, db_attr_t *attr)
{
    UNUSED(db);
    UNUSED(attr);
    FUNC_DEPRECATED();
    return 0;
}

int HIDB2(db_clean_file)(db_t *db, struct tm *before)
{
    UNUSED(db);
    UNUSED(before);
    FUNC_DEPRECATED();
    return 0;
}

/**
 * @cnt: limit to 1~100
 */
int HIDB2(db_clean_oldest)(db_t *db, int cnt)
{
    UNUSED(db);
    UNUSED(cnt);
    FUNC_DEPRECATED();
    return 0;
}

int HIDB2(db_freeze)(db_t *db)
{
    UNUSED(db);
    FUNC_DEPRECATED();
    return 0;
}

int HIDB2(db_unfreeze)(db_t *db)
{
    UNUSED(db);
    FUNC_DEPRECATED();
    return 0;
}

int HIDB2(db_get_level)(db_t *db)
{
    UNUSED(db);
    FUNC_DEPRECATED();
    return 2;
}

void HIDB2(db_set_debug_flag)(int debug_flag)
{
    UNUSED(debug_flag);
    FUNC_DEPRECATED();
}

void HIDB2(db_manual_purge)(db_t *db)
{
    UNUSED(db);
    FUNC_DEPRECATED();
}

int HIDB2(hidba_export)(const struct hidba_s *dba)
{
    UNUSED(dba);
    FUNC_DEPRECATED();
    return 0;
}

db_opt_t *HIDB2(db_get_opt)(db_t *db)
{
    UNUSED(db);
    FUNC_DEPRECATED();
    return NULL;
}

void HIDB2(db_set_opt)(db_t *db, const db_opt_t *opt)
{
    UNUSED(db);
    UNUSED(opt);
    FUNC_DEPRECATED();
}
