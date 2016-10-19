#ifndef _HDB_CONF_H_
#define _HDB_CONF_H_

#include "version.h"

#define DB_MARK_ID     2
#define DB_MAX_LEVEL   2

#define DB_DIR_DATA    "data"
#define DB_DIR_TMP     "tmp"
#define DB_DIR_RECYCLE "recycle"
#define DB_DIR_REMOTE  "remote"
#define DB_DIR_BAK     "backup"
#define DB_DIR_STATUS  "status"
#define DB_DIR_BIN     "bl"

#define DB_DATA_EXT   ".hdb"
#define DB_BIN_EXT     ".bin"

#define AB_PATH(_s, _cnf, _name) \
    snprintf(_s, sizeof(_s), "%s/%s", _cnf->dbpath, _name)

#define AB_SUB_PATH(_s, _cnf, _parent, _child) \
    snprintf(_s, sizeof(_s), "%s/%s/%s", _cnf->dbpath, _parent, _child)

#define AB_PATH_DATA(_s, _cnf) AB_PATH(_s, _cnf, DB_DIR_DATA)
#define AB_PATH_BIN(_s, _cnf)  AB_PATH(_s, _cnf, DB_DIR_BIN)
#define AB_PATH_TMP(_s, _cnf)  AB_PATH(_s, _cnf, DB_DIR_TMP)
#define AB_PATH_BAK(_s, _cnf) AB_PATH(_s, _cnf, DB_DIR_BAK)
#define AB_PATH_RECYCLE(_s, _cnf) AB_PATH(_s, _cnf, DB_DIR_RECYCLE)
#define AB_PATH_REMOTE(_s, _cnf) AB_PATH(_s, _cnf, DB_DIR_REMOTE)

#define DATA_PATH(_s, _cnf, _name) \
    snprintf(_s, sizeof(_s), "%s/"DB_DIR_DATA"/%s", _cnf->dbpath, _name)

#define DATA_FILE(_s, _cnf, _num, _lv) \
    snprintf(_s, sizeof(_s), "%s/"DB_DIR_DATA"/%"PRIu64"_%d"DB_DATA_EXT, _cnf->dbpath, _num, _lv)

#define BIN_PATH(_s, _cnf, _name) \
    snprintf(_s, sizeof(_s), "%s/"DB_DIR_BIN"/%s", _cnf->dbpath, _name)

#define BIN_FILE(_s, _cnf, _num) \
    snprintf(_s, sizeof(_s), "%s/"DB_DIR_BIN"/%"PRIu64 DB_BIN_EXT, _cnf->dbpath, _num)

#define TMP_PATH(_s, _cnf, _name) \
    snprintf(_s, sizeof(_s), "%s/"DB_DIR_TMP"/%s", _cnf->dbpath, _name)

#define TMP_FIRST_PARTIAL(_s, _cnf, _name) \
    snprintf(_s, sizeof(_s), "%s/"DB_DIR_TMP"/%s.part1", _cnf->dbpath, _name)

#define TMP_SECOND_PARTIAL(_s, _cnf, _name) \
    snprintf(_s, sizeof(_s), "%s/"DB_DIR_TMP"/%s.part2", _cnf->dbpath, _name)

#define TMP_FILE(_s, _cnf, _num) \
    sprintf(_s, "%s/"DB_DIR_TMP"/tmp.hdb.%"PRIu64, _cnf->dbpath, _num)

#define T conf_t
typedef struct conf_s T;

/* @n: new data
 * @o: old data
 */
typedef int (*KVCMP)(const void *old, const void *new);

#ifndef DB_KVFILTER
#define DB_KVFILTER
typedef int32_t (*DB_KFILTER)(char *, size_t);
typedef int32_t (*DB_VFILTER)(const char *, size_t, int *expire);
#endif

#define DBCNF_PREFETCH_KCACHE 1
#define DBCNF_PREFETCH_VCACHE (1 << 1)

struct conf_s {
    /* must be the first field */
    obj_t  super;

    int    flag;
    int    db_level;
    int    imq_limit;
    size_t batch_size;
    size_t bin_size;        /* binglog size */
    size_t mtb_size;        /* memtable size */
    size_t bin_mxsize;      /* blocking if binglog reach this size */
    size_t mtb_mxsize;      /* blocking if memtable reach this size */
    size_t ftb_size;        /* file table size */
    size_t ftb_min_kcnt;    
    size_t shrink_cpct_cnt;
    size_t vcache;
    size_t tmp_fnum;

    rwlock_t lock;

    char *dbpath;
    DB_KFILTER kflt;
    DB_VFILTER vflt;

    /* methods */
    int   (*parse)(T *thiz, const char *conf_str);
    int   (*init)(T *thiz);     /* init with default value */
    void  (*destroy)(T *thiz);

    int   (*mkeyflt)(T *thiz, mkv_t *kv);   
    int   (*mvalflt)(T *thiz, mkv_t *kv);   
    int   (*fkeyflt)(T *thiz, fkv_t *fkv);   
    int   (*fvalflt)(T *thiz, fkv_t *fkv);   
};

T *conf_create(pool_t *mpool);

#undef T
#endif
