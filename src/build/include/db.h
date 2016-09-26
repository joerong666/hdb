#ifndef HIDB2_DB_H_
#define HIDB2_DB_H_

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <sys/time.h>
#include <time.h>

#include "db_com_def.h"
#define HIDB2(fn) fn ## _v2

#ifndef DEF_HIDB_STRUCT
#define DEF_HIDB_STRUCT

#ifndef DB_MAX_LEVEL 
#  define DB_MAX_LEVEL 16
#endif

#define DBATTR_INIT(__attr) do {    \
    (__attr)->wait_compact = 0;     \
    (__attr)->build_manifest = 0;   \
    (__attr)->readonly = 0;         \
    (__attr)->auto_purge = 1;       \
    (__attr)->manual_purge = 0;     \
    (__attr)->split_size = 0;       \
    (__attr)->bloom_level = 1;      \
    (__attr)->keyfilter = NULL;     \
    (__attr)->valfilter = NULL;     \
} while(0)

typedef struct db_s db_t;

typedef struct
{
	/* DEFAULT:false. will not wait for completing compaction before exit. */	
	bool wait_compact; 

	/* DEFAULT:false. Will not build MANIFEST if it exists. ignored in new version*/
	bool build_manifest;

    /* DEFAULT:false. If readonly, compact threads will not create */
    bool readonly;

    /* DEFAULT: 1 */
    bool auto_purge;

    /* DEFAULT: 0 */
    bool manual_purge;

    /* DEFAULT: 4(means 4MB, range: 1MB~32MB), file is splitted base on this size */
    uint32_t split_size;

    /* DEFAULT: 1, if provided, level less than this value will add bloom filter */
    uint16_t bloom_level;

    /* k-v will be filtered if key matched */
    DB_KFILTER keyfilter;

    /* k-v will be filtered if value matched */
    DB_VFILTER valfilter;
} db_attr_t;

/* debug flag */
enum db_debug_flag {
    DB_DFLAG_DEFAULT               = 0
    ,DB_DFLAG_KV                   = (1 << 0)
    ,DB_DFLAG_GET                  = (1 << 1)
    ,DB_DFLAG_SET                  = (1 << 2)
    ,DB_DFLAG_COMPACT              = (1 << 3)
    ,DB_DFLAG_ITERATE              = (1 << 4)
    ,DB_DFLAG_MANIFEST             = (1 << 5)
};

/* operation flag */
enum db_op_flag {
    DB_OPFLAG_DEFAULT              = 0
    ,DB_OPFLAG_CLEAN_EFILE         = (1 << 0)
    ,DB_OPFLAG_KEYFILTER_DROP      = (1 << 1)
};                                   
                                    
typedef struct                     
{                             
    int op_flag;
} db_opt_t;

typedef struct
{
    uint64_t file_cnt[DB_MAX_LEVEL];
    uint64_t total_efile;
    uint64_t total_get;
    uint64_t total_set;
    uint64_t total_del;
    uint64_t io_get;
    uint64_t er_get;
    uint64_t er_upd;
    uint64_t filtered_key;
    uint64_t filtered_val;
    struct timeval max_hit_time;
    struct timeval max_nhit_time;
    struct timeval max_utime;
} db_stats_t;

typedef struct iter_s iter_t;

struct hidba_s {
    char *dbpath;  /* dbpath to export */
    char *dstpath;  /* dest path to load from or export to */
    char *hs_ids; /* split data via these hash ids, just export file regarding to export_level */
    char *exp_time; /* file modified before this time will not export, format: 2015-02-04 00:00:00 */
    int hs_idx;  /* only those keys match hash_ids[hash_idx] will export, -1 means just split without filter */
    int exp_lvl; /* only export data below this level(not include this level itself) */
    int do_quick; /* quickly export, etc */
    int tc_idx;   /* test case index in db_bench.c; 4=>test_load_dir, 9=>test_export */
    DB_VFILTER vfilter; /* value match this filter will be abandoned */
};

#define HIDBA_INITIALIZER {.dbpath = NULL, .dstpath = NULL, .hs_ids = NULL, \
                           .hs_idx = -1, .exp_lvl = -1, .do_quick = 0, .vfilter = NULL, \
                           .exp_time = NULL, .tc_idx = 9 \
                        }
#endif

#define DBVER_UNKNOWN   -1  /* not expected */
#define DBVER_EMPTY     0   /* dbe is empty */
#define DBVER_V1        1   
#define DBVER_V2        2

/*
 * @ret: one of DBVER_*
 */
int  dbe_version(char *path);

db_t *HIDB2(db_open)(const char *path, const db_attr_t *attr);

void HIDB2(db_close)(db_t *db);

void HIDB2(db_print_state)(db_t *db);

int HIDB2(db_put)(db_t *db, char *key, uint16_t key_size, char *val, uint32_t val_size);

int HIDB2(db_del)(db_t *db, char *key, uint16_t key_size);

int HIDB2(db_mput)(db_t *db, kvec_t *vec, size_t cnt);

int HIDB2(db_mdel)(db_t *db, kvec_t *vec, size_t cnt);

int HIDB2(db_get)(db_t *db, char *key, uint16_t key_size, 
			char **val, uint32_t *val_size, void *(*alloc_func)(size_t));

/* get via key prefix */
iter_t *HIDB2(db_pget)(db_t *db, char *kprefix, size_t ks);

/* del via key prefix */
int HIDB2(db_pdel)(db_t *db, char *kprefix, size_t ks);

int HIDB2(db_set_rulefilter)(db_t *db, DB_KFILTER flt);

int HIDB2(db_set_valfilter)(db_t *db, DB_VFILTER flt);

int HIDB2(db_get_attr)(db_t *db, db_attr_t *attr);

int HIDB2(db_get_stats)(db_t *db, db_stats_t *stats);

db_opt_t *HIDB2(db_get_opt)(db_t *db);

void HIDB2(db_set_opt)(db_t *db, const db_opt_t *opt);

int HIDB2(db_load_dir)(db_t *db, const char *dir);

int HIDB2(db_clean_file)(db_t *db, struct tm *before);

/**
 * @cnt: limit to 1~100
 */
int HIDB2(db_clean_oldest)(db_t *db, int cnt);

int HIDB2(db_freeze)(db_t *db);

int HIDB2(db_unfreeze)(db_t *db);

iter_t *HIDB2(db_create_it)(db_t *db, int level);

void HIDB2(db_destroy_it)(iter_t *it);

/*
* Return code:
* 1: finished
* 0: success
* -1: error
*/
int HIDB2(db_iter)(iter_t *it, char **key, int *k_size, char **val, int *v_size, void *(*alloc_func)(size_t));

int HIDB2(db_clear)(db_t *db);

int HIDB2(db_get_level)(db_t *db);

void HIDB2(db_set_debug_flag)(int debug_flag);

int HIDB2(db_run)(db_t *db);

int HIDB2(db_flush)(db_t *db);

int HIDB2(db_checkpoint)(db_t *db);

void HIDB2(db_manual_purge)(db_t *db);

/* begin global transaction */
int HIDB2(db_gtx_begin)(db_t *db);

/* commit global transaction */
int HIDB2(db_gtx_comit)(db_t *db);

int HIDB2(hidba_export)(const struct hidba_s *dba);

#endif
