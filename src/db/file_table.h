#ifndef _HDB_FILE_TABLE_H_
#define _HDB_FILE_TABLE_H_

#include "btree.h"
#include "mem_table.h"

#define T ftb_t
typedef struct ftb_s T;
typedef struct ftbset_s ftbset_t;
typedef struct compactor_s compactor_t;

struct ftb_s {
    /* must be the first field */
    obj_t  super;

    /* public fields */
    int flag;
    rwlock_t lock;
    char file[G_MEM_MID];
    conf_t *conf;
    struct list_head fnode; /* node of db file list */
    struct list_head cnode; /* node of compactor file list */
    btree_t *model;

    struct ftb_pri *pri;

    /* methods */
    int   (*init)(T *thiz);
    void  (*destroy)(T *thiz);
    void  (*clean)(T *thiz);
    void  (*backup)(T *thiz);

    int   (*find)(T *thiz, mkey_t *k, mval_t *v);
    int   (*exist)(T *thiz, mkey_t *k, uint64_t ver);
    int   (*store)(T *thiz, mtb_t *mtb);
    int   (*restore)(T *thiz);
    void  (*search_overlap)(T *thiz, ftbset_t *fset, struct list_head *ovr);
};

#define FSET_FLG_ORDERED 1

struct ftbset_s {
    /* must be the first field */
    obj_t  super;

    int flag;
    int flist_len;
    rwlock_t lock;
    struct list_head flist; /* db file list */

    struct ftbset_pri *pri;

    /* methods */
    void  (*destroy)(ftbset_t *thiz);

    int   (*len)(ftbset_t *thiz);
    int   (*push)(ftbset_t *thiz, T *item);
    int   (*find)(ftbset_t *thiz, mkey_t *k, mval_t *v);
    int   (*exist)(ftbset_t *thiz, mkey_t *k, ftb_t *until_pos, uint64_t ver);
    int   (*find_ins_pos)(ftbset_t *thiz, ftb_t *ftb, ftb_t **pos);
    ftb_t *(*top)(ftbset_t *thiz);
    ftb_t *(*tail)(ftbset_t *thiz);
    ftb_t *(*search_cpct_tb)(ftbset_t *thiz, int cpct_type);
};

struct compactor_s {
    /* must be the first field */
    obj_t  super;

    /* public fields */
    int flag;
    int type;
    uint64_t nfnum1;    /* new file num, for split */
    uint64_t nfnum2;    /* new file num, for split */

    conf_t *conf;
    ftb_t *src_ftb;
    ftbset_t *src_fset;
    ftbset_t *dst_fset;

    /* methods */
    void  (*destroy)(struct compactor_s *thiz);

    void  (*compact)(struct compactor_s *thiz);
};

T *ftb_create(pool_t *mpool);
ftbset_t *ftbset_create(pool_t *mpool);
compactor_t *compactor_create(pool_t *mpool);

#undef T
#endif

