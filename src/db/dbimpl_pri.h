#ifndef _HDB_DBIMPL_PRI_H_
#define _HDB_DBIMPL_PRI_H_

#include "hdb_inc.h"
#include "dbimpl.h"

#define HI_PREFIX(_f) hdb_ ## _f
#define T dbimpl_t

#define DB_STATS_FETCH(_field) (SELF->stats.total_ ## _field)
    
#define DBRD_STATS_INCR(_field) do {      \
    atomic_incr_b(SELF->stats.total_ ## _field);    \
    atomic_incr_b(SELF->stats.total_rd);            \
} while(0)

#define DBWR_STATS_INCR(_field) do {      \
    atomic_incr_b(SELF->stats.total_ ## _field);    \
    atomic_incr_b(SELF->stats.total_wr);            \
} while(0)

typedef struct dbstat_s {
    uint64_t total_get;
    uint64_t total_pget;
    uint64_t total_put;
    uint64_t total_del;
    uint64_t total_mput;
    uint64_t total_mdel;
    uint64_t total_pdel;
    uint64_t total_rd;
    uint64_t total_wr;
} dbstat_t;

struct dbimpl_pri {
    int flag;
    int notice;
    uint64_t max_fnum;
    uint64_t seq;

    dbstat_t stats;

    rwlock_t lock;

    pthread_mutex_t compact_mtx;
    pthread_cond_t  compact_cond;

    pthread_mutex_t bin_mtx;
    pthread_cond_t  bin_cond;

    pthread_mutex_t dump_mtx;
    pthread_cond_t  dump_cond;

    pthread_mutex_t major_mtx;
    pthread_cond_t  major_cond;

    pthread_mutex_t flush_mtx;
    pthread_cond_t  flush_cond;

    pthread_mutex_t prot_mtx;
    pthread_cond_t  prot_cond;

    char *path;
    mtb_t *mmtb;
    mtbset_t *imq;
    compactor_t *cpct;
    thpool_t *thpool;
};

enum notify_e {
    NTF_TIMER       = 1,
    NTF_WRITE_BIN   = 1 << 1,
    NTF_FLUSH_MMT   = 1 << 2,
    NTF_FLUSH_IMQ   = 1 << 3,
    NTF_FLUSH_DB    = 1 << 4,
    NTF_MTB_DUMP    = 1 << 5,
    NTF_COMPACT     = 1 << 6,
    NTF_PROT        = 1 << 7,
    NTF_DBDESTROY   = 1 << 8,
    NTF_PRINT_STATS = 1 << 9,
    NTF_MAX,
};

uint64_t HI_PREFIX(next_fnum)(T *thiz);

uint64_t HI_PREFIX(get_seq)(T *thiz);

#undef T
#endif
