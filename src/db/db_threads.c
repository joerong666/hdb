#include "dbimpl_pri.h"
#include "operation.h"
#include "db_threads.h"

#define T dbimpl_t

#define LEVEL_FILE_CNT(_lv)  0
#define STAT_INTERVAL 60

#define ATTACH_NOTICE(_nt) ATOMIC_ATTACH(SELF->notice, _nt)
#define DETACH_NOTICE(_nt) ATOMIC_DETACH(SELF->notice, _nt)

static void timer_thread(T *thiz);
static void ntf_write_bin(T *thiz);
static void ntf_mtb_dump(T *thiz);
static void ntf_compact(T *thiz);

typedef void (*NTF_EXECUTOR)(T *thiz);
static NTF_EXECUTOR g_ntf_exec[NTF_MAX] = {
    [NTF_TIMER] = timer_thread,
    [NTF_WRITE_BIN] = ntf_write_bin,
    [NTF_MTB_DUMP] = ntf_mtb_dump,
    [NTF_COMPACT] = ntf_compact,
};

void HI_PREFIX(notify)(T *thiz, int notice)
{
    int nt = SELF->notice;

    switch (notice) {
#if 1
    case NTF_TIMER:
        thpool_add_job(SELF->thpool, (void *(*)(void *))g_ntf_exec[notice], thiz);
        break;
#endif
#if 1
    case NTF_WRITE_BIN:
        if (nt & notice) {
            if (0 == pthread_mutex_trylock(&SELF->bin_mtx)) {
                pthread_cond_signal(&SELF->bin_cond);
                pthread_mutex_unlock(&SELF->bin_mtx);
            }
        } else {
            ATTACH_NOTICE(notice);
            thpool_add_job(SELF->thpool, (void *(*)(void *))g_ntf_exec[notice], thiz);
        }
        break;
#endif
#if 1
    case NTF_FLUSH_MMT:
        if (0 == pthread_mutex_trylock(&SELF->bin_mtx)) {
            ATTACH_NOTICE(notice);
            pthread_cond_signal(&SELF->bin_cond);
            pthread_mutex_unlock(&SELF->bin_mtx);
        }
        break;
#endif
#if 1
    case NTF_FLUSH_IMQ:
    case NTF_FLUSH_DB:
        pthread_mutex_lock(&SELF->bin_mtx);
        ATTACH_NOTICE(notice);
        pthread_cond_signal(&SELF->bin_cond);
        pthread_mutex_unlock(&SELF->bin_mtx);
        break;
#endif
#if 1
    case NTF_PRINT_STATS:
        ATTACH_NOTICE(notice);
        if (0 == pthread_mutex_trylock(&SELF->dump_mtx)) {
            pthread_cond_signal(&SELF->dump_cond);
            pthread_mutex_unlock(&SELF->dump_mtx);
        }
        break;
    case NTF_MTB_DUMP:
        if (nt & notice) {
            if (0 == pthread_mutex_trylock(&SELF->dump_mtx)) {
                pthread_cond_signal(&SELF->dump_cond);
                pthread_mutex_unlock(&SELF->dump_mtx);
            }
        } else {
            ATTACH_NOTICE(notice);
            thpool_add_job(SELF->thpool, (void *(*)(void *))g_ntf_exec[notice], thiz);
        }
        break;
#endif
#if 1
    case NTF_COMPACT:
        if (nt & notice) {
            pthread_mutex_lock(&SELF->compact_mtx);
            pthread_cond_signal(&SELF->compact_cond);
            pthread_mutex_unlock(&SELF->compact_mtx);
        } else {
            ATTACH_NOTICE(notice);
            thpool_add_job(SELF->thpool, (void *(*)(void *))g_ntf_exec[notice], thiz);
        }
        break;
#endif
    case NTF_PROT:
        pthread_mutex_lock(&SELF->prot_mtx);
        pthread_cond_signal(&SELF->prot_cond);
        pthread_mutex_unlock(&SELF->prot_mtx);
        break;
    case NTF_DBDESTROY:
        ATTACH_NOTICE(notice);

        pthread_mutex_lock(&SELF->bin_mtx);
        pthread_cond_signal(&SELF->bin_cond);
        pthread_mutex_unlock(&SELF->bin_mtx);

        pthread_mutex_lock(&SELF->dump_mtx);
        pthread_cond_signal(&SELF->dump_cond);
        pthread_mutex_unlock(&SELF->dump_mtx);

        pthread_mutex_lock(&SELF->compact_mtx);
        pthread_cond_signal(&SELF->compact_cond);
        pthread_mutex_unlock(&SELF->compact_mtx);
        break;
    default:
        ERROR("invalid notice=%d", notice);
    }
}

static void timer_thread(T *thiz)
{
    struct timeval tv;
    static uint64_t tps_wr = 0, tps_ts = 0, ts = 0, delt_ts = 0;

    while (1) {
        if (SELF->notice & NTF_DBDESTROY) break;

        gettimeofday(&tv, NULL);

        ts = tv.tv_sec * 1e6 + tv.tv_usec;
        atomic_casv(SELF->seq, ts);

        if (tv.tv_sec - tps_ts >= 1) {
            tps_ts = tv.tv_sec;

            /* update tps <= 100, notify flush */
            if ((DB_STATS_FETCH(wr) - tps_wr) <= 100) {
                HI_PREFIX(notify)(thiz, NTF_FLUSH_MMT);
            }

            tps_wr = SELF->stats.total_wr;
        }

        if (tv.tv_sec - delt_ts >= STAT_INTERVAL) {
            delt_ts = tv.tv_sec;
            HI_PREFIX(notify)(thiz, NTF_PRINT_STATS);
        }

        usleep(1);
    }
}

static void ntf_write_bin(T *thiz)
{
    mtb_t *m;

    while (1) {
        pthread_mutex_lock(&SELF->bin_mtx);
        while (1) {
            m = HI_PREFIX(get_mmtb)(thiz);
            
            if (m->write_ready(m)) {
                m->write_bin(m);
            }

            if (SELF->notice & (NTF_FLUSH_MMT | NTF_FLUSH_DB)) {
                m->flush(m);
                DETACH_NOTICE(NTF_FLUSH_MMT);
            }

            if (SELF->notice & (NTF_FLUSH_IMQ | NTF_FLUSH_DB)) {
                SELF->imq->flush(SELF->imq);
                DETACH_NOTICE(NTF_FLUSH_IMQ);
            }

            if (SELF->notice & NTF_FLUSH_DB) {
                DETACH_NOTICE(NTF_FLUSH_DB);
                SELF->imq->flush_notify(SELF->imq);
                m->flush_notify(m);
            }

            if (SELF->notice & NTF_DBDESTROY) {
                pthread_mutex_unlock(&SELF->bin_mtx);
                return;
            }

            pthread_cond_wait(&SELF->bin_cond, &SELF->bin_mtx);
        }
        pthread_mutex_unlock(&SELF->bin_mtx);
    }
}

static void do_mtb_dump(T *thiz)
{
    int r;
    uint64_t fnum;
    char fname[G_MEM_MID];
    mtb_t *mtb;
    ftb_t *ftb = NULL;
    ftbset_t *fsets = thiz->fsets[0];

    mtb = SELF->imq->top(SELF->imq);

    HI_PREFIX(notify)(thiz, NTF_FLUSH_IMQ);
    mtb->flush_wait(mtb);

    fnum = HI_PREFIX(next_fnum)(thiz);
    ftb = ftb_create(NULL);
    ftb->conf = thiz->conf;
    TMP_FILE(ftb->file, thiz->conf, fnum);

    r = ftb->init(ftb);
    if (r != 0) goto _out;

    INFO("store %s via %s", ftb->file, mtb->file);
    r = ftb->store(ftb, mtb);
    if (r != 0) goto _out;

    DATA_FILE(fname, thiz->conf, fnum, 0);
    rename(ftb->file, fname);
    strcpy(ftb->file, fname);

    r = fsets->push(fsets, ftb);
    if (r != 0) goto _out;

    SELF->imq->pop(SELF->imq);
    mtb->clean(mtb);
    mtb->destroy(mtb);

    HI_PREFIX(notify)(thiz, NTF_PROT);
    HI_PREFIX(notify)(thiz, NTF_COMPACT);

_out:
    if (r != 0 && ftb != NULL) {
        ftb->clean(ftb);
        ftb->destroy(ftb);
    }

    return;
}

static char *db_stats(T *thiz)
{
    static char msg[G_MEM_SML];
    static uint64_t delt_put = 0, delt_del = 0, delt_mput = 0,
                    delt_mdel = 0, delt_pdel = 0, delt_wr = 0,
                    delt_get = 0,  delt_pget = 0, delt_rd = 0;
    dbstat_t *stats = &SELF->stats;

    snprintf(msg, sizeof(msg),
           "get:%"PRIu64", pget:%"PRIu64", total_rd:%"PRIu64", tps_rd:%"PRIu64
           ", put:%"PRIu64", del:%"PRIu64", mput:%"PRIu64
           ", mdel:%"PRIu64", pdel:%"PRIu64", total_wr:%"PRIu64", tps_wr:%"PRIu64
           ,stats->total_get  - delt_get
           ,stats->total_pget - delt_pget
           ,stats->total_rd   - delt_rd
           ,(stats->total_rd   - delt_rd) / STAT_INTERVAL
           ,stats->total_put  - delt_put
           ,stats->total_del  - delt_del
           ,stats->total_mput - delt_mput
           ,stats->total_mdel - delt_mdel
           ,stats->total_pdel - delt_pdel
           ,stats->total_wr   - delt_wr
           ,(stats->total_wr   - delt_wr) / STAT_INTERVAL
          );

    delt_get   = stats->total_get ;
    delt_pget  = stats->total_pget;
    delt_rd    = stats->total_rd  ;
    delt_put   = stats->total_put ;
    delt_del   = stats->total_del ;
    delt_mput  = stats->total_mput;
    delt_mdel  = stats->total_mdel;
    delt_pdel  = stats->total_pdel;
    delt_wr    = stats->total_wr  ;

    return msg;
}

static void print_stats(T *thiz)
{
    DETACH_NOTICE(NTF_PRINT_STATS);

    PROMPT(
           "\r\nOPERATION: %s"
           "\r\nIMQ: %s"
           "\r\nL0: %s"
           "\r\nL1: %s"
           ,db_stats(thiz)
           ,SELF->imq->stats_info(SELF->imq)
           ,thiz->fsets[0]->stats_info(thiz->fsets[0])
           ,thiz->fsets[1]->stats_info(thiz->fsets[1])
          );
}

static void ntf_mtb_dump(T *thiz)
{
    while (1) {
        if (SELF->notice & NTF_DBDESTROY) break;

        pthread_mutex_lock(&SELF->dump_mtx);
        while (1) {
            if (SELF->notice & NTF_PRINT_STATS) print_stats(thiz);
            if (SELF->notice & NTF_DBDESTROY) break;
            if (!SELF->imq->empty(SELF->imq)) break;

            pthread_cond_wait(&SELF->dump_cond, &SELF->dump_mtx);
        }

        if (!(SELF->notice & NTF_DBDESTROY)) {
            do_mtb_dump(thiz);
        }
        pthread_mutex_unlock(&SELF->dump_mtx);
    }
}

struct cpct_job_s {
    T *db;
    compactor_t *cpct;
};

static void compact_job(struct cpct_job_s *job)
{
    CONSOLE_DEBUG("compact %s, type=%d", job->cpct->src_ftb->file, job->cpct->type);
    job->cpct->compact(job->cpct);

    pthread_mutex_lock(&job->db->pri->compact_mtx);
    ATOMIC_DETACH(job->db->pri->flag, job->cpct->type);
    pthread_mutex_unlock(&job->db->pri->compact_mtx);

    CONSOLE_DEBUG("compact fin");
    HI_PREFIX(notify)(job->db, NTF_COMPACT);
    job->cpct->destroy(job->cpct);
}

static int raise_cpct_major(T *thiz)
{
    int r = 0, i;
    struct cpct_job_s *job;
    compactor_t *cpct;
    ftbset_t *src_fset, *dst_fset;
    ftb_t *ftb;

    if (SELF->flag & (CPCT_SPLIT | CPCT_SHRINK | CPCT_MAJOR | CPCT_AJACENT)) return 0;

    for (i = 0; i < thiz->conf->db_level - 1; i++) {
        src_fset = thiz->fsets[i];
        dst_fset = thiz->fsets[i + 1];

        if (src_fset->len(src_fset) <= LEVEL_FILE_CNT(i)) continue;

        ftb = src_fset->search_cpct_tb(src_fset, CPCT_MAJOR);
        if (ftb == NULL) continue;

        TRACE("%s", __func__);
        cpct = compactor_create(NULL);

        cpct->type = CPCT_MAJOR;
        cpct->conf = thiz->conf;
        cpct->src_ftb = ftb;
        cpct->src_fset = src_fset;
        cpct->dst_fset = dst_fset;

        job = PALLOC(cpct->super.mpool, sizeof(*job));
        job->db = thiz;
        job->cpct = cpct;

        thpool_add_job(SELF->thpool, (void *(*)(void *))compact_job, job);

        r = 1;
        break;
    }

    return r;
}

static int raise_cpct_split(T *thiz, int op)
{
    int r = 0, i;
    struct cpct_job_s *job;
    compactor_t *cpct;
    ftbset_t *fset;
    ftb_t *ftb = NULL;

    if (SELF->flag & (CPCT_SHRINK | CPCT_MAJOR | CPCT_SPLIT | CPCT_AJACENT)) return 0;

    for (i = 1; i < thiz->conf->db_level; i++) {
        fset = thiz->fsets[i];

        if (op == CPCT_SPLIT) {
            ftb = fset->search_cpct_tb(fset, CPCT_SPLIT);
        } else if (thiz->fsets[0]->len(thiz->fsets[0]) == 0) {
            ftb = fset->search_cpct_tb(fset, CPCT_SHRINK);
        }

        if (ftb == NULL) continue;

        cpct = compactor_create(NULL);

        cpct->type = op;
        cpct->nfnum1 = HI_PREFIX(next_fnum)(thiz);
        if (op == CPCT_SPLIT) {
            cpct->nfnum2 = HI_PREFIX(next_fnum)(thiz);
        }

        cpct->conf = thiz->conf;
        cpct->src_ftb = ftb;
        cpct->src_fset = fset;
        cpct->dst_fset = fset;

        job = PALLOC(cpct->super.mpool, sizeof(*job));
        job->db = thiz;
        job->cpct = cpct;

        thpool_add_job(SELF->thpool, (void *(*)(void *))compact_job, job);

        r = 1;
        break;
    }

    return r;
}

static int raise_cpct_ajacent(T *thiz)
{
    int r = 0, i;
    struct cpct_job_s *job;
    compactor_t *cpct;
    ftbset_t *src_fset, *dst_fset;
    ftb_t *ftb;

    if (SELF->flag & (CPCT_SPLIT | CPCT_SHRINK | CPCT_MAJOR | CPCT_AJACENT)) return 0;

    do {
        i = thiz->conf->db_level - 1;

        src_fset = thiz->fsets[i];
        dst_fset = thiz->fsets[i];

        ftb = src_fset->search_cpct_tb(src_fset, CPCT_AJACENT);
        if (ftb == NULL) continue;

        TRACE("%s", __func__);
        cpct = compactor_create(NULL);

        cpct->type = CPCT_AJACENT;
        cpct->conf = thiz->conf;
        cpct->src_ftb = ftb;
        cpct->src_fset = src_fset;
        cpct->dst_fset = dst_fset;

        job = PALLOC(cpct->super.mpool, sizeof(*job));
        job->db = thiz;
        job->cpct = cpct;

        thpool_add_job(SELF->thpool, (void *(*)(void *))compact_job, job);

        r = 1;
        break;
    } while(0);

    return r;
}

static int raise_cpct_L0(T *thiz)
{
    if (SELF->flag & CPCT_L0) return 0;

    /* TODO!! */
    return 0;
}

static void raise_compact(T *thiz)
{
    int r;

    r = raise_cpct_L0(thiz);
    if (r) SELF->flag |= CPCT_L0; 
#if 1
    r = raise_cpct_split(thiz, CPCT_SPLIT);
    if (r) SELF->flag |= CPCT_SPLIT; 
#endif
#if 1
    r = raise_cpct_major(thiz);
    if (r) SELF->flag |= CPCT_MAJOR; 
#endif
#if 1
    r = raise_cpct_ajacent(thiz);
    if (r) SELF->flag |= CPCT_AJACENT; 
#endif
#if 1
    r = raise_cpct_split(thiz, CPCT_SHRINK);
    if (r) SELF->flag |= CPCT_SHRINK; 
#endif
}

static void ntf_compact(T *thiz)
{
    while (1) {
        if (SELF->notice & NTF_DBDESTROY) break;

        pthread_mutex_lock(&SELF->compact_mtx);
        raise_compact(thiz);

        pthread_cond_wait(&SELF->compact_cond, &SELF->compact_mtx);
        pthread_mutex_unlock(&SELF->compact_mtx);
    }
}

int HI_PREFIX(prepare_thpool)(T *thiz)
{
    SELF->thpool = thpool_init(5, 20);
    if (SELF->thpool == NULL) return -1;

    return 0;
}


