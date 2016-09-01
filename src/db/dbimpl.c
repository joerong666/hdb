#include "hdb_inc.h"
#include "coding.h"
#include "btree_aux.h"
#include "btree.h"
#include "mem_table.h"
#include "file_table.h"
#include "dbimpl.h"

#define T dbimpl_t

#define LEVEL_FILE_CNT(_lv)  0

#if 0
    $ date -d '2016-01-01' +%s
      1451577600
#define BASE_TIMESTAMP 1451577600
#else
#define BASE_TIMESTAMP 0
#endif

#define ATTACH_NOTICE(_nt)                  \
{                                           \
    int _xnt = SELF->notice;                \
    _xnt |= (_nt);                          \
    _xnt = atomic_casv(SELF->notice, _xnt); \
}

#define DETACH_NOTICE(_nt)                  \
{                                           \
    int _xnt = SELF->notice;                \
    _xnt &= ~(_nt);                         \
    _xnt = atomic_casv(SELF->notice, _xnt); \
}

#define DB_STATS_FETCH(_field) (SELF->stats.total_ ## _field)
      
#define DBRD_STATS_INCR(_field) do {                \
    atomic_incr_b(SELF->stats.total_ ## _field);    \
    atomic_incr_b(SELF->stats.total_rd);            \
} while(0)

#define DBWR_STATS_INCR(_field) do {                \
    atomic_incr_b(SELF->stats.total_ ## _field);    \
    atomic_incr_b(SELF->stats.total_wr);            \
} while(0)

typedef struct dbstat_s {
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

    pthread_mutex_t rw_lock;

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

static char *g_dirs[] = { "", DB_DIR_DATA, DB_DIR_TMP, DB_DIR_BAK, 
                          DB_DIR_RECYCLE, DB_DIR_BIN
                        };

enum notify_e {
    NTF_TIMER     = 1,
    NTF_WRITE_BIN = 1 << 1,
    NTF_FLUSH_BIN = 1 << 2,
    NTF_MTB_DUMP  = 1 << 3,
    NTF_COMPACT   = 1 << 4,
    NTF_PROT      = 1 << 5,
    NTF_DBDESTROY = 1 << 6,
    NTF_MAX,
};

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

static int   config(T *thiz, const char *conf_str)
{
    int r = 0;

    if (thiz->conf == NULL) {
        thiz->conf = conf_create(SUPER->mpool);
        thiz->conf->dbpath = SELF->path;
        thiz->conf->init(thiz->conf);
    }

    if (conf_str != NULL) {
        r = thiz->conf->parse(thiz->conf, conf_str);
    }

    return r;
}

static uint64_t next_fnum(T *thiz)
{
    atomic_incr_b(SELF->max_fnum);
    return SELF->max_fnum;
}

static void notify(T *thiz, int notice)
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
    case NTF_FLUSH_BIN:
        ATTACH_NOTICE(notice);
        if (0 == pthread_mutex_trylock(&SELF->bin_mtx)) {
            pthread_cond_signal(&SELF->bin_cond);
            pthread_mutex_unlock(&SELF->bin_mtx);
        }
        break;
#endif
#if 1
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
        SELF->notice |= NTF_DBDESTROY;
        pthread_cond_signal(&SELF->bin_cond);
        pthread_cond_signal(&SELF->dump_cond);
        pthread_cond_signal(&SELF->compact_cond);
        break;
    default:
        ERROR("invalid notice=%d", notice);
    }
}

static int prepare_dirs(T *thiz)
{
    char fname[G_MEM_MID];
    int r = 0, i;

    for (i = 0; i < (int)ARR_LEN(g_dirs); i++) {
        AB_PATH(fname, thiz->conf, g_dirs[i]);
        PROMPT("mkdir %s if not exist", fname);

        if (mkdir(fname, 0775) == -1 && errno != EEXIST) {
            r = -1;
            ERROR("mkdir %s, errno=%d", g_dirs[i], errno);
        }
    }

    return r;
}

static int prepare_files(T *thiz)
{
    /* TODO!! create some file needed */ 

    return 0;
}

static int prepare_mtb(T *thiz)
{
    int r;

    mtb_t *m = mtb_create(NULL);

    BIN_FILE(m->file, thiz->conf, next_fnum(thiz));
    m->conf = thiz->conf;
    r = m->init(m);
    if (r != 0) return r;

    pthread_mutex_lock(&SELF->rw_lock);
    SELF->mmtb = m;
    pthread_mutex_unlock(&SELF->rw_lock);

    return 0;
}

static void load_protect(T *thiz)
{
    int len;

    pthread_mutex_lock(&SELF->prot_mtx);
    while(1) {
        len = SELF->imq->len(SELF->imq);
        if (len > thiz->conf->imq_limit) {
            PROMPT("len of imq exceed %d, block push!", thiz->conf->imq_limit);
            pthread_cond_wait(&SELF->prot_cond, &SELF->prot_mtx);
        } else {
            break;
        }
    }
    pthread_mutex_unlock(&SELF->prot_mtx);
}

static int db_flush(T *thiz)
{
    int r = 0;

    pthread_mutex_lock(&SELF->bin_mtx);
    r = SELF->mmtb->flush(SELF->mmtb);
    pthread_mutex_unlock(&SELF->bin_mtx);

    return r;
}

static int   switch_mtb(T *thiz)
{
    int r = 0;

    if (SELF->mmtb->full(SELF->mmtb)) {
        db_flush(thiz);

        load_protect(thiz);

        r = SELF->imq->trypush(SELF->imq, SELF->mmtb);
        if (r != 0) {
            DEBUG("imq's lock not ready");
            r = 0;
            goto _out;
        }

        r = prepare_mtb(thiz);
        if (r != 0) goto _out;

        notify(thiz, NTF_MTB_DUMP);
    }

_out:
    return r;
}

static uint64_t get_seq(T *thiz)
{
    if (SELF->seq == 0) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        SELF->seq = (tv.tv_sec - BASE_TIMESTAMP) * 1e3 + tv.tv_usec * 1e-3;
    }

    return SELF->seq;
}

static void timer_thread(T *thiz)
{
    uint64_t seq;
    struct timeval tv;

    static uint64_t i = 0, total_wr = 0;

    while (1) {
        if (SELF->notice & NTF_DBDESTROY) break;

        gettimeofday(&tv, NULL);

        seq = (tv.tv_sec - BASE_TIMESTAMP) * 1e3 + tv.tv_usec * 1e-3;
        atomic_casv(SELF->seq, seq);

        if (++i % 1000 == 0) {
            if ((DB_STATS_FETCH(wr) - total_wr) <= 10) {
                INFO("tps <= 10, notify flush");
                notify(thiz, NTF_FLUSH_BIN);
            }

            i = 0;
            total_wr = SELF->stats.total_wr;
        }

        usleep(1000);
    }
}

static int   put_i(T *thiz, mkv_t *kv)
{
    int r;

    if (kv->k.len >= G_KSIZE_LIMIT) {
        ERROR("klen=%u too big", kv->k.len);
        return -1;
    }

    r = switch_mtb(thiz);
    if (r != 0) return -1;

    kv->seq = get_seq(thiz);

    r = SELF->mmtb->push(SELF->mmtb, kv);
    if (r != 0) return -1;

    notify(thiz, NTF_WRITE_BIN);
    return 0;
}

static int   put(T *thiz, mkv_t *kv)
{
    DBWR_STATS_INCR(put);

    kv->type = 0;
    return put_i(thiz, kv); 
}

static int   del(T *thiz, mkey_t *k)
{
    mkv_t kv;

    DBWR_STATS_INCR(del);
    memset(&kv, 0x0, sizeof(kv));
    kv.type = KV_OP_DEL;
    memcpy(&kv.k, k, sizeof(mkey_t));
    
    return put_i(thiz, &kv); 
}

static int   mput_i(T *thiz, mkv_t *kvs, size_t cnt)
{
    int r, i;
    uint64_t seq;

    r = switch_mtb(thiz);
    if (r != 0) return r;

    for (i = 0; i < (int)cnt; i++) {
        if (kvs[i].k.len >= G_KSIZE_LIMIT) {
            ERROR("klen=%u too big", kvs[i].k.len);
            return -1;
        }
    }

    seq = get_seq(thiz);

    RWLOCK_WRITE(&SELF->mmtb->lock);
    RWLOCK_WRITE(&SELF->mmtb->model->lock);
    for (i = 0; i < (int)cnt; i++) {
        kvs[i].seq = seq;
        r = SELF->mmtb->push_unsafe(SELF->mmtb, &kvs[i]);
        if (r != 0) break;
    }
    RWUNLOCK(&SELF->mmtb->model->lock);
    RWUNLOCK(&SELF->mmtb->lock);

    if (r != 0) return -1;

    notify(thiz, NTF_WRITE_BIN);
    return 0;
}

static int   mput(T *thiz, mkv_t *kvs, size_t cnt)
{
    size_t i;

    DBWR_STATS_INCR(mput);
    for (i = 0; i < cnt; i++) {
        kvs[i].type = 0;
    }

    return mput_i(thiz, kvs, cnt);
}

static int   mdel(T *thiz, mkey_t *keys, size_t cnt)
{
    size_t i;
    mkv_t kvs[cnt];

    DBWR_STATS_INCR(mdel);
    memset(kvs, 0x0, sizeof(kvs));

    for (i = 0; i < cnt; i++) {
        kvs[i].type = KV_OP_DEL;
        memcpy(&kvs[i].k, &keys[i], sizeof(mkey_t));
    }

    return mput_i(thiz, kvs, cnt);
}

static int   pdel(T *thiz, mkey_t *prefix)
{
    int r = 0;
    uint64_t seq;
    dbit_impl_t *it;
    mkv_t kv;

    DBWR_STATS_INCR(pdel);
    seq = get_seq(thiz);

    RWLOCK_WRITE(&SELF->mmtb->lock);
    RWLOCK_WRITE(&SELF->mmtb->model->lock);

    it = thiz->get_iter(thiz, prefix, NULL);
    it->flag |= IT_UNSAFE | IT_ONLY_KEY;

    while(it->next(it, &kv)) {
        kv.type = KV_OP_DEL;
        kv.seq = seq;
        r = SELF->mmtb->push_unsafe(SELF->mmtb, &kv);
        if (r != 0) break;
    }

    it->destroy(it);
    RWUNLOCK(&SELF->mmtb->model->lock);
    RWUNLOCK(&SELF->mmtb->lock);

    return r;
}

static int   find_Ln(T *thiz, mkey_t *k, mval_t *v)
{
    int r = RC_NOT_FOUND, i;
    ftbset_t *fset;

    for (i = 0; i < thiz->conf->db_level; i++) {
        fset = thiz->fsets[i];
        r = fset->find(fset, k, v);
        if (r == RC_FOUND || r == RC_ERR) goto _out;
    }

_out:
    return r;
}

static int   get(T *thiz, mkey_t *k, mval_t *v)
{
    int r;

    r = SELF->mmtb->find(SELF->mmtb, k, v);
    if (r != RC_NOT_FOUND) goto _out;

    r = SELF->imq->find(SELF->imq, k, v);
    if (r != RC_NOT_FOUND) goto _out;

    r = find_Ln(thiz, k, v);
    if (r != RC_NOT_FOUND) goto _out;

_out:
    if (r == RC_FOUND) return 0;

    return -1;
}

static int prepare_thread_pool(T *thiz)
{
    SELF->thpool = thpool_init(5, 20);
    if (SELF->thpool == NULL) return -1;

    return 0;
}

static int prepare_run(T *thiz)
{
    int r;

    r = prepare_thread_pool(thiz);
    if (r != 0) goto _out;

    notify(thiz, NTF_TIMER);
    notify(thiz, NTF_WRITE_BIN);
    notify(thiz, NTF_MTB_DUMP);
    notify(thiz, NTF_COMPACT);

_out:
    return r;
}

static int bl_flt(const struct dirent *ent)
{
    return (fnmatch("*"DB_BIN_EXT, ent->d_name, 0) == 0);
}

static int dt_flt(const struct dirent *ent)
{
    return (fnmatch("*"DB_DATA_EXT, ent->d_name, 0) == 0);
}

static int file_cmp(const void *a, const void *b)
{
    const struct dirent **ent_a = (const struct dirent **)a;
    const struct dirent **ent_b = (const struct dirent **)b;

    uint64_t an = strtoull((*ent_a)->d_name, NULL, 10);
    uint64_t bn = strtoull((*ent_b)->d_name, NULL, 10);

    if(an < bn) return +1;
    if(an > bn) return -1;

    return 0;
}

static int   recover_bl(T *thiz)
{
    int r = 0, m, n;
    uint64_t fnum = 0;
    char fname[G_MEM_MID];
    mtb_t *mtb;
    struct stat st;
    struct dirent **namelist = NULL, *ent;

    AB_PATH_BIN(fname, thiz->conf);
    m = n = scandir(fname, &namelist, bl_flt, file_cmp);
    if (n == -1) {
        ERROR("scandir %s, errno=%d", fname, errno);
        return -1;
    }    

    while (n > 0) { 
        ent = namelist[--n];
        
        r = sscanf(ent->d_name, "%ju", &fnum);
        if (r != 1) {
            ERROR("%s filename format incorrect, skip", ent->d_name);
            continue;
        }

        if (SELF->max_fnum < fnum) SELF->max_fnum = fnum;

        BIN_PATH(fname, thiz->conf, ent->d_name);
        stat(fname, &st);
        if (st.st_size <= 0) {
            ERROR("%s empty, remove it", fname);
            remove(fname);
            continue;
        }

        mtb = mtb_create(NULL);
        mtb->conf = thiz->conf;
        BIN_PATH(mtb->file, thiz->conf, ent->d_name);

        mtb->init(mtb);
        r = mtb->restore(mtb);
        if (r != 0) {
            ERROR("restore %s fail, skip", mtb->file);
            mtb->destroy(mtb);
            continue;
        }

        if (SELF->mmtb != NULL) {
            SELF->imq->push(SELF->imq, SELF->mmtb);
        }

        SELF->mmtb = mtb;
    }

    if (namelist) {
        while(--m >= 0) {
            free(namelist[m]);
        }

        free(namelist);
    }

    return 0;
}

static int   recover_dt(T *thiz)
{
    int r = 0, level = 0, m, n;
    uint64_t fnum = 0;
    char fname[G_MEM_MID];
    struct stat st;
    struct dirent **namelist = NULL, *ent;
    ftb_t *ftb;
    ftbset_t *fset;

    AB_PATH_DATA(fname, thiz->conf);
    m = n = scandir(fname, &namelist, dt_flt, file_cmp);
    if (n == -1) {
        ERROR("scandir %s, errno=%d", fname, errno);
        return -1;
    }    

    while (n > 0) { 
        ent = namelist[--n];

        /* file name format "fnum_level.ext", eg: 1_0.hdb, 2_1.hdb */
        r = sscanf(ent->d_name, "%ju_%d", &fnum, &level);
        if (r != 2) {
            ERROR("%s filename format incorrect, skip", ent->d_name);
            continue;
        }

        if (SELF->max_fnum < fnum) SELF->max_fnum = fnum;

        if (level >= thiz->conf->db_level) {
            ERROR("%s dblevel=%d exceed %d, skip", ent->d_name, level, thiz->conf->db_level);
            continue;
        }

        DATA_PATH(fname, thiz->conf, ent->d_name);
        stat(fname, &st);
        if (st.st_size <= 0) {
            ERROR("%s empty, remove it", fname);
            remove(fname);
            continue;
        }

        ftb = ftb_create(NULL);
        ftb->conf = thiz->conf;
        DATA_PATH(ftb->file, thiz->conf, ent->d_name);

        ftb->init(ftb);
        r = ftb->restore(ftb);
        if (r != 0) {
            ERROR("restore %s fail, backup", ftb->file);
            ftb->backup(ftb);
            ftb->destroy(ftb);
            r = 0;
            continue;
        }

        fset = thiz->fsets[level];
        r = fset->push(fset, ftb);
        if (r != 0) {
            ERROR("push %s fail, backup", ent->d_name);
            ftb->backup(ftb);
            ftb->destroy(ftb);
        }
    }

    if (namelist) {
        while(--m >= 0) {
            free(namelist[m]);
        }

        free(namelist);
    }

    return 0;
}

static int   recover(T *thiz)
{
    int r;

    r = recover_bl(thiz);
    if (r != 0) return -1;

    r = recover_dt(thiz);
    if (r != 0) return -1;

    return 0;
}

static int cleanup(T *thiz)
{
    char fname[G_MEM_MID];
    DIR *dir;
    struct dirent *ent;

    AB_PATH_TMP(fname, thiz->conf);
    dir = opendir(fname);
    if (dir == NULL) {
        ERROR("opendir %s, errno=%d", fname, errno);
        return -1;
    }

    while((ent = readdir(dir)) != NULL) {
        if (ent->d_type != DT_REG) continue;

        TMP_PATH(fname, thiz->conf, ent->d_name);
        remove(fname);
    }

    closedir(dir);
    return 0;
}

static int repaire(T *thiz)
{
    /* TODO!! repaire db if file corrupted */
    return 0;
}

static void ntf_write_bin(T *thiz)
{
    while (1) {
        if (SELF->notice & NTF_DBDESTROY) {
            db_flush(thiz);
            break;
        }

        pthread_mutex_lock(&SELF->bin_mtx);
        while (1) {
            if (SELF->notice & NTF_DBDESTROY) break;

            if (SELF->notice & NTF_FLUSH_BIN) {
                SELF->mmtb->flush(SELF->mmtb);
                DETACH_NOTICE(NTF_FLUSH_BIN);
            } else if (SELF->mmtb->write_ready(SELF->mmtb)) {
                SELF->mmtb->write(SELF->mmtb);
            }

            pthread_cond_wait(&SELF->bin_cond, &SELF->bin_mtx);
        }
        pthread_mutex_unlock(&SELF->bin_mtx);
    }
}

static void do_mtb_dump(T *thiz)
{
    int r;
    ftbset_t *fsets = thiz->fsets[0];
    mtb_t *mtb;
    ftb_t *ftb = NULL;

    mtb = SELF->imq->top(SELF->imq);

    ftb = ftb_create(NULL);
    ftb->conf = thiz->conf;
    DATA_FILE(ftb->file, thiz->conf, next_fnum(thiz), 0);

    r = ftb->init(ftb);
    if (r != 0) goto _out;

    INFO("store %s via %s", ftb->file, mtb->file);
    r = ftb->store(ftb, mtb);
    if (r != 0) goto _out;

    r = fsets->push(fsets, ftb);
    if (r != 0) goto _out;

    SELF->imq->pop(SELF->imq);
    mtb->clean(mtb);
    mtb->destroy(mtb);

    notify(thiz, NTF_PROT);
    notify(thiz, NTF_COMPACT);

_out:
    if (r != 0 && ftb != NULL) {
        ftb->clean(ftb);
        ftb->destroy(ftb);
    }

    return;
}

static void ntf_mtb_dump(T *thiz)
{
    while (1) {
        if (SELF->notice & NTF_DBDESTROY) break;

        pthread_mutex_lock(&SELF->dump_mtx);
        while (1) {
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
    job->cpct->compact(job->cpct);

    pthread_mutex_lock(&job->db->pri->compact_mtx);
    ATOMIC_DETACH_FLAG(job->db->pri->flag, job->cpct->type);
    pthread_mutex_unlock(&job->db->pri->compact_mtx);

    notify(job->db, NTF_COMPACT);
    job->cpct->destroy(job->cpct);
}

static int raise_cpct_major(T *thiz)
{
    int r = 0, i;
    struct cpct_job_s *job;
    compactor_t *cpct;
    ftbset_t *src_fset, *dst_fset;
    ftb_t *ftb;

    if (SELF->flag & (CPCT_SPLIT | CPCT_REMOTE | CPCT_MAJOR)) return 0;

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

static int raise_cpct_split(T *thiz)
{
    int r = 0, i;
    struct cpct_job_s *job;
    compactor_t *cpct;
    ftbset_t *fset;
    ftb_t *ftb;

    if (SELF->flag & (CPCT_REMOTE | CPCT_MAJOR | CPCT_SPLIT)) return 0;

    for (i = 1; i < thiz->conf->db_level; i++) {
        fset = thiz->fsets[i];

        ftb = fset->search_cpct_tb(fset, CPCT_SPLIT);
        if (ftb == NULL) continue;

        TRACE("%s", __func__);
        cpct = compactor_create(NULL);

        cpct->type = CPCT_SPLIT;
        cpct->nfnum1 = next_fnum(thiz);
        cpct->nfnum2 = next_fnum(thiz);

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

static int raise_cpct_L0(T *thiz)
{
    if (SELF->flag & CPCT_L0) return 0;

    /* TODO!! */
    return 0;
}

static int raise_cpct_remote(T *thiz)
{
    if (SELF->flag & (CPCT_SPLIT | CPCT_REMOTE | CPCT_MAJOR)) return 0;

    /* TODO!! */
    return 0;
}

static void raise_compact(T *thiz)
{
    int r;

    r = raise_cpct_L0(thiz);
    if (r) SELF->flag |= CPCT_L0; 

    r = raise_cpct_split(thiz);
    if (r) SELF->flag |= CPCT_SPLIT; 

    r = raise_cpct_major(thiz);
    if (r) SELF->flag |= CPCT_MAJOR; 

    r = raise_cpct_remote(thiz);
    if (r) SELF->flag |= CPCT_REMOTE; 
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

static dbit_impl_t *get_iter(T *thiz, mkey_t *start, mkey_t *stop)
{
    dbit_impl_t *it;
    
    it = dbit_impl_create(NULL);
    it->container = thiz;

    if (start != NULL) {
        it->start.len = start->len;
        it->start.data = PALLOC(it->super.mpool, start->len);
        memcpy(it->start.data, start->data, start->len);
    }

    if (stop != NULL) {
        it->stop.len = stop->len;
        it->stop.data = PALLOC(it->super.mpool, stop->len);
        memcpy(it->stop.data, stop->data, stop->len);
    }

    pthread_mutex_lock(&SELF->rw_lock);
    it->mmtb = SELF->mmtb;
    it->imq = SELF->imq;
    it->ftb = thiz->fsets[0]->tail(thiz->fsets[0]);
    it->init(it);
    pthread_mutex_unlock(&SELF->rw_lock);

    return it;
}

/****************************************
** basic function
*****************************************/
static int init(T *thiz)
{
    int i;

    thiz->fsets = PALLOC(SUPER->mpool, sizeof(ftbset_t *) * thiz->conf->db_level);

    for (i = 0; i < thiz->conf->db_level; i++) {
        thiz->fsets[i] = ftbset_create(SUPER->mpool);
        if (i > 0) {
            thiz->fsets[i]->flag |= FSET_FLG_ORDERED;
        }
    }

    return 0;
}

static int   db_open(T *thiz, const char *path, const char *conf_str)
{
    int r;

    if (strlen(path) + 64 > G_MEM_MID) {
        ERROR("len of file name too long");
        return -1;
    }

    SELF->path = PALLOC(SUPER->mpool, strlen(path) + 1);
    strcpy(SELF->path, path);

    r = config(thiz, conf_str);
    if (r != 0) goto _out;

    r = init(thiz);
    if (r != 0) goto _out;

    r = prepare_dirs(thiz);
    if (r != 0) goto _out;

    r = prepare_files(thiz);
    if (r != 0) goto _out;

#if 0   /* TODO!! stop other process accessing */
    r = lock_db(thiz);
    if (r != 0) goto _out;
#endif

    r = recover(thiz);
    if (r != 0) goto _out;

    r = cleanup(thiz);
    if (r != 0) goto _out;
   
    if (SELF->mmtb == NULL) {
        r = prepare_mtb(thiz);
        if (r != 0) goto _out;
    }

#if 0  /* call it manually: start thread pools */
    r = prepare_run(thiz);
    if (r != 0) goto _out;
#endif

_out:
    return r;
}

static void  db_close(T *thiz)
{
    db_flush(thiz);
}

static void destroy(T *thiz)
{
    int i;
    ftbset_t *fst;

    notify(thiz, NTF_DBDESTROY);
    thpool_destroy(SELF->thpool);

    for (i = 0; i < thiz->conf->db_level; i++) {
        fst = thiz->fsets[i];
        fst->destroy(fst);
    }

    SELF->imq->destroy(SELF->imq);
    SELF->mmtb->destroy(SELF->mmtb);

    del_obj(thiz);
}

static int _init(T *thiz)
{
    pthread_mutex_init(&SELF->rw_lock, NULL);

    pthread_mutex_init(&SELF->compact_mtx, NULL);
    pthread_cond_init(&SELF->compact_cond, NULL);

    pthread_mutex_init(&SELF->bin_mtx, NULL);
    pthread_cond_init(&SELF->bin_cond, NULL);

    pthread_mutex_init(&SELF->dump_mtx, NULL);
    pthread_cond_init(&SELF->dump_cond, NULL);

    pthread_mutex_init(&SELF->major_mtx, NULL);
    pthread_cond_init(&SELF->major_cond, NULL);

    pthread_mutex_init(&SELF->flush_mtx, NULL);
    pthread_cond_init(&SELF->flush_cond, NULL);

    pthread_mutex_init(&SELF->prot_mtx, NULL);
    pthread_cond_init(&SELF->prot_cond, NULL);

    ADD_METHOD(destroy);
    ADD_METHOD(config);
    ADD_METHOD(put);
    ADD_METHOD(del);
    ADD_METHOD(get);
    ADD_METHOD(mput);
    ADD_METHOD(mdel);
    ADD_METHOD(pdel);
    ADD_METHOD(get_iter);
    ADD_METHOD(recover);
    ADD_METHOD(repaire);
    thiz->open = db_open;
    thiz->close = db_close;
    thiz->flush = db_flush;
    thiz->prepare = prepare_run;

    SELF->imq = mtbset_create(SUPER->mpool);

    return 0;
}

T *dbimpl_create(pool_t *mpool)
{
     T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
     if (thiz == NULL) return NULL;

     SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

     _init(thiz);

     return thiz;           
}


