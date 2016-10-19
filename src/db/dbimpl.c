#include "dbimpl_pri.h"
#include "db_threads.h"
#include "operation.h"
#include "recover.h"

#define T dbimpl_t

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

uint64_t HI_PREFIX(next_fnum)(T *thiz)
{
    atomic_incr_b(SELF->max_fnum);
    return SELF->max_fnum;
}

uint64_t HI_PREFIX(get_seq)(T *thiz)
{
    uint64_t seq = SELF->seq;

    if (seq == 0) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        seq = tv.tv_sec * 1e6 + tv.tv_usec;
        atomic_casv(SELF->seq, seq);
    }

    return seq;
}

static int prepare_run(T *thiz)
{
    int r;

    r = HI_PREFIX(prepare_thpool)(thiz);
    if (r != 0) goto _out;

    HI_PREFIX(notify)(thiz, NTF_TIMER);
    HI_PREFIX(notify)(thiz, NTF_WRITE_BIN);
    HI_PREFIX(notify)(thiz, NTF_MTB_DUMP);
    HI_PREFIX(notify)(thiz, NTF_COMPACT);

_out:
    return r;
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

    r = HI_PREFIX(prepare_dirs)(thiz);
    if (r != 0) goto _out;

    r = HI_PREFIX(prepare_files)(thiz);
    if (r != 0) goto _out;

#if 0   /* TODO!! stop other process accessing */
    r = lock_db(thiz);
    if (r != 0) goto _out;
#endif

    r = HI_PREFIX(recover)(thiz);
    if (r != 0) goto _out;

    r = HI_PREFIX(cleanup)(thiz);
    if (r != 0) goto _out;
   
    if (SELF->mmtb == NULL) {
        r = HI_PREFIX(prepare_mtb)(thiz);
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
    HI_PREFIX(flush)(thiz);
}

static void destroy(T *thiz)
{
    int i;
    ftbset_t *fst;

    SAY_DEBUG("flush db");
    HI_PREFIX(flush)(thiz);
    HI_PREFIX(notify)(thiz, NTF_DBDESTROY);

    SAY_DEBUG("destroy thread pool");
    thpool_destroy(SELF->thpool);

    SAY_DEBUG("destroy file que");
    for (i = 0; i < thiz->conf->db_level; i++) {
        fst = thiz->fsets[i];
        fst->destroy(fst);
    }

    SAY_DEBUG("destroy imq");
    SELF->imq->destroy(SELF->imq);

    SAY_DEBUG("destroy mmtb");
    SELF->mmtb->destroy(SELF->mmtb);

    SAY_DEBUG("destroy db");
    del_obj(thiz);
}

static int _init(T *thiz)
{
    RWLOCK_INIT(&SELF->lock);

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

    thiz->put        = HI_PREFIX(put);
    thiz->del        = HI_PREFIX(del);
    thiz->get        = HI_PREFIX(get);
    thiz->mput       = HI_PREFIX(mput);
    thiz->mdel       = HI_PREFIX(mdel);
    thiz->pget       = HI_PREFIX(pget);
    thiz->pdel       = HI_PREFIX(pdel);
    thiz->recover    = HI_PREFIX(recover);
    thiz->repaire    = HI_PREFIX(repaire);
    thiz->checkpoint = HI_PREFIX(checkpoint);
    thiz->flush      = HI_PREFIX(flush);
    thiz->get_iter   = HI_PREFIX(get_iter);

    thiz->open = db_open;
    thiz->close = db_close;
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


