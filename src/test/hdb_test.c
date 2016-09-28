#include <stdio.h>
#include <time.h>
#include <sys/types.h>
#include <string.h>
#include <stdlib.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <pthread.h>

#if 0
#include "db/include/db.h"
#endif
#include "build/include/db.h"

#define UNUSED(_x) ((void)(_x))
#define gettid() syscall(__NR_gettid)

#define PRINT(_f, ...)  fprintf(_f, "%zu tid-%zd %s:%d, ", time(NULL), gettid(), __FILE__, __LINE__); \
                        fprintf(_f, __VA_ARGS__); fprintf(_f, "\r\n")

#ifdef FOR_UNIT_TEST
#define TRACE(...)  PRINT(stderr, __VA_ARGS__) 
#define DEBUG(...)  
#define INFO(...)   PRINT(stderr, __VA_ARGS__)
#define WARN(...)   PRINT(stderr, __VA_ARGS__)
#define ERROR(...)  PRINT(stderr, __VA_ARGS__)
#define FATAL(...)  PRINT(stderr, __VA_ARGS__)
#define PROMPT(...) PRINT(stdout, __VA_ARGS__)
#else
#    include "log.h"
#    define TRACE  log_debug
#    define DEBUG  log_debug
#    define INFO   log_info
#    define WARN   log_warn
#    define ERROR  log_error
#    define FATAL  log_fatal
#    define PROMPT log_prompt
#endif

#if defined(USE_ZMALLOC)
#   include "zmalloc.h"
#   define MY_Malloc zmalloc
#   define MY_Free(_p)   do {           \
       if (_p) {                        \
           zfree(_p);                   \
       }                                \
      /* _p = NULL;*/                   \
    } while(0)
#else
#   define MY_Malloc malloc             
#   define MY_Free(_p)   do {           \
           if (_p) {                    \
                free(_p);               \
           }                            \
          /* _p = NULL;*/               \
    } while(0)
#endif

#define T db_t

#define TEST_START() \
    int _l;                              \
    PRINT(stdout, "%s start", __func__); \
    for (_l = 0; _l < loop; _l++) {    

#define TEST_FIN() \
    }               \
    PRINT(stdout, "%s finish, press any key to continue", __func__);

enum l_case_e {
    CASE_PUT  = 1,
    CASE_MPUT = 1 << 1,
    CASE_DEL  = 1 << 2,
    CASE_MDEL = 1 << 3,
    CASE_PDEL = 1 << 4,
    CASE_ITER = 1 << 5,
    CASE_GET  = 1 << 6,
    CASE_PGET = 1 << 7,
    CASE_PUT_GET = 1 << 8,
    CASE_PUT_PGET = 1 << 9,
    CASE_MPUT_PGET = 1 << 10,
    CASE_SINGLE_PGET = 1 << 11,
    CASE_SINGLE_GET = 1 << 12,
    CASE_RANDOM_GET = 1 << 13,
};

static int l_tcase = 0, loop = 1, dbver = 1;
static size_t l_ksize = 32, l_vsize = 256, l_kvcnt = 100000;
static char kpattern[512], vpattern[40960], l_prefix[512];

static void test_init(T *db)
{
    int i;

    for (i = 0; i < l_ksize; i++) {
        kpattern[i] = 'k';
    }
    kpattern[i] = '\0';

    for (i = 0; i < l_vsize; i++) {
        vpattern[i] = 'v';
    }
    vpattern[i] = '\0';
}

static int test_db_put(T *db)
{
    int r = 0, i, ks, vs;
    char *kdata, *vdata;

    TEST_START()
    for (i = 0; i < l_kvcnt; i++) {
        kdata = MY_Malloc(l_ksize + 32);
        vdata = MY_Malloc(l_vsize + 32);

        sprintf(kdata, "%s-%d", kpattern, i);
        sprintf(vdata, "%s-%d", vpattern, i);

        ks = strlen(kdata);
        vs = strlen(vdata);

        if (dbver == 1) {
#if 0
            r = db_put(db, kdata, ks, vdata, vs);
#endif
        } else {
            r = HIDB2(db_put)(db, kdata, ks, vdata, vs);
        }

        if (r != 0) return r;
    }

    TEST_FIN()

    return 0;
}

static int test_db_mput(T *db)
{
    int r = 0, i, ks, vs, ks2, vs2;
    char *kdata, *vdata, *kdata2, *vdata2;
    kvec_t *kvs;

    TEST_START()
    for (i = 0; i < l_kvcnt; i++) {
        kvs = MY_Malloc(sizeof(kvec_t) * 2);

        kdata  = MY_Malloc(l_ksize + 32);
        vdata  = MY_Malloc(l_vsize + 32);
        kdata2 = MY_Malloc(l_ksize + 32);
        vdata2 = MY_Malloc(l_vsize + 32);

        sprintf(kdata, "%s-%d", kpattern, i);
        sprintf(vdata, "%s-%d", vpattern, i);

        sprintf(kdata2, "%s-%d", kpattern, i + l_kvcnt);
        sprintf(vdata2, "%s-%d", vpattern, i + l_kvcnt);

        ks = strlen(kdata);
        vs = strlen(vdata);

        ks2 = strlen(kdata2);
        vs2 = strlen(vdata2);

        kvs[0].k  = kdata;
        kvs[0].v  = vdata;
        kvs[0].ks = ks;
        kvs[0].vs = vs;

        kvs[1].k  = kdata2;
        kvs[1].v  = vdata2;
        kvs[1].ks = ks2;
        kvs[1].vs = vs2;

        r = HIDB2(db_mput)(db, kvs, 2);
        if (r != 0) return r;
    }

    TEST_FIN()

    return 0;
}

static int test_db_del(T *db)
{
    int r = 0, i, ks, vs;
    char kdata[256], *vdata;

    TEST_START()
    for (i = 0; i < l_kvcnt; i++) {
        sprintf(kdata, "%s-%d", kpattern, i);

        ks = strlen(kdata);

        if (dbver == 1) {
#if 0
            r = db_del(db, kdata, ks);
#endif
        } else {
            r = HIDB2(db_del)(db, kdata, ks);
        }

        if (r != 0) return r;
    }

    TEST_FIN()

    return 0;
}

static int test_db_pdel(T *db)
{
    int r = 0;

    TEST_START()
    r = HIDB2(db_pdel)(db, l_prefix, strlen(l_prefix));
    if (r != 0) return r;

    TEST_FIN()

    return 0;
}

static int test_db_mdel(T *db)
{
    int r = 0, i, ks, ks2;
    char *kdata, *kdata2;
    kvec_t *kvs;

    TEST_START()
    for (i = 0; i < l_kvcnt; i++) {
        kvs = MY_Malloc(sizeof(kvec_t) * 2);

        kdata  = MY_Malloc(l_ksize + 32);
        kdata2 = MY_Malloc(l_ksize + 32);

        sprintf(kdata, "%s-%d", kpattern, i);

        sprintf(kdata2, "%s-%d", kpattern, i + l_kvcnt);

        ks = strlen(kdata);

        ks2 = strlen(kdata2);

        kvs[0].k  = kdata;
        kvs[0].ks = ks;

        kvs[1].k  = kdata2;
        kvs[1].ks = ks2;

        r = HIDB2(db_mdel)(db, kvs, 2);
        if (r != 0) return r;
    }

    TEST_FIN()

    return 0;
}

static int test_db_get(T *db)
{
    int i, r, ks, vs;
    char *kdata, *vdata, *vdp;
    
    kdata = MY_Malloc(l_ksize + 32);
    vdata = MY_Malloc(l_vsize + 32);

    TEST_START();
    for (i = 0; i < l_kvcnt; i++) {
        sprintf(kdata, "%s-%d", kpattern, i);
        ks = strlen(kdata);

        if (dbver == 1) {
#if 0
            r = db_get(db, kdata, ks, &vdp, &vs, NULL);
#endif
        } else {
            r = HIDB2(db_get)(db, kdata, ks, &vdp, &vs, NULL);
        }

        if (r == 0) {
            sprintf(vdata, "%s-%d", vpattern, i);
            r = memcmp(vdata, vdp, vs);
            if (r != 0) {
                ERROR("i=%d, vlen=%d, vdata=%.*s", i, vs, vs, vdp);
            }

            MY_Free(vdp);
        } else {
            ERROR("%s not found or error, r=%d", kdata, r);
        }
    }

    TEST_FIN()

    MY_Free(kdata);
    MY_Free(vdata);
    return 0;
}

static int test_db_random_get(T *db)
{
    int i, j, r, ks, vs;
    char *kdata, *vdp;
    struct timeval tv;
    
    kdata = MY_Malloc(l_ksize + 32);

    TEST_START();
    for (i = 0, j = 0; j < l_kvcnt; j++) {
        gettimeofday(&tv, NULL);
        srandom(tv.tv_usec);
		i = (int)((random() / (RAND_MAX + 1.0)) * l_kvcnt);

        sprintf(kdata, "%s-%d", kpattern, i);
        ks = strlen(kdata);

        if (dbver == 1) {
#if 0
            r = db_get(db, kdata, ks, &vdp, &vs, NULL);
#endif
        } else {
            r = HIDB2(db_get)(db, kdata, ks, &vdp, &vs, NULL);
        }

        if (r == 0) {
            WARN("i=%d, vlen=%d, vdata=%.*s", i, vs, vs, vdp);
            MY_Free(vdp);
        } else {
            ERROR("%s not found or error, r=%d", kdata, r);
        }
    }

    TEST_FIN()

    MY_Free(kdata);
    return 0;
}

static int test_db_iter(T *db)
{
    int r, ks, vs, cnt = 0;
    char *k, *v, *kt, *vt;
    iter_t *it;

    TEST_START();

    if (dbver == 1) {
#if 0
        it = db_create_it(db, 15);
#endif
    } else {
        it = HIDB2(db_create_it)(db, 15);
    }

    while (1) {
        if (dbver == 1) {
#if 0
            r = db_iter(it, &k, &ks, &v, &vs, NULL);
#endif
        } else {
            r = HIDB2(db_iter)(it, &k, &ks, &v, &vs, NULL);
        }

        if (r != 0) break;

        kt = strchr(k, '-');
        vt = strchr(v, '-');
        if ((ks - (kt - k) != vs - (vt - v))) {
            ERROR("ks=%d  vs=%d  %.*s  %.*s", ks, vs, ks, k, vs, v);
            goto _next;
        }

        if (memcmp(kt, vt, ks - (kt - k)) != 0) {
            ERROR("ks=%d  vs=%d  %.*s  %.*s", ks, vs, ks, k, vs, v);
            goto _next;
        }

        WARN("ks=%d  vs=%d  %.*s  %.*s", ks, vs, ks, k, vs, v);
_next:
        cnt++;
        MY_Free(k);
        MY_Free(v);
    }

    if (dbver == 1) {
#if 0
        db_destroy_it(it);
#endif
    } else {
        HIDB2(db_destroy_it)(it);
    }

    PRINT(stdout, "iterate cnt=%d", cnt);
    TEST_FIN()

    return 0;
}

static int test_db_pget(T *db)
{
    int ks, vs;
    char *k, *v, *kt, *vt;
    iter_t *it;

    TEST_START();

    it = HIDB2(db_pget)(db, l_prefix, strlen(l_prefix));

    while (HIDB2(db_iter)(it, &k, &ks, &v, &vs, NULL) == 0) {
        kt = strchr(k, '-');
        vt = strchr(v, '-');

        if ((ks - (kt - k) != vs - (vt - v))) {
            INFO("err: ks=%d  vs=%d  %.*s  %.*s", ks, vs, ks, k, vs, v);
            goto _next;
        }

        if (memcmp(kt, vt, ks - (kt - k)) != 0) {
            INFO("err: ks=%d  vs=%d  %.*s  %.*s", ks, vs, ks, k, vs, v);
            goto _next;
        }

        WARN("ks=%d  vs=%d  %.*s %.*s", ks, vs, ks, k, vs, v);

_next:
        MY_Free(k);
        MY_Free(v);
    }

    HIDB2(db_destroy_it)(it);
    TEST_FIN()

    return 0;
}

static int test_db_single_pget(T *db)
{
    int i, r, ks, vs;
    char *kdata, *vdata, *vdp, *kdp;
    iter_t *it;
    
    kdata = MY_Malloc(l_ksize + 32);
    vdata = MY_Malloc(l_vsize + 32);

    TEST_START();
    for (i = 0; i < l_kvcnt; i++) {
        sprintf(kdata, "%s-%d", kpattern, i);
        ks = strlen(kdata);

        if (dbver == 1) {
#if 0
            r = db_get(db, kdata, ks, &vdp, &vs, NULL);
#endif
        } else {
            it = HIDB2(db_pget)(db, kdata, ks);
            r = HIDB2(db_iter)(it, &kdp, &ks, &vdp, &vs, NULL);
        }

        if (r == 0) {
            sprintf(vdata, "%s-%d", vpattern, i);
            r = memcmp(vdata, vdp, vs);
            if (r != 0) {
                ERROR("i=%d, vlen=%d, vdata=%.*s", i, vs, vs, vdp);
            }

            MY_Free(kdp);
            MY_Free(vdp);
        } else {
            WARN("%s not found or error, r=%d", kdata, r);
        }

        if (dbver != 1) {
            HIDB2(db_destroy_it)(it);
        }
    }

    TEST_FIN()

    MY_Free(kdata);
    MY_Free(vdata);
    return 0;
}

static int test_db_single_get(T *db)
{
    int r, ks, vs;
    char *kdata, *vdp, *kdp;
    
    kdata = MY_Malloc(l_ksize + 32);

    TEST_START();
    sprintf(kdata, "%s", l_prefix);
    ks = strlen(kdata);

    if (dbver == 1) {
#if 0
        r = db_get(db, kdata, ks, &vdp, &vs, NULL);
#endif
    } else {
        r = HIDB2(db_get)(db, kdata, ks, &vdp, &vs, NULL);
    }

    if (r == 0) {
        MY_Free(vdp);
        WARN("vs=%d, v=%.*s", vs, vdp);
    } else {
        WARN("%s not found or error, r=%d", kdata, r);
    }

    TEST_FIN()

    MY_Free(kdata);
    return 0;
}

#ifndef FOR_UNIT_TEST
static void init_log()
{
    static int g_log_level = PF_LOG_WARN;
    static pf_log_config_t g_log_cnf;

    pf_log_init_config(&g_log_cnf);
    strcpy(g_log_cnf.log_file, "log");
    g_log_cnf.log_level = g_log_level;
    pf_log_init(&g_log_cnf);
}
#endif

int main(int argc, char *argv[])
{
    T *db;
    char buf[32];
    char dbpath[256];

    UNUSED(argc);
    UNUSED(argv);

#ifndef FOR_UNIT_TEST
    init_log();
#endif 

    strcpy(dbpath, argv[1]);
    l_tcase = atoi(argv[2]);

    if (argc >= 4) {
        l_kvcnt = atoi(argv[3]);
    }

    if (argc >= 5) {
        strcpy(l_prefix, argv[4]);
    }

    if (argc >= 6) {
        loop = atoi(argv[5]);
    }

    dbver = dbe_version(dbpath);

    if (dbver == 1) {
#if 0
        db = db_open(dbpath, NULL);
#endif
    } else {
        db = HIDB2(db_open)(dbpath, NULL);
    }

    test_init(db);

    if (dbver == 1) {
#if 0
        db_run(db);
#endif
    } else {
        HIDB2(db_run)(db);
    }

    if (l_tcase & CASE_PUT) {
        test_db_put(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_MPUT) {
        test_db_mput(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_DEL) {
        test_db_del(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_PDEL) {
        test_db_pdel(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_MDEL) {
        test_db_mdel(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_ITER) {
        test_db_iter(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_GET) {
        test_db_get(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_SINGLE_GET) {
        test_db_single_get(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_RANDOM_GET) {
        test_db_random_get(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_PGET) {
        test_db_pget(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_SINGLE_PGET) {
        test_db_single_pget(db);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_PUT_GET) {
        pthread_t pth, gth;

        pthread_create(&pth, NULL, test_db_put, db);
        pthread_create(&gth, NULL, test_db_get, db);

        pthread_join(pth, NULL);
        pthread_join(gth, NULL);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_PUT_PGET) {
        pthread_t pth, gth;

        pthread_create(&pth, NULL, test_db_put, db);
        pthread_create(&gth, NULL, test_db_pget, db);

        pthread_join(pth, NULL);
        pthread_join(gth, NULL);

        read(1, buf, 1);
    }

    if (l_tcase & CASE_MPUT_PGET) {
        pthread_t pth, gth;

        pthread_create(&pth, NULL, test_db_mput, db);
        pthread_create(&gth, NULL, test_db_pget, db);

        pthread_join(pth, NULL);
        pthread_join(gth, NULL);

        read(1, buf, 1);
    }

    if (dbver == 1) {
#if 0
        db_close(db);
#endif
    } else {
        PRINT(stdout, "press any key to checkpoint db\n");
        read(1, buf, 1);
        HIDB2(db_checkpoint)(db);

        PRINT(stdout, "press any key to close db\n");
        read(1, buf, 1);

        HIDB2(db_close)(db);
    }

    PRINT(stdout, "press any key to exit\n");
    read(1, buf, 1);

    return 0;
}
