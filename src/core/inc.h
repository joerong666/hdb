#ifndef _INC_H_
#define _INC_H_

#define _XOPEN_SOURCE 500
#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <inttypes.h>
#include <time.h>
#include <strings.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/syscall.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>
#include <libgen.h>
#include <dirent.h>
#include <fnmatch.h>
#include <limits.h>
#include <dirent.h>

#include "hash_func.h"
#include "list.h"
#include "palloc.h"
#include "obj.h"
#include "crc.h"
#include "buf.h"
#include "rwlock.h"
#include "kv.h"
#include "thpool.h"
#include "conf.h"
#include "my_malloc.h"

#define gettid() syscall(__NR_gettid)

#ifdef FOR_UNIT_TEST
#    define PRINT(_f, ...)  fprintf(_f, "%zu tid-%zd %s:%d, ", time(NULL), gettid(), __FILE__, __LINE__); \
                        fprintf(_f, __VA_ARGS__); fprintf(_f, "\r\n")
#    define TRACE(...)  PRINT(stderr, __VA_ARGS__) 
#    define DEBUG(...)  /* PRINT(stderr, __VA_ARGS__) */
#    define INFO(...)   PRINT(stderr, __VA_ARGS__)
#    define WARN(...)   PRINT(stderr, __VA_ARGS__)
#    define ERROR(...)  PRINT(stderr, __VA_ARGS__)
#    define FATAL(...)  PRINT(stderr, __VA_ARGS__)
#    define PROMPT(...) PRINT(stdout, __VA_ARGS__)
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

#define MY_Strdup my_strdup
#define MY_Malloc my_malloc
#define MY_Calloc my_calloc
#define MY_Realloc my_realloc
#define MY_Free my_free
#define MY_Memalign my_memalign
#define MY_AlignFree my_alignfree

#define POOL_CREATE     pool_create
#define POOL_DESTROY    pool_destroy
#define POOL_RESET      pool_reset
#define PALLOC          palloc
#define PCALLOC         pcalloc

#if 0
#define CONSOLE_DEBUG(...) do {             \
    fprintf(stdout, "%zd ", time(0));        \
    fprintf(stdout, __VA_ARGS__);           \
    fprintf(stdout, "\r\n");                \
} while(0)

#define SAY_DEBUG(...) do {                 \
    fprintf(stdout, "%zd ", time(0));        \
    fprintf(stdout, "press any key to ");   \
    fprintf(stdout, __VA_ARGS__);           \
    fprintf(stdout, "\r\n");                \
    getchar();                              \
} while(0)
#else
#define CONSOLE_DEBUG(...)
#define SAY_DEBUG(...) 
#endif

/* INPUT or OUTPUT arg identify */
#define IN 
#define OUT

/* _out label is used by ragel, use this for ELSE_OUT, GO_OUT macro */
#define OUT_LABEL _chk_out 

#define NOP TRACE("nothing to do!!")
#define RDONLY

#define SHOULD_NOT_REACH() ASSERT(0)

#ifndef UNUSED
#   define UNUSED(x) ((void)x)
#endif

#define ARR_LEN(_a) (sizeof(_a) / sizeof(typeof((_a)[0])))

#define ASSERT(expr) do {                                   \
    if(!(expr)) {                                           \
        FATAL("%s, %s, %d", #expr, __FILE__, __LINE__);     \
        abort();                                            \
    }                                                       \
}while(0)

#define NOUSED __attribute__((__unused__))
#define atomic_add_a(__x, _n)       __sync_fetch_and_add(&(__x), _n)
#define atomic_add_b(__x, _n)       __sync_add_and_fetch(&(__x), _n)
#define atomic_sub_a(__x, _n)       __sync_fetch_and_sub(&(__x), _n)
#define atomic_sub_b(__x, _n)       __sync_sub_and_fetch(&(__x), _n)
#define atomic_incr_a(__x)          __sync_fetch_and_add(&(__x), 1)
#define atomic_incr_b(__x)          __sync_add_and_fetch(&(__x), 1)
#define atomic_decr_a(__x)          __sync_fetch_and_sub(&(__x), 1)
#define atomic_decr_b(__x)          __sync_sub_and_fetch(&(__x), 1)
#define atomic_zero_a(__x)          __sync_fetch_and_and(&(__x), 0)
#define atomic_zero_b(__x)          __sync_and_and_fetch(&(__x), 0)
#define atomic_nzero_a(__x)         __sync_fetch_and_or(&(__x), 1)
#define atomic_nzero_b(__x)         __sync_or_and_fetch(&(__x), 1)
#define atomic_casv(__x, __val)     __sync_val_compare_and_swap(&(__x), __x, __val)
#define atomic_casb(__x, __val)     __sync_bool_compare_and_swap(&(__x), __x, __val)

#define ATOMIC_ATTACH_FLAG(_flg, _x) do {       \
    int _xflg = _flg;                           \
    _xflg |= (_x);                              \
    _xflg = atomic_casv(_flg, _xflg);           \
} while(0)

/* assign for avoiding warning */
#define ATOMIC_DETACH_FLAG(_flg, _x) do {       \
    int _xflg = _flg;                           \
    _xflg &= ~(_x);                             \
    _xflg = atomic_casv(_flg, _xflg);           \
} while(0)

#define ATOMIC_CHECK_FLAG(_flg, _x, _r) do {    \
    int _xflg = (_flg);                         \
    if ((_xflg) & _x) _r = 1;                   \
    else _r = 0;                                \
} while(0)

#define TIME_ADD_START(__tv)                    \
    struct timeval __ts, __te, __td;            \
    if((__tv) != NULL) {                        \
        gettimeofday(&__ts, NULL);              \
    }

#define TIME_ADD_FINISH(__tv)               \
    if((__tv) != NULL) {                    \
        gettimeofday(&__te, NULL);          \
        timersub(&__te, &__ts, &__td);      \
        timeradd(__tv, &__td, __tv);        \
    }

#define OP_START()                              \
    PROMPT("operation start[%s]", __func__);    \
    struct timeval __ots, __ote, __otd;         \
    gettimeofday(&__ots, NULL)

#define TV_TO_SEC(_tv)  ((_tv)->tv_sec + (1e-6 * (_tv)->tv_usec))
#define TV_SUB_SEC(_a, _b)  (((_a)->tv_sec - (_b)->tv_sec) + (1e-6 * ((_a)->tv_usec - (_b)->tv_usec)))
#define TV_SUB_MSEC(_a, _b)  (((_a)->tv_sec - (_b)->tv_sec) * 1e3 + (1e-3 * ((_a)->tv_usec - (_b)->tv_usec)))
#define TV_TO_MSEC(_tv)  ((_tv)->tv_sec * 1e3 + (1e-3 * (_tv)->tv_usec))
#define TV_SUB_USEC(_a, _b)  (((_a)->tv_sec - (_b)->tv_sec) * 1e6 + ((_a)->tv_usec - (_b)->tv_usec))

#define TV_SET_IF_ZERO(_t) do {                             \
    if ((_t)->tv_sec != 0 || (_t)->tv_usec != 0) break;     \
                                                            \
    gettimeofday(_t, NULL);                                 \
} while(0)

#define MY_INIT_LIST_HEAD(_l, _len)     INIT_LIST_HEAD(_l); (*_len) = 0

#define MY_LIST_DEL(_n, _len)                           \
        if ((*_len) > 0) {                              \
            list_del(_n);                               \
            (*(_len))--;                                \
        }

#define MY_LIST_ADD(_n, _l, _len)                       \
        list_add(_n, _l);                               \
        (*(_len))++

#define MY_LIST_ADD_TAIL(_n, _l, _len)                  \
        list_add_tail(_n, _l);                          \
        (*(_len))++

#define MY_LIST_MOVE(_n, _l, _olen, _nlen)              \
        if (!list_empty(_n)) {                          \
            list_move(_n, _l);                          \
            (*(_olen))--;                               \
            (*(_nlen))++;                               \
        }

#define MY_LIST_MOVE_TAIL(_n, _l, _olen, _nlen)         \
        if ((*_olen) > 0) {                             \
            list_move_tail(_n, _l);                     \
            (*(_olen))--;                               \
            (*(_nlen))++;                               \
        }

#define MY_LIST_SPLICE_INIT(_s, _d, _slen, _dlen)       \
        list_splice_init(_s, _d);                       \
        (*(_dlen)) = (*(_dlen)) + (*(_slen));           \
        (*(_slen)) = 0

#define MY_LIST_SPLICE_TAIL_INIT(_s, _d, _slen, _dlen)  \
        list_splice_tail_init(_s, _d);                  \
        (*(_dlen)) = (*(_dlen)) + (*(_slen));           \
        (*(_slen)) = 0

#define FD_INIT(_fd) _fd = -1       

#define FD_CLOSE(_fd)               \
        if (_fd >= 0) close(_fd);   \
        _fd = -1

#define RWLOCK_INIT(_lc) rwlock_mutex_init(_lc)
#define RWLOCK_READ(_lc) rwlock_mutex_rdlock(_lc)
#define RWLOCK_WRITE(_lc) rwlock_mutex_wrlock(_lc)
#define RWLOCK_TRY_READ(_lc) rwlock_mutex_tryrdlock(_lc)
#define RWLOCK_TRY_WRITE(_lc) rwlock_mutex_trywrlock(_lc)
#define RWUNLOCK(_lc) rwlock_mutex_unlock(_lc)

enum g_consts_e {
    G_IP_SIZE           = 128,
    G_MPOOL_SIZE        = 4 * 1024,
    G_IOBUF_SIZE        = 64 * 1024,
    G_BLOCK_SIZE        = 4 * 1024,
    G_MAX_MULTI_KEY     = 256,
    G_MAX_KEY_LEN       = 256,
    G_MEM_MINI          = 64,
    G_MEM_SML           = 256,
    G_MEM_MID           = 1024,
    G_MEM_BIG           = 4096,
    G_REDUNDENCY_SIZE   = 64,
    G_KSIZE_LIMIT       = 200,
};

enum bool_e {
    TRUE = 1,
    FALSE = 0
};

enum rc_code
{
    RC_MIN = -20, /* as a sentinel, don't use this as a meaningful value */

    RC_INTERNAL_ERR,

    RC_ERR = -1,        /*    other error */
    RC_OK = 0,            

    RC_FALSE = 0,            
    RC_TRUE  = 1,            

    RC_NOT_FOUND = 0,
    RC_FOUND = 1,   
    RC_EXIST = 2,   

    RC_MAX,        /* as a sentinel, don't use this as a meaningful value */
};

#endif
