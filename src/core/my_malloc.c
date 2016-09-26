#include <unistd.h>
#include <stdlib.h>
#include "my_malloc.h"

#ifndef USE_ZMALLOC

void *my_malloc(size_t sz)
{
    return malloc(sz);
}

void *my_calloc(size_t sz)
{
    return calloc(sz, 1);
}

void *my_realloc(void *p, size_t sz)
{
    return realloc(p, sz);
}

void my_free(void *p)
{
    if (p) free(p);
}

void *my_memalign(size_t sz)
{
    void *p;

    int r = posix_memalign(&p, getpagesize(), sz);
    if (r == -1) {                                         
        /*ERROR("memalign errno=%d", errno);*/
        return NULL;
    }

    return p;
}

void my_alignfree(void *p)
{
    if (p) free(p);
}

#else
#warning "USE ZMALLOC instead of malloc ..................."
#   include "zmalloc.h"

void *my_malloc(size_t sz)
{
    return zmalloc(sz);
}

void *my_calloc(size_t sz)
{
    return zcalloc(sz);
}

void *my_realloc(void *p, size_t sz)
{
    return zrealloc(p, sz);
}

void my_free(void *p)
{
    if (p) zfree(p);
}

void *my_memalign(size_t sz)
{
#if 1
    void *p;

    /* statistic seems not correct if use je_posix_memalign */
    int r = posix_memalign(&p, getpagesize(), sz);
    if (r == -1) {                                         
        /*ERROR("memalign errno=%d", errno);*/
        return NULL;
    }

    return p;
#else
    return zmalloc(sz);
#endif
}

void my_alignfree(void *p)
{
#if 1
    if (p) free(p);
#else
    if (p) zfree(p);
#endif
}

#endif
