#ifndef _MY_MALLOC_H_
#define _MY_MALLOC_H_

void *my_malloc(size_t sz);
void *my_calloc(size_t sz);
void *my_realloc(void *p, size_t sz);
void my_free(void *p);
void *my_memalign(size_t sz);
void my_alignfree(void *p);

#endif
