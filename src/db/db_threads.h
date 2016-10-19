#ifndef _HDB_THREADS_H_
#define _HDB_THREADS_H_

#define T dbimpl_t

int  HI_PREFIX(prepare_thpool)(T *thiz);

void HI_PREFIX(notify)(T *thiz, int notice);

#undef T
#endif
