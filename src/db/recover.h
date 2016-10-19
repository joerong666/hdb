#ifndef _HDB_RECOVER_H_
#define _HDB_RECOVER_H_

#define T dbimpl_t

int HI_PREFIX(prepare_dirs)(T *thiz);

int HI_PREFIX(prepare_files)(T *thiz);

int HI_PREFIX(recover)(T *thiz);

int HI_PREFIX(cleanup)(T *thiz);

int HI_PREFIX(repaire)(T *thiz);

#undef T
#endif
