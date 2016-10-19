#ifndef _HDB_OPERATION_H_
#define _HDB_OPERATION_H_

#define T dbimpl_t

int          HI_PREFIX(prepare_mtb)(T *thiz);

mtb_t       *HI_PREFIX(get_mmtb)(T *thiz);

void         HI_PREFIX(set_mmtb)(T *thiz, mtb_t *tb);

int          HI_PREFIX(put)(T *thiz, mkv_t *kv);

int          HI_PREFIX(del)(T *thiz, mkey_t *k);

int          HI_PREFIX(mput)(T *thiz, mkv_t *kvs, size_t cnt);

int          HI_PREFIX(mdel)(T *thiz, mkey_t *keys, size_t cnt);

dbit_impl_t *HI_PREFIX(pget)(T *thiz, mkey_t *prefix);

int          HI_PREFIX(pdel)(T *thiz, mkey_t *prefix);

int          HI_PREFIX(get)(T *thiz, mkey_t *k, mval_t *v);

int          HI_PREFIX(flush)(T *thiz);

int          HI_PREFIX(checkpoint)(T *thiz);

dbit_impl_t *HI_PREFIX(get_iter)(T *thiz, mkey_t *start, mkey_t *stop);

#undef T
#endif
