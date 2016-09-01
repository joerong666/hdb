#ifndef _CORE_HTABLE_H_
#define _CORE_HTABLE_H_

#include "skiplist.h"

#define T htable_t
typedef struct htable_s T;
typedef struct htiter_s htiter_t;

/* @n: new data
 * @o: old data
 */
typedef int (*HTCMP)(const void *new, const void *old);
typedef unsigned int (*HASHFUNC)(const void *data);
typedef int (*HTITERCMP)(void *data, void *start, void *stop);

enum htable_e {
    HTB_INS_EXIST = 1,
    HTB_ITER_LOCK,
};

struct htable_s {
    /* must be the first field */
    obj_t  super;

    /* public fields */
    int fd;
    int flag;
    int cap;    /* capacity */
    int tb_cnt; 
    rwlock_t lock;

    HTCMP cmp;
    HASHFUNC hfunc;
    void (*dfree_func)(void *d);
    skiplist_t **tbs;

    struct htable_pri *pri;

    /* methods */
    int   (*init)(T *thiz);
    void  (*destroy)(T *thiz);
    void  (*destroy_data)(T *thiz);

    /* if existed, return old data 
     * @ret: = 0 : insert succ, @out_data is set null
     *       < 0: insert fail
     *       HTB_INS_EXIST: @out_data placed the old data
     */
    int   (*push)(T *thiz, void *in_data, void **out_data);   
    int   (*push_unsafe)(T *thiz, void *in_data, void **out_data);   
    void *(*find)(T *thiz, const void *data);
    int   (*exist)(T *thiz, const void *data);
    int   (*empty)(T *thiz);   

    htiter_t *(*get_iter)(T *thiz, void *start, void *stop, HTITERCMP cmp);
};

struct htiter_s {
    obj_t  super;

    T *container;
    skiter_t **sub_its;
    void *start;
    void *stop;
    HTITERCMP cmp;

    struct htiter_pri *pri;

    int   (*init)(htiter_t *it);
    int   (*has_next)(htiter_t *it);
    int   (*get)(htiter_t *it, void **out_data);
    int   (*next)(htiter_t *it);
    void  (*destroy)(htiter_t *it);
};

T *htable_create(pool_t *mpool);
htiter_t *htiter_create(pool_t *mpool);

#undef T
#endif


#if 0   /* you may use these hash func */
  
unsigned int JSHash(char* str, unsigned int len)  
{  
   unsigned int hash = 1315423911;  
   unsigned int i    = 0;  
  
   for(i=0; i<len; str++, i++) {   
      hash ^= ((hash<<5) + toupper(*str) + (hash>>2));  
   }   
   return hash;  
}  

#endif
