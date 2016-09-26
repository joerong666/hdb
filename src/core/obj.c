#include <stddef.h>
#include <stdint.h>

#include "palloc.h"
#include "obj.h"

#define G_MPOOL_SIZE 4096

void *new_obj(pool_t *mpool, size_t sz)
{
    pool_t *mp = mpool;
    obj_t *ob = NULL;

    if (mp == NULL) {
        mp = pool_create(G_MPOOL_SIZE);
        if (mp == NULL) {
            return NULL;
        }

        ob = (obj_t *)pcalloc(mp, sz);
        if (ob == NULL) {
            pool_destroy(mp);
            return NULL;
        }   

        ob->new_mp_flag = 1;
    } else {
        ob = pcalloc(mp, sz);
        if (ob == NULL) {
            return NULL;
        }   

        ob->new_mp_flag = 0;
    }   

    ob->mpool = mp; 
    return ob;
}

void del_obj(void *obj)
{
    obj_t *ob = (obj_t *)obj;
    if (!ob || !ob->mpool) return;

    if (ob->new_mp_flag) {
        pool_destroy(ob->mpool);
    } else {
        pfree(ob->mpool, obj);
    }  
}
