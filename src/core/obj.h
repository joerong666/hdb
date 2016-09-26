#ifndef _CORE_OBJ_H_
#define _CORE_OBJ_H_

#define THIZ(_t, _pri) (_t = (typeof(_t))((char *)(_pri) - sizeof(typeof(*_t))))
#define SELF (thiz->pri)
#define SUPER (&thiz->super)
#define ADD_METHOD(_m) (thiz->_m = _m)

typedef struct obj_s {
    char new_mp_flag;   /* do not use this field anywhere anytime!! */
    pool_t *mpool;
} obj_t;

/**
* @description: obj should be zeroed
* @mpool
* @sz: size of subject of obj_t
* ret: pointer to super obj(type of obj_t), convert it to real type
*/
void *new_obj(pool_t *mpool, size_t sz);

/**
* @obj: type of obj_t
*/
void del_obj(void *obj);

#endif
