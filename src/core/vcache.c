#include "inc.h"
#include "vcache.h"

#define T  vcache_t 

typedef struct vnode_s {  
    uint32_t id;
    void *data;  
    struct vnode_s *next;  
} vnode_t;  
  
struct vcache_pri {
    vnode_t **tb;
};

static void evict_item(T *thiz, int pos)
{
    int i = pos;
    vnode_t *it = NULL, *n;

    for (i = 0; i < pos; i++) {
        it = SELF->tb[i];
        if (it != NULL) goto _out;
    }

    for (i = pos; i < thiz->cap; i++) {
        it = SELF->tb[i];
        if (it != NULL) goto _out;
    }

_out:
    while (it != NULL) {
        n = it->next;

        MY_Free(it->data);
        MY_Free(it);

        thiz->size--;
        it = n;
    }
    SELF->tb[i] = NULL;
}

int push(T *thiz, uint32_t id, void *data)  
{
    int i = 0, pos;
    vnode_t *head, *it, *n;
  
    pos = id % thiz->cap;  
    head = SELF->tb[pos];

    if (head == NULL) {
        vnode_t *n = (vnode_t *)MY_Malloc(sizeof(*n));  
        n->id   = id;
        n->data = data;
        n->next = NULL;

        SELF->tb[pos] = n;
        thiz->size++;
        
        goto _out;
    }
    
    it = head;  
    if (it->id == id) return 1;

    for (i = 0; it->next; i++) {
        it = it->next;
        if (it->id == id) return 1;
    }
  
    n = (vnode_t *)MY_Malloc(sizeof(*n));  
    n->id   = id;
    n->data = data;
    n->next = NULL;

    it->next = n;
    thiz->size++;

_out:
    if (i >= 5) { /* limit 5 item for performance */
        n = head;
        SELF->tb[pos] = head->next;

        MY_Free(n->data);
        MY_Free(n);

        thiz->size--;
    }

    if (thiz->size > thiz->cap) {
        evict_item(thiz, pos);
    }

    return 0;
}  

void *get(T *thiz, uint32_t id)
{  
    size_t pos = id % thiz->cap;  

    if (SELF->tb[pos]) {  
        vnode_t *it = SELF->tb[pos];  

        while (it) {  
            if (it->id == id)  return it->data;

            it = it->next;  
        }  
    }  

    return NULL;  
}  
  
static void release(T *thiz)
{
    int i;
    vnode_t *n, *t;

    for (i = 0; i < thiz->cap; i++) {
        t = SELF->tb[i];
        if (t == NULL) continue;

        while (t != NULL) {
            n = t->next;

            MY_Free(t->data);
            MY_Free(t);

            t = n;
        }
    }
}

static void destroy(T *thiz)
{
    release(thiz);
    del_obj(thiz);
}

static int init(T *thiz)
{
    RWLOCK_INIT(&thiz->lock);
    thiz->size = 0;

    if (thiz->cap > 0) {
        SELF->tb = (vnode_t **)PCALLOC(SUPER->mpool, sizeof(vnode_t *) * thiz->cap);
    }

    return 0;
}

static int _init(T *thiz)
{
    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(push);
    ADD_METHOD(get);

    return 0;
}

T *vcache_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    _init(thiz);

    return thiz;           
}
