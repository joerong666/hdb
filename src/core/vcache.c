#include "inc.h"
#include "vcache.h"

#define T  vcache_t 

typedef struct vnode_s {  
    char *id;
    void *data;  
    struct vnode_s *next;  
} vnode_t;  
  
struct vcache_pri {
    int size;
    vnode_t **tb;
};

static void evict_item(T *thiz)
{
    int i, pos;
    vnode_t *it, *n;
    long int m = random();

    i = pos = m % thiz->cap;
    it = SELF->tb[pos];
    if (it != NULL) goto _out;

    for (i = 0; i < pos && it == NULL; i++) {
        it = SELF->tb[i];
        if (it != NULL) goto _out;
    }

    for (i = pos; i < thiz->cap && it == NULL; i++) {
        it = SELF->tb[i];
        if (it != NULL) goto _out;
    }

_out:
    while (it != NULL) {
        n = it->next;

        MY_Free(it->id);
        MY_Free(it->data);
        MY_Free(it);

        SELF->size--;
        it = n;
    }
    SELF->tb[i] = NULL;
}

int push(T *thiz, char *id, void *data)  
{
    int i;
    size_t pos = JSHash(id, strlen(id)) % thiz->cap;  
    vnode_t *head = SELF->tb[pos];
    vnode_t *it, *n;
  
    if (head == NULL) {
        vnode_t *n = (vnode_t *)MY_Malloc(sizeof(*n));  
        n->id   = MY_Strdup(id);
        n->data = data;
        n->next = NULL;

        SELF->tb[pos] = n;
        SELF->size++;
        
        return 0;
    }
    
    it = head;  
    if (strcmp(it->id, id) == 0) return 1;

    for (i = 0; it->next; i++) {
        it = it->next;
        if (strcmp(it->id, id) == 0) return 1;
    }
  
    n = (vnode_t *)MY_Malloc(sizeof(*n));  
    n->id   = MY_Strdup(id);
    n->data = data;
    n->next = NULL;

    it->next = n;
    SELF->size++;

    if (i >= 5) { /* limit 5 item for performance */
        n = head;
        SELF->tb[pos] = head->next;

        MY_Free(n->id);
        MY_Free(n->data);
        MY_Free(n);

        SELF->size--;
    }

    if (SELF->size > thiz->cap) {
        evict_item(thiz);
    }

    return 0;
}  

void *get(T *thiz, char *id)
{  
    size_t pos = JSHash(id, strlen(id)) % thiz->cap;  

    if (SELF->tb[pos]) {  
        vnode_t *it = SELF->tb[pos];  

        while (it) {  
            if (strcmp(it->id, id) == 0)  return it->data;

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

            MY_Free(t->id);
            MY_Free(t->data);
            MY_Free(t);

            t = n;
        }
    }
}

static void destroy(T *thiz)
{
    del_obj(thiz);
}

static int init(T *thiz)
{
    RWLOCK_INIT(&thiz->lock);
    SELF->tb = (vnode_t **)PCALLOC(SUPER->mpool, sizeof(vnode_t *) * thiz->cap);
    return 0;
}

static int _init(T *thiz)
{
    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(release);
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
