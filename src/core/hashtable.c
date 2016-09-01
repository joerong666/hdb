#include "inc.h"
#include "hashtable.h"

typedef struct hnode_s {  
    void *data;  
    struct hnode_s *next;  
} hnode_t;  
  
struct hashtable_s {
    size_t cap;
    HFUNC hfunc;
    HCMP cmp;
    hnode_t **tb;
};

ht_t *ht_create(size_t cap, HFUNC hfunc, HCMP cmp)
{
    ht_t *h = MY_Calloc(sizeof(ht_t) + sizeof(hnode_t *) * cap);
    h->tb = (hnode_t **)(((char *)h) + sizeof(ht_t));

    h->cap = cap;
    h->hfunc = hfunc;
    h->cmp = cmp;

    return h;
}

void ht_destroy(ht_t *h)
{
    MY_Free(h);
}

int ht_push(ht_t *ht, void *data)  
{
    size_t pos = ht->hfunc(data) % ht->cap;  
    hnode_t *head = ht->tb[pos];
  
    if (head == NULL) {
        hnode_t *n = (hnode_t *)MY_Malloc(sizeof(*n));  
        n->data = data;
        n->next = NULL;

        ht->tb[pos] = n;
        
        return 0;
    } else if (ht->cmp(head->data, data) == 0) {  
        return 1;  
    }  

    hnode_t *it = head;  
    while (it->next) {  
        if (ht->cmp(it->data, data) == 0) {  
            return 1;  
        }  

        it = it->next;  
    }  
  
    hnode_t *n = (hnode_t *)MY_Malloc(sizeof(*n));  
    n->data = data;
    n->next = NULL;

    it->next = n;
    return 0;
}  

void *ht_find(ht_t *ht, void *data)
{  
    size_t pos = ht->hfunc(data) % ht->cap;  

    if (ht->tb[pos]) {  
        hnode_t *it = ht->tb[pos];  

        while (it) {  
            if (ht->cmp(it->data, data) == 0)  return it->data;

            it = it->next;  
        }  
    }  

    return NULL;  
}  
  
