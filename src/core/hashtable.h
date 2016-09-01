#ifndef _HASHTABLE_H_
#define _HASHTABLE_H_

typedef struct hashtable_s ht_t;
typedef int (*HCMP)(void *old, void *new);
typedef unsigned int (*HFUNC)(void *data);

ht_t *ht_create(size_t cap, HFUNC hfunc, HCMP cmp);

void ht_destroy(ht_t *h);

int ht_push(ht_t *ht, void *data);

void *ht_find(ht_t *ht, void *data);


static inline unsigned int JSHash(char* str, unsigned int len)
{
   unsigned int hash = 1315423911;
   unsigned int i    = 0;

   for(i=0; i<len; str++, i++) {
      hash ^= ((hash<<5) + toupper(*str) + (hash>>2));
   }
   return hash;
}

#endif
