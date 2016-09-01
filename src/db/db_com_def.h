#ifndef _DB_COM_DEF_H_
#define _DB_COM_DEF_H_

#ifndef DB_KVFILTER
#define DB_KVFILTER
typedef int32_t (*DB_KFILTER)(char *);
typedef int32_t (*DB_VFILTER)(const char *, size_t, int *expire);
#endif

typedef struct
{
    size_t ks;
    size_t vs;
    char *v;
    char *k;
} kvec_t;

#endif /* _DB_COM_DEF_H_ */

