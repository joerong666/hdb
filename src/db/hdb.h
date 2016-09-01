#ifndef _HDB_H_
#define _HDB_H_

#define DB_PREFIX(_m) hdb_

typedef struct DB_PREFIX(iter_s) DB_PREFIX(iter_t);

typedef int (*KFILTER)(char *k, size_t ks);
typedef int (*VFILTER)(const char *v, size_t vs, int *expire);

/**********************************************
 * Basic
 **********************************************/
DB_PREFIX(t) *DB_PREFIX(db)open(const char *path);

void DB_PREFIX(close)(DB_PREFIX(t) *db);

/**********************************************
 *  DB Operation
 **********************************************/
int DB_PREFIX(put)(DB_PREFIX(t) *db, char *k, size_t ks, char *v, size_t vs);

int DB_PREFIX(del)(DB_PREFIX(t) *db, char *k, size_t ks);

ssize_t DB_PREFIX(get)(DB_PREFIX(t) *db, char *k, size_t ks, char **v);

/**********************************************
 * Filter
 **********************************************/
void DB_PREFIX(set_kfilter)(DB_PREFIX(t) *db, KFILTER flt);

void DB_PREFIX(set_vfilter)(DB_PREFIX(t) *db, VFILTER flt);

/**********************************************
 * Iterator
 **********************************************/
DB_PREFIX(iter_t) *DB_PREFIX(create_it)(DB_PREFIX(t) *db, int flag);

void DB_PREFIX(destroy_it)(DB_PREFIX(iter_t) *it);

int DB_PREFIX(it_hasnext)(DB_PREFIX(iter_t) *it);

ssize_t DB_PREFIX(it_next)(DB_PREFIX(iter_t) *it, char **k, size_t *ks, char **v);

/**********************************************
 * Util
 **********************************************/
void DB_PREFIX(state)(DB_PREFIX(t) *db);

#endif


