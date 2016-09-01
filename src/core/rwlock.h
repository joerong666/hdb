#ifndef _RWLOCK_H_
#define _RWLOCK_H_

typedef struct rwlock_s
{
    int r_waits;
    int w_waits;
    int refs;

    pthread_mutex_t mtx;
    pthread_cond_t r_cond;
    pthread_cond_t w_cond;
} rwlock_t;

int rwlock_mutex_init(rwlock_t *rw);

int rwlock_mutex_rdlock(rwlock_t *rw);
int rwlock_mutex_wrlock(rwlock_t *rw);
int rwlock_mutex_tryrdlock(rwlock_t *rw);
int rwlock_mutex_trywrlock(rwlock_t *rw);
int rwlock_mutex_unlock(rwlock_t *rw);

#if 0
int rwlock_spin_rdlock(rwlock_t *rw);
int rwlock_spin_wrlock(rwlock_t *rw);
int rwlock_spin_runlock(rwlock_t *rw);
int rwlock_spin_wunlock(rwlock_t *rw);
#endif

#endif
