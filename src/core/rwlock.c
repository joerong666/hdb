#include <assert.h>
#include <pthread.h>

#include "rwlock.h"

int rwlock_mutex_init(rwlock_t *rw)
{
    rw->r_waits = 0;
    rw->w_waits = 0;
    rw->refs = 0;

    pthread_mutex_init(&rw->mtx, NULL);
    return 0;
}

int rwlock_mutex_rdlock(rwlock_t *rw)
{
    pthread_mutex_lock(&rw->mtx);
    rw->r_waits++;

#if 0   /* write lock first */
    while (rw->refs < 0 || rw->w_waits > 0) {
        pthread_cond_wait(&rw->r_cond, &rw->mtx);
    }
#else   /* read lock first */
    while (rw->refs < 0) {
        pthread_cond_wait(&rw->r_cond, &rw->mtx);
    }
#endif

    rw->refs++; /* got read lock */
    rw->r_waits--;
    pthread_mutex_unlock(&rw->mtx);

    return 0;
}

int rwlock_mutex_wrlock(rwlock_t *rw)
{
    pthread_mutex_lock(&rw->mtx);
    rw->w_waits++;

    while (rw->refs != 0) {
        pthread_cond_wait(&rw->w_cond, &rw->mtx);
    }

    rw->refs--; /* got write lock */
    rw->w_waits--;
    pthread_mutex_unlock(&rw->mtx);

    return 0;
}

int rwlock_mutex_tryrdlock(rwlock_t *rw)
{
    int r = 0;

    pthread_mutex_lock(&rw->mtx);

#if 0   /* write lock first */
    if (rw->refs < 0 || rw->w_waits > 0) {
        r = -1;
        goto _out;
    }
#else   /* read lock first */
    if (rw->refs < 0 || rw->w_waits > 0) {
        r = -1;
        goto _out;
    }
#endif

    rw->r_waits++;
    rw->refs++; /* got read lock */
    rw->r_waits--;

_out:
    pthread_mutex_unlock(&rw->mtx);

    return r;
}

int rwlock_mutex_trywrlock(rwlock_t *rw)
{
    int r = 0;

    pthread_mutex_lock(&rw->mtx);

    if (rw->refs != 0) {
        r = -1;
        goto _out;
    }

    rw->w_waits++;
    rw->refs--; /* got write lock */
    rw->w_waits--;

_out:
    pthread_mutex_unlock(&rw->mtx);

    return r;
}

int rwlock_mutex_unlock(rwlock_t *rw)
{
    pthread_mutex_lock(&rw->mtx);

    if (rw->refs < 0) {
        rw->refs++;
    } else if (rw->refs > 0) {
        rw->refs--;
    }

    assert(rw->refs >= 0);

    if (rw->w_waits > 0) {
        if (rw->refs == 0) {
            pthread_cond_signal(&rw->w_cond);
        }
    } else if (rw->r_waits > 0) {
        pthread_cond_signal(&rw->r_cond);
    }

    pthread_mutex_unlock(&rw->mtx);

    return 0;
}

#if 0
static inline void  
rwlock_init(struct rwlock *lock) {  
    lock->write = 0;  
    lock->read = 0;  
}  
  
static inline void  
rwlock_rlock(struct rwlock *lock) {  
    for (;;) {  
        // isuued a full memory barrier. This typically means that operations issued   
        // prior to the barrier are guaranteed to be performed before operations issued after the barrier.  
        while(lock->write) {  
            __sync_synchronize();  
        }  
        __sync_add_and_fetch(&lock->read,1);  
        // 在给nreaders + 1 之后再次检查是否有写入者，有的话此次读锁请求失败  
        if (lock->write) {  
            __sync_sub_and_fetch(&lock->read,1);  
        } else {  
            break;  
        }  
    }  
}  
  
static inline void  
rwlock_wlock(struct rwlock *lock) {  
    // 如果没有写者，__sync_lock_test_and_set会返回0，表示此次请求写锁成功；  
    // 否则表示有其它写者，则空转  
    while (__sync_lock_test_and_set(&lock->write,1)) {}  
    // 在开始写入之前发现有读者进入，则要等到前面的操作完成  
    while(lock->read) {  
        __sync_synchronize();  
    }  
}  
  
static inline void  
rwlock_wunlock(struct rwlock *lock) {  
    __sync_lock_release(&lock->write);  
}  
  
static inline void  
rwlock_runlock(struct rwlock *lock) {  
    __sync_sub_and_fetch(&lock->read,1);  
}  
#endif
