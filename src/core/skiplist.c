#include    "inc.h"
#include    "skiplist.h"

#define T   skiplist_t

#define MAXLEVEL 32
#define SKIPLIST_P 0.25      /* Skiplist P = 1/4 */

#if 0
typedef struct skl_node_s {
	int lv;
    void *data;

    struct skl_lv {
        struct skl_node_s *fw;
    } lvs[1];
} skl_node;
#endif

struct skiter_pri {
    skl_node *cur_pos;    
};

struct skiplist_pri {
#if 0
    int lv;
    rwlock_t lock;

    skl_node *head;
#endif
};

static skl_node *create_node(T *thiz, int lv)
{
    skl_node *n = PALLOC(SUPER->mpool, sizeof(*n) + lv * sizeof(struct skl_lv));
    if (n == NULL) return NULL;

    n->lv = lv;
    n->data = NULL;

    int j;
    for(j = 0; j < lv; j++) {
        n->lvs[j].fw = NULL;
    }

    return n;
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
static int random_level(T *thiz)
{
    int lv = 1;
    while((random() & 0xFFFF) < (SKIPLIST_P * 0xFFFF)) {
        lv += 1;
    }

    return (lv < thiz->maxlv) ? lv : thiz->maxlv;
}

static int push_i(T *thiz, void *in_data, void **out_data, int safe)
{
	skl_node *update[thiz->maxlv], *x, *n;
	int r = 0, lv, i;
    void *od;

    if (safe) {
        RWLOCK_WRITE(&thiz->lock);
    }

	x = thiz->head;
	for (i = thiz->lv - 1; i >= 0; i--) {
		while (x->lvs[i].fw && thiz->cmp( x->lvs[i].fw->data, in_data) < 0) {
			x = x->lvs[i].fw;
		}

		update[i] = x;

		if(x->lvs[i].fw && thiz->cmp(x->lvs[i].fw->data, in_data) == 0) { 
            /* cannot dup, if should, you can use a list for data */
            od = x->lvs[i].fw->data;

            if (thiz->collide != NULL) {
                in_data = thiz->collide(in_data, od);
            }

            x->lvs[i].fw->data = in_data;

            if (out_data != NULL) *out_data = od;

			r = 1;
            goto _out;
		}
	}

    lv = random_level(thiz);
	n = create_node(thiz, lv);
    if (n == NULL) {
        r = -1;
        goto _out;
    }

    n->data = in_data;

	if (lv > thiz->lv) {
		for (i = thiz->lv; i < lv; i++) {
			update[i] = thiz->head;
		}

        thiz->lv = lv;
	}

	x = n;
	for (i = 0; i < lv; i++) {
		x->lvs[i].fw = update[i]->lvs[i].fw;

		update[i]->lvs[i].fw = x;
	}

_out:
    if (safe) {
        RWUNLOCK(&thiz->lock);
    }

	return r;
}

static int push(T *thiz, void *in_data, void **out_data)
{
    return push_i(thiz, in_data, out_data, 1);
}

static int push_unsafe(T *thiz, void *in_data, void **out_data)
{
    return push_i(thiz, in_data, out_data, 0);
}

static void *find(T *thiz, const void *data)
{
	int i, c = -1;
    void *r = NULL;
	skl_node *x;

    RWLOCK_READ(&thiz->lock);
	x = thiz->head;
	for (i = thiz->lv - 1; i >= 0; i--) {
		while (x->lvs[i].fw && (c = thiz->cmp(x->lvs[i].fw->data, data)) < 0) {
			x = x->lvs[i].fw;
		}

		if (c == 0 && x->lvs[i].fw) {
            r = x->lvs[i].fw->data;
            goto _out;
        }
	}

_out:
    RWUNLOCK(&thiz->lock);

	return r;
}

static int exist(T *thiz, const void *data)
{
    void *d;

    /* TODO!! check via bloom filter first */
    d = find(thiz, data);
    return (d != NULL);
}

static int empty(T *thiz)
{
    int r;

    RWLOCK_READ(&thiz->lock);
    r = (thiz->head->lvs[0].fw == NULL);
    RWUNLOCK(&thiz->lock);

	return r;
}

#if 0 /* no need */
static void remove_i(T *thiz, skl_node *x, skl_node **update)
{
	int i;

	for (i = 0; i < thiz->lv; i++) {
		if (update[i]->lvs[i].fw == x) {
			update[i]->lvs[i].fw = x->lvs[i].fw;
		}
	}

	while(thiz->lv > 1 && thiz->head->lvs[thiz->lv - 1].fw == NULL) {
		thiz->lv--;
	}
}

static void *remove(T *thiz, void *data)
{
	skl_node *update[thiz->maxlv], *x;
	int i, c;
    void *out_data = NULL;

    RWLOCK_WRITE(&thiz->lock);
	x = thiz->head;
	for (i = thiz->lv - 1; i >= 0; i--) {
		while (x->lvs[i].fw && thiz->cmp(x->lvs[i].fw->data, data) < 0) {
			x = x->lvs[i].fw;
		}

        update[i] = x;
	}

	x = x->lvs[0].fw;
	if (thiz->cmp(x->data, data) == 0) {
		remove_i(thiz, x, update);

        out_data = x->data;
        destroy_node(thiz, x);

		goto _out;
	}

_out:
    RWUNLOCK(&thiz->lock);

	return out_data;
}
#endif

static skiter_t *get_iter(T *thiz, void *start, void *stop, SKITERCMP cmp)
{
    skiter_t *it = skiter_create(NULL);
    it->container = thiz;
    it->start = start;
    it->stop = stop;
    it->cmp = cmp;

    it->init(it);

    return it;
}

/****************************************
** basic function
*****************************************/
static int init(T *thiz)
{
    if (thiz->maxlv <= 0 || thiz->maxlv > MAXLEVEL) thiz->maxlv = MAXLEVEL;

    thiz->head = create_node(thiz, thiz->maxlv);
    if (thiz->head == NULL) return -1;

    return 0;
}

void destroy_data(T *thiz)
{
    skl_node *it = thiz->head;

    while (it->lvs[0].fw) {
        thiz->dfree_func(it->lvs[0].fw->data);
        it = it->lvs[0].fw;
    }
}

void destroy(T *thiz)
{
    del_obj(thiz);
}

static int _init(T *thiz)
{
    thiz->lv = 1;
    RWLOCK_INIT(&thiz->lock);

    thiz->cmp = NULL;
    thiz->collide = NULL;

    ADD_METHOD(init);
    ADD_METHOD(destroy);
    ADD_METHOD(destroy_data);
    ADD_METHOD(push);
    ADD_METHOD(push_unsafe);
    ADD_METHOD(find);
    ADD_METHOD(exist);
    ADD_METHOD(empty);
    ADD_METHOD(get_iter);

    return 0;
}

T *skiplist_create(pool_t *mpool)
{
    T *thiz = new_obj(mpool, sizeof(*thiz) + sizeof(*SELF));
    if (thiz == NULL) return NULL;

    SELF = (typeof(SELF))((char *)thiz + sizeof(*thiz));

    if (_init(thiz) != 0) {
        del_obj(thiz);     
        return NULL;       
    }                      

    return thiz;           
}


