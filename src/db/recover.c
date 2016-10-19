#include "dbimpl_pri.h"
#include "recover.h"

#define T dbimpl_t

static char *g_dirs[] = { "", DB_DIR_DATA, DB_DIR_TMP, DB_DIR_BAK, 
                          DB_DIR_RECYCLE, DB_DIR_BIN
                        };

int HI_PREFIX(prepare_dirs)(T *thiz)
{
    char fname[G_MEM_MID];
    int r = 0, i;

    for (i = 0; i < (int)ARR_LEN(g_dirs); i++) {
        AB_PATH(fname, thiz->conf, g_dirs[i]);
        INFO("mkdir %s if not exist", fname);

        if (mkdir(fname, 0775) == -1 && errno != EEXIST) {
            r = -1;
            ERROR("mkdir %s, errno=%d", g_dirs[i], errno);
        }
    }

    return r;
}

int HI_PREFIX(prepare_files)(T *thiz)
{
    /* TODO!! create some file needed */ 
    UNUSED(thiz);
    return 0;
}

static int bl_flt(const struct dirent *ent)
{
    return (fnmatch("*"DB_BIN_EXT, ent->d_name, 0) == 0);
}

static int dt_flt_L0(const struct dirent *ent)
{
    return (fnmatch("*_0"DB_DATA_EXT, ent->d_name, 0) == 0);
}

static int dt_flt_L1(const struct dirent *ent)
{
    return (fnmatch("*_1"DB_DATA_EXT, ent->d_name, 0) == 0);
}

static int file_cmp(const void *a, const void *b)
{
    const struct dirent **ent_a = (const struct dirent **)a;
    const struct dirent **ent_b = (const struct dirent **)b;

    uint64_t an = strtoull((*ent_a)->d_name, NULL, 10);
    uint64_t bn = strtoull((*ent_b)->d_name, NULL, 10);

    if(an < bn) return +1;
    if(an > bn) return -1;

    return 0;
}

static int   recover_bl(T *thiz)
{
    int r = 0, m, n;
    uint64_t fnum = 0;
    char fname[G_MEM_MID];
    mtb_t *mtb;
    struct stat st;
    struct dirent **namelist = NULL, *ent;

    AB_PATH_BIN(fname, thiz->conf);
    m = n = scandir(fname, &namelist, bl_flt, file_cmp);
    if (n == -1) {
        ERROR("scandir %s, errno=%d", fname, errno);
        return -1;
    }    

    while (n > 0) { 
        ent = namelist[--n];
        
        r = sscanf(ent->d_name, "%ju", &fnum);
        if (r != 1) {
            ERROR("%s filename format incorrect, skip", ent->d_name);
            continue;
        }

        BIN_PATH(fname, thiz->conf, ent->d_name);
        stat(fname, &st);
        if (st.st_size <= 0) {
            ERROR("%s empty, remove", fname);
            remove(fname);
            continue;
        }

        if (SELF->max_fnum < fnum) SELF->max_fnum = fnum;

        mtb = mtb_create(NULL);
        mtb->conf = thiz->conf;
        BIN_PATH(mtb->file, thiz->conf, ent->d_name);

        mtb->init(mtb);
        r = mtb->restore(mtb);
        if (r != 0) {
            ERROR("restore %s fail, skip", mtb->file);
            mtb->destroy(mtb);
            continue;
        }

        if (SELF->mmtb != NULL) {
            SELF->imq->push(SELF->imq, SELF->mmtb);
        }

        SELF->mmtb = mtb;
    }

    if (namelist) {
        while(--m >= 0) {
            free(namelist[m]);
        }

        free(namelist);
    }

    if (SELF->mmtb != NULL && SELF->mmtb->full(SELF->mmtb)) {
        SELF->imq->push(SELF->imq, SELF->mmtb);
        SELF->mmtb = NULL;
    }

    return 0;
}

static int   recover_dt_Ln(T *thiz, int lv)
{
    int r = 0, level = 0, m, n;
    uint64_t fnum = 0;
    char fname[G_MEM_MID];
    struct stat st;
    struct dirent **namelist = NULL, *ent;
    ftb_t *ftb;
    ftbset_t *fset;

    AB_PATH_DATA(fname, thiz->conf);
    if (lv == 0) {
        m = n = scandir(fname, &namelist, dt_flt_L0, file_cmp);
    } else {
        m = n = scandir(fname, &namelist, dt_flt_L1, file_cmp);
    }

    if (n == -1) {
        ERROR("scandir %s, errno=%d", fname, errno);
        return -1;
    }    

    while (n > 0) { 
        ent = namelist[--n];

        /* file name format "fnum_level.ext", eg: 1_0.hdb, 2_1.hdb */
        r = sscanf(ent->d_name, "%ju_%d", &fnum, &level);
        if (r != 2) {
            ERROR("%s filename format incorrect, skip", ent->d_name);
            continue;
        }

        if (level != lv) {
            ERROR("%s level %d!=%d, skip", ent->d_name, level, lv);
            continue;
        }

        DATA_PATH(fname, thiz->conf, ent->d_name);
        stat(fname, &st);
        if (st.st_size <= 0) {
            ERROR("%s empty, remove", fname);
            remove(fname);
            continue;
        }

        if (SELF->max_fnum < fnum) SELF->max_fnum = fnum;

        ftb = ftb_create(NULL);
        ftb->conf = thiz->conf;
        DATA_PATH(ftb->file, thiz->conf, ent->d_name);

        ftb->init(ftb);
        r = ftb->restore(ftb);
        if (r != 0) {
            ERROR("restore %s fail, backup", ftb->file);
            ftb->backup(ftb);
            ftb->destroy(ftb);
            r = 0;
            continue;
        }

        fset = thiz->fsets[level];
        r = fset->push(fset, ftb);
        if (r != 0) {
            if (lv != 0) {
                ERROR("push %s fail, move to level-0", ent->d_name);
                r = thiz->fsets[0]->push(thiz->fsets[0], ftb);
            }
           
            if (r != 0) {
                ERROR("push %s to level-0 fail, backup", ent->d_name);
                ftb->backup(ftb);
                ftb->destroy(ftb);
            }
        }
    }

    if (namelist) {
        while(--m >= 0) {
            free(namelist[m]);
        }

        free(namelist);
    }

    return 0;
}

static int   recover_dt(T *thiz)
{
    int r, i;

    for (i = thiz->conf->db_level - 1; i >= 0; i--) {
        r = recover_dt_Ln(thiz, i);
        if (r != 0) return -1;
    }

    for (i = thiz->conf->db_level - 1; i > 0; i--) {
        thiz->fsets[i]->ajust_index(thiz->fsets[i]);
    }

    return 0;
}

int   HI_PREFIX(recover)(T *thiz)
{
    int r;

    r = recover_bl(thiz);
    if (r != 0) return -1;

    r = recover_dt(thiz);
    if (r != 0) return -1;

    return 0;
}

int HI_PREFIX(cleanup)(T *thiz)
{
    char fname[G_MEM_MID];
    DIR *dir;
    struct dirent *ent;

    AB_PATH_TMP(fname, thiz->conf);
    dir = opendir(fname);
    if (dir == NULL) {
        ERROR("opendir %s, errno=%d", fname, errno);
        return -1;
    }

    while((ent = readdir(dir)) != NULL) {
        if (ent->d_type != DT_REG) continue;

        TMP_PATH(fname, thiz->conf, ent->d_name);
        remove(fname);
    }

    closedir(dir);
    return 0;
}

int HI_PREFIX(repaire)(T *thiz)
{
    /* TODO!! repaire db if file corrupted */
    UNUSED(thiz);
    return 0;
}


