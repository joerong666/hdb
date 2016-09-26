#ifndef _HDB_INC_H_
#define _HDB_INC_H_

#include "inc.h"
#include "btree_aux.h"

enum cpct_type_e {
    CPCT_MAJOR = 0,
    CPCT_L0,
    CPCT_AJACENT,
    CPCT_REMOTE,
    CPCT_SHRINK,
    CPCT_SPLIT,
    CPCT_MERGE,
    CPCT_MAX,
};

enum it_flag_e {
    IT_BEG          = 1,
    IT_MMTB         = (1 << 1),
    IT_IMTB         = (1 << 2),
    IT_L0           = (1 << 3),
    IT_Ln           = (1 << 4),
    IT_ONLY_KEY     = (1 << 5),
    IT_UNSAFE       = (1 << 6),
    IT_FETCH_RANGE  = (1 << 7),
    IT_FETCH_PREFIX = (1 << 8),
    IT_FIN          = (1 << 9),
};

#endif
