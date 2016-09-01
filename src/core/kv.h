#ifndef _HDB_KV_H_
#define _HDB_KV_H_

enum kv_type_e {
    KV_OP_DEL = 1,     /* default ADD */
    KV_VTP_BINOFF = 1 << 1, /* value identify binlog's off */
    KV_KTP_DELT = 1 << 2,   /* default as a integral item */
    KV_KTP_FROM_FILE = 1 << 3,  /* key from db file */
    KV_VTP_FROM_FILE = 1 << 4,  /* val from db file or binlog */
    KV_MERGE_KEEP_OLD = 1 << 5,
    KV_DEPRECATED = 1 << 6, /* may be overcovered on the case of existence */
    KV_IN_MM_CACHE = 1 << 7, /* has saved to mm_kvlist cache */
    KV_IN_IM_CACHE = 1 << 8, /* has saved to im_kvlist cache */
};

typedef struct key_s {
    uint8_t len;   
    char *data; 
} mkey_t;

typedef struct val_s {
    uint32_t len;   
    char *data; 
} mval_t;

typedef struct kv_s {
    uint16_t type;   /* type of value, normal val or file name */
    uint16_t vcrc; /* value crc */
    uint32_t off;
    uint64_t seq;
    mkey_t  k;
    mval_t  v;
} mkv_t;

typedef struct fkv_s {
    int blktype;
    uint32_t voff;   /* base on blkoff */
    uint32_t blkoff; /* child block offset */
    mkey_t kshare;    
    mkey_t kdelt;    
    mkv_t *kv;
} fkv_t;

#endif
