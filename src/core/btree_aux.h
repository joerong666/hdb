#ifndef _BTREE_AUX_H_
#define _BTREE_AUX_H_

#include "vcache.h"

#define BTR_SEC_SIZE 512
#define BTR_HEADER_BLK_SIZE (8 * BTR_SEC_SIZE)
#define BTR_VAL_BLK_SIZE (64 * BTR_SEC_SIZE)
#define BTR_LEAF_BLK_SIZE (8 * BTR_SEC_SIZE)
#define BTR_INDEX_BLK_SIZE (8 * BTR_SEC_SIZE)
#define BTR_FILTER_BLK_SIZE (8 * BTR_SEC_SIZE)

/* block meta size */
#define BTR_BLK_TAILER_SIZE 5  /* 5 => |blktype(1B)|CRC32(4B)| in block head/tail */
#define BTR_VAL_BLK_MSIZE 16
#define BTR_LEAF_BLK_MSIZE 64
#define BTR_INDEX_BLK_MSIZE 64
#define BTR_FILTER_BLK_MSIZE 16

#define BTR_INDEX_KGRP    24
#define BTR_KCNT_PER_IGRP 8
#define BTR_KCNT_PER_LGRP 8 
#define BIN_HEADER_SIZE 64

enum btree_e {
    BTR_HEADER_BLK = 1,
    BTR_VAL_BLK, 
    BTR_LEAF_BLK,
    BTR_INDEX_BLK,
    BTR_FILTER_BLK,
};

typedef struct btrstat_s {
    uint64_t io_hit;
    uint64_t cache_hit;
} btrstat_t;

typedef struct hdr_block_s {
    char    magic[8];
    uint8_t blktype;
    uint8_t version;
    uint8_t shrink_cpct_cnt;
    uint8_t tree_heigh;
    uint16_t filter_cnt;
    uint32_t node_cnt;
    uint32_t leaf_cnt;
    uint32_t key_cnt;
    uint32_t leaf_off;
    uint32_t fend_off;
    uint32_t filter_off;
    uint32_t map_off;
    uint32_t map_size;
    uint32_t last_voff;

    btrstat_t stat;

    mkey_t beg;  /* begin key of key range */
    mkey_t end;  /* end key of key range */
    char *map_buf;  /* mmap buf for index info */
    vcache_t *vcache;
} hdr_block_t;

size_t io_read(int fd, void *buf, size_t count);
ssize_t io_write(int fd, const void *buf, size_t count);
size_t io_pread(int fd, void *buf, size_t count, off_t offset);
ssize_t io_pwrite(int fd, const void *buf, size_t count, off_t offset);

int seri_keyitem_size(fkv_t *fkv, mkey_t *share, mkey_t *delt);
char *deseri_kname(fkv_t *fkv, char *share, char *delt);
char *deseri_key(fkv_t *fkv, char *share, char *delt);
char *seri_kmeta(char *dst, size_t len, fkv_t *fkv);
int seri_hdr(char *blkbuf, hdr_block_t *hdr);
int deseri_hdr(hdr_block_t *hdr, char *blkbuf);
int read_val(int fd, fkv_t *fkv, mval_t *v);
int read_vcache(vcache_t *vc, int fd, fkv_t *fkv, mval_t *v);
int wrap_block_crc(char *blkbuf, size_t blksize);
int blk_crc32_check(char *blkbuf, size_t blksize);
size_t bin_kv_size(mkv_t *kv);
size_t seri_bin_kv(char *buf, size_t total, mkv_t *kv);
ssize_t deseri_bin_kv(char *buf, off_t xoff, size_t len, mkv_t *kv);
char *move_to_klen_pos(int blktype, char *src);
char *move_to_key_pos(int blktype, char *src);
char *deseri_kmeta(fkv_t *fkv, char *src);

int key_cmp(mkey_t *k1, mkey_t *k2);
int mkv_cmp(mkv_t *kv1, mkv_t *kv2);
int fkv_cmp(fkv_t *kv1, fkv_t *kv2);
int merge_filter(fkv_t *fkv);

void check_fval(int fd, mkv_t *kv);

int extract_fkey(fkv_t *fkv);
int extract_fval(int fd, fkv_t *fkv);

int gen_tmp_file(conf_t *cnf, char *fname);
int open_tmp_file(conf_t *cnf, char *fname);

void backup(conf_t *cnf, char *file);
void recycle(conf_t *cnf, char *file);

#endif
