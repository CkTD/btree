#ifndef __BT_H__
#define __BT_H__

#include <stdint.h>

typedef struct BTreeOpenFlag {
    const char *file;
    uint64_t    order;
    int         create_if_missing;
    int         error_if_exist;
} BTreeOpenFlag;

typedef struct _BTree       BTree;
typedef struct _BTreeValues BTreeValues;

BTree *bt_open(BTreeOpenFlag flag);
void   bt_insert(BTree *bt, uint64_t key, uint64_t value);
void   bt_flush(BTree *bt);
void   bt_print(BTree *bt);

// if crush befor bt_flush, any modification will be lost.
// if crush inside bt_flush, the tree will be corrupted
void   bt_flush(BTree *bt);
void   bt_close(BTree *bt);

void         bt_values_destory(BTreeValues *values);
uint64_t     bt_values_get_value(BTreeValues *values, uint64_t index);
uint64_t     bt_values_get_count(BTreeValues *values);


BTreeValues *bt_search(BTree *bt, uint64_t limit, uint64_t key);

#endif // __BT_H__
