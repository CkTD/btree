#ifndef __BT_H__
#define __BT_H__

#include <stdint.h>

typedef struct BT_OpenFlag {
    int         create_if_missing;
    const char *file;
    uint64_t    order;
} BT_OpenFlag;

typedef struct _BTree BTree;

#endif // __BT_H__
