// Copyright (C) 2019 zn
// 
// This file is part of btree.
// 
// btree is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// btree is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// 
// You should have received a copy of the GNU General Public License
// along with btree.  If not, see <http://www.gnu.org/licenses/>.

#ifndef __BTREE_H__
#define __BTREE_H__

#include <stdint.h>



typedef struct _BTreeValues BTreeValues;
uint64_t   bt_values_get_count(BTreeValues *values);
uint64_t   bt_values_get_value(BTreeValues *values, uint64_t index);
void       bt_values_destory(BTreeValues *values);


typedef struct BTreeOpenFlag {
    const char *file;
    uint64_t    order;
    int         create_if_missing;
    int         error_if_exist;
} BTreeOpenFlag;

typedef struct _BTree       BTree;

BTree *bt_open(BTreeOpenFlag flag);
void   bt_insert(BTree *bt, uint64_t key, uint64_t value);
void   bt_flush(BTree *bt);
void   bt_print(BTree *bt);
// if crush befor bt_flush, any modification will be lost.
// if crush inside bt_flush, the tree will be corrupted
void   bt_flush(BTree *bt);
void   bt_close(BTree *bt);
BTreeValues *bt_search(BTree *bt, uint64_t limit, uint64_t key);
BTreeValues *bt_search_range(BTree *bt, uint64_t limit, uint64_t key_min, uint64_t key_max);

#endif // __BTREE_H__
