/**
 * Copyright (C) 2019 zn
 * 
 * This file is part of btree.
 * 
 * btree is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * btree is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with btree.  If not, see <http://www.gnu.org/licenses/>.
 */


#include <stdio.h>
#include <stdlib.h>

#include "btree.h"


int main()
{
    uint64_t       k, n;
    BTree         *bt;
    BTreeOpenFlag  flag;

    flag.create_if_missing = 1;
    flag.error_if_exist = 0;
    flag.file = "./test.bt";
    flag.order = 3;

    bt = bt_open(flag);
    
//    printf("Tree order: %lu\n", flag.order);
//    printf("Empty tree:\n");
//    bt_print(bt);

    printf("插入10 个随机的 key/value pair...\n");
    for(k=0;k<10;k++)
    {
        n = rand() % 100;
        bt_insert(bt, n, n);
        printf("\tinsert (%ld, %ld)\n", n, n);
    }
    printf("Insert done.\n");
   
    printf("Tree:");
    bt_print(bt);

    printf("Search done.\n");
    bt_close(bt);
    return 0;
}
