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
    BTreeValues   *values;

    
    flag.create_if_missing = 1;
    flag.error_if_exist = 0;
    flag.file = "./test.bt";
    flag.order = 101;

    bt = bt_open(flag);
    //bt_print(bt);

    n = 544;
    bt_insert(bt, n, n);


    for(k=0;k<1000000;k++)
    {
        bt_insert(bt, k, k);
        //bt_insert(bt, rand(), rand());
        //bt_print(bt);
    }
    
    values = bt_search(bt, 100, n);
    printf("Search res[%lu]\n", bt_values_get_count(values));
    for(k=0;k<bt_values_get_count(values); k++)
        printf("%lu ", bt_values_get_value(values, k));
    printf("\n");
    bt_values_destory(values);

    values = bt_search_range(bt, 100, n, n+15);
    printf("Search res[%lu]\n", bt_values_get_count(values));
    for(k=0;k<bt_values_get_count(values); k++)
        printf("%lu ", bt_values_get_value(values, k));
    printf("\n");
    bt_values_destory(values);

    bt_close(bt);
    return 0;
}