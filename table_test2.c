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

#include "table.h"

int main()
{
    TableOpenFlag   flag;
    Table          *table;
    TableRows      *rows;
    TableRow       *row;
    uint64_t        i,j,n,count;
    

    flag.dir = "test_table";
    flag.create_if_missing = 1;
    flag.error_if_exist = 0;

    table = table_open(flag);
    
    printf("在第0列上建立索引...");
    if( table_create_index(table, 0) == 0)
    {
        printf("create index on column 0 successfully\n");
    }
    else
    {
        printf("index on column 0 already exist!\n");
    }


    printf("插入 1000000 行随机数据，每一行所有属性相同...\n");
    for(i = 0; i < 1000000; i++)
    {
        row = table_row_new();
        n = rand() % 1000000;
        for(j=0;j<100;j++)
            table_row_set_property(row, j, n );
        table_append(table, row);
        if (n<=1010 && n >=1000)
            printf("\t插入一个属性都为 %ld 的行\n", n);
    }

    printf("查找 第0个属性的范围在 1000 到 10010 的行，limit = 100.\n");
    rows = table_search_range(table, 0, 1000, 1010, 100);
    count = table_rows_get_counts(rows);
    for(i = 0; i < count; i++)
    {
        row = table_rows_get_row(rows, i);
        printf("\tROW %lu, first 10 properties: ", i);
        for(j = 0; j < 10; j++)
        {
            printf("%lu ", table_row_get_property(row, j));
        }
        printf("\n");
    }
    table_rows_destory(rows);

    printf("关闭表(刷磁盘)....\n");
    table_close(table);
    printf("done\n");
    return 0;
}
