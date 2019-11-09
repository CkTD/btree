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
#include <time.h>

#include "table.h"



//  args:    table_dir_name  Y/N(index) insert search
//  example  ./table_test3  test_table Y 30000 30000
int main(int argc, char* argv [])
{
    TableOpenFlag   flag;
    Table          *table;
    TableRows      *rows;
    TableRow       *row;
    uint64_t        i,j,n, count;
    (void)count; 
    clock_t time;
    const char* dir = argv[1];
    int index = *argv[2] == 'Y' ? 1: 0;
    int insert  = atoi(argv[3]);
    int search = atoi(argv[4]);

    flag.dir = dir;
    flag.create_if_missing = 1;
    flag.error_if_exist = 0;

    table = table_open(flag);
    
    if(index)
    {
        printf("在第0列上建立索引...");
        if( table_create_index(table, 0) == 0)
        {
            printf("create index on column 0 successfully\n");
        }
        else
        {
            printf("index on column 0 already exist!\n");
        }
    }


    printf("插入 %d 行随机数据，每一行所有属性相同...\n", insert);
    time = clock();
    for(i = 0; i < insert; i++)
    {
        row = table_row_new();
        n = rand() % insert;
        for(j=0;j<100;j++)
            table_row_set_property(row, j, n );
        table_append(table, row);
    }
    printf("插入用时 %f", (float)(clock()- time)/ CLOCKS_PER_SEC);

    printf("查表%d此\n", search);
    time = clock();
    for(i=0;i<search;i++)
    {
    n = rand() % insert;
    rows = table_search_range(table, 0, n-10 , n, 100);
    count = table_rows_get_counts(rows);
    //printf(".");
    table_rows_destory(rows);
    }
    printf("查找用时 %f", (float)(clock()- time)/ CLOCKS_PER_SEC);

    printf("关闭表(刷磁盘)....\n");
    table_close(table);
    printf("done\n");
    return 0;
}
