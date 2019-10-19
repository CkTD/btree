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
    uint64_t        i,j,count;

    flag.dir = "test_table";
    flag.create_if_missing = 1;
    flag.error_if_exist = 0;

    table = table_open(flag);

    for(i = 0; i < 1000; i++)
    {
        row = table_row_new();
        for(j=0;j<100;j++)
            table_row_set_property(row, j, i * 1000 + j );
        table_append(table, row);
    }

    rows = table_search_range(table, 0, 0,5000, 100);
    count = table_rows_get_counts(rows);
    for(i = 0; i < count; i++)
    {
        row = table_rows_get_row(rows, i);
        printf("ROW %lu, first 10 properties: ", i);
        for(j = 0; j < 10; j++)
        {
            printf("%lu ", table_row_get_property(row, j));
        }
        printf("\n");
    }
    table_rows_destory(rows);


    if( table_create_index(table, 0) == 0)
    {
        printf("create index on column 0 successfully\n");
    }
    else
    {
        printf("index on column 0 already exist!\n");
    }

    rows = table_search_range(table, 0, 0,5000, 100);
    count = table_rows_get_counts(rows);
    for(i=0;i<count;i++)
    {
        row = table_rows_get_row(rows, i);
        printf("ROW %lu, first 10 props: ", i);
        for(j = 0; j < 10; j++)
        {
            printf("%lu ", table_row_get_property(row, j));
        }
        printf("\n");
    }
    table_rows_destory(rows);

    rows = table_search_range(table, 1, 0,5000, 100);
    count = table_rows_get_counts(rows);
    for(i=0;i<count;i++)
    {
        row = table_rows_get_row(rows, i);
        printf("ROW %lu, first 10 props: ", i);
        for(j = 0; j < 10; j++)
        {
            printf("%lu ", table_row_get_property(row, j));
        }
        printf("\n");
    }
    table_rows_destory(rows);

    table_close(table);
    return 0;
}