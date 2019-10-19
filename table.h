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


#ifndef __TABLE_H__
#define __TABLE_H__

#include <stdint.h>



// table_row_new only intend for insert one row into table.
// DO NOT free the pointer returned by table_row_new. 
// after insert, Table will manage it.
typedef struct _TableRow TableRow;

TableRow *table_row_new();
uint64_t  table_row_get_property(TableRow *row, uint64_t index);
void      table_row_set_property(TableRow *row, uint64_t index, uint64_t value);



// TableRows holds search result.
// destory should be called explicitly for every search result. if not, memory leak.
typedef struct _TableRows TableRows;

uint64_t   table_rows_get_counts(TableRows *rows);
TableRow  *table_rows_get_row(TableRows *rows, uint64_t rowid);
void       table_rows_destory(TableRows *rows);



typedef struct TableOpenFlag {
    const char *dir;
    int         create_if_missing;
    int         error_if_exist;
} TableOpenFlag;
typedef struct _Table Table;

Table     *table_open(TableOpenFlag flag);
void       table_append(Table *table, TableRow *row);
TableRows *table_search_range(Table *table, uint64_t column, uint64_t min_value, uint64_t max_value, uint64_t limit);
TableRows *table_search(Table *table, uint64_t column, uint64_t value, uint64_t limit);
// return value:  0 for success, 1 for already exist
int  table_create_index(Table *table, uint64_t column);
void table_flush(Table *table);
void table_close(Table *table);

#endif // __TABLE_H__