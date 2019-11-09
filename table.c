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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <assert.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pthread.h>

#include "btree.h"
#include "table.h"

/*
Files Structure:
    TableMeta
    for i = 0:TableMeta.row_counts-1
        TableRow

*/

#define TABLE_FILE_MAGIC 0xaaaaaaaa
#define COLUMNS 100

 struct _TableRow {
    int64_t properties[COLUMNS];
} ;

struct _TableRows {
    uint64_t   counts;
    TableRow **rows;
};

// for convenient, don't update meta every time it's member change
// this data structure is only used fro read from / write to disk
typedef struct TableMeta {
    uint64_t     file_magic;
    uint64_t     row_counts;            // this is table->content->rows->counts
    uint64_t     index_flag[COLUMNS];   // this is table->indexs->index_flags
} TableMeta;

typedef struct TableContent {
    char       *file_path;
    int         file_fd;
    uint64_t    next_rowid_to_flush;
    TableRows  *rows;
    TableMeta   meta;
} TableContent;

typedef struct TableIndex {
    const char *dir;                    // used to figure out index file name on disk
    uint64_t    index_flag[COLUMNS];
    BTree      *index_trees[COLUMNS];
} TableIndex;


struct _Table {
    char            *dir;
    TableIndex      *indexs;
    TableContent    *content;
    pthread_mutex_t  mutex;
};




/////////////////////////////////////////////////
//  TableRow
/////////////////////////////////////////////////

TableRow *table_row_new()
{
    TableRow *row;

    row = (TableRow *)malloc(sizeof(TableRow));

    return row;
}

uint64_t table_row_get_property(TableRow *row, uint64_t index)
{
    return row->properties[index];
}

void table_row_set_property(TableRow *row, uint64_t index, uint64_t value)
{
    row->properties[index] = value;
}

static void table_row_destory(TableRow *row)
{
    free(row);
}


/////////////////////////////////////////////////
//  TableRows
/////////////////////////////////////////////////

uint64_t table_rows_get_counts(TableRows *rows)
{
    return rows->counts;
}

static uint64_t table_rows_append_row(TableRows *rows, TableRow *row)
{
    uint64_t rowid;
    rowid = rows->counts;

    rows->counts++;
    rows->rows = (TableRow **)realloc(rows->rows, sizeof(TableRow *) * (rows->counts));
    rows->rows[rowid] = row;

    return rowid;
}

TableRow *table_rows_get_row(TableRows *rows, uint64_t rowid)
{
    assert(rowid < rows->counts);
    
    return rows->rows[rowid];
}

static void _table_rows_destory(TableRows *rows, int destory_row)
{
    int i;

    if (destory_row)
    {
        for(i = 0; i< rows->counts; i++)
        {
            table_row_destory(rows->rows[i]);
        }
    }
    free(rows->rows);
    free(rows);
}

static TableRows *table_rows_new_empty()
{
    TableRows *rows;

    rows = (TableRows *)malloc(sizeof(TableRows));
    rows->counts = 0;
    rows->rows = NULL;

    return rows;
}

void table_rows_destory(TableRows *rows)
{
    _table_rows_destory(rows, 0);
}




/////////////////////////////////////////////////
//  TableContent
/////////////////////////////////////////////////
static uint64_t table_content_get_row_counts(TableContent *content)
{
    return table_rows_get_counts(content->rows);
}

static char *table_content_get_path(const char *dir)
{
    int   len;
    char *full_path;
    const char *fixed_name = "/table";  // 6 characters
    len = strlen(dir);
    full_path = (char *)malloc(len + 7);
    strcpy(full_path, dir);
    strcpy(full_path + len, fixed_name);
    return full_path;
}

static void table_content_open_file(TableContent *content)
{
    if (content->file_fd == -1)
    {
        content->file_fd = open(content->file_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    }
    assert(content->file_fd != -1);
}

static TableContent *table_content_new_empty(const char* dir)
{
    TableContent *content;

    content = (TableContent *)malloc(sizeof(TableContent));

    content->file_path = table_content_get_path(dir);
    content->file_fd = -1;
    content->next_rowid_to_flush = 0;
    content->rows = table_rows_new_empty();

    content->meta.file_magic = TABLE_FILE_MAGIC;
    content->meta.row_counts = 0;
    memset(content->meta.index_flag, 0, sizeof(uint64_t) * COLUMNS);
    return content;
}

static TableContent *table_content_new_from_file(const char *dir)
{
    TableContent *content;
    TableRows    *rows;
    TableRow     *row;
    int           i;
    ssize_t       rtv;

    content = (TableContent *)malloc(sizeof(TableContent));

    content->file_fd = -1;
    content->file_path = table_content_get_path(dir);
    table_content_open_file(content);

    rtv = lseek(content->file_fd, 0, SEEK_SET);
    assert(rtv == 0);
    rtv = read(content->file_fd, &content->meta, sizeof(TableMeta));
    assert(rtv == sizeof(TableMeta));
    assert(content->meta.file_magic == TABLE_FILE_MAGIC);

    content->next_rowid_to_flush = content->meta.row_counts;

    rows = table_rows_new_empty();
    for(i = 0; i < content->meta.row_counts; i ++)
    {
        row = (TableRow *)malloc(sizeof(TableRow));
        rtv = read(content->file_fd, row, sizeof(TableRow));
        assert(rtv == sizeof(TableRow));
        table_rows_append_row(rows, row);
    }
    content->rows = rows;

    return content;
}

static void table_content_update_meta(TableContent *content, uint64_t *index_flag)
{
    content->meta.row_counts = table_rows_get_counts(content->rows);
    memcpy(content->meta.index_flag, index_flag, sizeof(uint64_t) * COLUMNS);
}

static void table_content_flush(TableContent *content)
{
    ssize_t       rtv;
    TableRow     *row;
    uint64_t      row_counts;
    int           i;

    table_content_open_file(content);
    rtv = lseek(content->file_fd, 0, SEEK_SET);
    assert(rtv == 0);
    rtv = write(content->file_fd, &content->meta, sizeof(TableMeta));

    row_counts = table_rows_get_counts(content->rows);
    assert(rtv == sizeof(TableMeta));
    rtv = lseek(content->file_fd, sizeof(TableRow) * content->next_rowid_to_flush, SEEK_CUR);
    assert(rtv == sizeof(TableRow) * content->next_rowid_to_flush + sizeof(TableMeta));
    for(i = content->next_rowid_to_flush; i < row_counts; i++)
    {
        row = table_rows_get_row(content->rows, i);
        rtv = write(content->file_fd, row, sizeof(TableRow));
        assert(rtv == sizeof(TableRow));
    }
}

static uint64_t table_content_append_row(TableContent *content, TableRow * row)
{
    return table_rows_append_row(content->rows, row);
}

static TableRows *table_content_get_all_rows(TableContent *content)
{
    return content->rows;
}

static TableRow *table_content_get_row(TableContent *content, uint64_t rowid)
{
    return table_rows_get_row(content->rows, rowid);
}

static void table_content_destory(TableContent *content)
{
    _table_rows_destory(content->rows, 1);
    free(content->file_path);
    free(content);
}


/////////////////////////////////////////////////
//  TableIndex
/////////////////////////////////////////////////

static int table_index_is_exist(TableIndex * index, uint64_t column)
{
    return index->index_flag[column];
}

static TableIndex *table_index_new_empty(const char *dir)
{
    TableIndex *index;
    
    index = (TableIndex *)malloc(sizeof(TableIndex));

    index->dir = dir;
    memset(index->index_trees, 0, sizeof(BTree *) * COLUMNS);
    memset(index->index_flag, 0, sizeof(uint64_t) * COLUMNS);

    return index;
}

static TableIndex *table_index_new_by_meta(const char *dir, TableMeta *meta)
{
    TableIndex *index;
    
    index = (TableIndex *)malloc(sizeof(TableIndex));

    index->dir = dir;
    memset(index->index_trees, 0, sizeof(BTree *) * COLUMNS);
    memcpy(index->index_flag, meta->index_flag, sizeof(uint64_t) * COLUMNS);

    return index;
}

const void table_index_get_path(TableIndex *index, uint64_t column, char* buff)
{
    int   dir_len;
    dir_len = strlen(index->dir);
    strcpy(buff, index->dir);
    buff[dir_len] = '/';
    sprintf(buff + dir_len + 1, "column%lu.index", column);
}

static BTree *table_index_get(TableIndex *index, uint64_t column, int is_creat)
{
    BTree        *bt;
    BTreeOpenFlag flag;
    char          full_name[64];

    if (index->index_trees[column] == NULL)
    {
        table_index_get_path(index, column, full_name);

        flag.file = full_name;
        flag.order = 101;
        if (is_creat)
        {
            flag.create_if_missing = 1;
            flag.error_if_exist = 1;
        }
        else
        {
            flag.create_if_missing = 0;
            flag.error_if_exist = 0;
        }
        

        bt = bt_open(flag);
        assert(bt != NULL);
        index->index_trees[column] = bt;
    }


    return index->index_trees[column];
}

static void table_index_update(TableIndex *index, TableRow *row, uint64_t rowid)
{
    uint64_t  col, value;
    BTree    *bt;

    for(col = 0; col < COLUMNS; col++)
    {
        if(index->index_flag[col] == 1)
        {
            value = table_row_get_property(row, col);
            bt = table_index_get(index, col, 0);
            bt_insert(bt, value, rowid);
        }
    }
}

static int table_index_create(TableIndex *index, uint64_t column, TableRows *all_rows)
{
    BTree     *bt;
    TableRow  *row;
    uint64_t   row_counts, rowid, value;

    // do nothing if index on this column already exist
    if(index->index_flag[column] != 0)
    {
        assert(index->index_trees[column] != NULL);
        return -1;
    }

    // create an empty index on column
    index->index_flag[column] = 1;
    bt = table_index_get(index, column, 1);
    assert(index->index_trees[column] != NULL);

    // update newly created index by all rows
    row_counts = table_rows_get_counts(all_rows);
    for(rowid = 0; rowid < row_counts; rowid++)
    {
        row = table_rows_get_row(all_rows, rowid);
        value = table_row_get_property(row, column);
        bt_insert(bt, value, rowid);
    }

    return 0;
}

static void table_index_flush(TableIndex *index)
{
    int i;

    for(i = 0; i < COLUMNS; i++)
    {
        if(index->index_trees[i] != NULL)
        {
            assert(index->index_flag[i] == 1);
            bt_flush(index->index_trees[i]);
        }
    }
}

static void table_index_destory(TableIndex *index)
{
    int i;

    for(i = 0; i < COLUMNS; i++)
    {
        if(index->index_trees[i] != NULL)
        {
            assert(index->index_flag[i] == 1);
            bt_close(index->index_trees[i]);
        }
    }

    free(index);
}

/////////////////////////////////////////////////
//  Table
/////////////////////////////////////////////////

/*
static uint64_t table_get_row_counts(Table *table, uint64_t count)
{
    return table->meta.row_counts;
}
*/

static void table_lock(Table *table)
{
    int rtv;
    rtv = pthread_mutex_lock(&table->mutex);
    assert(rtv == 0);
}

static void table_unlock(Table *table)
{
    int rtv;
    rtv = pthread_mutex_unlock(&table->mutex);
    assert(rtv == 0);
}

static TableRows *table_search_by_index(Table *table, uint64_t column, uint64_t min_value, uint64_t max_value, uint64_t limit)
{
    TableRows   *rows;
    TableRow    *row;
    BTree       *bt;
    BTreeValues *values;
    uint64_t     rowid, i, counts;

    rows = table_rows_new_empty();
    bt = table_index_get(table->indexs, column, 0);
    values = bt_search_range(bt, limit, min_value, max_value);
    counts = bt_values_get_count(values);
    for(i = 0; i < counts; i++)
    {
        rowid = bt_values_get_value(values, i);
        row = table_content_get_row(table->content, rowid);
        table_rows_append_row(rows, row);
    }
    bt_values_destory(values);
    return rows;
}

static TableRows *table_search_by_exhaustion(Table *table, uint64_t column, uint64_t min_value, uint64_t max_value, uint64_t limit)
{
    TableRows   *rows;
    TableRow    *row;
    uint64_t     rowid ,row_counts, value;

    rows = table_rows_new_empty();
    
    row_counts = table_content_get_row_counts(table->content);
    for(rowid = 0; rowid < row_counts && limit > 0; rowid++)
    {
        row = table_content_get_row(table->content, rowid);
        value = table_row_get_property(row, column);
        if(value >= min_value && value <= max_value)
        {
            table_rows_append_row(rows, row);
            limit--;
        }
    }

    return rows;
}

TableRows *table_search_range(Table *table, uint64_t column, uint64_t min_value, uint64_t max_value, uint64_t limit)
{
    TableRows *rows;

    table_lock(table);

    if(table_index_is_exist(table->indexs, column))
        rows = table_search_by_index(table, column, min_value, max_value, limit);
    else
        rows = table_search_by_exhaustion(table, column, min_value, max_value, limit);

    table_unlock(table);

    return rows;
}

TableRows *table_search(Table *table, uint64_t column, uint64_t value, uint64_t limit)
{
    TableRows *rows;

    table_lock(table);

    rows = table_search_range(table, column, value, value, limit);

    table_unlock(table);

    return rows;
}

void table_append(Table *table, TableRow *row)
{
    uint64_t rowid;
    
    table_lock(table);
    
    rowid = table_content_append_row(table->content, row);
    table_index_update(table->indexs, row, rowid);

    table_unlock(table);
}

int table_create_index(Table *table, uint64_t column)
{
    TableRows *rows;
    int rtv;
    
    table_lock(table);
    
    rows = table_content_get_all_rows(table->content);
    rtv = table_index_create(table->indexs, column, rows);
    
    table_unlock(table);
    return rtv;
}

static Table *table_new_empty(const char *dir)
{
    Table *table;
    char  *h_dir;
    int    rtv;

    table = (Table *)malloc(sizeof(Table));
    
    h_dir = (char *)malloc(strlen(dir) + 1);
    strcpy(h_dir, dir);

    rtv = mkdir(h_dir, S_IRUSR | S_IWUSR | S_IXUSR);
    assert(rtv == 0);

    table->dir = h_dir;
    table->content = table_content_new_empty(dir);
    table->indexs = table_index_new_empty(dir);
    rtv = pthread_mutex_init(&table->mutex, NULL);
    if (rtv != 0)
    {
        table_close(table);
        return NULL;
    }
    return table;    
}

static Table *table_new_from_file(const char *dir)
{
    Table *table;
    char  *h_dir;
    int rtv;

    table = (Table *)malloc(sizeof(Table));
    
    h_dir = (char *)malloc(strlen(dir) + 1);
    strcpy(h_dir, dir);

    table->dir = h_dir;
    table->content = table_content_new_from_file(dir);
    table->indexs = table_index_new_by_meta(dir, &(table->content->meta));
    rtv = pthread_mutex_init(&table->mutex, NULL);
    if (rtv != 0)
    {
        table_close(table);
        return NULL;
    }
    
    return table;    
}

Table *table_open(TableOpenFlag flag)
{
    if(access(flag.dir, F_OK) != -1)
    {
        if(flag.error_if_exist)
            return NULL;
        return table_new_from_file(flag.dir);
    }
    else
    {
        // table not exist!
        if(!flag.create_if_missing)
            return NULL;
        return table_new_empty(flag.dir);
    }
}

void table_flush(Table *table)
{
    table_content_update_meta(table->content, table->indexs->index_flag);
    table_content_flush(table->content);
    table_index_flush(table->indexs);
}

void table_close(Table *table)
{
    pthread_mutex_destroy(&table->mutex);
    table_flush(table);
    table_index_destory(table->indexs);
    table_content_destory(table->content);
    free(table->dir);
    free(table);
}
