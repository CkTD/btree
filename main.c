#include "btree.h"
#include <stdio.h>
#include <stdlib.h>


int main()
{
    int k;
    BTree *bt;
    BTreeOpenFlag flag;
    flag.create_if_missing = 1;
    flag.error_if_exist = 0;
    flag.file = "./test.bt";
    flag.order = 5;

    bt = bt_open(flag);
    //bt_print(bt);

    uint64_t n = 1927183746;
    bt_insert(bt, n, n);    
    for(k=0;k<1000000;k++)
    {
        bt_insert(bt, rand(), rand());
        //bt_print(bt);
    }
    
    BTreeValues *values = bt_search(bt, 100, n);

    printf("Search res[%d]\n", bt_values_get_count(values));
    for(k=0;k<bt_values_get_count(values); k++)
    {
        printf("%d ", bt_values_get_value(values, k));
    }
    printf("\n");

    bt_values_destory(values);
    bt_close(bt);
    return 0;
}