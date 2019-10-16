#include "btree.h"

int main()
{
    int k;
    BTree *bt;
    BT_OpenFlag flag;
    flag.create_if_missing = 1;
    flag.file = "./test.bt";
    flag.order = 101;

    bt = bt_new(flag);
    //bt_print(bt);
    for(k=0;k<10000000;k++)
    {
        bt_insert(bt, k, k);
            //bt_print(bt);
    }


    return 0;
}