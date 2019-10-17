#include "btree.h"

int main()
{
    int k;
    BTree *bt;
    BT_OpenFlag flag;
    flag.create_if_missing = 1;
    flag.file = "./test.bt";
    flag.order = 7;

    bt = bt_new(flag);
    bt_print(bt);
    /*
    for(k=0;k<2;k++)
    {
        bt_insert(bt, k+100, k+100);
            bt_print(bt);
    }
    */
    bt_flush(bt);
    return 0;
}