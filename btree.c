#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

#include "btree.h"
#include "list.h"


/*

Files Structure:
                   +-------+--------+---------+--------+--------+
    block id       |   0   |    1   |    2    |    3   |  ..... |
                   +-------+--------+---------+--------+--------+
    block countent | Meta  |  ROOT  |   NODE  |  NODE  |  ..... |
                   +-------+--------+---------+--------+--------+

    each block have same size, blk0 -> Meta Block, blk1 -> Root Node Block


Contents in different type of block
    BTreeNodeBlk followed by:

    LEAF_NODE or ROOT_LEAF_NODE

        [V0 K0 V1 K1 ... Vm Km]     
            ROOT:        m is [0, order - 1]
            NON ROOT:    m is [(order - 1) / 2, (order - 1)]

        +----+----+--------+-----+-----------+------------+
        | k0 | k1 |  ....  | Km  |   empty   | K(order-1) |
        +----+----+--------+-----+-----------+------------+
        |    |    |  ...   |
        V0   V1   V2       Vm

        Ki: uint_64 keys (i = 1:(order-1))
        Vi: uint_64 values


    ROOT_NODE or INTERNAL_NODE

        [CN0 K0 CN1 K1 CN2 K2 ... CNm Km CN(m+1)]
            ROOT:      m is [1, order - 1]
            INTERNAL:  m is [(order - 1) / 2, (order - 1)]

        +----+----+--------+-----+-----------+------------+
        | k0 | k1 |  ....  | Km  |   empty   | K(order-1) |
        +----+----+--------+-----+-----------+------------+
        |    |    |  ...   |
       CN0  CN1  CN2      CNm

        cni: uint_64 blk index to children node 
*/

// Represent MetaData on disk 
typedef struct {
    // fixed content
    uint64_t order;
    uint64_t blk_size;
    uint64_t blk_counts;
    uint64_t max_blkid;
    // padding to blk_size
} BTreeMetaBlk;

typedef struct {
    BTreeMetaBlk *blk;
    int          dirty;
} BTreeMeta;


#define BT_NODE_TYPE_ROOT      1
#define BT_NODE_TYPE_INTERNAL  2
#define BT_NODE_TYPE_LEAF      4

// Represent Tree Node on disk 
typedef struct {
    // fixed content
    uint64_t type;
    uint64_t parent_idx;
    uint64_t left_sibling_idx;
    uint64_t right_sibling_idx;
    uint64_t key_counts;
    // key and pointers is decided by order of the tree:
    // for i = 0:order-2
    //     uint64_t child_i_index or value
    //     uint64_t key_i     
    // and one extra:
    // uint64_t child_order-1_index 
      

    // sizeof(key and pointers) = (order * 2 - 1 ) * 64
    // block size = sizeof(key and pointers) + sizeof(BTreeNodeBlk)
} BTreeNodeBlk;

typedef struct {
    BTreeNodeBlk   *blk;     
    uint64_t        blkid;
    int             dirty;

    BTree          *tree;

    BTreeNode       *parent;
    BTreeNode       *left_sibling;
    BTreeNode       *right_sibling;
    BTreeNode      **children;     

    // if the node is modified
    // chain it in one of new_node_chain, deleted_node_chain, dirty_node_chain.
    struct list_head       chain;   
    // which chain this node in? (the block status)    
    struct list_head      *state;  
} BTreeNode;

struct _BTree {
    const char  *file_path;
    int          file_fd;

    BTreeNode     *root;
    BTreeMeta     *meta;

    struct list_head     new_node_chain;
    struct list_head     deleted_node_chain;
    struct list_head     dirty_node_chain;

    // calculated by meta for convenience
    uint64_t order;
    uint64_t blk_size;
    uint64_t min_keys; //for normal nodes(internal and non-root leaf);
    uint64_t max_keys; //for normal nodes(internal and non-root leaf);
};




static BTreeMetaBlk *bt_meta_blk_new_empty(uint64_t blksize, uint64_t order)
{
    BTreeMetaBlk * blk = (BTreeMetaBlk *)malloc(blksize);
    blk->order = order;
    blk->blk_size = blksize;
    blk->blk_counts = 2;  // empty tree has Meta and Root block
    blk->max_blkid = 1;
    return blk;
}

static uint64_t bt_meta_blk_next_blkid(BTreeMetaBlk *blk)
{
    return ++blk->max_blkid;
}

static BTreeMeta *bt_meta_new_empty(BTreeMetaBlk *blk)
{
    BTreeMeta *meta = (BTreeMeta *)malloc(sizeof(BTreeMeta));
    meta->dirty = 1;
    meta->blk = blk;
    return meta;
}

static uint64_t bt_meta_next_bklid(BTreeMeta *meta)
{
    meta->dirty = 1;
    return bt_meta_blk_next_blkid(meta->blk);
}



static BTreeNodeBlk *bt_node_blk_new_empty(uint64_t blk_size, uint64_t type)
{
    BTreeNodeBlk *blk =  (BTreeNodeBlk *)malloc(blk_size); 
    blk->type = type;
    blk->parent_idx = 0;
    blk->left_sibling_idx = 0;
    blk->right_sibling_idx = 0;
    blk->key_counts = 0;
}

static BTreeNodeBlk *bt_node_blk_new_from_file()
{
    // TODO
    return NULL;
}

// index range from [0, key_counts - 1]
static uint64_t bt_node_blk_get_key(BTreeNodeBlk *blk, uint64_t index)
{
    assert(blk->key_counts >0 && index < blk->key_counts);
    return *(uint64_t *)
            ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2 + 1));
}

static void bt_node_blk_set_key(BTreeNodeBlk *blk, uint64_t index, uint64_t key)
{
    assert(blk->key_counts >0 && index < blk->key_counts);
    *(uint64_t *)
        ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2 + 1)) = key;
}

// index range from [0, key_counts - 1]
static uint64_t bt_node_blk_get_value(BTreeNodeBlk *blk, uint64_t index)
{
    assert(blk->type == BT_NODE_TYPE_LEAF);
    assert(index < blk->key_counts);
    return *(uint64_t *)
            ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2));

}

static void bt_node_blk_set_value(BTreeNodeBlk *blk, uint64_t index, uint64_t value)
{
    assert(blk->type == BT_NODE_TYPE_LEAF);
    assert(index < blk->key_counts);
    *(uint64_t *)
        ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2)) = value;

}

// index range from [0, key_counts]
static uint64_t bt_node_blk_get_child_idx(BTreeNodeBlk *blk, uint64_t index)
{
    assert(blk->type != BT_NODE_TYPE_LEAF);
    assert(index <= blk->key_counts);
    return *(uint64_t *)
            ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2));
}

static void bt_node_blk_set_child_idx(BTreeNodeBlk *blk, uint64_t index, uint64_t idx)
{
    assert(blk->type != BT_NODE_TYPE_LEAF);
    assert(index <= blk->key_counts);
    *(uint64_t *)
        ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2)) = idx;
}


static uint64_t bt_node_blk_get_key_count(BTreeNodeBlk *blk)
{
    return blk->key_counts;
}

static void bt_node_blk_set_key_count(BTreeNodeBlk *blk, uint64_t count)
{
    blk->key_counts = count;
}

static int bt_node_blk_get_type(BTreeNodeBlk *blk)
{
    return blk->type;
}

static void bt_node_blk_set_type(BTreeNodeBlk *blk, int type)
{
    blk->type = type;
}

static void bt_node_blk_link_sibling(BTreeNodeBlk *left, BTreeNodeBlk *right, uint64_t left_idx, uint64_t right_idx)
{
    left->right_sibling_idx = right_idx;
    right->left_sibling_idx = left_idx;
}

static void bt_node_blk_link_parent_child(BTreeNodeBlk *parent, BTreeNodeBlk *child, uint64_t parent_idx, uint64_t child_idx, uint64_t index)
{
    bt_node_blk_set_child_idx(parent, index, child_idx);
    child->parent_idx = parent_idx;
}

// insert a key,value pair into a LEAF node blk
// caller check the node is not full
void bt_node_leaf_blk_insert(BTreeNodeBlk *blk, uint64_t key, uint64_t value)
{
    uint64_t pos;
    char *src;
    char *dest;
    uint64_t n;

    assert(blk->type == BT_NODE_TYPE_LEAF);

    pos = 0;
    
    while (bt_node_blk_get_key(blk, pos) <= key && pos < blk->key_counts)
        pos ++;
    
    src = (char *)blk + sizeof(BTreeNodeBlk) + pos * sizeof(uint64_t) * 2;
    dest = src + 2 *sizeof(uint64_t);
    n = (blk->key_counts - pos) * 2 * sizeof(uint64_t);
    memmove(dest, src, n);

    *(uint64_t *)src = key;
    *((uint64_t *)(src) + 1) = value;

    blk->key_counts += 1;
}




static BTreeNode *bt_node_new(BTree *tree, BTreeNodeBlk *blk)
{
    BTreeNode *node = (BTreeNode *)malloc(sizeof(BTreeNode));
    node->blkid = bt_next_blkid(tree);
    node->blk = blk;
    node->tree = tree;
    node->parent = NULL;
    node->left_sibling = NULL;
    node->right_sibling = NULL;
    node->children = (BTreeNode **)malloc(sizeof(BTreeNode *) * bt_get_order(tree));
    bt_node_marked_new(node);
    return node;
}

int bt_node_get_type(BTreeNode *node)
{
    return bt_node_blk_get_type(node);
}

void bt_node_set_type(BTreeNode *node, int type)
{
    bt_node_blk_set_type(node ,type);
    bt_node_marked_dirty(node);
}

static void bt_node_load_blk(BTreeNode *node)
{
    // TODO
}

static uint64_t bt_node_get_key(BTreeNode *node, uint64_t index)
{
    if(!node->blk)
        bt_node_load_blk(node);

    return bt_node_blk_get_key(node->blk, index);
}

static void bt_node_set_key(BTreeNode *node, uint64_t index, uint64_t key)
{
    if(!node->blk)
        bt_node_load_blk(node);

    bt_node_blk_set_key(node->blk, index, key);

    bt_node_marked_dirty(node);
}

static uint64_t bt_node_get_value(BTreeNode *node, uint64_t index)
{
    if(!node->blk)
        bt_node_load_blk(node);
    return bt_node_blk_get_value(node->blk, index);
}

static uint64_t bt_node_set_value(BTreeNode *node, uint64_t index, uint64_t value)
{
    if(!node->blk)
        bt_node_load_blk(node);

    bt_node_blk_set_value(node->blk, index, value);

    bt_node_marked_dirty(node);
}

static uint64_t bt_node_get_child_idx(BTreeNode *node, uint64_t index)
{
    if(!node->blk)
        bt_node_load_blk(node);
    return bt_node_blk_get_child_idx(node->blk, index);
}

static void bt_node_set_child_idx(BTreeNode *node, uint64_t index, uint64_t idx)
{
    if(!node->blk)
        bt_node_load_blk(node);

    bt_node_blk_set_child_idx(node->blk, index, idx);

    bt_node_marked_dirty(node);
}

static uint64_t bt_node_get_key_count(BTreeNode *node)
{
    if(!node->blk)
        bt_node_load_blk(node); 
    return bt_node_blk_get_key_count(node->blk);
}

static void bt_node_set_key_count(BTreeNode *node, uint64_t count)
{
    if(!node->blk)
        bt_node_load_blk(node);

    bt_node_blk_set_key_count(node->blk, count);

    bt_node_marked_dirty(node);
}

static void bt_node_link_sibling(BTreeNode *left, BTreeNode *right)
{
    left->right_sibling = right;
    right->left_sibling = left;
    bt_node_blk_link_sibling(left->blk, right->blk, left->blkid, right->blkid);
    bt_node_marked_dirty(left);
    bt_node_marked_dirty(right);
}

static void bt_node_link_parent_child(BTreeNode *parent, BTreeNode *child, uint64_t index)
{
    parent->children[index] = child;
    child->parent = parent;
    bt_node_blk_link_parent_child(parent, child, parent->blkid, child->blkid, index);

    bt_node_marked_dirty(parent);
    bt_node_marked_dirty(child);
}

// insert a key,value pair into a LEAF node
// caller check the node is not full
static void bt_node_leaf_insert(BTreeNode *leaf, uint64_t key, uint64_t value)
{
    bt_node_marked_dirty(leaf);
    bt_node_leaf_blk_insert(leaf->blk, key, value);    
}

static uint64_t bt_next_blkid(BTree *bt)
{
    return bt_meta_next_bklid(bt->meta);
}


static void bt_split(BTree *bt, BTreeNode *node)
{
    BTreeNode *new1;
    BTreeNode *new2;
    BTreeNodeBlk *blk;
    int type;

    // should be called only if node is full
    assert(bt_node_get_key_count(node) == bt->max_keys);
    
    type = bt_node_get_type(node);
    
    if (type && BT_NODE_TYPE_ROOT)
    {
        // Root Split
        if(type && BT_NODE_TYPE_LEAF)
        {
            // Root is the only node
            blk = bt_node_blk_new_empty(bt->blk_size, BT_NODE_TYPE_LEAF);
            new1 = bt_node_new(bt, blk);
            blk = bt_node_blk_new_empty(bt->blk_size, BT_NODE_TYPE_LEAF);
            new2 = bt_node_new(bt, blk);
            bt_node_link_sibling(new1, new2);
            bt_node_link_parent_child(node, new1, 0);
            bt_node_link_parent_child(node, new2, 1);
            bt_node_set_key_count(node ,2);
            bt_node_set_type(node, BT_NODE_TYPE_ROOT);
            
        }
        else
        {
            // Root is not leaf
            // TODO
        }
    }

    else if (type && BT_NODE_TYPE_LEAF)
    {
        // TODO
    }

    else if (type && BT_NODE_TYPE_INTERNAL)
    {
        // TODO
    }

    else
    {
        assert(0);
    }
    
}

static void *bt_node_marked_dirty(BTreeNode *node)
{
    assert(node->state != &node->tree->deleted_node_chain);

    if(node->state == &node->tree->new_node_chain)
        return;
    if(node->state == &node->tree->dirty_node_chain)
        return;
    
    node->state = &node->tree->dirty_node_chain;
    list_add(&node->chain, &node->tree->dirty_node_chain);
}

static void *bt_node_marked_new(BTreeNode *node)
{
    assert(node->state = NULL);
}




uint64_t bt_get_order(BTree *bt)
{
    return bt->order;
}

uint64_t bt_get_max_keys(BTree *bt)
{
    return bt->max_keys;
}

uint64_t bt_get_min_keys(BTree *bt)
{
    return bt->min_keys;
}


static BTree *bt_new_from_file(const char *file)
{
    BTree *bt = (BTree *)malloc(sizeof(BTree));
    // TODO
}

static BTree *bt_new_empty(const char *file, uint64_t order)
{
    uint64_t      blksize;
    BTree        *bt;
    BTreeNodeBlk *nodeblk;
    BTreeMetaBlk *metablk;
    
    bt = (BTree *)malloc(sizeof(BTree));
 
    INIT_LIST_HEAD(&bt->deleted_node_chain);
    INIT_LIST_HEAD(&bt->new_node_chain);
    INIT_LIST_HEAD(&bt->dirty_node_chain);

    // !!!copy file name to heap!!!
    bt->file_path = file;
    bt->order = order;
    bt->max_keys = order - 1;
    bt->min_keys = order / 2;

    blksize = sizeof(BTreeNodeBlk) + (order * 2 - 1) * sizeof(uint64_t);
    bt->blk_size = blksize;
    assert(blksize >= sizeof(BTreeMetaBlk));

    metablk = bt_meta_blk_new_empty(blksize, order);
    bt->meta = bt_meta_new_empty(metablk);

    nodeblk = bt_node_blk_new_empty(bt->meta->blk->blk_size, 
                                    BT_NODE_TYPE_LEAF | BT_NODE_TYPE_ROOT);
    bt->root = bt_node_new(bt, nodeblk);
    return bt;
}

BTree * bt_new(BT_OpenFlag flag)
{
    if(flag.file == NULL)
        return NULL;
    if(access(flag.file, F_OK) != -1)
    {
        return bt_new_from_file(flag.file);
    }
    else
    {
        // file not exist!
        if(!flag.create_if_missing)
            return NULL;
        if(flag.order % 2 != 1 || flag.order < 3)
            return NULL;
        return bt_new_empty(flag.file, flag.order);
    }
}

// given a node, return the leaf derived from node that should contain key
static BTreeNode *bt_node_search(BTreeNode *node, uint64_t key)
{
    uint64_t i, key_count;
    key_count = bt_node_get_key_count(node);
    if(bt_node_get_type(node) == BT_NODE_TYPE_LEAF)
        return node;
    for(i = 0; i < key_count; i++)
    {
        if (key <= bt_node_get_key(node, i))
        {
            return bt_node_search(node->children[i], key);
        }
    }
    return bt_node_search(node->children[i], key);
}


void bt_insert(BTree *bt, uint64_t key, uint64_t value)
{
    BTreeNode  *leaf;

    // Perform a search to determine what bucket the new record should go into.
    leaf = bt_node_search(bt->root, key);
    
    
    if(bt_node_get_key_count(leaf) == bt->max_keys)
    {
        // The bucket is full, do split before insert.
        bt_split(bt, leaf);
    }

    bt_node_leaf_insert(leaf, key, value);
    
    
    assert(bt_node_get_key_count(leaf) <= bt->max_keys);
}