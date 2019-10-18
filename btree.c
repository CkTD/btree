#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "btree.h"
#include "list.h"


#include <stdio.h>


# define FILE_MAGIC 0xbbbbbbbb
/*

Files Structure:
                   +-------+--------+---------+--------+--------+
    block id       |   0   |    1   |    2    |    3   |  ..... |
                   +-------+--------+---------+--------+--------+
    block countent | Meta  |  NODE  |   NODE  |  NODE  |  ..... |
                   +-------+--------+---------+--------+--------+

    each block have same size, blk0 -> Meta Block


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
    uint64_t magic;
    // fixed content
    uint64_t order;
    uint64_t blk_size;
    uint64_t root_blkid;
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
    uint64_t key_counts;
    uint64_t parent_blkid;
    uint64_t left_sibling_blkid;
    uint64_t right_sibling_blkid;
    // key and pointers is decided by order of the tree:
    // for i = 0:order-2
    //     uint64_t child_i_index or value
    //     uint64_t key_i     
    // and one extra:
    // uint64_t child_order-1_index 
      

    // sizeof(key and pointers) = (order * 2 - 1 ) * 64
    // block size = sizeof(key and pointers) + sizeof(BTreeNodeBlk)
} BTreeNodeBlk;

struct BTreeNode {
    BTree                 *tree;
    BTreeNodeBlk          *blk;     
    uint64_t               blkid;
      
    struct list_head       chain;      // new or dirty or deleted?
    struct list_head      *state;      // which chain this node in? (the block status)    
};
typedef struct BTreeNode BTreeNode;

struct _BTree {
    const char  *  file_path;
    int            file_fd;

    BTreeNode     *root;
    BTreeMeta     *meta;

    // keep node states, only flush new and modified node's blk
    struct list_head     new_node_chain;
    struct list_head     deleted_node_chain;
    struct list_head     dirty_node_chain;

    // calculated by meta for convenience
    uint64_t       min_keys; //for none root
    uint64_t       max_keys; //for none root

    // node will be loaded to memory first time it is accessed
    // we keep blkidx to node here, not loaded node have value NULL.
    BTreeNode    **blkid_to_node;
};

static BTreeValues *bt_values_new();
static void bt_values_put_value(BTreeValues *values, uint64_t value);

static uint64_t bt_next_blkid(BTree *bt);
static uint64_t bt_get_order(BTree *bt);
static uint64_t bt_get_max_keys(BTree *bt);
static uint64_t bt_get_min_keys(BTree *bt);
static uint64_t bt_get_blksize(BTree *bt);
static void bt_load_blk(BTree *bt, void *dst, uint64_t index);
static void bt_set_node(BTree *bt, uint64_t blkid, BTreeNode *node);
static BTreeNode *bt_get_node(BTree *bt, uint64_t blkid);




/////////////////////////////////////////////////
//  BTreeMetas
/////////////////////////////////////////////////

static BTreeMetaBlk *bt_meta_blk_new_empty(uint64_t order, uint64_t blksize)
{
    BTreeMetaBlk * blk;
    
    blk = (BTreeMetaBlk *)malloc(blksize);
    // prevent valgrind complain Syscall param write(buf) points to uninitialised byte(s)
    memset(blk, 0, blksize);    
    blk->magic = FILE_MAGIC;
    blk->order = order;
    blk->blk_size = blksize;
    blk->blk_counts = 1;
    blk->max_blkid = 0;
    blk->root_blkid = 1;
    return blk;
}

static BTreeMetaBlk *bt_meta_blk_new_from_file(BTree *bt)
{
    BTreeMetaBlk   *blk;

    blk = (BTreeMetaBlk *)malloc(sizeof(BTreeMetaBlk));
    bt_load_blk(bt, blk, 0);    // metablk has blkid 0
    assert(blk->magic == FILE_MAGIC);   // bad tree file
    blk = (BTreeMetaBlk *)realloc(blk, blk->blk_size);

    memset((char *)blk + sizeof(BTreeMetaBlk), 0, blk->blk_size - sizeof(BTreeMetaBlk));

    return blk;
}

static void bt_meta_blk_destory(BTreeMetaBlk *blk)
{
    free(blk);
}

static BTreeMeta *bt_meta_new_empty(uint64_t order, uint64_t blksize)
{
    BTreeMeta    *meta;
    BTreeMetaBlk *blk;

    blk = bt_meta_blk_new_empty(order, blksize);
    meta = (BTreeMeta *)malloc(sizeof(BTreeMeta));
    meta->dirty = 1;
    meta->blk = blk;

    return meta;
}

static void bt_meta_destory(BTreeMeta *meta)
{
    bt_meta_blk_destory(meta->blk);
    free(meta);
}

static BTreeMeta *bt_meta_new_from_file(BTree *bt)
{
    BTreeMeta      *meta;
    BTreeMetaBlk   *blk;

    blk = bt_meta_blk_new_from_file(bt);
    meta = (BTreeMeta *)malloc(sizeof(BTreeMeta));
    meta->dirty = 0;
    meta->blk = blk;

    return meta;
}

static void bt_meta_set_root_blkid(BTreeMeta *meta, uint64_t root_blkid)
{
    meta->dirty = 1;
    meta->blk->root_blkid = root_blkid;
}

static void bt_set_root_blkid(BTree *bt, uint64_t root_blkid)
{
    bt_meta_set_root_blkid(bt->meta, root_blkid);
}

static uint64_t bt_meta_get_root_blkid(BTreeMeta *meta)
{
    return meta->blk->root_blkid;
}

static uint64_t bt_meta_get_order(BTreeMeta *meta)
{
    return meta->blk->order;
}

static uint64_t bt_meta_get_blksize(BTreeMeta *meta)
{
    return meta->blk->blk_size;
}

static uint64_t bt_meta_get_maxblkid(BTreeMeta *meta)
{
    return meta->blk->max_blkid;
}

static uint64_t bt_meta_next_blkid(BTreeMeta *meta)
{
    meta->dirty = 1;
    meta->blk->blk_counts ++;
    return ++meta->blk->max_blkid;
}




/////////////////////////////////////////////////
//  BTreeNodeBlk
/////////////////////////////////////////////////

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

static uint64_t bt_node_blk_get_parent_blkid(BTreeNodeBlk *blk)
{
    return blk->parent_blkid;
}

static uint64_t bt_node_blk_get_left_sibling_blkid(BTreeNodeBlk *blk)
{
    return blk->left_sibling_blkid;
}

static uint64_t bt_node_blk_get_right_sibling_blkid(BTreeNodeBlk *blk)
{
    return blk->right_sibling_blkid;
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
    *(uint64_t *)
        ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2 + 1)) = key;
}

// index range from [0, key_counts - 1]
static uint64_t bt_node_blk_get_value(BTreeNodeBlk *blk, uint64_t index)
{
    assert(blk->type & BT_NODE_TYPE_LEAF);
    assert(index < blk->key_counts);
    return *(uint64_t *)
            ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2));

}

static void bt_node_blk_set_value(BTreeNodeBlk *blk, uint64_t index, uint64_t value)
{
    assert(blk->type & BT_NODE_TYPE_LEAF);
    assert(index < blk->key_counts);
    *(uint64_t *)
        ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2)) = value;

}

// index range from [0, key_counts]
static uint64_t bt_node_blk_get_child_blkid(BTreeNodeBlk *blk, uint64_t index)
{
    assert(!(blk->type & BT_NODE_TYPE_LEAF));
    assert(index <= blk->key_counts);
    return *(uint64_t *)
            ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2));
}

static void bt_node_blk_set_child_blkid(BTreeNodeBlk *blk, uint64_t index, uint64_t blkid)
{
    assert(!(blk->type & BT_NODE_TYPE_LEAF));
    *(uint64_t *)
        ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2)) = blkid;
}

// copy key/value or key/idx pair from srcto dst
// caller do link.
static void bt_node_blk_copy_half_pairs(BTreeNodeBlk *dst, BTreeNodeBlk *src, uint64_t start, uint64_t n)
{
    uint64_t content_offset;

    //assert(start + n <= max_keys);
    content_offset = sizeof(BTreeNodeBlk);
    n = n * 2 * sizeof(uint64_t);
    if (!(dst->type & BT_NODE_TYPE_LEAF))
        n += sizeof(uint64_t);
    memcpy((char*) dst + content_offset , (char *)src + content_offset + sizeof(int64_t) * (start * 2), n);
}

static void bt_node_blk_link_sibling(BTreeNodeBlk *left, BTreeNodeBlk *right, uint64_t left_idx, uint64_t right_idx)
{
    left->right_sibling_blkid = right_idx;
    right->left_sibling_blkid = left_idx;
}

static void bt_node_blk_link_parent_child(BTreeNodeBlk *parent, BTreeNodeBlk *child, uint64_t parent_blkid, uint64_t child_blkid, uint64_t index)
{
    bt_node_blk_set_child_blkid(parent, index, child_blkid);
    child->parent_blkid = parent_blkid;
}


static uint64_t bt_node_blk_leaf_search(BTreeNodeBlk *blk, uint64_t key)
{
    uint64_t pos;

    pos = 0;
                                                            // not <= here. we want index of *key* tha equal to key
    while (pos < blk->key_counts && bt_node_blk_get_key(blk, pos) < key)
        pos ++;

    return pos;
}

// insert a key,value pair into a LEAF node blk
// The allocated blk in memory can hold one more (key/value)!
// after this function return, the blk can hold one more key than max_keys
static void bt_node_blk_leaf_insert(BTreeNodeBlk *blk, uint64_t key, uint64_t value)
{
    uint64_t  pos;
    char     *src;
    char     *dest;
    uint64_t  n;

    assert(blk->type & BT_NODE_TYPE_LEAF);

    pos = bt_node_blk_leaf_search(blk, key);
    
    src = (char *)blk + sizeof(BTreeNodeBlk) + pos * sizeof(uint64_t) * 2;
    dest = src + 2 *sizeof(uint64_t);
    n = (blk->key_counts - pos) * 2 * sizeof(uint64_t);
    memmove(dest, src, n);

    *(uint64_t *)src = value;
    *((uint64_t *)(src) + 1) = key;

    blk->key_counts += 1;
}

void bt_node_blk_none_leaf_make_space(BTreeNodeBlk *blk, uint64_t index)
{
    char *src;
    char *dest;
    uint64_t n;

    assert(!(blk->type & BT_NODE_TYPE_LEAF));
    src = (char *)blk + sizeof(BTreeNodeBlk) + index * sizeof(uint64_t) * 2;
    dest = src + sizeof(uint64_t) * 2;
    n = (blk->key_counts - index) *  2 * sizeof(uint64_t) + sizeof(uint64_t);
    
    memmove(dest, src, n);
}

static BTreeNodeBlk *bt_node_blk_new_empty(uint64_t blk_size, uint64_t type)
{
    BTreeNodeBlk *blk;
    uint64_t      mem_size;
    mem_size = blk_size + sizeof(uint64_t) * 2;
    blk =  (BTreeNodeBlk *)malloc(mem_size);
    // prevent valgrind complain Syscall param write(buf) points to uninitialised byte(s)
    memset(blk, 0, blk_size);
    blk->type = type;
    blk->parent_blkid = 0;
    blk->left_sibling_blkid = 0;
    blk->right_sibling_blkid = 0;
    blk->key_counts = 0;
}

static BTreeNodeBlk *bt_node_blk_new_from_file(BTree *bt, uint64_t blkid)
{
    BTreeNodeBlk *blk;
    uint64_t      blksize;

    blksize = bt_get_blksize(bt);
    blk =  (BTreeNodeBlk *)malloc(blksize + sizeof(uint64_t) * 2);
    memset(blk, 0, blksize);
    bt_load_blk(bt, blk, blkid);
    return blk;
}

static void bt_node_blk_destory(BTreeNodeBlk *blk)
{
    free(blk);
}




/////////////////////////////////////////////////
//  BTreeNode
/////////////////////////////////////////////////

static void bt_node_marked_dirty(BTreeNode *node)
{
    assert(node->state != &node->tree->deleted_node_chain);

    if(node->state == &node->tree->new_node_chain)
        return;
    if(node->state == &node->tree->dirty_node_chain)
        return;
    
    node->state = &node->tree->dirty_node_chain;
    list_add(&node->chain, &node->tree->dirty_node_chain);
}

static void bt_node_marked_new(BTreeNode *node)
{
    assert(node->state == NULL);
    node->state = &node->tree->new_node_chain;
    list_add(&node->chain, &node->tree->new_node_chain);
}

uint64_t bt_node_get_blkid(BTreeNode *node)
{
    return node->blkid;
}

int bt_node_get_type(BTreeNode *node)
{
    return bt_node_blk_get_type(node->blk);
}

void bt_node_set_type(BTreeNode *node, int type)
{
    bt_node_blk_set_type(node->blk ,type);
    bt_node_marked_dirty(node);
}

static uint64_t bt_node_get_key(BTreeNode *node, uint64_t index)
{
    return bt_node_blk_get_key(node->blk, index);
}

static void bt_node_set_key(BTreeNode *node, uint64_t index, uint64_t key)
{
    bt_node_blk_set_key(node->blk, index, key);
    bt_node_marked_dirty(node);
}

static uint64_t bt_node_get_value(BTreeNode *node, uint64_t index)
{
    return bt_node_blk_get_value(node->blk, index);
}

static uint64_t bt_node_set_value(BTreeNode *node, uint64_t index, uint64_t value)
{
    bt_node_blk_set_value(node->blk, index, value);

    bt_node_marked_dirty(node);
}

static uint64_t bt_node_get_key_count(BTreeNode *node)
{
    return bt_node_blk_get_key_count(node->blk);
}

static void bt_node_set_key_count(BTreeNode *node, uint64_t count)
{
    bt_node_blk_set_key_count(node->blk, count);
    bt_node_marked_dirty(node);
}

static BTreeNode *bt_node_get_left_sibling(BTreeNode *node)
{
    uint64_t blkid;

    blkid = bt_node_blk_get_left_sibling_blkid(node->blk);
    if (blkid == 0)
        return NULL;

    return bt_get_node(node->tree, blkid);
}

static BTreeNode *bt_node_get_right_sibling(BTreeNode *node)
{
    uint64_t blkid;

    blkid = bt_node_blk_get_right_sibling_blkid(node->blk);
    if (blkid == 0)
        return NULL;
        
    return bt_get_node(node->tree, blkid);
}

static BTreeNode *bt_node_get_parent(BTreeNode *node)
{
    uint64_t blkid;

    blkid = bt_node_blk_get_parent_blkid(node->blk);
    if (blkid == 0)
        return NULL;
        
    return bt_get_node(node->tree, blkid);
}

static BTreeNode *bt_node_get_child(BTreeNode *node, uint64_t index)
{
    uint64_t blkid;
    blkid = bt_node_blk_get_child_blkid(node->blk, index);
    assert(blkid);

    return bt_get_node(node->tree, blkid);
}

static void bt_node_link_sibling(BTreeNode *left, BTreeNode *right)
{
    if (!left || !right)
        return;

    bt_node_blk_link_sibling(left->blk, right->blk, left->blkid, right->blkid);

    bt_node_marked_dirty(left);
    bt_node_marked_dirty(right);
}

static void bt_node_link_parent_child(BTreeNode *parent, BTreeNode *child, uint64_t index)
{
    bt_node_blk_link_parent_child(parent->blk, child->blk, parent->blkid, child->blkid, index);

    bt_node_marked_dirty(parent);
    bt_node_marked_dirty(child);
}

static void bt_node_move_half_content(BTreeNode* new, BTreeNode *node, uint64_t min_keys)
{
    uint64_t      i;
    uint64_t      start;

    start = min_keys + 1;

    // copy pairs
    bt_node_blk_copy_half_pairs(new->blk, node->blk, start, min_keys);
    
    // if the node is not LEAF, link parent - child
    if(!(bt_node_get_type(node) & BT_NODE_TYPE_LEAF))
    {
        // copy one more.
        for( i = 0; i <= min_keys ; i ++ )
        {
            bt_node_link_parent_child(new,bt_node_get_child(node, i+start), i);
        }
    }

    // set key counts
    bt_node_set_key_count(new, min_keys);
    if(bt_node_get_type(node) & BT_NODE_TYPE_LEAF)
    {
        bt_node_set_key_count(node, min_keys + 1);   
    }
    else
    {
        bt_node_set_key_count(node, min_keys);   
    }

    bt_node_marked_dirty(node);
    bt_node_marked_dirty(new);
}

static void bt_node_set_key_and_link(BTreeNode *parent, uint64_t index, uint64_t key, BTreeNode *left, BTreeNode *right)
{
    bt_node_set_key(parent, index, key);
    bt_node_link_parent_child(parent, left, index);
    bt_node_link_parent_child(parent, right, index + 1);
}

static uint64_t bt_node_get_child_index(BTreeNode *node, BTreeNode *child)
{
    uint64_t index;

    assert(!(bt_node_get_type(node) & BT_NODE_TYPE_LEAF));
    for (index = 0; index <= bt_node_get_key_count(node); index++)
    {
        if (bt_node_get_child(node, index) == child)
            return index;
    }
    assert(0);
}

static BTreeNode *bt_node_new_empty(BTree *tree, uint64_t type)
{
    BTreeNode    *node;
    BTreeNodeBlk *blk;

    blk = bt_node_blk_new_empty(bt_get_blksize(tree), type);
    node = (BTreeNode *)malloc(sizeof(BTreeNode));
    node->blkid = bt_next_blkid(tree);
    node->blk = blk;
    node->tree = tree;
    node->state = NULL;
    bt_node_marked_new(node); // init state and chain

    bt_set_node(tree, node->blkid, node);
    
    return node;
}

static BTreeNode *bt_node_new_from_file(BTree *bt, uint64_t blkid)
{
    BTreeNode    *node;
    BTreeNodeBlk *blk;
    blk =  bt_node_blk_new_from_file(bt, blkid);
    node = (BTreeNode *)malloc(sizeof(BTreeNode));
    node->blk = blk;
    node->blkid = blkid;
    node->tree = bt;
    node->state = NULL;     // clean node.

    bt_set_node(bt, blkid, node);

    return node;
}

static BTreeNode *bt_node_new_root_with_one_key(BTree *bt, uint64_t split_key, BTreeNode *left, BTreeNode *right)
{
    BTreeNode    *new_root;

    new_root = bt_node_new_empty(bt, BT_NODE_TYPE_ROOT);

    bt_node_set_key_and_link(new_root, 0, split_key, left, right);
    bt_node_set_key_count(new_root , 1);

    bt->root = new_root;
    bt_set_root_blkid(bt, new_root->blkid);

    return new_root;
}

// cut the overfull(one more key than max_keys) node in to half. create and return new node.
static BTreeNode *bt_node_cut(BTreeNode *node, uint64_t *split_key)
{
    BTree        *tree;
    uint64_t      type;
    BTreeNode    *new;
    BTreeNodeBlk *blk;
    uint64_t      min_keys, max_keys, blk_size;

    tree = node->tree;
    min_keys = bt_get_min_keys(tree);
    max_keys = bt_get_max_keys(tree);
    blk_size = bt_get_blksize(tree);

    assert(bt_node_get_key_count(node) == max_keys + 1);

    if(bt_node_get_type(node) & BT_NODE_TYPE_LEAF)
    {
        type = BT_NODE_TYPE_LEAF;
    }
    else
    {
        type = BT_NODE_TYPE_INTERNAL;
    }

    new = bt_node_new_empty(tree, type);
    *split_key = bt_node_get_key(node, min_keys);
    // not link internal node in same layer?
    //if(bt_node_get_type(node) & BT_NODE_TYPE_LEAF)
    //{
        bt_node_link_sibling(new, bt_node_get_right_sibling(node));
        bt_node_link_sibling(node, new);
    //}
    bt_node_move_half_content(new, node, min_keys);
 

    bt_node_set_type(node, type);
    return new;
}

static void bt_node_none_leaf_insert(BTree * bt, BTreeNode *parent, BTreeNode *left, BTreeNode *new, uint64_t key);
static void bt_node_none_leaf_split(BTree *bt, BTreeNode *node)
{
    uint64_t      split_key;
    uint64_t      type;
    BTreeNode    *new;

    assert(!(bt_node_get_type(node) & BT_NODE_TYPE_LEAF));
    assert(bt_node_get_key_count(node) == bt_get_max_keys(bt) + 1);

    type = bt_node_get_type(node);
    new = bt_node_cut(node, &split_key);

    if (type & BT_NODE_TYPE_ROOT)
    {
        // Root(none leaft) split
        bt_node_new_root_with_one_key(bt, split_key, node, new);
    }
    else
    {   
        bt_node_none_leaf_insert(bt, bt_node_get_parent(node), node, new, split_key);
        //bt_node_none_leaf_insert(bt, bt_node_get_parent(node), node, new, split_key);
    }
}

static void bt_node_none_leaf_make_space(BTreeNode *node, uint64_t index)
{
    uint64_t last;
    uint64_t n;
    uint64_t i;

    last = bt_node_get_key_count(node);
    n = last - index + 1;

    bt_node_blk_none_leaf_make_space(node->blk, index);
}

static void bt_node_none_leaf_insert(BTree * bt, BTreeNode *parent, BTreeNode *left, BTreeNode *new, uint64_t key)
{
    uint64_t index;

    assert(parent);
    assert(!(bt_node_get_type(parent) & BT_NODE_TYPE_LEAF));
    assert(bt_node_get_key_count(parent) <= bt_get_max_keys(bt) + 1);

    index = bt_node_get_child_index(parent, left);
    bt_node_none_leaf_make_space(parent, index);
    bt_node_set_key_and_link(parent, index, key, left, new);
    bt_node_set_key_count(parent, bt_node_get_key_count(parent) + 1);
    if (bt_node_get_key_count(parent) > bt_get_max_keys(bt))
        bt_node_none_leaf_split(bt, parent);

    bt_node_marked_dirty(left);
    bt_node_marked_dirty(new);
    bt_node_marked_dirty(parent);
}

static void bt_node_leaf_split(BTreeNode *node)
{
    uint64_t      split_key;
    uint64_t      type;
    BTreeNode    *new;

    assert(bt_node_get_type(node) & BT_NODE_TYPE_LEAF);
    assert(bt_get_max_keys(node->tree) + 1 == bt_node_get_key_count(node));
    
    type = bt_node_get_type(node);
    new = bt_node_cut(node, &split_key);

    if (type & BT_NODE_TYPE_ROOT)
    {
        // Root is the only leaf node
        bt_node_new_root_with_one_key(node->tree, split_key, node, new);
    }
    else
    {
        bt_node_none_leaf_insert(node->tree, bt_node_get_parent(node), node, new, split_key);
    } 
}
// insert a key,value pair into a LEAF node
static void bt_node_leaf_insert(BTreeNode *leaf, uint64_t key, uint64_t value)
{
    bt_node_marked_dirty(leaf);
    bt_node_blk_leaf_insert(leaf->blk, key, value);    
    if(bt_node_get_key_count(leaf) > bt_get_max_keys(leaf->tree))
    {
        // The bucket is full, do split before insert.
        bt_node_leaf_split(leaf);
    }
}

// given a node, return the leaf derived from node that should contain key
static BTreeNode *bt_node_search(BTreeNode *node, uint64_t key)
{
    uint64_t i, key_count;

    if(bt_node_get_type(node) & BT_NODE_TYPE_LEAF)
        return node;

    key_count = bt_node_get_key_count(node);
    for(i = 0; i < key_count; i++)
    {
        if (key <= bt_node_get_key(node, i))
        {
            return bt_node_search(bt_node_get_child(node, i), key);
        }
    }
    return bt_node_search(bt_node_get_child(node, i), key);
}

// return 1 iff the caller need to check next sibling elss 0
// thai is          #keys_put_in_to_value < limit && 
//                  last key in this node has key = key
int bt_node_leaf_fetch_values(BTreeNode *leaf, BTreeValues *values, uint64_t limit, uint64_t key)
{
    uint64_t count;
    uint64_t index;
    uint64_t keys_in_node;
    uint64_t k, v;

    count = 0;
    keys_in_node = bt_node_blk_get_key_count(leaf->blk);

    // empty tree!
    if(keys_in_node == 0)
    {
        assert(bt_node_get_type(leaf) & BT_NODE_TYPE_ROOT);
        return 0;
    }

    index = bt_node_blk_leaf_search(leaf->blk, key);

    for(; index < keys_in_node; index ++)
    {
        k = bt_node_blk_get_key(leaf->blk, index);
        if(k != key)
            break;
        v = bt_node_blk_get_value(leaf->blk, index);
        bt_values_put_value(values, v);

        count ++;
        if(count == limit)
            break;
    }

    if((index == keys_in_node) && (count < limit))
        return 1;
    else
        return 0;
}

static void bt_node_destory(BTreeNode *node)
{
    bt_node_blk_destory(node->blk);
    free(node);
}




/////////////////////////////////////////////////
//  BTreeValues(result for search in tree)
/////////////////////////////////////////////////
struct _BTreeValues
{
    uint64_t  counts;
    uint64_t *values;
};

static BTreeValues *bt_values_new()
{
    BTreeValues *values;
    values = (BTreeValues *)malloc(sizeof(BTreeValues));

    values->counts = 0;
    values->values = NULL;
}

void bt_values_destory(BTreeValues *values)
{
    free(values->values);
    free(values);
}

uint64_t bt_values_get_count(BTreeValues *values)
{
    return values->counts;
}

static void bt_values_put_value(BTreeValues *values, uint64_t value)
{
    values->counts ++;
    // TODO : efficent allocate
    values->values = realloc(values->values, values->counts * sizeof(uint64_t));
    values->values[values->counts - 1] = value;
}

uint64_t bt_values_get_value(BTreeValues *values, uint64_t index)
{
    assert(index < values->counts);
    return values->values[index];
}




/////////////////////////////////////////////////
//  BTree
/////////////////////////////////////////////////

static uint64_t bt_get_max_keys(BTree *bt)
{
    return bt->max_keys;
}

static uint64_t bt_get_min_keys(BTree *bt)
{
    return bt->min_keys;
}

static uint64_t bt_get_order(BTree *bt)
{
    return bt_meta_get_order(bt->meta);
}

static uint64_t bt_get_blksize(BTree *bt)
{
    return bt_meta_get_blksize(bt->meta);
}

static uint64_t bt_get_max_blkid(BTree *bt)
{
    return bt_meta_get_maxblkid(bt->meta);
}

static uint64_t bt_next_blkid(BTree *bt)
{
    return bt_meta_next_blkid(bt->meta);
}

static uint64_t bt_get_root_blkid(BTree *bt)
{
    return bt_meta_get_root_blkid(bt->meta);
}

static void bt_set_node(BTree *bt, uint64_t blkid, BTreeNode *node)
{

    uint64_t max_blkid;
    max_blkid = bt_get_max_blkid(bt);

    assert(blkid <= max_blkid);


    // TODO....  only realloc if really needed
    if(blkid == max_blkid)
        bt->blkid_to_node = (BTreeNode **)realloc(bt->blkid_to_node, sizeof(BTreeNode *) * (max_blkid + 1));
    bt->blkid_to_node[blkid] = node;
}

static BTreeNode *bt_get_node(BTree *bt, uint64_t blkid)
{
    uint64_t max_blkid;
    max_blkid = bt_get_max_blkid(bt);

    assert(blkid <= max_blkid);

    // TODO....  only realloc if really needed
    if (bt->blkid_to_node[blkid] == NULL)
        bt_node_new_from_file(bt, blkid);

    return bt->blkid_to_node[blkid];
}

static void bt_open_file(BTree *bt)
{
    if(bt->file_fd == -1)
    {
        bt->file_fd = open(bt->file_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    }
    assert(bt->file_fd != -1);
}

static void bt_load_blk(BTree *bt, void *dst, uint64_t blkid)
{
    ssize_t   rtv;
    uint64_t  blksize;

    // when load Meta block, blksize is unknown
    if (blkid == 0)
        blksize = sizeof(BTreeMetaBlk);
    else
        blksize = bt_get_blksize(bt);
    
    bt_open_file(bt);
    rtv = lseek(bt->file_fd, blkid * blksize, SEEK_SET);
    assert(rtv == blkid * blksize);
    rtv = read(bt->file_fd, dst, blksize);
    assert(rtv == blksize);

}

static void bt_store_blk(BTree *bt, uint64_t blkid)
{
    ssize_t   rtv;
    uint64_t  blksize;
    void     *blk;

    if(blkid == 0)
        blk = (void *)bt->meta->blk;
    else
        blk = (void *)bt->blkid_to_node[blkid]->blk;
    

    blksize = bt_get_blksize(bt);
    bt_open_file(bt);
    rtv = lseek(bt->file_fd, blkid * blksize, SEEK_SET);
    assert(rtv == blkid * blksize);
    rtv = write(bt->file_fd, blk, blksize);
    assert(rtv == blksize);
}

static void bt_load_meta(BTree *bt)
{
    BTreeMeta   *meta;
    
    meta = bt_meta_new_from_file(bt);

    bt->meta = meta;
}

static void bt_load_root(BTree *bt)
{
    uint64_t      root_blk_id;
    BTreeNode    *root;
   
    root_blk_id = bt_get_root_blkid(bt);
    root = bt_node_new_from_file(bt, root_blk_id);
    
    bt->root = root;
}

static BTree *bt_new_from_file(const char *file)
{
    BTree    *bt;
    uint64_t  order;
    
    bt = (BTree *)malloc(sizeof(BTree));
    INIT_LIST_HEAD(&bt->deleted_node_chain);
    INIT_LIST_HEAD(&bt->new_node_chain);
    INIT_LIST_HEAD(&bt->dirty_node_chain);
    bt->file_path = file;
    bt->file_fd = -1;
    bt_load_meta(bt);
    order = bt_get_order(bt);
    bt->max_keys = order - 1;
    bt->min_keys = order / 2;
    // node blk id start from 1
    bt->blkid_to_node = (BTreeNode **)malloc(sizeof(BTreeNode *) * (bt_get_max_blkid(bt) + 1));
    memset(bt->blkid_to_node, 0, sizeof(BTreeNode *)*(bt_get_max_blkid(bt) + 1));

    bt_load_root(bt);

    return bt;
}

static BTree *bt_new_empty(const char *file, uint64_t order)
{
    uint64_t      blksize;
    BTree        *bt;
    
    bt = (BTree *)malloc(sizeof(BTree));
 
    INIT_LIST_HEAD(&bt->deleted_node_chain);
    INIT_LIST_HEAD(&bt->new_node_chain);
    INIT_LIST_HEAD(&bt->dirty_node_chain);

    // !!!copy file name to heap!!!
    bt->file_path = file;
    bt->file_fd = -1;
    bt->max_keys = order - 1;
    bt->min_keys = order / 2;

    blksize = sizeof(BTreeNodeBlk) + (order * 2 - 1) * sizeof(uint64_t);
    assert(blksize >= sizeof(BTreeMetaBlk));

    bt->meta = bt_meta_new_empty(order, blksize);

    bt->blkid_to_node = (BTreeNode **)malloc(sizeof(BTreeNode *) * (bt_get_max_blkid(bt) + 1));
    memset(bt->blkid_to_node, 0, bt_get_max_blkid(bt) + 1);

    bt->root = bt_node_new_empty(bt, BT_NODE_TYPE_LEAF | BT_NODE_TYPE_ROOT);

    return bt;
}

void bt_flush(BTree *bt)
{
    BTreeNode *node;

    if(bt->meta->dirty)
        bt_store_blk(bt, 0);

    list_for_each_entry(node, &bt->new_node_chain, chain)
        bt_store_blk(bt, bt_node_get_blkid(node));

    list_for_each_entry(node, &bt->dirty_node_chain, chain)
        bt_store_blk(bt, bt_node_get_blkid(node));

}

void bt_insert(BTree *bt, uint64_t key, uint64_t value)
{
    BTreeNode  *leaf;

    leaf = bt_node_search(bt->root, key);
    bt_node_leaf_insert(leaf, key, value);   
    
    assert(bt_node_get_key_count(leaf) <= bt->max_keys);
}

BTreeValues *bt_search(BTree *bt, uint64_t limit, uint64_t key)
{
    BTreeNode   *leaf;
    BTreeValues *values;
    int          remind;
    int          b_continue;

    values = bt_values_new();
    leaf = bt_node_search(bt->root, key);

    do
    {
        remind = limit - bt_values_get_count(values);
        b_continue = bt_node_leaf_fetch_values(leaf, values, remind, key);

        leaf = bt_node_get_right_sibling(leaf);
        if(leaf == NULL)
            break;

    } while (b_continue);

    return values;
}

BTree * bt_open(BTreeOpenFlag flag)
{
    assert(flag.file);
    if(access(flag.file, F_OK) != -1)
    {
        if(flag.error_if_exist)
            return NULL;
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

void bt_close(BTree *bt)
{
    uint64_t i;

    // flush tree to disk
    bt_flush(bt);

    // destory loaded node
    for(i = 1; i <= bt_get_max_blkid(bt); i++)
    {
        if(bt->blkid_to_node[i])
            bt_node_destory(bt->blkid_to_node[i]);
    }
    // destory meta     
    bt_meta_destory(bt->meta);


    free(bt->blkid_to_node);
    free(bt);
}





/////////////////////////////////////////////////
//  print tree
/////////////////////////////////////////////////

void bt_node_print(BTreeNode* node)
{
    int i;
    printf("---------- Node(id: %d, %x) ----------\n[", node->blkid, node);
    if(node->blk->type & BT_NODE_TYPE_INTERNAL)
        printf("INTERNAL ");
    if(node->blk->type & BT_NODE_TYPE_LEAF)
        printf("LEAF ");
    if(node->blk->type & BT_NODE_TYPE_ROOT)
        printf("ROOT");


    printf("]P: %d, L: %d, R: %d, #K: %d\n|",node->blk->parent_blkid, node->blk->left_sibling_blkid, node->blk->right_sibling_blkid, node->blk->key_counts);
    if(!(node->blk->type & BT_NODE_TYPE_LEAF)) 
    {
        for (i = 0; i < node->blk->key_counts; i++)
        {
            printf(" I_%d (%d) K_%d (%d) | ", i, bt_node_get_blkid(bt_node_get_child(node, i)), i, bt_node_get_key(node, i));
        }

        printf("I_%d (%d) |\n", node->blk->key_counts, bt_node_get_blkid(bt_node_get_child(node, i)));
        if(node->blk->key_counts)
        for(i=0;i <= node->blk->key_counts;i++)
        {
            bt_node_print(bt_node_get_child(node, i));
        }
    }
    else
    {
        for (i = 0; i < node->blk->key_counts; i++)
        {
            printf(" V_%d (%d) K_%d (%d) | ", i, bt_node_get_value(node, i), i, bt_node_get_key(node, i));
        }
        printf("\n");
    }
}

void bt_print(BTree *bt)
{
    printf("---------- Meta ----------\n");
    printf("order:    %d\n", bt_get_order(bt));
    printf("blksize:  %d\n", bt->meta->blk->blk_size);
    printf("blkcount: %d\n", bt->meta->blk->blk_counts);
    printf("maxblkid: %d\n", bt->meta->blk->max_blkid);
    printf("rootblk:  %d\n", bt->meta->blk->root_blkid);
    bt_node_print(bt->root);

    BTreeNode *node;

    printf("new blks: ");
    list_for_each_entry(node, &bt->new_node_chain, chain)
    {
        printf("%d ",node->blkid);
    }
    printf("\n");

    printf("dirty blks: ");
    list_for_each_entry(node, &bt->dirty_node_chain, chain)
    {
        printf("%d ",node->blkid);
    }
    printf("\n");
}