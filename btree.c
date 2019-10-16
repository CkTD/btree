#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>

#include "btree.h"
#include "list.h"


#include <stdio.h>

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

struct BTreeNode{
    BTreeNodeBlk   *blk;     
    uint64_t        blkid;
    int             dirty;

    BTree                 *tree;
    struct BTreeNode      *parent;
    struct BTreeNode      *left_sibling;
    struct BTreeNode      *right_sibling;
    struct BTreeNode     **children;     

    // if the node is modified
    // chain it in one of new_node_chain, deleted_node_chain, dirty_node_chain.
    struct list_head       chain;   
    // which chain this node in? (the block status)    
    struct list_head      *state;  
};

typedef struct BTreeNode BTreeNode;

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
    blk->blk_counts = 1;  // empty tree has Meta and Root block
    blk->max_blkid = 0;
    blk->root_blkid = 0;
    return blk;
}

static BTreeMeta *bt_meta_new_empty(BTreeMetaBlk *blk)
{
    BTreeMeta *meta = (BTreeMeta *)malloc(sizeof(BTreeMeta));
    meta->dirty = 1;
    meta->blk = blk;
    return meta;
}

static uint64_t bt_meta_blk_next_blkid(BTreeMetaBlk *blk)
{
    blk->blk_counts ++;
    return ++blk->max_blkid;
}

static uint64_t bt_meta_next_bklid(BTreeMeta *meta)
{
    meta->dirty = 1;
    return bt_meta_blk_next_blkid(meta->blk);
}

static uint64_t bt_meta_get_root_blkid(BTreeMeta *meta)
{
    return meta->blk->root_blkid;
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

static uint64_t bt_next_blkid(BTree *bt)
{
    return bt_meta_next_bklid(bt->meta);
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




static BTreeNodeBlk *bt_node_blk_new_empty(uint64_t blk_size, uint64_t type)
{
    BTreeNodeBlk *blk =  (BTreeNodeBlk *)malloc(blk_size + sizeof(uint64_t) * 2); 
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
static uint64_t bt_node_blk_get_child_idx(BTreeNodeBlk *blk, uint64_t index)
{
    assert(!(blk->type & BT_NODE_TYPE_LEAF));
    assert(index <= blk->key_counts);
    return *(uint64_t *)
            ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2));
}

static void bt_node_blk_set_child_idx(BTreeNodeBlk *blk, uint64_t index, uint64_t idx)
{
    assert(!(blk->type & BT_NODE_TYPE_LEAF));
    *(uint64_t *)
        ((char *)blk + sizeof(BTreeNodeBlk) + sizeof(uint64_t) * (index * 2)) = idx;
}

// copy key/value or key/idx pair from srcto dst
static void bt_node_blk_copy_content(BTreeNodeBlk *dst, BTreeNodeBlk *src, uint64_t start, uint64_t n)
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
// The allocated blk in memory can hold one more (key/value)!
void bt_node_blk_leaf_insert(BTreeNodeBlk *blk, uint64_t key, uint64_t value)
{
    uint64_t pos;
    char *src;
    char *dest;
    uint64_t n;

    assert(blk->type & BT_NODE_TYPE_LEAF);

    pos = 0;
    
    while (pos < blk->key_counts && bt_node_blk_get_key(blk, pos) <= key)
        pos ++;
    
    src = (char *)blk + sizeof(BTreeNodeBlk) + pos * sizeof(uint64_t) * 2;
    dest = src + 2 *sizeof(uint64_t);
    n = (blk->key_counts - pos) * 2 * sizeof(uint64_t);
    memmove(dest, src, n);

    *(uint64_t *)src = key;
    *((uint64_t *)(src) + 1) = value;

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


static BTreeNode *bt_node_new(BTree *tree, BTreeNodeBlk *blk)
{
    BTreeNode *node = (BTreeNode *)malloc(sizeof(BTreeNode));
    node->blkid = bt_next_blkid(tree);
    node->blk = blk;
    node->tree = tree;
    node->parent = NULL;
    node->left_sibling = NULL;
    node->right_sibling = NULL;
    node->state = NULL;
    node->children = (BTreeNode **)malloc(sizeof(BTreeNode *) * bt_get_order(tree));
    bt_node_marked_new(node);
    return node;
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
    if (!left || !right)
        return;

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
    bt_node_blk_link_parent_child(parent->blk, child->blk, parent->blkid, child->blkid, index);

    bt_node_marked_dirty(parent);
    bt_node_marked_dirty(child);
}

static void bt_node_copy_content(BTreeNode* dst, BTreeNode *src, uint64_t start, uint64_t n)
{
    bt_node_blk_copy_content(dst->blk, src->blk, start, n);
    bt_node_set_key_count(dst, n);
    bt_node_marked_dirty(dst);
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
        if (node->children[index] == child)
            return index;
    }
    assert(0);
}

BTreeNode *bt_node_new_root(BTree *bt, uint64_t split_key, BTreeNode *left, BTreeNode *right)
{
    BTreeNode    *new_root;
    BTreeNodeBlk *new_root_blk;

    new_root_blk = bt_node_blk_new_empty(bt->blk_size, BT_NODE_TYPE_ROOT);
    new_root = bt_node_new(bt, new_root_blk);

    bt_node_set_key_and_link(new_root, 0, split_key, left, right);
    bt_node_set_key_count(new_root , 1);

    bt->root = new_root;
    bt_set_root_blkid(bt, new_root->blkid);


    return new_root;
}

static BTreeNode *bt_node_cut(BTree *bt, BTreeNode *node, uint64_t *split_key)
{
    assert(bt_node_get_key_count(node) == bt_get_max_keys(bt) + 1);

    uint64_t      type;
    uint64_t      i;
    uint64_t      left_key_counts;
    BTreeNode    *new;
    BTreeNodeBlk *blk;
    if(bt_node_get_type(node) & BT_NODE_TYPE_LEAF)
    {
        type = BT_NODE_TYPE_LEAF;
        left_key_counts = bt->min_keys + 1;
    }
    else
    {
        type = BT_NODE_TYPE_INTERNAL;
        left_key_counts = bt->min_keys;
    }

    blk = bt_node_blk_new_empty(bt->blk_size, type);
    new = bt_node_new(bt, blk);
    *split_key = bt_node_get_key(node, bt->min_keys);
    bt_node_link_sibling(new, node->right_sibling);
    bt_node_link_sibling(node, new);
    bt_node_copy_content(new, node, bt->min_keys + 1, bt->min_keys);
    bt_node_set_key_count(node ,left_key_counts);    
    bt_node_set_key_count(new ,bt->min_keys);

    // the node is not LEAF, also copy children pointers ..
    if(!(bt_node_get_type(node) & BT_NODE_TYPE_LEAF))
    {
        for( i = 0; i <= bt->min_keys ; i ++ )
        {
            new->children[i] = node->children[i + bt->min_keys + 1];
            new->children[i]->parent = new;
        }
    }

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
    new = bt_node_cut(bt, node, &split_key);

    if (type & BT_NODE_TYPE_ROOT)
    {
        // Root(none leaft) split
        bt_node_new_root(bt, split_key, node, new);
    }
    else
    {
        bt_node_none_leaf_insert(bt, node->parent, node, new, split_key);
    }
}

static void bt_node_none_leaf_make_space(BTreeNode *node, uint64_t index)
{
    uint64_t last;
    uint64_t n;
    uint64_t i;

    last = bt_node_get_key_count(node);
    n = last - index + 1;

    for(i = 0; i < n; i++)
        node->children[last + 1 - i ] = node->children[last - i];

    bt_node_blk_none_leaf_make_space(node->blk, index);
}

static void bt_node_none_leaf_insert(BTree * bt, BTreeNode *parent, BTreeNode *left, BTreeNode *new, uint64_t key)
{
    uint64_t index;

    assert(parent);
    assert(!(bt_node_get_type(parent) & BT_NODE_TYPE_LEAF));
    assert(bt_node_get_key_count(parent) <= bt_get_max_keys(bt) + 1);

    index = bt_node_get_child_index(parent, left);
    //assert(parent->children[index - 1] == left);
    bt_node_none_leaf_make_space(parent, index);
    bt_node_set_key_and_link(parent, index, key, left, new);
    bt_node_set_key_count(parent, bt_node_get_key_count(parent) + 1);
    if (bt_node_get_key_count(parent) > bt_get_max_keys(bt))
        bt_node_none_leaf_split(bt, parent);

    bt_node_marked_dirty(left);
    bt_node_marked_dirty(new);
    bt_node_marked_dirty(parent);
}

static void bt_node_leaf_split(BTree *bt, BTreeNode *node)
{
    uint64_t      split_key;
    uint64_t      type;
    BTreeNode    *new;

    assert(bt_node_get_type(node) & BT_NODE_TYPE_LEAF);
    assert(bt->max_keys + 1 == bt_node_get_key_count(node));
    
    type = bt_node_get_type(node);
    new = bt_node_cut(bt, node, &split_key);

    if (type & BT_NODE_TYPE_ROOT)
    {
        // Root is the only leaf node
        bt_node_new_root(bt, split_key, node, new);
    }
    else
    {
        bt_node_none_leaf_insert(bt, node->parent, node, new, split_key);
    } 
}

// insert a key,value pair into a LEAF node
// caller check the node is not full
static void bt_node_leaf_insert(BTree *bt, BTreeNode *leaf, uint64_t key, uint64_t value)
{
    bt_node_marked_dirty(leaf);
    bt_node_blk_leaf_insert(leaf->blk, key, value);    
    if(bt_node_get_key_count(leaf) > bt->max_keys)
    {
        // The bucket is full, do split before insert.
        bt_node_leaf_split(bt, leaf);
    }
}




// given a node, return the leaf derived from node that should contain key
static BTreeNode *bt_node_search(BTreeNode *node, uint64_t key)
{
    uint64_t i, key_count;
    key_count = bt_node_get_key_count(node);
    if(bt_node_get_type(node) & BT_NODE_TYPE_LEAF)
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
    bt_node_leaf_insert(bt, leaf, key, value);   
    
    assert(bt_node_get_key_count(leaf) <= bt->max_keys);
}

static BTree *bt_new_from_file(const char *file)
{
    BTree *bt = (BTree *)malloc(sizeof(BTree));
    return bt;
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


    printf("]P: %d, L: %d, R: %d, #K: %d\n|",node->blk->parent_idx, node->blk->left_sibling_idx, node->blk->right_sibling_idx, node->blk->key_counts);
    if(!(node->blk->type & BT_NODE_TYPE_LEAF)) 
    {
        for (i = 0; i < node->blk->key_counts; i++)
        {
            printf(" I_%d (%d) K_%d (%d) | ", i, bt_node_get_child_idx(node, i), i, bt_node_get_key(node, i));
        }

        printf("I_%d (%d) |\n", node->blk->key_counts, bt_node_get_child_idx(node, i));
        if(node->blk->key_counts)
        for(i=0;i <= node->blk->key_counts;i++)
        {
            bt_node_print(node->children[i]);
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
    bt_node_print(bt->root);;

}