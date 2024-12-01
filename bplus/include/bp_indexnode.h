#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include <record.h>
#include <bf.h>
#include <bp_file.h>
#include <bp_datanode.h>



typedef struct{
    bool is_root; // αν ειναι ριζα (για να σταματήσει η αναδρομή στο insert)
    int num_keys; // αριθμος κλειδιων (ετσι μπορούμε να βρούμε και των αριθμό pointer)
    int block_id; // id του block
    int parent_id; // id του γονεα

}BPLUS_INDEX_NODE;

int create_index_node(int file_desc, BPLUS_INFO* bplus_info);
BPLUS_INDEX_NODE* get_metadata_indexnode(int file_desc, int block_id);
void insert_key_indexnode(int fd, int id_index_node, BPLUS_INFO* bplus_info, int key, int child_id);
void print_index_node(int fd, int id);


#endif