#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include <record.h>
#include <bf.h>
#include <bp_file.h>
#include <bp_datanode.h>



typedef struct{
    int is_data_node;
    int num_keys; // αριθμος κλειδιων (ετσι μπορούμε να βρούμε και των αριθμό pointer)
    int block_id; // id του block
    int parent_id; // id του γονεα

}BPLUS_INDEX_NODE;

int create_index_node(int file_desc, BPLUS_INFO* bplus_info);
BPLUS_INDEX_NODE* get_metadata_indexnode(int file_desc, int block_id);
void insert_key_indexnode(int fd, int id_index_node, BPLUS_INFO* bplus_info, int key, int left_child_id, int right_child_id);
void print_index_node(int fd, int id);
int split_index_node(int fd, BPLUS_INFO* bplus_info, int index_node_id, int key_to_insert, int child_id);
bool is_full_indexnode(int fd, int id, BPLUS_INFO* bplus_info);
void update_parents(int fd, BPLUS_INFO* bplus_info, int parent_id);


#endif