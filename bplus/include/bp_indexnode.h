#ifndef BP_INDEX_NODE_H
#define BP_INDEX_NODE_H
#include <record.h>
#include <bf.h>
#include <bp_file.h>


typedef struct{
    int num_keys; // αριθμος κλειδιων
    int block_id; // id του block
    int parent_id; // id του γονεα
}BPLUS_INDEX_NODE;

int create_index_node(int file_desc, BPLUS_INFO* bplus_info){
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_BF(BF_AllocateBlock(file_desc, block));

    void* data = BF_Block_GetData(block);

    //δημιουργια μεταδεδομενων του μπλοκ ευρετηριου
    BPLUS_INDEX_NODE index_node;
    index_node.num_keys = 0;
    index_node.block_id = bplus_info->num_of_blocks;

    memcpy(data, &index_node, sizeof(BPLUS_INDEX_NODE));

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return index_node.block_id;
}

#endif