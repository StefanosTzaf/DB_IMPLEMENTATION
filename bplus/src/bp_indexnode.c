#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_indexnode.h"
#include "bp_file.h"
#include "record.h"

//δημιουργια ενος κενου μπλοκ ευρετηριου
int create_index_node(int file_desc, BPLUS_INFO* bplus_info){
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_BF(BF_AllocateBlock(file_desc, block));

    void* data = BF_Block_GetData(block);

    //δημιουργια μεταδεδομενων του μπλοκ ευρετηριου
    BPLUS_INDEX_NODE index_node;
    index_node.num_keys = 0;
    index_node.block_id = bplus_info->num_of_blocks;

    //αντιγραφη των μεταδεδομενων στο block
    memcpy(data, &index_node, sizeof(BPLUS_INDEX_NODE));

    BF_Block_SetDirty(block); //τροποποιησαμε το block αρα το κανουμε dirty
    CALL_BF(BF_UnpinBlock(block)); //δεν το χρειαζομαστε πια
    BF_Block_Destroy(&block); //καταστροφη του δεικτη στο block


    bplus_info->num_of_blocks++; //αυξανουμε τον αριθμο των μπλοκ
    


    return index_node.block_id;
}


BPLUS_INDEX_NODE* get_metadata_indexnode(int file_desc, int block_id){
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_BF(BF_GetBlock(file_desc, block_id, block));

    void* data = BF_Block_GetData(block);

    BPLUS_INDEX_NODE* index_node;

    //αντιγραφη των μεταδεδομενων απο το block στην δομη index_node
    memcpy(index_node, data, sizeof(BPLUS_INDEX_NODE));

    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return index_node;
}

