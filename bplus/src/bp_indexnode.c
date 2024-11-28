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

    return index_node.block_id;
}