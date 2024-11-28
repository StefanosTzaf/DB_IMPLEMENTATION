#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include <bp_datanode.h>

// δημιουργει ενα νεο μπλοκ δεδομενων και προσθετει τα μεταδεδομενα του
int create_data_node(int file_desc, BPLUS_INFO* bplus_info){
    
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_BF(BF_AllocateBlock(file_desc, block)); 

    void* data = BF_Block_GetData(block);

    BPLUS_DATA_NODE data_node; //δημιουργια μεταδεδομενων του μπλοκ δεδομενων
    data_node.num_records = 0; //αρχικα δεν εχει εγγραφες
    data_node.block_id = bplus_info->num_of_blocks; //η αριθμηση ξεκιναει απο το 0 αρα αν 
    //εχουμε 2 μπλοκ συνολο, το id του νεου θα ειναι 2

    memcpy(data, &data_node, sizeof(BPLUS_DATA_NODE)); //αντιγραφη μεταδεδομενων στο block

    BF_Block_SetDirty(block); //αφου τροποποιηθηκε το block, το κανουμε dirty
    CALL_BF(BF_UnpinBlock(block)); //δεν το χρειαζομαστε αλλο
    BF_Block_Destroy(&block); //καταστρεφουμε τον δεικτη στο block

    return data_node.block_id; 
}


// επιστρεφει τα μεταδεδομενα ενος μπλοκ δεδομενων
BPLUS_DATA_NODE* get_metadata_datanode(int file_desc, int block_id){
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_BF(BF_GetBlock(file_desc, block_id, block));

    void* data = BF_Block_GetData(block);

    BPLUS_DATA_NODE* data_node;

    //αντιγραφουμε τα μεταδεδομενα απο το block στην δομη data_node
    memcpy(data_node, data, sizeof(BPLUS_DATA_NODE));

    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return data_node;
}



