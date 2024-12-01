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

    bplus_info->num_of_blocks++; //αυξανουμε τον αριθμο των μπλοκ  
    
    BF_Block_SetDirty(block); //τροποποιησαμε το block αρα το κανουμε dirty
    CALL_BF(BF_UnpinBlock(block)); //δεν το χρειαζομαστε πια
    BF_Block_Destroy(&block); //καταστροφη του δεικτη στο block


    return index_node.block_id;
}


BPLUS_INDEX_NODE* get_metadata_indexnode(int file_desc, int block_id){
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(file_desc, block_id, block));

    void* data = BF_Block_GetData(block);

    BPLUS_INDEX_NODE* index_node;

    //αντιγραφη των μεταδεδομενων απο το block στην δομη index_node
    index_node = (BPLUS_INDEX_NODE*)(data);

    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return index_node;
}


//προσθηκη ζευγους κλειδι-δεικτης σε ενα μπλοκ ευρετηριου
//η συναρτηση καλειται με το id του block που σπαει στα δυο, δηλαδη το αριστερο παιδι
//και κλειδι key, το μικροτερο του νεου block δηλαδη του δεξιου
void insert_key_indexnode(int fd, int id_index_node, BPLUS_INFO* bplus_info, int key, int left_child_id){

    BF_Block* block;
    BF_Block_Init(&block);
    
    //αποθηκευση του block ευρετηριου στον δεικτη block
    CALL_BF(BF_GetBlock(fd, id_index_node, block));
    void* data = BF_Block_GetData(block);
    BPLUS_INDEX_NODE* metadata_index_node = (BPLUS_INDEX_NODE*)data; //μεταδεδομενα του block

    //αποθηκευση του αριστερου παιδιου στον δεικτη left_child
    BF_Block* left_child;
    BF_Block_Init(&left_child);
    CALL_BF(BF_GetBlock(fd, left_child_id, left_child));
    BPLUS_DATA_NODE* metadata_left_child = get_metadata_datanode(fd, left_child_id); //μεταδεδομενα του αριστερου παιδιου

    //αποθηκευση id block δεξιου παιδιου
    int right_child = metadata_left_child->next_block;

    //αν το μπλοκ ευρετηριου ειναι κενο, δηλαδη δημιουργηθηκε νεο μπλοκ ευρετηριου
    //μετα απο split ενος block δεδομενων
    if(metadata_index_node->num_keys == 0){
   
        memcpy(data + sizeof(BPLUS_INDEX_NODE) + sizeof(int), &key, sizeof(int)); //το κλειδι
        memcpy(data + sizeof(BPLUS_INDEX_NODE), &left_child_id, sizeof(int)); //αριστερα του κλειδιου
        memcpy(data + sizeof(BPLUS_INDEX_NODE) + 2 * sizeof(int), &right_child, sizeof(int)); //δεξια του κλειδιου
    }

    else{
        //αν το μπλοκ ευρετηριου δεν ειναι κενο πρεπει να βρουμε την θεση που θα μπει το νεο κλειδι
        int current_key;
        bool is_max = true;
        for(int i = 1; i <= 2 * metadata_index_node->num_keys + 1; i+=2){

            memcpy(&current_key, data + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int), sizeof(int));

            //αν το νεο κλειδι ειναι μικροτερο απο το κλειδι της θεσης i πρεπει να μπει πριν απο αυτο
            if(key < current_key){
                
                is_max = false;

                //μετακινηση των κλειδιων και των δεικτων κατα μια θεση δεξια
                void* tempDest = data + sizeof(BPLUS_INDEX_NODE) + (i + 2) * sizeof(int);
                void* tempSrc = data + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int);
                memmove(tempDest, tempSrc, (2 * metadata_index_node->num_keys + 1 - i) * sizeof(int));
                
                //εισαγωγη του νεου κλειδιου 
                memcpy(tempSrc, &key, sizeof(int));

                //εισαγωγη νεου δεικτη δεξια απο το κλειδι
                memcpy(tempSrc + sizeof(int), &left_child_id, sizeof(int));
            }
           
        }

        //αν ειναι το μεγιστο κλειδι, πρεπει να το προσθεσουμε στο τελος μαζι με εναν δεικτη στο δεξι παιδι
        if(is_max == true){
            memcpy(data + sizeof(BPLUS_INDEX_NODE) + (2 * metadata_index_node->num_keys + 1) * sizeof(int), &key, sizeof(int));
            memcpy(data + sizeof(BPLUS_INDEX_NODE) + (2 * metadata_index_node->num_keys + 2) * sizeof(int), &right_child, sizeof(int));
        }    
        
    }

    metadata_index_node->num_keys++; //αυξανουμε τον αριθμο των κλειδιων

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);  
    
    CALL_BF(BF_UnpinBlock(left_child));
    BF_Block_Destroy(&left_child);
   

}


void print_index_node(int fd, int id){
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, id, block));

    void* data = BF_Block_GetData(block);
    BPLUS_INDEX_NODE* metadata = get_metadata_indexnode(fd, id);

    printf("Block id: %d\n", metadata->block_id);
    printf("Number of keys: %d\n", metadata->num_keys);
    

    for(int i = 1; i < 2*metadata->num_keys + 1; i+=2){
        
        int key;
        int child_id;
        
        if(i == 1){
            memcpy(&child_id, data + sizeof(BPLUS_INDEX_NODE), sizeof(int));
            printf("%d | ", child_id);
        }
        
        memcpy(&key, data + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int), sizeof(int));
        memcpy(&child_id, data + sizeof(BPLUS_INDEX_NODE) + (i + 1) * sizeof(int), sizeof(int));
        printf("Key: %d | %d | ", key, child_id);
    }
    
    printf("\n\n");

    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
}
