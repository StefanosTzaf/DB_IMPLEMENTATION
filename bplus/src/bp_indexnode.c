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

int split_index_node(int fd, BPLUS_INFO bplus_info, int index_node_id, int key_to_insert, int child_id){
    
    //######## αρχικο block ευρετηριου ##########//
    BF_Block* block1;
    BF_Block_Init(&block1);
    CALL_BF(BF_GetBlock(fd, index_node_id, block1));

    //δεδομενα του block ευρετηριου το οποιο θελουμε να γινει split
    void* data1 = BF_Block_GetData(block1);
    BPLUS_INDEX_NODE* metadata1 = get_metadata_indexnode(fd, index_node_id);

    //####### νεο block ευρετηριου ##########//
    int new_index_node_id = create_index_node(fd, &bplus_info);
    BF_Block* block2;
    BF_Block_Init(&block2);
    CALL_BF(BF_GetBlock(fd, new_index_node_id, block2));

    void* data2 = BF_Block_GetData(block2);
    BPLUS_INDEX_NODE* metadata2 = get_metadata_indexnode(fd, new_index_node_id);


    //######## ισομοιρασμος δεικτων κ κλειδιων ########//
    int split_point = (metadata1->num_keys + 1) / 2; // +1 λογω του νεου κλειδιου, ωστε να υπολογιστει σωστα το μεσαιο κλειδι που πρεπει να ανεβει πανω

    int pos = 0;
    bool is_max = true;
    int current_key;


    //πόσα στοιχεία έχει το block πριν σπάσει
    int count_of_elements = 2 * metadata1->num_keys + 1;
    for(int i = 1; i <= count_of_elements; i+=2){
        
        //δεν κοιταμε τον 1ο δεικτη
        memcpy(&current_key, data1 + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int), sizeof(int));

        if(key_to_insert < current_key){
            is_max = false;

            pos = i;
            break;
        }
    }

     if(is_max == true){
        pos = metadata1->num_keys * 2 + 1; //πρεπει να μπει στο τελος
    }

    //νεο μπλοκ
    void* dest = data2 + sizeof(BPLUS_INDEX_NODE);

    if(pos < 2*split_point){
        //το 2*split_point δειχνει στο τελος του κλειδιου αρα πρεπει να βαλουμε + 1 για να κρατησουμε κ τον δεικτη
        void* source = data1 + sizeof(BPLUS_INDEX_NODE) + (2*split_point + 1 ) * sizeof(int);
        
        //κραταμε το πρωτο κλειδι απο το δευτερο μισο του block, δηλ αυτο που πρεπει να ανεβει πανω
        int key_to_move_up = (int)(data1 + sizeof(BPLUS_INDEX_NODE) + (2*split_point + 1)*sizeof(int));
        
        memmove(dest, source, (2*split_point + 2) * sizeof(int));
        insert_key_indexnode(fd, index_node_id, &bplus_info, key_to_insert, child_id);

    }
    //αν ειναι το μεσαιο κλειδι
    else if(pos == 2*split_point + 1){
        void* source = data1 + sizeof(BPLUS_INDEX_NODE) + (2*split_point) * sizeof(int);
        memmove(dest, source, (2*split_point) * sizeof(int));
        insert_key_indexnode(fd, new_index_node_id, &bplus_info, key_to_insert, child_id);

        int key_to_move_up = (int)(data2 + sizeof(BPLUS_INDEX_NODE) + sizeof(int));
    }
    else{
        void* source = data1 + sizeof(BPLUS_INDEX_NODE) + (2*split_point + 1) * sizeof(int);
        memmove(dest, source, (2*split_point) * sizeof(int));
        insert_key_indexnode(fd, new_index_node_id, &bplus_info, key_to_insert, child_id);

        int key_to_move_up = (int)(data2 + sizeof(BPLUS_INDEX_NODE) + sizeof(int));
    }

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
