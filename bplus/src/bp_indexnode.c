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
    index_node.parent_id = -1;
    index_node.is_data_node = 0;

    //αντιγραφη των μεταδεδομενων στο block
    memcpy(data, &index_node, sizeof(BPLUS_INDEX_NODE));

    bplus_info->num_of_blocks++; //αυξανουμε τον αριθμο των μπλοκ  
    
    BF_Block_SetDirty(block); //τροποποιησαμε το block αρα το κανουμε dirty
    CALL_BF(BF_UnpinBlock(block)); //δεν το χρειαζομαστε πια
    BF_Block_Destroy(&block); //καταστροφη του δεικτη στο block

    //ενημερωση block 0 με τα μεταδεδομενα
    BF_Block* block0;
    BF_Block_Init(&block0);
    CALL_BF(BF_GetBlock(file_desc, 0, block0));

    BF_Block_SetDirty(block0); 
    CALL_BF(BF_UnpinBlock(block0));
    BF_Block_Destroy(&block0);


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


// Προσθηκη ζευγους κλειδι-δεικτης σε ενα μπλοκ ευρετηριου
// Η συναρτηση δεχεται το id του block που σπαει(left_child_id), 
// καθως και το id του block που δημιουργειται κατα το split(right_child_id)
// Το κλειδι key ειναι το μικροτερο του νεου block(right_child_id) και ειναι αυτο που πρεπει να εισαχθει ως δεικτης.
void insert_key_indexnode(int fd, int id_index_node, BPLUS_INFO* bplus_info, int key, int left_child_id, int right_child_id){

    BF_Block* block;
    BF_Block_Init(&block);
    
    //αποθηκευση του block ευρετηριου στον δεικτη block
    CALL_BF(BF_GetBlock(fd, id_index_node, block));
    void* data = BF_Block_GetData(block);
    BPLUS_INDEX_NODE* metadata_index_node = (BPLUS_INDEX_NODE*)data; //μεταδεδομενα του block


    //αν το μπλοκ ευρετηριου ειναι κενο, δηλαδη δημιουργηθηκε νεο μπλοκ ευρετηριου
    //μετα απο split ενος block δεδομενων
    if(metadata_index_node->num_keys == 0){
        memcpy(data + sizeof(BPLUS_INDEX_NODE) + sizeof(int), &key, sizeof(int)); //το κλειδι
        memcpy(data + sizeof(BPLUS_INDEX_NODE), &left_child_id, sizeof(int)); //αριστερα του κλειδιου
        memcpy(data + sizeof(BPLUS_INDEX_NODE) + 2 * sizeof(int), &right_child_id, sizeof(int)); //δεξια του κλειδιου
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
                memcpy(tempSrc + sizeof(int), &right_child_id, sizeof(int));

                break;
            }
           
        }

        //αν ειναι το μεγιστο κλειδι, πρεπει να το προσθεσουμε στο τελος μαζι με εναν δεικτη στο δεξι παιδι
        if(is_max == true){

            memcpy(data + sizeof(BPLUS_INDEX_NODE) + (2 * metadata_index_node->num_keys + 1) * sizeof(int), &key, sizeof(int));
            memcpy(data + sizeof(BPLUS_INDEX_NODE) + (2 * metadata_index_node->num_keys + 2) * sizeof(int), &right_child_id, sizeof(int));
        }    
        
    }

    metadata_index_node->num_keys++; //αυξανουμε τον αριθμο των κλειδιων

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);  
    

}

// σπαει ενα block ευρετηριου σε δυο και εισαγει το μεσαιο κλειδι στον γονεα.
// Δεχεται ως παραμετρους το αρχειο, τα μεταδεδομενα του bplus, το id του block που θα σπασει, το κλειδι που θα εισαχθει 
// και το id του block που πρεπει να εισαχθει ως δεικτης
int split_index_node(int fd, BPLUS_INFO* bplus_info, int index_node_id, int key_to_insert, int child_id){
    
    // data block στο οποιο θα δειχνει ο νεος δεικτης που θα εισαχθει 
    BF_Block* child;
    BF_Block_Init(&child);
    CALL_BF(BF_GetBlock(fd, child_id, child));
    BPLUS_DATA_NODE* metadata_child = get_metadata_datanode(fd, child_id);

    //######## αρχικο block ευρετηριου ##########//
    BF_Block* block1;
    BF_Block_Init(&block1);
    CALL_BF(BF_GetBlock(fd, index_node_id, block1));
    
    void* data1 = BF_Block_GetData(block1);
    BPLUS_INDEX_NODE* metadata1 = get_metadata_indexnode(fd, index_node_id);

    //####### νεο block ευρετηριου ##########//
    int new_index_node_id = create_index_node(fd, bplus_info);
    BF_Block* block2;
    BF_Block_Init(&block2);
    CALL_BF(BF_GetBlock(fd, new_index_node_id, block2));

    void* data2 = BF_Block_GetData(block2);
    BPLUS_INDEX_NODE* metadata2 = get_metadata_indexnode(fd, new_index_node_id);


    //######## ισομοιρασμος δεικτων κ κλειδιων ########//
    int split_point = (metadata1->num_keys) / 2; 

    int pos = 0;
    bool is_max = true;
    int current_key;
    int keys[metadata1->num_keys + 1]; //πινακας με τα κλειδια
    for(int i = 0; i < metadata1->num_keys + 1; i++){
        keys[i] = 0;
    }

    int current_p;
    int pointers[metadata1->num_keys + 2]; //πινακας με τους δεικτες

    int j = 0;

    //πόσα στοιχεία έχει το block πριν σπάσει
    int count_of_elements = 2 * metadata1->num_keys + 1;
    for(int i = 1; i <= count_of_elements; i+=2){
        
        //αποθηκευση κλειδιου
        memcpy(&current_key, data1 + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int), sizeof(int));
        keys[j] = current_key;

        //επειδη προσπερναμε τον πιο αριστερο δεικτη, πρεπει να τον αποθηκευσουμε ξεχωριστα
        if(i == 1){ //εαν εξεταζουμε το πρωτο κλειδι
            memcpy(&current_p, data1 + sizeof(BPLUS_INDEX_NODE), sizeof(int));
            pointers[j] = current_p; 
        }

        //αποθηκευση δεικτη δεξια απο το κλειδι
        memcpy(&current_p, data1 + sizeof(BPLUS_INDEX_NODE) + (i + 1) * sizeof(int), sizeof(int));
        pointers[j + 1] = current_p;
        
        if(key_to_insert < current_key){
            is_max = false;
            pos = j; //αποθηκευση της θεσης που θα μπει το νεο κλειδι ως προς το indexing του πινακα    
        }
        j++;
    }

    //αν το κλειδι ειναι το μεγιστο πρεπει να μπει στη τελευταια θεση του πινακα
     if(is_max == true){
        pos = metadata1->num_keys; 
    }

    //ενημερωνουμε το parent id του block που εισαγεται ως δεικτης
    //αν ανηκει στο πρωτο μισο του block, ο γονεας παραμενει ο αρχικος index node
    if(pos < split_point){
        metadata_child->parent_id = index_node_id;
    }
    //αν ανηκει στο δευτερο μισο του block, ο γονεας γινεται το νεο index node
    else{
        metadata_child->parent_id = new_index_node_id;
    }



    //μετακινηση κλειδιων απο τη θεση pos και μετα κατα μια θεση δεξια
    for(int i = metadata1->num_keys; i > pos; i--){
        keys[i] = keys[i - 1];
    }

    //εισαγωγη του νεου κλειδιου στην θεση pos
    keys[pos] = key_to_insert;

    //μετακινηση δεικτων απο τη θεση pos + 1 και μετα κατα μια θεση δεξια
    for(int i = metadata1->num_keys + 1; i > pos + 1; i--){
        pointers[i] = pointers[i - 1];
    }

    //εισαγωγη του νεου δεικτη στην θεση pos + 1, δηλαδη δεξια του νεου κλειδιου
    pointers[pos + 1] = child_id;


    //ευρεση μεσαιου κλειδιου, που πρεπει να ανεβει πανω
    //αν ο αριθμος κλειδιων ειναι περιττος, τοτε θα παρει ακριβως το μεσαιο κλειδι: 5, 10, 15 με split point = 1 και keys[1] = 10
    //αλλιως: 5, 10, 15, 20 με split point = 2 και keys[2] = 15
    int key_to_move_up = keys[split_point];

    //μετακινηση των κλειδιων μεχρι και το split point - 1 στο παλιο block
    for(int i = 0; i < split_point; i++){
        
        memcpy(data1 + sizeof(BPLUS_INDEX_NODE) + (2 * i + 1) * sizeof(int), &keys[i], sizeof(int));
        
        //τον πρωτο δεικτη(ο αριστερος) πρεπει να μπει πριν το πρωτο κλειδι κ αμεσως μετα τα μεταδεδομενα
        if (i == 0){
            memcpy(data1 + sizeof(BPLUS_INDEX_NODE), &pointers[i], sizeof(int));
        }

        memcpy(data1 + sizeof(BPLUS_INDEX_NODE) + (2 * i + 2) * sizeof(int), &pointers[i + 1], sizeof(int));
                
    }

    for(int i = split_point + 1; i <= metadata1->num_keys; i++){

        if(i == split_point + 1){
            // θελουμε να παρουμε τον δεικτη που αντιστοιχει στο κλειδι που μετακινειται πανω
            // ωστε να τοποθετηθει πρωτος στο νεο block
            memcpy(data2 + sizeof(BPLUS_INDEX_NODE), &pointers[split_point + 1], sizeof(int));
        }
        
        memcpy(data2 + sizeof(BPLUS_INDEX_NODE) + (2 * (i - split_point - 1) + 1) * sizeof(int), &keys[i], sizeof(int));

        memcpy(data2 + sizeof(BPLUS_INDEX_NODE) + (2 * (i - split_point - 1) + 2) * sizeof(int), &pointers[i + 1], sizeof(int));
    }
          

    //ενημερωση μεταδεδομενων των δυο index nodes
    metadata1->num_keys = split_point;
    
    //αν το split point ειναι περιττο, γιατι το αρχικο μεγεθος των κλειδιων ειναι περιττο(πχ για 7 κλειδια εχουμε split point = 3)
    //τοτε καθε block παιρνει τον ιδιο αριθμο κλειδιων, αφου το μεσαιο θα αφαιρεθει
    if(split_point % 2 != 0){
        metadata2->num_keys = split_point;
    }

    //αλλιως το δεξι block παιρνει ενα κλειδι λιγοτερο απο το αριστερο 
    else{ 
        metadata2->num_keys = split_point - 1;
    }



    BF_Block_SetDirty(block1);
    CALL_BF(BF_UnpinBlock(block1));
    BF_Block_Destroy(&block1);
    
    BF_Block_SetDirty(block2);
    CALL_BF(BF_UnpinBlock(block2));
    BF_Block_Destroy(&block2);
    
    BF_Block_SetDirty(child);
    CALL_BF(BF_UnpinBlock(child));
    BF_Block_Destroy(&child);


    //############ Διαχειριση του γονεα #############//
    if(metadata1->parent_id == -1){
        int new_root_id = create_index_node(fd, bplus_info);
        
        BF_Block* new_root;
        BF_Block_Init(&new_root);
        CALL_BF(BF_GetBlock(fd, new_root_id, new_root));

        BPLUS_INDEX_NODE* new_root_data = get_metadata_indexnode(fd, new_root_id);
        new_root_data->is_root = 1;

        insert_key_indexnode(fd, new_root_id, bplus_info, key_to_move_up, index_node_id, new_index_node_id);

        bplus_info->root_block = new_root_id;
        bplus_info->height++;

        metadata1->parent_id = new_root_id;
        metadata2->parent_id = new_root_id;

        BF_Block_SetDirty(new_root);
        CALL_BF(BF_UnpinBlock(new_root));
        BF_Block_Destroy(&new_root);

    }

    else{
        //αν ο γονεας εχει χωρο, εισαγωγη εκει
        if(is_full_indexnode(fd, metadata1->parent_id) == false){
            insert_key_indexnode(fd, metadata1->parent_id, bplus_info, key_to_move_up, index_node_id, new_index_node_id);
        }
        //αλλιως αναδρομικη κληση της split_index_node
        else{
            return split_index_node(fd, bplus_info, metadata1->parent_id, key_to_move_up, new_index_node_id);
        }
    }
 
    return new_index_node_id;

}


// εκτυπωνει τα μεταδεδομενα ενος index node καθως και τους δεικτες με τα κλειδια του
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

// επιστρεφει true αν το index node με id ειναι γεματο και false αν δεν ειναι
bool is_full_indexnode(int fd, int id){
    
    bool is_full = false;

    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, id, block));

    void* data = BF_Block_GetData(block);
    BPLUS_INDEX_NODE* metadata = get_metadata_indexnode(fd, id);

    int total_elements = (BF_BLOCK_SIZE - sizeof(BPLUS_INDEX_NODE)) / sizeof(int);
    int max_keys = (total_elements - 1) / 2;

    if(metadata->num_keys == max_keys){
        is_full = true;
    }
    

    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    
    return is_full;
}