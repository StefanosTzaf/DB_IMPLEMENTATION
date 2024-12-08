#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include "bp_datanode.h"

// δημιουργει ενα νεο μπλοκ δεδομενων και προσθετει τα μεταδεδομενα του
//  επιστρεφει το id του νεου μπλοκ
int create_data_node(int file_desc, BPLUS_INFO* bplus_info){
    
    BF_Block* block;
    BF_Block_Init(&block);

    CALL_BF(BF_AllocateBlock(file_desc, block)); 

    void* data = BF_Block_GetData(block);

    BPLUS_DATA_NODE data_node; //δημιουργια μεταδεδομενων του μπλοκ δεδομενων
    data_node.num_records = 0; //αρχικα δεν εχει εγγραφες
   
    int count;
    BF_GetBlockCounter(file_desc, &count);
    data_node.block_id =  count - 1; //το id του block (-1 γιατι η αριθμηση ξεκιναει απο το 0)
   
    data_node.next_block = -1; //αρχικα δεν υπαρχει επομενο block
    data_node.parent_id = -1;
    data_node.is_data_node = 1; //ειναι block δεδομενων
    data_node.minKey = -1; //αρχικοποιηση με τιμη που δεν υπαρχει

    memcpy(data, &data_node, sizeof(BPLUS_DATA_NODE)); //αντιγραφη μεταδεδομενων στο block

    BF_Block_SetDirty(block); //αφου τροποποιηθηκε το block, το κανουμε dirty
    CALL_BF(BF_UnpinBlock(block)); //δεν το χρειαζομαστε αλλο
    BF_Block_Destroy(&block); //καταστρεφουμε τον δεικτη στο block

    BF_Block* block0;
    BF_Block_Init(&block0);
    CALL_BF(BF_GetBlock(file_desc, 0, block0));

    BF_Block_SetDirty(block0); 
    CALL_BF(BF_UnpinBlock(block0));
    BF_Block_Destroy(&block0);


    return data_node.block_id; 
}


// επιστρεφει τα μεταδεδομενα ενος μπλοκ δεδομενων
BPLUS_DATA_NODE* get_metadata_datanode(int file_desc, int block_id){
    
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(file_desc, block_id, block));

    void* data = BF_Block_GetData(block);

    //αντιγραφουμε τα μεταδεδομενα απο το block στην δομη data_node
    BPLUS_DATA_NODE* data_node = (BPLUS_DATA_NODE*)(data);

    

    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return data_node;
}




//προσθετει μια νεα εγγραφη στο μπλοκ δεδομενων το οποιο εχει χωρο
void insert_rec_in_datanode(int fd, int node, BPLUS_INFO* bplus_info, Record new_rec){
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, node, block));

    void* data = BF_Block_GetData(block);
    BPLUS_DATA_NODE* metadata = (BPLUS_DATA_NODE*)data; //μεταδεδομενα του block

    int key = new_rec.id;
 
    //αν δεν υπαρχιυν εγγραφες απλα προσθετουμε την εγγραφη μετα τα μεταδεδομενα
    if(metadata->num_records == 0){
        metadata->minKey = key;
        memcpy(data + sizeof(BPLUS_DATA_NODE), &new_rec, sizeof(Record));
    }

    //αν υπαρχουν εγγραφες τοποθετουμε την νεα εγγραφη ακριβως πριν την πρωτη εγγραφη που εχει μεγαλυτερο κλειδι
    else{
        
        int previousMinKey = metadata->minKey; //το προηγουμενο ελαχιστο κλειδι του block
        if(key < previousMinKey){
            metadata->minKey = key;
        }

        bool is_max = true; //αν το κλειδι της νεας εγγραφης ειναι το μεγαλυτερο

        for(int i = 0; i < metadata->num_records; i++){
        
            Record* rec = (Record*) (data + sizeof(BPLUS_DATA_NODE) + i * sizeof(Record));

            //η νεα εγγραφη πρεπει να μπει πριν απο αυτη
            if(rec->id > key){
                
                is_max = false; //βρηκαμε μεγαλυτερο κλειδι

                //πρεπει να μετακινησουμε ολες τις εγγραφες απο την ι-οστη, μια θεση δεξια ωστε να μπει η νεα στην καταλληλη θεση
                memmove(data + sizeof(BPLUS_DATA_NODE) + (i + 1) * sizeof(Record), data + sizeof(BPLUS_DATA_NODE) + i * sizeof(Record), 
                    (metadata->num_records - i) * sizeof(Record));
                
                //εισαγουμε την εγγραφη στην θεση που αδειασαμε
                memcpy(data + sizeof(BPLUS_DATA_NODE) + i * sizeof(Record), &new_rec, sizeof(Record));
                
                break;
            }   
        }
        
        //αν η νεα εγγραφη εχει το μεγαλυτερο κλειδι, πρεπει να εισαχθει στο τελος
        if(is_max == true){
                memcpy(data + sizeof(BPLUS_DATA_NODE) + metadata->num_records * sizeof(Record), &new_rec, sizeof(Record));
        }

    }

    metadata->num_records++; //αυξηση εγγραφων στο block

    BF_Block_SetDirty(block); 
    CALL_BF( BF_UnpinBlock(block));   

    BF_Block_Destroy(&block);
}




// Χωριζει ενα μπλοκ δεδομενων στα δυο, μεταφερει τις μισες εγγραφες στο νεο block και εισαγει τη νεα εγραφη στο καταλληλο block 
// Τελος, επιστρεφει το id του νεου block ου δημιουργηθηκε
int split_data_node(int fd, int id, BPLUS_INFO* bplus_info, Record new_rec){

    //######### Αρχικο block ##########//    
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, id, block));

    void* data = BF_Block_GetData(block); //δεδομενα του block
    BPLUS_DATA_NODE* metadata = (BPLUS_DATA_NODE*)data; //μεταδεδομενα του block
    
    //οι εγγραφες του μπλοκ μαζι με τη νεα πρεπει να ισομοιραστουν 
    int split_point = (metadata->num_records + 1) / 2; // +1 λογω της νεας εγγραφης


    //############# ορισμος νεου block ##########//
    BF_Block* new_block;
    BF_Block_Init(&new_block);

    int new_id = create_data_node(fd, bplus_info); //δημιουργια νεου block
    CALL_BF(BF_GetBlock(fd, new_id, new_block)); //αποθηκευση νεου block στον δεικτη new_block
    
    void* new_data = BF_Block_GetData(new_block); //δεδομενα νεου block
    BPLUS_DATA_NODE* new_metadata = get_metadata_datanode(fd, new_id); //μεταδεδομενα του νεου block

    new_metadata->next_block = metadata->next_block; //το επομενο block του νεου γινεται αυτο του παλιου
    metadata->next_block = new_id; //το επομενο block του παλιου γινεται το νεο


    //αναζητηση της θεσης που θα μπει η νεα εγγραφη
    int pos = 0; //position της νεας εγγραφης
    bool is_max = true; //αν το κλειδι της νεας εγγραφης ειναι το μεγαλυτερο
    for(int i = 0; i < metadata->num_records; i++){ 
        
        Record* rec = (Record*) (data + sizeof(BPLUS_DATA_NODE) + i * sizeof(Record)); //εξεταζουμε την εγγραφη i
        
        if(new_rec.id < rec->id){ //αν το κλειδι της νεας εγγραφης ειναι μικροτερο απο το κλειδι της εγγραφης i
            is_max = false;
            pos = i; //η θεση της νεας εγγραφης ειναι πριν την εγγραφη i, δηλαδη αν pos = 2 πρεπει να μπει η 0, η 1 μετα η νεα και μετα η 2
            break;
        }
    }

    //αν η νεα εγγραφη εχει το μεγιστο κλειδι, τοτε πρεπει να μπει στο τελος
    if(is_max == true){
        pos = metadata->num_records;
    }
    
    int old_num_records = metadata->num_records; //αριθμος εγγραφων του παλιου block αρχικα
     
     //ανανεωση αριθμου εγγραφων του παλιου block
    if((old_num_records + 1) % 2 == 0){ //αν οι παλιες εγγραφες + την νεα ισομοιραζονται ακριβως
        metadata->num_records = split_point;
        new_metadata->num_records = split_point;
    }
    else{
        metadata->num_records = old_num_records - split_point + 1;
        new_metadata->num_records = old_num_records - split_point ;//μια εγγραφη λιγοτερη
    }


    //αν η νεα εγγραφή πρεπει να μπει στο παλιο block
    if(pos < metadata->num_records){

        //μετακινηση των εγγραφων που ειναι από την μέση και μετα 
        void* tempDest = new_data + sizeof(BPLUS_DATA_NODE); // θα τα μετακινήσουμε στο δεξί μπλοκ
        
        // ποιες εγγραφές; αυτές που είναι από την μέση και μετά(δεν έχουμε προσθεσει ακόμα την νέα εγγραφη αρα -1)
        //το metadata->num_records περιεχει πλεον τις μισες εγγραφες, αρα το tempSrc δειχνει απο τη μεσαια εγγραφη κ μετα
        void* tempSrc = data + sizeof(BPLUS_DATA_NODE) + ( (metadata->num_records - 1) * sizeof(Record) ); //
        memmove(tempDest, tempSrc, new_metadata->num_records  * sizeof(Record));

        metadata->num_records--; //στην πραγματικοτητα δεν εχει εισαχθει ακομη η εγγραφη οποτε ενημερωνεται μεσα στη συναρτηση
        insert_rec_in_datanode(fd, id, bplus_info, new_rec); //εισαγωγη της νεας εγγραφης στο παλιο block
    
    }

    //αν η νεα εγγραφη πρεπει να μπει στο νεο block
    else{

        void* tempDest = new_data + sizeof(BPLUS_DATA_NODE); 
        void* tempSrc = data + sizeof(BPLUS_DATA_NODE) + metadata->num_records * sizeof(Record); //απο την μεση και μετα
        memmove(tempDest, tempSrc, (new_metadata->num_records - 1) * sizeof(Record));

        new_metadata->num_records--;
        insert_rec_in_datanode(fd, new_id, bplus_info, new_rec); //εισαγωγη της νεας εγγραφης στο νεο block
    }
    
    //Ενημερωση min κλειδιων των δυο block
    Record* firstRec1 = (Record*) (data + sizeof(BPLUS_DATA_NODE)); //πρωτη εγγραφη του παλιου block
    Record* firstRec2 = (Record*) (new_data + sizeof(BPLUS_DATA_NODE)); //πρωτη εγγραφη του νεου block
    
    metadata->minKey = firstRec1->id; //το minKey του παλιου block γινεται το κλειδι της πρωτης εγγραφης
    new_metadata->minKey = firstRec2->id; //το minKey του νεου block γινεται το κλειδι της πρωτης εγγραφης

    return new_id;
}


//εκτυπωνει τις εγγραφες ενος μπλοκ δεδομενων
void print_data_node(int fd, int id){
    // Άνοιγμα αρχείου σε λειτουργία εγγραφής
    FILE* file = fopen("re.txt", "a");
    if (file == NULL) {
        perror("Error opening file");
        return;
    }

    // Λήψη του block
    BF_Block* block;
    BF_Block_Init(&block);
    CALL_BF(BF_GetBlock(fd, id, block));

    // Λήψη δεδομένων
    void* data = BF_Block_GetData(block);
    BPLUS_DATA_NODE* metadata = (BPLUS_DATA_NODE*)data;

    // Εκτύπωση μεταδεδομένων
    fprintf(file, "Block id: %d\n", metadata->block_id);
    fprintf(file, "Number of records: %d\n", metadata->num_records);
    fprintf(file, "Next block: %d\n", metadata->next_block);

    // Εκτύπωση εγγραφών
    for (int i = 0; i < metadata->num_records; i++) {
        Record* rec = (Record*)(data + sizeof(BPLUS_DATA_NODE) + i * sizeof(Record));
        fprintf(file, "(%d, %s, %s, %s)\n", rec->id, rec->name, rec->surname, rec->city);
    }
    fprintf(file, "\n");

    // Αποδέσμευση πόρων
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    // Κλείσιμο αρχείου
    fclose(file);
}
