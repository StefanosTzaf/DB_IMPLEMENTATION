#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include <bp_datanode.h>
#include <stdbool.h>

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }




int BP_CreateFile(char *fileName){
  
  CALL_BF(BF_CreateFile(fileName));

  int file_desc;
  CALL_BF(BF_OpenFile(fileName, &file_desc));

    //Δημιουργία δεικτη σε block
    //το block δεν ειναι τιποτα αλλο παρα ενας δεικτης στα δεδομενα ενος μπλοκ
    //που τον χρησιμοποιουμε για να εχουμε προσβαση σε αυτα 
    //οταν δεν τον χρειαζομαστε αλλο πρεπει να τον καταστρεψουμε
    BF_Block* block;

    BF_Block_Init(&block);

    //Δέσμευση block στο αρχείο
    BF_AllocateBlock(file_desc, block);

    //δεδομενα του block
    void* data = BF_Block_GetData(block);

    //αρχικοποιηση των μεταδεδομενων του Β+ δεντρου
    BPLUS_INFO bplus_info;
    bplus_info.file_desc = file_desc;
    bplus_info.root_block = 0;
    bplus_info.height = 0;
    bplus_info.record_size = sizeof(Record);
    bplus_info.key_size = sizeof(int);
    bplus_info.first_data_block = 1;
    bplus_info.is_open = true;
    bplus_info.max_records_per_block = (BF_BLOCK_SIZE - sizeof(BPLUS_DATA_NODE)) / sizeof(Record);
    bplus_info.num_of_blocks = 1; //μονο αυτο με τα μεταδεδομενα του αρχειου

    memcpy(data, &bplus_info, sizeof(BPLUS_INFO));


    BF_Block_SetDirty(block); //αφου τροποποιηθηκε το block, το κανουμε dirty
    
    CALL_BF(BF_UnpinBlock(block)); //unpin του block, αφου δεν το χρειαζομαστε πια

    BF_Block_Destroy(&block); //καταστροφη του δεικτη

    CALL_BF(BF_CloseFile(file_desc)); //κλεισιμο του αρχειου

  return 0;
}


BPLUS_INFO* BP_OpenFile(char *fileName, int *file_desc){
  CALL_BF(BF_OpenFile(fileName, file_desc));

  BF_Block* block;
  BF_Block_Init(&block);

  //επιστρεφει το block 0 με τα μεταδεδομενα στη μεταβλητη block
  CALL_BF(BF_GetBlock(*file_desc, 0, block));

  void* data = BF_Block_GetData(block);

  //αποθηκευση των μεταδεδομενων στη δομη bplus_info
  BPLUS_INFO* bplus_info = (BPLUS_INFO*)data;

  //μονο unpin χωρις set_dirty αφου δεν αλλαξαμε κατι
  CALL_BF(BF_UnpinBlock(block));
  
  BF_Block_Destroy(&block);

  return bplus_info;
}

int BP_CloseFile(int file_desc, BPLUS_INFO* info){

  //τα block εχουν γινει ηδη unpinned στις υπολοιπες συναρτησεις
  //οποτε αρκει να κλεισουμε το αρχειο

  CALL_BF(BF_CloseFile(file_desc));
  
  return 0;
}

int BP_InsertEntry(int fd,BPLUS_INFO *bplus_info, Record record){

  //Αν το B+ δέντρο είναι κενό, δημιουργούμε ένα νέο block δεδομένων μονο
  //το οποιο αποτελει και τη ριζα του δεντρου
  if(bplus_info->height == 0){
    
    int id_datanode = create_data_node(fd, bplus_info);
    insert_rec_in_datanode(fd, id_datanode, bplus_info, record);

    bplus_info->root_block = id_datanode; //η ριζα προς το παρον ειναι block δεδομενων
    bplus_info->height++;

    return id_datanode; //επιστροφη του block id που εγινε η εισαγωγη 
  }


  //Αν το B+ δέντρο δεν είναι κενό
  int root = bplus_info->root_block;
  int height_of_current_root = bplus_info->height;

  //Εύρεση του block δεδομένων που πρεπει να γίνει η εισαγωγή
  int data_block_id = BP_FindDataBlockToInsert(fd, record.id, root, height_of_current_root);

  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd, data_block_id, block));

  BPLUS_DATA_NODE* data_node = get_metadata_datanode(fd, data_block_id);

  //αν υπαρχει χωρος στο block δεδομενων, εισαγουμε την εγγραφη σε αυτο
  if(data_node->num_records < bplus_info->max_records_per_block){
    insert_rec_in_datanode(fd, data_block_id, bplus_info, record);
  }

  //αν δεν υπαρχει χωρος πρεπει να το σπασουμε
  else{  
    
    split_data_node(fd, data_block_id, bplus_info, record);
  }
  

  return 0;
}

int BP_GetEntry(int file_desc,BPLUS_INFO *bplus_info, int value,Record** record){
  
  
  return 0;
}


int BP_FindDataBlockToInsert(int fd, int key, int root, int height_of_current_root){

  //είμαστε σε block δεδομένων (end case της αναδρομής)
  if(height_of_current_root == 1){
    return root;
  }


  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd, root, block));

  void* data = BF_Block_GetData(block);
  int num_of_keys = get_metadata_indexnode(fd, root)->num_keys;

  int child_id = 0;
  int current_key;

  BF_Block* child;
  BF_Block_Init(&child);
  

  //θελουμε να πηγαινει απο κλειδι σε κλειδι
  for(int i = 1; i <= (2 * num_of_keys + 1) ; i+= 2){
    
    memcpy(&current_key, data + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int), sizeof(int));

    if(key < current_key){
  
      memcpy(&child_id, data + sizeof(BPLUS_INDEX_NODE) + (i - 1) * sizeof(int), sizeof(int)); 

      //το block που πρεπει να ακολουθησουμε στην πορεια      
      CALL_BF(BF_GetBlock(fd, child_id, child));
  
      break;
    }

    //αν φτασαμε στο τελευταιο κλειδι και δεν εχουμε βρει καποιο μικροτερο
    else if(i == 2 * num_of_keys - 1){

      memcpy(&child_id, data + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int), sizeof(int)); 

      //αποθηκευση του δεξιου block με τιμες κλειδιων >= του τελευταιου κλειδιου που εξετασαμε
      CALL_BF(BF_GetBlock(fd, child_id, child));
      break;
    } 

  }


  //αναδρομική κλήση
  --height_of_current_root;
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);

  return BP_FindDataBlockToInsert(fd, key,  child_id, height_of_current_root);
    
}

