#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include <bp_datanode.h>
#include <stdbool.h>

#define MAX_OPEN_FILES 20

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }
int openFiles[MAX_OPEN_FILES];


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
    bplus_info.max_records_per_block = (BF_BLOCK_SIZE - sizeof(BPLUS_DATA_NODE)) / sizeof(Record);

    int total_elements = (BF_BLOCK_SIZE - sizeof(BPLUS_INDEX_NODE)) / sizeof(int);
    int max_keys = (total_elements - 1) / 2;
    bplus_info.max_keys_per_index = max_keys;

    memcpy(data, &bplus_info, sizeof(BPLUS_INFO));


    BF_Block_SetDirty(block); //αφου τροποποιηθηκε το block, το κανουμε dirty
    CALL_BF(BF_UnpinBlock(block)); //unpin του block, αφου δεν το χρειαζομαστε πια
    BF_Block_Destroy(&block); //καταστροφη του δεικτη

    CALL_BF(BF_CloseFile(file_desc)); //κλεισιμο του αρχειου

  return 0;
}


BPLUS_INFO* BP_OpenFile(char *fileName, int *file_desc){
  CALL_BF(BF_OpenFile(fileName, file_desc));

  //αποθηκευση του ανοιχτου αρχειου στον πινακα
  openFiles[*file_desc] = 1;

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

  //το αρχειο πλεον ειναι κλειστο
  openFiles[file_desc] = 0;
  CALL_BF(BF_CloseFile(file_desc));
  
  return 0;
}


int BP_InsertEntry(int fd,BPLUS_INFO *bplus_info, Record record){

  //επιστροφη του block id που εγινε η εισαγωγη
  int return_value;

  //Αν το B+ δέντρο είναι κενό, δημιουργούμε ένα νέο block δεδομένων μονο
  //το οποιο αποτελει και τη ριζα του δεντρου
  if(bplus_info->height == 0){
    int id_datanode = create_data_node(fd, bplus_info);
    insert_rec_in_datanode(fd, id_datanode, bplus_info, record);

    bplus_info->root_block = id_datanode; //η ριζα προς το παρον ειναι block δεδομενων
    bplus_info->height++;

    return id_datanode; //επιστροφη του block id που εγινε η εισαγωγη 
  }

  //αν υπαρχει ηδη εγγραφη με το ιδιο id
  Record tmpRec;  //Αντί για malloc
  Record* result = &tmpRec;
  if(BP_GetEntry(fd, bplus_info, record.id, &result) == 0){
    return -1;
  }

  
  //Αν το B+ δέντρο δεν είναι κενό
  int root = bplus_info->root_block;
  int height_of_current_root = bplus_info->height;

  //Εύρεση του block δεδομένων που πρεπει να γίνει η εισαγωγή
  int data_block_to_insert = BP_FindDataBlockToInsert(fd, record.id, root, height_of_current_root);

  BF_Block* block;
  BF_Block_Init(&block);
  //αποθηκευση του block στο οποιο πρεπει να γινει εισαγωγη, στον δεικτη block
  CALL_BF(BF_GetBlock(fd, data_block_to_insert, block));

  BPLUS_DATA_NODE* metadata_datanode = get_metadata_datanode(fd, data_block_to_insert);

  //αν υπαρχει χωρος στο block δεδομενων, εισαγουμε την εγγραφη σε αυτο
  if(metadata_datanode->num_records < bplus_info->max_records_per_block){
    
    insert_rec_in_datanode(fd, data_block_to_insert, bplus_info, record);
    
    return_value = data_block_to_insert;

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return return_value;
  }

  //αν δεν υπαρχει χωρος πρεπει να το σπασουμε
  else{  

    int parent_id;
    //αν δεν υπαρχει γονεας index node πρεπει να δημιουργηθει
    //αυτο θα ισχυει μονο στην περιπτωση που εχουμε ενα μοναδικο block δεδομενων
    //το οποιο ειναι και η ριζα του δεντρου
    if(metadata_datanode->parent_id == -1){

      //δημιουργια πρωτου index node, δηλαδη της νεας ριζας
      parent_id = create_index_node(fd, bplus_info);
      bplus_info->height++;
      bplus_info->root_block = parent_id;

      metadata_datanode->parent_id = parent_id;
    }

    else{
      parent_id = metadata_datanode->parent_id;

    }

    //επιστροφη νεου block δεδομενων που δημιουργηθηκε μετα το split
    int new_data_node_id = split_data_node(fd, data_block_to_insert, bplus_info, record);

    //αποθηκευση του μικροτερου κλειδιου του νεου block δεδομενων, που πρεπει να εισαχθει σε index node
    int key_to_move_up = get_metadata_datanode(fd, new_data_node_id)->minKey;


    // Ορισμος του νεου block δεδομενων που δημιουργηθηκε απο το split
    BF_Block* new_data_node;
    BF_Block_Init(&new_data_node);
    CALL_BF(BF_GetBlock(fd, new_data_node_id, new_data_node));
    BPLUS_DATA_NODE* new_metadata_datanode = get_metadata_datanode(fd, new_data_node_id);

    //αν ο γονεας index node εχει χωρο, προσθηκη κλειδιου σε αυτο
    if(is_full_indexnode(fd, parent_id, bplus_info) == false){

      new_metadata_datanode->parent_id = parent_id;
      insert_key_indexnode(fd, parent_id, bplus_info, key_to_move_up, data_block_to_insert, new_data_node_id);
    }

    //αν δεν εχει χωρο σπαμε το index node
    else{
      int new_index_node = split_index_node(fd, bplus_info, parent_id, key_to_move_up, new_data_node_id);
      
    }


    BF_Block_SetDirty(new_data_node);
    CALL_BF(BF_UnpinBlock(new_data_node));
    BF_Block_Destroy(&new_data_node);

    BF_Block_SetDirty(block);
    CALL_BF(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return_value = BP_FindDataBlockToInsert(fd, record.id, bplus_info->root_block, bplus_info->height);

    return return_value;
  }
  

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

  return -1;
}



int BP_GetEntry(int file_desc, BPLUS_INFO *bplus_info, int value, Record** record){
  //βρισκουμε σε ποιο block δεδομενων θα επρεπε να βρισκεται το κλειδι
  int height = bplus_info->height;
  int root = bplus_info->root_block;
  int block_with_rec = BP_FindDataBlockToInsert(file_desc, value, root, height);

  BF_Block* block;
  BF_Block_Init(&block);

  // printf("HEIGHT before %d\n", bplus_info->height);

  CALL_BF(BF_GetBlock(file_desc, block_with_rec, block));
  
 printf("HEIGHT after %d\n", bplus_info->height);



  BPLUS_DATA_NODE* metadata_datanode = get_metadata_datanode(file_desc, block_with_rec);
  char* data = BF_Block_GetData(block);

  int num_of_recs = metadata_datanode->num_records;

  for(int i = 0; i < num_of_recs; i++){
    
    Record* rec = (Record*)(data + sizeof(BPLUS_DATA_NODE) + i * sizeof(Record));
    //αν βρεθηκε η εγγραφη, επιστρεφουμε 0 και το record δειχνει στην εγγραφη αυτη
    if(rec->id == value){
      memcpy(*record, rec, sizeof(Record));

      BF_UnpinBlock(block);
      BF_Block_Destroy(&block);
      return 0;
    }
  }

  // //Αν δεν βρεθηκε τετοια εγγραφη
  *record = NULL;

  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);

  return -1;
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
  
  bool is_max = true;

  //θελουμε να πηγαινει απο κλειδι σε κλειδι
  for(int i = 1; i < (2 * num_of_keys + 1) ; i+= 2){
 
    memcpy(&current_key, data + sizeof(BPLUS_INDEX_NODE) + i * sizeof(int), sizeof(int));
    
    if(key < current_key){
  
      is_max = false;

      memcpy(&child_id, data + sizeof(BPLUS_INDEX_NODE) + (i - 1) * sizeof(int), sizeof(int)); 
      //το block που πρεπει να ακολουθησουμε στην πορεια      
  
      break;
    }

  }

    //αν φτασαμε στο τελευταιο κλειδι και δεν εχουμε βρει καποιο μικροτερο
  if(is_max == true){

    int total_elements = 2 * num_of_keys + 1;

    memcpy(&child_id, data + sizeof(BPLUS_INDEX_NODE) + (total_elements - 1) * sizeof(int), sizeof(int)); 

  } 



  //αναδρομική κλήση
  height_of_current_root--;
  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);

  return BP_FindDataBlockToInsert(fd, key,  child_id, height_of_current_root);
    
}


void BP_PrintBlock(int fd, int block_id, BPLUS_INFO* bplus_info){


  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd, block_id, block));

  char* data = BF_Block_GetData(block);

  int* is_datanode;
  is_datanode = (int*)(data);

  if(*is_datanode == 1){
    print_data_node(fd, block_id);
  }

  else{
    print_index_node(fd, block_id);
  }

  BF_UnpinBlock(block);
  BF_Block_Destroy(&block);
}


void BP_PrintTree(int fd, BPLUS_INFO* bplus_info){

  FILE* file = fopen("output.txt", "a");

  fprintf(file, "\nPrinting B+ Tree\n\n");
  fprintf(file, "METADATA\n");

  fprintf(file, "---Root block: %d\n", bplus_info->root_block); 
  fprintf(file, "---Height: %d\n", bplus_info->height);
  fprintf(file, "---parent of root: %d\n", get_metadata_indexnode(fd, bplus_info->root_block)->parent_id);
  int count;
  BF_GetBlockCounter(fd, &count);
  fprintf(file, "---Number of blocks: %d\n\n-------------------\n", count);
  fflush(file);
  
  for(int i = 1; i < count; i++){
    BP_PrintBlock(fd, i, bplus_info);
  }

  fclose(file);
}


void BP_SetInfo(int fd, BPLUS_INFO* bplus_info, int max_records_per_data, int max_keys_per_index){
  
  BF_Block* block;
  BF_Block_Init(&block);
  CALL_BF(BF_GetBlock(fd, 0, block));

  void* data = BF_Block_GetData(block);

  bplus_info = (BPLUS_INFO*)data;
  bplus_info->max_records_per_block = max_records_per_data;
  bplus_info->max_keys_per_index = max_keys_per_index;

  BF_Block_SetDirty(block);
  CALL_BF(BF_UnpinBlock(block));
  BF_Block_Destroy(&block);

}