#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include <bp_datanode.h>
#include <stdbool.h>

#define bplus_ERROR -1

#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      return bplus_ERROR;     \
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

    BPLUS_INFO bplus_info;
    bplus_info.file_desc = file_desc;
    bplus_info.root_block = 0;
    bplus_info.height = 0;
    bplus_info.record_size = sizeof(Record);
    bplus_info.key_size = sizeof(int);
    bplus_info.first_data_block = 1;
    bplus_info.is_open = true;
    bplus_info.max_records_per_block = (BF_BLOCK_SIZE - sizeof(BPLUS_DATA_NODE)) / sizeof(Record);
    bplus_info.num_of_blocks = 1;

    memcpy(data, &bplus_info, sizeof(BPLUS_INFO));


    BF_Block_SetDirty(block); //αφου τροποποιηθηκε το block, το κανουμε dirty
    CALL_BF(BF_UnpinBlock(block)) //δεν το χρειαζομαστε πλεον
    BF_Block_Destroy(&block); //καταστροφη του δεικτη

    BF_CloseFile(file_desc); //κλεισιμο του αρχειου

  return 0;
}


BPLUS_INFO* BP_OpenFile(char *fileName, int *file_desc){
  CALL_BF(BF_OpenFile(fileName, file_desc));

  BF_Block* block;
  BF_Block_Init(&block);

  //επιστρεφει το block με τα μεταδεδομενα
  CALL_BF(BF_GetBlock(*file_desc, 0, block));

  void* data = BF_Block_GetData(block);

  //αποθηκευση των μεταδεδομενων στη δομη bplus_info
  BPLUS_INFO* bplus_info = (BPLUS_INFO*)data;

  CALL_BF(BF_UnpinBlock(block)); //μονο unpin χωρις set_dirty αφου δεν αλλαξαμε κατι
  BF_Block_Destroy(&block);

  return bplus_info;
}

int BP_CloseFile(int file_desc,BPLUS_INFO* info){

  //τα block εχουν γινει ηδη unpinned στις υπολοιπες συναρτησεις
  //οποτε αρκει να κλεισουμε το αρχειο

  CALL_BF(BF_CloseFile(file_desc));
  
  return 0;
}

int BP_InsertEntry(int file_desc,BPLUS_INFO *bplus_info, Record record)
{ 
  return 0;
}

int BP_GetEntry(int file_desc,BPLUS_INFO *bplus_info, int value,Record** record)
{  
  *record=NULL;
  return 0;
}

