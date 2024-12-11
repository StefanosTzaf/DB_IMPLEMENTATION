#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "bp_file.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"

//#############################################################//
// #####  MAIN.C  //
// ########################################################### //

#define RECORDS_NUM 2324 // you can change it if you want
#define FILE_NAME_3 "data3.db"


#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
}

void insertEntries();



int main(){
 
  insertEntries();
}

void insertEntries(){


  FILE* file = fopen("output.txt", "w");
  fclose(file);

  BF_Init(LRU);
  BP_CreateFile(FILE_NAME_3);
  int file_desc;
  BPLUS_INFO* info = BP_OpenFile(FILE_NAME_3, &file_desc);
  Record record;
  
  for (int i = 0; i < RECORDS_NUM; i++){
    record = randomRecord();
    BP_InsertEntry(file_desc, info, record);
    // printf("inserting reec %d\n", record.id);

    Record tmpRec;  //Αντί για malloc
    Record* result=&tmpRec;
    int x = BP_GetEntry(file_desc, info, record.id, &result);
    if(x == -1){
      printf("Record with id %d not found\n", record.id);
    }    
  }
   BP_PrintTree(file_desc, info);

  BP_CloseFile(file_desc,info);
  BF_Close();
}



