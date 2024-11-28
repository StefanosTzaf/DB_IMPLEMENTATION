#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "bp_file.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"

#define RECORDS_NUM 200 // you can change it if you want
#define FILE_NAME "data.db"

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
void findEntries();
void test();

int main()
{
  
  printf("max num of records per block %ld\n", (BF_BLOCK_SIZE - sizeof(BPLUS_DATA_NODE)) / sizeof(Record));

  test();


  
  // insertEntries();
  // findEntries();

  ////////////////////////////////////////////////
  
}

void insertEntries(){
  BF_Init(LRU);
  BP_CreateFile(FILE_NAME);
  int file_desc;
  BPLUS_INFO* info = BP_OpenFile(FILE_NAME, &file_desc);
  Record record;
  for (int i = 0; i < RECORDS_NUM; i++)
  {
    record = randomRecord();
    BP_InsertEntry(file_desc,info, record);
  }
  BP_CloseFile(file_desc,info);
  BF_Close();
}

void findEntries(){
  int file_desc;
  BPLUS_INFO* info;

  BF_Init(LRU);
  info=BP_OpenFile(FILE_NAME, &file_desc);

  Record tmpRec;  //Αντί για malloc
  Record* result=&tmpRec;
  
  int id=159; 
  printf("Searching for: %d\n",id);
  BP_GetEntry( file_desc,info, id,&result);
  if(result!=NULL)
    printRecord(*result);

  BP_CloseFile(file_desc,info);
  BF_Close();
}


void test(){
  BF_Init(LRU);
  BP_CreateFile(FILE_NAME);

  int file_desc;
  BPLUS_INFO* info;
  info = BP_OpenFile(FILE_NAME, &file_desc);
  
  char name[15] = "John";
  char surname[15] = "Doe";
  char city[15] = "Athens";

  char name2[15] = "John";
  char surname2[15] = "Bic";
  char city2[15] = "Sofia";


  Record rec1;
  rec1.id = 12;
  strcpy(rec1.name, name);
  strcpy(rec1.surname, surname);
  strcpy(rec1.city, city);

  Record rec2;
  rec2.id = 5;
  strcpy(rec2.name, name2);
  strcpy(rec2.surname, surname2);
  strcpy(rec2.city, city2);

  Record rec3;
  rec3.id = 45;
  strcpy(rec3.name, name2);
  strcpy(rec3.surname, surname2);
  strcpy(rec3.city, city2);

  int id = create_data_node(file_desc, info);
  
  insert_rec_in_datanode(file_desc, id, info, rec1);
  insert_rec_in_datanode(file_desc, id, info, rec2);
  insert_rec_in_datanode(file_desc, id, info, rec3);


  print_data_node(file_desc, id);

  BP_CloseFile(file_desc,info);
  BF_Close();
}
