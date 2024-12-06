#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "bp_file.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"

#define RECORDS_NUM 100 // you can change it if you want
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
void test_insert_rec_in_datanode();

int main()
{
  
  printf("max num of records per block %ld\n", (BF_BLOCK_SIZE - sizeof(BPLUS_DATA_NODE)) / sizeof(Record));
  
  int total_elements = (BF_BLOCK_SIZE - sizeof(BPLUS_INDEX_NODE)) / sizeof(int);
  int max_keys = (total_elements - 1) / 2;
  printf("max num of keys in indexnode is %d\n", max_keys);
  // test_insert_rec_in_datanode();


  
  insertEntries();
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
    BP_InsertEntry(file_desc, info, record);
  }


  printf("block counter: %d\n", info->num_of_blocks);
  printf("height: %d\n\n", info->height);

  // print_data_node(file_desc, 1);
  print_data_node(file_desc, 4);
  print_index_node(file_desc, 2);
  // print_data_node(file_desc, 3);
  print_data_node(file_desc, 16);


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


void test_insert_rec_in_datanode(){
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
  rec1.id = 1;
  strcpy(rec1.name, name);
  strcpy(rec1.surname, surname);
  strcpy(rec1.city, city);

  Record rec20;
  rec20.id = 20;
  strcpy(rec20.name, name2);
  strcpy(rec20.surname, surname2);
  strcpy(rec20.city, city2);

  Record rec3;
  Record rec4;
  Record rec5;
  Record rec15;
  Record rec25;
  Record rec22;

  rec3.id = 3;
  rec4.id = 4;
  rec5.id = 5;
  rec15.id = 15;
  rec25.id = 25;
  rec22.id = 22;

  strcpy(rec3.name, name2);
  strcpy(rec3.surname, surname2);
  strcpy(rec3.city, city2);

  strcpy(rec4.name, name);
  strcpy(rec4.surname, surname);
  strcpy(rec4.city, city);

  strcpy(rec5.name, name2);
  strcpy(rec5.surname, surname2);
  strcpy(rec5.city, city2);
  
  strcpy(rec15.name, name2);
  strcpy(rec15.surname, surname2);
  strcpy(rec15.city, city2);

  strcpy(rec25.name, name); 
  strcpy(rec25.surname, surname);
  strcpy(rec25.city, city);

  strcpy(rec22.name, name2);
  strcpy(rec22.surname, surname2);
  strcpy(rec22.city, city2);


  int left_id = create_data_node(file_desc, info);
  
  insert_rec_in_datanode(file_desc, left_id, info, rec1);
  insert_rec_in_datanode(file_desc, left_id, info, rec3);
  insert_rec_in_datanode(file_desc, left_id, info, rec4);
  insert_rec_in_datanode(file_desc, left_id, info, rec5);


  int index_node = create_index_node(file_desc, info);
  int right_id = split_data_node(file_desc, left_id, info, rec20);
  insert_key_indexnode(file_desc, index_node, info, 5, left_id);

  int new_split = split_data_node(file_desc, right_id, info, rec15);
  insert_key_indexnode(file_desc, index_node, info, 20, right_id);

  insert_rec_in_datanode(file_desc, new_split, info, rec25);
  
  int split2 = split_data_node(file_desc, new_split, info, rec22);

  int index2 = split_index_node(file_desc, info, index_node, 25, split2);

  print_data_node(file_desc, left_id);
  print_data_node(file_desc, right_id);
  print_data_node(file_desc, new_split);
  print_data_node(file_desc, split2);
  print_index_node(file_desc, index_node);
  print_index_node(file_desc, index2);


  BP_CloseFile(file_desc,info);
  BF_Close();
}

