#ifndef BP_DATANODE_H
#define BP_DATANODE_H
#include <record.h>
#include <record.h>
#include <bf.h>
#include <bp_file.h>
#include <bp_indexnode.h>


#define CALL_BF(call)         \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK)        \
    {                         \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


typedef struct {
    int is_data_node;
    int num_records; // αριθμος εγγραφων
    int block_id;    // id του block
    int next_block;  // id του επομενου block δεδομενων
    int minKey;     // το μικροτερο κλειδι του block
    int parent_id; //id του γονεα
} BPLUS_DATA_NODE;


int create_data_node(int file_desc, BPLUS_INFO* bplus_info);
BPLUS_DATA_NODE* get_metadata_datanode(int file_desc, int block_id);
void print_data_node(int fd, int id);
void insert_rec_in_datanode(int fd, int node, BPLUS_INFO* bplus_info, Record rec);
int split_data_node(int fd, int node, BPLUS_INFO* bplus_info, Record rec);



#endif 