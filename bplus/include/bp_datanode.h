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
    int num_records; // αριθμος εγγραφων
    int block_id;    // id του block
    int next_block;  // id του επομενου block δεδομενων

} BPLUS_DATA_NODE;


int create_data_node(int file_desc, BPLUS_INFO* bplus_info);
BPLUS_DATA_NODE* get_metadata_datanode(int file_desc, int block_id);


//den exw ulopoihsei - den jerw an xreiazodai akomh
int find_record_in_data_node(BPLUS_DATA_NODE* data_node, int key);
int split_data_node(BPLUS_DATA_NODE* src_node);

#endif 