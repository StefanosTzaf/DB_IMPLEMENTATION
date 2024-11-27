#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "bp_file.h"
#include "record.h"
#include <bp_datanode.h>




int find_record_in_data_node(BPLUS_DATA_NODE* data_node, int key) {
    // Binary search for the record with the given key
    int left = 0;
    int right = data_node->num_records - 1;

    while (left <= right) {
        int mid = left + (right - left) / 2;

        if (data_node->records[mid].id == key) {
            return mid;  // Record found
        }

        if (data_node->records[mid].id < key) {
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    // Record not found
    return -1;
}

int split_data_node(BPLUS_DATA_NODE* src_node, BPLUS_DATA_NODE* dest_node) {
    // Determine the split point (middle of the records)
    int split_point = (src_node->num_records + 1) / 2;

    // Initialize destination node
    dest_node->num_records = src_node->num_records - split_point;
    dest_node->next_data_block = src_node->next_data_block;

    // Copy records to the destination node
    memcpy(dest_node->records, 
           &src_node->records[split_point], 
           dest_node->num_records * sizeof(Record));
    
    // Reduce source node's record count
    src_node->num_records = split_point;

    // Link the nodes
    dest_node->next_data_block = src_node->next_data_block;
    src_node->next_data_block = dest_node->block_id;

    // Return the first key of the new node (for index node insertion)
    return dest_node->records[0].id;
}