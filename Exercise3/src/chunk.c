#include <stdio.h>
#include <stdbool.h>
#include "chunk.h"
#include "merge.h"
#include "hp_file.h"

CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk)
{
    CHUNK_Iterator iterator;
    iterator.file_desc = fileDesc;
    iterator.current = 1; // Start from first block
    iterator.blocksInChunk = blocksInChunk;

    // Get the actual last block ID and verify it
    iterator.lastBlocksID = HP_GetIdOfLastBlock(fileDesc);
    if (iterator.lastBlocksID <= 0)
    {
        // Handle error case
        iterator.lastBlocksID = 1;
    }
    return iterator;
}

int CHUNK_GetNext(CHUNK_Iterator *iterator, CHUNK *chunk)
{
    // Strict boundary check
    if (iterator->current > (iterator->lastBlocksID / iterator->blocksInChunk))
        return -1;

    chunk->file_desc = iterator->file_desc;
    chunk->from_BlockId = ((iterator->current - 1) * iterator->blocksInChunk) + 1; // number of blocks till the current one plus one

    // Ensure we don't read past the end of file
    int remainingBlocks = iterator->lastBlocksID - chunk->from_BlockId + 1;
    if (remainingBlocks <= 0) return -1;

    // Calculate actual chunk size
    if (remainingBlocks < iterator->blocksInChunk)
        chunk->blocksInChunk = remainingBlocks;
    else
        chunk->blocksInChunk = iterator->blocksInChunk;

    chunk->to_BlockId = chunk->from_BlockId + chunk->blocksInChunk - 1;

    // Double-check boundary
    if (chunk->to_BlockId > iterator->lastBlocksID)
    {
        chunk->to_BlockId = iterator->lastBlocksID;
        chunk->blocksInChunk = chunk->to_BlockId - chunk->from_BlockId + 1;
    }

    // Calculate records in this chunk
    chunk->recordsInChunk = chunk->blocksInChunk * HP_GetMaxRecordsInBlock(iterator->file_desc);

    // Update iterator position
    iterator->current++;

    return 0;
}

int CHUNK_GetIthRecordInChunk(CHUNK *chunk, int i, Record *record)
{
    // Create a record iterator for the chunk
    CHUNK_RecordIterator iterator = CHUNK_CreateRecordIterator(chunk);

    // Iterate through the records to find the ith record
    int currentRecordIndex = 0;
    while (CHUNK_GetNextRecord(&iterator, record) == 0)
    {
        if (currentRecordIndex == i)
        {
            if (HP_GetRecord(chunk->file_desc, iterator.currentBlockId, iterator.cursor, record) != 0)
                return -1;
            return 0; // Found the ith record
        }
        currentRecordIndex++;
    }

    return -1; // Record index out of range or error occurred
}

int CHUNK_UpdateIthRecord(CHUNK *chunk, int i, Record record)
{
    // Create a record iterator for the chunk
    CHUNK_RecordIterator iterator = CHUNK_CreateRecordIterator(chunk);

    // Iterate through the records to find the ith record
    Record tempRecord;
    int currentRecordIndex = 0;
    while (CHUNK_GetNextRecord(&iterator, &tempRecord) == 0)
    {
        if (currentRecordIndex == i)
        {
            // Update the ith record
            int recordsPerBlock = HP_GetMaxRecordsInBlock(chunk->file_desc);
            int blockOffset = iterator.currentBlockId - chunk->from_BlockId;
            int recordOffset = iterator.cursor - 1; // Account for post-increment in cursor
            if (recordOffset < 0)
            {
                recordOffset = recordsPerBlock - 1;
                blockOffset--;
            }

            int blockId = chunk->from_BlockId + blockOffset;
            if (HP_UpdateRecord(chunk->file_desc, blockId, recordOffset, record) != 0)
            {
                return -1; // Failed to update record
            }
            return 0; // Successfully updated
        }
        currentRecordIndex++;
    }

    return -1; // Record index out of range or error occurred
}

void CHUNK_Print(CHUNK chunk)
{
    Record record;
    int recordsPerBlock = HP_GetMaxRecordsInBlock(chunk.file_desc);

    for (int blockId = chunk.from_BlockId; blockId <= chunk.to_BlockId; blockId++)
    {
        for (int i = 0; i < recordsPerBlock; i++)
        {
            if (HP_GetRecord(chunk.file_desc, blockId, i, &record) == 0)
            {
                printf("Record %d: ", i);
                printRecord(record); // Replace with actual print logic
            }
        }
        if (HP_Unpin(chunk.file_desc, blockId) != 0)
        {
            printf("Failed to unpin block %d\n", blockId);
        }
    }
}

CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk)
{
    CHUNK_RecordIterator iterator;
    iterator.chunk = *chunk;
    iterator.currentBlockId = chunk->from_BlockId;
    iterator.cursor = 0;
    return iterator;
}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator, Record *record)
{
    if (iterator->currentBlockId > iterator->chunk.to_BlockId)
        return -1; // No more records

    if (HP_GetRecord(iterator->chunk.file_desc, iterator->currentBlockId, iterator->cursor, record) != 0)
        return -1; // Failed to get record

    if (HP_Unpin(iterator->chunk.file_desc, iterator->currentBlockId) != 0)
        printf("Failed to unpin block %d\n", iterator->currentBlockId);

    iterator->cursor++;
    int recordsPerBlock = HP_GetMaxRecordsInBlock(iterator->chunk.file_desc);
    if (iterator->cursor >= recordsPerBlock)
    {
        iterator->cursor = 0;
        iterator->currentBlockId++;
    }

    return 0;
}
