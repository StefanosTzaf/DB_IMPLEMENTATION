#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

bool shouldSwap(Record *rec1, Record *rec2)
{
    // Comparison logic: Sorting by name, then surname
    int nameComparison = strcmp(rec1->name, rec2->name);
    if (nameComparison < 0)
        return false; // rec1 comes before rec2
    if (nameComparison > 0)
        return true; // rec1 comes after rec2

    // If names are the same, compare surnames
    return strcmp(rec1->surname, rec2->surname) > 0;
}

void sort_FileInChunks(int file_desc, int numBlocksInChunk)
{
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, numBlocksInChunk);
    CHUNK chunk;

    // Iterate through all chunks and sort them
    while (CHUNK_GetNext(&iterator, &chunk) == 0)
    {
        sort_Chunk(&chunk);
    }
}

void sort_Chunk(CHUNK *chunk)
{
    int recordsPerBlock = HP_GetMaxRecordsInBlock(chunk->file_desc);
    int totalRecords = chunk->recordsInChunk;
    Record *records = malloc(totalRecords * sizeof(Record));

    // Retrieve all records from the chunk
    int recordIndex = 0;
    for (int blockId = chunk->from_BlockId; blockId <= chunk->to_BlockId; blockId++)
    {
        for (int i = 0; i < recordsPerBlock; i++)
        {
            if (recordIndex >= totalRecords)
                break;
            HP_GetRecord(chunk->file_desc, blockId, i, &records[recordIndex++]);
        }
        HP_Unpin(chunk->file_desc, blockId);
    }

    // Sort the records using bubble sort (or other in-place sorting algorithm)
    for (int i = 0; i < totalRecords - 1; i++)
    {
        for (int j = 0; j < totalRecords - i - 1; j++)
        {
            if (shouldSwap(&records[j], &records[j + 1]))
            {
                Record temp = records[j];
                records[j] = records[j + 1];
                records[j + 1] = temp;
            }
        }
    }

    // Write sorted records back to the chunk
    recordIndex = 0;
    for (int blockId = chunk->from_BlockId; blockId <= chunk->to_BlockId; blockId++)
    {
        for (int i = 0; i < recordsPerBlock; i++)
        {
            if (recordIndex >= totalRecords)
                break;
            HP_UpdateRecord(chunk->file_desc, blockId, i, records[recordIndex++]);
        }
        HP_Unpin(chunk->file_desc, blockId);
    }

    free(records);
}