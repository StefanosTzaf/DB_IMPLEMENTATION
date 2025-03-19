#include <merge.h>
#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include "chunk.h"
#include "hp_file.h"
#include <string.h>

void merge(int input_FileDesc, int chunkSize, int bWay, int output_FileDesc)
{
    // Variables for the merge process
    CHUNK_Iterator chunkIterator = CHUNK_CreateIterator(input_FileDesc, chunkSize);
    CHUNK *chunks = malloc(bWay * sizeof(CHUNK));
    CHUNK_RecordIterator *recordIterators = malloc(bWay * sizeof(CHUNK_RecordIterator));
    Record *currentRecords = malloc(bWay * sizeof(Record));
    bool *hasValidRecord = malloc(bWay * sizeof(bool));
    int maxRecordsPerBlock = HP_GetMaxRecordsInBlock(output_FileDesc);
    Record *outputBuffer = malloc(maxRecordsPerBlock * sizeof(Record));
    int outputBufferIndex = 0;
    bool moreChunksAvailable = true;

    while (moreChunksAvailable)
    {
        int activeChunks = 0;

        // Initialize hasValidRecord array
        for (int i = 0; i < bWay; i++)
        {
            hasValidRecord[i] = false;
        }

        // Get bWay chunks
        for (int i = 0; i < bWay; i++)
        {
            if (CHUNK_GetNext(&chunkIterator, &chunks[i]) == 0)
            {
                recordIterators[i] = CHUNK_CreateRecordIterator(&chunks[i]);
                if (CHUNK_GetNextRecord(&recordIterators[i], &currentRecords[i]) == 0)
                {
                    hasValidRecord[i] = true;
                    activeChunks++;
                }
            }
            else
            {
                moreChunksAvailable = false;
                break;
            }
        }

        if (activeChunks == 0)
            break;

        // Merge the current set of chunks
        while (activeChunks > 0)
        {
            int minIndex = -1;

            // Find the smallest record among active chunks
            for (int i = 0; i < bWay; i++)
            {
                if (hasValidRecord[i])
                {
                    if (minIndex == -1 ||
                        strcmp(currentRecords[i].name, currentRecords[minIndex].name) < 0 ||
                        (strcmp(currentRecords[i].name, currentRecords[minIndex].name) == 0 &&
                         strcmp(currentRecords[i].surname, currentRecords[minIndex].surname) < 0))
                    {
                        minIndex = i;
                    }
                }
            }

            if (minIndex == -1)
                break;

            // Add the smallest record to output buffer
            outputBuffer[outputBufferIndex++] = currentRecords[minIndex];

            // Flush buffer if full
            if (outputBufferIndex >= maxRecordsPerBlock)
            {
                for (int i = 0; i < outputBufferIndex; i++)
                {
                    HP_InsertEntry(output_FileDesc, outputBuffer[i]);
                }
                outputBufferIndex = 0;
            }

            // Get next record from the chunk that provided the minimum
            if (CHUNK_GetNextRecord(&recordIterators[minIndex], &currentRecords[minIndex]) != 0)
            {
                hasValidRecord[minIndex] = false;
                activeChunks--;
            }
        }
    }

    // Flush remaining records
    if (outputBufferIndex > 0)
    {
        for (int i = 0; i < outputBufferIndex; i++)
        {
            HP_InsertEntry(output_FileDesc, outputBuffer[i]);
        }
    }

    // Free allocated memory
    free(chunks);
    free(recordIterators);
    free(currentRecords);
    free(hasValidRecord);
    free(outputBuffer);
}