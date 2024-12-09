#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "bp_file.h"
#include "bp_datanode.h"
#include "bp_indexnode.h"
#include <assert.h>

//################################################################################# //
// ##### TEST FOR SEQUENTIAL INSERTION, DUPLICATE INSERTION AMD NON-EXISTENT SEARCH //
// ################################################################################ //

#define RECORDS_NUM 1000 // you can change it if you want
#define FILE_NAME_1 "data1.db"
#define FILE_NAME_2 "data2.db"
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

void testSequentialInsert();
void testDuplicateInsert();
void testSearchNonExistent();

int main(){
    testSequentialInsert();
    testDuplicateInsert();
    testSearchNonExistent();

}


// Ελεγχος για συνεχομενες εισαγωγες με id απο 0 εως 100
void testSequentialInsert() {
    printf("\n--- Testing Sequential Insertion ---\n");
    
    BF_Init(LRU);
    BP_CreateFile(FILE_NAME_1);
    
    int file_desc;
    BPLUS_INFO* info = BP_OpenFile(FILE_NAME_1, &file_desc);
    
    // Εισαγωγη 100 συνεχομενων εγγραφων
    Record record;
    for (int i = 1; i <= 100; i++) {
        record.id = i;
        sprintf(record.name, "name_%d", i);
        sprintf(record.surname, "surname_%d", i);
        sprintf(record.city, "city_%d", i);
        
        int result = BP_InsertEntry(file_desc, info, record);
        if(result == -1){
            printf("Failed to insert record with id %d\n", i);
        }
    }
    
    // Επαληθευση αποτελεσματων

    Record tmpRec; // Αντί για malloc
    Record *result = &tmpRec;

    bool test_ok = true;

    for(int i = 1; i <= 100; i++){
        if(BP_GetEntry(file_desc, info, i, &result) == 0){
            // printf("Found record: ");
            // printRecord(*result);
        } 
        else{
            test_ok = false;
            printf("Failed to find record with id %d\n", i);
        }
    }

    if(test_ok){
        printf("All records found\n");
    }
    else{
        printf("Test failed\n");
    }
    
    BP_CloseFile(file_desc, info);
    BF_Close();
}


//Ελεγχος για εισαγωγη διπλοτυπων εγγραφων
void testDuplicateInsert(){
    printf("\n--- Testing Duplicate Insertion ---\n");
    
    BF_Init(LRU);
    BP_CreateFile(FILE_NAME_2);
    
    int file_desc;
    BPLUS_INFO* info = BP_OpenFile(FILE_NAME_2, &file_desc);
    
    // Εισαγωγη 100 συνεχομενων εγγραφων
    Record record;
    for (int i = 1; i <= 100; i++) {
        record.id = i;
        sprintf(record.name, "name_%d", i);
        sprintf(record.surname, "surname_%d", i);
        sprintf(record.city, "city_%d", i);
        
        int result = BP_InsertEntry(file_desc, info, record);
        if(result == -1){
            printf("Failed to insert record with id %d\n", i);
        }
    }

    for(int i = 0; i < 10; i++){
        record.id = i;
        sprintf(record.name, "name_%d", i);
        sprintf(record.surname, "surname_%d", i);
        sprintf(record.city, "city_%d", i);
        
        int result = BP_InsertEntry(file_desc, info, record);
        if (result == -1){
            printf("Record with id %d already exists\n", i);
        }
    }

}


// Αναζητηση μη υπαρχουσων εγγραφων
void testSearchNonExistent(){
    printf("\n--- Testing Search for Non-existent Records ---\n");
    
    BF_Init(LRU);
    BP_CreateFile("search_test.db");
    
    int file_desc;
    BPLUS_INFO* info = BP_OpenFile("search_test.db", &file_desc);
    
    Record record;
    for(int i = 1; i <= 100; i++){
        record.id = i;
        sprintf(record.name, "name_%d", i);
        sprintf(record.surname, "surname_%d", i);
        sprintf(record.city, "city_%d", i);
        
        int result = BP_InsertEntry(file_desc, info, record);
        if(result == -1){
            printf("Failed to insert record with id %d\n", i);
        }
    }
    

    Record* result = NULL;

    //η πρωτη μονο εγγραφη πρεπει να βρεθει
    int ids[] = {105, 115, 125, 135, 145};
    
    for(int i = 0; i < 5; i++) {
        printf("Searching for non-existent id: %d -> ", ids[i]);
        
        int search_result = BP_GetEntry(file_desc, info, ids[i], &result);
        
        if(search_result == -1){
            printf("correctly failed to find record %d\n", ids[i]);
        } 
        else{
            printf("Unexpectedly found record %d\n\n", ids[i]);
        }
    }
    
    BP_CloseFile(file_desc, info);
    BF_Close();
}


