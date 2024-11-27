#ifndef BP_FILE_H
#define BP_FILE_H
#include <record.h>
#include <stdbool.h>



typedef struct {
    int file_desc;              // 
    int root_block;             // μπλοκ ριζας
    int height;                 // υψος B+ tree
    int record_size;            // μεγεθος μιας εγγραφης
    int key_size;               // μεγεθος κλειδιου
    int first_data_block;       // το id του πρωτου block δεδομενων
    bool is_open;               // αν το αρχειο ειναι ανοιχτο ή οχι
    int max_records_per_block;  // μεγιστος αριθμος εγγραφων ανα block
    int num_of_blocks;           // αριθμος block
} BPLUS_INFO;


/** BP_CreateFile: Αυτή η συνάρτηση χρησιμοποιείται για τη δημιουργία και 
 * αρχικοποίηση ενός κενού αρχείου B+ δέντρου με το όνομα fileName. Κατά 
 * τη δημιουργία του αρχείου, προστίθενται επίσης τα μεταδεδομένα BP_info 
 * στο πρώτο μπλοκ του αρχείου. Εάν η λειτουργία είναι επιτυχής, επιστρέφει 
 * 0, αλλιώς επιστρέφει -1.**/
int BP_CreateFile(char *fileName);

/** BP_OpenFile: Αυτή η συνάρτηση ανοίγει το αρχείο B+ δέντρου με το όνομα 
 * fileName και ανακτά τα μεταδεδομένα του. Η BP_info είναι η δομή που 
 * επιστρέφεται και περιέχει τα μεταδεδομένα του B+ δέντρου. Η παράμετρος 
 * *file_desc είναι ο αναγνωριστικός αριθμός αρχείου που λαμβάνεται από το 
 * BF_OpenFile.**/
BPLUS_INFO* BP_OpenFile(char *fileName, int *file_desc);

/** BP_CloseFile: Αυτή η συνάρτηση κλείνει το αρχείο B+ δέντρου που 
 * αναγνωρίζεται από το file_desc. Εάν είναι επιτυχής, επιστρέφει 0, 
 * διαφορετικά επιστρέφει -1. Σε περίπτωση επιτυχούς κλεισίματος, 
 * απελευθερώνει επίσης τη μνήμη που καταλαμβάνει η δομή BP_info, η οποία 
 * αποθηκεύει τη μορφή του B+ δέντρου στη μνήμη.**/
int BP_CloseFile(int file_desc,BPLUS_INFO* info);

/** BP_InsertEntry: Αυτή η συνάρτηση εισάγει μία νέα εγγραφή στη δομή του 
 * B+ δέντρου. Το αρχείο αναγνωρίζεται από το file_desc, τα μεταδεδομένα 
 * του B+ δέντρου βρίσκονται στο BP_info, και η εγγραφή που θα εισαχθεί 
 * ορίζεται από τη δομή Record. Η συνάρτηση εντοπίζει τον κατάλληλο κόμβο 
 * δεδομένων για το κλειδί της εγγραφής και την εισάγει, κάνοντας τις 
 * απαραίτητες αλλαγές για να διατηρηθεί η ισορροπία του δέντρου. Εάν η 
 * εισαγωγή είναι επιτυχής, επιστρέφει το blockId όπου εισήχθη η εγγραφή, 
 * διαφορετικά επιστρέφει -1.**/
int BP_InsertEntry(int file_desc,BPLUS_INFO* bplus_info, Record record);

/** BP_GetEntry: Αυτή η συνάρτηση αναζητά μια εγγραφή σε ένα B+ δέντρο με 
 * κλειδί που ταιριάζει με το συγκεκριμένο `id`. Ξεκινώντας από τη ρίζα, 
 * διασχίζει το B+ δέντρο για να εντοπίσει το σχετικό φύλλο όπου θα ήταν 
 * αποθηκευμένο το κλειδί. Αν βρεθεί μια ταιριαστή εγγραφή, η συνάρτηση 
 * θέτει το `result` να δείχνει στην εγγραφή αυτή και επιστρέφει `0` για 
 * να υποδείξει επιτυχία. Αν δεν υπάρχει τέτοια εγγραφή, η συνάρτηση θέτει 
 * το `result` σε `nullptr` και επιστρέφει `-1` για να υποδείξει αποτυχία.**/
int BP_GetEntry(int file_desc, BPLUS_INFO* header_info, int id, Record** result);




#endif 
