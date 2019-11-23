#include "mapreduce.h"
#include "threadpool.h"
#include <vector>
#include <iostream>
#include <sys/stat.h>
#include <algorithm>
#include <pthread.h>

struct table
{
    //An array of vector of pairs that the mappers will map to
    std::vector<std::pair<std::string, std::string>> *data;
    int numPartitions;
    
    //A lock array that will be mapped to each vector so that
    //Locking is more efficient
    std::vector<pthread_mutex_t> locks; // one mutex per element
    Reducer reducerFunc;

    void init(int num_partitions) {
        numPartitions = num_partitions;
        data = new std::vector<std::pair<std::string, std::string>>[numPartitions];

        //Initialize each pthread in the array
        for (int i=0; i<numPartitions; i++){
            pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
            locks.push_back(mutex);
            pthread_mutex_init(&locks[i], NULL);
        }
    }

    //Create a key-value pair, lock the vector at the given partition, and push the key-value pair
    void addKVP(char* key, char* value, unsigned long partition){
        int index = (int) partition;
        std::pair<std::string, std::string> kvp ( (std::string(key)), (std::string(value)) );
        pthread_mutex_lock(&(locks[index]));
        data[index].push_back(kvp);
        pthread_mutex_unlock(&(locks[index]));
    }
};
table *mapperTable = new table();

//Hash function
unsigned long MR_Partition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

//Add a key value pair
void MR_Emit(char *key, char *value) {
    mapperTable->addKVP(key, value, MR_Partition(key, mapperTable->numPartitions));
}

char *MR_GetNext(char *key, int partition_number) {
    std::string keyString = std::string(key);

    //If there are no more pairs in the vector, then return NULL
    if (mapperTable->data[partition_number].empty())
        return NULL;

    //Since the vector is sorted, the key we're counting for will be the first
    //key in the vector. Hence, if the first key value is equal to the given key
    //Then pop that pair, and return a string (in this case it is the key itself)
    std::string tableKey = mapperTable->data[partition_number].back().first;
    std::string tableValue = mapperTable->data[partition_number].back().second;
    if (tableKey == keyString){
        mapperTable->data[partition_number].pop_back();
        return &tableValue[0];
    }
    //Otherwise (we popped all the same key values in the vector), return NULL
    else
        return NULL;
}

bool sortinrev(const std::pair<std::string,std::string> &a,  
               const std::pair<std::string,std::string> &b) 
{ 
    return (a.first > b.first); 
} 

//First, sort the vector by the first value in the pair (the key in this case)
//Then, keep calling the reducer function until thet vector is empty with the first value
//In the vector
void MR_ProcessPartition(int partition_number) {
    std::sort(mapperTable->data[partition_number].begin(), 
                mapperTable->data[partition_number].end(),
                sortinrev);
    std::string key;

    while (!mapperTable->data[partition_number].empty()) {
        key = mapperTable->data[partition_number].back().first;
        mapperTable->reducerFunc(&key[0], partition_number);
    }
}

void MR_Run(int num_files, char *filenames[],
            Mapper map, int num_mappers,
            Reducer concate, int num_reducers)
{
    //First, initialize the mapper table with the number of partitions
    mapperTable->init(num_reducers);
    //Create the mappers threadpool
    ThreadPool_t *mapper_tp = ThreadPool_create(num_mappers);
    
    //Create a vector of files so that we can sort on the file's size
    std::vector<char *> files;
    for (int i=0; i < num_files; i++) {
        files.push_back(filenames[i]);
    }

    //Sort by file size (first file will be the largest)
    std::sort(files.begin(), files.end(), [](char *a, char *b) -> bool {
        struct stat sb1;
        struct stat sb2;

        if (lstat(a, &sb1) == -1) {
            perror("lstat");
        }

        if (lstat(b, &sb2) == -1) {
            perror("lstat");
        }
        return (long long) sb1.st_size > (long long) sb2.st_size;
    });

    //Add the files. Since the threadpool continually pushes to front, and pops from the back
    //The first file it will handle will be the largest file
    for (int i=0; i<num_files; i++){
        ThreadPool_add_work(mapper_tp, (thread_func_t) map, files[i]);
    }
    //Get the work by calling threadpool destroy
    ThreadPool_destroy(mapper_tp);

    //Save the reducer function in the table to easily call it from anywhere
    mapperTable->reducerFunc = concate;
    
    //Create a vector reducer threads
    std::vector<pthread_t> reducerThreads;
    pthread_attr_t attr;
    if (pthread_attr_init(&attr) != 0)
        perror("pthread_attr_init");

    //Create n threads, where the ith thread is assigned to the ith partition
    for (int i=0; i<num_reducers; i++){
        reducerThreads.push_back(0);

        if (pthread_create(&(reducerThreads[i]), &attr,
                            (void *(*)(void *)) MR_ProcessPartition, (void *) (size_t) i) != 0)
            perror("pthread_create");
    }

    
    if (pthread_attr_destroy(&attr) != 0)
        perror("pthread_attr_destroy");

    //Now join on all the threads to complete the work
    for (unsigned int i=0; i<reducerThreads.size(); i++){
        pthread_join(reducerThreads[i], NULL);
    }    

    //Delete all the structures that were initialized with new in order
    delete[] mapperTable->data;
    delete mapperTable;
}