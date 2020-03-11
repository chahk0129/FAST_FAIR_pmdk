#include "btree.h"
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <algorithm>
#include <thread>

using namespace std;

#define SEARCH
// #define DELETION
// #define MIXED
#define POOL_SIZE (53687091200) // 50GB
//#define POOL_SIZE (10737418240) // 10GB

uint64_t numBitFlips = 0;
void clear_cache();


int main (int argc, char* argv[])
{
  if(argc < 3){
      cerr << "Usage: " << argv[0] << "path numData numThreads" << endl;
      exit(1);
  }
  char path[32];
  strcpy(path, argv[1]);
  int numData = atoi(argv[2]);
  int32_t numThreads = atoi(argv[3]);
  if (numThreads < 1) {
    cerr << "Error: main() - #" << __LINE__ << " numThreads < 1." << endl;
    exit(1);
  }

  TOID(btree) bt = OID_NULL;
  PMEMobjpool* pop;
  if(access(path, 0) != 0){
      pop = pmemobj_create(path, "btree", POOL_SIZE, 0666);
      if(pop == NULL){
	  cerr << "pmemobj_create error" << endl;
	  exit(1);
      }
      bt = POBJ_ROOT(pop, btree);
      D_RW(bt)->constructor(pop);
  }
  else{
      if((pop = pmemobj_open(path, "btree")) == NULL){
	  cerr << "pmemobj_open error" << endl;
	  exit(1);
      }
      bt = POBJ_ROOT(pop, btree);
  }
 
  struct timespec start, end;
  int64_t elapsed;


  cout << "Params: numData(" << numData << ") numThreads(" << numThreads << ")" << endl;
  cout << PAGESIZE << " - (" << sizeof(class page) << ")" << endl;

  int64_t* keys = (int64_t*)malloc(sizeof(int64_t)*numData);
  ifstream ifs;
  string dataset = "/home/chahg0129/dataset/input_rand.txt";
  ifs.open(dataset);
  if (!ifs) {
    cerr << "No file." << endl;
    exit(1);
  } else {
    for (int i = 0; i < numData; i++) {
      ifs >> keys[i];
    }
    ifs.close();
    cout << dataset << " is used." << endl;
  }

  int half = numData/2;
  int chunk = half/numThreads;
  for(int i=0; i<half; i++){ // Warmup
    D_RW(bt)->btree_insert(keys[i], reinterpret_cast<char*>(keys[i]));
  }

  vector<int> num_read(numThreads);
	vector<int> num_write(numThreads);
	vector<int> num_delete(numThreads);
  vector<thread> mixed_threads;

  auto mixed_work = [&bt, &keys, &num_write, &num_read, &num_delete, &half](int from, int to, int tid) {
    int read_cnt = 0;
    int write_cnt = 0;
    int delete_cnt = 0;
    int insert_idx = half + from;
    int search_idx = from;
    for (int i = from; i < to; i+= 1000) {
      int r = rand() % 100;
      if ( r < 10 ) {
        for ( int j = insert_idx; j < insert_idx+1000 ; j++ ) {
          D_RW(bt)->btree_insert(keys[j], reinterpret_cast<char*>(keys[j]));
	  D_RW(bt)->btree_delete(keys[j]);
        }
        insert_idx += 1000;
        write_cnt += 1000;
	delete_cnt += 1000;
      }
      else if (r < 30){
	  for(int j= insert_idx; j< insert_idx +1000; j++){
	      D_RW(bt)->btree_insert(keys[j], reinterpret_cast<char*>(keys[j]));
	  }
	  insert_idx += 1000;
	  write_cnt += 1000;
      }
      else{
	for ( int j = search_idx; j < search_idx+1000 ; j++ ) {
          D_RW(bt)->btree_search(keys[j % half]);
        }
        search_idx += 1000;
        read_cnt+=1000;
      }
    }
    num_write[tid] = write_cnt;
    num_read[tid] = read_cnt;
    num_delete[tid] = delete_cnt;
  };

  clear_cache();
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int i = 0; i < numThreads; i++) {
    if (i != numThreads -1 )
      mixed_threads.emplace_back(thread(mixed_work, chunk*i, chunk*(i+1), i));
    else
      mixed_threads.emplace_back(thread(mixed_work, chunk*i, half, i));
  }

  for (auto &thread : mixed_threads) {
    thread.join();
  }

  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsed = (end.tv_sec-start.tv_sec)*1000000000 + (end.tv_nsec-start.tv_nsec);
  cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*((half)/(elapsed/1000.0))) << "\tOps/sec" << endl;
  int readsum = 0;
	int writesum = 0;
	int deletesum = 0;
  for ( auto i : num_read ) {
    readsum += i;
  }
	for(auto i: num_write){
		writesum += i;
	}
	for(auto i: num_delete){
	    deletesum += i;
	}
//    cout << "ReadNum(" << sum << ")\t" << "WriteNum(" << (half - 1000 * sum)/1000 << ")" << endl;
  cout << "ReadNum(" << readsum << ")\t" << "WriteNum(" << writesum << ")\t" << "DeleteNum(" << deletesum << ")" << endl;

} 

void clear_cache() {
#ifndef DEBUG
  int* dummy = new int[1024*1024*256];
  for (int i=0; i<1024*1024*256; i++) {
    dummy[i] = i;
  }

  for (int i=100;i<1024*1024*256;i++) {
    dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
  }

  delete[] dummy;
#endif
}
