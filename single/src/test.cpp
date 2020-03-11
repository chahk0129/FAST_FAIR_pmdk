#include "btree.h"
#include <cstdlib>
#include <unistd.h>

//#define POOL_SIZE (1073741824) // 1GB
#define POOL_SIZE (10737418240) // 10GB
//#define POOL_SIZE (53687091200) // 50GB
//#define SEARCH
#define DELETION
using namespace std;

void clear_cache() {
  int* dummy = new int[1024*1024*256];
  for(int i=0;i<1024*1024*256; i++)
    dummy[i] = i;
  for(int i=100;i<1024*1024*256;i++)
    dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
  delete[] dummy;
}

// MAIN
int main(int argc, char** argv)
{
  // Parsing arguments
  if(argc < 3){
	  fprintf(stderr, "Usage: %s path numData\n", argv[0]);
	  exit(1);
  }
  char path[32];
  strcpy(path, argv[1]);
  int numData = atoi(argv[2]);
  TOID(btree) bt=TOID_NULL(btree);
  PMEMobjpool *pop;

  if (access(path, 0) != 0) {
    pop = pmemobj_create(path, "btree", POOL_SIZE, 0666);
    bt = POBJ_ROOT(pop, btree);
    D_RW(bt)->constructor(pop);
  }
  else {
    pop = pmemobj_open(path, "btree");
    bt = POBJ_ROOT(pop, btree);
//    if(TOID_IS_NULL(bt)) D_RW(bt)->constructor(pop);
    D_RW(bt)->constructor(pop);
  }

  struct timespec start, end;

  int64_t* keys = (int64_t*)malloc(sizeof(int64_t)*numData);
  ifstream ifs;
  string dataset = "/home/chahg0129/dataset/input_rand.txt";
  ifs.open(dataset);
  if(!ifs) {
    cout << "input loading error!" << endl;
    delete[] keys;
    exit(-1);
  }

  for(int i=0; i<numData; ++i)
    ifs >> keys[i]; 

  ifs.close();
  printf("PAGE SIZE: %d\n", sizeof(page));

  clear_cache();
  {
    clock_gettime(CLOCK_MONOTONIC,&start);

    for(int i = 0; i < numData; ++i) {
      D_RW(bt)->btree_insert(keys[i], reinterpret_cast<char*>(keys[i])); 
    }

    clock_gettime(CLOCK_MONOTONIC,&end);

    uint64_t elapsed_time = 
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);

    printf("INSERT elapsed_time: %lu usec\t %lu Ops/sec\n", elapsed_time/1000, (uint64_t)(1000000*(numData/(elapsed_time/1000.0))));
#ifdef BREAKDOWN
    printf("Node Traversal(usec): %lu\n", (elapsed_time - insertTime)/1000);
    printf("Node Update(usec): %lu\n", (insertTime - clwbTime)/1000);
    printf("clwb(usec): %lu\n", (clwbTime)/1000);
#endif
 }

#ifdef SEARCH
  {
  int failedSearch = 0;
  clear_cache();
    clock_gettime(CLOCK_MONOTONIC,&start);

    for(int i = 0; i < numData; ++i) {
      auto ret = D_RW(bt)->btree_search(keys[i]);
	  if(ret != reinterpret_cast<char*>(keys[i])){
		  failedSearch++;
	  }
    }

    clock_gettime(CLOCK_MONOTONIC,&end);

    uint64_t elapsed_time = 
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);

    printf("SEARCH elapsed_time: %lu usec\t %lu Ops/sec\n", elapsed_time/1000, (uint64_t)(1000000*(numData/(elapsed_time/1000.0))));
  printf("FailedSearch: %d\n", failedSearch);
  }
#endif

#ifdef DELETION
  {
	  clear_cache();
	  clock_gettime(CLOCK_MONOTONIC, &start);
	for(int i=0; i<numData/10; i++){
		D_RW(bt)->btree_delete(keys[i]);
	}
    clock_gettime(CLOCK_MONOTONIC,&end);

    uint64_t elapsed_time = 
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);

    printf("DELETION elapsed_time: %lu usec\t %lu Ops/sec\n", elapsed_time/1000, (uint64_t)(1000000*((numData/10)/(elapsed_time/1000.0))));


    clear_cache();
	  clock_gettime(CLOCK_MONOTONIC, &start);
    for(int i=numData/10; i<numData; i++){
	D_RW(bt)->btree_search(keys[i]);
    }
    clock_gettime(CLOCK_MONOTONIC,&end);
    elapsed_time = 
      (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
    printf("SEARCH AFTER DELETION elapsed_time: %lu usec\t %lu Ops/sec\n", elapsed_time/1000, (uint64_t)(1000000*((numData - numData/10)/(elapsed_time/1000.0))));

  }
#endif
  delete[] keys;
  pmemobj_close(pop);
  return 0;
}


