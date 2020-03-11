#include "btree.h"

//#define POOL_SIZE (1073741824) // 10GB
//#define POOL_SIZE (21474836480) // 20GB
#define POOL_SIZE (53687091200) // 50GB

void clear_cache() {
	int* dummy = new int[1024*1024*256];
	for(int i=0; i<1024*1024*256; i++){
		dummy[i] = i;
	}

	for(int i=100; i<1024*1024*256; i++){
		dummy[i] = dummy[i-rand()%100] + dummy[i+rand()%100];
	}
	
	delete[] dummy;
}

// MAIN
int main(int argc, char** argv)
{
	if(argc < 4){
		fprintf(stderr, "Usage: %s path numData numThreads\n", argv[0]);
		exit(1);
	}
	char path[32];
	strcpy(path, argv[1]);
	int numData = atoi(argv[2]);
	int numThreads = atoi(argv[3]);
	if(numThreads < 1){
		fprintf(stderr, "numThreads must be larger than 1\n");
		exit(1);
	}

   TOID(btree) bt=TOID_NULL(btree);
   PMEMobjpool *pop;

  if (access(path, 0) != 0) {
	pop = pmemobj_create(path, "btree", POOL_SIZE, 0666); 
	if(pop == NULL){
		fprintf(stderr, "pmemobj_create error\n");
		exit(1);
	}
    bt = POBJ_ROOT(pop, btree);
    D_RW(bt)->constructor(pop);
  }
  else {
    pop = pmemobj_open(path, "btree");
	if(pop == NULL){
		fprintf(stderr, "pmemboj_open error\n");
		exit(1);
	}
    bt = POBJ_ROOT(pop, btree);
  }

  struct timespec start, end;
  uint64_t elapsed;

  printf("Params: numData(%d) numThreads(%d)\n", numData, numThreads);
  entry_key_t* keys = new entry_key_t[numData];

  ifstream ifs;
  string dataset = "/home/chahg0129/dataset/input_rand.txt";
  ifs.open(dataset);

  if(!ifs){
    cout << "input loading error!" << endl;
  }

  for(int i=0; i<numData; ++i){
    ifs >> keys[i]; 
  }
  ifs.close();

  vector<thread> insertingThreads;
  vector<thread> searchingThreads;

  auto insert = [&bt, &keys](int from, int to){
	  for(int i=from; i<to; i++)
		  D_RW(bt)->btree_insert(keys[i], reinterpret_cast<char*>(keys[i]));
  };

  int failedSearch = 0;
  auto search = [&bt, &keys, &failedSearch](int from, int to){
	  for(int i=from; i<to; i++){
		  auto ret = D_RW(bt)->btree_search(keys[i]);
		  if(ret != reinterpret_cast<char*>(keys[i])){
			  failedSearch++;
		  }
	  }
  };

/*
  cout << "Warmup Starts" << endl;
  int half = numData / 2;
  for(int i=0; i<half; i++){
	  D_RW(bt)->btree_insert(keys[i], reinterpret_cast<char*>(keys[i]));
  }
  cout << "Warmup: inserted " << half << " key-value pairs" << endl;

  int chunk = half / numThreads;

  
  clock_gettime(CLOCK_MONOTONIC,&start);
  for(int i=0; i<numThreads; i++){
	  if(i != numThreads-1)
		  searchingThreads.emplace_back(thread(search, chunk*i, chunk*(i+1)));
	  else
		  searchingThreads.emplace_back(thread(search, chunk*i, numData));
  }
  for(auto& t: searchingThreads) t.join();
  cout << "SEarch done" << endl;
*/



  int chunk = numData/ numThreads;
  clear_cache();
  clock_gettime(CLOCK_MONOTONIC,&start);
  for(int i=0; i<numThreads; i++){
	  if(i != numThreads-1)
		  insertingThreads.emplace_back(thread(insert, chunk*i, chunk*(i+1)));
	  else
		  insertingThreads.emplace_back(thread(insert, chunk*i, numData));
  }
  for(auto& t: insertingThreads) t.join();
  clock_gettime(CLOCK_MONOTONIC,&end);
  elapsed = (end.tv_sec-start.tv_sec)*1000000000 + (end.tv_nsec-start.tv_nsec);
  cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\t Insertion" << endl;
  


  clear_cache();
  clock_gettime(CLOCK_MONOTONIC,&start);
  for(int i=0; i<numThreads; i++){
	  if(i != numThreads-1)
		  searchingThreads.emplace_back(thread(search, chunk*i, chunk*(i+1)));
	  else
		  searchingThreads.emplace_back(thread(search, chunk*i, numData));
  }
  for(auto& t: searchingThreads) t.join();
  clock_gettime(CLOCK_MONOTONIC,&end);
  elapsed = (end.tv_sec-start.tv_sec)*1000000000 + (end.tv_nsec-start.tv_nsec);
  cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\t Search" << endl;
  cout << "failedSearch\t" << failedSearch << endl;



//  D_RW(bt)->printAll();

  delete[] keys;

  pmemobj_close(pop);
  return 0;
}


