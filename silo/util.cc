#include <stdio.h>
#include <sys/syscall.h> // syscall(SYS_gettid),
#include <sys/time.h>
#include <sys/types.h> // syscall(SYS_gettid),
#include <unistd.h> // syscall(SYS_gettid),

#include <bitset>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <fstream>

#include "../include/check.hpp"
#include "../include/cache_line_size.hpp"
#include "../include/debug.hpp"
#include "../include/random.hpp"
#include "../include/zipf.hpp"

#include "include/atomic_tool.hpp"
#include "include/common.hpp"
#include "include/procedure.hpp"
#include "include/transaction.hpp"
#include "include/tuple.hpp"

void 
chkArg(const int argc, char *argv[])
{
  if (argc != 11) {
    cout << "usage:./main TUPLE_NUM MAX_OPE THREAD_NUM RRATIO RMW ZIPF_SKEW YCSB CLOCK_PER_US EPOCH_TIME EXTIME" << endl << endl;

    cout << "example:./main 1000000 10 24 50 off 0 on 2400 40 3" << endl << endl;
    cout << "TUPLE_NUM(int): total numbers of sets of key-value (1, 100), (2, 100)" << endl;
    cout << "MAX_OPE(int):    total numbers of operations" << endl;
    cout << "THREAD_NUM(int): total numbers of thread." << endl;
    cout << "RRATIO : read ratio [%%]" << endl;
    cout << "RMW : read modify write. on or off."<< endl;
    cout << "ZIPF_SKEW : zipf skew. 0 ~ 0.999..." << endl;
    cout << "YCSB : on or off. switch makeProcedure function." << endl;
    cout << "CLOCK_PER_US: CPU_MHZ" << endl;
    cout << "EPOCH_TIME(int)(ms): Ex. 40" << endl;
    cout << "EXTIME: execution time." << endl << endl;
    cout << "Tuple " << sizeof(Tuple) << endl;
    cout << "uint64_t_64byte " << sizeof(uint64_t_64byte) << endl;
    cout << "KEY_SIZE : " << KEY_SIZE << endl;
    cout << "VAL_SIZE : " << VAL_SIZE << endl;
    exit(0);
  }
  chkInt(argv[1]);
  chkInt(argv[2]);
  chkInt(argv[3]);
  chkInt(argv[4]);
  chkInt(argv[8]);
  chkInt(argv[9]);
  chkInt(argv[10]);

  TUPLE_NUM = atoi(argv[1]);
  MAX_OPE = atoi(argv[2]);
  THREAD_NUM = atoi(argv[3]);
  RRATIO = atoi(argv[4]);
  string argrmw = argv[5];
  ZIPF_SKEW = atof(argv[6]);
  string argycsb = argv[7];
  CLOCK_PER_US = atof(argv[8]);
  EPOCH_TIME = atoi(argv[9]);
  EXTIME = atoi(argv[10]);
  
  if (RRATIO > 100) {
    ERR;
  }

  if (ZIPF_SKEW >= 1) {
    cout << "ZIPF_SKEW must be 0 ~ 0.999..." << endl;
    ERR;
  }

  if (argycsb == "on")
    YCSB = true;
  else if (argycsb == "off")
    YCSB = false;
  else
    ERR;

  if (THREAD_NUM < 2) {
    printf("One thread is epoch thread, and others are worker threads.\n\
So you have to set THREAD_NUM >= 2.\n\n");
  }

  try {
    if (posix_memalign((void**)&ThLocalEpoch, CACHE_LINE_SIZE, THREAD_NUM * sizeof(uint64_t_64byte)) != 0) ERR;  //[0]は使わない
    if (posix_memalign((void**)&CTIDW, CACHE_LINE_SIZE, THREAD_NUM * sizeof(uint64_t_64byte)) != 0) ERR; //[0]は使わない
  } catch (bad_alloc) {
    ERR;
  }
  //init
  for (unsigned int i = 0; i < THREAD_NUM; ++i) {
    ThLocalEpoch[i].obj = 0;
    CTIDW[i].obj = 0;
  }
}

bool
chkSpan(struct timeval &start, struct timeval &stop, long threshold)
{
  long diff = 0;
  diff += (stop.tv_sec - start.tv_sec) * 1000 * 1000 + (stop.tv_usec - start.tv_usec);
  if (diff > threshold) return true;
  else return false;
}

bool
chkClkSpan(uint64_t &start, uint64_t &stop, uint64_t threshold)
{
  uint64_t diff = 0;
  diff = stop - start;
  if (diff > threshold) return true;
  else return false;
}

bool
chkEpochLoaded()
{
  uint64_t nowepo = atomicLoadGE();
//全てのワーカースレッドが最新エポックを読み込んだか確認する．
  for (unsigned int i = 1; i < THREAD_NUM; ++i) {
    if (__atomic_load_n(&(ThLocalEpoch[i].obj), __ATOMIC_ACQUIRE) != nowepo) return false;
  }

  return true;
}


void 
displayDB() 
{
  Tuple *tuple;
  for (unsigned int i = 0; i < TUPLE_NUM; ++i) {
    tuple = &Table[i];
    cout << "------------------------------" << endl; //-は30個
    cout << "key: " << i << endl;
    cout << "val: " << tuple->val << endl;
    cout << "TIDword: " << tuple->tidword.obj << endl;
    cout << "bit: " << tuple->tidword.obj << endl;
    cout << endl;
  }
}

void 
displayPRO(Procedure *pro) 
{
  for (unsigned int i = 0; i < MAX_OPE; ++i) {
      cout << "(ope, key, val) = (";
    switch(pro[i].ope){
      case Ope::TREAD:
        cout << "TREAD";
        break;
      case Ope::TWRITE:
        cout << "TWRITE";
        break;
      default:
        break;
    }
      cout << ", " << pro[i].key
      << ", " << pro[i].val << ")" << endl;
  }
}

void
genLogFile(std::string &logpath, const int thid)
{
  // 変数定義
  const int PATHNAME_SIZE = 512;
  char pathname[PATHNAME_SIZE];

  // 変数初期化
  memset(pathname, '\0', PATHNAME_SIZE);

  // カレントディレクトリ取得
  if (getcwd(pathname, PATHNAME_SIZE) == NULL) ERR;

  logpath = pathname;
  logpath += "/log/log" + to_string(thid);

  createEmptyFile(logpath);
}

void 
makeDB() 
{
  Tuple *tmp;
  Xoroshiro128Plus rnd;
  rnd.init();

  try {
    if (posix_memalign((void**)&Table, 64, (TUPLE_NUM) * sizeof(Tuple)) != 0) ERR;
  } catch (bad_alloc) {
    ERR;
  }

  for (unsigned int i = 0; i < TUPLE_NUM; ++i) {
    tmp = &Table[i];
    tmp->tidword.epoch = 1;
    tmp->tidword.latest = 1;
    tmp->tidword.lock = 0;
    tmp->val[0] = 'a'; tmp->val[1] = '\0';
  }

}

void 
makeProcedure(Procedure *pro, Xoroshiro128Plus &rnd) 
{
  for (unsigned int i = 0; i < MAX_OPE; ++i) {
    if ((rnd.next() % 100) < RRATIO)
      pro[i].ope = Ope::TREAD;
    else
      pro[i].ope = Ope::TWRITE;
    
    pro[i].key = rnd.next() % TUPLE_NUM;
  }
}

void 
makeProcedure(Procedure *pro, Xoroshiro128Plus &rnd, FastZipf &zipf) {
  for (unsigned int i = 0; i < MAX_OPE; ++i) {
    if ((rnd.next() % 100) < RRATIO)
      pro[i].ope = Ope::TREAD;
    else
      pro[i].ope = Ope::TWRITE;

    pro[i].key = zipf() % TUPLE_NUM;
  }
}

void
setThreadAffinity(int myid)
{
#ifdef Linux
  pid_t pid = syscall(SYS_gettid);
  cpu_set_t cpu_set;

  CPU_ZERO(&cpu_set);
  CPU_SET(myid % sysconf(_SC_NPROCESSORS_CONF), &cpu_set);

  if (sched_setaffinity(pid, sizeof(cpu_set_t), &cpu_set) != 0)
    ERR;
#endif // Linux

  return;
}

void
waitForReadyOfAllThread()
{
  unsigned int expected, desired;
  expected = Running.load(std::memory_order_acquire);
  do {
    desired = expected + 1;
  } while (!Running.compare_exchange_weak(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire));

  while (Running.load(std::memory_order_acquire) != THREAD_NUM);
  return;
}

void
writeValGenerator(char *writeVal, size_t val_size, size_t thid)
{
  // generate write value for this thread.
  uint num(thid), digit(1);
  while (num != 0) {
    num /= 10;
    if (num != 0) ++digit;
  }
  char thidString[digit];
  sprintf(thidString, "%ld", thid); 
  for (size_t i = 0; i < val_size;) {
    for (uint j = 0; j < digit; ++j) {
      writeVal[i] = thidString[j];
      ++i;
      if (i == val_size - 2) {
        break;
      }
    }
  }
  writeVal[val_size - 1] = '\0';
  // -----
}
