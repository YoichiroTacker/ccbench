#pragma once

#include <vector>

#include "../../include/rwlock.hpp"
#include "../../include/string.hpp"
#include "../../include/util.hpp"

#include "ss2pl_op_element.hpp"
#include "tuple.hpp"

enum class TransactionStatus : uint8_t {
  inFlight,
  committed,
  aborted,
};

extern void writeValGenerator(char *writeVal, size_t val_size, size_t thid);

class TxExecutor {
public:
  int thid;
  std::vector<RWLock*> r_lockList;
  std::vector<RWLock*> w_lockList;
  TransactionStatus status = TransactionStatus::inFlight;

  vector<SetElement<Tuple>> readSet;
  vector<SetElement<Tuple>> writeSet;

  char writeVal[VAL_SIZE];
  char returnVal[VAL_SIZE];

  TxExecutor(int myid) : thid(myid) {
    readSet.reserve(MAX_OPE);
    writeSet.reserve(MAX_OPE);
    r_lockList.reserve(MAX_OPE);
    w_lockList.reserve(MAX_OPE);

    genStringRepeatedNumber(writeVal, VAL_SIZE, thid); 
  }

  SetElement<Tuple> *searchReadSet(uint64_t key);
  SetElement<Tuple> *searchWriteSet(uint64_t key);
  void tbegin();
  char* tread(uint64_t key);
  void twrite(uint64_t key);
  void commit();
  void abort();
  void unlock_list();

  Tuple* get_tuple(Tuple *table, uint64_t key) {
    return &table[key];
  }
};
