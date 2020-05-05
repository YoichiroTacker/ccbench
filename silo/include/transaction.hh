#pragma once

#include <iostream>
#include <set>
#include <vector>

#include "../../include/fileio.hh"
#include "../../include/procedure.hh"
#include "../../include/result.hh"
#include "../../include/string.hh"
#include "common.hh"
#include "log.hh"
#include "silo_op_element.hh"
#include "tuple.hh"

#define LOGSET_SIZE 1000

using namespace std;

enum class TransactionStatus : uint8_t {
  kInFlight,
  kCommitted,
  kAborted,
};

class TxnExecutor {
 public:
  vector<ReadElement<Tuple>> read_set_;
  vector<WriteElement<Tuple>> write_set_;
  vector<Procedure> pro_set_;

  vector<LogRecord> log_set_;
  LogHeader latest_log_header_;

  TransactionStatus status_;
  unsigned int thid_;
  /* lock_num_ ...
   * the number of locks in local write set.
   */
  Result* sres_;

  File logfile_;

  Tidword mrctid_;
  Tidword max_rset_, max_wset_;

  char write_val_[VAL_SIZE];
  char return_val_[VAL_SIZE];

  TxnExecutor(int thid, Result* sres);

  void displayWriteSet();
  void begin();
  void read(uint64_t key);
  void write(uint64_t key);
  bool validationPhase();
  void abort();
  void writePhase();
  void wal(uint64_t ctid);
  void lockWriteSet();
  void unlockWriteSet();
  void unlockWriteSet(std::vector<WriteElement<Tuple>>::iterator end);
  ReadElement<Tuple>* searchReadSet(uint64_t key);
  WriteElement<Tuple>* searchWriteSet(uint64_t key);

  Tuple* get_tuple(Tuple* table, uint64_t key) { return &table[key]; }
};
