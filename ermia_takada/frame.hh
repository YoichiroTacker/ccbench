#pragma once

#include <atomic>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <array>
#include <xmmintrin.h>
#include <chrono>
#include <algorithm>
#include <set>
#include <new>
#include <cassert>
#include <cstddef>
#include <cstdint>

#include "../include/zipf.hh"

using namespace std;

class Result
{
public:
    uint64_t local_abort_counts_ = 0;
    uint64_t local_commit_counts_ = 0;
    uint64_t total_abort_counts_ = 0;
    uint64_t total_commit_counts_ = 0;
    uint64_t local_scan_abort_counts_ = 0;
    uint64_t total_scan_abort_counts_ = 0;
    uint64_t local_scan_commit_counts_ = 0;
    uint64_t total_scan_commit_counts_ = 0;
    uint64_t local_readphase_counts_ = 0;
    uint64_t local_writephase_counts_ = 0;
    uint64_t local_commitphase_counts_ = 0;
    uint64_t local_wwconflict_counts_ = 0;
    uint64_t total_readphase_counts_ = 0;
    uint64_t total_writephase_counts_ = 0;
    uint64_t total_commitphase_counts_ = 0;
    uint64_t total_wwconflict_counts_ = 0;
    uint64_t local_traversal_counts_ = 0;
    uint64_t total_traversal_counts_ = 0;
    uint64_t local_readonly_abort_counts_ = 0;
    uint64_t total_readonly_abort_counts_ = 0;
    vector<int> local_additionalabort;
    vector<int> total_additionalabort;
    uint64_t local_rdeadlock_abort_counts_ = 0;
    uint64_t total_rdeadlock_abort_counts_ = 0;
    uint64_t local_wdeadlock_abort_counts_ = 0;
    uint64_t total_wdeadlock_abort_counts_ = 0;

    void
    displayAllResult(double time);

    void addLocalAllResult(const Result &other);
};

enum class Compilemode : uint8_t
{
    RC,
    RC_Repair,
    RCL,
    RCL_Saferetry,
    RCL_ELR,
};

enum class Status : uint8_t
{
    inFlight,
    committed,
    aborted,
};

class Version
{
public:
    Version *prev_;                        // Pointer to overwritten version
    std::array<std::byte, DATA_SIZE> val_; // value
    std::atomic<uint32_t> pstamp_;         // Version access stamp, eta(V)
    std::atomic<uint32_t> sstamp_;         // Version successor stamp, pi(V)
    std::atomic<uint32_t> cstamp_;         // Version creation stamp, c(V)
    std::atomic<Status> status_;
    std::atomic<uint32_t> pstamp_for_rlock_; // 提案手法用, eta
    bool locked_flag_;                       // rlockによって待たされたupdate transaction

    Version() { init(); }

    void init();
};

class WRLock
{
public:
    std::atomic<int> counter;
    bool isupgraded;
    WRLock()
    {
        counter.store(0, std::memory_order_release);
        isupgraded = false;
    }

    bool w_try_lock() // 1
    {
        int expected, desired(1);
        for (;;)
        {
            expected = counter.load(std::memory_order_acquire);
            if (expected != 0)
                return false;
            if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
                return true;
        }
    }

    bool r_try_lock() //-1
    {
        int expected, desired;
        for (;;)
        {
            expected = counter.load(std::memory_order_acquire);
            if (expected != 1)
                desired = expected - 1;
            else
                return false;
            if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
                return true;
        }
    }

    void w_unlock()
    {
        auto expected = counter.load(std::memory_order_relaxed);
        assert(expected == 1);
        counter.store(0, std::memory_order_release);
    }

    void r_unlock()
    {
        int expected, desired;
        for (;;)
        {
            expected = counter.load(std::memory_order_acquire);
            assert(expected <= -1);
            desired = expected + 1;
            if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
                break;
        }
    }

    // if the transaction already have r-lock,
    /*void rw_upgrade()
    {
        int expected, desired(1);
        for (;;)
        {
            expected = counter.load(std::memory_order_acquire);
            if (expected != -1)
                continue;
            if (counter.compare_exchange_strong(expected, desired, std::memory_order_acq_rel, std::memory_order_acquire))
                break;
        }
    }*/
};

class Tuple
{
public:
    uint64_t key;
    std::atomic<Version *> latest_;
    // std::mutex mt_;
    std::atomic<size_t> rlocked;
    WRLock mmt_;

    Tuple()
    {
        latest_.store(nullptr);
        rlocked.store(0);
    }
};

enum class Ope : uint8_t
{
    READ,
    WRITE,
};

class Operation
{
public:
    uint64_t key_;
    Version *ver_;
    Tuple *tuple_; // for lock
    std::array<std::byte, DATA_SIZE> value_;

    Operation(uint64_t key) : key_(key) {}
    Operation(uint64_t key, Version *ver, Tuple *tuple) : key_(key), ver_(ver), tuple_(tuple) {}
    Operation(uint64_t key, Version *ver) : key_(key), ver_(ver) {}
    Operation(uint64_t key, Version *ver, std::array<std::byte, DATA_SIZE> value) : key_(key), ver_(ver), value_(value) {}
};

class Task
{
public:
    Ope ope_;
    uint64_t key_;
    std::array<std::byte, DATA_SIZE> write_val_;

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
    Task(Ope ope, uint64_t key, std::array<std::byte, DATA_SIZE> write_val) : ope_(ope), key_(key), write_val_(write_val) {}
};

class Transaction
{
public:
    uint8_t thid_;                 // thread ID
    uint32_t cstamp_ = 0;          // Transaction end time, c(T)
    uint32_t pstamp_ = 0;          // Predecessor high-water mark, η (T)
    uint32_t sstamp_ = UINT32_MAX; // Successor low-water mark, pi (T)
    uint32_t txid_;                // TID and begin timestamp
    Status status_ = Status::inFlight;
    int abortcount_ = 0;
    bool istargetTx = false; // rlockをかけているtransaction
    uint32_t ex_cstamp_ = 0;

    vector<Operation> read_set_;  // write set
    vector<Operation> write_set_; // read set
    vector<Task> task_set_;       // 生成されたtransaction
    vector<int> task_set_sorted_; // for rcl+robust safe retry

    // repair
    vector<Task> retrying_task_set_;
    vector<Operation> validated_read_set_;
    bool isearlyaborted = false;

    Result *res_;

    Transaction() {}

    Transaction(uint8_t thid, Result *res) : thid_(thid), res_(res)
    {
        read_set_.reserve(max_ope_readonly);
        write_set_.reserve(max_ope);
        task_set_.reserve(max_ope_readonly);
        task_set_sorted_.reserve(max_ope_readonly);
        // repair
        validated_read_set_.reserve(max_ope_readonly);
    }

    bool searchReadSet(unsigned int key);

    bool searchWriteSet(unsigned int key);

    void ssn_tbegin();

    void tbegin();

    void ssn_tread(Version *ver, uint64_t key);

    void tread(uint64_t key);

    void ssn_twrite(Version *desired, uint64_t key);

    void twrite(uint64_t key, std::array<std::byte, DATA_SIZE> write_val);

    void ssn_commit();

    void commit();

    void ssn_abort();

    void abort();

    void verify_exclusion_or_abort();

    static Tuple *get_tuple(uint64_t key);

    bool isreadonly();

    void repair_read();

    void ssn_repair_commit();

    void utils_abort();

    void utils_commit();
};

void print_mode();

void viewtask(vector<Task> &tasks);