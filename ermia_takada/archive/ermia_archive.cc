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

#include "../include/zipf.hh"

/*#define TIDFLAG 1
#define CACHE_LINE_SIZE 64
#define PAGE_SIZE 4096
#define clocks_per_us 2100
#define extime 3
#define max_ope 10
#define max_ope_readonly 10
#define ronly_ratio 30
#define rratio 50
#define thread_num 10
#define tuple_num 1000
#define skew 0      */

using namespace std;

class Result
{
public:
    uint64_t local_abort_counts_ = 0;
    uint64_t local_commit_counts_ = 0;
    uint64_t total_abort_counts_ = 0;
    uint64_t total_commit_counts_ = 0;
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

    void displayAllResult()
    {
        cout << "abort_counts_:\t\t\t" << total_abort_counts_ << endl;
        cout << "commit_counts_:\t\t\t" << total_commit_counts_ << endl;
        cout << "read SSNcheck abort:\t\t" << total_readphase_counts_ << endl;
        cout << "write SSNcheck abort:\t\t" << total_writephase_counts_ << endl;
        cout << "commit SSNcheck abort:\t\t" << total_commitphase_counts_ << endl;
        cout << "ww conflict abort:\t\t" << total_wwconflict_counts_ << endl;
        cout << "total_readonly_abort:\t\t" << total_readonly_abort_counts_ << endl;
        // displayAbortRate
        long double ave_rate =
            (double)total_abort_counts_ /
            (double)(total_commit_counts_ + total_abort_counts_);
        cout << fixed << setprecision(4) << "abort_rate:\t\t\t" << ave_rate << endl;
        // cout << "traversal counts:\t\t" << total_traversal_counts_ << endl;

        // count the number of recursing abort
        vector<int> tmp;
        tmp = total_additionalabort;
        std::sort(tmp.begin(), tmp.end());
        tmp.erase(std::unique(tmp.begin(), tmp.end()), tmp.end());
        for (auto itr = tmp.begin(); itr != tmp.end(); itr++)
        {
            size_t count = std::count(total_additionalabort.begin(), total_additionalabort.end(), *itr);
            cout << *itr << " " << count << endl;
        }
    }

    void addLocalAllResult(const Result &other)
    {
        total_abort_counts_ += other.local_abort_counts_;
        total_commit_counts_ += other.local_commit_counts_;
        total_readphase_counts_ += other.local_readphase_counts_;
        total_writephase_counts_ += other.local_writephase_counts_;
        total_commitphase_counts_ += other.local_commitphase_counts_;
        total_wwconflict_counts_ += other.local_wwconflict_counts_;
        total_traversal_counts_ += other.local_traversal_counts_;
        total_readonly_abort_counts_ += other.local_readonly_abort_counts_;
        total_additionalabort.insert(total_additionalabort.end(), other.local_additionalabort.begin(), other.local_additionalabort.end());
    }
};

bool isReady(const std::vector<char> &readys)
{
    for (const char &b : readys)
    {
        if (!b)
            return false;
    }
    return true;
}

void waitForReady(const std::vector<char> &readys)
{
    while (!isReady(readys))
    {
        _mm_pause();
    }
}

std::vector<Result>
    ErmiaResult;

void initResult() { ErmiaResult.resize(thread_num); }

std::atomic<uint64_t> timestampcounter(1); // timestampを割り当てる

enum class Status : uint8_t
{
    inFlight,
    committed,
    aborted,
};

class Version
{
public:
    Version *prev_; // Pointer to overwritten version
    uint64_t val_;
    std::atomic<uint32_t> pstamp_; // Version access stamp, eta(V)
    std::atomic<uint32_t> sstamp_; // Version successor stamp, pi(V)
    std::atomic<uint32_t> cstamp_; // Version creation stamp, c(V)
    std::atomic<Status> status_;

    Version() { init(); }

    void init()
    {
        pstamp_.store(0, std::memory_order_release);
        sstamp_.store(UINT32_MAX, std::memory_order_release);
        cstamp_.store(0, std::memory_order_release);
        status_.store(Status::inFlight, std::memory_order_release);
        this->prev_ = nullptr;
    }
};

class Tuple
{
public:
    uint64_t key;
    std::atomic<Version *> latest_;

    Tuple() { latest_.store(nullptr); }
};

Tuple *Table;       // databaseのtable
std::mutex SsnLock; // giant lock

enum class Ope : uint8_t
{
    READ,
    WRITE,
};

class Operation
{
public:
    uint64_t key_;
    uint64_t value_; // read value
    Version *ver_;

    Operation(uint64_t key, Version *ver, uint64_t value) : key_(key), ver_(ver), value_(value) {}
    Operation(uint64_t key, Version *ver) : key_(key), ver_(ver) {}
};

class Task
{
public:
    Ope ope_;
    uint64_t key_;
    uint64_t write_val_;

    Task(Ope ope, uint64_t key) : ope_(ope), key_(key) {}
    Task(Ope ope, uint64_t key, uint64_t write_val) : ope_(ope), key_(key), write_val_(write_val) {}
};

void viewtask(vector<Task> &tasks)
{
    for (auto itr = tasks.begin(); itr != tasks.end(); itr++)
    {
        if (itr->ope_ == Ope::READ)
        {
            cout << "R" << itr->key_ << " ";
        }
        else
        {
            cout << "W" << itr->key_ << " ";
        }
    }
    cout << endl;
}

void makeTask(std::vector<Task> &tasks, Xoroshiro128Plus &rnd, FastZipf &zipf)
{
    tasks.clear();
    if ((rnd.next() % 100) < ronly_ratio)
    {
        for (size_t i = 0; i < max_ope_readonly; ++i)
        {
            uint64_t tmpkey;
            // decide access destination key.
            tmpkey = zipf() % tuple_num;
            tasks.emplace_back(Ope::READ, tmpkey);
        }
    }
    else
    {
        for (size_t i = 0; i < max_ope; ++i)
        {
            uint64_t tmpkey;
            // decide access destination key.
            tmpkey = zipf() % tuple_num;
            // decide operation type.
            if ((rnd.next() % 100) < rratio)
            {
                tasks.emplace_back(Ope::READ, tmpkey);
            }
            else
            {
                tasks.emplace_back(Ope::WRITE, tmpkey, zipf());
            }
        }
    }
}

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

    vector<Operation> read_set_;  // write set
    vector<Operation> write_set_; // read set
    vector<Task> task_set_;       // 生成されたtransaction

    Result *res_;

    Transaction() {}

    Transaction(uint8_t thid, Result *res) : thid_(thid), res_(res)
    {
        read_set_.reserve(max_ope_readonly);
        write_set_.reserve(max_ope);
        task_set_.reserve(max_ope_readonly);
    }

    bool searchReadSet(unsigned int key)
    {
        for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
        {
            if ((*itr).key_ == key)
                return true;
        }
        return false;
    }

    bool searchWriteSet(unsigned int key)
    {
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        {
            if ((*itr).key_ == key)
                return true;
        }
        return false;
    }

    void tbegin()
    {
        this->txid_ = atomic_fetch_add(&timestampcounter, 1);
        this->cstamp_ = 0;
        pstamp_ = 0;
        sstamp_ = UINT32_MAX;
        status_ = Status::inFlight;
    }

    void ssn_tread(Version *ver, uint64_t key)
    {
        // update eta(t) with w:r edges
        this->pstamp_ = max(this->pstamp_, ver->cstamp_.load(memory_order_acquire));

        if (ver->sstamp_.load(memory_order_acquire) == (UINT32_MAX))
            //// no overwrite yet
            read_set_.emplace_back(key, ver, ver->val_);
        else
            // update pi with r:w edge
            this->sstamp_ = min(this->sstamp_, ver->sstamp_.load(memory_order_acquire));
        verify_exclusion_or_abort();
    }

    void tread(uint64_t key)
    {
        // read-own-writes, re-read from previous read in the same tx.
        if (searchWriteSet(key) == true || searchReadSet(key) == true)
            goto FINISH_TREAD;

        // get version to read
        Tuple *tuple;
        tuple = get_tuple(key);
        Version *ver;
        ver = tuple->latest_.load(memory_order_acquire);
        while (ver->status_.load(memory_order_acquire) != Status::committed || txid_ < ver->cstamp_.load(memory_order_acquire))
        {
            ver = ver->prev_;
            ++res_->local_traversal_counts_;
        }

        ssn_tread(ver, key);

        if (this->status_ == Status::aborted)
        {
            ++res_->local_readphase_counts_;
            goto FINISH_TREAD;
        }
    FINISH_TREAD:
        return;
    }

    void ssn_twrite(Version *desired, uint64_t key)
    {
        // Insert my tid for ver->prev_->sstamp_
        desired->prev_->pstamp_.store(this->txid_, memory_order_release);

        // Update eta with w:r edge
        this->pstamp_ = max(this->pstamp_, desired->prev_->pstamp_.load(memory_order_acquire));

        verify_exclusion_or_abort();

        // t.reads.discard(v)
        for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
        {
            if ((*itr).key_ == key)
            {
                read_set_.erase(itr);
                break;
            }
        }
    }

    void twrite(uint64_t key, uint64_t write_val)
    {
        // update local write set
        if (searchWriteSet(key) == true)
            return;

        Tuple *tuple;
        tuple = get_tuple(key);

        // If v not in t.writes:
        Version *expected, *desired;
        desired = new Version();
        desired->cstamp_.store(this->txid_, memory_order_release);
        desired->val_ = write_val;

        Version *vertmp;
        expected = tuple->latest_.load(memory_order_acquire);

        for (;;)
        {
            // first committer wins
            //  Forbid a transaction to update a record that has a committed version later than its begin timestamp.
            if (expected->status_.load(memory_order_acquire) == Status::inFlight)
            {
                if (this->txid_ <= expected->cstamp_.load(memory_order_acquire))
                {
                    this->status_ = Status::aborted;
                    ++res_->local_wwconflict_counts_;
                    goto FINISH_TWRITE;
                }
                expected = tuple->latest_.load(memory_order_acquire);
                continue;
            }

            // if latest version is not comitted, vertmp is latest committed version.
            vertmp = expected;
            while (vertmp->status_.load(memory_order_acquire) != Status::committed)
            {
                vertmp = vertmp->prev_;
            }

            if (txid_ < vertmp->cstamp_.load(memory_order_acquire))
            {
                // Writers must abort if they would overwirte a version created after their snapshot.
                this->status_ = Status::aborted;
                ++res_->local_wwconflict_counts_;
                goto FINISH_TWRITE;
            }
            desired->prev_ = expected;
            if (tuple->latest_.compare_exchange_strong(
                    expected, desired, memory_order_acq_rel, memory_order_acquire))
                break;
        }

        ssn_twrite(desired, key);

        //   t.writes.add(V)
        write_set_.emplace_back(key, desired);

        if (this->status_ == Status::aborted)
            ++res_->local_writephase_counts_;

    FINISH_TWRITE:
        return;
    }

    void ssn_commit()
    {
        // finalize pi(T)
        this->sstamp_ = min(this->sstamp_, this->cstamp_);
        for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
        {
            this->sstamp_ = min(this->sstamp_, (*itr).ver_->sstamp_.load(memory_order_acquire));
        }

        // finalize eta(T)
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        {
            this->pstamp_ = max(this->pstamp_, (*itr).ver_->prev_->pstamp_.load(memory_order_acquire));
        }

        // ssn_check_exclusion
        if (pstamp_ < sstamp_)
            this->status_ = Status::committed;
        else
        {
            status_ = Status::aborted;
            ++res_->local_commitphase_counts_;
            return;
        }

        // update eta(V)
        for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
        {
            (*itr).ver_->pstamp_.store((max((*itr).ver_->pstamp_.load(memory_order_acquire), this->cstamp_)), memory_order_release);
        }

        // update pi
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        {
            (*itr).ver_->prev_->sstamp_.store(this->sstamp_, memory_order_release);
            // initialize new version
            (*itr).ver_->pstamp_.store(this->cstamp_, memory_order_release);
        }
    }

    void commit()
    {
        this->cstamp_ = atomic_fetch_add(&timestampcounter, 1);

        // begin pre-commit
        SsnLock.lock();

        ssn_commit();

        if (this->status_ == Status::committed)
        {
            // これはSIでも必要
            for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
            {
                (*itr).ver_->cstamp_.store(this->cstamp_, memory_order_release);
                (*itr).ver_->status_.store(Status::committed, memory_order_release);
            }
            SsnLock.unlock();
        }
        else
        {
            if (this->status_ == Status::inFlight)
                cout << "commit error" << endl;
            SsnLock.unlock();
            return;
        }
        read_set_.clear();
        write_set_.clear();

        if (this->abortcount_ != 0)
            res_->local_additionalabort.push_back(this->abortcount_);
        this->abortcount_ = 0;
        return;
    }

    void ssn_abort()
    {
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        {
            Version *next_committed = (*itr).ver_->prev_;
            while (next_committed->status_.load(memory_order_acquire) != Status::committed)
                next_committed = next_committed->prev_;
            // cancel successor mark(sstamp)
            next_committed->sstamp_.store(UINT32_MAX, memory_order_release);
        }
    }

    void abort()
    {
        ssn_abort();

        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        {
            (*itr).ver_->status_.store(Status::aborted, memory_order_release);
        }
        write_set_.clear();

        // notify that this transaction finishes reading the version now.
        read_set_.clear();
        ++res_->local_abort_counts_;
        if (isreadonly())
        {
            ++res_->local_readonly_abort_counts_;
            ++this->abortcount_;
        }
    }

    void verify_exclusion_or_abort()
    {
        if (this->pstamp_ >= this->sstamp_)
        {
            this->status_ = Status::aborted;
        }
    }

    static Tuple *get_tuple(uint64_t key)
    {
        return (&Table[key]);
    }

    bool isreadonly()
    {
        for (auto itr = task_set_.begin(); itr != task_set_.end(); itr++)
        {
            if (itr->ope_ == Ope::WRITE)
            {
                return false;
            }
        }
        return true;
    }
};

void displayParameter()
{
    cout << "max_ope:\t\t\t" << max_ope << endl;
    cout << "rratio:\t\t\t\t" << rratio << endl;
    cout << "thread_num:\t\t\t" << thread_num << endl;
}

void makeDB()
{
    posix_memalign((void **)&Table, PAGE_SIZE, (tuple_num) * sizeof(Tuple));
    for (int i = 0; i < tuple_num; i++)
    {
        Table[i].key = 0;
        Version *verTmp = new Version();
        verTmp->status_.store(Status::committed, memory_order_release);
        verTmp->val_ = 0;
        Table[i].latest_.store(verTmp, memory_order_release);
    }
}

void worker(size_t thid, char &ready, const bool &start, const bool &quit)
{
    Xoroshiro128Plus rnd;
    rnd.init();
    Result &myres = std::ref(ErmiaResult[thid]);
    FastZipf zipf(&rnd, skew, tuple_num);

    Transaction trans(thid, (Result *)&ErmiaResult[thid]);

    ready = true;

    while (start == false)
    {
    }

    while (quit == false)
    {
        makeTask(trans.task_set_, rnd, zipf);
        // viewtask(trans.task_set_);

    RETRY:
        if (quit == true)
            break;

        trans.tbegin();
        for (auto itr = trans.task_set_.begin(); itr != trans.task_set_.end();
             ++itr)
        {
            if ((*itr).ope_ == Ope::READ)
            {
                trans.tread((*itr).key_);
            }
            else if ((*itr).ope_ == Ope::WRITE)
            {
                trans.twrite((*itr).key_, (*itr).write_val_);
            }
            // early abort.
            if (trans.status_ == Status::aborted)
            {
                trans.abort();

                goto RETRY;
            }
        }
        trans.commit();
        if (trans.status_ == Status::committed)
        {
            myres.local_commit_counts_++;
        }
        else if (trans.status_ == Status::aborted)
        {
            trans.abort();
            goto RETRY;
        }
    }
    return;
}

int main(int argc, char *argv[])
{
    // displayParameter();
    makeDB();
    chrono::system_clock::time_point starttime, endtime;

    bool start = false;
    bool quit = false;
    initResult();
    std::vector<char> readys(thread_num);

    std::vector<std::thread> thv;
    for (size_t i = 0; i < thread_num; ++i)
    {
        thv.emplace_back(worker, i, std::ref(readys[i]), std::ref(start),
                         std::ref(quit));
    }
    waitForReady(readys);

    starttime = chrono::system_clock::now();
    start = true;
    this_thread::sleep_for(std::chrono::milliseconds(1000 * extime));
    quit = true;

    for (auto &th : thv)
    {
        th.join();
    }
    endtime = chrono::system_clock::now();

    double time = static_cast<double>(chrono::duration_cast<chrono::microseconds>(endtime - starttime).count() / 1000.0);
    // printf("time %lf[ms]\n", time);

    for (unsigned int i = 0; i < thread_num; ++i)
    {
        ErmiaResult[0].addLocalAllResult(ErmiaResult[i]);
    }
    ErmiaResult[0].displayAllResult();

    uint64_t result = (ErmiaResult[0].total_commit_counts_ * 1000) / time;
    cout << "latency[ns]:\t\t\t" << powl(10.0, 9.0) / result * thread_num
         << endl;
    cout << "throughput[tps]:\t\t" << result << endl;

    std::quick_exit(0);

    return 0;
}
