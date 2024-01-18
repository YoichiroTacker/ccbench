#include "frame.hh"
#include <string.h>

using namespace std;

extern std::atomic<uint64_t> timestampcounter;
extern std::mutex SsnLock;
extern enum Compilemode MODE;
extern TransactionTable *TMT[thread_num];

void print_mode()
{
    if (MODE == Compilemode::SI)
        cout << "this result is executed by SI+SSN" << endl;
    if (MODE == Compilemode::SI_Repair)
        cout << "this result is executed by SI+SSN+Repair" << endl;
}

void Transaction::tbegin()
{
    this->txid_ = atomic_fetch_add(&timestampcounter, 1);

    // transaction tableに追加
    TransactionTable *tmt, *newelement;
    uint32_t ex_cstamp;
    tmt = __atomic_load_n(&TMT[this->thid_], __ATOMIC_ACQUIRE);
    if (this->status_ == Status::aborted)
        ex_cstamp = this->ex_cstamp_;
    else
        ex_cstamp = 0;

    newelement = new TransactionTable(this->txid_, 0, UINT32_MAX, ex_cstamp, Status::inFlight);
    __atomic_store_n(&TMT[thid_], newelement, __ATOMIC_RELEASE);

    ssn_tbegin();
}

void Transaction::tread(uint64_t key)
{
    if (searchWriteSet(key) == true || searchReadSet(key) == true)
        return;

    Tuple *tuple;
    tuple = get_tuple(key);

    Version *ver;
    ver = tuple->latest_.load(memory_order_acquire);
    while (ver->status_.load(memory_order_acquire) != Status::committed || txid_ < ver->cstamp_.load(memory_order_acquire))
    {
        ver = ver->prev_;
        // ++res_->local_traversal_counts_;
    }

    ssn_tread(ver, key);

    if (this->status_ == Status::aborted)
    {
        //++res_->local_readphase_counts_;
        return;
    }
}

void Transaction::twrite(uint64_t key, std::array<std::byte, DATA_SIZE> write_val)
{
    if (searchWriteSet(key) == true)
        return;

    Tuple *tuple;
    tuple = get_tuple(key);

    Version *expected, *desired;
    desired = new Version();
    desired->cstamp_.store(this->txid_, memory_order_release);
    assert(desired->pstamp_.load(memory_order_acquire) == 0);
    assert(desired->sstamp_.load(memory_order_acquire) == UINT32_MAX);
    desired->sstamp_.store(UINT32_MAX & ~(TIDFLAG));

    Version *vertmp;
    expected = tuple->latest_.load(memory_order_acquire);

    for (;;)
    {
        // prevent dirty write
        if (expected->status_.load(memory_order_acquire) == Status::inFlight)
        {
            if (this->txid_ <= expected->cstamp_.load(memory_order_acquire))
            {
                this->status_ = Status::aborted;
                ++res_->local_wwconflict_counts_;
                return;
            }
            expected = tuple->latest_.load(memory_order_acquire);
            continue;
        }
        // first committer wins
        //  Forbid a transaction to update a record that has a committed version later than its begin timestamp.
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
            return;
        }
        desired->prev_ = expected;
        if (tuple->latest_.compare_exchange_strong(
                expected, desired, memory_order_acq_rel, memory_order_acquire))
            break;
    }

    memcpy(desired->val_.data(), write_val.data(), DATA_SIZE);

    ssn_twrite(desired, key);

    if (this->status_ == Status::aborted)
        //++res_->local_writephase_counts_;
        this->isearlyaborted = true;
}

void Transaction::repair_read()
{
    assert(validated_read_set_.size() + retrying_task_set_.size() == task_set_.size());

    // validate repaired read set
    // validated_read_set_の各データについて、更新されていないかどうか確かめる
    vector<Operation>::iterator itr = validated_read_set_.begin();
    while (itr != validated_read_set_.end())
    {
        if ((*itr).ver_->sstamp_.load(memory_order_acquire) != (UINT32_MAX & ~(TIDFLAG)))
        {
            retrying_task_set_.emplace_back(Ope::READ, (*itr).key_);
            validated_read_set_.erase(itr);
        }
        else
        {
            upReadersBits((*itr).ver_, this->thid_);
            ++itr;
        }
    }
    assert(validated_read_set_.size() + retrying_task_set_.size() == task_set_.size());

    // retry aborted operation
    for (auto itr = retrying_task_set_.begin(); itr != retrying_task_set_.end(); ++itr)
    {
        tread((*itr).key_);
        if (status_ == Status::aborted)
            break;
    }
}

void Transaction::commit()
{
    this->cstamp_ = atomic_fetch_add(&timestampcounter, 1);

    TransactionTable *tmt = __atomic_load_n(&TMT[this->thid_], __ATOMIC_ACQUIRE);
    tmt->status_.store(Status::committing);
    tmt->cstamp_.store(this->cstamp_, memory_order_release);

    // SsnLock.lock();

    // ssn_commit();
    ssn_parallel_commit();

    if (this->status_ == Status::committed)
    {
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        {
            (*itr).ver_->cstamp_.store(this->cstamp_, memory_order_release);
            (*itr).ver_->status_.store(Status::committed, memory_order_release);
        }
        // SsnLock.unlock();
    }
    else
    {
        // SsnLock.unlock();
        return;
    }
    read_set_.clear();
    write_set_.clear();

    istargetTx = false;
    validated_read_set_.clear();
    isearlyaborted = false;
    retrying_task_set_.clear();
}

void Transaction::abort()
{
    TransactionTable *tmt = __atomic_load_n(&TMT[this->thid_], __ATOMIC_ACQUIRE);
    tmt->status_.store(Status::aborted, memory_order_release);

    ssn_abort();

    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        (*itr).ver_->status_.store(Status::aborted, memory_order_release);
    }

    if (MODE == Compilemode::RC && isreadonly() && isearlyaborted == false)
        res_->local_validatedset_size_.push_back(pair(1, this->abortcount_));

    // 提案手法 transaction repair
    if (MODE == Compilemode::SI_Repair && (istargetTx || isreadonly()) && isearlyaborted == false && !read_set_.empty())
    {
        this->ex_cstamp_ = this->cstamp_;
        this->istargetTx = true;

        if (!retrying_task_set_.empty())
            retrying_task_set_.clear(); // 2回目以降のabortの場合
        for (auto itr = read_set_.begin(); itr != read_set_.end(); itr++)
        {
            if (this->pstamp_ < (*itr).ver_->sstamp_.load(memory_order_acquire))
                validated_read_set_.push_back(*itr);
            else
                retrying_task_set_.emplace_back(Ope::READ, (*itr).key_);
        }
        assert(validated_read_set_.size() + retrying_task_set_.size() == task_set_.size());
    }
    write_set_.clear();
    read_set_.clear();
    this->isearlyaborted = false;
}