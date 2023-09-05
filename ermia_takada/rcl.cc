#include "frame.hh"

using namespace std;

extern std::atomic<uint64_t> timestampcounter;
extern std::mutex SsnLock;

void print_mode()
{
    cout << "this result is executed by RCL+SSN" << endl;
}

void Transaction::tbegin()
{
    // if (this->lock_flag == false)
    this->txid_ = atomic_fetch_add(&timestampcounter, 1);
    ssn_tbegin();
}

void Transaction::tread(uint64_t key)
{
    // read-own-writes, re-read from previous read in the same tx.
    if (searchWriteSet(key) == true || searchReadSet(key) == true)
        goto FINISH_TREAD;

    // get version to read
    Tuple *tuple;
    tuple = get_tuple(key);

    if (!this->lock_flag)
    {
        Version *expected;
        for (;;)
        {
            expected = tuple->latest_.load(memory_order_acquire);
            if (!tuple->mmt_.r_try_lock())
            {
                if (this->txid_ >= expected->cstamp_.load(memory_order_acquire))
                {
                    this->status_ = Status::aborted;
                    ++res_->local_wwconflict_counts_;
                    goto FINISH_TREAD;
                }
            }
            else
                break;
        }
    }

    Version *ver;
    ver = tuple->latest_.load(memory_order_acquire);
    while (ver->status_.load(memory_order_acquire) != Status::committed || txid_ < ver->cstamp_.load(memory_order_acquire))
    {
        ver = ver->prev_;
        ++res_->local_traversal_counts_;
    }

    ssn_tread(ver, key);

    if (!this->lock_flag)
        tuple->mmt_.r_unlock(); // short duration?

    if (this->status_ == Status::aborted)
    {
        ++res_->local_readphase_counts_;
        goto FINISH_TREAD;
    }
FINISH_TREAD:
    return;
}

void Transaction::twrite(uint64_t key, uint64_t write_val)
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

    //----------------------------------------------------------------
    // deadlock prevention(no-wait)
    /*if (!tuple->mmt_.w_try_lock())
    {
        this->status_ = Status::aborted;
        ++res_->local_wwconflict_counts_;
        goto FINISH_TWRITE;
    }*/
    //----------------------------------------------------------------
    // deadlock prevention(wait-die)
    for (;;)
    {
        expected = tuple->latest_.load(memory_order_acquire);
        if (!tuple->mmt_.w_try_lock())
        {
            //  deadlock prevention for write lock
            if (this->txid_ > expected->cstamp_.load(memory_order_acquire))
            {
                this->status_ = Status::aborted;
                ++res_->local_wdeadlock_abort_counts_;
                goto FINISH_TWRITE;
            }
            //----------------------------------------------------------------
            // deadlock prevention for r-only lock
            if (tuple->rlocked.load() > 0)
            {
                desired->locked_flag_ = true;
                for (auto itr = write_set_.begin(); itr != write_set_.end(); itr++)
                {
                    Tuple *tmp = (*itr).tuple_;
                    if (tmp->rlocked.load() > 0)
                    {
                        this->status_ = Status::aborted;
                        ++res_->local_rdeadlock_abort_counts_;
                        goto FINISH_TWRITE;
                    }
                }
            }
            //----------------------------------------------------------------
        }
        else
            break;
    }
    //----------------------------------------------------------------
    desired->prev_ = expected;
    tuple->latest_ = desired;

    ssn_twrite(desired, key);

    if (this->status_ == Status::aborted)
        ++res_->local_writephase_counts_;

FINISH_TWRITE:
    return;
}

void Transaction::commit()
{
    this->cstamp_ = atomic_fetch_add(&timestampcounter, 1);

    // begin pre-commit
    SsnLock.lock();

    ssn_commit();

    if (this->status_ == Status::committed)
    {
        for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        {
            (*itr).ver_->cstamp_.store(this->cstamp_, memory_order_release);
            (*itr).ver_->status_.store(Status::committed, memory_order_release);
            Tuple *tmp = (*itr).tuple_;
            tmp->mmt_.w_unlock();
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

    // readonlylock unlock
    if (this->lock_flag == true)
    {
        for (auto itr = task_set_sorted_.begin(); itr != task_set_sorted_.end(); itr++)
        {
            Tuple *tmptuple = get_tuple(*itr);
            tmptuple->rlocked.fetch_add(-1);
            tmptuple->mmt_.r_unlock();
        }
        this->lock_flag = false;
        task_set_sorted_.clear();
    }
    read_set_.clear();
    write_set_.clear();
    if (this->abortcount_ != 0)
    {
        res_->local_additionalabort.push_back(this->abortcount_);
        this->abortcount_ = 0;
    }
}

void Transaction::abort()
{
    ssn_abort();

    if (this->lock_flag)
        cout << "error abort" << endl;
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        (*itr).ver_->status_.store(Status::aborted, memory_order_release);
        Tuple *tmp = (*itr).tuple_;
        tmp->mmt_.w_unlock();
    }
    write_set_.clear();
    read_set_.clear();
    ++res_->local_abort_counts_;
    if (isreadonly())
    {
        ++res_->local_readonly_abort_counts_;
        ++this->abortcount_;
    }

    // 提案手法: read only transactionのlock
    if (USE_LOCK == 1 && isreadonly() == true)
    {
        // sorting
        assert(task_set_sorted_.empty());
        for (int i = 0; i < task_set_.size(); i++)
            task_set_sorted_.push_back(task_set_.at(i).key_);
        std::sort(task_set_sorted_.begin(), task_set_sorted_.end());
        task_set_sorted_.erase(std::unique(task_set_sorted_.begin(), task_set_sorted_.end()), task_set_sorted_.end());

        // deadlock(between read locks) prevention
        for (auto itr = task_set_sorted_.begin(); itr != task_set_sorted_.end(); itr++)
        {
            Tuple *tmptuple = get_tuple(*itr);
            tmptuple->rlocked.fetch_add(1);
            for (;;)
            {
                if (tmptuple->mmt_.r_try_lock())
                    break;
            }
            // this->txid_ = this->cstamp_;
        }
        this->lock_flag = true;
    }
}