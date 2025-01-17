#include "frame.hh"
#include <string.h>

using namespace std;

extern std::atomic<uint64_t> timestampcounter;
extern std::mutex SsnLock;
extern enum Compilemode MODE;

void print_mode()
{
    if (MODE == Compilemode::RCL)
        cout << "this result is executed by RCL + SSN" << endl;
    else if (MODE == Compilemode::RCL_Saferetry)
        cout << "this result is executed by RCL + SSN + Robust Safe Retry" << endl;
    else if (MODE == Compilemode::RCL_ELR)
        cout << "this result is executed by RCL + SSN with ELR" << endl;
}

void Transaction::tbegin()
{
    this->txid_ = atomic_fetch_add(&timestampcounter, 1);
    ssn_tbegin();

    // ELR
    if (MODE == Compilemode::RCL_ELR && this->istargetTx)
        this->cstamp_ = atomic_fetch_add(&timestampcounter, 1);
}

void Transaction::tread(uint64_t key)
{
    if (searchWriteSet(key) == true || searchReadSet(key) == true)
        goto FINISH_TREAD;

    Tuple *tuple;
    tuple = get_tuple(key);

    if (!this->istargetTx)
    {
        Version *expected;
        for (;;)
        {
            expected = tuple->latest_.load(memory_order_acquire);
            if (!tuple->mmt_.r_try_lock())
            {
                // r-ronlylock deadlock prevention
                if (this->txid_ >= expected->cstamp_.load(memory_order_acquire))
                {
                    this->status_ = Status::aborted;
                    //++res_->local_rdeadlock_abort_counts_;
                    goto FINISH_TREAD;
                }
                // r-w deadlock prevention
                for (auto itr = write_set_.begin(); itr != write_set_.end(); itr++)
                {
                    Tuple *tmp = (*itr).tuple_;
                    if (tmp->rlocked.load() > 0)
                    {
                        this->status_ = Status::aborted;
                        //++res_->local_rdeadlock_abort_counts_;
                        goto FINISH_TREAD;
                    }
                }
            }
            else
                break;
        }
        //----------------------------------------------------------------
    }

    Version *ver;
    ver = tuple->latest_.load(memory_order_acquire);
    while (ver->status_.load(memory_order_acquire) != Status::committed)
    {
        ver = ver->prev_;
        //++res_->local_traversal_counts_;
    }

    ssn_tread(ver, key);

    if (!this->istargetTx)
        tuple->mmt_.r_unlock(); // short duration

    // ELR
    if (MODE == Compilemode::RCL_ELR && this->istargetTx)
    {
        tuple->rlocked.fetch_sub(1);
        tuple->mmt_.r_unlock();
    }

    if (this->status_ == Status::aborted)
    {
        //++res_->local_readphase_counts_;
        goto FINISH_TREAD;
    }
FINISH_TREAD:
    return;
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

    memcpy(desired->val_.data(), write_val.data(), DATA_SIZE); // FYI:desired->valにwrite_valをコピー

    for (;;)
    {
        expected = tuple->latest_.load(memory_order_acquire);
        for (auto itr = write_set_.begin(); itr != write_set_.end(); itr++)
        {
            // w-ronlylock deadlock prevention
            Tuple *tmp = (*itr).tuple_;
            if (tmp->rlocked.load() > 0)
            {
                this->status_ = Status::aborted;
                // w-ronlylock deadlock
                ++res_->local_rdeadlock_abort_counts_;
                goto FINISH_TWRITE;
            }
        }
        if (tuple->rlocked.load() > 0)
        {
            desired->locked_flag_ = true;
            // ronly lockが取られていて待っている状態
            ++res_->local_traversal_counts_;
            continue;
        }
        if (!tuple->mmt_.w_try_lock())
        {
            //  w-w deadlock prevention
            if (this->txid_ > expected->cstamp_.load(memory_order_acquire))
            {
                this->status_ = Status::aborted;
                // w-w deadlock counts
                ++res_->local_wdeadlock_abort_counts_;
                goto FINISH_TWRITE;
            }
            // ronly lockもなくてw-w deadlockもないけどlockが取れない
            //++res_->local_rdeadlock_abort_counts_;
        }
        else
            break;
    }
    //----------------------------------------------------------------
    desired->prev_ = expected;
    tuple->latest_ = desired;

    ssn_twrite(desired, key);

    if (this->status_ == Status::aborted)
        //++res_->local_writephase_counts_;

    FINISH_TWRITE:
        return;
}

void Transaction::commit()
{
    if (!(this->istargetTx && MODE == Compilemode::RCL_ELR))
        this->cstamp_ = atomic_fetch_add(&timestampcounter, 1);

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
    if (this->istargetTx)
    {
        if (MODE == Compilemode::RCL_Saferetry)
        {
            for (auto itr = task_set_sorted_.begin(); itr != task_set_sorted_.end(); itr++)
            {
                Tuple *tmptuple = get_tuple(*itr);
                tmptuple->rlocked.fetch_sub(1);
                tmptuple->mmt_.r_unlock();
            }
        }
        this->istargetTx = false;
        task_set_sorted_.clear();
    }

    read_set_.clear();
    write_set_.clear();
}

void Transaction::abort()
{
    ssn_abort();

    if (this->istargetTx)
        cout << "error abort" << endl;

    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        (*itr).ver_->status_.store(Status::aborted, memory_order_release);
        Tuple *tmp = (*itr).tuple_;
        tmp->mmt_.w_unlock();
    }
    write_set_.clear();
    read_set_.clear();

    // 提案手法: read only transactionのlock
    if (MODE != Compilemode::RCL && isreadonly() == true)
    {
        // sorting
        assert(task_set_sorted_.empty());
        for (int i = 0; i < task_set_.size(); i++)
            task_set_sorted_.push_back(task_set_.at(i).key_);
        std::sort(task_set_sorted_.begin(), task_set_sorted_.end());
        task_set_sorted_.erase(std::unique(task_set_sorted_.begin(), task_set_sorted_.end()), task_set_sorted_.end());

        // lock
        for (auto itr = task_set_sorted_.begin(); itr != task_set_sorted_.end(); itr++)
        {
            Tuple *tmptuple = get_tuple(*itr);
            tmptuple->rlocked.fetch_add(1);
            for (;;)
            {
                if (tmptuple->mmt_.r_try_lock())
                    break;
            }
        }
        this->istargetTx = true;
        // ELR
        if (MODE == Compilemode::RCL_ELR)
            this->ex_cstamp_ = this->cstamp_;
    }
}