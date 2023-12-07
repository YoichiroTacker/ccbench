#include "frame.hh"

extern Tuple *Table;
extern int USE_LOCK;

using namespace std;

void Transaction::ssn_tbegin()
{
    this->cstamp_ = 0;
    pstamp_ = 0;
    sstamp_ = UINT32_MAX;
    status_ = Status::inFlight;
}

void Transaction::ssn_tread(Version *ver, uint64_t key)
{
    this->pstamp_ = max(this->pstamp_, ver->cstamp_.load(memory_order_acquire));

    if (ver->sstamp_.load(memory_order_acquire) == (UINT32_MAX))
        read_set_.emplace_back(key, ver, ver->val_);
    else
        this->sstamp_ = min(this->sstamp_, ver->sstamp_.load(memory_order_acquire));
    verify_exclusion_or_abort();

    // ELR
    if (this->istargetTx == true && USE_LOCK == 2)
        ver->pstamp_.store(max(this->cstamp_aborted, ver->pstamp_.load(memory_order_acquire)), memory_order_release);
}

void Transaction::ssn_twrite(Version *desired, uint64_t key)
{
    // Insert my tid for ver->prev_->sstamp_
    desired->prev_->pstamp_.store(this->txid_, memory_order_release);
    if (desired->locked_flag_ && USE_LOCK == 1)
        this->pstamp_ = max(this->pstamp_, desired->prev_->pstamp_for_rlock_.load(memory_order_acquire));
    else
        this->pstamp_ = max(this->pstamp_, desired->prev_->pstamp_.load(memory_order_acquire));

    write_set_.emplace_back(key, desired, &Table[key]);

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
    {
        if ((*itr).key_ == key)
        {
            read_set_.erase(itr);
            break;
        }
    }
    verify_exclusion_or_abort();
}

void Transaction::ssn_commit()
{
    this->sstamp_ = min(this->sstamp_, this->cstamp_);
    // if using RCL
    assert(this->sstamp_ == this->cstamp_);

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
        this->sstamp_ = min(this->sstamp_, (*itr).ver_->sstamp_.load(memory_order_acquire));

    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        if ((*itr).ver_->locked_flag_ && USE_LOCK == 1)
            this->pstamp_ = max(this->pstamp_, (*itr).ver_->prev_->pstamp_for_rlock_.load(memory_order_acquire));
        else
            this->pstamp_ = max(this->pstamp_, (*itr).ver_->prev_->pstamp_.load(memory_order_acquire));
    }

    if (pstamp_ < sstamp_)
        this->status_ = Status::committed;
    else
    {
        status_ = Status::aborted;
        ++res_->local_commitphase_counts_;
        return;
    }

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
    {
        (*itr).ver_->pstamp_.store((max((*itr).ver_->pstamp_.load(memory_order_acquire), this->cstamp_)), memory_order_release);
        // extention of forced forward edge
        if (USE_LOCK == 1 && this->istargetTx)
            (*itr).ver_->pstamp_for_rlock_.store(this->pstamp_);
    }

    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        (*itr).ver_->prev_->sstamp_.store(this->sstamp_, memory_order_release);
        (*itr).ver_->pstamp_.store(this->cstamp_, memory_order_release);
    }
}

void Transaction::ssn_abort()
{
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        Version *next_committed = (*itr).ver_->prev_;
        while (next_committed->status_.load(memory_order_acquire) != Status::committed)
            next_committed = next_committed->prev_;
        next_committed->sstamp_.store(UINT32_MAX, memory_order_release);
    }
}

void Transaction::verify_exclusion_or_abort()
{
    if (this->pstamp_ >= this->sstamp_)
    {
        this->status_ = Status::aborted;
    }
}