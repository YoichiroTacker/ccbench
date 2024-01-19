#include "frame.hh"

extern Tuple *Table;
extern int USE_LOCK;
extern TransactionTable *TMT[thread_num];

using namespace std;

uint32_t leftshift(uint32_t tsmp, bool tidflag)
{
    if (tidflag == 0)
        tsmp = (tsmp << 1) | 0;
    else
        tsmp = (tsmp << 1) | 1;
    return tsmp;
}

uint32_t rightshift(uint32_t tsmp)
{
    tsmp = tsmp >> 1;
    return tsmp;
}

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

    if (ver->sstamp_.load(memory_order_acquire) == (UINT32_MAX & ~(TIDFLAG)))
        read_set_.emplace_back(key, ver, ver->val_);
    else
    {
        this->sstamp_ = min(this->sstamp_, rightshift(ver->sstamp_.load(memory_order_acquire)));
        read_set_.emplace_back(key, ver);
    }
    upReadersBits(ver, this->thid_);
    verify_exclusion_or_abort();

    // ELR
    /*if (this->istargetTx == true && USE_LOCK == 2)
        ver->pstamp_.store(max(this->ex_cstamp_, ver->pstamp_.load(memory_order_acquire)), memory_order_release);*/
}

void Transaction::ssn_twrite(Version *desired, uint64_t key)
{
    // Insert my tid for ver->prev_->sstamp_
    desired->prev_->sstamp_.store(leftshift(this->thid_, 1), memory_order_release); // thidでTMTにアクセスしたい

    /*if (desired->locked_flag_ && USE_LOCK == 1)
        this->pstamp_ = max(this->pstamp_, desired->prev_->pstamp_for_rlock_.load(memory_order_acquire));
    else*/
    this->pstamp_ = max(this->pstamp_, desired->prev_->pstamp_.load(memory_order_acquire));

    write_set_.emplace_back(key, desired, &Table[key]);

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
    {
        if ((*itr).key_ == key)
        {
            downReadersBits((*itr).ver_, this->thid_);
            read_set_.erase(itr);
            break;
        }
    }
    verify_exclusion_or_abort();
}

void Transaction::ssn_commit()
{
    vector<Operation>::iterator itr = validated_read_set_.begin();
    while (itr != validated_read_set_.end())
    {
        if ((*itr).ver_->sstamp_.load(memory_order_acquire) != (UINT32_MAX & ~(TIDFLAG)))
        {
            this->status_ = Status::aborted;
            read_set_.push_back(*itr);
            validated_read_set_.erase(itr);
        }
        else
            ++itr;
    }

    if (this->status_ == Status::aborted)
        return;

    this->sstamp_ = min(this->sstamp_, this->cstamp_);

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
    {
        if ((*itr).ver_->sstamp_.load(memory_order_acquire) != (UINT32_MAX & ~(TIDFLAG)))
            this->sstamp_ = min(this->sstamp_, rightshift((*itr).ver_->sstamp_.load(memory_order_acquire)));
    }
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
        this->pstamp_ = max(this->pstamp_, (*itr).ver_->prev_->pstamp_.load(memory_order_acquire));

    if (pstamp_ < sstamp_)
        this->status_ = Status::committed;
    else
    {
        status_ = Status::aborted;
        //++res_->local_commitphase_counts_;
        return;
    }

    for (auto itr = validated_read_set_.begin(); itr != validated_read_set_.end(); ++itr)
        (*itr).ver_->pstamp_.store(max((*itr).ver_->pstamp_.load(memory_order_acquire), this->cstamp_), memory_order_release);

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
    {
        (*itr).ver_->pstamp_.store((max((*itr).ver_->pstamp_.load(memory_order_acquire), this->cstamp_)), memory_order_release);
        downReadersBits((*itr).ver_, this->thid_);
    }

    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        (*itr).ver_->prev_->sstamp_.store(leftshift(this->sstamp_, 0), memory_order_release);
        (*itr).ver_->pstamp_.store(this->cstamp_, memory_order_release);
        (*itr).ver_->cstamp_.store(this->cstamp_, memory_order_release);
    }
}

void Transaction::ssn_parallel_commit()
{
    TransactionTable *tmt;
    vector<Operation>::iterator itr = validated_read_set_.begin();
    while (itr != validated_read_set_.end())
    {
        if ((*itr).ver_->sstamp_.load(memory_order_acquire) != (UINT32_MAX & ~(TIDFLAG)))
        {
            this->status_ = Status::aborted;
            read_set_.push_back(*itr);
            validated_read_set_.erase(itr);
        }
        else
            ++itr;
    }

    if (this->status_ == Status::aborted)
        return;

    // finalize pi(T)
    this->sstamp_ = min(this->sstamp_, this->cstamp_);

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
    {
        uint32_t v_sstamp = (*itr).ver_->sstamp_.load(memory_order_acquire);

        if ((v_sstamp & TIDFLAG)) // 最下位bitが1==未コミットのwriteが同時にcommitしている
        {
            tmt = __atomic_load_n(&TMT[rightshift(v_sstamp)], __ATOMIC_ACQUIRE);
            if (tmt->status_.load(memory_order_acquire) == Status::committing)
            {
                while (tmt->cstamp_.load(memory_order_acquire) == 0)
                    ;
                if (tmt->cstamp_.load(memory_order_acquire) < this->cstamp_)
                {
                    while (tmt->status_.load(memory_order_acquire) == Status::committing)
                        ;
                    if (tmt->status_.load(memory_order_acquire) == Status::committed)
                        this->sstamp_ = min(this->sstamp_, tmt->sstamp_.load(memory_order_acquire));
                }
            }
        }
        else if (v_sstamp != (UINT32_MAX & ~(TIDFLAG)))
            this->sstamp_ = min(this->sstamp_, rightshift(v_sstamp));
    }

    //   finalize eta(T)
    uint64_t one = 1;
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        Version *ver = (*itr).ver_;
        while (ver->status_.load(memory_order_acquire) != Status::committed)
            ver = ver->prev_;
        uint64_t rdrs = ver->readers_.load(memory_order_acquire);
        for (int worker = 0; worker < thread_num; ++worker)
        {
            if ((rdrs & (one << worker)) ? 1 : 0) // 各threadに対応するreaders bitsが立っていたら
            {
                tmt = __atomic_load_n(&TMT[worker], __ATOMIC_ACQUIRE);
                if (tmt->status_.load(memory_order_acquire) == Status::committing)
                {
                    while (tmt->cstamp_.load(memory_order_acquire) == 0)
                        ;
                    if (tmt->cstamp_.load(memory_order_acquire) < this->cstamp_)
                    {
                        while (tmt->status_.load(memory_order_acquire) == Status::committing)
                            ;
                        if (tmt->status_.load(memory_order_acquire) == Status::committed)
                            this->pstamp_ = max(this->pstamp_, tmt->cstamp_.load(memory_order_acquire));
                    }
                }
            }
        }
        this->pstamp_ = max(this->pstamp_, ver->pstamp_.load(memory_order_acquire));
    }

    tmt = TMT[thid_];
    if (pstamp_ < sstamp_)
    {
        status_ = Status::committed;
        tmt->sstamp_.store(this->sstamp_, memory_order_release);
        tmt->status_.store(Status::committed, memory_order_release);
    }
    else
    {
        status_ = Status::aborted;
        return;
    }

    for (auto itr = validated_read_set_.begin(); itr != validated_read_set_.end(); ++itr)
    {
        uint32_t pstmp = (*itr).ver_->pstamp_.load(memory_order_acquire);
        while (pstamp_ < this->cstamp_)
        {
            uint32_t tmp = (*itr).ver_->pstamp_.load(memory_order_acquire);
            if (__atomic_compare_exchange_n(&tmp, &pstmp, this->cstamp_, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
                break;
            pstmp = (*itr).ver_->pstamp_.load(memory_order_acquire);
        }
        downReadersBits((*itr).ver_, this->thid_);
    }

    // update eta
    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
    {
        uint32_t pstmp = (*itr).ver_->pstamp_.load(memory_order_acquire);
        while (pstamp_ < this->cstamp_)
        {
            uint32_t tmp = (*itr).ver_->pstamp_.load(memory_order_acquire);
            if (__atomic_compare_exchange_n(&tmp, &pstmp, this->cstamp_, false, __ATOMIC_ACQ_REL, __ATOMIC_RELAXED))
                break;
            pstmp = (*itr).ver_->pstamp_.load(memory_order_acquire);
        }
        downReadersBits((*itr).ver_, this->thid_);
    }

    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        (*itr).ver_->prev_->sstamp_.store(leftshift(this->sstamp_, 0), memory_order_release);
        (*itr).ver_->cstamp_.store(this->cstamp_, memory_order_release);
        (*itr).ver_->pstamp_.store(this->cstamp_, memory_order_release);
    }
    // TMT[thid_]->ex_cstamp_.store(cstamp_, memory_order_release);
}

void Transaction::ssn_abort()
{
    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        Version *next_committed = (*itr).ver_->prev_;
        while (next_committed->status_.load(memory_order_acquire) != Status::committed)
            next_committed = next_committed->prev_;
        next_committed->sstamp_.store(UINT32_MAX & ~(TIDFLAG), memory_order_release);
    }

    for (auto itr = validated_read_set_.begin(); itr != validated_read_set_.end(); ++itr)
        downReadersBits((*itr).ver_, this->thid_);

    for (auto itr = read_set_.begin(); itr != read_set_.end(); ++itr)
        downReadersBits((*itr).ver_, this->thid_);
}

void Transaction::verify_exclusion_or_abort()
{
    if (this->pstamp_ >= this->sstamp_)
    {
        this->status_ = Status::aborted;
    }
}

void upReadersBits(Version *ver, uint8_t thid_)
{
    uint64_t expected, desired;
    expected = ver->readers_.load(memory_order_acquire);
    for (;;)
    {
        desired = expected | (1 << thid_);
        if (ver->readers_.compare_exchange_weak(
                expected, desired, memory_order_acq_rel, memory_order_acquire))
            break;
    }
}

void downReadersBits(Version *ver, uint8_t thid_)
{
    uint64_t expected, desired;
    expected = ver->readers_.load(memory_order_acquire);
    for (;;)
    {
        desired = expected & ~(1 << thid_);
        if (ver->readers_.compare_exchange_weak(
                expected, desired, memory_order_acq_rel, memory_order_acquire))
            break;
    }
}