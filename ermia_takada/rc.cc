#include "frame.hh"
#include <string.h>

using namespace std;

extern std::atomic<uint64_t> timestampcounter;
extern std::mutex SsnLock;
int USE_LOCK = 0;

void print_mode()
{
    if (USE_LOCK == 0)
        cout << "this result is executed by RC+SSN" << endl;
    else if (USE_LOCK == 1)
        cout << "this result is executed by RC + SSN + Repair" << endl;
}

void Transaction::tbegin()
{
    this->txid_ = atomic_fetch_add(&timestampcounter, 1);
    ssn_tbegin();
}

void Transaction::tread(uint64_t key)
{
    if (searchWriteSet(key) == true || searchReadSet(key) == true)
        goto FINISH_TREAD;

    Tuple *tuple;
    tuple = get_tuple(key);

    Version *ver;
    ver = tuple->latest_.load(memory_order_acquire);
    while (ver->status_.load(memory_order_acquire) != Status::committed)
    {
        ver = ver->prev_;
        //++res_->local_traversal_counts_;
    }

    ssn_tread(ver, key);

    if (this->status_ == Status::aborted)
    {
        //++res_->local_readphase_counts_;
        // this->isearlyaborted = true;
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

    memcpy(desired->val_.data(), write_val.data(), DATA_SIZE);

    for (;;)
    {
        expected = tuple->latest_.load(memory_order_acquire);
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
        }
        else
            break;
    }

    desired->prev_ = expected;
    tuple->latest_ = desired;

    ssn_twrite(desired, key);

    if (this->status_ == Status::aborted)
        //++res_->local_writephase_counts_;
        this->isearlyaborted = true;

FINISH_TWRITE:
    return;
}

void Transaction::repair_read()
{
    vector<Operation>::iterator itr = validated_read_set_.begin();
    while (itr != validated_read_set_.end())
    {
        if ((*itr).ver_->sstamp_.load(memory_order_acquire) != UINT32_MAX)
        {
            task_set_sorted_.emplace_back(Ope::READ, (*itr).key_);
            validated_read_set_.erase(itr);
        }
        else
            ++itr;
    }
}

void Transaction::commit()
{
    this->cstamp_ = atomic_fetch_add(&timestampcounter, 1);

    SsnLock.lock();

    if (istargetTx == true)
    {
        ssn_repair_commit();
    }
    else
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
    read_set_.clear();
    write_set_.clear();

    if (this->istargetTx == true && this->status_ == Status::committed)
    {
        istargetTx = false;
    }
    validated_read_set_.clear();
    isearlyaborted = false;
    task_set_sorted_.clear();
}

void Transaction::abort()
{
    ssn_abort();

    for (auto itr = write_set_.begin(); itr != write_set_.end(); ++itr)
    {
        (*itr).ver_->status_.store(Status::aborted, memory_order_release);
        Tuple *tmp = (*itr).tuple_;
        tmp->mmt_.w_unlock();
    }

    // 提案手法 transaction repair
    if (USE_LOCK == 1 && isreadonly() && isearlyaborted == false && !read_set_.empty())
    {
        this->ex_cstamp_ = this->cstamp_;
        this->istargetTx = true;
        //  1回目のabortの場合
        if (validated_read_set_.empty())
        {
            assert(task_set_sorted_.empty());
            assert(validated_read_set_.empty());

            for (auto itr = read_set_.begin(); itr != read_set_.end(); itr++)
            {
                if (this->pstamp_ < (*itr).ver_->sstamp_.load(memory_order_acquire))
                    validated_read_set_.push_back(*itr);
                else
                    task_set_sorted_.emplace_back(Ope::READ, (*itr).key_);
            }
            assert(validated_read_set_.size() + task_set_sorted_.size() == task_set_.size());
        }
        else
        {
            // 2回目以降のabortの場合
            assert(validated_read_set_.size() + read_set_.size() == task_set_.size());
            task_set_sorted_.clear();
            for (auto itr = read_set_.begin(); itr != read_set_.end(); itr++)
            {
                if (this->pstamp_ < (*itr).ver_->sstamp_)
                    validated_read_set_.push_back(*itr);
                else
                    task_set_sorted_.emplace_back(Ope::READ, (*itr).key_);
            }
            assert(validated_read_set_.size() + task_set_sorted_.size() == task_set_.size());
        }
    }
    write_set_.clear();
    read_set_.clear();
    this->isearlyaborted = false;
}