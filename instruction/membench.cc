
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>

#include <atomic>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

#include "../include/atomic_wrapper.hpp"
#include "../include/cache_line_size.hpp"
#include "../include/cpu.hpp"
#include "../include/debug.hpp"
#include "../include/random.hpp"
#include "../include/tsc.hpp"
#include "../include/util.hpp"

#define GiB 1073741824
#define MiB 1048576
#define KiB  1024

using std::cout;
using std::endl;

const uint64_t WORK_MEM_PER_THREAD = 256UL << 20; // 256 MBytes.
const size_t PAGE_SIZE = 4096;

void
sleepMs(size_t ms)
{
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

class AlignedMemory
{
  void *mem_;
  size_t size_;
public:
  AlignedMemory(size_t alignedSize, size_t allocateSize) {
    size_ = allocateSize;
    if (::posix_memalign(&mem_, alignedSize, allocateSize) != 0) {
      throw std::runtime_error("posix_memalign failed.");
    }
  }
  ~AlignedMemory() noexcept {
    ::free(mem_);
  }
  char* data() { return (char*)mem_; }
  const char* data() const { return (const char*)mem_; }
  size_t size() const { return size_;}
};

void
fillArray(void* data, size_t size)
{
  assert(size % CACHE_LINE_SIZE == 0);
  char *p = (char *)data;
  char *goal = p + size;
  alignas(CACHE_LINE_SIZE) const uint64_t buf[8] = {1, 0, 0, 0, 0, 0, 0, 0};
  while (p < goal) {
    memcpy(p, &buf[0], sizeof(buf));
    p += sizeof(buf);
  }
}

void
writeFromCacheline(void* dst, const void* src, size_t size)
{
  char *p = (char *)dst;
  while (size > CACHE_LINE_SIZE) {
    ::memcpy(p, src, CACHE_LINE_SIZE);
    p += CACHE_LINE_SIZE;
    size -= CACHE_LINE_SIZE;
  }
  ::memcpy(p, src, size);
}

bool
isReady(const std::vector<char>& readys)
{
  for (const char& b : readys) {
    if (!loadAcquire(b)) return false;
  }
  return true;
}

void
waitForReady(const std::vector<char>& readys)
{
  while (!isReady(readys)) {
    _mm_pause();
  }
}

struct Config
{
  size_t nr_threads;
  size_t run_sec;
  const char *workload;
  size_t bulk_size;
};

enum class WorkloadType {
  Unknown,
  SeqRead,
  SeqWrite,
  RndRead,
  RndWrite,
};

WorkloadType
getWorkloadType(const char *name)
{
  struct {
    WorkloadType type;
    const char *name;
  } tbl[] = {
    {WorkloadType::Unknown, "unknown"},
    {WorkloadType::SeqRead, "seq_read"},
    {WorkloadType::SeqWrite, "seq_write"},
    {WorkloadType::RndRead, "rnd_read"},
    {WorkloadType::RndWrite, "rnd_write"},
  };

  for (size_t i = 0; i < sizeof(tbl) / sizeof(tbl[0]); ++i) {
    if (::strncmp(tbl[i].name, name, 20) == 0) {
      return tbl[i].type;
    }
  }
  return WorkloadType::Unknown;
}

void seqReadWorker(size_t idx, size_t bulk_size, char& ready, const bool& start, const bool& quit, uint64_t& count) try
{
  setThreadAffinity(idx);

  const uint64_t size = WORK_MEM_PER_THREAD;
  assert(size % bulk_size == 0);
  
  AlignedMemory mem(PAGE_SIZE, size);
  fillArray(mem.data(), mem.size());

  uint64_t transferred = 0;
  AlignedMemory buf(PAGE_SIZE, bulk_size);
  uint64_t buf2 = 0;
  const char *p = (const char *)mem.data();
  const char *goal = p + size;

  storeRelease(ready, 1);
  while (!loadAcquire(start)) _mm_pause();

  while (!loadAcquire(quit)) {
    ::memcpy(buf.data(), p, bulk_size);
    transferred += bulk_size;

    p += bulk_size;
    if (p >= goal) p = (const char *)mem.data();
  }

  storeRelease(count, transferred);
} catch (std::exception& e) {
  ::fprintf(::stderr, "seqReadWorker error: %s\n", e.what());
}

void
runExpr(const Config& cfg)
{
  bool start = false;
  bool quit = false;
  std::vector<char> readys(cfg.nr_threads);
  std::vector<uint64_t> counts(cfg.nr_threads);
  std::vector<std::thread> thv;
  for (size_t i = 0; i < cfg.nr_threads; ++i) {
    switch (getWorkloadType(cfg.workload)) {
      case WorkloadType::SeqRead:
        thv.emplace_back(seqReadWorker, i, cfg.bulk_size, std::ref(readys[i]), std::ref(start), std::ref(quit), std::ref(counts[i]));
        break;
      default:
        throw std::runtime_error("unknown worklaod");
    }
  }
  waitForReady(readys);
  storeRelease(start, true);
  for (size_t i = 0; i < cfg.run_sec; ++i) {
    sleepMs(1000);
  }
  storeRelease(quit, true);
  for (auto& th : thv) th.join();

  uint64_t total = 0;
  for (uint64_t c : counts) {
    total += c;
  }

  const double latency_ns = (1000000000.0 * cfg.nr_threads * cfg.run_sec * cfg.bulk_size) / (double)(total);
  ::printf("nr_threads %zu run_sec %zu workload %-10s bulk_size %4zu Bps %15" PRIu64 " ops %15" PRIu64 " latency_ns %5.3f\n"
      , cfg.nr_threads, cfg.run_sec, cfg.workload, cfg.bulk_size
      , total / cfg.run_sec, total / cfg.bulk_size / cfg.run_sec, latency_ns);
  ::fflush(::stdout);
}

void
put8(const uint64_t* p)
{
  for (size_t i = 0; i < 24; ++i) {
    ::printf("%" PRIu64 "", p[i]);
  }
  ::printf("\n");
}

int
main(int argc, char *argv[]) try
{
  if (argc != 2) ERR;
  size_t nr_threads = atoi(argv[1]);

  size_t run_sec = 3;
  size_t nr_loop = 3;
  
  //for (const char *workload : {"seq_read", "seq_write", "rnd_read", "rnd_write"}) {
  for (const char *workload : {"seq_read"}) {
    for (size_t bulk_size : {8, 64, 1024, 4096}) {
      assert(bulk_size % sizeof(uint64_t) == 0);
      for (size_t i = 0; i < nr_loop; ++i) {
        runExpr({nr_threads, run_sec, workload, bulk_size});
      }
    }
  }
} catch (std::exception& e) {
  ::fprintf(::stderr, "main error: %s\n", e.what());
  ::exit(1);
}
