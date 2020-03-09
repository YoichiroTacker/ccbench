# CCBench : Redesign and Implement Many Concurrency Control

---

## Installing a binary distribution package
On Debian/Ubuntu Linux, execute below statement or bootstrap_apt.sh.
```
$ sudo apt update -y
$ sudo apt-get install -y libgflags-dev libgoogle-glog-dev cmake cmake-curses-gui libboost-filesystem-dev
```

## Prepare using
```
$ git clone this_repository
$ cd ccbench
$ source bootstrap.sh
```
Processing of bootstrap.sh :  
git submodule init, update. <br>
Build third_party/masstree, third_party/mimalloc.<br>
Export LD_LIBRARY_PATH to ./third_party/mimalloc/out/release for using mimalloc library.<br>
<br>
So it's script should be executed by "source" command.<br>
I recommend you that you also add LD_LIBRARY_PATH to your ~/.bashrc by yourself.
<br>
Each protocols has own Makefile, so you should build each.
<br>
Prepare gflags for command line options.
```
$ cd third_party/gflags
$ mkdir build
$ cd build
$ ccmake ..
  - Press 'c' to configure the build system and 'e' to ignore warnings.
  - Set CMAKE_INSTALL_PREFIX and other CMake variables and options.
  - Continue pressing 'c' until the option 'g' is available.
  - Then press 'g' to generate the configuration files for GNU Make.
$ make -j && make install
```
Prepare glog for command line options.
```
./autogen.sh && ./configure && make -j && make install
```
---

## Silo
It was proposed at SOSP'2013 by Stephen Tu.
- modify
  - reduce roop in concurrency control protocol
  - backoff (easy to use from ccbench/include/backoff.hh by a few restriction)

---

## ERMIA (SI + SSN)
ERMIA was proposed at SIGMOD'2016 by Kangnyeon Kim.  
SSN is serialization certifier which has to be executed serial.  
Latch-free SSN was proposed at VLDB'2017 by Tianzheng Wang.  

### Implemented
- original
  - SSN  
  - Latch-free SSN  
    - Leveraging existing infrastructure
- modify
  - Garbage Collection for Transaction Mapping Table
  - rapid garbage collection from Cicada's paper.
  - backoff (easy to use from ccbench/include/backoff.hh by a few restriction)

---

## TicToc
TicToc was proposed at SIGMOD'2016 by Xiangyao Yu.  

### Implemented
- original
  - all opt.
    - No-Wait Locking in Validation Phase
    - Preemptive Aborts
    - Timestamp History
- modify
  - reduce roop in concurrency control protocol
  - backoff (easy to use from ccbench/include/backoff.hh by a few restriction)

---

## MOCC
MOCC was proposed at VLDB'2017 by Tianzheng Wang.  

### Implemented
- modify
  - new temprature protocol reduces contentions and improves throughput much.
  - backoff (easy to use from ccbench/include/backoff.hh by a few restriction)

---

## Instruction Survey
* fetch\_add
* xoroshiro128+
* memory benchmark
* masstree unit test
---

## Data Structure
### Masstree
This is a submodule.  
usage:  
`git submodule init`  
`git submodule update`  
tanabe's wrapper is include/masstree\_wrapper.hpp

---

## Experimental data
https://github.com/thawk105/ccdata 

---
This activity was supported by Cybozu Labs Youth 8th term. (2018/4/10 - 2019/4/10)
