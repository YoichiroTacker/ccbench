#pragma once

#include <pthread.h>
#include <string.h>

#include <atomic>
#include <cstdint>

#include "../../include/cache_line_size.hpp"

struct Tidword {
  union {
    uint64_t obj;
    struct {
      bool lock:1;
      bool latest:1;
      bool absent:1;
      uint64_t tid:29;
      uint64_t epoch:32;
    };
  };

  Tidword() : obj(0) {};

  bool operator==(const Tidword& right) const {
    return obj == right.obj;
  }

  bool operator!=(const Tidword& right) const {
    return !operator==(right);
  }

  bool operator<(const Tidword& right) const {
    return this->obj < right.obj;
  }
};

class Tuple {
public:
  Tidword tidword;

  char keypad[KEY_SIZE];
  char val[VAL_SIZE];

#ifndef SHAVE_REC
  int8_t pad[CACHE_LINE_SIZE - ((8 + KEY_SIZE + VAL_SIZE) % CACHE_LINE_SIZE)];
#endif

};

