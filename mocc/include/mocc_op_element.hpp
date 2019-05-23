#pragma once

#include <iostream>

#include "../../include/op_element.hpp"

using std::cout;
using std::endl;

template <typename T>
class ReadElement : public Op_element<T> {
public:
  using Op_element<T>::Op_element;

  Tidword tidword;
  char val[VAL_SIZE];
  bool failedVerification;

  ReadElement(Tidword tidword, uint64_t key, T* rcdptr, char *newVal) : Op_element<T>::Op_element(key, rcdptr) {
    this->tidword = tidword;
    memcpy(val, newVal, VAL_SIZE);
    this->failedVerification = false;
  }

  ReadElement(uint64_t key, T* rcdptr) : Op_element<T>::Op_element(key, rcdptr) {
    failedVerification = true;
  }

  bool operator<(const ReadElement& right) const {
    return this->key < right.key;
  }
};

template <typename T>
class WriteElement : public Op_element<T> {
public:
  using Op_element<T>::Op_element;

  bool operator<(const WriteElement& right) const {
    return this->key < right.key;
  }
};

