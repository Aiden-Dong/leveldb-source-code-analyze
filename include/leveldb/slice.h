// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Slice is a simple structure containing a pointer into some external
// storage and a size.  The user of a Slice must ensure that the slice
// is not used after the corresponding external storage has been
// deallocated.
//
// Multiple threads can invoke const methods on a Slice without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Slice must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_SLICE_H_
#define STORAGE_LEVELDB_INCLUDE_SLICE_H_

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

#include "leveldb/export.h"

namespace leveldb {

/**
 * leveldb 自定义的 string 类
 */
class LEVELDB_EXPORT Slice {
 public:
  // Create an empty slice.
  Slice() : data_(""), size_(0) {}

  // Create a slice that refers to d[0,n-1].
  Slice(const char* d, size_t n) : data_(d), size_(n) {}

  // string 转为 slice, 允许隐式转换
  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  // 字符串转 Slice, 允许隐式转换
  Slice(const char* s) : data_(s), size_(strlen(s)) {}

  // 拷贝构造函数, 默认生成
  Slice(const Slice&) = default;

  // 拷贝赋值函数, 采用默认生成
  Slice& operator=(const Slice&) = default;

  // 返回 char *
  const char* data() const { return data_; }

  // 返回字符大小
  size_t size() const { return size_; }

  // 判断 slice 是否为空
  bool empty() const { return size_ == 0; }

  // 返回 slice(char *) 第 n 个字符
  char operator[](size_t n) const {
    assert(n < size());
    return data_[n];
  }

  // 清空 slice
  void clear() {
    data_ = ""; // 指针变更， const 无关
    size_ = 0;
  }

  // Drop the first "n" bytes from this slice.
  //  删除前面 n 个字符
  void remove_prefix(size_t n) {
    assert(n <= size());
    data_ += n;
    size_ -= n;
  }

  // Slice -> std::string
  std::string ToString() const { return std::string(data_, size_); }

  // 字符串比较
  //   <  0 iff "*this" <  "b",
  //   == 0 iff "*this" == "b",
  //   >  0 iff "*this" >  "b"
  int compare(const Slice& b) const;

  // Return true iff "x" is a prefix of "*this"
  // 判断 string 是否是以 x 开始
  bool starts_with(const Slice& x) const {
    return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
  }

 private:
  // string体， 不可变类型
  const char* data_;
  // string 长度
  size_t size_;
};

inline bool operator==(const Slice& x, const Slice& y) {
  return ((x.size() == y.size()) &&
          (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

inline int Slice::compare(const Slice& b) const {
  const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
  int r = memcmp(data_, b.data_, min_len);
  if (r == 0) {
    if (size_ < b.size_)
      r = -1;
    else if (size_ > b.size_)
      r = +1;
  }
  return r;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_SLICE_H_
