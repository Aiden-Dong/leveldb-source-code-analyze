// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/comparator.h"

#include <algorithm>
#include <cstdint>
#include <string>
#include <type_traits>

#include "leveldb/slice.h"
#include "util/logging.h"
#include "util/no_destructor.h"

namespace leveldb {

Comparator::~Comparator() = default;

namespace {
class BytewiseComparatorImpl : public Comparator {
 public:
  BytewiseComparatorImpl() = default;

  const char* Name() const override { return "leveldb.BytewiseComparator"; }

  int Compare(const Slice& a, const Slice& b) const override {
    return a.compare(b);
  }

  void FindShortestSeparator(std::string* start, const Slice& limit) const override {
    // Find length of common prefix
    size_t min_length = std::min(start->size(), limit.size());
    size_t diff_index = 0;

    // 去除掉两者公共部分
    while ((diff_index < min_length) &&
           ((*start)[diff_index] == limit[diff_index])) {
      diff_index++;
    }

    if (diff_index >= min_length) {
      // 表示 start 与 limit 是包含关系（字符包含）
    } else {
      // 标识 start 与 limit 不是包含关系， 有却别
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);  //获得 start 的第一个区别位

      if (diff_byte < static_cast<uint8_t>(0xff) &&  // 可能相等
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) { // start[diff_index] +1 < limit[diff_index])
        (*start)[diff_index]++;
        start->resize(diff_index + 1); // 截断字符串到能比较两者不同的最短字符串
        assert(Compare(*start, limit) < 0);
      }
    }
  }

  /***
   * 用于找到比key大的最短字符串
   * 如传入“helloworld”，返回的key可能是“i”而已。
   */
  void FindShortSuccessor(std::string* key) const override {
    // Find first character that can be incremented
    size_t n = key->size();
    for (size_t i = 0; i < n; i++) {
      const uint8_t byte = (*key)[i];
      if (byte != static_cast<uint8_t>(0xff)) {
        (*key)[i] = byte + 1;
        key->resize(i + 1);
        return;
      }
    }
    // *key is a run of 0xffs.  Leave it alone.
  }
};
}  // namespace

const Comparator* BytewiseComparator() {
  static NoDestructor<BytewiseComparatorImpl> singleton;
  return singleton.get();
}

}  // namespace leveldb
