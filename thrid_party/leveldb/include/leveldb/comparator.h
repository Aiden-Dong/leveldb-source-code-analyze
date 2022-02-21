// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_
#define STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_

#include <string>

#include "leveldb/export.h"

namespace leveldb {

class Slice;

/***
 * 
 * key 比较器接口
 * 
 **/
class LEVELDB_EXPORT  Comparator {
 public:
  virtual ~Comparator();

  /***
   * Three-way comparison.  Returns value:
   *   <  0, a < b
   *   == 0, a = b
   *   >  0, a > b
   */
  virtual int Compare(const Slice& a, const Slice& b) const = 0;

  // 比较器的名字, 以leveldb 开头
  virtual const char* Name() const = 0;

  // 高级功能：这些功能用于减少索引块等内部数据结构的空间需求。
  // 如果*start<limit，则在[start，limit]中将 *start 改为短字符串。
  // 简单的比较器实现可能会返回 *start 不变，也就是说，这种方法的实现不做任何事情是正确的。

  /**
   * 这些功能功能用于减少索引块等内部数据结构的空间需求
   * 如果 start < limit , 则将 start 更改为 [start, limit] 中较短的字符串
   * 简单的比较器实现可能会以 start 不变返回，即此方法的实现不执行任何操作也是正确
   */
  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const = 0;


  /**
   * 将 key 更改为 string >=
   */
  virtual void FindShortSuccessor(std::string* key) const = 0;
};

// Return a builtin comparator that uses lexicographic byte-wise
// ordering.  The result remains the property of this module and
// must not be deleted.
LEVELDB_EXPORT const Comparator* BytewiseComparator();

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_COMPARATOR_H_
