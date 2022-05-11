// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch holds a collection of updates to apply atomically to a DB.
//
// The updates are applied in the order in which they are added
// to the WriteBatch.  For example, the value of "key" will be "v3"
// after the following batch is written:
//
//    batch.Put("key", "v1");
//    batch.Delete("key");
//    batch.Put("key", "v2");
//    batch.Put("key", "v3");
//
// Multiple threads can invoke const methods on a WriteBatch without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same WriteBatch must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
#define STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_

#include <string>

#include "leveldb/export.h"
#include "leveldb/status.h"

namespace leveldb {

class Slice;

/****
 * 基本数据结构 :
 *  | SeqNum(8字节) | Key个数(4字节) |
 *  | kTypeValue(1字节)    | key 长度(变长32位) | key 具体数据 | value 长度(变长32位) | value 具体数据 |
 *  ...
 *  | kTypeDeletion(1字节) | key 长度(变长32位) | key 具体数据 |
 */
class LEVELDB_EXPORT WriteBatch {

 public:
  class LEVELDB_EXPORT Handler {
   public:
    virtual ~Handler();
    virtual void Put(const Slice& key, const Slice& value) = 0;
    virtual void Delete(const Slice& key) = 0;
  };

  WriteBatch();

  WriteBatch(const WriteBatch&) = default;
  WriteBatch& operator=(const WriteBatch&) = default;

  ~WriteBatch();


  /***
   * 接受用户提交的 key, value 写入系统
   * @param key    user_key
   * @param value  value
   */
  void Put(const Slice& key, const Slice& value);

  // 以追加写的方式删除 key
  void Delete(const Slice& key);

  // 清理这个 batch 的所有内容
  void Clear();

  // 获取数据的内存占用大小
  size_t ApproximateSize() const;

  // 将 source 的内容全量追加到 this 中
  void Append(const WriteBatch& source);

  // 遍历所有的keyValue, 并交给 handler 填充
  // MemTableInserter 子类 对应的 memtable 操作
  Status Iterate(Handler* handler) const;

 private:
  friend class WriteBatchInternal; // 内部工具性质辅助类

  // writebatch 具体数据
  std::string rep_;  // See comment in write_batch.cc for the format of rep_
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_WRITE_BATCH_H_
