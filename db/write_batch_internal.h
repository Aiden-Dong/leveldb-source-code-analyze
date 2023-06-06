// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

namespace leveldb {

class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  // 返回 writebatch 中有多少条数据
  static int Count(const WriteBatch* batch);

  // 设置这个writebatch 中的数据数量
  // 对应设置 writebatch 的第二个部分
  static void SetCount(WriteBatch* batch, int n);

  // 获取这个writebatch的 SeqNum
  // 对应 writebatch 的第一部分
  static SequenceNumber Sequence(const WriteBatch* batch);

  // 设置 writebatch 的 SeqNum
  // 对应设置 writebatch 的第一部分
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // 获取 writebatch 中的内容
  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  // 获取 writebatch 的长度
  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  // 重新设置 writebatch 里面的内容
  static void SetContents(WriteBatch* batch, const Slice& contents);

  // 遍历 WriteBatch 的内容，将数据落地到 memtable
  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  // 将 src 与 dst 中的内容合并到 dst
  static void Append(WriteBatch* dst, const WriteBatch* src);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
