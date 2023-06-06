// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_MEMTABLE_H_

#include <string>

#include "db/dbformat.h"
#include "db/skiplist.h"
#include "leveldb/db.h"
#include "util/arena.h"

namespace leveldb {

class InternalKeyComparator;
class MemTableIterator;

/***
 * 数据内存缓冲区，内部使用skiplist实现
 */
class MemTable {
 public:
  // 构造器
  explicit MemTable(const InternalKeyComparator& comparator);

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  // 增加一个外部引用
  void Ref() { ++refs_; }

  // 释放引用
  // 如果外部无引用， 则释放存储
  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  // 返回容量大小
  size_t ApproximateMemoryUsage();

  // 返回迭代器
  Iterator* NewIterator();

  /***
   * 添加一个 kv 数据到 memtable 中
   * @param seq     seqnum 唯一标识这个kv的序列号
   * @param type    存储类型 kTypeDeletion(删除操作),  kTypeValue(插入操作)
   * @param key     数据实体 key
   * @param value   数据实体 value
   */
  void Add(SequenceNumber seq, ValueType type, const Slice& key, const Slice& value);

  // 从MemTable中查询数据
  bool Get(const LookupKey& key, std::string* value, Status* s);

 private:
  friend class MemTableIterator;
  friend class MemTableBackwardIterator;

  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& c) : comparator(c) {}
    int operator()(const char* a, const char* b) const;
  };

  typedef SkipList<const char*, KeyComparator> Table;   // 数据体， 使用跳表维护

  ~MemTable();  // Private since only Unref() should be used to delete it

  KeyComparator comparator_;   // key 比较器
  int refs_;
  Arena arena_;                // 内存池工具

  // 跳表结构实现
  Table table_;
};

}

#endif  // STORAGE_LEVELDB_DB_MEMTABLE_H_
