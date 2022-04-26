// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An iterator yields a sequence of key/value pairs from a source.
// The following class defines the interface.  Multiple implementations
// are provided by this library.  In particular, iterators are provided
// to access the contents of a Table or a DB.
//
// Multiple threads can invoke const methods on an Iterator without
// external synchronization, but if any of the threads may call a
// non-const method, all threads accessing the same Iterator must use
// external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
#define STORAGE_LEVELDB_INCLUDE_ITERATOR_H_

#include "leveldb/export.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

/****
 * 迭代器工具
 *
 * 派生类 :
 *       - LevelFileNumIterator  SST 文件遍历器
 *       - TwoLevelIterator      SST 遍历器
 *       - Iter                  Block 遍历器
 *       - MergingIterator       合并SST迭代器
 */
class LEVELDB_EXPORT Iterator {
 public:
  Iterator();

  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  virtual ~Iterator();

  /***
   * 当前是否有有效的key
   */
  virtual bool Valid() const = 0;

  /***
   * 移动游标到最开始位置
   * key_value 指向最开始的 kv
   */
  virtual void SeekToFirst() = 0;

  /***
   * 移动游标到最后位置
   * key_value 指向最后的 kv
   */
  virtual void SeekToLast() = 0;


  /***
   * 查找第一个key >= target 的数据
   */
  virtual void Seek(const Slice& target) = 0;

  /***
   * 找到下一个key,value目标
   */
  virtual void Next() = 0;

  /***
   * 找到上一个 key, value 目标
   */
  virtual void Prev() = 0;

  /***
   * 找到当前游标位置的 key
   */
  virtual Slice key() const = 0;

  /***
   * 找到当前游标位置的 value
   */
  virtual Slice value() const = 0;

  /***
   * 当前迭代器的操作状态
   */
  virtual Status status() const = 0;

  /***
   * 当前迭代器的清理函数
   */
  using CleanupFunction = void (*)(void* arg1, void* arg2);
  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);

 private:
  // Cleanup functions are stored in a single-linked list.
  // The list's head node is inlined in the iterator.
  struct CleanupNode {
    // True if the node is not used. Only head nodes might be unused.
    bool IsEmpty() const { return function == nullptr; }
    // Invokes the cleanup function.
    void Run() {
      assert(function != nullptr);
      (*function)(arg1, arg2);
    }

    // The head node is used if the function pointer is not null.
    CleanupFunction function;
    void* arg1;
    void* arg2;
    CleanupNode* next;
  };
  CleanupNode cleanup_head_;
};

// Return an empty iterator (yields nothing).
LEVELDB_EXPORT Iterator* NewEmptyIterator();

// Return an empty iterator with the specified status.
LEVELDB_EXPORT Iterator* NewErrorIterator(const Status& status);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_ITERATOR_H_
