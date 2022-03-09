// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
#define STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_

#include "leveldb/iterator.h"

namespace leveldb {

struct ReadOptions;

/***
 * 返回一个新的两级迭代器。
 * 两级迭代器包含一个索引迭代器，其值指向一个块序列，其中每个块本身就是一个键、值对序列。
 * 返回的两级迭代器产生块序列中所有键/值对的串联。
 * 获得“索引”的所有权，并在不再需要时将其删除。
 *
 * 使用提供的函数将索引值转换为对应块内容的迭代器。
 *
 * @param index_iter       index 迭代器
 * @param block_function   读取 block 的回调函数
 * @param arg              block_function 参数
 * @param options          block_function 参数
 *
 * @return                 TwoLevelIterator
 */
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_function)(void* arg, const ReadOptions& options, const Slice& index_value),
    void* arg,
    const ReadOptions& options);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_TWO_LEVEL_ITERATOR_H_
