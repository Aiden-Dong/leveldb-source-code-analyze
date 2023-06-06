// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_H_

#include <cstddef>
#include <cstdint>

#include "leveldb/iterator.h"

namespace leveldb {

struct BlockContents;
class Comparator;
/***
 * 主要用来操作 BlockContents 数据，因为 BlockContent 数据本身操作较为困难
 * 所以提供了 BlockContexts中间类
 *
 * @{BlockBuilder} 构建的数据包解析器
 *
 * 数据结构 :
 *
 *     data_            ->  share_key_value_0
 *                          share_key_value_1
 *                          .....
 *     current_         ->  share_key_value_h
 *                          .....
 *                          share_key_value_n
 *     restarts_        ->  restart_0
 *                          restart_1
 *                          ....
 *                          restart_m
 *                          ....
 *                          restart_k
 *                          num_restarts_
 *
 * 主要作用 :
 *    - 保存 BlockContents 转换后的数据， 存储到 cache 中
 *    - BlockContexts 数据为 BlockBuilder 构造的复杂数据结构，Block 中提供了 Block::Iter 用来检索 BlockBuilder 对应结构的数据
 */
class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(const BlockContents& contents);

  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;

  ~Block();

  size_t size() const { return size_; }
  Iterator* NewIterator(const Comparator* comparator);

 private:
  class Iter;  // 声明内部类

  uint32_t NumRestarts() const;

  const char* data_;          // 记录数据起始地址
  size_t size_;               // 记录数据的大小
  uint32_t restart_offset_;   // 重启点的初始偏移位置
  bool owned_;                // 堆存处还是栈存储
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_H_
