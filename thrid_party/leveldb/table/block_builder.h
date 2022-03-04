// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;

/****
 *  BlockBuild 用来构建 datablock 与 indexblock
 *  数据格式 :
 *      | shared_length(Varint32) | unshared_length(Varint32) | value_length(Varint32) | delta_key(string) | value(string) |
 *      | shared_length(Varint32) | unshared_length(Varint32) | value_length(Varint32) | delta_key(string) | value(string) |
 *      。。。。
 *      | shared_length(Varint32) | unshared_length(Varint32) | value_length(Varint32) | delta_key(string) | value(string) |
 *      | restarts_[0](Fixed32) | restarts_[1](Fixed32) | restarts_[2](Fixed32) | ... | restarts_[k](Fixed32) |
 *      | restarts_size(Fixed32) |
 *
 *  注意 :
 *      第一个数据 shared_length := 0, delta_key 表示一个完整的key
 *      调用 reset() 以后， 同样 shared_length := 0, delta_key 表示一个完整的key
 */
class BlockBuilder {
 public:
  /***
   * 构造函数
   */
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  /***
   * 本次block 结束， 清空状态
   */
  void Reset();

  void Add(const Slice& key, const Slice& value);

  Slice Finish();

  /***
 * 返回整个 buffer 的大小
 */
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  const Options* options_;          // 配置选项
  std::string buffer_;              // 序列化之后的数据
  std::vector<uint32_t> restarts_;  // 重启点
  int counter_;                     // 重启点计数器
  bool finished_;                   // 是否结束
  std::string last_key_;            // 记录上一次 key
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
