// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval  >= 1);
  restarts_.push_back(0);  // 记录第一个重启位置
}

/***
 * Block 写入完成后， 清空所有的状态，重新写入一个block
 */
void BlockBuilder::Reset() {
  buffer_.clear();          // 清空缓存
  restarts_.clear();        // 清空所有的重启点
  restarts_.push_back(0);   // 记录第一个重启点
  counter_ = 0;             // 数据计数重置
  finished_ = false;
  last_key_.clear();
}

/***
 * 返回整个 buffer 的大小
 */
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);       // 填充所有的重启点到 buffer
  }
  PutFixed32(&buffer_, restarts_.size());     // 填充重启点的数量到 buffer
  finished_ = true;
  return Slice(buffer_);                            // 返回整个 buffer
}

/***
 * 添加 KeyValue， 如果添加次数超过 options_->block_restart_interval, 则记录一下偏移点
 *
 * 添加位置 :  buffer_
 * 添加格式 :  | shared_length | unshared_length | value_length | delta_key | value |
 *
 * last_key_ : 指向上次写入的数据
 *
 */
void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);     // 得到上次的key

  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);

  size_t shared = 0;

  // block_restart_interval 控制着重启点之间的距离
  if (counter_ < options_->block_restart_interval) {
    // 记录相同key的位置
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    // 重启过程中 key 相当于置空 -> share=0
    restarts_.push_back(buffer_.size());  // 记录重启点的 buffer 偏移
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;  // 非共享 key 的长度

  // 存储数据长度
  PutVarint32(&buffer_, shared);       // 写入共享 key 长度   -- 变长 32 位
  PutVarint32(&buffer_, non_shared);   // 写入非共享 key 长度 -- 变长 32 位
  PutVarint32(&buffer_, value.size()); // 写入 value 长度    -- 变长 32 位

  // 存储真实数据
  buffer_.append(key.data() + shared, non_shared);   // 非共享 key 部分的存储
  buffer_.append(value.data(), value.size());           // 数据存储

  // 更新 last_key 表示为  上次插入的key
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);

  assert(Slice(last_key_) == key);
  counter_++;  // 计数加一
}

}  // namespace leveldb
