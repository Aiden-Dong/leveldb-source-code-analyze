// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;                       //
static const size_t kFilterBase = 1 << kFilterBaseLg;         // 2KB 数据构建一个 BloomFilter

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);  // 现在需要的 filter_offset 数量
  assert(filter_index >= filter_offsets_.size());

  // 存在的意义是为了多写几个无效的偏移
  // 然后用于快速基于 block_offset 快速定位 filter_offsets
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());   // 记录上次 key 长度
  keys_.append(k.data(), k.size()); // 填充 key 数据
}

Slice FilterBlockBuilder::Finish() {

  // 如果有 key 没有还没有计算完bloomFilter 则触发计算
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    // 填充每个bloom过滤器的偏移
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 填充总的Bloom过滤器的长度
  PutFixed32(&result_, array_offset);
  // 填充 11
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

/***
 * Bloom 过滤器构建过程
 */
void FilterBlockBuilder::GenerateFilter() {

  const size_t num_keys = start_.size(); // 获取 key 的数量

  if (num_keys == 0) {
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // 因为每次都是填充上一次的key, 所以这次记录总得key的长度

  // 提取出所有的 key 使用  vector 存储
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // 存储上次 Bloom 过滤器写入后的数据大小
  filter_offsets_.push_back(result_.size());

  // 计算 Bloom 过滤器的值， 并追加到 result 中
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  // 清空 key
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

/***
 * 构造器
 * @param policy  过滤器策略
 * @param contents Filter Block
 */
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy, const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {

  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1]; // 获取最后的 base_lg

  // last_word 为 FilterBlock 中，最后一个filter数据的结束偏移量
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;

  data_ = contents.data();

  // 定位到filter索引起始位置
  offset_ = data_ + last_word;
  // 计算有多少个  filter 索引
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {

  // 基于 block_offset 定位到 offset_[index]
  uint64_t index = block_offset >> base_lg_;

  if (index < num_) {
    // 截取 Bloom 过滤器
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);

    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start);
      // 匹配查找
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
