// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/filter_policy.h"

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

/***
 * leveldb bloom 过滤器实现
 */
namespace {
static uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

class BloomFilterPolicy : public FilterPolicy {
 public:
  explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
    // We intentionally round down to reduce probing cost a little bit
    k_ = static_cast<size_t>(bits_per_key * 0.69);  // 0.69 =~ ln(2)
    if (k_ < 1) k_ = 1;
    if (k_ > 30) k_ = 30;
  }

  const char* Name() const override { return "leveldb.BuiltinBloomFilter2"; }

  /***
   * BloomFilter 生成 放置到 ${dst} 中
   * @param keys
   * @param n
   * @param dst
   */
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {

    /***
     * 预估至少需要多少位来存储这些 key 的bloom filter 标识
     */
    size_t bits = n * bits_per_key_;
    if (bits < 64) bits = 64;
    // 向上对齐，因为至少要存一个字节
    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    /***
     * 初始化 BloomFilter 位, 置空
     */
    const size_t init_size = dst->size();     // 获取原始大小
    dst->resize(init_size + bytes, 0);  // 扩大新的位大小， 并且填空新的元素为 0[初始化 bloom 过滤器]
    dst->push_back(static_cast<char>(k_));  // 将最后一位存放这个过滤器用了多少个 hash 函数

    char* array = &(*dst)[init_size];  // 获取到 bloom 过滤器的首部位置

    /***
     * 设置每个key的BloomFilter位
     */
    for (int i = 0; i < n; i++) {

      uint32_t h = BloomHash(keys[i]);

      // 高低位转换 : 17 + 15 = 32, 用与 hash 偏移因子
      const uint32_t delta = (h >> 17) | (h << 15);

      for (size_t j = 0; j < k_; j++) {             // k_ 个 hash 函数
        const uint32_t bitpos = h % bits;           // 取 bits 个有效位
        array[bitpos / 8] |= (1 << (bitpos % 8));   // 对应的byte的对应位置 1
        h += delta;
      }
    }
  }

  /**
   * Bloom 过滤器校验
   * @param key 用户校验的key
   * @param bloom_filter 存放的bloom 过滤器值
   * @return
   */
  bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const override {

    /***
     * 获取 bloom 过滤器的长度
     */
    const size_t len = bloom_filter.size();
    if (len < 2) return false;

    const char* array = bloom_filter.data();
    const size_t bits = (len - 1) * 8;

    // 获得有多少个 hash 函数，最多30个
    const size_t k = array[len - 1];
    if (k > 30) {
      // Reserved for potentially new encodings for short bloom filters.
      // Consider it a match.
      return true;
    }

    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);  // hash 比对
    for (size_t j = 0; j < k; j++) {
      const uint32_t bitpos = h % bits;
      if ((array[bitpos / 8] & (1 << (bitpos % 8))) == 0) return false;
      h += delta;
    }
    return true;
  }

 private:
  size_t bits_per_key_;   // 预估每个key大概需要多少位可以存储
  size_t k_;              // k 个 hash 函数, 默认 30 个
};
}  // namespace

const FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}

}  // namespace leveldb
