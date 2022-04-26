// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DBFORMAT_H_
#define STORAGE_LEVELDB_DB_DBFORMAT_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "leveldb/comparator.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// Grouping of constants.  We may want to make some of these
// parameters set via options.
namespace config {

// 磁盘上最大的level个数，默认为7
static const int kNumLevels = 7;

// 第 0 层是 sstable 到达这个阈值时触发的压缩， 默认是4
static const int kL0_CompactionTrigger = 4;

// 第0层sst文件数量上限
static const int kL0_SlowdownWritesTrigger = 8;

// 第 0 层 sstable 到达这个阈值时将会停止写，等到压缩结束
static const int kL0_StopWritesTrigger = 12;

// Maximum level to which a new compacted memtable is pushed if it
// does not create overlap.  We try to push to level 2 to avoid the
// relatively expensive level 0=>1 compactions and to avoid some
// expensive manifest file operations.  We do not push all the way to
// the largest level since that can generate a lot of wasted disk
// space if the same key space is being repeatedly overwritten.
// 新压缩产生 sstable 最多允许推送到几层
static const int kMaxMemCompactLevel = 2;

// Approximate gap in bytes between samples of data read during iteration.
// 在数据迭代过程中，也会检查是否满足压缩条件，该参数控制读取的最大字节数
static const int kReadBytesPeriod = 1048576;

}  // namespace config



/**************************************************************************
 * 数据插入 标志
 *
 * ValueType 与 SequenceNumber 两者的关系 :
 *   在一个 uint64 中，ValueType 占一个字节， SequenceNumber 占7个字节 : (SequenceNumber << 8) | ValueType
 *
 *   static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
 *     assert(seq <= kMaxSequenceNumber);
 *     assert(t <= kValueTypeForSeek);
 *    return (seq << 8) | t;
 *  }
 */

// 数据落地类型
enum ValueType {
  kTypeDeletion = 0x0,  // 删除操作
  kTypeValue = 0x1      // 插入操作
};

// kValueTypeForSeek 定义在构造 ParsedInternalKey 对象以查找特定序列号时应传递的 ValueType
// 由于我们按降序对序列号进行排序，并且值类型嵌入为内部键序列号中的低8位，
// 因此我们需要使用编号最高的 ValueType，而不是最低的 ValueType.
static const ValueType kValueTypeForSeek = kTypeValue;

// 一个递增的 uint64 整数，用来标识同一个key 哪个数据最新
typedef uint64_t SequenceNumber;

// unsign long long
// 使用了7个字节
static const SequenceNumber kMaxSequenceNumber = ((0x1ull << 56) - 1);


/*************************************************************************************
 * Key 的类型
 *
 *   user_key : 用户输入的 key (slice 格式)
 *   InternalKey : 内部实现 key
 *           - std::string rep_
 *
 *   ParsedInternalKey : IntenalKey 的解析， 因为 InternalKey 是一个字符串
 *           - Slice user_key;
 *           - SequenceNumber sequence;
 *           - ValueType type;
 *
 *
 *    LookupKey : 用户 DBimpl::Get 中
 *          组成 :
 *             - InternalKey长度
 *             - InternalKey
 *
 *             - char space_[200];
 *                          lookup 中有一个细小的内存优化, 就是类似string的sso优化
 *                          其优化在于，当字符串比较短时， 直接将其数据存储在栈中， 而不去堆中动态申请空间，这就避免了申请堆空间所需的开销
 *             - const char* start_;
 *             - const char* kstart_;
 *             - const char* end_;
 *
 *    MemtableKey : 存储在 memtable 中的 key
 *           - 这个 key 比较特殊，因为他包含了 value 的
 *
 *
 */
class InternalKey;

struct ParsedInternalKey {
  Slice user_key;
  SequenceNumber sequence;
  ValueType type;

  ParsedInternalKey() {}  // Intentionally left uninitialized (for speed)
  ParsedInternalKey(const Slice& u, const SequenceNumber& seq, ValueType t)
      : user_key(u), sequence(seq), type(t) {}
  std::string DebugString() const;
};

// 计算 InternalKey 的长度, user_key + 64 位 序列码
inline size_t InternalKeyEncodingLength(const ParsedInternalKey& key) {
  return key.user_key.size() + 8;
}

// 将 ParsedInternalKey 放入 result 中 : user_key + {sequence + type}(64 位长)
void AppendInternalKey(std::string* result, const ParsedInternalKey& key);

// 将 InternalKey 转化为 ParsedInternalKey
bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result);

// 从 InternalKey 中提取 user_key
inline Slice ExtractUserKey(const Slice& internal_key) {
  assert(internal_key.size() >= 8);
  // 除掉64位编码的前面的 为 user_key
  return Slice(internal_key.data(), internal_key.size() - 8);
}

/***
 * InternalKey 比较器
 * 装饰器模式
 */
class InternalKeyComparator : public Comparator {
 private:
  const Comparator* user_comparator_;

 public:
  explicit InternalKeyComparator(const Comparator* c) : user_comparator_(c) {}
  const char* Name() const override;

  /***
   * InternalKey 比较器 :
   *    a > b : 1
   *    a < b : -1
   *    a = b : 0
   * @return
   */
  int Compare(const Slice& a, const Slice& b) const override;
  void FindShortestSeparator(std::string* start, const Slice& limit) const override;
  void FindShortSuccessor(std::string* key) const override;
  const Comparator* user_comparator() const { return user_comparator_; }

  int Compare(const InternalKey& a, const InternalKey& b) const;
};

/***
 * InternalKey 过滤策略
 * 装饰器模式
 */

class InternalFilterPolicy : public FilterPolicy {
 private:
  const FilterPolicy* const user_policy_;

 public:
  explicit InternalFilterPolicy(const FilterPolicy* p) : user_policy_(p) {}
  const char* Name() const override;
  void CreateFilter(const Slice* keys, int n, std::string* dst) const override;
  // 判断 key 是否在
  bool KeyMayMatch(const Slice& key, const Slice& filter) const override;
};


class InternalKey {
 private:
  // user_key + {sequence_num + value_type}(64)
  std::string rep_;

 public:
  InternalKey() {}  // Leave rep_ as empty to indicate it is invalid
  InternalKey(const Slice& user_key, SequenceNumber s, ValueType t) {
    AppendInternalKey(&rep_, ParsedInternalKey(user_key, s, t));
  }

  bool DecodeFrom(const Slice& s) {
    rep_.assign(s.data(), s.size());
    return !rep_.empty();
  }

  Slice Encode() const {
    assert(!rep_.empty());
    return rep_;
  }
  // 获取 user_key
  Slice user_key() const { return ExtractUserKey(rep_); }

  // 重新设定一个 ParsedInternalKey
  void SetFrom(const ParsedInternalKey& p) {
    rep_.clear();
    AppendInternalKey(&rep_, p);
  }

  void Clear() { rep_.clear(); }

  std::string DebugString() const;
};

inline int InternalKeyComparator::Compare(const InternalKey& a, const InternalKey& b) const {
  return Compare(a.Encode(), b.Encode());
}
// 将 InternalKey -> ParsedInternalKey
inline bool ParseInternalKey(const Slice& internal_key, ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) return false;

  // 提取最后编码
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);

  // 截取 ValueType
  uint8_t c = num & 0xff;

  // 截取 SequenceNumber
  result->sequence = num >> 8;

  result->type = static_cast<ValueType>(c);

  // 提取 user_key
  result->user_key = Slice(internal_key.data(), n - 8);

  // 判断下数据类型是否正确
  return (c <= static_cast<uint8_t>(kTypeValue));
}

/***
 * data :
 *      - user_data_size + seqNum size
 *      - user_data
 *      - seqNum
 */
class LookupKey {
 public:

  // Initialize *this for looking up user_key at a snapshot with
  // the specified sequence number.
  LookupKey(const Slice& user_key, SequenceNumber sequence);

  LookupKey(const LookupKey&) = delete;
  LookupKey& operator=(const LookupKey&) = delete;

  ~LookupKey();

  /***
   * Return a key suitable for lookup in a MemTable.
   *
   * 包含:
   *    - internalkey 长度
   *    - internalKey
   */
  Slice memtable_key() const { return Slice(start_, end_ - start_); }

  /***
   * 包含 :
   *    - Internalkey
   */
  Slice internal_key() const { return Slice(kstart_, end_ - kstart_); }

  /***
   * 包含:
   *  - userkey
   */
  Slice user_key() const { return Slice(kstart_, end_ - kstart_ - 8); }

 private:
  // We construct a char array of the form:
  //    klength  varint32               <-- start_
  //    userkey  char[klength]          <-- kstart_
  //    tag      uint64
  //                                    <-- end_
  // The array is a suitable MemTable key.
  // The suffix starting with "userkey" can be used as an InternalKey.
  const char* start_;
  const char* kstart_;
  const char* end_;
  char space_[200];  // 如果空间占用不高，就栈上分配
};

inline LookupKey::~LookupKey() {
  if (start_ != space_) delete[] start_;
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DBFORMAT_H_
