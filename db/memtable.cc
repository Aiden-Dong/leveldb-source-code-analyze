// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {

/***
 * 获取 key或者 value
 *
 * data :
 *      - key_length
 *      - key
 *      - seqnum
 *      - value_length
 *      - value
 */
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

/**
 * 初始化 Memtable 内部实际使用的跳表结构维护
 */
MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator),
      refs_(0),
      table_(comparator_, &arena_) {}

MemTable::~MemTable() { assert(refs_ == 0); }

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

/***
 * memtable key 比较器
 *
 * key 结构 : user_key | seqnumber
 */
int MemTable::KeyComparator::operator()(const char* aptr, const char* bptr) const {

  Slice a = GetLengthPrefixedSlice(aptr);  // 从数据中提取 key
  Slice b = GetLengthPrefixedSlice(bptr);
  int  res = comparator.Compare(a, b);

  return res;
}

/***
 * 将 target 与 target_length 拼接
 * target_length:target
 */
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

/***
 *  table 遍历器与查找器
 *  skip_table::iterator 装饰器
 */
class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

/**
 * <pre>
 *  将用户提交的的KeyValue插入到MemTable中
 * </pre>
 * @param s      seqnumber
 * @param type   ValueType
 * @param key    UserKey
 * @param value  value 数据， 对于delete事件， value 为空
 */
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key, const Slice& value) {

  size_t key_size = key.size();            // user_key
  size_t val_size = value.size();          // value

  size_t internal_key_size = key_size + 8; // internal_key size

  // 从内存池中分配内存: key_size | internel_key | value_size | value
  const size_t encoded_len = VarintLength(internal_key_size) + internal_key_size + VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(encoded_len);

  // 填充 key_size
  char* p = EncodeVarint32(buf, internal_key_size);

  // 填充 InternalKey
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;


  // 填充 value_size
  p = EncodeVarint32(p, val_size);

  // 填充 value
  std::memcpy(p, value.data(), val_size);               // value

  assert(p + val_size == buf + encoded_len);

  // 数据插入到 memtable 中
  table_.Insert(buf);                                  // 插入跳表结构中
}

/***
 *
 * @param key memkey
 * @param value value
 * @param s
 * @return
 */
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {

  Slice memkey = key.memtable_key();     // 返回key的 MemTable 表示法  | key_lenth | user_key | seqnumber |
  Table::Iterator iter(&table_);

  // memkey_data :
  //    - user_data.size + seqNum.size
  //    - userdata
  //    - seqnum
  // 本质是调用 : FindGreaterOrEqual
  iter.Seek(memkey.data());

  // 数据已经找到
  if (iter.Valid()) {

    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]

    const char* entry = iter.key();

    uint32_t key_length;

    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);

    // 因为拿到的都是近似相等的数据， 所以要进行精确比对
    if (comparator_.comparator.user_comparator()->Compare(Slice(key_ptr, key_length - 8), key.user_key()) == 0) {

      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8); // 获取序列号

      // 获取数据类型
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          // 获取 value
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb
