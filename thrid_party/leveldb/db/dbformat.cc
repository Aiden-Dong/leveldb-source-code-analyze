// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/dbformat.h"

#include <cstdio>
#include <sstream>

#include "port/port.h"
#include "util/coding.h"

namespace leveldb {

// 将序列号与类型拼接在一起
static uint64_t PackSequenceAndType(uint64_t seq, ValueType t) {
  assert(seq <= kMaxSequenceNumber);
  assert(t <= kValueTypeForSeek);
  return (seq << 8) | t;
}

// 将 ParsedInternalKey 放入 result 中
void AppendInternalKey(std::string* result, const ParsedInternalKey& key) {
  result->append(key.user_key.data(), key.user_key.size());    // 首先存入 user key
  PutFixed64(result, PackSequenceAndType(key.sequence, key.type));  // 最后64位填充 sequence+type
}

std::string ParsedInternalKey::DebugString() const {
  std::ostringstream ss;
  ss << '\'' << EscapeString(user_key.ToString()) << "' @ " << sequence << " : "
     << static_cast<int>(type);
  return ss.str();
}

std::string InternalKey::DebugString() const {
  ParsedInternalKey parsed;
  if (ParseInternalKey(rep_, &parsed)) {
    return parsed.DebugString();
  }
  std::ostringstream ss;
  ss << "(bad)" << EscapeString(rep_);
  return ss.str();
}

const char* InternalKeyComparator::Name() const {
  return "leveldb.InternalKeyComparator";
}

/***
 * InternalKey 比较器
 * @param akey
 * @param bkey
 * @return
 */
int InternalKeyComparator::Compare(const Slice& akey, const Slice& bkey) const {
  // Order by:
  //    increasing user key (according to user-supplied comparator)
  //    decreasing sequence number
  //    decreasing type (though sequence# should be enough to disambiguate)

  // 首先比较 user_key 提升速度
  int r = user_comparator_->Compare(ExtractUserKey(akey), ExtractUserKey(bkey));

  // 如果 user_key 相同， 则比较其版本
  if (r == 0) {
    const uint64_t anum = DecodeFixed64(akey.data() + akey.size() - 8);  // 获取版本号
    const uint64_t bnum = DecodeFixed64(bkey.data() + bkey.size() - 8);  // 获取版本号

    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}


void InternalKeyComparator::FindShortestSeparator(std::string* start, const Slice& limit) const {
  // Attempt to shorten the user portion of the key
  Slice user_start = ExtractUserKey(*start);     // 从 start 中获取 user_key
  Slice user_limit = ExtractUserKey(limit);      // 从 limit 中提取 user_key

  std::string tmp(user_start.data(), user_start.size()); // 复制 start

  user_comparator_->FindShortestSeparator(&tmp, user_limit);

  if (tmp.size() < user_start.size() &&
      user_comparator_->Compare(user_start, tmp) < 0) {
    // user_start > tmp
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*start, tmp) < 0);
    assert(this->Compare(tmp, limit) < 0);
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortSuccessor(std::string* key) const {
  Slice user_key = ExtractUserKey(*key);
  std::string tmp(user_key.data(), user_key.size());
  user_comparator_->FindShortSuccessor(&tmp);
  if (tmp.size() < user_key.size() &&
      user_comparator_->Compare(user_key, tmp) < 0) {
    // User key has become shorter physically, but larger logically.
    // Tack on the earliest possible number to the shortened user key.
    PutFixed64(&tmp,
               PackSequenceAndType(kMaxSequenceNumber, kValueTypeForSeek));
    assert(this->Compare(*key, tmp) < 0);
    key->swap(tmp);
  }
}

const char* InternalFilterPolicy::Name() const { return user_policy_->Name(); }

/**
 *
 * @param keys : 多个 InternalKey 集合
 * @param n : InternalKey 个数
 * @param dst : 基于
 */
void InternalFilterPolicy::CreateFilter(const Slice* keys, int n, std::string* dst) const {
  // We rely on the fact that the code in table.cc does not mind us
  // adjusting keys[].
  Slice* mkey = const_cast<Slice*>(keys);

  for (int i = 0; i < n; i++) {
    mkey[i] = ExtractUserKey(keys[i]);
    // TODO(sanjay): Suppress dups?
  }
  user_policy_->CreateFilter(keys, n, dst);
}

bool InternalFilterPolicy::KeyMayMatch(const Slice& key, const Slice& f) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), f);
}

LookupKey::LookupKey(const Slice& user_key, SequenceNumber s) {
  size_t usize = user_key.size();
  size_t needed = usize + 13;  // +8(seq) +5(usersize)

  char* dst;

  // 分配空间
  if (needed <= sizeof(space_)) {
    dst = space_;
  } else {
    dst = new char[needed];
  }

  start_ = dst; // 标识内存起始地址

  dst = EncodeVarint32(dst, usize + 8); // 存储总的数据长度 (user_key + seqnum)
  kstart_ = dst;                              // 用户起始地址
  std::memcpy(dst, user_key.data(), usize);   // 用户名实体
  dst += usize;
  EncodeFixed64(dst, PackSequenceAndType(s, kValueTypeForSeek)); // 序列号
  dst += 8;
  end_ = dst;  // 终止指针
}

}  // namespace leveldb
