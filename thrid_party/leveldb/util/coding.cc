// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/coding.h"

namespace leveldb {

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(value)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(value)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

/***
 * 将 32 位整形变为可变长度的整形， 用来节省空间
 * 在变长整形中，每个字节低七位用来保存真实数据， 最高一位设置数据是否连续:
 *   0xxx xxxx 1xxx xxxx 1xxx xxxx 1xxx xxxx
 */
char* EncodeVarint32(char* dst, uint32_t v) {

  uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
  static const int B = 128;

  if (v < (1 << 7)) {
    // 可以使用1个字节存储
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    // 需要使用2个字节存储
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    // 需要使用3个字节存储
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    // 需要使用4个字节存储
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    // 需要使用5个字节存储
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}

/**
 * 将变长32位的数据存放到 dst 中
 */
void PutVarint32(std::string* dst, uint32_t v) {
  char buf[5]; // 最大只能存储 5 个字节
  char* ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf); // 拷贝
}

char* EncodeVarint64(char* dst, uint64_t v) {
  static const int B = 128;
  uint8_t* ptr = reinterpret_cast<uint8_t*>(dst);
  while (v >= B) {
    *(ptr++) = v | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<uint8_t>(v);
  return reinterpret_cast<char*>(ptr);
}

void PutVarint64(std::string* dst, uint64_t v) {
  char buf[10];
  char* ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

/***
 * 将 value 的长度与value 填充到dst
 */
void PutLengthPrefixedSlice(std::string* dst, const Slice& value) {
  PutVarint32(dst, value.size());  // 填充 value 长度
  dst->append(value.data(), value.size()); // 填充 value 数据
}

int VarintLength(uint64_t v) {
  int len = 1;
  while (v >= 128) {
    v >>= 7;
    len++;
  }
  return len;
}

const char* GetVarint32PtrFallback(const char* p, const char* limit, uint32_t* value) {

  uint32_t result = 0;

  for (uint32_t shift = 0; shift <= 28 && p < limit; shift += 7) {
    uint32_t byte = *(reinterpret_cast<const uint8_t*>(p));
    p++;

    if (byte & 128) {  // 表示中间位
      result |= ((byte & 127) << shift);  // 拼接有效部分
    } else {  // 标识最后一位
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}


/***
 * 将 变长 32 位的数据字符串转为 uint32
 */
bool GetVarint32(Slice* input, uint32_t* value) {
  const char* p = input->data();          // 获取数据首部指针
  const char* limit = p + input->size();  // 获取数据尾部指针

  const char* q = GetVarint32Ptr(p, limit, value);

  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, limit - q);     // 拷贝剩余数据
    return true;
  }
}

const char* GetVarint64Ptr(const char* p, const char* limit, uint64_t* value) {
  uint64_t result = 0;
  for (uint32_t shift = 0; shift <= 63 && p < limit; shift += 7) {
    uint64_t byte = *(reinterpret_cast<const uint8_t*>(p));
    p++;
    if (byte & 128) {
      // More bytes are present
      result |= ((byte & 127) << shift);
    } else {
      result |= (byte << shift);
      *value = result;
      return reinterpret_cast<const char*>(p);
    }
  }
  return nullptr;
}

bool GetVarint64(Slice* input, uint64_t* value) {
  const char* p = input->data();
  const char* limit = p + input->size();
  const char* q = GetVarint64Ptr(p, limit, value);
  if (q == nullptr) {
    return false;
  } else {
    *input = Slice(q, limit - q);
    return true;
  }
}


bool GetLengthPrefixedSlice(Slice* input, Slice* result) {

  uint32_t len;

  // 获取
  if (GetVarint32(input, &len) && input->size() >= len) {
    *result = Slice(input->data(), len);
    input->remove_prefix(len);
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb
