// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

/***
 * 初始化 type_crc_[kMaxRecordType] 存放crc值
 * @param type_crc
 */
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}

Writer::~Writer() = default;

Status Writer::AddRecord(const Slice& slice) {

  const char* ptr = slice.data(); // 数据体指针
  size_t left = slice.size();     // 数据体大小

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;

  do {

    const int leftover = kBlockSize - block_offset_;  // 计算剩余空间

    assert(leftover >= 0);

    if (leftover < kHeaderSize) {
      // 剩余空间不足以存放 record header

      if (leftover > 0) {
        static_assert(kHeaderSize == 7, "");
        // 剩余空间用0填充
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }

      // 重新开一个 block
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 能够存放数据体的大小
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;

    // 实际能写入的数据大小
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);  // 是否能够写完

    if (begin && end) {
      // 这个数据包能够一次性存储到一个 block 里面
      type = kFullType;
    } else if (begin) {
      // 数据包不能够一次性存储到一个block里面， 需要拆解 record
      // 这是第一个 redcord
      type = kFirstType;
    } else if (end) {
      // 数据包不能够一次性存储到一个block里面， 需要拆解 record
      // 这是最后一个 record
      type = kLastType;
    } else {
      // 数据包不能够一次性存储到一个block里面， 需要拆解 record
      // 这是中间 record redcord
      type = kMiddleType;
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

/***
 *  Record 格式 :
 *    CRC(4个字节)  | length(2个字节) | recordtype(1个字节) | valueh
 * @return
 */
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,size_t length) {

  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // 计算填充数据头 :
  //   crc        - 4 byte
  //   length     - 2 byte
  //   recordtype - 1 byte
  char buf[kHeaderSize];
  // length
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  // type
  buf[6] = static_cast<char>(t);

  // 计算数据体的 crc 值
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // 写入数据头部
  Status s = dest_->Append(Slice(buf, kHeaderSize));

  // 写入数据体，并写盘
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
