// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

/****
 *
 * Record 格式 :
 *    CRC(4个字节)  | length(2个字节) | recordtype(1个字节) | value
 *
 *    record :=
 *          checksum : uint32
 *          length   : uint16
 *          type     : uint8
 *          data     : uint8[length]
 *
 *
 */
namespace leveldb {
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,
  kFullType = 1,      // 新的 block, 又刚好装下， 完美
  kFirstType = 2,     // 新 block, 但是一个又装不下
  kMiddleType = 3,    // 其他场景
  kLastType = 4       // 上一份数据到本block, 结束了
};
static const int kMaxRecordType = kLastType;

static const int kBlockSize = 32768;  // 32 KB

// CRC + length + type
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
