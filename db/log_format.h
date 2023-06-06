// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

/******
 * 基于Block的文件维护方式
 * 用于 : WAL, MANIFEST
 *
 * 文件的格式 : | BLOCK | BLOCK | BLOCK | BLOCK | BLOCK | BLOCK | BLOCK |
 *    Block 大小 : 32KB
 *
 * 每个BLOCK可能会有一到多条RECORD, RECORD的数据大小随机
 * 用户提交的写入数据在这里会争取落在一个Block中，用一个Record.
 * 如果数据太大或者Block的剩余空间不足，不能一次性将一条记录写在一个Block中。
 * 那么记录将被拆分成多个Record写在多个Block中。
 *
 * Record 格式 : | CRC(4个字节)  | length(2个字节) | recordtype(1个字节) | value(length个字节) |
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
