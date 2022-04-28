// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_WRITER_H_
#define STORAGE_LEVELDB_DB_LOG_WRITER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class WritableFile;

namespace log {


 /***
  * 基于Block数据格式的写文件句柄
  * 内部使用 WritableFile 追加写方式， 每次写Record完成后会刷盘操作
  * 每次写记录(AddRecord)过程中,可能会写一到多个Record.
  * 写多个Record是发生在跨Block写的过程。
  */
class Writer {
 public:

  explicit Writer(WritableFile* dest);

  Writer(WritableFile* dest, uint64_t dest_length);

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;

  ~Writer();

  /***
   * 将上游提供的数据(slice)写入到Block中， 可能这个过程写入多个Block。
   *
   * 写入过程 :
   *   1. 根据要写入的数据，计算一次性写入Block需要的空间(slice.size() + 7(首部大小))
   *   2. 找到当前在写的Block, 计算Block的剩余空间(kBlockSize - block_offset_)
   *   3. 如果剩余空间不足写一个首部, 则将Block剩余空间填充为0，重新开一个新的Block写入数据
   *   4. 根据要写入Block剩余空间与当前的数据量，如果一个Block无法存放整个数据，则将数据拆分成多个部分写入多个Block.
   *
   * @param slice 上游提供的要写入Block中的数据
   */
  Status AddRecord(const Slice& slice);

 private:
  /***
   * 将数据(来自上游提交的部分或者整体数据),根据Block协议封装成一个数据帧(Record)写入到Block中
   *
   * 数据帧(Record)格式: | CRC(4个字节)  | length(2个字节) | recordtype(1个字节) | value(length个字节) |
   */
  Status EmitPhysicalRecord(RecordType type, const char* ptr, size_t length);

  WritableFile* dest_;   // 绑定写文件, 所有的 block 写入此文件
  int block_offset_;     // 当前block偏移量

  uint32_t type_crc_[kMaxRecordType + 1];   // 用于CRC校验
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_WRITER_H_
