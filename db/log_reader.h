// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {

class SequentialFile;

namespace log {

/****
 * Block文件的读句柄
 * 基于SequentialFile顺序读取文件方式
 * 读取Block采用的是缓存机器
 * 每次可能会读取多个Record, 拼接成一个完整的上层数据记录
 */
class Reader {
 public:
  // 失败时的汇报回调
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "bytes" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  /***
   * 创建一个将从“*file”返回日志记录的 read 工具, 使用read时，“*file”必须保持活动状态。
   * 如果“reporter”不为空，则每当由于检测到损坏而删除某些数据时，都会通知它。“*reporter”在该read使用期间必须保持活动状态。
   * 如果“checksum”为真，则验证校验和（如果可用）。
   * 读取器将从位于文件内 物理位置>=initial_offset 的第一条记录开始读取。
   */
  Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset);

  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  ~Reader();

  /***
   * 读取一条有效的记录
   *
   * @param record 用于返回的记录
   * @param scratch 辅助record 用于提供拼接record功能
   */
  bool ReadRecord(Slice* record, std::string* scratch);

  uint64_t LastRecordOffset();

 private:
  // Extend record types with the following special values
  enum {
    kEof = kMaxRecordType + 1,         // block 读取到结束的标识
    kBadRecord = kMaxRecordType + 2    // 错误块标识
  };

  // Skips all blocks that are completely before "initial_offset_".
  //
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

  /***
   * 功能 : 从 Block 中读取一个 Record
   *
   * 这里采用了缓存的机制，文件IO会每次读取一个完整的Block放到缓存中.
   * 每次给上游返回一个Record
   * 当缓存没有Record时，在从磁盘读取一个Block ...
   *
   * @param result 返回 record 数据体
   * @return 返回 record type
   */
  unsigned int ReadPhysicalRecord(Slice* result);


  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  SequentialFile* const file_;  // block 文件
  Reporter* const reporter_;    // 数据损坏报告器
  bool const checksum_;         // 是否启用校验和
  char* const backing_store_;   // 数据块读取缓冲区
  Slice buffer_;                // 数据存储缓冲区
  bool eof_;                    // 文件是否读取到最后的标识

  // ReadRecord 返回的最后一条记录的偏移量.
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  uint64_t end_of_buffer_offset_;  // buffer 的最后偏移点

  // 开始查找要返回的第一条记录的偏移量
  uint64_t const initial_offset_;

  // 如果在寻道后重新同步（初始偏移量>0），则为True。
  // 特别是，在这种模式下，可以无提示地跳过kMiddleType和kLastType记录的运行
  bool resyncing_;
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
