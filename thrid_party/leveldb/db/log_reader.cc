// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;

Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum, uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}

Reader::~Reader() { delete[] backing_store_; }

bool Reader::SkipToInitialBlock() {
  const size_t offset_in_block = initial_offset_ % kBlockSize;         // 计算在一个块内的偏移位置
  uint64_t block_start_location = initial_offset_ - offset_in_block;    // 需要跳过的完整 block 的位置

  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {  // 剩余位置无法存放一个record 跳过整个 block
    block_start_location += kBlockSize;
  }

  end_of_buffer_offset_ = block_start_location;  // 记录当前偏移位置

  // Skip to start of first block that can contain the initial record
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);  // 报告状态
      return false;
    }
  }

  return true;
}

/***
 *
 * 读取一条有效的记录
 */
bool Reader::ReadRecord(Slice* record, std::string* scratch) {

  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }

  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;  // 缓冲区里面是否有 record

  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  uint64_t prospective_record_offset = 0;

  Slice fragment;

  while (true) {
    // 获取一个 record
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    uint64_t physical_record_offset = end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();

    if (resyncing_) {
      // 重新寻道情况下， 跳过中间块， 定位到第一个有效记录
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {

          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }

        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:

        if (in_fragmented_record) {
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }

        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),"missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),"missing start of fragmented record(2)");
        } else {
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;

      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption((fragment.size() + (in_fragmented_record ? scratch->size() : 0)),buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}

uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

/***
 * 从 block 中读取一个 record
 * @param result 返回 record 数据体
 * @return 返回 record type
 */
unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  while (true) {

    if (buffer_.size() < kHeaderSize) { // buffer 中没有一个完成的数据块
      if (!eof_) {                      // 文件是否读取到最后的标识
        buffer_.clear();

        // 一次性读取一个完整的 block
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);

        end_of_buffer_offset_ += buffer_.size();  // 设置 buffer 的大小
        if (!status.ok()) {
          // 数据读取异常
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          // 数据没有读取到一个 kBlockSize，便已经到了文件结束，
          // 数据存在问题
          eof_ = true;
        }

        continue;
      } else {
        // 文件已经读取到最后，并且没有读取到一个有效的 block
        buffer_.clear();
        return kEof;
      }
    }

    // 解析 block 中第一个 record 的 header
    // Record 格式 :
    //   CRC(4个字节)  | length(2个字节) | recordtype(1个字节) | value
    const char* header = buffer_.data();
    // length
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;  //
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;

    // type
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);

    if (kHeaderSize + length > buffer_.size()) {  // 数据格式异常
      size_t drop_size = buffer_.size();
      buffer_.clear();

      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      return kEof;
    }

    if (type == kZeroType && length == 0) {      // 无效块
      buffer_.clear();
      return kBadRecord;
    }

    // CRC 校验
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }

    // 删除这个 record
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length < initial_offset_) {
      result->clear();
      return kBadRecord;
    }

    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
