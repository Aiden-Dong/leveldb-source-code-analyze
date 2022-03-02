// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_FORMAT_H_
#define STORAGE_LEVELDB_TABLE_FORMAT_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/table_builder.h"

namespace leveldb {

class Block;
class RandomAccessFile;
struct ReadOptions;

/*****
 * table_format :
 *
 *              <beginning_of_file>
 *              [data block 1]
 *              [data block 2]
 *              ...
 *              [data block N]
 *              [meta block 1]
 *              ...
 *              [meta block K]
 *              [metaindex block]
 *              [index block]
 *              [Footer]        (fixed size; starts at file_size - sizeof(Footer))
 *              <end_of_file>
 *
 *
 */


/***
 * BlockHandle 是指向 datablock 或 metablock 的文件范围的指针。
 */
class BlockHandle {
 public:
  // Maximum encoding length of a BlockHandle
  // 变长 64 位最大占用 10 个字节
  // 64/7
  enum { kMaxEncodedLength = 10 + 10 };

  BlockHandle();

  // The offset of the block in the file.
  uint64_t offset() const { return offset_; }
  void set_offset(uint64_t offset) { offset_ = offset; }

  // The size of the stored block
  uint64_t size() const { return size_; }
  void set_size(uint64_t size) { size_ = size; }

  // 序列号工具
  // offset_, size_ (可变整型)
  void EncodeTo(std::string* dst) const;
  // 反序列化工具
  // 从 input 字符串中解码处 offset_, size_
  Status DecodeFrom(Slice* input);

 private:
  /**
   * offset_, size_
   */
  uint64_t offset_;   // block 的起始位置
  uint64_t size_;     // block 的大小
};

// Footer encapsulates the fixed information stored at the tail
// end of every table file.

/***
 * Footer 封装了存储在每个表文件末尾的固定信息。
 * 固定 48 个字节
 */
class Footer {
 public:

  // 使用 enum 代替 #define
  // Footer 的长度， 固定48个字节
  // 它是由两个块句柄和一个幻数(kTableMagicNumber)组成
  enum { kEncodedLength = 2 * BlockHandle::kMaxEncodedLength + 8 };

  Footer() = default;

  // metaindex_handle_
  const BlockHandle& metaindex_handle() const { return metaindex_handle_; }
  void set_metaindex_handle(const BlockHandle& h) { metaindex_handle_ = h; }

  // index_handle_
  const BlockHandle& index_handle() const { return index_handle_; }
  void set_index_handle(const BlockHandle& h) { index_handle_ = h; }

  // 序列化工具
  // 将 metaindex_handle_， index_handle_， kTableMagicNumber 序列化出去
  // 填充到 dst
  void EncodeTo(std::string* dst) const;

  // 反序列化工具
  // 反序列化 metaindex_handle_, index_handle_, kTableMagicNumber
  // 填充到 footer
  Status DecodeFrom(Slice* input);

 private:
  BlockHandle metaindex_handle_;  // 指向 meta index 块
  BlockHandle index_handle_;      // 指向 data index 块
};

// kTableMagicNumber was picked by running
//    echo http://code.google.com/p/leveldb/ | sha1sum
// and taking the leading 64 bits.
static const uint64_t kTableMagicNumber = 0xdb4775248b80fb57ull;

// 1-byte type + 32-bit crc
static const size_t kBlockTrailerSize = 5;

struct BlockContents {
  Slice data;           // Actual contents of data
  bool cachable;        // True iff data can be cached
  bool heap_allocated;  // True iff caller should delete[] data.data()
};

// Read the block identified by "handle" from "file".  On failure
// return non-OK.  On success fill *result and return OK.
Status ReadBlock(RandomAccessFile* file, const ReadOptions& options, const BlockHandle& handle, BlockContents* result);

// Implementation details follow.  Clients should ignore,

inline BlockHandle::BlockHandle()
    : offset_(~static_cast<uint64_t>(0)), size_(~static_cast<uint64_t>(0)) {}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FORMAT_H_
