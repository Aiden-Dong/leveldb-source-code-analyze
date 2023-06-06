// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_VERSION_EDIT_H_

#include <set>
#include <utility>
#include <vector>

#include "db/dbformat.h"

namespace leveldb {

class VersionSet;

struct FileMetaData {

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) {}

  int refs;              // 引用数量
  int allowed_seeks;     // Seeks allowed until compaction
  uint64_t number;       // sst 文件编号
  uint64_t file_size;    // 文件大小
  InternalKey smallest;  // sst 文件最小 key
  InternalKey largest;   // sst 文件最大 key
};

/******
 * 在 leveldb 中使用 versionEdit 来记录每次变更。 因此，数据库正常/非正常关闭重新打开后，只需要
 * 按顺序把 MANIFEST 文件中的 VersionEdit 执行一遍，就可以把数据恢复到宕机前的最新版本。
 *
 * 内存中的数据持久化由WAL文件保证，所以WAL文件+sstables文件，就可以保证数据不会丢失
 *
 * Manifest 文件可以看做是 VersionEdit 的日志，它内部维护了每一层 sst 文件的一个列表
 * 为了快速回复需要将这些变更持久化到磁盘上
 *
 *
 * 数据结构 :
 *
 * kComparator[tag]     : var32 bytes | comparator_lenth  : var32 bytes | comparator_ |          // 存放比较器名称信息
 * kLogNumber[tag]      : var32 bytes | log_number_       : var64 bytes |                        // 存放日志编号信息
 * kPreLogNumber[tag]   : var32 bytes | prev_log_number_  : var64 bytes |                        // 存放PreLog编号信息
 * kNextFileNumber[tag] : var32 bytes | next_file_number_ : var64 bytes |                        // 存放Next文件编号信息
 * kLastSequence[tag]   : var32 bytes | last_sequence_    : var64 bytes |                        // 存放最后的SeqNum信息
 * kCompactPointer[tag] : var32 bytes | level_number      : var32 bytes | internal_key_len : var32 bytes | internal_key            |
 * kCompactPointer[tag] : var32 bytes | level_number      : var32 bytes | internal_key_len : var32 bytes | internal_key            |
 * ...
 * kCompactPointer[tag] : var32 bytes | level_number      : var32 bytes | internal_key_len : var32 bytes | internal_key            |  // 存放每一层要压缩的key
 * kDeletedFile[tag]    : var32 bytes | level_number      : var32 bytes | file_number      : var64 bytes | // 存放删除的文件记录
 * kDeletedFile[tag]    : var32 bytes | level_number      : var32 bytes | file_number      : var64 bytes |
 * ...
 * kDeletedFile[tag]    : var32 bytes | level_number      : var32 bytes | file_number      : var64 bytes |
 * kNewFile[tag]        : var32 bytes | level_number      : var32 bytes | file_number      : var64 bytes | file_size : var64 bytes |  smallest_key_len  : var32 bytes | smallest_key | largest_key_len : var32 bytes | largest |  // 存放新增的SST文件
 * kNewFile[tag]        : var32 bytes | level_number      : var32 bytes | file_number      : var64 bytes | file_size : var64 bytes |  smallest_key_len  : var32 bytes | smallest_key | largest_key_len : var32 bytes | largest |
 * ...
 * kNewFile[tag]        : var32 bytes | level_number      : var32 bytes | file_number      : var64 bytes | file_size : var64 bytes |  smallest_key_len  : var32 bytes | smallest_key | largest_key_len : var32 bytes | largest |
 */
class VersionEdit {
 public:
  VersionEdit() { Clear(); }
  ~VersionEdit() = default;

  /****
   * 清空 VersionEdit 信息
   */
  void Clear();

  /***
   * 设置比较器的名字
   * @param name 比较器名称
   */
  void SetComparatorName(const Slice& name) {
    has_comparator_ = true;
    comparator_ = name.ToString();
  }

  /****
   * 设置日志编号
   * @param num 日志编号
   */
  void SetLogNumber(uint64_t num) {
    has_log_number_ = true;
    log_number_ = num;
  }

  /*****
   * 设置 PreLogNumber
   * @param num
   */
  void SetPrevLogNumber(uint64_t num) {
    has_prev_log_number_ = true;
    prev_log_number_ = num;
  }

  /****
   * 设置下一个文件编号
   * @param num 下一个文件编号
   */
  void SetNextFile(uint64_t num) {
    has_next_file_number_ = true;
    next_file_number_ = num;
  }

  /***
   * 设置最后的 SequenceNumber
   * @param seq
   */
  void SetLastSequence(SequenceNumber seq) {
    has_last_sequence_ = true;
    last_sequence_ = seq;
  }


  /****
   * 存储当前VersionEdit执行压缩的level与压缩的最大的user_key
   */
  void SetCompactPointer(int level, const InternalKey& key) {
    compact_pointers_.push_back(std::make_pair(level, key));
  }

  /***
   * 将一个新的SST添加到version中
   *
   * @param level          SST 要添加到的level
   * @param file           SST 文件编号
   * @param file_size      SST 文件编码
   * @param smallest       SST 文件最小key
   * @param largest        SST 文件最大key
   */
  void AddFile(int level,
               uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {

    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(std::make_pair(level, f));
  }

  // 移出一个文件从version里
  void RemoveFile(int level, uint64_t file) {
    deleted_files_.insert(std::make_pair(level, file));
  }

  /*****
   * 序列化 VersionEdit 到 MANIFEST
   *
   * @param dst
   */
  void EncodeTo(std::string* dst) const;

  /***
   * 从 MANIFEST 反序列化数据到到 VersionEdit
   * @param src
   * @return
   */
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;

 private:
  friend class VersionSet;

  typedef std::set<std::pair<int, uint64_t>> DeletedFileSet;

  std::string comparator_;            // 比较器名字
  uint64_t log_number_;               // 日志编号， 该日志之前的数据均可删除
  uint64_t prev_log_number_;          // 已经弃用
  uint64_t next_file_number_;         // 下一个文件编号(ldb, idb, MANIFEST文件共享一个序号空间)
  SequenceNumber last_sequence_;      // 最后的 Seq_num

  bool has_comparator_;               //
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;

  std::vector<std::pair<int, InternalKey>> compact_pointers_;   // 存储在本次执行的压缩操作对应的level与压缩的最大的key
  DeletedFileSet deleted_files_;                                // 相比上次 version 而言， 本次需要删除的文件有哪些
  std::vector<std::pair<int, FileMetaData>> new_files_;         // 相比于上次 version 而言， 本次新增文件有哪些
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_EDIT_H_
