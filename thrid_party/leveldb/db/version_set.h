// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include <map>
#include <set>
#include <vector>

#include "db/dbformat.h"
#include "db/version_edit.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

/***
 * 对于level>0层的sst，基于Key的定位SST文件
 * 因为sst是有序的，进行二分查找即可
 * @param icmp
 * @param files
 * @param key
 * @return
 */
int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key);

/****
 * 内部判断给定的 key 上下限，在对应的 level sst 集合中，是否有重合,
 * 被 Version::OverlapInLevel 调用
 *
 * @param icmp                    key比较器
 * @param disjoint_sorted_files   是否是level-0层文件
 * @param files                   指定的level的sst集合
 * @param smallest_user_key       Key下限
 * @param largest_user_key        key上限
 * @return
 */
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

/****
 * DB 下面所有的 SST 访问空间
 * 在 mem_cache 中数据定期溢写出一个SST文件， 构成的 level-0 层sst
 * 由此可见， level-0 层的sst文件，每个sst内部数据有序， 但是 sst 之间数据无序。
 * 当 level-0 层的数据规模超过一定程度后， 就会与下一层合并， 合并过程中会保证 sst 之前的有序性
 */
class Version {
 public:
  /**
   * 请求的信息
   */
  struct GetStats {
    FileMetaData* seek_file;      // 要查询的文件
    int seek_file_level;          // 文件所在的 level
  };

  /***
   *
   * version 记录了当前所有的 sst 文件， 很多场景下需要对这些 sst 进行遍历，
   * 因此 leveldb 中对所有 sst 文件的 iterator 进行了保存， 以便后续使用。
   *
   * 保存每一层的迭代器， 其中第0层和非0层创建的迭代器不一样
   *
   * 对于 level = 0 的 sstable 文件，直接通过 TableCache::NewIterator() 接口创建，这会直接载入 SST 所有的元数据到内存中。
   * 对于 level > 0 的 sstable 文件， 通过函数 NewTwoLevelIterator() 创建一个TwoLevelIterator, 这会使用懒加载模式
   *
   * @param iters 用于保存 sst 的 Iterator
   */
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  /***
   * 从 SST 中读取所需要的数据
   *
   * @param key
   * @param val
   * @param stats
   * @return
   */
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val, GetStats* stats);

  /***
   * 当查找文件而且没有找到时， 更新 seek 次数状态
   *
   * @param stats
   * @return
   */
  bool UpdateStats(const GetStats& stats);

  /***
   * 统计读的样本， 主要用在迭代器中
   *
   * @param key
   * @return
   */
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  void Ref();
  void Unref();

  /****
   * 在所给定的 level 中找出和[begin, end]有重合的 sstable 文件
   * 注意的是 level-0 层多个文件存在重叠，需要单独遍历每个文件
   *
   * 改函数常被用来压缩的时候使用，根据 leveldb 的设计， level 层合并 Level+1 层Merge时候，level中所有重叠的sst都会参加。
   * 这一点需要特别注意。
   *
   * @param level    要查找的层级
   * @param begin    开始查询的 key
   * @param end      结束查询的 key
   * @param inputs   用于收集重叠的文件
   */
  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);


  /***
   * 检查是否和指定 level 的文件有重叠。
   * 内部直接调用 SomeFileOverlapsRange
   * @param level                 要检查的 level 层
   * @param smallest_user_key     给定的重叠 key 下限
   * @param largest_user_key      给定的重叠 key 的上限
   * @return
   */
  bool OverlapInLevel(int level,
                      const Slice* smallest_user_key,
                      const Slice* largest_user_key);


  /***
   * 选择内存中数据 dump 到磁盘的哪一层
   *
   * @param smallest_user_key
   * @param largest_user_key
   * @return
   */
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  /***
   * 表示某一层有多少个 sst 文件
   * @param level
   * @return
   */
  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  /***
   * 删除当前版本中引用为0的file
   */
  ~Version();

  /***
   * 对于同一个 level 的多个 sst 的查询遍历器
   * 应用在 level > 0 层级别上
   * 第一层用于定位 sst 第二层用于 sst 内部遍历
   * @param options
   * @param level     要遍历的层级
   * @return
   */
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  /***
   * 按层级依次去查询 sst 文件， 找到要查询的key
   * @param user_key
   * @param internal_key
   * @param arg
   * @param func
   */
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg, bool (*func)(void*, int, FileMetaData*));

  VersionSet* vset_;  // 表示这个 verset 隶属于哪一个 verset_set, 在 leveldb 中只有一个 versetset

  /***
   * versionset 是一个双向链表结构
   * 里面每一个 node 是一个 Version
   */
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list

  int refs_;          // 有多少服务还引用这个版本


  /***
   * 当前版本的所有数据 -- 二级指针结构
   * 第一层代表每一个 level 级别
   * 第二层代表同一个 level 级别下面的 sst 文件number
   */
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // Next file to compact based on seek stats.
  FileMetaData* file_to_compact_;                           // 用于 seek 次数超过阈值之后需要压缩的文件
  int file_to_compact_level_;                               // 用于 seek 次数超过阈值之后需要压缩的文件所在的level

  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  double compaction_score_;                                 // 用于检查 size 超过阈值之后需要压缩的文件
  int compaction_level_;                                    // 用于检查 size 查过阈值之后需要压缩的文件所在的 level
};







class VersionSet {
 public:
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  Status Recover(bool* save_manifest);

  // Return the current version.
  Version* current() const { return current_; }

  // Return the current manifest file number
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  Env* const env_;
  const std::string dbname_;
  const Options* const options_;
  TableCache* const table_cache_;
  const InternalKeyComparator icmp_;
  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit edit_;

  // Each compaction reads inputs from "level_" and "level_+1"
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  std::vector<FileMetaData*> grandparents_;
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
