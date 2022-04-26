// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include <atomic>
#include <deque>
#include <set>
#include <string>

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class DBImpl : public DB {
 public:
  DBImpl(const Options& options, const std::string& dbname);

  DBImpl(const DBImpl&) = delete;
  DBImpl& operator=(const DBImpl&) = delete;

  ~DBImpl() override;

  // Implementations of the DB interface
  Status Put(const WriteOptions&, const Slice& key,
             const Slice& value) override;
  Status Delete(const WriteOptions&, const Slice& key) override;
  Status Write(const WriteOptions& options, WriteBatch* updates) override;
  Status Get(const ReadOptions& options, const Slice& key,
             std::string* value) override;
  Iterator* NewIterator(const ReadOptions&) override;
  const Snapshot* GetSnapshot() override;
  void ReleaseSnapshot(const Snapshot* snapshot) override;
  bool GetProperty(const Slice& property, std::string* value) override;
  void GetApproximateSizes(const Range* range, int n, uint64_t* sizes) override;
  void CompactRange(const Slice* begin, const Slice* end) override;

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice* begin, const Slice* end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator* TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

 private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey* begin;  // null means beginning of key range
    const InternalKey* end;    // null means end of key range
    InternalKey tmp_storage;   // Used to keep track of compaction progress
  };

  /***
   * 记录压缩过程中的 metric
   */
  struct CompactionStats {

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats& c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }

    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;
  };

  Iterator* NewInternalIterator(const ReadOptions&,
                                SequenceNumber* latest_snapshot,
                                uint32_t* seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit* edit, bool* save_manifest)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status* s) const;

  // Delete any unneeded files and stale in-memory entries.
  void RemoveObsoleteFiles() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  // Errors are recorded in bg_error_.
  void CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, bool last_log, bool* save_manifest,
                        VersionEdit* edit, SequenceNumber* max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /***
   * IMemTable写到level0层sst
   *
   * @param mem  要被刷盘的 SST
   * @param edit 用于记录基于当前版本的变更项
   * @param base 当前版本链接
   */
  Status WriteLevel0Table(MemTable* mem, VersionEdit* edit, Version* base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  /******
   *
   * 1. 有错误的话，直接推出
   * 2. 如果允许延迟，而且文件个数没有超过硬限制，那么可以等1s, 但是只能等一次
   * 3. 如果不能延迟，而且mem又有充足的空间，那么也不需要进行处理，直接退出循环
   * 4. 如果已经有一个minor压缩压缩线程了，此时不能在启动一个
   * 5. 如果第0层文件超过了硬限制，此时需要停止写了。等待被唤醒
   * 6. 否则就需要创建新的mem,开始 minor 压缩， 调用 @{MaybeScheduleCompaction}
   *
   * @param force 是否强制写
   *            if force==true : 不允许延迟
   *            else : 允许延迟
   */
  Status MakeRoomForWrite(bool force) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  WriteBatch* BuildBatchGroup(Writer** last_writer)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void RecordBackgroundError(const Status& s);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void* db);
  void BackgroundCall();
  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState* compact);
  Status FinishCompactionOutputFile(CompactionState* compact, Iterator* input);
  Status InstallCompactionResults(CompactionState* compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Comparator* user_comparator() const {
    return internal_comparator_.user_comparator();
  }

  // Constant after construction
  Env* const env_;                                              // 系统环境接口
  const InternalKeyComparator internal_comparator_;             // InternalKey 比较器
  const InternalFilterPolicy internal_filter_policy_;           // InternalKey 过滤策略
  const Options options_;                                       // 系统选项
  const bool owns_info_log_;                                    // 表示是否启用了info_log打印日志
  const bool owns_cache_;                                       // 表示是否启用了 block 缓存
  const std::string dbname_;                                    // 数据库名称

  TableCache* const table_cache_;                               // 基于缓存的数据查询接口

  FileLock* db_lock_;                                           // 文件锁

  port::Mutex mutex_;                                            // 互斥锁
  std::atomic<bool> shutting_down_;                              // 表示db是否关闭
  port::CondVar background_work_finished_signal_ GUARDED_BY(mutex_);

  MemTable* mem_;                                                // 活跃的 memtab
  MemTable* imm_ GUARDED_BY(mutex_);                             // 要被压缩的 memtab

  std::atomic<bool> has_imm_;                                    // 表示是否已经有一个 imm_, 因为只有一个线程在压缩， 所以只有一个 imm_

  WritableFile* logfile_;                                        // 日志文件写句柄， log_的底层写文件接口
  uint64_t logfile_number_ GUARDED_BY(mutex_);                   // 当前日志编号
  log::Writer* log_;                                             // WAL 操作句柄

  uint32_t seed_ GUARDED_BY(mutex_);                             // 用于采样

  // Queue of writers.
  std::deque<Writer*> writers_ GUARDED_BY(mutex_);               // 用于批量写

  WriteBatch* tmp_batch_ GUARDED_BY(mutex_);

  SnapshotList snapshots_ GUARDED_BY(mutex_);                    // 快照, leveldb 支持从某个快照读取数据

  std::set<uint64_t> pending_outputs_ GUARDED_BY(mutex_);        //  保护某些sst文件不被删除， 主要在CompactMemTable中

  bool background_compaction_scheduled_ GUARDED_BY(mutex_);      // 表示后台压缩线程是否已经被调度或者在运行

  ManualCompaction* manual_compaction_ GUARDED_BY(mutex_);       // 手动压缩句柄

  VersionSet* const versions_ GUARDED_BY(mutex_);                // 版本管理

  // Have we encountered a background error in paranoid mode?
  Status bg_error_ GUARDED_BY(mutex_);

  CompactionStats stats_[config::kNumLevels] GUARDED_BY(mutex_);  // 用于记录每次压缩的耗时与文件量等信息
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
Options SanitizeOptions(const std::string& db,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_DB_IMPL_H_
