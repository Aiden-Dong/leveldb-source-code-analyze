// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <set>
#include <string>
#include <vector>

#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/status.h"
#include "leveldb/table.h"
#include "leveldb/table_builder.h"
#include "port/port.h"
#include "table/block.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"
#include "util/mutexlock.h"

namespace leveldb {

const int kNumNonTableCacheFiles = 10;

// Information kept for every waiting writer
struct DBImpl::Writer {
  explicit Writer(port::Mutex* mu)
      : batch(nullptr), sync(false), done(false), cv(mu) {}

  Status status;
  WriteBatch* batch;
  bool sync;
  bool done;
  port::CondVar cv;
};

/****
 * 用于记录压缩过程信息
 */
struct DBImpl::CompactionState {
  // Files produced by compaction

  //输出文件
  struct Output {
    uint64_t number;                 // 文件编号
    uint64_t file_size;              // 文件大小
    InternalKey smallest, largest;   // key 范围
  };

  // 用于指向当前正在输出的文件信息
  Output* current_output() { return &outputs[outputs.size() - 1]; }

  explicit CompactionState(Compaction* c)
      : compaction(c),
        smallest_snapshot(0),
        outfile(nullptr),
        builder(nullptr),
        total_bytes(0) {}

  Compaction* const compaction;      // 压缩信息

  // Sequence numbers < smallest_snapshot are not significant since we
  // will never have to service a snapshot below smallest_snapshot.
  // Therefore if we have seen a sequence number S <= smallest_snapshot,
  // we can drop all entries for the same key with sequence numbers < S.
  SequenceNumber smallest_snapshot;

  std::vector<Output> outputs;      // 所有的输出文件，每次合并可能会写出多个文件

  // State kept for output being generated
  WritableFile* outfile;            // 写文件的操作句柄， 只关联到最新的output文件
  TableBuilder* builder;            // SST 构建句柄，只关联到最新的output文件

  uint64_t total_bytes;            // 此次压缩的总的压缩文件大小
};

// Fix user-supplied options to be reasonable
template <class T, class V>
static void ClipToRange(T* ptr, V minvalue, V maxvalue) {
  if (static_cast<V>(*ptr) > maxvalue) *ptr = maxvalue;
  if (static_cast<V>(*ptr) < minvalue) *ptr = minvalue;
}

/***
 * 配置调整
 * 数据库目录创建
 * 日志重命名
 * 缓存创建
 */
Options SanitizeOptions(const std::string& dbname,
                        const InternalKeyComparator* icmp,
                        const InternalFilterPolicy* ipolicy,
                        const Options& src) {
  Options result = src;
  result.comparator = icmp;
  result.filter_policy = (src.filter_policy != nullptr) ? ipolicy : nullptr;

  // 修正数据范围
  ClipToRange(&result.max_open_files, 64 + kNumNonTableCacheFiles, 50000);  // 74 ~ 50000
  ClipToRange(&result.write_buffer_size, 64 << 10, 1 << 30);                // 64KB ~ 1GB
  ClipToRange(&result.max_file_size, 1 << 20, 1 << 30);                     // 1MB ~ 1GB
  ClipToRange(&result.block_size, 1 << 10, 4 << 20);                        // 1KB ~ 4MB

  if (result.info_log == nullptr) {
    // Open a log file in the same directory as the db
    src.env->CreateDir(dbname);  // 创建数据库目录如果不存在的话
    // 更新LOG, 将之前的 LOG -> LOG.old
    // 创建新的LOG文件
    src.env->RenameFile(InfoLogFileName(dbname), OldInfoLogFileName(dbname));
    Status s = src.env->NewLogger(InfoLogFileName(dbname), &result.info_log);

    if (!s.ok()) {
      // No place suitable for logging
      result.info_log = nullptr;
    }
  }

  if (result.block_cache == nullptr) {
    // 创建8MB的缓存空间
    result.block_cache = NewLRUCache(8 << 20);
  }
  return result;
}

static int TableCacheSize(const Options& sanitized_options) {
  // Reserve ten files or so for other uses and give the rest to TableCache.
  return sanitized_options.max_open_files - kNumNonTableCacheFiles;
}

DBImpl::DBImpl(const Options& raw_options, const std::string& dbname)
    : env_(raw_options.env),
      internal_comparator_(raw_options.comparator),                         // InternelKey 比较器
      internal_filter_policy_(raw_options.filter_policy),                   // InternelKey 过滤器
      options_(SanitizeOptions(dbname, &internal_comparator_, &internal_filter_policy_, raw_options)),   // 调整参数，创建目录，开启缓存
      owns_info_log_(options_.info_log != raw_options.info_log),
      owns_cache_(options_.block_cache != raw_options.block_cache),
      dbname_(dbname),
      table_cache_(new TableCache(dbname_, options_, TableCacheSize(options_))),   // TableCache 开启
      db_lock_(nullptr),
      shutting_down_(false),
      background_work_finished_signal_(&mutex_),                                           // 等待后台线程完成信号
      mem_(nullptr),
      imm_(nullptr),
      has_imm_(false),
      logfile_(nullptr),
      logfile_number_(0),
      log_(nullptr),
      seed_(0),
      tmp_batch_(new WriteBatch),
      background_compaction_scheduled_(false),
      manual_compaction_(nullptr),
      versions_(new VersionSet(dbname_, &options_, table_cache_, &internal_comparator_)) {}

DBImpl::~DBImpl() {
  // Wait for background work to finish.
  mutex_.Lock();
  shutting_down_.store(true, std::memory_order_release);
  while (background_compaction_scheduled_) {
    background_work_finished_signal_.Wait();
  }
  mutex_.Unlock();

  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

  delete versions_;
  if (mem_ != nullptr) mem_->Unref();
  if (imm_ != nullptr) imm_->Unref();
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;

  if (owns_info_log_) {
    delete options_.info_log;
  }
  if (owns_cache_) {
    delete options_.block_cache;
  }
}

Status DBImpl::NewDB() {

  // 创建一个新的VersionEdit
  VersionEdit new_db;
<<<<<<< HEAD
=======
  new_db.SetComparatorName(user_comparator()->Name());
  new_db.SetLogNumber(0);                                 // 日志文件编号
  new_db.SetNextFile(2);                                  // 下一个文件编号
  new_db.SetLastSequence(0);
>>>>>>> c40d24552cdc7937c1d6af142fc9331699e07f54

  new_db.SetComparatorName(user_comparator()->Name());  // 设置比较器名称
  new_db.SetLogNumber(0);                                // 设置WAL日志文件编号-0
  new_db.SetNextFile(2);                                 // 设置MENIFEST文件编号-2
  new_db.SetLastSequence(0);                              // 设置lastseqence-0

  // 创建 MANIFEST-001 文件
  const std::string manifest = DescriptorFileName(dbname_, 1);

  WritableFile* file;

  Status s = env_->NewWritableFile(manifest, &file);

  if (!s.ok()) {
    return s;
  }
  // 初始化的 VersoinEdit数据落地到 MENIFEST 文件
  {
    log::Writer log(file);

    // 将VersionEdit 信息写入MENIFEST
    std::string record;
    new_db.EncodeTo(&record);
    s = log.AddRecord(record);

    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
  }

  delete file;
  if (s.ok()) {
    // Make "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dbname_, 1);
  } else {
    env_->RemoveFile(manifest);
  }
  return s;
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || options_.paranoid_checks) {
    // No change needed
  } else {
    Log(options_.info_log, "Ignoring error %s", s->ToString().c_str());
    *s = Status::OK();
  }
}

/*****
 * 清理过期文件
 *  对于 MANIFEST文件， 只保留两个版本
 *  对于 WAL 则保留MANIFEST文件中的涉及到的WAL
 *  对于 SST 则保留VersionSet中涉及到的所有的SST
 */
void DBImpl::RemoveObsoleteFiles() {

  mutex_.AssertHeld();

  if (!bg_error_.ok()) {
    // After a background error, we don't know whether a new version may
    // or may not have been committed, so we cannot safely garbage collect.
    return;
  }

  // 调用的是拷贝构造函数， 复制 pending_outputs_ 里面的sst文件编号
  std::set<uint64_t> live = pending_outputs_;

  // 将所有的version中存活过的sst插入到 live中
  versions_->AddLiveFiles(&live);

  std::vector<std::string> filenames;

  // 遍历dbname_目录下的文件与目录(仅第一层)，将文件名放置到filenames中
  env_->GetChildren(dbname_, &filenames);  // Ignoring errors on purpose

  uint64_t number;
  FileType type;
  std::vector<std::string> files_to_delete;

  // 遍历数据库下面的所有文件名
  for (std::string& filename : filenames) {

    if (ParseFileName(filename, &number, &type)) {
      bool keep = true;

      switch (type) {
        case kLogFile:   // WAL
          // 如果 WAL 日志是当前VERSION的日志文件
          // 或者 WAL 日志是上一个VERSION的日志文件
          // 则继续保持
          keep = ((number >= versions_->LogNumber()) || (number == versions_->PrevLogNumber()));
          break;
        case kDescriptorFile:
          // 如果WAL日志文件是当前VERSION的MENIFEST
          // 则继续保持
          keep = (number >= versions_->ManifestFileNumber());
          break;
        case kTableFile:
          // 如果在VersionSet所有的版本中有存在这个SST, 则不删除
          // 用于回滚?
          keep = (live.find(number) != live.end());
          break;
        case kTempFile:
          // Any temp files that are currently being written to must
          // be recorded in pending_outputs_, which is inserted into "live"
          keep = (live.find(number) != live.end());
          break;
        case kCurrentFile:
        case kDBLockFile:
        case kInfoLogFile:
          keep = true;
          break;
      }

      if (!keep) {

        // 放入删除的SST中
        files_to_delete.push_back(std::move(filename));
        if (type == kTableFile) {
          // 从TableCache中释放这个缓存(如果存在于缓存的话)
          table_cache_->Evict(number);
        }

        Log(options_.info_log, "Delete type=%d #%lld\n", static_cast<int>(type), static_cast<unsigned long long>(number));
      }
    }
  }

  mutex_.Unlock();
  // 对于要删除的文件，进行物理删除
  for (const std::string& filename : files_to_delete) {
    env_->RemoveFile(dbname_ + "/" + filename);
  }
  mutex_.Lock();
}

Status DBImpl::Recover(VersionEdit* edit, bool* save_manifest) {

  mutex_.AssertHeld();

  // Ignore error from CreateDir since the creation of the DB is
  // committed only when the descriptor is created, and this directory
  // may already exist from a previous failed creation attempt.
  env_->CreateDir(dbname_);  // 创建数据库如果不存在的情况

  assert(db_lock_ == nullptr);

<<<<<<< HEAD
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);  // 创建文件锁
=======
  Status s = env_->LockFile(LockFileName(dbname_), &db_lock_);
>>>>>>> c40d24552cdc7937c1d6af142fc9331699e07f54

  if (!s.ok()) {
    return s;
  }

  if (!env_->FileExists(CurrentFileName(dbname_))) {
<<<<<<< HEAD
    // 如果 CURRENT 文件不存在， 表示新服务，没有MANIFEST 文件

    if (options_.create_if_missing) {
      // 创建新的DB
      Log(options_.info_log, "Creating DB %s since it was missing.", dbname_.c_str());
=======

    if (options_.create_if_missing) {

      Log(options_.info_log, "Creating DB %s since it was missing.", dbname_.c_str());

      // 创建新的VersionEdit与MANIFEST文件
>>>>>>> c40d24552cdc7937c1d6af142fc9331699e07f54
      s = NewDB();

      if (!s.ok()) {
        return s;
      }
    } else {
<<<<<<< HEAD
      return Status::InvalidArgument( dbname_, "does not exist (create_if_missing is false)");
=======
      return Status::InvalidArgument(dbname_, "does not exist (create_if_missing is false)");
>>>>>>> c40d24552cdc7937c1d6af142fc9331699e07f54
    }

  } else {
    if (options_.error_if_exists) {
<<<<<<< HEAD
      return Status::InvalidArgument(dbname_, "exists (error_if_exists is true)");
=======
      return Status::InvalidArgument(dbname_,"exists (error_if_exists is true)");
>>>>>>> c40d24552cdc7937c1d6af142fc9331699e07f54
    }
  }

  // 从MANIFEST 文件中恢复到当前的状态
  s = versions_->Recover(save_manifest);

  if (!s.ok()) {
    return s;
  }
  SequenceNumber max_sequence(0);

  // Recover from all newer log files than the ones named in the
  // descriptor (new log files may have been added by the previous
  // incarnation without registering them in the descriptor).
  //
  // Note that PrevLogNumber() is no longer used, but we pay
  // attention to it in case we are recovering a database
  // produced by an older version of leveldb.
  const uint64_t min_log = versions_->LogNumber();
  const uint64_t prev_log = versions_->PrevLogNumber();
  std::vector<std::string> filenames;
  s = env_->GetChildren(dbname_, &filenames);
  if (!s.ok()) {
    return s;
  }
  std::set<uint64_t> expected;
  versions_->AddLiveFiles(&expected);
  uint64_t number;
  FileType type;
  std::vector<uint64_t> logs;
  for (size_t i = 0; i < filenames.size(); i++) {
    if (ParseFileName(filenames[i], &number, &type)) {
      expected.erase(number);
      if (type == kLogFile && ((number >= min_log) || (number == prev_log)))
        logs.push_back(number);
    }
  }
  if (!expected.empty()) {
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%d missing files; e.g.",
                  static_cast<int>(expected.size()));
    return Status::Corruption(buf, TableFileName(dbname_, *(expected.begin())));
  }

  // Recover in the order in which the logs were generated
  std::sort(logs.begin(), logs.end());
  for (size_t i = 0; i < logs.size(); i++) {
    s = RecoverLogFile(logs[i], (i == logs.size() - 1), save_manifest, edit,
                       &max_sequence);
    if (!s.ok()) {
      return s;
    }

    // The previous incarnation may not have written any MANIFEST
    // records after allocating this log number.  So we manually
    // update the file number allocation counter in VersionSet.
    versions_->MarkFileNumberUsed(logs[i]);
  }

  if (versions_->LastSequence() < max_sequence) {
    versions_->SetLastSequence(max_sequence);
  }

  return Status::OK();
}

Status DBImpl::RecoverLogFile(uint64_t log_number, bool last_log,
                              bool* save_manifest, VersionEdit* edit,
                              SequenceNumber* max_sequence) {
  struct LogReporter : public log::Reader::Reporter {
    Env* env;
    Logger* info_log;
    const char* fname;
    Status* status;  // null if options_.paranoid_checks==false
    void Corruption(size_t bytes, const Status& s) override {
      Log(info_log, "%s%s: dropping %d bytes; %s",
          (this->status == nullptr ? "(ignoring error) " : ""), fname,
          static_cast<int>(bytes), s.ToString().c_str());
      if (this->status != nullptr && this->status->ok()) *this->status = s;
    }
  };

  mutex_.AssertHeld();

  // Open the log file
  std::string fname = LogFileName(dbname_, log_number);
  SequentialFile* file;
  Status status = env_->NewSequentialFile(fname, &file);
  if (!status.ok()) {
    MaybeIgnoreError(&status);
    return status;
  }

  // Create the log reader.
  LogReporter reporter;
  reporter.env = env_;
  reporter.info_log = options_.info_log;
  reporter.fname = fname.c_str();
  reporter.status = (options_.paranoid_checks ? &status : nullptr);
  // We intentionally make log::Reader do checksumming even if
  // paranoid_checks==false so that corruptions cause entire commits
  // to be skipped instead of propagating bad information (like overly
  // large sequence numbers).
  log::Reader reader(file, &reporter, true /*checksum*/, 0 /*initial_offset*/);
  Log(options_.info_log, "Recovering log #%llu",
      (unsigned long long)log_number);

  // Read all the records and add to a memtable
  std::string scratch;
  Slice record;
  WriteBatch batch;
  int compactions = 0;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &scratch) && status.ok()) {
    if (record.size() < 12) {
      reporter.Corruption(record.size(),
                          Status::Corruption("log record too small"));
      continue;
    }
    WriteBatchInternal::SetContents(&batch, record);

    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    status = WriteBatchInternal::InsertInto(&batch, mem);
    MaybeIgnoreError(&status);
    if (!status.ok()) {
      break;
    }
    const SequenceNumber last_seq = WriteBatchInternal::Sequence(&batch) +
                                    WriteBatchInternal::Count(&batch) - 1;
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateMemoryUsage() > options_.write_buffer_size) {
      compactions++;
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
      mem->Unref();
      mem = nullptr;
      if (!status.ok()) {
        // Reflect errors immediately so that conditions like full
        // file-systems cause the DB::Open() to fail.
        break;
      }
    }
  }

  delete file;

  // See if we should keep reusing the last log file.
  if (status.ok() && options_.reuse_logs && last_log && compactions == 0) {
    assert(logfile_ == nullptr);
    assert(log_ == nullptr);
    assert(mem_ == nullptr);
    uint64_t lfile_size;
    if (env_->GetFileSize(fname, &lfile_size).ok() &&
        env_->NewAppendableFile(fname, &logfile_).ok()) {
      Log(options_.info_log, "Reusing old log %s \n", fname.c_str());
      log_ = new log::Writer(logfile_, lfile_size);
      logfile_number_ = log_number;
      if (mem != nullptr) {
        mem_ = mem;
        mem = nullptr;
      } else {
        // mem can be nullptr if lognum exists but was empty.
        mem_ = new MemTable(internal_comparator_);
        mem_->Ref();
      }
    }
  }

  // 进行MemTable 刷盘操作
  if (mem != nullptr) {
    if (status.ok()) {
      *save_manifest = true;
      status = WriteLevel0Table(mem, edit, nullptr);
    }
    mem->Unref();
  }

  return status;
}

/***
 * IMemTable写到level0层SST
 * 过程就是调用基于MemTable迭代器，使用TableBuild构建SST，
 * 然后计算得到合适的level
 * 将新的SST变更填充到VersionEdit中
 *
 * @param mem  要被刷盘的 SST
 * @param edit 用于记录基于当前版本的变更项
 * @param base 当前版本链接
 */
Status DBImpl::WriteLevel0Table(MemTable* mem, VersionEdit* edit,
                                Version* base) {
  mutex_.AssertHeld();

  const uint64_t start_micros = env_->NowMicros();  // 获取开始写盘的微秒数

  FileMetaData meta;

  meta.number = versions_->NewFileNumber();         // 拿到一个新的文件编号用于写SST

  pending_outputs_.insert(meta.number);             // 将文件编号压入等待队列

  Iterator* iter = mem->NewIterator();               // 获取 memtable 的迭代器，用于遍历 kv

  Log(options_.info_log, "Level-0 table #%llu: started", (unsigned long long)meta.number);

  Status s;
  {
    mutex_.Unlock();
    // MemTable 数据落盘， 落盘过程中解锁， 提高系统效率
    s = BuildTable(dbname_, env_, options_, table_cache_, iter, &meta);
    mutex_.Lock();
  }

  // 记录落地消息
  Log(options_.info_log, "Level-0 table #%llu: %lld bytes %s",
      (unsigned long long)meta.number, (unsigned long long)meta.file_size, s.ToString().c_str());

  // 释放这个迭代器
  delete iter;

  // 从 Pending_outputs 中移出
  pending_outputs_.erase(meta.number);

  // Note that if file_size is zero, the file has been deleted and
  // should not be added to the manifest.
  int level = 0;

  if (s.ok() && meta.file_size > 0) {
    const Slice min_user_key = meta.smallest.user_key();
    const Slice max_user_key = meta.largest.user_key();

    if (base != nullptr) {
      // 判断 SST 落地到第几层合适
      level = base->PickLevelForMemTableOutput(min_user_key, max_user_key);
    }

    // 在 VersionEdit 中添加该SST到指定的level中
    edit->AddFile(level, meta.number, meta.file_size, meta.smallest, meta.largest);
  }

  // 记录写的耗时与文件大小
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros;
  stats.bytes_written = meta.file_size;
  stats_[level].Add(stats);
  return s;
}

/****
 * 主要功能是 IMemTable 刷盘操作
 *
 * 首先对于要刷盘的MemTable, 调用WriteLevel0Table进行构造SST刷盘
 *    - 这里注意的是不一定会溢写到level0层
 *    - 他会争取写到更高层，在条件允许的情况下
 *    - 将新增SST的记录填充到VersionEdit
 *
 * 然后基于新的VersionEdit合并形成新的Version
 * 最后对历史数据做出清理动作
 */
void DBImpl::CompactMemTable() {
  mutex_.AssertHeld();

  assert(imm_ != nullptr);

  // Save the contents of the memtable as a new Table
  VersionEdit edit;
  Version* base = versions_->current();  // 获得当前的版本
  base->Ref();


  Status s = WriteLevel0Table(imm_, &edit, base);  // 对 IMemTable 进行刷盘操作
  base->Unref();

  if (s.ok() && shutting_down_.load(std::memory_order_acquire)) {
    s = Status::IOError("Deleting DB during memtable compaction");
  }

  // Replace immutable memtable with the generated Table
  if (s.ok()) {
    edit.SetPrevLogNumber(0);
    edit.SetLogNumber(logfile_number_);         // 设置当前的WAL日志编号
    s = versions_->LogAndApply(&edit, &mutex_); // VersionSet 合并当前的VersionEdit添加的SST形成新的Version
  }

  if (s.ok()) {
    // 清理 IMemTalbe
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    RemoveObsoleteFiles();    // 清理那些无用的本地文件

  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::CompactRange(const Slice* begin, const Slice* end) {
  int max_level_with_files = 1;
  {
    MutexLock l(&mutex_);
    Version* base = versions_->current();
    for (int level = 1; level < config::kNumLevels; level++) {
      if (base->OverlapInLevel(level, begin, end)) {
        max_level_with_files = level;
      }
    }
  }
  TEST_CompactMemTable();  // TODO(sanjay): Skip if memtable does not overlap
  for (int level = 0; level < max_level_with_files; level++) {
    TEST_CompactRange(level, begin, end);
  }
}

void DBImpl::TEST_CompactRange(int level, const Slice* begin,
                               const Slice* end) {
  assert(level >= 0);
  assert(level + 1 < config::kNumLevels);

  InternalKey begin_storage, end_storage;

  ManualCompaction manual;
  manual.level = level;
  manual.done = false;
  if (begin == nullptr) {
    manual.begin = nullptr;
  } else {
    begin_storage = InternalKey(*begin, kMaxSequenceNumber, kValueTypeForSeek);
    manual.begin = &begin_storage;
  }
  if (end == nullptr) {
    manual.end = nullptr;
  } else {
    end_storage = InternalKey(*end, 0, static_cast<ValueType>(0));
    manual.end = &end_storage;
  }

  MutexLock l(&mutex_);
  while (!manual.done && !shutting_down_.load(std::memory_order_acquire) &&
         bg_error_.ok()) {
    if (manual_compaction_ == nullptr) {  // Idle
      manual_compaction_ = &manual;
      MaybeScheduleCompaction();
    } else {  // Running either my compaction or another compaction.
      background_work_finished_signal_.Wait();
    }
  }
  if (manual_compaction_ == &manual) {
    // Cancel my manual compaction since we aborted early for some reason.
    manual_compaction_ = nullptr;
  }
}

Status DBImpl::TEST_CompactMemTable() {
  // nullptr batch means just wait for earlier writes to be done
  Status s = Write(WriteOptions(), nullptr);
  if (s.ok()) {
    // Wait until the compaction completes
    MutexLock l(&mutex_);
    while (imm_ != nullptr && bg_error_.ok()) {
      background_work_finished_signal_.Wait();
    }
    if (imm_ != nullptr) {
      s = bg_error_;
    }
  }
  return s;
}

void DBImpl::RecordBackgroundError(const Status& s) {
  mutex_.AssertHeld();
  if (bg_error_.ok()) {
    bg_error_ = s;
    // 唤醒等待线程
    background_work_finished_signal_.SignalAll();
  }
}

/****
 * 开启后台压缩，需要注意的是 leveldb 只有一个压缩线程
 */
void DBImpl:: MaybeScheduleCompaction() {

  mutex_.AssertHeld();

  if (background_compaction_scheduled_) {
    // 表示后台已经有一个现成在调度
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // DB 开始关闭

  } else if (!bg_error_.ok()) {
    // Already got an error; no more changes
  } else if (imm_ == nullptr && manual_compaction_ == nullptr && !versions_->NeedsCompaction()) {
    // 没有需要刷写的 IMemTable
  } else {
    background_compaction_scheduled_ = true;
    env_->Schedule(&DBImpl::BGWork, this);
  }
}


void DBImpl::BGWork(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCall();
}


void DBImpl::BackgroundCall() {
  MutexLock l(&mutex_);          // 获得锁

  assert(background_compaction_scheduled_);

  if (shutting_down_.load(std::memory_order_acquire)) {
    // No more background work when shutting down.
  } else if (!bg_error_.ok()) {
    // No more background work after a background error.
  } else {
    BackgroundCompaction();
  }

  background_compaction_scheduled_ = false;

  // 以前的压缩可能在一个级别中生成了太多文件，因此如果需要，请重新安排另一次压缩。
  MaybeScheduleCompaction();
  background_work_finished_signal_.SignalAll();
}

/****
 * 基于线程异步执行的压缩过程
 * 首先会进行Minor Compaction过程，也就是MemTable刷盘过程
 *    - 过程调用的是 CompactMemTable()
 * 然后进行Major Compaction过程，合并SST
 *    - 如果有用户手动触发压缩行为， 则基于压缩行为调用 VersionSet::CompactRange() 方法
 *    - 否则调用 VersionSet::PickCompaction() 计算有没有达到压缩的阈值
 *
 * 对于Major压缩过程，如果能直接移动， 则直接修改元信息移动
 * 否则调用DoCompactionWork进行压缩。
 */
void DBImpl::BackgroundCompaction() {

  mutex_.AssertHeld();

  // IMemTable刷盘 -------------  Minor Compaction
  if (imm_ != nullptr) {
    CompactMemTable();
    return;
  }


  // Major Compaction
  Compaction* c;
  bool is_manual = (manual_compaction_ != nullptr);  // 存在压缩行为
  InternalKey manual_end;

  if (is_manual) {
    ManualCompaction* m = manual_compaction_;

    // 计算影响到的压缩的SST(对于level层与level+1层)
    c = versions_->CompactRange(m->level, m->begin, m->end);

    m->done = (c == nullptr);
    if (c != nullptr) {
      manual_end = c->input(0, c->num_input_files(0) - 1)->largest;   // inputs_0 层最后一个SST的最大key
    }

    Log(options_.info_log,
        "Manual compaction at level-%d from %s .. %s; will stop at %s\n",
        m->level, (m->begin ? m->begin->DebugString().c_str() : "(begin)"),
        (m->end ? m->end->DebugString().c_str() : "(end)"),
        (m->done ? "(end)" : manual_end.DebugString().c_str()));
  } else {
    // 判断是否达到了触发压缩的阈值
    // 第一种条件是某一个level层的数据量过大
    // 第二种条件是某一个sst的查询次数过多
    c = versions_->PickCompaction();
  }

  Status status;
  if (c == nullptr) {
    // Nothing to do
  } else if (!is_manual && c->IsTrivialMove()) {
    // 这个表示SST达到了压缩阈值， 并且此次压缩可以直接将需要压缩的level层SST直接移动到level+1层

    assert(c->num_input_files(0) == 1);  // 校验是否只有一个文件

    FileMetaData* f = c->input(0, 0);  // 拿到文件

    // 将SST从level层直接移动到level+1层
    c->edit()->RemoveFile(c->level(), f->number);
    c->edit()->AddFile(c->level() + 1, f->number, f->file_size, f->smallest, f->largest);

    // 更新一个版本
    status = versions_->LogAndApply(c->edit(), &mutex_);

    if (!status.ok()) {
      RecordBackgroundError(status);
    }
    VersionSet::LevelSummaryStorage tmp;
    Log(options_.info_log, "Moved #%lld to level-%d %lld bytes %s: %s\n",
        static_cast<unsigned long long>(f->number), c->level() + 1,
        static_cast<unsigned long long>(f->file_size),
        status.ToString().c_str(), versions_->LevelSummary(&tmp));
  } else {

    // 如果不能移动的， 开始压缩操作， 调用 CompactionState 生成新的压缩文件
    CompactionState* compact = new CompactionState(c);
    status = DoCompactionWork(compact);
    if (!status.ok()) {
      RecordBackgroundError(status);
    }

    // 进行清理操作
    CleanupCompaction(compact);
    c->ReleaseInputs();
    RemoveObsoleteFiles();
  }
  delete c;

  if (status.ok()) {
    // Done
  } else if (shutting_down_.load(std::memory_order_acquire)) {
    // Ignore compaction errors found during shutting down
  } else {
    Log(options_.info_log, "Compaction error: %s", status.ToString().c_str());
  }

  // 返回手动压缩的结束状态
  if (is_manual) {
    ManualCompaction* m = manual_compaction_;
    if (!status.ok()) {
      m->done = true;
    }
    if (!m->done) {
      // We only compacted part of the requested range.  Update *m
      // to the range that is left to be compacted.
      m->tmp_storage = manual_end;
      m->begin = &m->tmp_storage;
    }
    manual_compaction_ = nullptr;
  }
}

/*****
 * 做压缩清理动作
 * 释放掉没有释放掉的句柄
 *
 */
void DBImpl::CleanupCompaction(CompactionState* compact) {
  mutex_.AssertHeld();
  if (compact->builder != nullptr) {
    // May happen if we get a shutdown call in the middle of compaction
    compact->builder->Abandon();
    delete compact->builder;
  } else {
    assert(compact->outfile == nullptr);
  }
  delete compact->outfile;
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    pending_outputs_.erase(out.number);
  }
  delete compact;
}


/***
 * 构建用于写出压缩数据的输出文件句柄
 * TableBuilder
 */
Status DBImpl::OpenCompactionOutputFile(CompactionState* compact) {

  assert(compact != nullptr);
  assert(compact->builder == nullptr);

  // 拿到写入的文件编号
  uint64_t file_number;
  {
    mutex_.Lock();
    file_number = versions_->NewFileNumber();
    pending_outputs_.insert(file_number);
    CompactionState::Output out;
    out.number = file_number;
    out.smallest.Clear();
    out.largest.Clear();
    compact->outputs.push_back(out);
    mutex_.Unlock();
  }

  // 构建输出文件
  std::string fname = TableFileName(dbname_, file_number);
  Status s = env_->NewWritableFile(fname, &compact->outfile);
  if (s.ok()) {
    compact->builder = new TableBuilder(options_, compact->outfile);
  }
  return s;
}

/****
 * SST compaction 停止写出时的操作，可能发生在写过程文件数据量过大，或者已经完成compaction
 * 过程主要是完善CompactionState元信息
 *  TableBuilder::Finish() 完成SST构建
 *  数据落盘
 *  关闭文件句柄
 *  校验SST
 */
Status DBImpl::FinishCompactionOutputFile(CompactionState* compact, Iterator* input) {


  assert(compact != nullptr);
  assert(compact->outfile != nullptr);
  assert(compact->builder != nullptr);

  const uint64_t output_number = compact->current_output()->number;      // 当前文件编号
  assert(output_number != 0);

  // Check for iterator errors
  Status s = input->status();
  const uint64_t current_entries = compact->builder->NumEntries();       // 当前记录数

  if (s.ok()) {
    s = compact->builder->Finish();                                      // 完成数据写入
  } else {
    compact->builder->Abandon();
  }
  const uint64_t current_bytes = compact->builder->FileSize();
  compact->current_output()->file_size = current_bytes;                   // 记录当前文件字节数
  compact->total_bytes += current_bytes;

  delete compact->builder;
  compact->builder = nullptr;

  // 数据落盘
  if (s.ok()) {
    s = compact->outfile->Sync();
  }
  // 关闭文件
  if (s.ok()) {
    s = compact->outfile->Close();
  }

  delete compact->outfile;
  compact->outfile = nullptr;

  if (s.ok() && current_entries > 0) {
    // 校验 SST 的文件写入是否正确
    Iterator* iter = table_cache_->NewIterator(ReadOptions(), output_number, current_bytes);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(options_.info_log, "Generated table #%llu@%d: %lld keys, %lld bytes",
          (unsigned long long)output_number, compact->compaction->level(),
          (unsigned long long)current_entries,
          (unsigned long long)current_bytes);
    }
  }

  return s;
}

/***
 * 完成compation后，更新Version操作
 */
Status DBImpl::InstallCompactionResults(CompactionState* compact) {
  mutex_.AssertHeld();
  Log(options_.info_log, "Compacted %d@%d + %d@%d files => %lld bytes",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1,
      static_cast<long long>(compact->total_bytes));

  // 删除压缩记录中的SST
  compact->compaction->AddInputDeletions(compact->compaction->edit());

  const int level = compact->compaction->level();

  // 将新增的文件添加到level+1层
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    const CompactionState::Output& out = compact->outputs[i];
    compact->compaction->edit()->AddFile(level + 1, out.number, out.file_size, out.smallest, out.largest);
  }

  // 应用到新的版本中
  return versions_->LogAndApply(compact->compaction->edit(), &mutex_);
}

/****
 * 执行压缩过程
 * 过程中会合并掉重复的user_key
 * 在迭代压缩的kv过程中， 如果MemTable满数据， 还是会优先触发Memtable合并
 * 如果写的文件大小超过阈值(默认2M), 会关掉写出的SST, 重新开一个新的SST
 * 所以每次合并可能会写出多个SST文件
 *
 * ??? snapshots_ 的设置
 * ??? seqnumber 怎么写的
 */
Status DBImpl::DoCompactionWork(CompactionState* compact) {
  const uint64_t start_micros = env_->NowMicros();
  int64_t imm_micros = 0;  // Micros spent doing imm_ compactions

  Log(options_.info_log, "Compacting %d@%d + %d@%d files",
      compact->compaction->num_input_files(0), compact->compaction->level(),
      compact->compaction->num_input_files(1), compact->compaction->level() + 1);

  assert(versions_->NumLevelFiles(compact->compaction->level()) > 0);
  assert(compact->builder == nullptr);
  assert(compact->outfile == nullptr);

  // snapshots 的作用？
  if (snapshots_.empty()) {
    compact->smallest_snapshot = versions_->LastSequence();
  } else {
    compact->smallest_snapshot = snapshots_.oldest()->sequence_number();
  }

  // 拿到待压缩对象的SST
  Iterator* input = versions_->MakeInputIterator(compact->compaction);

  // Release mutex while we're actually doing the compaction work
  mutex_.Unlock();

  // 初始化迭代器
  input->SeekToFirst();



  Status status;
  ParsedInternalKey ikey;

  // 以下三个变量用于记录当前的user_key
  std::string current_user_key;
  bool has_current_user_key = false;
  SequenceNumber last_sequence_for_key = kMaxSequenceNumber;

  while (input->Valid() && !shutting_down_.load(std::memory_order_acquire)) {

    // 有待压缩的 MemTable, 先进行压缩
    if (has_imm_.load(std::memory_order_relaxed)) {
      const uint64_t imm_start = env_->NowMicros();
      mutex_.Lock();


      if (imm_ != nullptr) {
        CompactMemTable();
        background_work_finished_signal_.SignalAll();
      }

      mutex_.Unlock();
      imm_micros += (env_->NowMicros() - imm_start);
    }

    Slice key = input->key();

    // 如果合并后的key与grandparent重叠的字节大于阈值
    // 此时需要开启新的sst文件
    if (compact->compaction->ShouldStopBefore(key) &&
        compact->builder != nullptr) {
      status = FinishCompactionOutputFile(compact, input);
      if (!status.ok()) {
        break;
      }
    }

    // Handle key/value, add to state, etc.
    bool drop = false;

    if (!ParseInternalKey(key, &ikey)) {
      // 可以 error
      current_user_key.clear();
      has_current_user_key = false;
      last_sequence_for_key = kMaxSequenceNumber;
    } else {
      if (!has_current_user_key ||
          user_comparator()->Compare(ikey.user_key, Slice(current_user_key)) != 0) {
        // First occurrence of this user key
        current_user_key.assign(ikey.user_key.data(), ikey.user_key.size());  //设置当前处理的user key
        has_current_user_key = true;
        last_sequence_for_key = kMaxSequenceNumber;    // 对于新的出现的user_key不能被删除， 所以设置 sequence_num 为最大值
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        // Hidden by an newer entry for same user key
        drop = true;  // (A)
      } else if (ikey.type == kTypeDeletion &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 compact->compaction->IsBaseLevelForKey(ikey.user_key)) {
        // 1. 该key被标记为删除
        // 2. 过于古老
        // 3. 更高层已经没有该key了
        // For this user key:
        // (1) there is no data in higher levels
        // (2) data in lower levels will have larger sequence numbers
        // (3) data in layers that are being compacted here and have
        //     smaller sequence numbers will be dropped in the next
        //     few iterations of this loop (by rule (A) above).
        // Therefore this deletion marker is obsolete and can be dropped.
        drop = true;
      }

      last_sequence_for_key = ikey.sequence;
    }
#if 0
    Log(options_.info_log,
        "  Compact: %s, seq %d, type: %d %d, drop: %d, is_base: %d, "
        "%d smallest_snapshot: %d",
        ikey.user_key.ToString().c_str(),
        (int)ikey.sequence, ikey.type, kTypeValue, drop,
        compact->compaction->IsBaseLevelForKey(ikey.user_key),
        (int)last_sequence_for_key, (int)compact->smallest_snapshot);
#endif

    if (!drop) {
      // Open output file if necessary
      if (compact->builder == nullptr) {
        // 构建一个 TableBuilder, 用于写出压缩SST
        status = OpenCompactionOutputFile(compact);
        if (!status.ok()) {
          break;
        }
      }

      // 记录当前写出文件的最小user_key
      if (compact->builder->NumEntries() == 0) {
        compact->current_output()->smallest.DecodeFrom(key);
      }

      // 用替代操作，记录当前文件的最大的user_key
      compact->current_output()->largest.DecodeFrom(key);

      // 将当前的key添加到输出文件中
      compact->builder->Add(key, input->value());

      // 如果文件大小超过设置文件上限(默认2M)则停止写出
      // 并对写出文件做最后整理关闭工作
      if (compact->builder->FileSize() >= compact->compaction->MaxOutputFileSize()) {
        status = FinishCompactionOutputFile(compact, input);
        if (!status.ok()) {
          break;
        }
      }
    }

    input->Next();
  }

  if (status.ok() && shutting_down_.load(std::memory_order_acquire)) {
    status = Status::IOError("Deleting DB during compaction");
  }

  if (status.ok() && compact->builder != nullptr) {
    status = FinishCompactionOutputFile(compact, input);
  }
  if (status.ok()) {
    status = input->status();
  }
  delete input;
  input = nullptr;

  // 完成压缩

  // 统计压缩时间
  CompactionStats stats;
  stats.micros = env_->NowMicros() - start_micros - imm_micros;

  // 压缩前文件量
  for (int which = 0; which < 2; which++) {
    for (int i = 0; i < compact->compaction->num_input_files(which); i++) {
      stats.bytes_read += compact->compaction->input(which, i)->file_size;
    }
  }

  // 压缩后文件量
  for (size_t i = 0; i < compact->outputs.size(); i++) {
    stats.bytes_written += compact->outputs[i].file_size;
  }

  mutex_.Lock();

  // 追加压缩信息
  stats_[compact->compaction->level() + 1].Add(stats);

  if (status.ok()) {
    // 更新Version信息， 删掉合并的SST, 增加新的SST到level+1层
    status = InstallCompactionResults(compact);
  }

  if (!status.ok()) {
    RecordBackgroundError(status);
  }

  VersionSet::LevelSummaryStorage tmp;
  Log(options_.info_log, "compacted to: %s", versions_->LevelSummary(&tmp));
  return status;
}

namespace {

struct IterState {
  port::Mutex* const mu;
  Version* const version GUARDED_BY(mu);
  MemTable* const mem GUARDED_BY(mu);
  MemTable* const imm GUARDED_BY(mu);

  IterState(port::Mutex* mutex, MemTable* mem, MemTable* imm, Version* version)
      : mu(mutex), version(version), mem(mem), imm(imm) {}
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  state->mu->Lock();
  state->mem->Unref();
  if (state->imm != nullptr) state->imm->Unref();
  state->version->Unref();
  state->mu->Unlock();
  delete state;
}

}  // anonymous namespace

Iterator* DBImpl::NewInternalIterator(const ReadOptions& options,
                                      SequenceNumber* latest_snapshot,
                                      uint32_t* seed) {
  mutex_.Lock();
  *latest_snapshot = versions_->LastSequence();

  // Collect together all needed child iterators
  std::vector<Iterator*> list;
  list.push_back(mem_->NewIterator());
  mem_->Ref();
  if (imm_ != nullptr) {
    list.push_back(imm_->NewIterator());
    imm_->Ref();
  }
  versions_->current()->AddIterators(options, &list);
  Iterator* internal_iter =
      NewMergingIterator(&internal_comparator_, &list[0], list.size());
  versions_->current()->Ref();

  IterState* cleanup = new IterState(&mutex_, mem_, imm_, versions_->current());
  internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, nullptr);

  *seed = ++seed_;
  mutex_.Unlock();
  return internal_iter;
}

Iterator* DBImpl::TEST_NewInternalIterator() {
  SequenceNumber ignored;
  uint32_t ignored_seed;
  return NewInternalIterator(ReadOptions(), &ignored, &ignored_seed);
}

int64_t DBImpl::TEST_MaxNextLevelOverlappingBytes() {
  MutexLock l(&mutex_);
  return versions_->MaxNextLevelOverlappingBytes();
}

Status DBImpl::Get(const ReadOptions& options, const Slice& key, std::string* value) {
  Status s;
  MutexLock l(&mutex_);
  SequenceNumber snapshot;
  if (options.snapshot != nullptr) {
    snapshot =
        static_cast<const SnapshotImpl*>(options.snapshot)->sequence_number();
  } else {
    snapshot = versions_->LastSequence();
  }

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = versions_->current();
  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stat_update = false;
  Version::GetStats stats;

  // Unlock while reading from files and memtables
  {
    mutex_.Unlock();
    // First look in the memtable, then in the immutable memtable (if any).
    LookupKey lkey(key, snapshot);
    if (mem->Get(lkey, value, &s)) {
      // Done
    } else if (imm != nullptr && imm->Get(lkey, value, &s)) {
      // Done
    } else {
      s = current->Get(options, lkey, value, &stats);
      have_stat_update = true;
    }
    mutex_.Lock();
  }

  if (have_stat_update && current->UpdateStats(stats)) {
    MaybeScheduleCompaction();
  }
  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return s;
}

Iterator* DBImpl::NewIterator(const ReadOptions& options) {
  SequenceNumber latest_snapshot;
  uint32_t seed;
  Iterator* iter = NewInternalIterator(options, &latest_snapshot, &seed);
  return NewDBIterator(this, user_comparator(), iter,
                       (options.snapshot != nullptr
                            ? static_cast<const SnapshotImpl*>(options.snapshot)
                                  ->sequence_number()
                            : latest_snapshot),
                       seed);
}

void DBImpl::RecordReadSample(Slice key) {
  MutexLock l(&mutex_);
  if (versions_->current()->RecordReadSample(key)) {
    MaybeScheduleCompaction();
  }
}

const Snapshot* DBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  return snapshots_.New(versions_->LastSequence());
}

void DBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  MutexLock l(&mutex_);
  snapshots_.Delete(static_cast<const SnapshotImpl*>(snapshot));
}

// Convenience methods
Status DBImpl::Put(const WriteOptions& o, const Slice& key, const Slice& val) {
  return DB::Put(o, key, val);
}

Status DBImpl::Delete(const WriteOptions& options, const Slice& key) {
  return DB::Delete(options, key);
}

Status DBImpl::Write(const WriteOptions& options, WriteBatch* updates) {
  Writer w(&mutex_);

  w.batch = updates;
  w.sync = options.sync;
  w.done = false;

  MutexLock l(&mutex_);
  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // May temporarily unlock and wait.
  Status status = MakeRoomForWrite(updates == nullptr);
  uint64_t last_sequence = versions_->LastSequence();
  Writer* last_writer = &w;
  if (status.ok() && updates != nullptr) {  // nullptr batch is for compactions
    WriteBatch* write_batch = BuildBatchGroup(&last_writer);
    WriteBatchInternal::SetSequence(write_batch, last_sequence + 1);
    last_sequence += WriteBatchInternal::Count(write_batch);

    // Add to log and apply to memtable.  We can release the lock
    // during this phase since &w is currently responsible for logging
    // and protects against concurrent loggers and concurrent writes
    // into mem_.
    {
      mutex_.Unlock();
      status = log_->AddRecord(WriteBatchInternal::Contents(write_batch));
      bool sync_error = false;
      if (status.ok() && options.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchInternal::InsertInto(write_batch, mem_);
      }
      mutex_.Lock();
      if (sync_error) {
        // The state of the log file is indeterminate: the log record we
        // just added may or may not show up when the DB is re-opened.
        // So we force the DB into a mode where all future writes fail.
        RecordBackgroundError(status);
      }
    }
    if (write_batch == tmp_batch_) tmp_batch_->Clear();

    versions_->SetLastSequence(last_sequence);
  }

  while (true) {
    Writer* ready = writers_.front();
    writers_.pop_front();
    if (ready != &w) {
      ready->status = status;
      ready->done = true;
      ready->cv.Signal();
    }
    if (ready == last_writer) break;
  }

  // Notify new head of write queue
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }

  return status;
}

// REQUIRES: Writer list must be non-empty
// REQUIRES: First writer must have a non-null batch
WriteBatch* DBImpl::BuildBatchGroup(Writer** last_writer) {
  mutex_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* result = first->batch;
  assert(result != nullptr);

  size_t size = WriteBatchInternal::ByteSize(first->batch);

  // Allow the group to grow up to a maximum size, but if the
  // original write is small, limit the growth so we do not slow
  // down the small write too much.
  size_t max_size = 1 << 20;
  if (size <= (128 << 10)) {
    max_size = size + (128 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;  // Advance past "first"
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      // Do not include a sync write into a batch handled by a non-sync write.
      break;
    }

    if (w->batch != nullptr) {
      size += WriteBatchInternal::ByteSize(w->batch);
      if (size > max_size) {
        // Do not make batch too big
        break;
      }

      // Append to *result
      if (result == first->batch) {
        // Switch to temporary batch instead of disturbing caller's batch
        result = tmp_batch_;
        assert(WriteBatchInternal::Count(result) == 0);
        WriteBatchInternal::Append(result, first->batch);
      }
      WriteBatchInternal::Append(result, w->batch);
    }
    *last_writer = w;
  }
  return result;
}

// REQUIRES: mutex_ is held
// REQUIRES: this thread is currently at the front of the writer queue
Status DBImpl::MakeRoomForWrite(bool force) {
  mutex_.AssertHeld();

  assert(!writers_.empty());

  // 是否允许延迟
  bool allow_delay = !force;
  Status s;

  while (true) {
    // 如果有错误，则直接推出
    if (!bg_error_.ok()) {
      s = bg_error_;
      break;

    } else if (allow_delay && versions_->NumLevelFiles(0) >= config::kL0_SlowdownWritesTrigger) {
      // 当我们允许延迟，并且sst第0层文件数量超过软限制，
      // 我们将延迟1ms时间单位
      // 此外，这种延迟会将一些CPU移交给压缩线程，以防它与写入程序共享同一个内核。
      // 只能等1次

      mutex_.Unlock();
      env_->SleepForMicroseconds(1000);
      allow_delay = false;  // Do not delay a single write more than once
      mutex_.Lock();
    } else if (!force && (mem_->ApproximateMemoryUsage() <= options_.write_buffer_size)) {
      // 表示如果允许延迟，并且 memtable 使用的内存没有超过限制
      // 可以继续写入
      break;
    } else if (imm_ != nullptr) {
      // 如果已经有一个monior压缩的线程了，此时就不能在启用一个了
      // 需要等待上一个结束

      Log(options_.info_log, "Current memtable full; waiting...\n");
      background_work_finished_signal_.Wait();
    } else if (versions_->NumLevelFiles(0) >= config::kL0_StopWritesTrigger) {
      // 如果SST超过了硬限制， 此时不能在写入， 需要等待压缩

      Log(options_.info_log, "Too many L0 files; waiting...\n");
      background_work_finished_signal_.Wait();
    } else {

      assert(versions_->PrevLogNumber() == 0);

      uint64_t new_log_number = versions_->NewFileNumber();                            // 拿到新的日志编号， 当前verionset 日志编号+1
      WritableFile* lfile = nullptr;                                                   // {new_log_number}.log 操作句柄
      s = env_->NewWritableFile(LogFileName(dbname_, new_log_number), &lfile);  // 创建日志文件 {new_log_number}.log

      if (!s.ok()) {
        // 创建日志文件失败时， versionset 日志编号回滚
        versions_->ReuseFileNumber(new_log_number);
        break;
      }

      delete log_;
      delete logfile_;

      logfile_ = lfile;                     // 设置指向最新的日志句柄
      logfile_number_ = new_log_number;     // 日志文件设置
      log_ = new log::Writer(lfile);        // WAL 操作句柄

      imm_ = mem_;                          // 将当前的 MemTable -> IMemTable 用于刷盘
      has_imm_.store(true, std::memory_order_release); // imm 状态设置存在刷盘文件

      mem_ = new MemTable(internal_comparator_);  // 重新构建一个 MemTable 用于用户数据写入
      mem_->Ref();
      force = false;  // Do not force another compaction if have room

      // 开始压缩
      MaybeScheduleCompaction();
    }
  }
  return s;
}

bool DBImpl::GetProperty(const Slice& property, std::string* value) {
  value->clear();

  MutexLock l(&mutex_);
  Slice in = property;
  Slice prefix("leveldb.");
  if (!in.starts_with(prefix)) return false;
  in.remove_prefix(prefix.size());

  if (in.starts_with("num-files-at-level")) {
    in.remove_prefix(strlen("num-files-at-level"));
    uint64_t level;
    bool ok = ConsumeDecimalNumber(&in, &level) && in.empty();
    if (!ok || level >= config::kNumLevels) {
      return false;
    } else {
      char buf[100];
      std::snprintf(buf, sizeof(buf), "%d",
                    versions_->NumLevelFiles(static_cast<int>(level)));
      *value = buf;
      return true;
    }
  } else if (in == "stats") {
    char buf[200];
    std::snprintf(buf, sizeof(buf),
                  "                               Compactions\n"
                  "Level  Files Size(MB) Time(sec) Read(MB) Write(MB)\n"
                  "--------------------------------------------------\n");
    value->append(buf);
    for (int level = 0; level < config::kNumLevels; level++) {
      int files = versions_->NumLevelFiles(level);
      if (stats_[level].micros > 0 || files > 0) {
        std::snprintf(buf, sizeof(buf), "%3d %8d %8.0f %9.0f %8.0f %9.0f\n",
                      level, files, versions_->NumLevelBytes(level) / 1048576.0,
                      stats_[level].micros / 1e6,
                      stats_[level].bytes_read / 1048576.0,
                      stats_[level].bytes_written / 1048576.0);
        value->append(buf);
      }
    }
    return true;
  } else if (in == "sstables") {
    *value = versions_->current()->DebugString();
    return true;
  } else if (in == "approximate-memory-usage") {
    size_t total_usage = options_.block_cache->TotalCharge();
    if (mem_) {
      total_usage += mem_->ApproximateMemoryUsage();
    }
    if (imm_) {
      total_usage += imm_->ApproximateMemoryUsage();
    }
    char buf[50];
    std::snprintf(buf, sizeof(buf), "%llu",
                  static_cast<unsigned long long>(total_usage));
    value->append(buf);
    return true;
  }

  return false;
}

void DBImpl::GetApproximateSizes(const Range* range, int n, uint64_t* sizes) {
  // TODO(opt): better implementation
  MutexLock l(&mutex_);
  Version* v = versions_->current();
  v->Ref();

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    uint64_t start = versions_->ApproximateOffsetOf(v, k1);
    uint64_t limit = versions_->ApproximateOffsetOf(v, k2);
    sizes[i] = (limit >= start ? limit - start : 0);
  }

  v->Unref();
}

// Default implementations of convenience methods that subclasses of DB
// can call if they wish
Status DB::Put(const WriteOptions& opt, const Slice& key, const Slice& value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(opt, &batch);
}

Status DB::Delete(const WriteOptions& opt, const Slice& key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(opt, &batch);
}

DB::~DB() = default;

/****
 * 开启一个leveldb数据库
 */
Status DB::Open(const Options& options, const std::string& dbname, DB** dbptr) {

  *dbptr = nullptr;

  // 申请DB操作句柄
  DBImpl* impl = new DBImpl(options, dbname);

  impl->mutex_.Lock();

  VersionEdit edit;
  // Recover handles create_if_missing, error_if_exists
  bool save_manifest = false;

  Status s = impl->Recover(&edit, &save_manifest);

  if (s.ok() && impl->mem_ == nullptr) {

    // Create new log and a corresponding memtable.
    uint64_t new_log_number = impl->versions_->NewFileNumber();
    WritableFile* lfile;
    s = options.env->NewWritableFile(LogFileName(dbname, new_log_number), &lfile);

    if (s.ok()) {
      edit.SetLogNumber(new_log_number);
      impl->logfile_ = lfile;
      impl->logfile_number_ = new_log_number;
      impl->log_ = new log::Writer(lfile);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok() && save_manifest) {
    edit.SetPrevLogNumber(0);  // No older logs needed after recovery.
    edit.SetLogNumber(impl->logfile_number_);
    s = impl->versions_->LogAndApply(&edit, &impl->mutex_);
  }
  if (s.ok()) {
    impl->RemoveObsoleteFiles();
    impl->MaybeScheduleCompaction();
  }
  impl->mutex_.Unlock();
  if (s.ok()) {
    assert(impl->mem_ != nullptr);
    *dbptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Snapshot::~Snapshot() = default;

Status DestroyDB(const std::string& dbname, const Options& options) {
  Env* env = options.env;
  std::vector<std::string> filenames;
  Status result = env->GetChildren(dbname, &filenames);
  if (!result.ok()) {
    // Ignore error in case directory does not exist
    return Status::OK();
  }

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del = env->RemoveFile(dbname + "/" + filenames[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }
    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->RemoveFile(lockname);
    env->RemoveDir(dbname);  // Ignore error in case dir contains other files
  }
  return result;
}

}  // namespace leveldb
