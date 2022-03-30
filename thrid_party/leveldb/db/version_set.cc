// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

/***
 * 每一个 level 的最大的数据量为 : level * 10M
 *
 * @param options  选项
 * @param level    level 层
 * @return
 */
static double MaxBytesForLevel(const Options* options, int level) {

  double result = 10. * 1048576.0; // 10M

  while (level > 1) {
    result *= 10;
    level--;
  }

  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

/***
 * 计算当前version 每一个 Level 层的文件大小
 * @param files 每一个 level 里面的所有文件
 * @return
 */
static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

/***
 * 对于level>0层的sst，基于Key的定位SST文件
 * 因为sst是有序的，进行二分查找即可
 * @param icmp
 * @param files
 * @param key
 * @return
 */
int FindFile(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

static bool AfterFile(const Comparator* ucmp, const Slice* user_key, const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr && ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

/****
 * 内部判断给定的 key 上下限，在对应的 level sst 集合中，是否有重合
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
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();

  // 如果是第0层文件，则顺序遍历
  if (!disjoint_sorted_files) {
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      // smallest_user_key > f.largest || largest_user_key < f.smallest
      if (AfterFile(ucmp, smallest_user_key, f) || BeforeFile(ucmp, largest_user_key, f)) {
        // 不重叠
      } else {
        // 重叠
        return true;
      }
    }
    return false;
  }

  //对于非level-0层文件，采用二分查找法定位
  uint32_t index = 0;

  if (smallest_user_key != nullptr) {
    // 构造internalkey
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);

    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.

/***
 * 同一个 level 的 sst 文件遍历器
 * 用于 同一 level 下的 sst 二级遍历
 */
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp, const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }

  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;                  // key 比较器
  const std::vector<FileMetaData*>* const flist_;     // 同一个 level 层的数据遍历
  uint32_t index_;                                    // 当前定位的 sst 文件索引

  /**
   * sst 文件编号    : 8 bytes
   * 文件大小        : 8 bytes
   */
  mutable char value_buf_[16];
};

/***
 * 获取对应 SST 的 TwoLevelIterator
 *
 * @param arg          TableCache
 * @param options
 * @param file_value   sst 文件编号 + 文件大小
 * @return
 */
static Iterator* GetFileIterator(void* arg, const ReadOptions& options, const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),DecodeFixed64(file_value.data() + 8));
  }
}

/***
 * 对于同一个 level 的多个 sst 的查询遍历器
 * 应用在 level > 0 层级别上
 * 第一层用于定位 sst
 * 第二层用于 sst 内部遍历
 * @param options
 * @param level     要遍历的层级
 * @return
 */
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,int level) const {
  return NewTwoLevelIterator(new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator, vset_->table_cache_, options);
}

/***
 * 将本 db 中的所有的 sst 文件放入到双层迭代器中，
 * level == 0 层的 sst 之间范围会重复， 所以每一个 sst 都要放入到遍历器中
 * level >  0 层的 sst 之间范围不会重复， 只需要每一层构建一个 TwoLevelIterator 放置到迭代器中
 * @param options
 * @param iters
 */
void Version::AddIterators(const ReadOptions& options, std::vector<Iterator*>* iters) {

  // 首先直接加载  Level = 0的这部分数据
  // 因为这部分 sst 之间数据有重叠， sst 之间无序
  // 这部分可能会被频繁访问
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(options, files_[0][i]->number, files_[0][i]->file_size));
  }


  // 对于 level >0 的这一部分， 因为  sst 之间不重叠且有序
  // 可以使用 TwoLevelIterator 的方式懒加载这部分数据
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {

/***
 * 查询结果状态标识
 */
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};

/***
 * 保存查询结果
 */
struct Saver {
  SaverState state;             // 保存的查询状态结果  默认 kNnotFound
  const Comparator* ucmp;       // 用于匹配的比较器  InternalKeyComparator
  Slice user_key;               // 用户提交的 key
  std::string* value;           // 获取结果
};
}  // namespace

/***
 * 如果数据已经被查找到， 则保存 value
 * 数据查询到后首先解析成 ParsedInternalKey, 然后比较 key 是否匹配
 * 如果数据匹配且有效， 则保存查询结果
 * @param arg    Saver
 * @param ikey   匹配的key
 * @param v      value
 */
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);

  // 将 ikey 解析成具有 ParsedInternalKey 格式的 parsed_key
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}


/***
 * 按层级依次去查询 sst 文件， 找到要查询的key
 * 首先迭代第 0 层，第0层更具 filemeta, 定位所有 smallest < user_key < largest 的 sst
 *                然后根据 sst 的写入时间排序，优先查找时间最新的sst.
 * 第 0 层如果不能发现，遍寻找 level > 0 层， 因为 level > 0 层 sst 有序， 所以只需要依次遍历找到对应的那个即可
 * @param user_key
 * @param internal_key
 * @param arg
 * @param func
 */
void Version::ForEachOverlapping(Slice user_key,
                                 Slice internal_key,
                                 void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {

  const Comparator* ucmp = vset_->icmp_.user_comparator(); // BytewiseComparator

  // Search level-0 in order from newest to oldest.
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());


  // level == 0, 需要判断所有的文件
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];  // level-0 的 filemeta

    // 如果文件的 f->smallest < user_key < f->largest
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {

    // 为了提高顺序， 按照文件的新旧排序
    std::sort(tmp.begin(), tmp.end(), NewestFirst);

    for (uint32_t i = 0; i < tmp.size(); i++) {
      // 标识如果无需继续查找，则返回
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // 标识不在 level-0 中， 则去其他level中查询
  // Search other levels.
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.

    // 定位到要查询的key位于哪个 sst 文件中
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);

    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

/***
 * 查询 leveldb 找到对应的key数据
 * @param options
 * @param k
 * @param value 基于 key 获取到的value
 * @param stats 保存请求信息
 * @return
 */
Status Version::Get(const ReadOptions& options, const LookupKey& k, std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  // 用于 判断是否满足触发 compaction 的条件
  struct State {

    Saver saver;                     // 查询结果
    GetStats* stats;                 // 用于标志每次查找数据时，首次查找且没有找到数据的 SST 文件，一般位于第0层
                                     // 也可能不位于第0层

    const ReadOptions* options;      // 读取选项
    Slice ikey;                      // 用于匹配的 Key, internalkey

    // 用于标志每次查找时， 最后一次访问的文件
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;                        // 用于判断是否查询过程中出现了异常
    bool found;

    /**
     * 判断是否匹配, 如果找到对应的key 或者不需要继续寻找，则返回 False
     * 如果没有找到，需要继续寻找，则返回 True
     * @param arg     State 指针
     * @param level   sst 的层级
     * @param f       文件元信息
     * @return
     */
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // 标志第一次用于查找，但是没有找到数据的SST,一般位于第0层
        // 也可能不位于第0层
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;             // 将 last_file_read 信息定位到最后一次用于匹配的SST中
      state->last_file_read_level = level;

      // 从 sst 中查找对应的 internalkey
      // table_cache_ 中只返回 >= user_key 的第一个 key ,
      // 但是因为 sst 从小大大有序， 所以如果这个不是要寻找的数据， 则此 key 不在数据范围内
      state->s = state->vset->table_cache_
          ->Get(*state->options, f->number,f->file_size, state->ikey, &state->saver, SaveValue);

      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();    // 要查询的 internalkey
  state.vset = vset_;               // version_set

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

/***
 * 标识如果在样本测试中，某个key查找了多个sst文件， 那么第一个sst文件需要合并
 * @param stats
 * @return
 */
bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;

  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}


/***
 * leveldb 的 major compaction 机制其中之一就是 seek 次数查找阈值
 * 为了有效的统计每个 SST 被访问的次数 RecordReadSample 函数被置于 DbIter 之中
 * 根据采样的频率(config::kReadBytesPeriod控制)对 key 进行采样， 判断是否需要 compaction
 *
 * @param internal_key
 * @return
 */
bool Version::RecordReadSample(Slice internal_key) {

  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;   // 判断总共查了几个文件

      if (state->matches == 1) {
        // 记录首次匹配到的文件.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // 最多查询两个文件.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

/***
 * 检查是否和指定 level 的文件有重叠。
 * 内部直接调用 SomeFileOverlapsRange
 * @param level                 要检查的 level 层
 * @param smallest_user_key     给定的重叠 key 下限
 * @param largest_user_key      给定的重叠 key 的上限
 * @return
 */
bool Version::OverlapInLevel(int level,
                             const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_,
                               (level > 0),
                               files_[level],
                               smallest_user_key, largest_user_key);
}

/****
 * minor compaction 时， 选择要dump的level级别。
 * 由于第0层文件频繁的被访问，而且有严格的数量限制，另外多个SST之间还存在重叠，
 * 所以为了减少读放大，我们是否可以考虑将内存中的文件dump到磁盘时尽可能送到高层呢?
 *
 * PickLevelForMemTableOutput 函数作用就是判断最多能将sst送到第几层,它的原则是:
 * 1. 大于level-0的各层文件之间时有序的，如果放到对应的层数会导致文件间不严格有序，影响读取，则放弃
 * 2. 如果放到level+1层，于level+2层文件重叠很大，导致compact到该文件时，overlap文件过大，则放弃
 * 3. 最大返回level2
 *
 * @param smallest_user_key   要写出的sst的最小值
 * @param largest_user_key    要写出的sst的最大值
 * @return level
 */
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;

  // 首先判断是否跟 level-0 层文件有重叠
  // 如果跟level-0层文件有重叠，则直接写到level-0
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;

    while (level < config::kMaxMemCompactLevel) {
      // config::kMaxMemCompactLevel == 2

      // 如果跟 level+1层有重叠，则直接放弃
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }

      if (level + 2 < config::kNumLevels) {
        // 如果跟level+2层.合并时需要合并的文件量太大，则放弃
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

/****
 * 在所给定的 level 中找出和[begin, end]有重合的 sstable 文件
 * 注意的是 level-0 层多个文件存在重叠，可能需要扩大搜索范围，将目标文件于重叠文件一起合并
 *
 * 改函数常被用来压缩的时候使用，根据 leveldb 的设计， level 层合并 Level+1 层Merge时候，level中所有重叠的sst都会参加。
 * 这一点需要特别注意。
 *
 * @param level    要查找的层级
 * @param begin    开始查询的 key
 * @param end      结束查询的 key
 * @param inputs   用于收集重叠的文件
 */
void Version::GetOverlappingInputs(int level,
                                   const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);

  inputs->clear();

  // 获取 user_key
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }

  const Comparator* user_cmp = vset_->icmp_.user_comparator();

  // 遍历当前 Level 的所有 sst
  for (size_t i = 0; i < files_[level].size();) {

    FileMetaData* f = files_[level][i++];

    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();

    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // f->largest < user_begin , 表示没有交集
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // f->smallest > user_end, 表示没有交集
    } else {  // 存在交集的情况
      inputs->push_back(f);

      if (level == 0) {
        // 第0层需要特殊处理，因为SST文件之间可能有重叠，因此需要扩大搜索范围，保证level-0中要合并的sst+重叠sst一起合并
        // 因此可能需要重新搜索
        // 如果SST的key的最小值小于所给定的key下限, 或者SST的key的最大key大于所给定的key的上限
        // 此时扩大范围，找到所有的重叠 sst 一起合并
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          // f->smallest < user_begin
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr && user_cmp->Compare(file_limit, user_end) > 0) {
          // f-> largest > user_end
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

/*****
 * VersionSet 辅助类
 * 用于实现  Version + VersionEdit = Version 的功能 :
 *     +  对应 Apply
 *     =  对应 SaveTo
 */
class VersionSet::Builder {
 private:

  // 用于比较两个FileMetaData的smallest是否相等
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {

      int r = internal_comparator->Compare(f1->smallest, f2->smallest);

      if (r != 0) {
        return (r < 0);
      } else {
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;

  struct LevelState {
    std::set<uint64_t> deleted_files;      // 需要删除的文件
    FileSet* added_files;                  // 新增的文件，文件新旧比较顺序如下
                                           // 先比较 user_key, 相等在比较seq,如果在相等在比较文件编号
  };

  VersionSet* vset_;                       //
  Version* base_;                          //

  LevelState levels_[config::kNumLevels];  // 每一层的新增及删除文件

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();                                  // 添加引用

    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;        // 设置比较器

    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  // 由于levels_[level].added_files是动态分配的
  // 析构函数不释放会造成内存泄露
  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {

      // 不能直接 delete, 因为有可能有其他还在共享这个对象，所以只能复制出去
      const FileSet* added = levels_[level].added_files;

      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());

      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }

      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();  // 释放引用
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(const VersionEdit* edit) {

    // 将 edit中下一次需要压缩的level和最大key更新到versionset中
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first; // 提取需要压缩的Level
      vset_->compact_pointer_[level] = edit->compact_pointers_[i].second.Encode().ToString();
    }

    // 将 VersionEdit 中要删除的文件填充到这个version中
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // 将 VersionEdit 中新增的文件填充到这个version中
    // 需要设置元信息
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // 这里将特定数量的seek之后自动进行compact操作，假如 :
      //  1. 一次 seek 需要 10ms
      //  2. 读,写1MB文件消耗 10ms(100MB/s)
      //  3. 对1MB文件的compact操作时合计一共做了25MB的IO操作，包括 :
      //      从这个level读1MB
      //      从下个level读10-12MB
      //      向下一个level写10-12MB
      //  这一位这25次seek消耗与1MB数据的compact相当。也就是，
      //  一次 seek 的消耗与40KB数据的compact消耗近似。这里做一个保守估计，在一次compact之前每16kB的数据大约进行1次seek.
      //  allow_seeks 数目和文件数量有关
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);  // 如果存在这个文件数量，则
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  // 将Edit的变更信息应用到version中
  // 更新 version.files_ 信息
  void SaveTo(Version* v) {

    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;

    for (int level = 0; level < config::kNumLevels; level++) {

      // 拿到每一个level 下面的 sst
      const std::vector<FileMetaData*>& base_files = base_->files_[level];

      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();

      // 拿到VersionEdit中这个level下面的新增文件
      const FileSet* added_files = levels_[level].added_files;

      v->files_[level].reserve(base_files.size() + added_files->size());

      // 将原base_->files_[level]与levels_[level].added_files有序合并
      // added_files 为有序数据，
      // base_iter 不会重复填充，因为这是一个游标
      // 类似于插入排序
      for (const auto& added_file : *added_files) { // 遍历每一个新增文件, 有序

        // 以 base_iter 为游标，找到[base_iter,bpos)之间的数据填充
        // 设置 base_iter = bpos
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos;
             ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG  // debug
      // Make sure there is no overlap in levels > 0
      if (level > 0) {

        for (uint32_t i = 1; i < v->files_[level].size(); i++) {

          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;

          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  /***
   * 判断是否要将文件加入到version中去
   * 如果是要被delete的文件，则不加入
   * @param v     要被填充的 version
   * @param level 填充文件的 level
   * @param f     填充文件
   */
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {

    if (levels_[level].deleted_files.count(f->number) > 0) {
      // 表示文件在这个版本中要删除
      // 所以在新的Version中不在添加这个文件，以表示文件的删除
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];

      if (level > 0 && !files->empty()) {
        // 对于 level>0 层，保证数据有序
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest, f->smallest) < 0);
      }

      f->refs++;

      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname,
                       const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),      // next_file_number_ 初始化为 2
      manifest_file_number_(0),  // manifest_file_number_ 初始化为 0
      last_sequence_(0),         // last_sequence_ 初始化为0
      log_number_(0),            // 日志文件编号初始化为 0
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {

  AppendVersion(new Version(this));  // 添加一个空的 version
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);

  if (current_ != nullptr) {
    current_->Unref();
  }

  current_ = v;
  v->Ref();

  // 放置到双向链表的队尾
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

/*****
 * 将VersionEdit信息填充到Manifest文件中
 * 并将当前的修改记录VersionEdit迭代到新的Version里面
 *
 * @param edit  当前的修改记录
 * @param mu    文件锁
 */
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {

  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);               // 设置日志编号
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);     // 设置 prev_log 编号
  }

  edit->SetNextFile(next_file_number_);           // 设置下一个文件编码
  edit->SetLastSequence(last_sequence_);          // 设置 last_seqnum

  // 构建新的版本 v = current + edit
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  // 选择下一次需要压缩的文件，因为是单线程的
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;

  // 第一次时，versionedit 日志文件还没有被创建，比如刚打开一个全新的数据库时， 此时需要将当前的版本的状态作为
  // base 状态写入快照
  if (descriptor_log_ == nullptr) {
    assert(descriptor_file_ == nullptr);

    // Manifest : {dbname}/MANIFEST-{manifest_file_number_}
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);

    if (s.ok()) {
      // 表示此时重新开启一个新的 manifest 文件，所以先将当前版本记录
      // 作为未来回复的一个baseline
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }


  {
    mu->Unlock();

    // 将最新的VersionEdit填充到Manifest文件
    if (s.ok()) {

      std::string record;

      // 将最新的 edit 刷盘
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);

      if (s.ok()) {
        s = descriptor_file_->Sync();
      }

      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // 在每次新创建 Manifest 文件时
    // 将最新的 Manifest 文件作为当前可用的 manifest
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    // 将当前的 version 挂载到 versionset 中
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

/***
 * 从 Manifest 回复当前的Version
 * @param save_manifest
 */
Status VersionSet::Recover(bool* save_manifest) {

  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // 获取 CURRENT 的 MANIFEST 文件
  std::string current;

  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;

  // 拿到 MANIFEST 文件读取工具
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;                       // 记录数量

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,0 /*initial_offset*/);
    Slice record;
    std::string scratch;

    while (reader.ReadRecord(&record, &scratch) && s.ok()) {

      // 每次获取一个 VersionEdit
      // 有多少给 VersionEdit, Version就会被叠加几次
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);

      if (s.ok()) {
        if (edit.has_comparator_ && edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(edit.comparator_ + " does not match existing comparator ",icmp_.user_comparator()->Name());
        }
      }

      // 将VersionEdit及时应用到Version中
      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    // 将所有的变更应用到新的 Version 中
    Version* v = new Version(this);
    builder.SaveTo(v);
    Finalize(v);
    AppendVersion(v);

    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // 读取的 Manifest 文件
    // dscname : dbname_/{current}
    // current : 当前在读的Manifest
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",read_records, error.c_str());
  }

  return s;
}

/****
 * 基于配置与当前Manifest的文件大小，判断是否能够继续使用当前的Manifest
 */
bool VersionSet::ReuseManifest(const std::string& dscname, const std::string& dscbase) {

  // 基于配置选择是否复用Manifest
  if (!options_->reuse_logs) return false;

  FileType manifest_type;

  uint64_t manifest_number;
  uint64_t manifest_size;

  // 判断 Manifest 是否超过上限
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  // 以可追加的方式打开
  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);

  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

/****
 * 当产生新版本时， 遍历所有的层， 比较该层文件总大小与基准大小，得到一个最应当compat的层
 * double compaction_score_ 和 int compaction_level 主要就是在Finalize 函数中进行计算
 * double_
 * @param v
 */
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  // 遍历所有的level
  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;

    if (level == 0) {
      // 我们通过限制文件数量而不是字节数量来特别处理 level==0，原因有两个：
      // 1) 对于较大的写缓冲区大小，最好不要进行太多的level=0级压缩。
      // 2) level==0 的文件在每次读取时都会合并，因此我们希望避免在单个文件大小较小时出现过多的文件（可能是因为写入缓冲区设置较小，或压缩比非常高，或大量的覆盖/删除）。
      score = v->files_[level].size() / static_cast<double>(config::kL0_CompactionTrigger);

    } else {
      // level > 1 层 通过数据量来判断
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    // 找到一个最需要进行合并的层
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }


  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}


Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // 填充比较器
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // 填充 compact_pointer_
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // 填充当前的有效SST文件
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

/****
 * 读取所有版本的数据
 * 将所有版本存活过的sst 文件编号返回
 * @param live
 */
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

/***
 * 返回当前 level 层 sst 所有文件的大小
 * @param level
 * @return
 */
int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

/****
 * 遍历 从 level = [1 - maxlevel -1] 层
 * 寻找某个 sst 在next level 层，覆盖的 sst 所占据的文件大小的最大值
 * 只定位了最大覆盖字节， 并没有定位所在的 level 与 sst
 */
int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;

  std::vector<FileMetaData*> overlaps;

  // 遍历 从 level = [1 - maxlevel -1] 层
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    // 遍历每个 level 下面的所有 sst 文件
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];

      // 当前 level 的当前 sst 所在的 数据范围，在 level+1 层覆盖的sst
      // 计算覆盖的 sst 的文件字节数
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest, &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

/***
 * 基于给定的 sst 列表， 返回其中最大的 user_key 与最小的  user_key
 * @param inputs  对应层的sst文件
 * @param smallest  返回的最小值
 * @param largest   返回的最大值
 */
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest,
                          InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

/****
 * 返回当前两个 sst 文件的最大值与最小值
 *
 * @param inputs1 第一个 level 层的所有 sst
 * @param inputs2 第二个 level 层的所有 sst
 *
 * @param smallest 返回的最小的key
 * @param largest  返回的最大key
 */
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

/****
 * 创建多层合并迭代器， 用于 Compaction 后的数据进行 merge
 * @param c
 * @return
 */
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);

  Iterator** list = new Iterator*[space];

  int num = 0;

  for (int which = 0; which < 2; which++) {

    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          // level == 0 所有的文件都需要加入
          list[num++] = table_cache_->NewIterator(options, files[i]->number,files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),&GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

/***
 * 判断是否需要进行压缩
 * @return
 */
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;


  // 基于文件量判断是否超过阈值
  // level == 0 层， 是判断文件数量是否超过 4
  // level >= 1 层， 是判断文件字节数是否超过 10M * level 量
  const bool size_compaction = (current_->compaction_score_ >= 1);

  // 某个文件查询多次但是没有定位到数据时
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);

  // 如果时文件大小超过了阈值需要合并
  // 优先基于大小进行压缩
  // size 触发的 compaction 稍微复杂一点， 他需要上一次 compaction 做到了哪一个 key, 什么地方，然后大于改key
  // 的第一个文件即为 level n 的所选文件
  if (size_compaction) {
    level = current_->compaction_level_;  // 设置需要合并的 level 层
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {

      FileMetaData* f = current_->files_[level][i];

      // 基于上次 level 的压缩位置， 然后标识本次从哪里开始压缩
      if (compact_pointer_[level].empty() || icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }

    if (c->inputs_[0].empty()) {
      // 标识上次如果压缩到最后了， 在从最开始压缩
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);  // 直接压入 seek 对应的文件
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();


  // 因为 level == 0数据存在重叠， 如果指定的数据被合并到下一层，
  // 那么有可能 level==0中， 版本比合并的sst老的数据会优先查询到
  // 所以在选择压缩的时候，先基于对应的 sst , 并且找到这个 sst 的上下限
  // 基于重叠的数据， 找到所有重叠的 sst 一起合并
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  SetupOtherInputs(c);

  return c;
}

/****
 * 返回对应 sst 组中最大的 userKey
 * @param icmp 比较器
 * @param files  sst 文件
 * @param largest_key 返回的最大的 user_key
 */
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

/****
 *
 * @param icmp               user_key 比较器
 * @param level_files        对应的 level 层的所有 sst
 * @param largest_key        要压缩的最大的 user_key  ---- 压缩的sst 的 level_files level 层相同
 */
FileMetaData* FindSmallestBoundaryFile(const InternalKeyComparator& icmp,
                                       const std::vector<FileMetaData*>& level_files,
                                       const InternalKey& largest_key) {

  const Comparator* user_cmp = icmp.user_comparator();

  FileMetaData* smallest_boundary_file = nullptr;

  for (size_t i = 0; i < level_files.size(); ++i) {

    FileMetaData* f = level_files[i];

    if (icmp.Compare(f->smallest, largest_key) > 0 && user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==0) {

      if (smallest_boundary_file == nullptr || icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.

/****
 *
 * @param icmp               比较器
 * @param level_files        对应层的所有的 SST
 * @param compaction_files   用于压缩的 SST
 */
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {

  InternalKey largest_key;

  // 找到要压缩的 sst 的最大的 user_key
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;
  while (continue_searching) {
    FileMetaData* smallest_boundary_file = FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

/****
 * 计算 level +1 层需要参与的文件
 *
 * 尽可能选择多个文件进行压缩
 * @param c
 */
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();     // 记录要压缩的层
  InternalKey smallest, largest;

  // 处理 level 层的临界值，
  // 因为随着压缩，同一个 user key 可能处于两个sst
  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  GetRange(c->inputs_[0], &smallest, &largest);

  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    if (expanded0.size() > c->inputs_[0].size() && inputs1_size + expanded0_size < ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit, &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
      if (expanded1.size() == c->inputs_[1].size()) {
       Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));

        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

/****
 * 用于手动触发压缩
 */
Compaction* VersionSet::CompactRange(int level,
                                     const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // 需要避免再一次操作中要参与压缩的文件太多
  // 所以在 level+1 层，我们将合并的文件字节数不能太大
  // 但是针对第0层我们不会处理，因为第0层文件之间允许它重复，所以会选择很多个
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);  // 计算最大的文件限制

    uint64_t total = 0;

    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
