// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function, void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  // data_iter_.valid()
  bool Valid() const override { return data_iter_.Valid(); }

  // 获取 datablock entry 对应的 key
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  // 获取 datablock entry 对应的 value
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();
  void SetDataIterator(Iterator* data_iter);
  void InitDataBlock();

  BlockFunction block_function_;        // block 获取函数(回调)
  void* arg_;                           // 函数参数
  const ReadOptions options_;           // Block 读取选项
  Status status_;                       // Block 状态
  IteratorWrapper index_iter_;          // index block 的访问类
  IteratorWrapper data_iter_;           // data block  的访问类

  std::string data_block_handle_;        // IndexBlock Value
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function,
                                   void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
  // key >= target, 因为从最小的 key 遍历上来的
  // 所以如果存在， 肯定在这里
  index_iter_.Seek(target);

  InitDataBlock();  // 定位到指定的 datablock

  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);  // 从当前 datablock 中定位到 target

  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();

  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();

  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}


/***
 * 前向跳过，如果在正向查找或者遍历过程中导致了当前 block 遍历到了最后
 * 则向后加载下一个 block
 * 获取 sst 中的下一个 datablock
 */
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  // 如果前次遍历将
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();  // 从 indexblock 中获取下一个 datablock

    InitDataBlock();     // 初始化 datablock

    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();   // 将新的datablock 移到首位
  }
}

/***
 * 如果在前次的反向迭代或者查找过程中导致遍历到了block最开始的位置
 * 则抛弃当前的 block 并向前加载一个 block
 * 反向跳过
 * 获取 sst 中的前一个 datablock
 */
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

/***
 * 定位要读取的key 应该在哪个 datablock 里面
 */
void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {

    Slice handle = index_iter_.value();           // 从 indexBlock 的value中计算到需要读取的 datablock

    if (data_iter_.iter() != nullptr && handle.compare(data_block_handle_) == 0) {
      // 表示这个当前要获取的key，应该会在上次读取的datablock里面
    } else {
      // datablock 获取， 先从 cache 中拿
      // 如果 cache 不存在， 则从文件中拿
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function,
                              void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
