// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

/***
 * 用于合并多个sst文件的迭代器,
 * 主要是其 next()/pre()， 与归并排序异曲同工
 */
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {

    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();    // 初始化每一个迭代器指向最开始
    }

    FindSmallest();                  // current 指向最小key对应的迭代器
    direction_ = kForward;
  }


  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();  // 初始化每一个迭代器指向最后一个key
    }
    FindLargest();                // current 指向最大key对应的迭代器
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {

    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);      // 在每个迭代器中定位到 target
    }

    FindSmallest();                   // current 指向值较小的迭代器

    direction_ = kForward;
  }

  void Next() override {
    assert(Valid());


    // 基于上一个key, 使得每一个迭代器找到最小的比指定key大一点的最接近的key
    // 从候选集中选择最小的key, 即为next-key
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {

        IteratorWrapper* child = &children_[i];  // 遍历每一个迭代器

        if (child != current_) {

          child->Seek(key());

          if (child->Valid() && comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());  // 找到当前迭代器中 >=key中最接近的nkey
          if (child->Valid()) {
            child->Prev();        // 前一个一定<key
          } else {
            // 表示整体的sst 均小于 key
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();       // 寻找最小的key
  void FindLargest();        // 寻找最大的key

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;

  IteratorWrapper* children_;        // 这里是为了防止迭代器太多， 所以直接堆上分配
  int n_;
  IteratorWrapper* current_;         // 当前有效的迭代器，因为有多个，所以需要实时记录当前所用的

  Direction direction_;              // 表示迭代的方向
};


/***
 * 设置current_指向最小key对应的迭代器
 */
void MergingIterator::FindSmallest() {

  IteratorWrapper* smallest = nullptr;

  for (int i = 0; i < n_; i++) {

    IteratorWrapper* child = &children_[i];

    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

/****
 * 设置current_指向最大key对应的迭代器
 */
void MergingIterator::FindLargest() {

  IteratorWrapper* largest = nullptr;

  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
