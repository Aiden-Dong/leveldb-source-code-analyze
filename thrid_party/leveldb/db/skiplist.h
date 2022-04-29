// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_SKIPLIST_H_

// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

#include <atomic>
#include <cassert>
#include <cstdlib>

#include "util/arena.h"
#include "util/random.h"

namespace leveldb {

// leveldb 实现调表结构
template <typename Key, typename Comparator>
class SkipList {
 private:
  struct Node;

 public:
  explicit SkipList(Comparator cmp, Arena* arena);

  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

  // 插入一个 key 到列表中
  void Insert(const Key& key);

  // 判断一个 key 是否在列表中
  bool Contains(const Key& key) const;

  // 内部类
  // 循环遍历 skiplist 结构
  class Iterator {
   public:
    explicit Iterator(const SkipList* list);

    // 判断当前是否处于有效节点上
    bool Valid() const;

    // 返回当前位置对应的key
    const Key& key() const;

    // 将当前游标移动到下一个节点上
    void Next();

    // 将当前游标移动到上一个节点上
    void Prev();

    // 将游标移动到第一个指定的key上
    void Seek(const Key& target);

    // 将游标移动到第一个指定的key上
    void SeekToFirst();

    // 将游标移动到最后一个key上
    void SeekToLast();

   private:
    const SkipList* list_;  // 调表指针
    Node* node_;   // 调表中的某个元素
    // Intentionally copyable
  };

 private:
  enum { kMaxHeight = 12 };  // 指针最高12层

  // 获取最大指针数量
  inline int GetMaxHeight() const {
    return max_height_.load(std::memory_order_relaxed);
  }

  // 生成一个新的调表节点
  Node* NewNode(const Key& key, int height);
  int RandomHeight();

  // 比较两个 key 是否相同 
  bool Equal(const Key& a, const Key& b) const { return (compare_(a, b) == 0); }

  // Return true if key is greater than the data stored in "n"
  bool KeyIsAfterNode(const Key& key, Node* n) const;

  Node* FindGreaterOrEqual(const Key& key, Node** prev) const;

  Node* FindLessThan(const Key& key) const;

  Node* FindLast() const;

  // Immutable after construction
  // key 比较器
  Comparator const compare_; 

  // node 节点资源分配器
  Arena* const arena_;  // Arena used for allocations of nodes

  // 调表链接节点
  Node* const head_;

  // 当前跳表的最高深度
  std::atomic<int> max_height_;  // Height of the entire list

  Random rnd_;
};

// 调表节点结构
template <typename Key, class Comparator>
struct SkipList<Key, Comparator>::Node {
  explicit Node(const Key& k) : key(k) {}

  Key const key;

  // 返回第n个指针指向的一下个元素
  Node* Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_acquire);
  }

  // 设置第n个指针指向的下一个元素
  void SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_release);
  }

  // No-barrier variants that can be safely used in a few locations.
  Node* NoBarrier_Next(int n) {
    assert(n >= 0);
    return next_[n].load(std::memory_order_relaxed);
  }
  void NoBarrier_SetNext(int n, Node* x) {
    assert(n >= 0);
    next_[n].store(x, std::memory_order_relaxed);
  }

 private:
  // Array of length equal to the node height.  next_[0] is lowest level link.
  // 指向后面节点的调表
  // 数组长度不固定，动态分配
  // next_[0] 是最低级的链表
  std::atomic<Node*> next_[1];
};

// 创建具有n个指针，元素为key的,新的Node
// 使用 Arena 分配一个内存区域, 使用定位new运算符将 node 填充到该区域中
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::NewNode(
    const Key& key, int height) {
  char* const node_memory = arena_->AllocateAligned(
      sizeof(Node) 
      + sizeof(std::atomic<Node*>) * (height - 1));  
      // 动态调整数组长度，长度不固定 -1 因为Node中已经有一个元素
  
  return new (node_memory) Node(key); // 使用定位 new 运算符
}


// 初始化一个遍历器
template <typename Key, class Comparator>
inline SkipList<Key, Comparator>::Iterator::Iterator(const SkipList* list) {
  list_ = list;
  node_ = nullptr;
}

// 判断当前游标是否在一个有效节点上
// 是否有效使用 空指针判断
template <typename Key, class Comparator>
inline bool SkipList<Key, Comparator>::Iterator::Valid() const {
  return node_ != nullptr;
}

// 获取当前游标指向节点的key元素
template <typename Key, class Comparator>
inline const Key& SkipList<Key, Comparator>::Iterator::key() const {
  assert(Valid());
  return node_->key;
}

// 获取当前节点的下一个节点 -> next_[0] 指针
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Next() {
  assert(Valid());
  node_ = node_->Next(0);
}

// 获取当前节点的前一个节点
// 基于跳表的前向遍历
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Prev() {
  // Instead of using explicit "prev" links, we just search for the
  // last node that falls before key.
  assert(Valid());
  node_ = list_->FindLessThan(node_->key);
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// 将游标移动到指定位置
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::Seek(const Key& target) {
  node_ = list_->FindGreaterOrEqual(target, nullptr);
}

// 将游标移动到首部位置
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToFirst() {
  node_ = list_->head_->Next(0);
}

// 将游标移动到尾部位置
template <typename Key, class Comparator>
inline void SkipList<Key, Comparator>::Iterator::SeekToLast() {
  node_ = list_->FindLast();
  if (node_ == list_->head_) {
    node_ = nullptr;
  }
}

// 生成一个随机高度
template <typename Key, class Comparator>
int SkipList<Key, Comparator>::RandomHeight() {
  // Increase height with probability 1 in kBranching
  static const unsigned int kBranching = 4;
  int height = 1;
  while (height < kMaxHeight && rnd_.OneIn(kBranching)) {
    height++;
  }
  assert(height > 0);
  assert(height <= kMaxHeight);
  return height;
}

// 判断给定key 是否在对应的节点后方
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::KeyIsAfterNode(const Key& key, Node* n) const {
  // null n is considered infinite
  return (n != nullptr) && (compare_(n->key, key) < 0);
}

// 找到比指定key大的第一个元素，
// 并且设置前置节点指针指向该元素
// 主要提供给插入节点使用
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindGreaterOrEqual(const Key& key, Node** prev) const {

  // 从头部开始遍历
  Node* x = head_;
  int level = GetMaxHeight() - 1;     // 从最高级开始查找
  while (true) {
    Node* next = x->Next(level);      // 拿到下一个节点

    if (KeyIsAfterNode(key, next)) {   // 判断 key 是否是next 对应的下一个节点
      x = next;
    } else {
      if (prev != nullptr) prev[level] = x;  //

      if (level == 0) {
        return next;
      } else {
        // Switch to next list
        level--;
      }
    }
  }
}

// 重点内容 :
// 跳表的前向查表过程
// 从最大跨度依次减少跨度，直到找到对应的key
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node*
SkipList<Key, Comparator>::FindLessThan(const Key& key) const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    assert(x == head_ || compare_(x->key, key) < 0);
    Node* next = x->Next(level);
    if (next == nullptr || compare_(next->key, key) >= 0) { // 如果相等直接返回结果不就完了，为啥要依次递减
      if (level == 0) {
        return x;
      } else {
        level--;
      }
    } else {
      x = next;
    }
  }
}

// 基于前向遍历获取最后一个元素
template <typename Key, class Comparator>
typename SkipList<Key, Comparator>::Node* SkipList<Key, Comparator>::FindLast()
    const {
  Node* x = head_;
  int level = GetMaxHeight() - 1;
  while (true) {
    Node* next = x->Next(level);
    if (next == nullptr) {
      if (level == 0) {
        return x;
      } else {
        // Switch to next list
        level--;
      }
    } else {
      x = next;
    }
  }
}


/***
 * 初始化一个跳表结构 
 */
template <typename Key, class Comparator>
SkipList<Key, Comparator>::SkipList(Comparator cmp, Arena* arena)
    : compare_(cmp),
      arena_(arena),
      head_(NewNode(0 /* any key will do */, kMaxHeight)), // 首部初始化一个为0的的常量key
      max_height_(1),
      rnd_(0xdeadbeef) {
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, nullptr); // 初始化首部指针
  }
}


// 重点内容 : 
// 填充一个元素到跳表结构中
template <typename Key, class Comparator>
void SkipList<Key, Comparator>::Insert(const Key& key) {

  // TODO(opt): We can use a barrier-free variant of FindGreaterOrEqual()
  // here since Insert() is externally synchronized.
  
  Node* prev[kMaxHeight];  // pre 是一个未初始化的指针数组

  // prev的每个level指针指向对应级的下一个大于等于key的node节点
  // nodex返回大于等于key的第一个node节点
  Node* x = FindGreaterOrEqual(key, prev);

  // Our data structure does not allow duplicate insertion
  assert(x == nullptr || !Equal(key, x->key));

  int height = RandomHeight(); // 生成一个随机高度

  // 如果深度超过了当前最高的跳表指针深度，则需要将头部node指向改新node
  if (height > GetMaxHeight()) {
    for (int i = GetMaxHeight(); i < height; i++) {
      prev[i] = head_;
    }
    // It is ok to mutate max_height_ without any synchronization
    // with concurrent readers.  A concurrent reader that observes
    // the new value of max_height_ will see either the old value of
    // new level pointers from head_ (nullptr), or a new value set in
    // the loop below.  In the former case the reader will
    // immediately drop to the next level since nullptr sorts after all
    // keys.  In the latter case the reader will use the new node.
    max_height_.store(height, std::memory_order_relaxed);
  }

  // 节点插入列表
  x = NewNode(key, height);
  for (int i = 0; i < height; i++) {
    // NoBarrier_SetNext() suffices since we will add a barrier when
    // we publish a pointer to "x" in prev[i].
    x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
    prev[i]->SetNext(i, x);
  }
}

// 判断一个key是否在跳表中
template <typename Key, class Comparator>
bool SkipList<Key, Comparator>::Contains(const Key& key) const {
  Node* x = FindGreaterOrEqual(key, nullptr);
  if (x != nullptr && Equal(key, x->key)) {
    return true;
  } else {
    return false;
  }
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_SKIPLIST_H_
