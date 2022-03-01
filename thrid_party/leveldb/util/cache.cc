// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
// LRU 策略 : 淘汰最近最久未使用的缓存
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.


/****
 * 即用于 hashtable , 又用于lru缓存节点
 */
struct LRUHandle {
  void* value;                                          // 具体的值， 指针类型
  void (*deleter)(const Slice&, void* value);           // 定义回收节点的回调函数
  LRUHandle* next_hash;                                 // 使用在hashtable中，挂载同一个hash值上的单链表结构
  LRUHandle* next;                                      // 代表 LRU 双向链表中的下一个节点
  LRUHandle* prev;                                      // 代表 LRU 双向链表中的上一个节点
  size_t charge;                                        // 记录当前 value 所占用的内存大小， 用于后面超出容量后需要进行lru
  size_t key_length;                                    // 数据 key 的长度
  bool in_cache;                                        // 表示是否在缓存中
  uint32_t refs;                                        // 引用计数，因为当前节点可能会被多个组件使用， 不能简单的删除
  uint32_t hash;                                        // 记录当前key 的hasn值
  char key_data[1];                                     // key 数据位置

  // 获取 key
  Slice key() const {
    assert(next != this);

    return Slice(key_data, key_length);
  }
};

/***
 * HashTable : 数组加单链表结构
 *      1. hash table 的容量大小默认为0，
 *      2. 而且如果元素少于4时，hash table 按照元素的个数来分配
 *      3. 如果元素大于4时， 则按照2的倍数对齐
 *      4. 对于冲突采用链地址法
 *      5. rehash 的实际是当元素的个数大于链表长度时
 */
class HandleTable {
 public:
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  // 查找指定key的数据节点
  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }


  /***
   * 插入一个新的节点元素， 如果key存在就替换
   */
  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);  // 查找 hashtable 中是否存在这个节点
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;  // 如果不存在， 进行尾部插入

    if (old == nullptr) { // 表示插入操作
      ++elems_;
      if (elems_ > length_) { // 数据节点超过
        Resize();
      }
    }
    return old;
  }

  /***
   * 移出对应key的hash节点, 返回给用户， 不做清理操作
   */
  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash; // 摘掉此节点
      --elems_;
    }
    return result;
  }

 private:
  uint32_t length_;    // 当前 hash 表的总长度
  uint32_t elems_;     // 当前 hash 表实际元素个数
  LRUHandle** list_;  //  实际存储数据，底层结构， 每一个元素是一个指向 LRUHandler 的指针


  /***
   * 在 hash 表中查找元素
   * @param key 用于在同一个hash链上的key比对
   * @param hash 定位hash点
   * @return 指向对应 key 元素的 LRUHandler 指针， 使用二维指针的原因，可能要进行修改操作
   */
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }

  // 链表扩容
  void Resize() {
    uint32_t new_length = 4;  // 默认 4 个
    while (new_length < elems_) { // 如果元素超过 4个， 则向上 *2 对齐
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length]; // 二维指针

    // 初始化 new_list -> nullptr
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
      // 遍历当前的 list_
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;  // 获取当前节点指向的下一个节点
        // 重新计算当前节点应该挂载哪一个链上
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        // 放置在链的首部
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    // 清理旧的hash数组
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

/***
 * LRU : 置换最近最少使用节点的算法
 *   1. LRU 中的元素可能不仅仅只在 cache 中，有可能被外部所引用，因此我们不能直接删除节点
 *   2. 如果某个节点被修改/被其他引用，在空间不足时候，也不能参与LRU的计算
 *   3. in_use 表示即在缓存中， 也在外边被引用
 *   4. lru_.next 表示只在缓存中的节点
 *   5. table_是为了记录key和节点的映射关系，通过key可以快速定位到某个节点
 *   6. 调用insert/lookup 之后， 一定要使用 release 来释放句柄
 */
class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // 将数据插入到 Cache 中，需要注意的是这是为了方便快速比对，会保留节点的 hash 值
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,size_t charge,void (*deleter)(const Slice& key, void* value));
  // 查询目标节点
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  // 释放句柄， 本质还是操作引用
  void Release(Cache::Handle* handle);
  // 从缓存中删除节点
  void Erase(const Slice& key, uint32_t hash);
  // 清空节点
  void Prune();
  // 当前换在中数据所占用的内存
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);  // 从 in-use 链表中删除
  void LRU_Append(LRUHandle* list, LRUHandle* e); // 从 in-use 链表中移动到普通链表中
  void Ref(LRUHandle* e);    // 增加一个引用
  void Unref(LRUHandle* e);  // 节点引用等于0，才能调用free函数，否则只能移动
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // LRU 容量
  size_t capacity_;

  mutable port::Mutex mutex_;           // 操作锁
  size_t usage_ GUARDED_BY(mutex_);     // 获取当前 LRUCache 已经使用的内存
  LRUHandle lru_ GUARDED_BY(mutex_);    // 只存在缓存中的节点  refs == 1,
  LRUHandle in_use_ GUARDED_BY(mutex_); // 即在缓存中，又被引用的节点， refs >=2
  HandleTable table_ GUARDED_BY(mutex_);  // 用于快速获取某个节点
};

// 初始化双向链表结构
LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

// 析构只在缓存中的节点， 真正是否释放需要看 Unref 函数
// 由于lru
LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle

  for (LRUHandle* e = lru_.next; e != &lru_;) { // 遍历只在缓存中的聚聚
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {

  if (e->refs == 1 && e->in_cache) {
    // 如果只在缓存中
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  if (e->refs == 0) {  // 没有任何引用， 则释放资源.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // 表示没有被外部引用， 则放置到 lru 中
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

// 从双向链表中移出这个节点
void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

/**
 * 向双向链表最后插入一个节点元素
 */
void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}


/**
 * 查找一个节点，从 hashtable 中
 */
Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e); // 增加一个应用
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

/***
 * 申请释放改节点
 * 是否真的要释放需要判断 handler 的引用技术
 */
void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key, void* value)) {
  MutexLock l(&mutex_);

  LRUHandle* e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size())); // 创建一个 handler 节点

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());


  if (capacity_ > 0) {
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    LRU_Append(&in_use_, e);
    usage_ += charge;
    FinishErase(table_.Insert(e));  // 如果存在旧节点，则需要释放掉旧节点
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  // 如果容量不够， 需要
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    // 移出 lru 第一个节点
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// 从缓存中移出这个节点，是否释放取决于当前引用
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
  return e != nullptr;
}

// 基于 key 的移出策略
void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}

/***
 * 清空 缓存链表
 */
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

/***
 * LRU Cache 的进一步封装与优化
 *      多个 LRUCache 降低锁冲突
 */
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];  // 缓存数组   1 << kNumShardBits
  port::Mutex id_mutex_;
  uint64_t last_id_;

  // 对数据做 hash
  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }

  /**
   * 基于 hash 定位数组
   */
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    // 基于申请的缓存计算每个 cache 里的容量
    // 容量关系到 LRU 触发时间
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};
}

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
