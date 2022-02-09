// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {

/***
 * leveldb 内存资源池，用于leveldb 内存资源分配
 */
class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;  // 不需要拷贝构造函数
  Arena& operator=(const Arena&) = delete; // 不需要拷贝赋值

  ~Arena();

  // 为用户分配大小为 bytes 的内存
  char* Allocate(size_t bytes);

  // 从对齐的内存处，为用户分配大小为bytes 的内存
  char* AllocateAligned(size_t bytes);

  // 返回当前向系统申请的总的内存大小 memory_usage_
  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:

  //
  char* AllocateFallback(size_t bytes);

  // 向系统申请(new)
  // 分配指定大小为 block_bytes 的内存块(char *) 挂载到 blocks_ 中
  char* AllocateNewBlock(size_t block_bytes);

  // 内存池
  // 里面存放当前用户正在使用的内存
  std::vector<char*> blocks_;

  // 指向当前剩余的可以为用户提供内存请求的地址
  char* alloc_ptr_;
  // 可以提供给用户申请的内存大小
  size_t alloc_bytes_remaining_;

  // leveldb 向系统申请(new) 的内存大小
  std::atomic<size_t> memory_usage_;
};

// 分配指定数量的内存大小
// 如果当前的内存块满足申请需求，则从当前的内存块中取出一部分内存分配给用户
// 如果当前的内存块剩余内存不满足用户申请，则调用 AllocateFallback 为用户请求一块内存
//    -- 在这里，如果是申请的内存大于 1024KB, 则认为是申请的大内存， 则为其单独申请一块内存， 当前的block 仍然可以为后续内存请求分配内存
//    --        如果申请的内存不足 1024KB, 则认为当前的block 剩余的内存资源太少(内存碎片)，不足以满足用户后续的多数请求， 则重新分配一个block, 为用户提供内存分配需要
inline char* Arena::Allocate(size_t bytes) {
  assert(bytes > 0);
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
