// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

 // block 大小 4096
static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}


Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // 如果申请的内存大于 1/4 block_size
    // 直接分配一个 大小为 bytes 的内存块
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // 如果申请的内存不足 1/4 的内存块
  // 则申请一个4KB 的内存块
  // 然后将这个内存块的一部分分配给用户，然后记录剩余部分的位置与大小
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAligned(size_t bytes) {
  // 可以说是用来判断当前 cpu 地址宽度
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;

  // 地址宽度必须是 2 的指数倍
  static_assert((align & (align - 1)) == 0, "Pointer size should be a power of 2");

  // 将指针转为整数
  // 用来判断当前还差内存才会对齐
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);

  size_t needed = bytes + slop;
  char* result;

  if (needed <= alloc_bytes_remaining_) {
    // 内存先对齐
    result = alloc_ptr_ + slop;

    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }

  // 判断系统分配的内存是否对齐
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

// 向系统申请一个大小为 block_bytes 的内存块挂到 blocks_
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
