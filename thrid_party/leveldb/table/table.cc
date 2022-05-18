// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"

#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }

  Options options;               // Table 相关的参数信息
  Status status;                 // Table 相关的状态信息
  RandomAccessFile* file;        // Table 所持有的文件
  uint64_t cache_id;             // cache 本身所对应的缓存id， 自增生成
  FilterBlockReader* filter;     // filter block 块的读取
  const char* filter_data;       // 保存对应的 filter 数据

  BlockHandle metaindex_handle;  // 解析保存 metaindex_handler
  Block* index_block;            // index block 数据
};

Status Table::Open(const Options& options, RandomAccessFile* file, uint64_t size, Table** table) {

  *table = nullptr;

  // 数据大小太小，不足以构建 Footer
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }


  // Footer 区域解析
  // 将字节反序列化成 Footer
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength, &footer_input, footer_space);
  if (!s.ok()) return s;

  Footer footer;
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;

  // IndexBlock 区域解析
  // 并将数据解析成 IndexBlock
  BlockContents index_block_contents;
  ReadOptions opt;
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    Block* index_block = new Block(index_block_contents);  // 构造 index_block

    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();  // 赋值语句
    rep->index_block = index_block;    //
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;

    *table = new Table(rep);
    // 填充 rep->filter
    (*table)->ReadMeta(footer);
  }

  return s;
}

/***
 * 读取 Block 的过滤信息
 * @param footer
 */
void Table::ReadMeta(const Footer& footer) {

  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  // 读取  MetaIndexBlock， 里面是 Filter 信息
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    return;
  }

  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}

/***
 * 基于 filter_hadler 读取 Filter 信息
 * @param filter_handle_value
 */
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

/***
 * 删除这个 block 的空间
 */
static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

/***
 * 删除这个 cache block 的空间， Cache 回调函数
 */
static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

/***
 * 释放这个 block 的缓存
 */
static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

/***
 * 读取一个 Block 并返回这个 Block 的 Iterator
 * 优先从 cache 中加载 : {cache_id | offset << 8} -> datablock
 * cache 不存在再从文件中加载
 *
 * @param arg              table 句柄
 * @param options          配置选项
 * @param index_value      对应 block 的 block_handler
 *
 * @return datablock->iter
 */
Iterator* Table::BlockReader(void* arg, const ReadOptions& options, const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;  // 获取 Block Cache
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  //拿到 DataBlock索引信息， 解析 BlockHandler
  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);

  if (s.ok()) {
    BlockContents contents;

    if (block_cache != nullptr) {      // 具有缓存系统
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);    // 填充 cache_id
      EncodeFixed64(cache_key_buffer + 8, handle.offset());  // 填充 offset

      Slice key(cache_key_buffer, sizeof(cache_key_buffer));

      cache_handle = block_cache->Lookup(key);     // 寻找这个 key 是否存在

      if (cache_handle != nullptr) {        // 存在于缓存中
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {                             // 并不存在于缓存中
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            // 填充到缓存中
            cache_handle = block_cache->Insert(key, block, block->size(), &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  // 获取 Block::Iter
  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

/***
 * 获得 基于 indexblock 的 datablock 双层遍历器
 */
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(rep_->index_block->NewIterator(rep_->options.comparator), &Table::BlockReader, const_cast<Table*>(this), options);
}

/***
 * 基于回调的方式， 查找某个 key, 如果找到则调用 handle_result(arg, key, value)
 *
 * @param options            配置选项
 * @param k                  key
 * @param arg                参数
 * @param handle_result      找到外部数据时候触发的外部调用
 *
 */
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg, void (*handle_result)(void*, const Slice&,const Slice&)) {
  Status s;

  // 从IndexBlock中定位可能存在key的DataBlock位置
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);

  if (iiter->Valid()) {
    Slice handle_value = iiter->value();         // 定位到 datablock 的位置
    FilterBlockReader* filter = rep_->filter;    // 获得过滤器
    BlockHandle handle;

    // 使用 Bloom 过滤器判断一下数据是否在这个 DataBlock 里面
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() && !filter->KeyMayMatch(handle.offset(), k)) {
      // Bloom 过滤器没有找到这个数据
    } else {
      // BloomFilter 也判断数据位于这个DataBlock中，那么数据存在的可能性非常大
      // 现在进行读取DataBlock查找
      // 在DataBlock中进行查找
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);

      if (block_iter->Valid()) {
        // 找到这个数据，调用回调函数
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter = rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
