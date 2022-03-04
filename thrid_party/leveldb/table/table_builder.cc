// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

/***
 * TableBuild 内部封装解耦类
 */
struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr ? nullptr : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;                  // 选项
  Options index_block_options;      // 传递给 block 的配置选项
  WritableFile* file;               // 写文件指针
  uint64_t offset;                  // 记录写入的文件偏移位置
  Status status;                     // 记录写入状态
  BlockBuilder data_block;           // data_block 工具类
  BlockBuilder index_block;          // index_block 工具类
  std::string last_key;              // 记录写入的最后一个 key
  int64_t num_entries;               // 记录当前数据 key 的数量
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;  // filter_block 工具类

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle;  // Handle to add to index block

  std::string compressed_output;
};

/***
 * @param options
 * @param file
 */
TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {

    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

/***
 * 数据写入 sstable：
 *  1. key 的写入顺序为有序， 依次递增。
 *  2. 写入 datablock 之前首先写入 filter_block
 *  3. 写入datablock
 *  4. 如果 datablock 超过限制，进行datablock 刷盘操作
 *  5. 如果刷盘操作，在 index_block 中记录上个 datablock 的索引位置
 *
 * @param key
 * @param value
 */
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;      // 获得引用
  assert(!r->closed);
  if (!ok()) return;   // 记录写入状态

  if (r->num_entries > 0) {
    // 要求排序递增
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
    /**
     * 上一个 datablock 写入完成， 需要记录一下 datablock 的index 信息
     * index_block 数据存储：
     *      格式 : block_builder
     *           key   : 上次写入的key(r->last_key)
     *           value : pending_handle(上一个datablock 写入前的offset, 上一个datablock size)
     *
     */
    assert(r->data_block.empty());
    // 比较并截断 last_key
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);              // 序列化出偏移
    r->index_block.Add(r->last_key, Slice(handle_encoding));   // 记录last key 的偏移位置
    r->pending_index_entry = false;
  }

  if (r->filter_block != nullptr) {
    // Bloom 过滤器需要写入 key
    r->filter_block->AddKey(key);
  }

  // 将插入的 key 标志入 last_key
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;                 // 数据元素增一
  r->data_block.Add(key, value);    // datablock 插入元素

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();  // 计算 datablock 当前数据量的大小

  // 如果 datablock 数据量的大小超过设置的 block_size
  // 则 进行刷写操作
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

/***
 * 1. 记录 datablock 的 BlockHandle
 * 2. datablock 数据落盘，追踪落盘的还有 compress_type crc32
 * 3. 磁盘刷盘
 * 4. 统计当前block 的 bloom 过滤器值， filter_block
 * 5. index_block 记录datablock 索引
 */
void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;  // 如果没有数据则不写

  assert(!r->pending_index_entry);

  // 写 datablock
  WriteBlock(&r->data_block, &r->pending_handle);

  if (ok()) {
    r->pending_index_entry = true;
    // 刷盘操作
    r->status = r->file->Flush();
  }
  if (r->filter_block != nullptr) {
    // filter_block 将当前 datablock 的所有key归档成bloom过滤器
    r->filter_block->StartBlock(r->offset);
  }
}

/***
 * 数据刷盘操作 :
 *   写入类型 :
 *
 *        | datablock       uint8[n]
 *        | compress_type   uint8
 *        | crc32           uint32
 *
 *   1. datablock 数据归档
 *      填充 block 的每一个重启点与block的重启点数量
 *   2. 进行压缩操作
 *   3. 统计 BlockHandle
 *   4. 将 block, 压缩类型， crc32 写盘
 *   5. 清空 block
 *
 * @param block
 * @param handle
 */
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {

  assert(ok());
  Rep* r = rep_;

  // 填充 block 剩余信息
  //  - block 的每一个重启点
  //  - block 的重启点个数
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;  // 获取压缩类型

  // 是否进行压缩
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // block 数据落盘
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();

  // 清空 block 缓存
  block->Reset();
}

/***
 * Block 数据落地
 * 数据格式 :
 *        | datablock       uint8[n]
 *        | compress_type   uint8
 *        | crc32           uint32
 *
 * @param block_contents block 数据实体
 * @param type 压缩类型  kNoCompression(0x0),  kSnappyCompression(0x1)
 * @param handle  index 工具
 */
void TableBuilder::WriteRawBlock(const Slice& block_contents, CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  handle->set_offset(r->offset);              // 记录上次写入偏移
  handle->set_size(block_contents.size());    // 记录本次写入的bock数据量大小

  // 写入 block 数据
  r->status = r->file->Append(block_contents);

  if (r->status.ok()) {

    char trailer[kBlockTrailerSize];

    trailer[0] = type;  // 写入压缩类型

    // crc32 校验
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));

    // 将压缩类型与crc 校验码追加到文件
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;  // 更新写入文件偏移
    }
  }
}

/**
 * 返回上次磁盘操作的状态
 * @return
 */
Status TableBuilder::status() const { return rep_->status; }

/***
 * 数据写入完成， 进行归档操作 ：
 *    | data_block_1 | data_block_2 | ... | data_block_n |
 *    | filter_block |
 *    | meta_index_block |
 *    | index_block |
 *    | footer |
 * @return
 */
Status TableBuilder::Finish() {
  Rep* r = rep_;

  // Data Block 剩余数据落盘
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Filter Block 数据落盘
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression, &filter_block_handle);
  }

  // Meta index block 数据落盘
  // key   : 过滤器名称
  // value : 过滤器 BlockHandle
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);

    if (r->filter_block != nullptr) {
      // 记录过滤器名称
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());

      // 记录过滤器的位置索引信息
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // IndexBlock 数据落盘
  // key   : last_key
  // value : last_key 对应的 datablock 的 BlockHandle
  if (ok()) {

    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }

    WriteBlock(&r->index_block, &index_block_handle);
  }

  // 写 Footer 数据落盘
  // metaindex_block_handle 元信息记录
  // index_block_handle     元信息记录
  // kTableMagicNumber      数据落地
  if (ok()) {
    // 填充 Footer 数据
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);

    // 序列化数据
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);

    // 写入文件
    r->status = r->file->Append(footer_encoding);

    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }

  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
