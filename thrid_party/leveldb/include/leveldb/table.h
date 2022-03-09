// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_TABLE_H_
#define STORAGE_LEVELDB_INCLUDE_TABLE_H_

#include <cstdint>

#include "leveldb/export.h"
#include "leveldb/iterator.h"

namespace leveldb {

class Block;
class BlockHandle;
class Footer;
struct Options;
class RandomAccessFile;
struct ReadOptions;
class TableCache;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class LEVELDB_EXPORT Table {
 public:

  /***
   * 静态函数， 而且唯一是共有的接口， 构造 Table 对象
   * 主要负责解析出基本数据，如 Footer, 然后解析出 metaindex_block, index_block, filter_block
   * BlockContexts 对象到Block的转换， 其中主要是计算出 restart_offset_ 而且 Block 是可以被遍历的
   *
   * @param options 配置选项
   * @param file  需要读取的文件
   * @param file_size  文件的大小
   * @param table 需要构建的table
   * @return
   */
  static Status Open(const Options& options, RandomAccessFile* file, uint64_t file_size, Table** table);

  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;

  ~Table();

  /***
   * 返回 index block 的迭代器
   * 目的是为了确定 key 位于那个 datablock中
   */
  Iterator* NewIterator(const ReadOptions&) const;

  // Given a key, return an approximate byte offset in the file where
  // the data for that key begins (or would begin if the key were
  // present in the file).  The returned value is in terms of file
  // bytes, and so includes effects like compression of the underlying data.
  // E.g., the approximate offset of the last key in the table will
  // be close to the file length.
  uint64_t ApproximateOffsetOf(const Slice& key) const;

 private:
  friend class TableCache;

  struct Rep;


  /***
   * 提供给外部调用的回调函数， 用来加载 datablock
   */
  static Iterator* BlockReader(void*param, const ReadOptions&, const Slice&);

  explicit Table(Rep* rep) : rep_(rep) {}

  // 查找指定的 key, 内部先 bf 判断， 然后缓存获取，最后在读取文件
  Status InternalGet(const ReadOptions&, const Slice& key, void* arg, void (*handle_result)(void* arg, const Slice& k, const Slice& v));

  /**
   * 读取元数据， 主要是 Footer, 然后是 metaindex_handler, fil
   * @param footer
   */
  void ReadMeta(const Footer& footer);

  /***
   * 根据 Filter handler 还原 filter block 数据
   * @param filter_handle_value
   */
  void ReadFilter(const Slice& filter_handle_value);

  /***
 * struct Table::Rep {
 *   Options options;                // Table 相关参数信息
 *   Status status;                  // Table 相关状态信息
 *   RandomAccessFile* file;         // Table 所持有的文件
 *   uint64_t cache_id;              // Table 本身的缓存 id
 *   FilterBlockReader* filter;      // filter block 块的读取
 *   const char* filter_data;        // 保存对应的filter 数据
 *   BlockHandle metaindex_handle;   // 解析保存 metaindex_handler
 *   Block* index_block;             // index block 数据
 * };
 */
  Rep* const rep_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_TABLE_H_
