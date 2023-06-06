// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#ifndef STORAGE_LEVELDB_DB_TABLE_CACHE_H_
#define STORAGE_LEVELDB_DB_TABLE_CACHE_H_

#include <cstdint>
#include <string>

#include "db/dbformat.h"
#include "leveldb/cache.h"
#include "leveldb/table.h"
#include "port/port.h"

namespace leveldb {

class Env;

class TableCache {
 public:
  /**
   * Table Cache 构造器
   * @param dbname 数据表名字
   * @param options cache 配置选项
   * @param entries cache 容量
   */
  TableCache(const std::string& dbname, const Options& options, int entries);

  TableCache(const TableCache&) = delete;
  TableCache& operator=(const TableCache&) = delete;

  ~TableCache();


  /***
   * 返回改 sst 的 二级索引 TwoLevelIterator
   * 调用的 table 的NewIterator(...)
   *
   * @param options 读取选项
   * @param file_number sst 文件名称
   * @param file_size 文件大小
   * @param tableptr Table
   * @return
   */
  Iterator* NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size, Table** tableptr = nullptr);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value).
  // 查询指定的 key
  Status Get(const ReadOptions& options, uint64_t file_number,uint64_t file_size, const Slice& k, void* arg, void (*handle_result)(void*, const Slice&, const Slice&));

  // 淘汰指定的SST句柄
  void Evict(uint64_t file_number);

 private:

  // 查找指定的SSTable, 优先去缓存找， 找不到则去磁盘读取， 读取后插入到缓存中
  Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle**);

  Env* const env_;                      // 读取 SST 文件
  const std::string dbname_;            // sst 名字
  const Options& options_;              // Cache 参数配置

  /**
   * key   : {sst_file_number}
   * value : {TableAndFile}
   *
   * 说明 :
   *   table 中使用的是 option 中的全局 blockcache
   *       格式 : {cache_id, offset} -> datablock
   *       cacheid 是table中的内部变量
   *   tablecache 中使用的是 tablecache 内部cache
   *       格式 : {sst_file_number} -> {file + table}
   *
   *   需要注意的是当 tablecache 中的 table 缓存失效是， option 中的全局 blockcache 有部分 block 将无效
   */
  Cache* cache_;                        // ShardedLRUCache
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_TABLE_CACHE_H_
