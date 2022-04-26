// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_BUILDER_H_
#define STORAGE_LEVELDB_DB_BUILDER_H_

#include "leveldb/status.h"

namespace leveldb {

struct Options;
struct FileMetaData;

class Env;
class Iterator;
class TableCache;
class VersionEdit;

/****
 * 将 IMemTable 数据刷写到磁盘形成SST, 此过程在MemTable缓冲区到达上限数据落地时触发
 * 过程调用TableBuilder建立SST
 * SST 落地在level0
 *
 * @param dbname        数据库名称
 * @param env           系统相关操作句柄
 * @param options       配置选项
 * @param table_cache   SST读取接口
 * @param iter          MemTable 迭代器
 * @param meta          用于存储SST文件元信息
 */
Status BuildTable(const std::string& dbname, Env* env, const Options& options,
                  TableCache* table_cache, Iterator* iter, FileMetaData* meta);

}

#endif  // STORAGE_LEVELDB_DB_BUILDER_H_
