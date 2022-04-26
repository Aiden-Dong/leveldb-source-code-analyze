// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include "db/dbformat.h"
#include "db/filename.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {


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
Status BuildTable(const std::string& dbname,
                  Env* env,
                  const Options& options,
                  TableCache* table_cache,
                  Iterator* iter,
                  FileMetaData* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();   // MemTable 迭代器初始化

  std::string fname = TableFileName(dbname, meta->number); // 获取sst文件名称:{sst_number}.ldb

  if (iter->Valid()) {
    WritableFile* file;
    s = env->NewWritableFile(fname, &file);  // 创建sst文件刷写句柄

    if (!s.ok()) {
      return s;
    }

    TableBuilder* builder = new TableBuilder(options, file);  // SST 构造器

    meta->smallest.DecodeFrom(iter->key());                   // 在 file_meta 中记录最小 key
    Slice key;

    for (; iter->Valid(); iter->Next()) {
      key = iter->key();
      // 将数据填充到 datablock中，此时会顺便填充 filterblock 与 metaindexblock
      builder->Add(key, iter->value());
    }

    // 填写完成后， 在file_meta中记录最大Key
    if (!key.empty()) {
      meta->largest.DecodeFrom(key);
    }

    // 完成构建， 做 datablock, filterblock, metaindexblock, indexblock, footer收尾工作
    s = builder->Finish();

    // 在 file_meta 中记录文件大小
    if (s.ok()) {
      meta->file_size = builder->FileSize();
      assert(meta->file_size > 0);
    }
    delete builder;

    // 数据从磁盘缓冲区刷写到磁盘
    if (s.ok()) {
      s = file->Sync();
    }

    // 关闭文件句柄
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;

    if (s.ok()) {
      // 校验表是否可用
      // 同时这个操作将SST加入了缓冲区
      Iterator* it = table_cache->NewIterator(ReadOptions(), meta->number, meta->file_size);
      s = it->status();
      delete it;
    }
  }

  // Check for input iterator errors
  if (!iter->status().ok()) {
    s = iter->status();
  }

  if (s.ok() && meta->file_size > 0) {
    // Keep it
  } else {
    env->RemoveFile(fname);
  }
  return s;
}

}  // namespace leveldb
