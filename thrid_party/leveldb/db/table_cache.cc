// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
};

/**
 * 节点清理器
 * @param key
 * @param value
 */
static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  delete tf->table;  // 调用 RandomAccessFile 析构函数
  delete tf->file;   // 调用 Table 析构函数
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

/***
 * TableCache 构造器
 * @param dbname 数据表名称
 * @param options 配置选项
 * @param entries table cache 容量
 */
TableCache::TableCache(const std::string& dbname, const Options& options, int entries)
    : env_(options.env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)) {}

TableCache::~TableCache() { delete cache_; }

/***
 * 从 LRU 缓存表中找到 {dbname_}/{file_number}.ldb 的chache
 * 如果不存在，则创建一个新的缓存放入到 LRU 中
 *
 * @param file_number sst 的文件编码
 * @param file_size  文件大小
 * @param handle cache 节点
 * @return
 */
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle) {
  Status s;

  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));

  *handle = cache_->Lookup(key);  // 从 cache 中找到这个文件的缓存

  if (*handle == nullptr) {

    std::string fname = TableFileName(dbname_, file_number);  // {dbname_}/{file_number}.ldb

    RandomAccessFile* file = nullptr;
    Table* table = nullptr;

    // SSTable 随机访问器， 因为涉及到了索引信息
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      // {dbname_}/{file_number}.sst (旧的文件格式)
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == nullptr);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);  // 数据插入到 LRU 缓存链中, 并将 *handle 指向缓存节点
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size, Table** tableptr) {
  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

/***
 *
 *
 * @param options
 * @param file_number
 * @param file_size
 * @param k
 * @param arg
 * @param handle_result
 * @return
 */
Status TableCache::Get(const ReadOptions& options, uint64_t file_number, uint64_t file_size, const Slice& k, void* arg,
                       void (*handle_result)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);

  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, handle_result);
    cache_->Release(handle);   // 拿到以后要及时释放， 防止内存溢出
  }

  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
