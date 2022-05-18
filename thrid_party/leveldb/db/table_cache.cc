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
  delete tf->table;  // 调用  Table 析构函数
  delete tf->file;   // 调用  RandomAccessFile 析构函数
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  // 从 cache 中释放掉 handler
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
 */
Status TableCache::FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle) {
  Status s;

  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);    // 从 cache 中找到这个文件的缓存，key为文件编号file_number

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
      s = Table::Open(options_, file, file_size, &table);  // 构造 Table
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

/***
 * 获取指定的 sst 的 datablock 遍历查找工具
 *
 * @param options       查找选项
 * @param file_number   sst 文件编号
 * @param file_size     sst 文件大小
 * @param tableptr      table 指针
 *
 * @return              datablock 的双层遍历器
 */
Iterator* TableCache::NewIterator(const ReadOptions& options, uint64_t file_number, uint64_t file_size, Table** tableptr) {

  if (tableptr != nullptr) {
    *tableptr = nullptr;
  }

  Cache::Handle* handle = nullptr;
  Status s = FindTable(file_number, file_size, &handle);  // 读取一个 sst

  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  // 获取 sst 的 table 结构
  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;

  Iterator* result = table->NewIterator(options);  // 获取 sst 中的 datablock 访问工具

  result->RegisterCleanup(&UnrefEntry, cache_, handle);

  if (tableptr != nullptr) {
    *tableptr = table;
  }
  return result;
}

/***
 *
 *  从指定的 sst 文件中查找指定的key, 如果找到则调用 handle_result(args, key, value)
 *  这是 Table->InternalGet() 的封装版本
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

  // 查找SST
  Status s = FindTable(file_number, file_size, &handle);

  if (s.ok()) {
    // 获取SST的操作句柄Table
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    // 从Table中查询对应的key
    s = t->InternalGet(options, k, arg, handle_result);         //从Table中读取
    cache_->Release(handle);   // 拿到以后要及时释放， 防止内存溢出
  }

  return s;
}

/***
 * 从 cache 中删除这个缓存
 * @param file_number
 */
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

}  // namespace leveldb
