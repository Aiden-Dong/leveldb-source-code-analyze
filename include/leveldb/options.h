// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
#define STORAGE_LEVELDB_INCLUDE_OPTIONS_H_

#include <cstddef>
#include "leveldb/export.h"

/***
 * leveldb 数据库选项
 */

namespace leveldb {

class Cache;
class Comparator;
class Env;
class FilterPolicy;
class Logger;
class Snapshot;

// 数据库块压缩类型 
enum CompressionType {
  kNoCompression = 0x0,      // 无压缩类型
  kSnappyCompression = 0x1   // snappy 压缩类型
};

// Options to control the behavior of a database (passed to DB::Open)
struct LEVELDB_EXPORT Options {
  // Create an Options object with default values for all fields.
  Options();

  // key 的比较器， 默认是字典序
  const Comparator* comparator;

  // 打开数据库， 如果数据库不存在， 是否创建新的
  bool create_if_missing = false;

  // 打开数据库， 如果数据库存在，是否抛出错误
  bool error_if_exists = false;

  // 如果为 true, 则实现将其正在处理的数据进行积极检查， 如果检查到任何错误， 
  // 则提前停止。这可能会产生不可预见的后果。 
  // 例如 : 一个数据库条目的损坏可能导致大量条目变得不可读或者整个数据库变得无法打开
  bool paranoid_checks = false;

  // 封装了平台相关接口
  Env* env;

  // db 日志句柄
  Logger* info_log = nullptr;


  // mmetable的缓存区大小 默认 4M
  // 值大有利于性能提升
  // 但是内存可能会存在两份， 太大需要注意oom
  // 过大刷盘后，不利于数据恢复
  size_t write_buffer_size = 4 * 1024 * 1024;

  // 允许打开的最大文件数
  int max_open_files = 1000;

  // 块缓存
  // 对块的控制（用户数据存储在一组块中，块是从磁盘读取的单位）。
  // 如果非空，则为块使用指定的缓存。
  // 如果为 null，leveldb 将自动创建并使用 8MB 内部缓存。
  Cache* block_cache = nullptr;

  // 每个 block 的数据包大小
  // 每个块打包的用户数据的近似大小。请注意，此处指定的块大小对应于未压缩的数据。
  // 如果启用压缩，从磁盘读取的单元的实际大小可能会更小。此参数可以动态更改。
  size_t block_size = 4 * 1024; // 4KB

  // block 中记录完整 key 的间隔
  int block_restart_interval = 16;

  // 生成新文件的阈值(对于性能较好的文件系统可以调大该阈值，但是会增加数据恢复的时间)， 
  // 默认2M
  size_t max_file_size = 2 * 1024 * 1024;

  
  // 数据压缩类型
  CompressionType compression = kSnappyCompression;

  // 是否复用之前的 MANIFES 和 log files
  bool reuse_logs = false;

  // block 块中的过滤策略， 支持布隆过滤器
  const FilterPolicy* filter_policy = nullptr;
};

// Options that control read operations
struct LEVELDB_EXPORT ReadOptions {
  ReadOptions() = default;

  // 是否对从磁盘读取的数据进行校验
  bool verify_checksums = false;

  // 读取的 block 数据，是否加入到 cache 中
  bool fill_cache = true;

  // 记录当前的快照
  const Snapshot* snapshot = nullptr;
};

// Options that control write operations
struct LEVELDB_EXPORT WriteOptions {
  WriteOptions() = default;

  // 是否同步刷盘也就是调完 wirte 之后是否需要显示 fsync
  bool sync = false;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_INCLUDE_OPTIONS_H_
