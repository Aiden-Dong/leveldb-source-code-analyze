// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// File names used by DB code

#ifndef STORAGE_LEVELDB_DB_FILENAME_H_
#define STORAGE_LEVELDB_DB_FILENAME_H_

#include <cstdint>
#include <string>

#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "port/port.h"

/****
 * leveldb 文件操作相关类
 *
 * leveldb 涉及到的相关文件名称 :
 *
 *         - kLogFile         --------------------- dbname/[0-9].log
 *                            WAL
 *                            [日志文件 (*.log)] 存储最近 update 的序列。 每次更新都追加到当前日志文件。
 *                            当日志文件达到预定大小时（默认约 4MB），转换为SST（见下文）并为将来的更新创建一个新的日志文件。
 *                            当前日志文件的副本保存在内存结构中（memtable）。 每次读取时都会查询此副本，以便读取操作反映所有记录的更新。
 *         - kTableFile       --------------------- dbname/[0-9].(sst|ldb)
 *                            [排序表 (*.ldb)] 存储按关键字排序的条目序列。 每个条目要么是键的值，要么是键的删除标记。 （保留删除标记以隐藏旧排序表中存在的过时值）。
 *                            这组排序表被组织成某一系列文件，分布在不同的level。 从日志文件生成的排序表被放置在一个特殊的 young 级别（也称为 level-0）。
 *                            当年轻文件的数量超过某个阈值（目前为4个）时，所有年轻文件与所有重叠的 level-1文件合并在一起，以生成一系列新的 level-1文件（我们创建一个新的 level-1级文件, 每 2MB 数据一个文件。）
 *                            level-0 中的文件可能包含重叠的键。 然而，其他级别的文件不存在重叠键。
 *                            当级别 L 中文件的组合大小超过 (10^L) MB，这时将其中中的一个文件 level-L，将 level-(L+1) 中的所有重叠文件合并，形成 level-(L+1) 的一组新文件。
 *                            这样的整合操作可以把 young level 中新更新的数据逐渐往下一个level中迁移(这样可以使查询的成本降低到最小)。
 *         - kDBLockFile      --------------------- dbname/LOCK
 *
 *         - kDescriptorFile  --------------------- dbname/MANIFEST-[0-9]
 *                            MANIFEST 文件列出了组成每个 level 的一组排序表、相应的键范围和其他重要的元数据。
 *                            每当重新打开数据库时，都会创建一个新的 MANIFEST 文件（文件名中嵌入了一个新编号）。
 *                            MANIFEST 文件被格式化为日志，并且对服务状态所做的更改（如文件的添加或删除 VersionEdit）追加到此日志中。
 *         - kCurrentFile     --------------------- dbname/CURRENT        记载当前manifest文件名
 *                            CURRENT 是一个简单的文本文件，其中包含最新的 MANIFEST 文件的名称。
 *         - kTempFile        --------------------- dbname/[0-9].dbtmp    在 repair 数据库时，会重放wal日志，将这些和已有的sst合并写到临时文件中去，成功之后调用 rename 原子重命名
 *         - kInfoLogFile     --------------------- dbname/LOG.old 或者 dbname/LOG
 *
 *
 *         Level-0
 *           当日志文件增长超过一定大小(默认4MB) 时:
 *           创建一个全新的内村表和日志文件， 并在此处指导未来的更新
 *           在后台:
 *           1. 将之前memtable的内容写入 sstable
 *           2. 丢弃 memtable
 *           3. 删除旧的日志文件和旧的memtable.
 *           4. 将新的sstable 添加到 young(level-0)级别
 */

namespace leveldb {

class Env;

enum FileType {
  kLogFile,                // WAL
  kDBLockFile,             // 文件锁
  kTableFile,              // SST
  kDescriptorFile,         // MANIFEST
  kCurrentFile,            // CURRENT MENIFEST LINK
  kTempFile,               //
  kInfoLogFile             // Process Log
};

// Return the name of the log file with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string LogFileName(const std::string& dbname, uint64_t number);

// Return the name of the sstable with the specified number
// in the db named by "dbname".  The result will be prefixed with
// "dbname".
std::string TableFileName(const std::string& dbname, uint64_t number);

// Return the legacy file name for an sstable with the specified number
// in the db named by "dbname". The result will be prefixed with
// "dbname".
std::string SSTTableFileName(const std::string& dbname, uint64_t number);

// Return the name of the descriptor file for the db named by
// "dbname" and the specified incarnation number.  The result will be
// prefixed with "dbname".
std::string DescriptorFileName(const std::string& dbname, uint64_t number);

// Return the name of the current file.  This file contains the name
// of the current manifest file.  The result will be prefixed with
// "dbname".
std::string CurrentFileName(const std::string& dbname);

// Return the name of the lock file for the db named by
// "dbname".  The result will be prefixed with "dbname".
std::string LockFileName(const std::string& dbname);

// Return the name of a temporary file owned by the db named "dbname".
// The result will be prefixed with "dbname".
std::string TempFileName(const std::string& dbname, uint64_t number);

// Return the name of the info log file for "dbname".
std::string InfoLogFileName(const std::string& dbname);

// Return the name of the old info log file for "dbname".
std::string OldInfoLogFileName(const std::string& dbname);

// If filename is a leveldb file, store the type of the file in *type.
// The number encoded in the filename is stored in *number.  If the
// filename was successfully parsed, returns true.  Else return false.
bool ParseFileName(const std::string& filename, uint64_t* number,
                   FileType* type);

// Make the CURRENT file point to the descriptor file with the
// specified number.
Status SetCurrentFile(Env* env, const std::string& dbname,
                      uint64_t descriptor_number);

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_FILENAME_H_
