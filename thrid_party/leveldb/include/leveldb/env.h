// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// An Env is an interface used by the leveldb implementation to access
// operating system functionality like the filesystem etc.  Callers
// may wish to provide a custom Env object when opening a database to
// get fine gain control; e.g., to rate limit file system operations.
//
// All Env implementations are safe for concurrent access from
// multiple threads without any external synchronization.

#ifndef STORAGE_LEVELDB_INCLUDE_ENV_H_
#define STORAGE_LEVELDB_INCLUDE_ENV_H_

#include <cstdarg>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/export.h"
#include "leveldb/status.h"

// This workaround can be removed when leveldb::Env::DeleteFile is removed.
#if defined(_WIN32)
// On Windows, the method name DeleteFile (below) introduces the risk of
// triggering undefined behavior by exposing the compiler to different
// declarations of the Env class in different translation units.
//
// This is because <windows.h>, a fairly popular header file for Windows
// applications, defines a DeleteFile macro. So, files that include the Windows
// header before this header will contain an altered Env declaration.
//
// This workaround ensures that the compiler sees the same Env declaration,
// independently of whether <windows.h> was included.
#if defined(DeleteFile)
#undef DeleteFile
#define LEVELDB_DELETEFILE_UNDEFINED
#endif  // defined(DeleteFile)
#endif  // defined(_WIN32)

namespace leveldb {

class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class Slice;
class WritableFile;

class LEVELDB_EXPORT Env {
 public:
  Env();

  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;

  virtual ~Env();

  // 默认 PosixDefaultEnv
  static Env* Default();

  // 创建一个顺序读取文件(fname)的对象
  // 操作对象存储到 result 中
  // 通过 Status 返回值告知创建是否成功
  // 外部同步
  virtual Status NewSequentialFile(const std::string& fname, SequentialFile** result) = 0;

  // 创建一个随机读取文件(fname)的对象
  // 操作对象存储到 result 中
  // 通过 Status 返回值告知创建是否成功
  // 线程安全.
  virtual Status NewRandomAccessFile(const std::string& fname, RandomAccessFile** result) = 0;

  // 创建一个新的文件(fname, 如果已存在则删除已存在文件)，并返回一个写对象
  // The returned file will only be accessed by one thread at a time.
  virtual Status NewWritableFile(const std::string& fname, WritableFile** result) = 0;

  // 创建一个追加写操作的写对象，如果文件(fname)不存在,则创建新对象
  virtual Status NewAppendableFile(const std::string& fname, WritableFile** result);

  // 判断文件是否存在
  virtual bool FileExists(const std::string& fname) = 0;

  // 获取目录下面对象的文件并返回
  virtual Status GetChildren(const std::string& dir, std::vector<std::string>* result) = 0;

  // 删除文件
  virtual Status RemoveFile(const std::string& fname);

  // 删除文件操作
  // 默认调用 RemoveFile
  virtual Status DeleteFile(const std::string& fname);

  // 创建一个目录
  virtual Status CreateDir(const std::string& dirname) = 0;

  // 删除目录
  virtual Status RemoveDir(const std::string& dirname);

  // 删除目录
  // 默认调用 RemoveDir
  virtual Status DeleteDir(const std::string& dirname);

  // 获取文件的大小
  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

  // 文件重命名
  virtual Status RenameFile(const std::string& src, const std::string& target) = 0;

  // 执行文件锁操作
  virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

  // 文件锁解锁操作
  virtual Status UnlockFile(FileLock* lock) = 0;

  // 调教一个任务到后台线程去执行
  virtual void Schedule(void (*function)(void* arg), void* arg) = 0;

  // 启动一个线程去异步执行 (*function)(args) 操作
  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  // *path设置为可用于测试的临时目录。
  // 它可能是也可能不是刚刚创建的。同一进程的不同运行的目录可能不同，也可能不同，但后续调用将返回相同的目录。
  virtual Status GetTestDirectory(std::string* path) = 0;

  // 创建一个日志文件，用于记录操作日志
  virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

  // 返回当前时刻的微妙数
  virtual uint64_t NowMicros() = 0;

  // 休眠一个微妙数时间
  virtual void SleepForMicroseconds(int micros) = 0;
};


/***
 * 顺序型文件操作对象
 */
class LEVELDB_EXPORT SequentialFile {
 public:
  SequentialFile() = default;

  // 不能赋值与拷贝
  SequentialFile(const SequentialFile&) = delete;
  SequentialFile& operator=(const SequentialFile&) = delete;

  virtual ~SequentialFile();

  // 从文件中最多读取“n”个字节。
  // “scratch[0..n-1]”可以由该例程编写。
  // 将“*result”设置为已读取的数据（包括成功读取的字节数是否少于“n”）。
  // 可以将“*result”设置为指向“scratch[0..n-1]”中的数据，因此使用“*result”时，“scratch[0..n-1]”必须处于活动状态。
  // 如果遇到错误，则返回非OK状态。
  // 非线程安全
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  // 跳过文件中的“n”字节。这保证不会比读取相同数据慢，但可能会更快。
  //如果到达文件末尾，skip 将在文件末尾停止，并返回OK。
  virtual Status Skip(uint64_t n) = 0;
};


/***
 * 随机访问文件操作对象
 */
class LEVELDB_EXPORT RandomAccessFile {
 public:
  RandomAccessFile() = default;

  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;

  virtual ~RandomAccessFile();

  // 从 offset 处最多读取 n 个字节， 并返回给 result., result 指向 scratch
  virtual Status Read(uint64_t offset, size_t n, Slice* result, char* scratch) const = 0;
};

/***
 * 对文件进行写操作的对象
 */
class LEVELDB_EXPORT WritableFile {
 public:
  WritableFile() = default;

  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;

  virtual ~WritableFile();

  // 追加写操作
  virtual Status Append(const Slice& data) = 0;

  // 关闭文件操作
  virtual Status Close() = 0;
  // 数据刷盘
  virtual Status Flush() = 0;

  virtual Status Sync() = 0;
};

/***
 * 日志接口
 */
class LEVELDB_EXPORT Logger {
 public:
  Logger() = default;

  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;

  virtual ~Logger();

  //以指定的格式将条目写入日志文件。
  virtual void Logv(const char* format, std::va_list ap) = 0;
};

// 文件锁接口.
class LEVELDB_EXPORT FileLock {
 public:
  FileLock() = default;

  FileLock(const FileLock&) = delete;
  FileLock& operator=(const FileLock&) = delete;

  virtual ~FileLock();
};

// Log the specified data to *info_log if info_log is non-null.
void Log(Logger* info_log, const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 2, 3)))
#endif
    ;

// 将数据写入到文件
LEVELDB_EXPORT Status WriteStringToFile(Env* env, const Slice& data, const std::string& fname);

// 从文件中读取数据
LEVELDB_EXPORT Status ReadFileToString(Env* env, const std::string& fname, std::string* data);


/***
 * Env 的代理类
 */
class LEVELDB_EXPORT EnvWrapper : public Env {
 public:
  // Initialize an EnvWrapper that delegates all calls to *t.
  explicit EnvWrapper(Env* t) : target_(t) {}
  virtual ~EnvWrapper();

  // Return the target to which this Env forwards all calls.
  Env* target() const { return target_; }

  // The following text is boilerplate that forwards all methods to target().
  Status NewSequentialFile(const std::string& f, SequentialFile** r) override {
    return target_->NewSequentialFile(f, r);
  }
  Status NewRandomAccessFile(const std::string& f,
                             RandomAccessFile** r) override {
    return target_->NewRandomAccessFile(f, r);
  }
  Status NewWritableFile(const std::string& f, WritableFile** r) override {
    return target_->NewWritableFile(f, r);
  }
  Status NewAppendableFile(const std::string& f, WritableFile** r) override {
    return target_->NewAppendableFile(f, r);
  }
  bool FileExists(const std::string& f) override {
    return target_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_->GetChildren(dir, r);
  }
  Status RemoveFile(const std::string& f) override {
    return target_->RemoveFile(f);
  }
  Status CreateDir(const std::string& d) override {
    return target_->CreateDir(d);
  }
  Status RemoveDir(const std::string& d) override {
    return target_->RemoveDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }
  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }
  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }
  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }
  void Schedule(void (*f)(void*), void* a) override {
    return target_->Schedule(f, a);
  }
  void StartThread(void (*f)(void*), void* a) override {
    return target_->StartThread(f, a);
  }
  Status GetTestDirectory(std::string* path) override {
    return target_->GetTestDirectory(path);
  }
  Status NewLogger(const std::string& fname, Logger** result) override {
    return target_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return target_->NowMicros(); }
  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }

 private:
  Env* target_;
};

}  // namespace leveldb

#if defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)
#if defined(UNICODE)
#define DeleteFile DeleteFileW
#else
#define DeleteFile DeleteFileA
#endif  // defined(UNICODE)
#endif  // defined(_WIN32) && defined(LEVELDB_DELETEFILE_UNDEFINED)

#endif  // STORAGE_LEVELDB_INCLUDE_ENV_H_
