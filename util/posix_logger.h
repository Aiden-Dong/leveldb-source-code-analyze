// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Logger implementation that can be shared by all environments
// where enough posix functionality is available.

#ifndef STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
#define STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_

#include <sys/time.h>

#include <cassert>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <sstream>
#include <thread>

#include "leveldb/env.h"

namespace leveldb {

class PosixLogger final : public Logger {
 public:
  // Creates a logger that writes to the given file.
  //
  // The PosixLogger instance takes ownership of the file handle.
  explicit PosixLogger(std::FILE* fp) : fp_(fp) { assert(fp != nullptr); }

  ~PosixLogger() override { std::fclose(fp_); }

  void Logv(const char* format, std::va_list arguments) override {
    // Record the time as close to the Logv() call as possible.
    struct ::timeval now_timeval;
    ::gettimeofday(&now_timeval, nullptr);
    const std::time_t now_seconds = now_timeval.tv_sec;
    struct std::tm now_components;
    ::localtime_r(&now_seconds, &now_components);

    // Record the thread ID.
    constexpr const int kMaxThreadIdSize = 32;
    std::ostringstream thread_stream;

    // 获取线程id
    thread_stream << std::this_thread::get_id();
    std::string thread_id = thread_stream.str();   //
    if (thread_id.size() > kMaxThreadIdSize) {
      thread_id.resize(kMaxThreadIdSize);
    }

    // We first attempt to print into a stack-allocated buffer. If this attempt
    // fails, we make a second attempt with a dynamically allocated buffer.
    constexpr const int kStackBufferSize = 512;
    char stack_buffer[kStackBufferSize];
    static_assert(sizeof(stack_buffer) == static_cast<size_t>(kStackBufferSize), "sizeof(char) is expected to be 1 in C++");

    int dynamic_buffer_size = 0;  // Computed in the first iteration.
    for (int iteration = 0; iteration < 2; ++iteration) {
      // 第一次使用  stack_buffer[kStackBufferSize] 栈对象， 如果数据体太大超过栈空间则第二次重新分配
      // 第二次将使用 堆对象
      const int buffer_size =(iteration == 0) ? kStackBufferSize : dynamic_buffer_size;
      char* const buffer =(iteration == 0) ? stack_buffer : new char[dynamic_buffer_size];

      // 写入时间与线程ID
      int buffer_offset = std::snprintf(
          buffer, buffer_size, "%04d/%02d/%02d-%02d:%02d:%02d.%06d %s ",
          now_components.tm_year + 1900, now_components.tm_mon + 1,
          now_components.tm_mday, now_components.tm_hour, now_components.tm_min,
          now_components.tm_sec, static_cast<int>(now_timeval.tv_usec),
          thread_id.c_str());

      // The header can be at most 28 characters (10 date + 15 time +
      // 3 delimiters) plus the thread ID, which should fit comfortably into the
      // static buffer.
      assert(buffer_offset <= 28 + kMaxThreadIdSize);
      static_assert(28 + kMaxThreadIdSize < kStackBufferSize, "stack-allocated buffer may not fit the message header");
      assert(buffer_offset < buffer_size);

      // 写入用户自定义消息体
      std::va_list arguments_copy;
      va_copy(arguments_copy, arguments);
      buffer_offset += std::vsnprintf(buffer + buffer_offset, buffer_size - buffer_offset,format, arguments_copy);
      va_end(arguments_copy);

      // 数据体太大，无法栈上分配
      if (buffer_offset >= buffer_size - 1) {
        // The message did not fit into the buffer.
        if (iteration == 0) {
          dynamic_buffer_size = buffer_offset + 2;
          continue;
        }

        assert(false);
        buffer_offset = buffer_size - 1;
      }

      // Add a newline if necessary.
      if (buffer[buffer_offset - 1] != '\n') {
        buffer[buffer_offset] = '\n';
        ++buffer_offset;
      }

      // 数据刷盘
      assert(buffer_offset <= buffer_size);
      std::fwrite(buffer, 1, buffer_offset, fp_);
      std::fflush(fp_);

      if (iteration != 0) {
        delete[] buffer;
      }
      break;
    }
  }

 private:
  std::FILE* const fp_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_POSIX_LOGGER_H_
