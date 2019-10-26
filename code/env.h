#ifndef TABLE_STORAGE_ENV_H
#define TABLE_STORAGE_ENV_H

#include <cstdint>
#include "status.h"

class Env;
class RandomAccessFile;
class WritableFile;

Env* CreateEnv();

class Env {
 public:
  Env() = default;

  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;

  virtual ~Env() = default;

  virtual Status GetFileSize(const std::string& filename, size_t* size) = 0;

  virtual Status NewRandomAccessFile(const std::string& filename,
                                     RandomAccessFile** result) = 0;

  virtual Status NewWritableFile(const std::string& filename,
                                 WritableFile** result) = 0;

  virtual Status OpenWritableFile(const std::string& filename,
                                  WritableFile** result) = 0;
};

class RandomAccessFile {
 public:
  RandomAccessFile() = default;

  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;

  virtual ~RandomAccessFile() = default;

  virtual Status Read(uint64_t offset, size_t nbytes, char* scratch,
                      Slice* slice) = 0;
};

class WritableFile {
 public:
  WritableFile() = default;

  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;

  virtual ~WritableFile() = default;

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
};

#endif /* TABLE_STORAGE_ENV_H */
