#ifndef TABLE_STORAGE_TESTHELPER_H
#define TABLE_STORAGE_TESTHELPER_H

#include <string>
#include "env.h"
#include "coding.h"

class TestHelper {
 public:
  TestHelper() : env_(CreateEnv()) {}
  ~TestHelper() { delete env_; }

  Status LoadLastQueryResultsFromFile(const std::string& filename,
                                      int* result) {
    RandomAccessFile* f = nullptr;
    Status status;
    Slice slice;
    char scratch[sizeof(int)];

    status = env_->NewRandomAccessFile(filename, &f);
    if (status.ok()) {
      status = f->Read(0, sizeof(int), scratch, &slice);
      if (status.ok()) {
        *result = static_cast<int>(DecodeFixed32(scratch));
      }
      delete f;
    }
    return status;
  }

  Status SaveLastQueryResultsToFile(const std::string& filename, int x) {
    WritableFile* f = nullptr;
    Status status;
    status = env_->NewWritableFile(filename, &f);
    if (status.ok()) {
      std::string d;
      PutFixed32(&d, static_cast<uint32_t>(x));
      status = f->Append(d);
      delete f;
    }
    return status;
  }

 private:
  Env* env_;
};

#endif /* TABLE_STORAGE_TESTHELPER_H */
