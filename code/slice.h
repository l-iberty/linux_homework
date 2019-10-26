#ifndef TABLE_STORAGE_SLICE_H
#define TABLE_STORAGE_SLICE_H

#include <string>
#include <cstring>

class Slice {
 public:
  Slice() : data_(nullptr), size_(0) {}

  Slice(const char* d, size_t n) : data_(d), size_(n) {}

  Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}

  Slice(const char* s) : data_(s), size_(strlen(s)) {}

  Slice(const Slice&) = default;

  Slice& operator=(const Slice&) = default;

  const char* data() const { return data_; }

  size_t size() const { return size_; }

  std::string ToString() const { return std::string(data_, size_); }

 private:
  const char* data_;
  size_t size_;
};

#endif /* TABLE_STORAGE_SLICE_H */
