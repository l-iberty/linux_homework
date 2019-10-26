#ifndef TABLE_STORAGE_STATUS_H
#define TABLE_STORAGE_STATUS_H

#include "slice.h"

class Status {
 public:
  Status() : code_(kOk), msg_("") {}

  std::string ToString() const { return msg_.ToString(); }

  bool ok() const { return code_ == kOk; }

  bool IsNotFound() const { return code_ == kNotFound; }

  bool IsIOError() const { return code_ == kIOError; }

  bool IsGeneralError() const { return code_ == kGeneralError; }

  static Status OK() { return Status(kOk, ""); }

  static Status NotFound() { return Status(kNotFound, "not found"); }

  static Status IOError() { return Status(kIOError, "IO error"); }

  static Status GeneralError(const Slice& msg) {
    return Status(kGeneralError, msg);
  }

 private:
  enum Code { kOk = 0, kNotFound = 1, kIOError = 2, kGeneralError = 3 };

  Code code_;
  Slice msg_;

  Status(Code code, const Slice& msg) : code_(code), msg_(msg) {}
};

#endif /* TABLE_STORAGE_STATUS_H */
