#ifndef TABLE_STORAGE_STATUS_H
#define TABLE_STORAGE_STATUS_H


#include "slice.h"

class Status {
public:
    Status() : msg_("") {}

    std::string ToString() const { return std::string(msg_.data(), msg_.size()); }

    bool ok() const { return code_ == kOk; }

    static Status OK() { return Status(kOk, ""); }

    static Status NotFound() { return Status(kNotFound, "not found"); }

    static Status IOError() { return Status(kIOError, "IO error"); }

    static Status TooMuchData() { return Status(kTooMuchData, "too much data"); }

    static Status GeneralError() { return Status(kGeneralError, "general error"); }

private:
    enum Code {
        kOk = 0,
        kNotFound = 1,
        kIOError = 2,
        kTooMuchData = 3,
        kGeneralError = 4
    };

    Code code_;
    Slice msg_;

    Status(Code code, const Slice &msg) : code_(code), msg_(msg) {}
};

#endif /* TABLE_STORAGE_STATUS_H */
