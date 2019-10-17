#include <iostream>
#include <unistd.h>
#include <cassert>
#include <fcntl.h>
#include "writable_file.h"

WritableFile::WritableFile(const std::string &filename) :
        fd_(::open(filename.c_str(), O_RDWR | O_CREAT, 0664)),
        filename_(filename),
        offset_(0),
        pos_(0) {
    assert(fd_ != -1);
}

Status WritableFile::Append(const Slice &slice) {
    Status s;
    if (pos_ + slice.size() > kWritableBufferSize) {
        if (pos_ == 0) {
            size_t bytes_left = slice.size();
            size_t offset = 0;
            while (bytes_left > 0) {
                size_t n = std::min(sizeof(buf_), bytes_left);
                std::memcpy(buf_, slice.data() + offset, n);
                pos_ = n;
                s = Flush();
                if (!s.ok()) {
                    return s;
                }
                offset += n;
                bytes_left -= n;
                offset_ += n;
            }
            return Status::OK();
        }

        s = Flush();
        if (!s.ok()) {
            return s;
        } else {
            //std::cout << "{WritableFile.Append} Flush succeeded.\n";
        }
    }
    std::memcpy(buf_ + pos_, slice.data(), slice.size());
    pos_ += slice.size();
    offset_ += slice.size();
    return Status::OK();
}

Status WritableFile::Close() {
    Status s = Flush();
    if (!s.ok()) {
        return s;
    }
    if (fd_ >= 0 && ::close(fd_) == 0) {
        fd_ = -1;
        if (offset_ == 0) {
            remove(filename_.c_str());
        }
        return Status::OK();
    }
    return Status::IOError();
}

Status WritableFile::Flush() {
    if (pos_ == 0) {
        return Status::OK();
    }
    ssize_t n = ::write(fd_, buf_, pos_);
    if (n > 0) {
        pos_ = 0;
        return Status::OK();
    }
    return Status::IOError();
}

WritableFile *NewWritableFile(const std::string &filename) {
    return new WritableFile(filename.c_str());
}
