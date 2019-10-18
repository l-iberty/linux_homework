#ifndef TABLE_STORAGE_POSIXENV_H
#define TABLE_STORAGE_POSIXENV_H

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <cassert>
#include "env.h"


class PosixRandomAccessFile : public RandomAccessFile {
public:
    PosixRandomAccessFile(const std::string &filename, int fd, void *mmap_base, size_t length) :
            filename_(std::move(filename)),
            fd_(fd),
            mmap_base_(mmap_base),
            mmap_length_(length) {
        assert(fd_ != -1);
        assert(mmap_base_ != nullptr);
        assert(mmap_length_ > 0);
    }

    ~PosixRandomAccessFile() override {
        assert(fd_ != -1);
        assert(mmap_base_ != nullptr);
        assert(mmap_length_ > 0);
        ::close(fd_);
        ::munmap(mmap_base_, mmap_length_);
    }

    Status Read(uint64_t offset, size_t n, Slice *slice) override {
        const char *p = reinterpret_cast<const char *>(mmap_base_) + offset;
        n = std::min(n, mmap_length_ - offset);
        *slice = Slice(p, n);
        return Status::OK();
    }

private:
    const std::string filename_;
    int fd_;
    void *mmap_base_;
    size_t mmap_length_;
};

class PosixWritableFile : public WritableFile {
public:
    PosixWritableFile(const std::string &filename, int fd) :
            filename_(std::move(filename)),
            fd_(fd),
            pos_(0),
            offset_(0) {
        assert(fd != -1);
    }

    ~PosixWritableFile() override {
        Close();
    }

    Status Append(const Slice &slice) override {
        if (pos_ + slice.size() > kWritableBufferSize) {
            if (pos_ == 0) {
                /* 无法将slice中的数据一次性拷贝到buf_[],需要分多次拷贝 */
                size_t bytes_left = slice.size();
                off_t offset = 0;
                while (bytes_left > 0) {
                    size_t n = std::min(sizeof(buf_), bytes_left);
                    std::memcpy(buf_, slice.data() + offset, n);
                    pos_ = n;
                    Status s = Flush();
                    if (!s.ok()) {
                        return s;
                    }
                    offset += n;
                    bytes_left -= n;
                    offset_ += n;
                }
                return Status::OK();
            }

            Status s = Flush();
            if (!s.ok()) {
                return s;
            }
        }
        std::memcpy(buf_ + pos_, slice.data(), slice.size());
        pos_ += slice.size();
        offset_ += slice.size();
        return Status::OK();
    }

    Status Close() override {
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

    Status Flush() override {
        if (pos_ == 0) { /* no data in the buf_[] */
            return Status::OK();
        }
        ssize_t n = ::write(fd_, buf_, pos_);
        if (n > 0) {
            pos_ = 0;
            return Status::OK();
        }
        return Status::IOError();
    }

private:
    enum {
        kWritableBufferSize = 4096
    };

    const std::string filename_;
    int fd_;
    char buf_[kWritableBufferSize];
    size_t pos_;
    off_t offset_;
};

class PosixEnv : public Env {
public:
    PosixEnv() = default;

    PosixEnv(const PosixEnv &) = delete;
    PosixEnv &operator=(const PosixEnv &)= delete;

    ~PosixEnv() = default;

    Status GetFileSize(const std::string &filename, size_t *size) override {
        struct stat buf;
        if (::stat(filename.c_str(), &buf) >= 0) {
            *size = static_cast<size_t>(buf.st_size);
            return Status::OK();
        }
        return Status::IOError();
    }

    Status NewRandomAccessFile(const std::string &filename, RandomAccessFile **result) override {
        int fd = ::open(filename.c_str(), O_RDONLY);
        if (fd < 0) {
            return Status::IOError();
        }

        size_t file_size;
        Status s = GetFileSize(filename, &file_size);
        if (!s.ok()) {
            return s;
        }

        void *mmap_base = ::mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (mmap_base == nullptr) {
            return Status::IOError();
        }

        *result = new PosixRandomAccessFile(filename, fd, mmap_base, file_size);
        return Status::OK();
    }

    Status NewWritableFile(const std::string &filename, WritableFile **result) override {
        int fd = ::open(filename.c_str(), O_RDWR | O_CREAT, 0664);
        if (fd < 0) {
            return Status::IOError();
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }
};


#endif /* TABLE_STORAGE_POSIXENV_H */
