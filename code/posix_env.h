#ifndef TABLE_STORAGE_POSIXENV_H
#define TABLE_STORAGE_POSIXENV_H
#ifndef WIN32

#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <cassert>
#include <algorithm>
#include "env.h"

class PosixRandomAccessFile : public RandomAccessFile {
public:
    PosixRandomAccessFile(const std::string& filename, int fd)
        : filename_(std::move(filename))
        , fd_(fd)
    {
        assert(fd_ != -1);
    }

    ~PosixRandomAccessFile() override
    {
        assert(fd_ != -1);
        ::close(fd_);
    }

    Status Read(uint64_t offset, size_t nbytes, char* scratch, Slice* slice) override
    {
        assert(fd_ != -1);
        ssize_t bytes_read = ::pread(fd_, scratch, nbytes, offset);
        if (bytes_read != nbytes) {
            return Status::IOError();
        }
        *slice = Slice(scratch, nbytes);
        return Status::OK();
    }

private:
    const std::string filename_;
    int fd_;
};

class PosixWritableFile : public WritableFile {
public:
    PosixWritableFile(const std::string& filename, int fd)
        : filename_(std::move(filename))
        , fd_(fd)
        , pos_(0)
        , offset_(0)
    {
        assert(fd != -1);
    }

    ~PosixWritableFile() override
    {
        Close();
    }

    Status Append(const Slice& slice) override
    {
        if (pos_ + slice.size() > kWritableBufferSize) {
            Status status;
            if (pos_ == 0) {
                /* 无法将slice中的数据一次性拷贝到buf_[],需要分多次拷贝 */
                size_t bytes_left = slice.size();
                off_t offset = 0;
                while (bytes_left > 0) {
                    size_t n = std::min(sizeof(buf_), bytes_left);
                    std::memcpy(buf_, slice.data() + offset, n);
                    pos_ = n;
                    status = Flush();
                    if (!status.ok()) {
                        return status;
                    }
                    offset += n;
                    bytes_left -= n;
                    offset_ += n;
                }
                return Status::OK();
            }
            status = Flush();
            if (!status.ok()) {
                return status;
            }
        }
        std::memcpy(buf_ + pos_, slice.data(), slice.size());
        pos_ += slice.size();
        offset_ += slice.size();
        return Status::OK();
    }

    Status Close() override
    {
        Status status = Flush();
        if (!status.ok()) {
            return status;
        }
        if (fd_ >= 0 && ::close(fd_) == 0) {
            fd_ = -1;
            return Status::OK();
        }
        return Status::IOError();
    }

    Status Flush() override
    {
        if (pos_ == 0) { /* no data in the buf_[] */
            return Status::OK();
        }
        ssize_t bytes_written = ::write(fd_, buf_, pos_);
        if (bytes_written <= 0) {
            return Status::IOError();
        }
        pos_ = 0;
        return Status::OK();
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

    PosixEnv(const PosixEnv&) = delete;
    PosixEnv& operator=(const PosixEnv&) = delete;

    ~PosixEnv() = default;

    Status GetFileSize(const std::string& filename, size_t* size) override
    {
        struct stat buf;
        if (::stat(filename.c_str(), &buf) >= 0) {
            *size = static_cast<size_t>(buf.st_size);
            return Status::OK();
        }
        *size = 0;
        return Status::IOError();
    }

    Status NewRandomAccessFile(const std::string& filename, RandomAccessFile** result) override
    {
        int fd = ::open(filename.c_str(), O_RDONLY);
        if (fd < 0) {
            *result = nullptr;
            return Status::IOError();
        }

        *result = new PosixRandomAccessFile(filename, fd);
        return Status::OK();
    }

    Status NewWritableFile(const std::string& filename, WritableFile** result) override
    {
        int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0664);
        if (fd < 0) {
            *result = nullptr;
            return Status::IOError();
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }

    Status OpenWritableFile(const std::string& filename, WritableFile** result) override
    {
        int fd = ::open(filename.c_str(), O_RDWR | O_CREAT | O_APPEND, 0664);
        if (fd < 0) {
            *result = nullptr;
            return Status::IOError();
        }

        *result = new PosixWritableFile(filename, fd);
        return Status::OK();
    }
};

#endif /* WIN32 */
#endif /* TABLE_STORAGE_POSIXENV_H */
