#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include "random_access_file.h"

RandomAccessFile::RandomAccessFile(const std::string &filename) {
    fd_ = ::open(filename.c_str(), O_RDONLY);
    file_mapping_len_ = GetFileSize(filename);
    assert(fd_ != -1);
    assert(file_mapping_len_ > 0);
    file_mapping_base_ = ::mmap(nullptr, file_mapping_len_, PROT_READ, MAP_SHARED, fd_, 0);
    assert(file_mapping_base_ != nullptr);
}

RandomAccessFile::~RandomAccessFile() {
    ::munmap(file_mapping_base_, file_mapping_len_);
    ::close(fd_);
}

Status RandomAccessFile::Read(uint64_t offset, size_t n, Slice *slice) {
    const char *p = reinterpret_cast<const char *>(file_mapping_base_) + offset;
    n = std::min(n, file_mapping_len_ - offset);
    *slice = Slice(p, n);
    return Status::OK();
}

size_t RandomAccessFile::GetFileSize(const std::string &filename) {
    struct stat st;
    if (::stat(filename.c_str(), &st) >= 0) {
        return static_cast<size_t>(st.st_size);
    }
    return 0;
}
