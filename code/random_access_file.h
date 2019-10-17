#ifndef TABLE_STORAGE_RANDOM_ACCESS_FILE_H
#define TABLE_STORAGE_RANDOM_ACCESS_FILE_H


#include "status.h"

class RandomAccessFile {
public:
    RandomAccessFile(const std::string &filename);

    ~RandomAccessFile();

    Status Read(uint64_t offset, size_t n, Slice* slice);
private:
    size_t GetFileSize(const std::string& filename);

private:
    int fd_;
    void *file_mapping_base_;
    size_t file_mapping_len_;
};


#endif /* TABLE_STORAGE_RANDOM_ACCESS_FILE_H */
