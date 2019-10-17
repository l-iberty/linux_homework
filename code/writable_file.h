#ifndef TABLE_STORAGE_WRITABLEFILE_H
#define TABLE_STORAGE_WRITABLEFILE_H

#include "status.h"
#include "slice.h"

class WritableFile {
public:
    WritableFile(const std::string& filename);

    WritableFile(const WritableFile &) = delete;

    WritableFile &operator=(const WritableFile &)= delete;

    ~WritableFile() { Close(); }

    Status Append(const Slice &data);

    Status Close();

    Status Flush();

private:
    enum {
        kWritableBufferSize = 4096
    };

    int fd_;
    std::string filename_;
    size_t offset_;
    char buf_[kWritableBufferSize];
    size_t pos_;
};

WritableFile *NewWritableFile(const std::string& filename);


#endif /* TABLE_STORAGE_WRITABLEFILE_H */
