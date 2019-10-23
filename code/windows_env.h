#ifndef TABLE_STORAGE_WINDOWSENV_H
#define TABLE_STORAGE_WINDOWSENV_H

#ifdef WIN32
#define NOMINMAX

#include <Windows.h>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include "env.h"


class WindowsRandomAccessFile : public RandomAccessFile {
public:
    WindowsRandomAccessFile(const std::string &filename, HANDLE file_handle) :
            filename_(std::move(filename)),
            file_handle_(file_handle) {
		assert(file_handle_ != INVALID_HANDLE_VALUE);
    }

    ~WindowsRandomAccessFile() override {
		assert(file_handle_ != INVALID_HANDLE_VALUE);
		::CloseHandle(file_handle_);
    }

    Status Read(uint64_t offset, size_t nbytes, char *scratch, Slice *slice) override {
		assert(file_handle_ != INVALID_HANDLE_VALUE);
		DWORD bytes_read = 0;
		OVERLAPPED overlapped = { 0 };

		overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);
		overlapped.Offset = static_cast<DWORD>(offset);
		::ReadFile(file_handle_, scratch, nbytes, &bytes_read, &overlapped);
        if (bytes_read != nbytes) {
            return Status::IOError();
        }
        *slice = Slice(scratch, nbytes);
        return Status::OK();
    }

private:
    const std::string filename_;
	HANDLE file_handle_;
};

class WindowsWritableFile : public WritableFile {
public:
    WindowsWritableFile(const std::string &filename, HANDLE file_handle) :
            filename_(std::move(filename)),
			file_handle_(file_handle),
            pos_(0),
            offset_(0) {
		assert(file_handle_ != INVALID_HANDLE_VALUE);
    }

    ~WindowsWritableFile() override {
        Close();
    }

    Status Append(const Slice &slice) override {
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
                return status;
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

    Status Close() override {
        Status status = Flush();
        if (!status.ok()) {
            return status;
        }
        if (file_handle_ != INVALID_HANDLE_VALUE && ::CloseHandle(file_handle_)) {
			file_handle_ = INVALID_HANDLE_VALUE;
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
		DWORD bytes_written = 0;
		::WriteFile(file_handle_, reinterpret_cast<LPCVOID>(buf_), pos_, &bytes_written, nullptr);
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
	HANDLE file_handle_;
    char buf_[kWritableBufferSize];
    size_t pos_;
    off_t offset_;
};

class WindowsEnv : public Env {
public:
    WindowsEnv() = default;

    WindowsEnv(const WindowsEnv &) = delete;

    WindowsEnv &operator=(const WindowsEnv &)= delete;

    ~WindowsEnv() = default;

    Status GetFileSize(const std::string &filename, size_t *size) override {
		HANDLE file_handle = ::CreateFileA(filename.c_str(),
			GENERIC_READ,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr,
			OPEN_EXISTING,
			FILE_ATTRIBUTE_NORMAL,
			0);
		if (file_handle == INVALID_HANDLE_VALUE) {
		    *size = 0;
			return Status::IOError();
		}
		*size = ::GetFileSize(file_handle, nullptr);
		::CloseHandle(file_handle);
        return Status::OK();
    }

    Status NewRandomAccessFile(const std::string &filename, RandomAccessFile **result) override {
		HANDLE file_handle = ::CreateFileA(filename.c_str(),
			GENERIC_READ, 
			FILE_SHARE_READ | FILE_SHARE_WRITE, 
			nullptr,
			OPEN_EXISTING, 
			FILE_ATTRIBUTE_NORMAL, 
			0);
		if (file_handle == INVALID_HANDLE_VALUE) {
		    *result = nullptr;
			return Status::IOError();
		}

        *result = new WindowsRandomAccessFile(filename, file_handle);
        return Status::OK();
    }

	Status NewWritableFile(const std::string &filename, WritableFile **result) override {
		HANDLE file_handle = ::CreateFileA(filename.c_str(),
			GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr,
			CREATE_ALWAYS,
			FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH,
			0);
		if (file_handle == INVALID_HANDLE_VALUE) {
		    *result = nullptr;
			return Status::IOError();
		}

		*result = new WindowsWritableFile(filename, file_handle);
		return Status::OK();
	}

	Status OpenWritableFile(const std::string &filename, WritableFile **result) override {
		HANDLE file_handle = ::CreateFileA(filename.c_str(),
			GENERIC_READ | GENERIC_WRITE,
			FILE_SHARE_READ | FILE_SHARE_WRITE,
			nullptr,
			OPEN_ALWAYS,
			FILE_ATTRIBUTE_NORMAL | FILE_FLAG_WRITE_THROUGH,
			0);
		if (file_handle == INVALID_HANDLE_VALUE) {
		    *result = nullptr;
			return Status::IOError();
		}
		if (GetLastError() == ERROR_ALREADY_EXISTS) {
			if (INVALID_SET_FILE_POINTER == ::SetFilePointer(file_handle, 0, nullptr, FILE_END)) {
		        *result = nullptr;
				return Status::IOError();
			}
		}

        *result = new WindowsWritableFile(filename, file_handle);
        return Status::OK();
    }
};

#endif /* WIN32 */
#endif /* TABLE_STORAGE_WINDOWSENV_H */
