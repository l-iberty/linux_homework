#include <cassert>
#include <iostream>
#include <algorithm>
#include "table.h"
#include "coding.h"
#include "env.h"

Table::Table() :
        env_(CreateEnv()),
        index_file_(nullptr),
        table_file_readonly_(nullptr),
        index_file_readonly_(nullptr),
        nr_entries_(0),
        appending_finished_(false),
        index_attr_id_(-1) {
    Status status = env_->NewWritableFile(TABLE_FILE, &table_file_);
    assert(status.ok());
}

Table::~Table() {
    Finish();
    if (index_file_ != nullptr) { delete index_file_; }
    if (index_file_readonly_ != nullptr) { delete index_file_readonly_; }
    if (table_file_readonly_ != nullptr) { delete table_file_readonly_; }
}

Status Table::Append(std::vector<uint64_t> &data) {
    MutexLock l(&mutex_);
    if (table_file_ == nullptr) {
        return Status::GeneralError("Table::table_file_ has been closed.");
    }
    if (nr_entries_ >= kMaxTableEntries) {
        return Status::GeneralError("too much data");
    }
    assert(data.size() == kNumTableAttributes);
    std::string d;
    for (size_t i = 0; i < kNumTableAttributes; i++) {
        PutFixed64(&d, data[i]);
    }

    Status status = table_file_->Append(d);
    if (!status.ok()) {
        return status;
    }
    nr_entries_++;
    return Status::OK();
}

Status Table::BuildIndexBlock(int attr_id) {
    Status status;
    if (!appending_finished_) { /* 添加数据结束后才建立索引 */
        Finish();
    }
    assert(attr_id >= 0 && attr_id < kNumTableAttributes);
    index_attr_id_ = attr_id;

    if (table_file_readonly_ == nullptr) {
        status = env_->NewRandomAccessFile(TABLE_FILE, &table_file_readonly_);
        if (!status.ok()) {
            return status;
        }
    }

	/* 将attr_id所对应的列读入index_entries */
    Slice result;
	char *scratch = new char[sizeof(uint64_t)];
    uint64_t offset = attr_id * sizeof(uint64_t);
    std::vector<IndexEntry> index_entries;
    for (int i = 0; i < nr_entries_; i++) {
        table_file_readonly_->Read(offset, sizeof(uint64_t), scratch, &result);
        index_entries.push_back(std::make_pair(
                DecodeFixed64(result.data()), i));
        offset += kNumTableAttributes * sizeof(uint64_t);
    }
	delete[] scratch;

    if (index_file_ == nullptr) {
        status = env_->NewWritableFile(INDEX_FILE, &index_file_);
        if (!status.ok()) {
            return status;
        }
    }

	/* index_entries排序后写入索引文件index_file_ */
    std::sort(index_entries.begin(), index_entries.end());
	std::string d;
	for (size_t i = 0; i < index_entries.size(); i++) {
		PutFixed64(&d, index_entries[i].first);
		PutFixed32(&d, index_entries[i].second);
	}
	status = index_file_->Append(d);
	if (!status.ok()) {
		return status;
	}

    status = index_file_->Close();
    if (!status.ok()) {
        return status;
    }

	/* 打开只读的index_file_以便后续使用 */
    if (index_file_readonly_ == nullptr) {
        status = env_->NewRandomAccessFile(INDEX_FILE, &index_file_readonly_);
        if (!status.ok()) {
            return status;
        }
    }

    return Status::OK();
}

Status Table::Lookup(int attr_id, uint64_t lower_bound, uint64_t upper_bound,
                     std::vector<std::vector<uint64_t>> *results) {
    if (index_attr_id_ != attr_id) { /* 索引未建立则构建之 */
        std::cout << "index not existed, building it...\n";
        BuildIndexBlock(attr_id);
    }

    assert(table_file_readonly_ != nullptr);
    assert(index_file_readonly_ != nullptr);

    Slice slice;
	char *scratch = new char[kNumTableAttributes * sizeof(uint64_t)];

    std::vector<IndexEntry> index_entries;
    uint64_t offset = 0;
    for (int i = 0; i < nr_entries_; i++) {
        index_file_readonly_->Read(offset, sizeof(uint64_t), scratch, &slice);
        uint64_t var = DecodeFixed64(slice.data());
        index_file_readonly_->Read(offset + sizeof(uint64_t), sizeof(uint32_t), scratch, &slice);
        uint32_t index = DecodeFixed32(slice.data());
        index_entries.push_back(std::make_pair(var, index));
        offset += sizeof(uint64_t) + sizeof(uint32_t);
    }

    int lower_bound_idx = FindIndexEntryGreaterOrEqual(index_entries, lower_bound);
    int upper_bound_idx = FindIndexEntryLessOrEqual(index_entries, upper_bound);
    if (lower_bound_idx == -1 || upper_bound_idx == -1) {
		delete[] scratch;
        return Status::NotFound();
    }

    for (int i = lower_bound_idx; i <= upper_bound_idx; i++) {
        std::vector<uint64_t> r;
        const size_t nbytes_per_record = kNumTableAttributes * sizeof(uint64_t);
		table_file_readonly_->Read(index_entries[i].second * nbytes_per_record, 
			nbytes_per_record, scratch, &slice);
        for (uint64_t off = 0; off < nbytes_per_record; off += sizeof(uint64_t)) {
            r.push_back(DecodeFixed64(slice.data() + off));
        }
        results->push_back(r);
    }

	delete[] scratch;
    return Status::OK();
}

void Table::Finish() {
    if (table_file_ != nullptr) {
        delete table_file_;
        table_file_ = nullptr;
        appending_finished_ = true;
    }
}

int Table::FindIndexEntryGreaterOrEqual(std::vector<IndexEntry> &index_entries, uint64_t x) const {
    int left = 0, right = static_cast<int>(index_entries.size()) - 1;
    if (x <= index_entries[0].first) {
        return 0;
    } else if (x == index_entries[right].first) {
        return right;
    } else if (x > index_entries[right].first) {
        return -1;
    }

    while (left < right) {
        int mid = (left + right) >> 1;
        if (mid + 1 < index_entries.size() &&
            index_entries[mid].first < x && x <= index_entries[mid + 1].first) {
            return mid + 1;
        } else if (x == index_entries[mid].first) {
            return mid;
        } else if (x > index_entries[mid].first) {
            left = mid;
        } else if (x < index_entries[mid].first) {
            right = mid;
        }
    }
    return -1;
}

int Table::FindIndexEntryLessOrEqual(std::vector<IndexEntry> &index_entries, uint64_t x) const {
    int left = 0, right = static_cast<int>(index_entries.size()) - 1;
    if (x < index_entries[0].first) {
        return -1;
    } else if (x >= index_entries[right].first) {
        return right;
    } else if (x == index_entries[0].first) {
        return 0;
    }

    while (left < right) {
        int mid = (left + right) >> 1;
        if (mid + 1 < index_entries.size() &&
            index_entries[mid].first <= x && x < index_entries[mid + 1].first) {
            return mid;
        } else if (x == index_entries[mid].first) {
            return mid;
        } else if (x > index_entries[mid].first) {
            left = mid;
        } else if (x < index_entries[mid].first) {
            right = mid;
        }
    }
    return -1;
}
