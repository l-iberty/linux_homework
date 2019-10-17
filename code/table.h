#ifndef TABLE_STORAGE_TABLE_H
#define TABLE_STORAGE_TABLE_H

#include <vector>
#include <cstdint>

#include "status.h"
#include "writable_file.h"

#define TABLE_FILE "table"
#define INDEX_FILE "index"

class Table {
public:
    enum {
        kNumTableAttributes = 100, /* 每行100个属性, 100*8=800B */
        kMaxTableEntries = 10000000, /* 一共可以存储1,000,000行 */
    };

    typedef std::pair<uint64_t, uint32_t> IndexEntry;

    Table();

    Table(const Table &) = delete;

    Table &operator=(const Table &) = delete;

    ~Table();

    Status Append(std::vector<uint64_t> &data);

    Status BuildIndexBlock(int attr_id);

    Status Lookup(int attr_id, uint64_t lower_bound, uint64_t upper_bound, std::vector<std::vector<uint64_t>> *results);

    void Finish();

private:
    int FindIndexEntryLessOrEqual(std::vector<IndexEntry> &index_entries, uint64_t x) const;

    int FindIndexEntryGreaterOrEqual(std::vector<IndexEntry> &index_entries, uint64_t x) const;

private:
    WritableFile *table_file_;
    int nr_entries_;
    bool appending_finished_;
    int index_attr_id_; /* 当前索引文件是为哪个属性建立的索引? */

};

#endif /* TABLE_STORAGE_TABLE_H */
