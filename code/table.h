#ifndef TABLE_STORAGE_TABLE_H
#define TABLE_STORAGE_TABLE_H

#include <vector>
#include <cstdint>

#include "status.h"
#include "env.h"
#include "mutexlock.h"

class Table {
 public:
  enum {
    kNumTableAttributes = 100,  /* 每行100个属性, 100*8=800B */
    kMaxTableEntries = 1000000, /* 一共可以存储1,000,000行 */
  };

  Table(const std::string& table_file_name, const std::string& index_file_name,
        const std::string& manifest_file_name);

  Table(const Table&) = delete;

  Table& operator=(const Table&) = delete;

  ~Table();

  Status Append(std::vector<uint64_t>& data);

  Status BuildIndexBlock(int attr_id);

  Status Lookup(int attr_id, uint64_t lower_bound, uint64_t upper_bound,
                std::vector<std::vector<uint64_t> >* results);

  Status Finish();

 private:
  typedef std::pair<uint64_t, uint32_t> IndexEntry;

  int FindIndexEntryLessOrEqual(std::vector<IndexEntry>& index_entries,
                                uint64_t x) const;

  int FindIndexEntryGreaterOrEqual(std::vector<IndexEntry>& index_entries,
                                   uint64_t x) const;

  Status Recover();

  std::vector<uint64_t> Decode(const Slice& slice) const;

 private:
  Env* env_;
  WritableFile* table_file_, *index_file_;
  RandomAccessFile* table_file_readonly_, *index_file_readonly_;
  const std::string table_file_name_, index_file_name_, manifest_file_name_;
  int nr_entries_;
  bool appending_finished_;
  int index_attr_id_; /* 当前索引文件是为哪个属性建立的索引? */
  Mutex mutex_;
};

#endif /* TABLE_STORAGE_TABLE_H */
