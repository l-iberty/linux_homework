#ifndef TABLE_STORAGE_CODING_H
#define TABLE_STORAGE_CODING_H

#include <string>
#include <cstdint>

void EncodeFixed64(char* dst, uint64_t value);
void EncodeFixed32(char* dst, uint32_t value);

void PutFixed64(std::string* dst, uint64_t value);
void PutFixed32(std::string* dst, uint32_t value);

uint64_t DecodeFixed64(const char* ptr);
uint32_t DecodeFixed32(const char* ptr);

#endif /* TABLE_STORAGE_CODING_H */
