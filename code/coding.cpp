#include <cstring>
#include "coding.h"

void EncodeFixed64(char* dst, uint64_t value) {
  std::memcpy(dst, &value, sizeof(uint64_t));
}

void EncodeFixed32(char* dst, uint32_t value) {
  std::memcpy(dst, &value, sizeof(uint32_t));
}

void PutFixed64(std::string* dst, uint64_t value) {
  char buf[sizeof(uint64_t)];
  EncodeFixed64(buf, value);
  dst->append(buf, sizeof(buf));
}

void PutFixed32(std::string* dst, uint32_t value) {
  char buf[sizeof(uint32_t)];
  EncodeFixed32(buf, value);
  dst->append(buf, sizeof(buf));
}

uint64_t DecodeFixed64(const char* ptr) {
  uint64_t value;
  std::memcpy(&value, ptr, sizeof(uint64_t));
  return value;
}

uint32_t DecodeFixed32(const char* ptr) {
  uint32_t value;
  std::memcpy(&value, ptr, sizeof(uint32_t));
  return value;
}
