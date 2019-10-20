#ifndef TABLE_STORAGE_RANDOM_H
#define TABLE_STORAGE_RANDOM_H


#include <cstdint>
#include <vector>
#include <ctime>
#include <cstdlib>

class Random {
public:
    Random() {
        seed_ = static_cast<unsigned int>(time(nullptr));
        srand(seed_);
    }

    std::vector<uint64_t> GenerateRandomNumbers(int n) {
        std::vector<uint64_t> result;
        for (int i = 0; i < n; i++) {
			result.push_back(static_cast<uint64_t>(rand()));
        }
        return result;
    }

private:
    unsigned int seed_;
};


#endif /* TABLE_STORAGE_RANDOM_H */
