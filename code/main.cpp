#include <iostream>
#include <vector>
#include <stdlib.h>
#include <time.h>
#include <gtest/gtest.h>

#include "table.h"
#include "random.h"

TEST(table_storage, test1) {
    Table table;
    Random rnd;
    const int n = 1000000;
    const uint64_t lower_bound = 0;
    const uint64_t upper_bound = 100000000;
    int query_attr_id = 3;
    std::vector<std::vector<uint64_t>> expected_query_results;
    clock_t start, end;

    start = clock();
    std::cout << "Appending data...\n";
    for (int i = 0; i < n; i++) {
        std::vector<uint64_t> nums = rnd.GenerateRandomNumbers(Table::kNumTableAttributes);
        Status s = table.Append(nums);
        ASSERT_TRUE(s.ok());
        if (nums[query_attr_id] >= lower_bound && nums[query_attr_id] <= upper_bound) {
            expected_query_results.push_back(nums);
        }
    }
    end = clock();
    printf("Done. Time elapsed: %.5fs\n", (double) (end - start) / CLOCKS_PER_SEC);

    table.Finish();
    table.BuildIndexBlock(query_attr_id);

    std::vector<std::vector<uint64_t>> results;
    Status s = table.Lookup(query_attr_id, lower_bound, upper_bound, &results);
    if (!s.ok()) {
        std::cout << s.ToString() << std::endl;
    }

    ASSERT_EQ(results.size(), expected_query_results.size());
    for (int i = 0; i < results.size(); i++) {
        ASSERT_EQ(results[i].size(), expected_query_results[i].size());
    }

    std::cout << "First 20 query results...\n";
    for (int i = 0; i < 20; i++) {
        for (uint64_t x:results[i]) {
            std::cout << x << "\t";
        }
        std::cout << "\n";
    }
}


int main() {
    testing::InitGoogleTest();
    return RUN_ALL_TESTS();
}