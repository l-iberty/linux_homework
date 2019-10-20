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
    Status status;
	const int n = 1000000;
    const uint64_t lower_bound = 1000;
    const uint64_t upper_bound = 100000;
    int query_attr_id = 0;
    std::vector<uint64_t> expected_query_results;
    clock_t start, end;

    /* 向表中添加数据 */
    start = clock();
    std::cout << "Appending data...\n";
    for (int i = 0; i < n; i++) {
        std::vector<uint64_t> nums = rnd.GenerateRandomNumbers(Table::kNumTableAttributes);
        status = table.Append(nums);
        ASSERT_TRUE(status.ok());
        if (nums[query_attr_id] >= lower_bound && nums[query_attr_id] <= upper_bound) {
            expected_query_results.push_back(nums[query_attr_id]);
        }
    }
    end = clock();
    printf("Done. Time elapsed: %.5fs\n", (double) (end - start) / CLOCKS_PER_SEC);

    //table.Finish();
    //table.BuildIndexBlock(query_attr_id); // 如果不显式调用Finish()和BuildIndexBlock(), Lookup()会自己完成这两个调用

    /* 在query_attr_id指定的属性上进行范围查找 */
    std::vector<std::vector<uint64_t>> results;
    status = table.Lookup(query_attr_id, lower_bound, upper_bound, &results);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
    }

    /* Table::Lookup()调用结束后Table::table_file_将被关闭, 此时如果调用Table::Append()就会出错 */
    std::vector<uint64_t> temp(100, 100);
    status = table.Append(temp);
    ASSERT_FALSE(status.ok());
    printf("error message: %status\n", status.ToString().c_str());

    /* 校验查询结果 */
    ASSERT_EQ(results.size(), expected_query_results.size());
    for (size_t i = 0; i < results.size(); i++) {
        ASSERT_EQ(results[i].size(), Table::kNumTableAttributes);
        ASSERT_TRUE(results[i][query_attr_id] >= lower_bound &&
			results[i][query_attr_id] <= upper_bound);
    }

    /* 打印查询结果 */
    size_t cnt = std::min(static_cast<size_t>(100), results.size());
    printf("%lu query results with range [0x%llX, 0x%llX] on attr %d:\n",
		results.size(), lower_bound, upper_bound, query_attr_id);
    for (size_t i = 0; i < cnt; i++) {
        printf("[%lu] ", i);
        printf("...... %016lX ......\n", results[i][query_attr_id]);
    }
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}