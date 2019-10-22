#include <iostream>
#include <vector>
#include <stdlib.h>
#include <time.h>
#include <gtest/gtest.h>
#include <thread>

#include "table.h"
#include "random.h"

#if 0
TEST(table_storage, single_thread) {
    Table table;
    Random rnd;
    Status status;
    const int n = 1000000;
    const uint64_t lower_bound = 1000;
    const uint64_t upper_bound = 100000;
    int query_attr_id = 0;
    size_t nr_expected_query_results = 0;
    clock_t start, end;

    /* 向表中添加数据 */
    start = clock();
    std::cout << "Appending data...\n";
    for (int i = 0; i < n; i++) {
        std::vector<uint64_t> nums = rnd.GenerateRandomNumbers(Table::kNumTableAttributes);
        status = table.Append(nums);
        ASSERT_TRUE(status.ok());
        if (nums[query_attr_id] >= lower_bound && nums[query_attr_id] <= upper_bound) {
            nr_expected_query_results++;
        }
    }
    end = clock();
    printf("Done. Time elapsed: %.5fs\n", (double) (end - start) / CLOCKS_PER_SEC);

    //table.Finish();
    //table.BuildIndexBlock(query_attr_id); // 如果不显式调用Finish()和BuildIndexBlock(), Lookup()会自己完成这两个调用

    /* 在query_attr_id指定的属性上进行范围查找 */
    std::vector<std::vector<uint64_t>> query_results;
    status = table.Lookup(query_attr_id, lower_bound, upper_bound, &query_results);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
    }

    /* Table::Lookup()调用结束后Table::table_file_将被关闭, 此时如果调用Table::Append()就会出错 */
    std::vector<uint64_t> temp(100, 100);
    status = table.Append(temp);
    ASSERT_FALSE(status.ok());
    printf("error message: %s\n", status.ToString().c_str());

    /* 校验查询结果 */
    ASSERT_EQ(query_results.size(), nr_expected_query_results);
    for (size_t i = 0; i < query_results.size(); i++) {
        ASSERT_EQ(query_results[i].size(), Table::kNumTableAttributes);
        ASSERT_TRUE(query_results[i][query_attr_id] >= lower_bound &&
                    query_results[i][query_attr_id] <= upper_bound);
    }

    /* 打印查询结果 */
    size_t cnt = std::min(static_cast<size_t>(100), query_results.size());
#ifdef WIN32
    printf("%lu query query_results with range [0x%llX, 0x%llX] on attr %d:\n",
        query_results.size(), lower_bound, upper_bound, query_attr_id);
#else
    printf("%lu query query_results with range [0x%lX, 0x%lX] on attr %d:\n",
           query_results.size(), lower_bound, upper_bound, query_attr_id);
#endif // WIN32
    for (size_t i = 0; i < cnt; i++) {
        printf("[%lu] ", i);
        printf("...... %016lX ......\n", query_results[i][query_attr_id]);
    }
}
#endif

Table g_table;
size_t g_nr_expected_query_results;
Mutex g_mutex;
const uint64_t g_lower_bound = 1000;
const uint64_t g_upper_bound = 100000;
int g_query_attr_id = 0;

void thd_routine(const int n) {
    Random rnd;
    Status status;

    for (int i = 0; i < n; i++) {
        std::vector<uint64_t> nums = rnd.GenerateRandomNumbers(Table::kNumTableAttributes);
        status = g_table.Append(nums);
        ASSERT_TRUE(status.ok());
        if (nums[g_query_attr_id] >= g_lower_bound && nums[g_query_attr_id] <= g_upper_bound) {
            g_mutex.Lock();
            g_nr_expected_query_results++;
            g_mutex.Unlock();
        }
    }
}

TEST(table_storage, multi_thread) {
    Table table;
    Status status;
    const int n = 100000 / 8;

    std::thread t1(thd_routine, n);
    std::thread t2(thd_routine, n);
    std::thread t3(thd_routine, n);
    std::thread t4(thd_routine, n);
    std::thread t5(thd_routine, n);
    std::thread t6(thd_routine, n);
    std::thread t7(thd_routine, n);
    std::thread t8(thd_routine, n);

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
    t7.join();
    t8.join();

    //g_table.Finish();
    //g_table.BuildIndexBlock(query_attr_id); // 如果不显式调用Finish()和BuildIndexBlock(), Lookup()会自己完成这两个调用

    /* 在query_attr_id指定的属性上进行范围查找 */
    std::vector<std::vector<uint64_t>> query_results;
    status = g_table.Lookup(g_query_attr_id, g_lower_bound, g_upper_bound, &query_results);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
    }

    /* Table::Lookup()调用结束后Table::table_file_将被关闭, 此时如果调用Table::Append()就会出错 */
    std::vector<uint64_t> temp(100, 100);
    status = g_table.Append(temp);
    ASSERT_FALSE(status.ok());
    printf("error message: %s\n", status.ToString().c_str());

    /* 校验查询结果 */
    ASSERT_EQ(query_results.size(), g_nr_expected_query_results);
    for (size_t i = 0; i < query_results.size(); i++) {
        ASSERT_EQ(query_results[i].size(), Table::kNumTableAttributes);
        ASSERT_TRUE(query_results[i][g_query_attr_id] >= g_lower_bound &&
                    query_results[i][g_query_attr_id] <= g_upper_bound);
    }

    /* 打印查询结果 */
    size_t cnt = std::min(static_cast<size_t>(100), query_results.size());
#ifdef WIN32
    printf("%lu query query_results with range [0x%llX, 0x%llX] on attr %d:\n",
        query_results.size(), g_lower_bound, g_upper_bound, g_query_attr_id);
#else
    printf("%lu query query_results with range [0x%lX, 0x%lX] on attr %d:\n",
           query_results.size(), g_lower_bound, g_upper_bound, g_query_attr_id);
#endif // WIN32
    for (size_t i = 0; i < cnt; i++) {
        printf("[%lu] ", i);
        printf("...... %016lX ......\n", query_results[i][g_query_attr_id]);
    }
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}