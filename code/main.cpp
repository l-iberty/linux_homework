#include <iostream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <atomic>
#include <stdlib.h>
#include <time.h>
#include <gtest/gtest.h>

#include "table.h"
#include "random.h"
#include "coding.h"

#if 0
/* global vars for TEST(table_storage,multi_thread1) */
Table g_table("table0", "index0", "MANIFEST0");
std::atomic<size_t> g_nr_expected_query_results(0);
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
            g_nr_expected_query_results++;
        }
    }
}

TEST(table_storage, multi_thread) {
    Status status;
    const int nr_thds = 8;
    const int n = 1000 / nr_thds;

    std::thread **thds = new std::thread*[nr_thds];
    for (int i = 0; i < nr_thds; i++) {
        thds[i] = new std::thread(thd_routine, n);
    }
    for (int i = 0; i < nr_thds; i++) {
        thds[i]->join();
    }
    for (int i = 0; i < nr_thds; i++) {
        delete thds[i];
    }
    delete[] thds;

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
#endif

TEST(table_storage, single_thread1) {
    Table table("table1", "index1", "MANIFEST1");
    Random rnd;
    Status status;
    const int n = 1000;
    const uint64_t lower_bound = 100;
    const uint64_t upper_bound = 1000000;
    int query_attr_id = 0;
    size_t nr_expected_query_results = 0;
    clock_t start, end;

    Env *env = CreateEnv();
    RandomAccessFile *data_r = nullptr;
    Slice slice;
    char scratch[sizeof(int)];
    status = env->NewRandomAccessFile("data", &data_r);
    if (status.ok()) {
        status = data_r->Read(0, sizeof(int), scratch, &slice);
        ASSERT_TRUE(status.ok());
        nr_expected_query_results = static_cast<size_t>(DecodeFixed32(scratch));
        delete data_r;
    }

    /* 向表中添加数据 */
    start = clock();
    std::cout << "Appending data...\n";
    for (int i = 0; i < n; i++) {
        std::vector<uint64_t> nums = rnd.GenerateRandomNumbers(Table::kNumTableAttributes);
        status = table.Append(nums);
        if (!status.ok()) {
            printf("error message: %s\n", status.ToString().c_str());
        }
        ASSERT_TRUE(status.ok());
        if (nums[query_attr_id] >= lower_bound && nums[query_attr_id] <= upper_bound) {
            nr_expected_query_results++;
        }
    }
    end = clock();
    printf("Done. Time elapsed: %.5fs\n", (double) (end - start) / CLOCKS_PER_SEC);
    table.Finish();

    WritableFile *data_w = nullptr;
    status = env->NewWritableFile("data", &data_w);
    ASSERT_TRUE(status.ok());
    std::string d;
    PutFixed32(&d, static_cast<uint32_t>(nr_expected_query_results));
    status = data_w->Append(d);
    ASSERT_TRUE(status.ok());
    if (data_w != nullptr) {
        delete data_w;
    }

    //table.Finish();
    //table.BuildIndexBlock(query_attr_id); // 如果不显式调用Finish()和BuildIndexBlock(), Lookup()会自己完成这两个调用

    /* 在query_attr_id指定的属性上进行范围查找 */
    std::vector<std::vector<uint64_t>> query_results;
    status = table.Lookup(query_attr_id, lower_bound, upper_bound, &query_results);
    if (!status.ok()) {
        std::cout << status.ToString() << std::endl;
    }

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

TEST(table_storage, single_thread2) {
    Table table("table2", "index2", "MANIFEST2");
    Status status;
    std::vector<std::vector<uint64_t>> vec_nums = {
            std::vector<uint64_t>(100, 1), // 0
            std::vector<uint64_t>(100, 1), // 1
            std::vector<uint64_t>(100, 2), // 2
            std::vector<uint64_t>(100, 2), // 3
            std::vector<uint64_t>(100, 3), // 4
            std::vector<uint64_t>(100, 3), // 5
            std::vector<uint64_t>(100, 3), // 6
            std::vector<uint64_t>(100, 4), // 7
            std::vector<uint64_t>(100, 5) // 8
    };
    std::vector<std::vector<uint64_t>> results;
    status = table.Append(vec_nums[0]);
    ASSERT_TRUE(status.ok());
    status = table.Lookup(0, 0, 0, &results);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(results.empty());

    results.clear();
    status = table.Append(vec_nums[8]);
    ASSERT_TRUE(status.ok());
    status = table.Lookup(0, 1, 5, &results);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(results.size(), 2);
    ASSERT_EQ(results[0][0], 1);
    ASSERT_EQ(results[1][0], 5);

    for (int i = 1; i <= 7; i++) {
        status = table.Append(vec_nums[i]);
        ASSERT_TRUE(status.ok());
    }
    results.clear();
    status = table.Lookup(0, 1, 5, &results);
    ASSERT_TRUE(status.ok());
    std::unordered_map<uint64_t, int> counter;
    ASSERT_EQ(results.size(), 9);
    for (std::vector<uint64_t> &r : results) {
        counter[r[0]]++;
    }
    ASSERT_EQ(counter[1], 2);
    ASSERT_EQ(counter[2], 2);
    ASSERT_EQ(counter[3], 3);
    ASSERT_EQ(counter[4], 1);
    ASSERT_EQ(counter[5], 1);
}

int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}