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
#include "test_helper.h"

#if 1
/* global vars for TEST(table_storage,multi_thread1) */
Table g_table("table0", "index0", "MANIFEST0");
std::atomic<size_t> g_nr_expected_query_results(0);
const uint64_t g_lower_bound = 1000;
const uint64_t g_upper_bound = 100000000;
int g_query_attr_id = 0;

void thd_routine(const int n) {
  Random rnd;
  for (int i = 0; i < n; i++) {
    /* 添加记录 */
    std::vector<uint64_t> nums =
        rnd.GenerateRandomNumbers(Table::kNumTableAttributes);
    Status status = g_table.Append(nums);
    ASSERT_TRUE(status.ok());

    /* 添加的记录是否应该在后续的查询结果中出现? */
    if (nums[g_query_attr_id] >= g_lower_bound &&
        nums[g_query_attr_id] <= g_upper_bound) {
      g_nr_expected_query_results++;
    }
  }
}

TEST(table_storage, multi_thread) {
  TestHelper testhlp;
  Status status;
  const int nr_thds = 8;
  const int n = 1000 / nr_thds;

  /* 从data文件读取上次的查询结果数, 如果data文件存在的话 */
  testhlp.LoadLastQueryResultsFromFile(
      "data0", reinterpret_cast<int*>(&g_nr_expected_query_results));

  std::thread** thds = new std::thread* [nr_thds];
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

  /* 将新的查询结果数写入data文件 */
  status = testhlp.SaveLastQueryResultsToFile(
      "data0", static_cast<int>(g_nr_expected_query_results));
  ASSERT_TRUE(status.ok());

  /* 在query_attr_id指定的属性上进行范围查找. 由于索引未建立,
   * 查询时需要读取table_file */
  std::vector<std::vector<uint64_t> > query_results;
  status = g_table.Lookup(g_query_attr_id, g_lower_bound, g_upper_bound,
                          &query_results);
  if (!status.ok()) {
    ASSERT_TRUE(status.IsNotFound());
  }

  /* 校验查询结果 */
  ASSERT_EQ(query_results.size(), g_nr_expected_query_results);
  for (size_t i = 0; i < query_results.size(); i++) {
    ASSERT_EQ(query_results[i].size(), Table::kNumTableAttributes);
    ASSERT_TRUE(query_results[i][g_query_attr_id] >= g_lower_bound &&
                query_results[i][g_query_attr_id] <= g_upper_bound);
  }

  /* 建立索引后在次测试查询 */
  g_table.BuildIndexBlock(g_query_attr_id);
  query_results.clear();

  status = g_table.Lookup(g_query_attr_id, g_lower_bound, g_upper_bound,
                          &query_results);
  if (!status.ok()) {
    ASSERT_TRUE(status.IsNotFound());
  }

  /* 校验查询结果 */
  ASSERT_EQ(query_results.size(), g_nr_expected_query_results);
  for (size_t i = 0; i < query_results.size(); i++) {
    ASSERT_EQ(query_results[i].size(), Table::kNumTableAttributes);
    ASSERT_TRUE(query_results[i][g_query_attr_id] >= g_lower_bound &&
                query_results[i][g_query_attr_id] <= g_upper_bound);
  }

  /* 打印查询结果 */
  size_t cnt = std::min(static_cast<size_t>(10), query_results.size());
#ifdef WIN32
  printf("%lu query query_results with range [0x%llX, 0x%llX] on attr %d:\n",
         query_results.size(), g_lower_bound, g_upper_bound, g_query_attr_id);
#else
  printf("%lu query query_results with range [0x%lX, 0x%lX] on attr %d:\n",
         query_results.size(), g_lower_bound, g_upper_bound, g_query_attr_id);
#endif  // WIN32
  for (size_t i = 0; i < cnt; i++) {
    printf("[%lu] ", i);
    printf("...... %016lX ......\n", query_results[i][g_query_attr_id]);
  }
}
#endif

#if 1
TEST(table_storage, single_thread1) {
  Table table("table1", "index1", "MANIFEST1");
  TestHelper testhlp;
  Random rnd;
  Status status;
  const int n = 1000;
  const uint64_t lower_bound = 1000;
  const uint64_t upper_bound = 100000000;
  int query_attr_id = 0;
  size_t nr_expected_query_results = 0;
  std::vector<std::vector<uint64_t> > query_results;
  clock_t start, end;

  /* 从data文件读取上次的查询结果数, 如果data文件存在的话 */
  testhlp.LoadLastQueryResultsFromFile(
      "data1", reinterpret_cast<int*>(&nr_expected_query_results));

  /* 向表中添加数据 */
  start = clock();
  std::cout << "Appending data...\n";
  for (int i = 0; i < n; i++) {
    /* 添加记录 */
    std::vector<uint64_t> nums =
        rnd.GenerateRandomNumbers(Table::kNumTableAttributes);
    status = table.Append(nums);
    ASSERT_TRUE(status.ok());

    /* 添加的记录是否应该在后续的查询结果中出现? */
    if (nums[query_attr_id] >= lower_bound &&
        nums[query_attr_id] <= upper_bound) {
      nr_expected_query_results++;
    }

    /* 在query_attr_id指定的属性上进行范围查找. 由于索引未建立,
     * 查询时需要读取table_file */
    query_results.clear();
    status =
        table.Lookup(query_attr_id, lower_bound, upper_bound, &query_results);
    if (!status.ok()) {
      ASSERT_TRUE(status.IsNotFound());
    }

    /* 校验查询结果 */
    ASSERT_EQ(query_results.size(), nr_expected_query_results);
    for (size_t i = 0; i < query_results.size(); i++) {
      ASSERT_EQ(query_results[i].size(), Table::kNumTableAttributes);
      ASSERT_TRUE(query_results[i][query_attr_id] >= lower_bound &&
                  query_results[i][query_attr_id] <= upper_bound);
    }
  }
  end = clock();
  printf("Done. Time elapsed: %.5fs\n", (double)(end - start) / CLOCKS_PER_SEC);

  /* 将新的查询结果数写入data文件 */
  status = testhlp.SaveLastQueryResultsToFile(
      "data1", static_cast<int>(nr_expected_query_results));
  ASSERT_TRUE(status.ok());

  /* 建立索引 */
  status = table.Finish();
  ASSERT_TRUE(status.ok());
  status = table.BuildIndexBlock(query_attr_id);
  ASSERT_TRUE(status.ok());

  /* 在query_attr_id指定的属性上进行范围查找. 索引已建立, 查询时使用索引加速查找
   */
  query_results.clear();
  status =
      table.Lookup(query_attr_id, lower_bound, upper_bound, &query_results);
  if (!status.ok()) {
    ASSERT_TRUE(status.IsNotFound());
  }

  /* 校验查询结果 */
  ASSERT_EQ(query_results.size(), nr_expected_query_results);
  for (size_t i = 0; i < query_results.size(); i++) {
    ASSERT_EQ(query_results[i].size(), Table::kNumTableAttributes);
    ASSERT_TRUE(query_results[i][query_attr_id] >= lower_bound &&
                query_results[i][query_attr_id] <= upper_bound);
  }

  /* 打印查询结果. 最多打印10条记录 */
  size_t cnt = std::min(static_cast<size_t>(10), query_results.size());
#ifdef WIN32
  printf("%lu query query_results with range [0x%llX, 0x%llX] on attr %d:\n",
         query_results.size(), lower_bound, upper_bound, query_attr_id);
#else
  printf("%lu query query_results with range [0x%lX, 0x%lX] on attr %d:\n",
         query_results.size(), lower_bound, upper_bound, query_attr_id);
#endif  // WIN32
  for (size_t i = 0; i < cnt; i++) {
    printf("[%lu] ", i);
    printf("...... %016lX ......\n", query_results[i][query_attr_id]);
  }
}
#endif

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}