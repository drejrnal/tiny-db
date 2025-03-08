//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwlatch_test.cpp
//
// Identification: test/common/rwlatch_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <thread>  // NOLINT
#include <vector>

#include "common/rwlatch.h"
#include "common/logger.h"
#include "gtest/gtest.h"

namespace bustub {

class Counter {
 public:
  Counter() = default;
  void Add(int num) {
    mutex_.WLock();
    count_ += num;
    mutex_.WUnlock();
  }
  auto Read() -> int {
    int res;
    mutex_.RLock();
    res = count_;
    mutex_.RUnlock();
    return res;
  }

 private:
  int count_{0};
  ReaderWriterLatch mutex_{};
};

// NOLINTNEXTLINE
TEST(RWLatchTest, BasicTest) {
  int num_threads = 100;
  LOG_INFO("Spawning %d threads", num_threads);
}

TEST(RWLatchTest, WriteTest) {
  int num_threads = 100;
  Counter counter;
  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; i++) {
    threads.emplace_back([&counter] { counter.Add(1); });
  }
  for (auto &t : threads) {
    t.join();
  }
  EXPECT_EQ(counter.Read(), num_threads);
}
}  // namespace bustub
