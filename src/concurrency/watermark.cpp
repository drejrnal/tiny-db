#include "concurrency/watermark.h"
#include <algorithm>
#include <exception>
#include "common/exception.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }

  if (current_reads_.find(read_ts) != current_reads_.end()) {
    current_reads_[read_ts]++;
  } else {
    current_reads_[read_ts] = 1;
  }
  // get the minimum from keys of current_reads_
  auto min_read_ts = std::min_element(
      current_reads_.begin(), current_reads_.end(),
      [](const std::pair<timestamp_t, int> &a, const std::pair<timestamp_t, int> &b) { return a.first < b.first; });
  watermark_ = min_read_ts->first;
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  auto it = current_reads_.find(read_ts);
  if (it != current_reads_.end()) {
    if (--it->second == 0) {
      current_reads_.erase(it);
    }
  }
  if (current_reads_.empty()) {
    watermark_ = commit_ts_;
  } else {
    auto min_read_ts = std::min_element(
        current_reads_.begin(), current_reads_.end(),
        [](const std::pair<timestamp_t, int> &a, const std::pair<timestamp_t, int> &b) { return a.first < b.first; });
    watermark_ = min_read_ts->first;
  }
}

}  // namespace bustub
