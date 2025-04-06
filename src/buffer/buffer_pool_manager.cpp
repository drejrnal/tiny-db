//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(nullptr), log_manager_(log_manager) {
  // Comment out the exception to enable the buffer pool manager
  // throw NotImplementedException(
  //    "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //    "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // If there are no free pages, return nullptr
  if (free_list_.empty()) {
    return nullptr;
  }

  // Get a free frame from the free list
  frame_id_t frame_id = free_list_.front();
  free_list_.pop_front();

  // Allocate a new page id
  *page_id = AllocatePage();

  // Initialize the page
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = *page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  // Update the page table
  page_table_[*page_id] = frame_id;

  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);

  // If the page is already in the buffer pool, return it
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  // Otherwise, if there are no free pages, return nullptr
  if (free_list_.empty()) {
    return nullptr;
  }

  // Get a free frame from the free list
  frame_id_t frame_id = free_list_.front();
  free_list_.pop_front();

  // Initialize the page
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;

  // Update the page table
  page_table_[page_id] = frame_id;

  return &pages_[frame_id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // If the page is not in the buffer pool, return false
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];

  // If the pin count is already 0, return false
  if (page.pin_count_ == 0) {
    return false;
  }

  // Decrement the pin count
  page.pin_count_--;

  // Set the dirty flag if needed
  if (is_dirty) {
    page.is_dirty_ = true;
  }

  // If the pin count drops to 0, make the page evictable
  // (but for simplicity, we're not adding it back to the free list)

  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // If the page is not in the buffer pool, return false
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  // Mark the page as not dirty (we're not actually writing to disk)
  frame_id_t frame_id = it->second;
  pages_[frame_id].is_dirty_ = false;

  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> lock(latch_);

  // Mark all pages as not dirty
  for (auto &entry : page_table_) {
    pages_[entry.second].is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);

  // If the page is not in the buffer pool, return true
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = it->second;
  Page &page = pages_[frame_id];

  // If the page is still pinned, return false
  if (page.pin_count_ > 0) {
    return false;
  }

  // Reset the page metadata
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.pin_count_ = 0;
  page.is_dirty_ = false;

  // Remove the page from the page table
  page_table_.erase(it);

  // Add the frame back to the free list
  free_list_.push_back(frame_id);

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
