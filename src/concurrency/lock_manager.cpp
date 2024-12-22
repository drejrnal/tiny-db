//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

// helper function to determine whether current transaction can take the lock based on the txn isolation level, throw exception if not allowed to grant lock
void LockManager::ValidateLockMode(TransactionWith2PL *txn, LockMode lock_mode) {
  if (txn->GetIsolationLevel() == IsolationLevelWith2PL::READ_UNCOMMITTED &&
      ( (lock_mode == LockMode::SHARED) || (lock_mode == LockMode::INTENTION_SHARED) ) ) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if( txn->GetState() == TransactionStateWith2PL::SHRINKING ) {
    if ( (txn->GetIsolationLevel() == IsolationLevelWith2PL::READ_COMMITTED) && (lock_mode == LockMode::INTENTION_SHARED || lock_mode == LockMode::SHARED) ){
      return;
    }
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
}

auto LockManager::LockTable(TransactionWith2PL *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  //if lock validation of the txn fails, throw exception
  ValidateLockMode(txn, lock_mode);
  // construct a lock request based on txn
  auto *lock_request = new LockRequest(txn->GetTransactionId(), lock_mode, oid);
  // get the lock request queue of table oid, if the queue is empty, construct a new one, and add the lock request to
  // the queue
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    auto lock_queue = new LockRequestQueue();
    table_lock_map_[oid] = std::shared_ptr<LockRequestQueue>(lock_queue);
    std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
    lock_queue->request_queue_.push_back(std::shared_ptr<LockRequest>(lock_request));
    // TODO 向事务txn的shared_lock_set或exclusive_lock_set添加oid
    return true;
  }  // get the lock mode of the first lock request in the queue and check whether lock_mode is compatible with it
  auto lock_queue = table_lock_map_[oid];
  table_lock.unlock();
  if (lock_queue->txn_lock_request_map_.find(txn->GetTransactionId()) != lock_queue->txn_lock_request_map_.end()) {
    // lock upgrading because the lock request is already in the queue
    return UpgradeLockTable(txn, lock_mode, oid, lock_queue.get());
  }
  std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
  // this is the first lock request in this queue, so grant the lock to it
  if (lock_queue->request_queue_.empty()) {
    lock_request->granted_ = true;
    lock_queue->request_queue_.push_back(std::shared_ptr<LockRequest>(lock_request));
    return true;
  }
  // wait until the lock is compatible with the existing lock mode in the queue and consider there will be a concurrent transaction which will be granted the lock whern resource is unlocked
  lock_queue->cv_.wait(queue_lock, [&lock_queue, &lock_mode, this]() {
    for (const auto &traverse_lock_request : lock_queue->request_queue_) {
      if (traverse_lock_request->granted_ &&
          (lock_compatible_table_[traverse_lock_request->lock_mode_].find(lock_mode) ==
           lock_compatible_table_[traverse_lock_request->lock_mode_].end())) {
        return false;
      }
    }
    return lock_queue->upgrading_ == INVALID_TXN_ID;
  });
  // now this thread is notified by another thread or is compatible with the existing lock mode, so add the lock
  // request to the queue
  lock_request->granted_ = true;
  lock_queue->request_queue_.push_back(std::shared_ptr<LockRequest>(lock_request));
  // TODO 向事务txn的shared_lock_set或exclusive_lock_set添加oid
  return true;
}

auto LockManager::UpgradeLockTable(TransactionWith2PL *txn, LockMode lock_mode, const table_oid_t &oid,
                                   LockRequestQueue *request_queue) -> bool {
  std::unique_lock<std::mutex> queue_lock(request_queue->latch_);
  auto current_lock_request = request_queue->txn_lock_request_map_[txn->GetTransactionId()];
  // if txn does not hold the lock or the current lock mode is the same as the target lock mode, return false
  if (!(*current_lock_request)->granted_ || ((*current_lock_request)->lock_mode_ == lock_mode)) {
    return false;
  }
  if (!CanLockUpgrade((*current_lock_request)->lock_mode_, lock_mode)) {
    // TODO throw upgrade exception
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }
  // only one transaction is doing upgrading specific to a resource/table
  request_queue->cv_.wait(queue_lock, [&request_queue]() { return request_queue->upgrading_ == INVALID_TXN_ID; });
  request_queue->cv_.wait(queue_lock, [&request_queue, &txn, &lock_mode, this]() {
    for (const auto &traverse_lock_request : request_queue->request_queue_) {
      if (traverse_lock_request->txn_id_ == txn->GetTransactionId()) {
        continue;
      }
      if (traverse_lock_request->granted_ &&
          (lock_compatible_table_[traverse_lock_request->lock_mode_].find(lock_mode) ==
           lock_compatible_table_[traverse_lock_request->lock_mode_].end())) {
        return false;
      }
    }
    return request_queue->upgrading_ == INVALID_TXN_ID;
    // how to replace the above for-loop code into std::all_of() to make code tidy?
  });
  request_queue->upgrading_ = txn->GetTransactionId();
  (*current_lock_request)->lock_mode_ = lock_mode;
  (*current_lock_request)->granted_ = true;
  return true;
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
  // TODO lock upgrade的兼容表格
  return true;
}

auto LockManager::UnlockTable(TransactionWith2PL *txn, const table_oid_t &oid) -> bool {
  // find the lock request queue of table oid, and remove the lock request of txn from the queue
  std::unique_lock<std::mutex> table_lock(table_lock_map_latch_);
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    // todo throw exception
    return false;
  } else {
    auto lock_queue = table_lock_map_[oid];
    table_lock.unlock();
    std::unique_lock<std::mutex> queue_lock(lock_queue->latch_);
    if (lock_queue->request_queue_.empty()) {
      // todo throw exception
      return false;
    }
    lock_queue->request_queue_.pop_front();
    lock_queue->cv_.notify_all();
    // TODO 从事务txn的shared_lock_set或exclusive_lock_set删除oid
    return true;
  }
}

auto LockManager::LockRow(TransactionWith2PL *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(TransactionWith2PL *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { return false; }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

}  // namespace bustub
