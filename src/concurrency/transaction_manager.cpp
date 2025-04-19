//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();

  // Set the read timestamp for the transaction
  txn_ref->read_ts_ = running_txns_.commit_ts_;
  running_txns_.AddTxn(txn_ref->read_ts_);

  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  return txn_map_[txn_id].get();
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  txn->commit_ts_ = running_txns_.commit_ts_ + 1;

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }
  /*
  std::for_each(txn->undo_logs_.begin(), txn->undo_logs_.end(), [&txn](UndoLog &undo_log) {
    // 更新undo log的timestamp
    // transaction在更新tuple时，undo log记录的timestamp表示该事务当前正在进行修改，为特殊值，其他事务不可修改
    // 事务提交时，undo log的timestamp更新为commit timestamp
    undo_log.ts_ = txn->commit_ts_;
  });
  no need to update timestamp of undo log, since undo log is initialized with commit timestamp from old_meta of tuple
  */
  // 事务提交后，其所更改的tuple元信息需要更新到table heap中
  for (const auto &tuple_meta : txn->GetWriteSets()) {
    auto table = catalog_->GetTable(tuple_meta.first);
    std::for_each(tuple_meta.second.begin(), tuple_meta.second.end(), [&table, &txn](const RID &rid) {
      auto meta = TupleMeta{.ts_ = txn->commit_ts_, .is_deleted_ = false};
      table->table_->GetAndUpdateTupleMeta(rid, meta);
    });
  }
  // 当事务结构被析构时，其undo log会被清空，对应的undo link需要从version chain中移除
  txn->state_ = TransactionState::COMMITTED;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
  running_txns_.RemoveTxn(txn->read_ts_);

  last_commit_ts_.store(txn->commit_ts_);
  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  // Implement the abort logic
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  // Roll back any changes made by the transaction
  // For each tuple in the write set, we need to restore its original state
  for (const auto &tuple_meta : txn->GetWriteSets()) {
    auto table = catalog_->GetTable(tuple_meta.first);
    for (const auto &rid : tuple_meta.second) {
      // 获取table读锁
      auto read_page_guard = table->table_->AcquireTablePageReadLock(rid);
      auto table_page = read_page_guard.As<TablePage>();
      // Get the current metadata of the tuple
      TupleMeta current_meta = table->table_->GetTupleMetaWithLockAcquired(rid, table_page);
      read_page_guard.Drop();
      // Check if this is a tuple modified by this transaction (indicated by temporary txn ID)
      if (current_meta.ts_ == txn->GetTransactionTempTs()) {
        // Find the original state of the tuple before this transaction modified it
        std::optional<UndoLink> undo_link_opt = GetUndoLink(rid);
        if (undo_link_opt.has_value()) {
          auto undo_link = undo_link_opt.value();
          // Only process if this undolink belongs to our transaction
          if (undo_link.prev_txn_ == txn->GetTransactionId()) {
            // Get the undo log
            auto undo_log = txn->GetUndoLog(undo_link.prev_log_idx_);

            // If there was a previous version, restore it
            if (undo_log.prev_version_.IsValid()) {
              // Update the version chain to skip this transaction's entry
              UpdateUndoLink(rid, undo_log.prev_version_);
            } else {
              // If no previous version, this was a newly inserted tuple by this transaction
              // Set timestamp to 0 and mark as deleted to indicate it never existed
              UpdateUndoLink(rid, std::nullopt);
            }
            // Restore the original metadata timestamp from the previous version
            auto restored_meta = TupleMeta{.ts_ = undo_log.ts_, .is_deleted_ = undo_log.is_deleted_};
            table->table_->UpdateTupleInPlace(restored_meta, undo_log.tuple_, rid);
          }
        }
      }
    }
  }

  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  timestamp_t watermark = running_txns_.GetWatermark();

  std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);

  // First collect all running transactions and organize their read timestamps for O(log N) lookups
  std::map<timestamp_t, int> running_txn_read_ts;
  for (const auto &[_, txn] : txn_map_) {
    if (txn->GetTransactionState() == TransactionState::RUNNING) {
      timestamp_t read_ts = txn->GetReadTs();
      running_txn_read_ts[read_ts]++;
    }
  }

  for (auto it = txn_map_.begin(); it != txn_map_.end();) {
    const auto &txn = it->second;

    // Only handle committed or aborted transactions
    if (txn->GetTransactionState() != TransactionState::COMMITTED &&
        txn->GetTransactionState() != TransactionState::ABORTED) {
      ++it;
      continue;
    }

    /** Check if all undo logs of this transaction are no longer needed */
    bool can_gc = true;
    for (const auto &undo_log : txn->undo_logs_) {
      // If this version's timestamp is >= watermark, some transaction might need it
      if (undo_log.ts_ >= watermark) {
        can_gc = false;
        break;
      }

      // An undo log may be needed by a running transaction if:
      // 1. The running txn's read_ts is >= this version's timestamp AND
      // 2. The running txn's read_ts is < the next version's timestamp
      // Using a binary search on sorted read_ts for better performance

      auto next_link = undo_log.next_version_;
      timestamp_t next_ts = std::numeric_limits<timestamp_t>::max();  // Default if no next version

      if (next_link.IsValid()) {
        auto next_undo_link = UndoLink{.prev_txn_ = next_link.next_txn_, .prev_log_idx_ = next_link.next_log_idx_};
        auto next_log = GetUndoLog(next_undo_link);
        next_ts = next_log.ts_;
      }

      // Find the first read_ts >= undo_log.ts_
      auto lower_bound_it = running_txn_read_ts.lower_bound(undo_log.ts_);
      // Check if any read_ts falls in the range [undo_log.ts_, next_ts)
      if (lower_bound_it != running_txn_read_ts.end() && lower_bound_it->first < next_ts) {
        can_gc = false;
        break;
      }
    }

    if (can_gc) {
      it = txn_map_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace bustub
