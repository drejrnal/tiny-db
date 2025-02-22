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
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));

  // Set the read timestamp for the transaction
  txn_ref->read_ts_ = running_txns_.commit_ts_;
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
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

  // TODO(fall2023): Implement the commit logic!
  // TODO iterate all the tuples modified by the transaction, and set tuple meta's ts to commit timestamp
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  std::for_each(txn->undo_logs_.begin(), txn->undo_logs_.end(), [&txn](UndoLog &undo_log) {
    // 更新undo log的timestamp
    // transaction在更新tuple时，undo log记录的timestamp表示该事务当前正在进行修改，为特殊值，其他事务不可修改
    // 事务提交时，undo log的timestamp更新为commit timestamp
    undo_log.ts_ = txn->commit_ts_;
  });
  // todo 事务提交后，其所做的更改需要更新到table heap中
  // 当事务结构被析构时，其undo log会被清空，对应的undo link需要从version chain中移除

  // TODO(fall2023): set commit timestamp + update last committed timestamp here.

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

  // TODO(fall2023): Implement the abort logic!

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}

void TransactionManager::GarbageCollection() {
  timestamp_t watermark = running_txns_.GetWatermark();

  std::unique_lock<std::shared_mutex> lock(txn_map_mutex_);
  // First collect all running transactions
  std::vector<Transaction *> running_txns;
  for (const auto &[_, txn] : txn_map_) {
    if (txn->GetTransactionState() == TransactionState::RUNNING) {
      running_txns.push_back(txn.get());
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

    /** 检查当前事务所有的undolog是否不再被使用，若都不再被使用，则从事务表中删除该事务 */
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
      // This ensures we don't prematurely delete versions that running txns need
      for (const auto *running_txn : running_txns) {
        //如果事务的read_ts属于[undo log.ts_，next_undo_log.ts_),则不能回收undolog
        auto read_ts = running_txn->GetReadTs();
        if (read_ts >= undo_log.ts_) {
          auto next_link = undo_log.next_version_;
          // next_link为当前undolog的下一个版本,转换为UndoLink便于获取下一个版本的undolog
          auto next_undo_link = UndoLink{.prev_txn_ = next_link.next_txn_, .prev_log_idx_ = next_link.next_log_idx_};
          if (next_link.IsValid()) {
            auto next_log = GetUndoLog(next_undo_link);
            if (next_log.ts_ > read_ts) {
              can_gc = false;
              break;  // No need to check other running txns
            }
          }
        }
      }
      //当前事务存在不能回收的Undolog，则当前事务不被回收
      if (!can_gc) {
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
