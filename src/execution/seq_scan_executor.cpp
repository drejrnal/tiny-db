//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), table_iter_(nullptr, RID(), RID()) {
  this->plan_ = plan;
  this->table_info_ = exec_ctx->GetCatalog()->GetTable(plan->table_oid_);
}

void SeqScanExecutor::Init() {
  this->table_heap_ = this->table_info_->table_.get();
  this->table_iter_ = this->table_info_->table_->MakeIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_manager = exec_ctx_->GetTransactionManager();
  Schema schema = this->table_info_->schema_;
  if (this->table_iter_.IsEnd()) {
    return false;
  }
  // 获取table读锁
  auto current_rid = this->table_iter_.GetRID();
  auto read_page_guard = this->table_heap_->AcquireTablePageReadLock(current_rid);
  auto table_page = read_page_guard.As<TablePage>();

  auto next_tuple_pair = this->table_heap_->GetTupleWithLockAcquired(current_rid, table_page);
  if (txn->GetTransactionTempTs() == next_tuple_pair.first.ts_ || next_tuple_pair.first.ts_ <= txn->GetReadTs()) {
    // 读取当前事务正在修改的tuple或者当前tuple的时间戳小于当前事务的读时间戳
    *tuple = next_tuple_pair.second;
    *rid = tuple->GetRid();
  } else {
    std::vector<UndoLog> undo_logs = CollectUndoLogs(txn, next_tuple_pair.second.GetRid(), txn_manager);
    std::optional<Tuple> result = ReconstructTuple(&schema, next_tuple_pair.second, next_tuple_pair.first, undo_logs);
    if (result == std::nullopt) {
      ++this->table_iter_;
      return false;
    }
    *tuple = result.value();
    *rid = tuple->GetRid();
  }
  ++this->table_iter_;
  read_page_guard.Drop();
  return true;
}
}  // namespace bustub
