//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  this->plan_ = plan;
  this->child_executor_ = std::move(child_executor);
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void UpdateExecutor::Init() { child_executor_->Init(); }

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple child_tuple;
  RID child_rid;
  if (!child_executor_->Next(&child_tuple, &child_rid)) {
    return false;
  }
  // Update the tuple
  TableHeap *table = table_info_->table_.get();
  Transaction *txn = exec_ctx_->GetTransaction();
  TransactionManager *txn_mgr = exec_ctx_->GetTransactionManager();
  const Schema &schema = child_executor_->GetOutputSchema();
  auto old_meta = table->GetTupleMeta(child_rid);
  // 如果当前tuple正在被其他事务修改
  if (old_meta.ts_ != txn->GetTransactionId() && (((old_meta.ts_ & TXN_START_ID) >> 62) != 0)) {
    return false;
  } else if (old_meta.ts_ > txn->GetReadTs()) {
    // 如果tuple已提交，因当前事务的read_ts小于tuple的ts，所以不可见
    return false;
  } else {
    TupleMeta meta = TupleMeta{.ts_ = txn->GetTransactionId(), .is_deleted_ = false};
    std::vector<Value> updated_values;
    updated_values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      // update tuple
      updated_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple updated_tuple(updated_values, &child_executor_->GetOutputSchema());
    if (table->UpdateTupleInPlace(meta, updated_tuple, child_rid,
                                  [&old_meta](const TupleMeta &origin_meta, const Tuple &tuple, const RID rid) -> bool {
                                    return origin_meta.ts_ == old_meta.ts_;
                                  })) {
      table->UpdateTupleMeta(meta, child_rid);
      UndoLink new_undo_link;
      auto prev_undo_link = txn_mgr->GetUndoLink(child_rid);
      if (txn->GetUndoLogIndex(child_rid) != INVALID_UNDOLOG_INDEX) {
        UndoLog exist_undo_log = txn->GetUndoLog(txn->GetUndoLogIndex(child_rid));
        UpdateExistUndoLog(&exist_undo_log, false, schema, child_tuple, updated_tuple);
        txn->ModifyUndoLog(txn->GetUndoLogIndex(child_rid), exist_undo_log);
      } else {
        auto new_undo_log = GenerateNewUndoLog(txn, child_tuple, updated_tuple, false,
                                               child_executor_->GetOutputSchema(), prev_undo_link);
        new_undo_link = txn->AppendUndoLog(new_undo_log);
        txn->RecordRidUndoLogIndex(child_rid, txn->GetUndoLogNum() - 1);
      }
      /* 更新page version info表 */
      txn_mgr->UpdateUndoLink(child_rid, new_undo_link, [&prev_undo_link](const std::optional<UndoLink> &undo) -> bool {
        if (undo.has_value() && prev_undo_link.has_value()) {
          return undo.value() == prev_undo_link.value();
        } else if (!undo.has_value() && !prev_undo_link.has_value()) {
          return true;
        } else {
          return false;
        }
      });
      *rid = child_rid;
      return true;
    } else {
      return false;
    }
  }
}

}  // namespace bustub
