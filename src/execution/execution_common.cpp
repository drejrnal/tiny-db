#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
/**
 * @param schema base tuple对应的schema
 * @param base_tuple
 * @param base_meta
 * @param undo_logs
 * @return
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  if (undo_logs.empty()) {
    return base_meta.is_deleted_ ? std::nullopt : std::make_optional(base_tuple);
  } else if ((undo_logs.end()- 1)->is_deleted_) {
    // 这里undo log出现被删除的情况,因为不会tuple被删除又重新插入，但是根据ts的比较，当前事务读取的tuple是被删除的，所以undo log最后一项是个删除项
    return std::nullopt;
  } else {
    std::vector<Value> values{schema->GetColumnCount()};
    for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
      values[i] = base_tuple.GetValue(schema, i);
    }
    for (UndoLog undo_log : undo_logs) {
      if (!undo_log.is_deleted_) {
        // tuple里每一列只维护在一个undo log里
        for (uint32_t i = 0; i < undo_log.modified_fields_.size(); ++i) {
          if (undo_log.modified_fields_[i]) {
            values[i] = undo_log.tuple_.GetValue(schema, i);
          }
        }
      }
    }
    return std::make_optional<Tuple>(values, schema);
  }
}

/**
 * 调用前需判断是否读当前事务正修改的tuple，只有不是的情况，才会调用此函数
 */
auto CollectUndoLogs(Transaction *txn, RID rid, TransactionManager *txn_manager) -> std::vector<UndoLog> {
  std::vector<UndoLog> undo_logs;
  std::optional<UndoLink> undo_link_optional = txn_manager->GetUndoLink(rid);
  if (undo_link_optional.has_value()) {
    UndoLink undo_link = undo_link_optional.value();
    // undo_link此处拿到以后，会存在并发更新的情况
    while (undo_link.prev_txn_ != INVALID_TXN_ID) {
      UndoLog undo_log = txn_manager->GetUndoLog(undo_link);
      if (undo_log.ts_ >= txn->GetReadTs()) {
        undo_logs.emplace_back(undo_log);
      } else {
        break;
      }
      undo_link = undo_log.prev_version_;
    }
  }
  return undo_logs;
}

/**
 * 构造只保留更新字段的tuple
 */
static auto generateModifiedTuple(const Tuple &old_tuple, const Tuple &updated_tuple, const Schema &schema) {
  std::vector<Value> modified_values;
  std::vector<uint32_t> modified_columns_indice;
  for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
    if (old_tuple.GetValue(&schema, i).CompareNotEquals(updated_tuple.GetValue(&schema, i)) == CmpBool::CmpTrue) {
      modified_columns_indice.emplace_back(i);
      modified_values.push_back(old_tuple.GetValue(&schema, i));
    }
  }
  auto modified_schema = Schema::CopySchema(&schema, modified_columns_indice);
  return Tuple{modified_values, &modified_schema};
}

auto GenerateNewUndoLog(Transaction *txn, const Tuple &old_tuple, const Tuple &updated_tuple, bool is_deleted, timestamp_t prev_commit_ts,
                        const Schema &schema, const std::optional<UndoLink> &prev_link) -> UndoLog {
  UndoLog undo_log;
  undo_log.ts_ = prev_commit_ts;
  if (!is_deleted){
    undo_log.modified_fields_.reserve(schema.GetColumnCount());
    UpdateExistUndoLog(&undo_log, is_deleted, schema, old_tuple, updated_tuple);
    if (prev_link.has_value()) {
      undo_log.prev_version_ = prev_link.value();
    }
  }
  return undo_log;
}

auto UpdateExistUndoLog(UndoLog *exist_undo_log, bool is_deleted, const Schema &schema, const Tuple &old_tuple,
                        const Tuple &updated_tuple) -> void {
  exist_undo_log->is_deleted_ = is_deleted;
  if (!is_deleted) {
    for (uint32_t i = 0; i < schema.GetColumnCount(); ++i) {
      if (old_tuple.GetValue(&schema, i).CompareNotEquals(updated_tuple.GetValue(&schema, i)) == CmpBool::CmpTrue) {
        exist_undo_log->modified_fields_[i] = true;
      } else {
        exist_undo_log->modified_fields_[i] = false;
      }
    }
    exist_undo_log->tuple_ = generateModifiedTuple(old_tuple, updated_tuple, schema);
  }
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  // Traverse the table heap and print each tuple's version chain
  auto iter = table_heap->MakeIterator();

  while (!iter.IsEnd()) {
    auto rid = iter.GetRID();
    // 获取table读锁
    auto read_page_guard = table_heap->AcquireTablePageReadLock(rid);
    auto table_page = read_page_guard.As<TablePage>();

    auto [meta, tuple] = table_heap->GetTupleWithLockAcquired(rid, table_page);
    read_page_guard.Drop();
    // Print the tuple info
    fmt::println(
        stderr, "RID={}/{} ts={} tuple={}", rid.GetPageId(), rid.GetSlotNum(),
        (meta.ts_ & TXN_START_ID) ? fmt::format("txn{}", (meta.ts_ ^ TXN_START_ID)) : fmt::format("{}", meta.ts_),
        meta.is_deleted_ ? "<del marker>" : tuple.ToString(&table_info->schema_));

    // Traverse the version chain
    std::optional<UndoLink> undo_link_opt = txn_mgr->GetUndoLink(rid);
    std::string indent = "  ";

    while (undo_link_opt.has_value()) {
      UndoLink undo_link = undo_link_opt.value();
      if (undo_link.prev_txn_ == INVALID_TXN_ID) {
        break;
      }

      UndoLog undo_log = txn_mgr->GetUndoLog(undo_link);
      fmt::println(stderr, "{}txn{}@{} {} ts={}", indent,
                   (undo_link.prev_txn_ ^ TXN_START_ID),  // Human readable txn id
                   undo_link.prev_log_idx_,
                   undo_log.is_deleted_ ? "<del>" : undo_log.tuple_.ToString(&table_info->schema_), undo_log.ts_);

      // Move to the next version in the chain
      undo_link_opt = undo_log.prev_version_.IsValid() ? std::make_optional(undo_log.prev_version_) : std::nullopt;
    }

    // Move to the next tuple
    ++iter;
  }
}

}  // namespace bustub
