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
 * 如果tuple被删除，直接返回base_tuple，需要保证tableheap保存删除前的base tuple
 * @param schema
 * @param base_tuple
 * @param base_meta
 * @param undo_logs
 * @return
 */
auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  std::vector<Value> values{schema->GetColumnCount()};
  if (base_meta.is_deleted_){
    return base_tuple;
  }
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    values[i] = base_tuple.GetValue(schema, i);
  }
  if (undo_logs.end()->is_deleted_){
    return std::nullopt;
  }
  for (UndoLog undoLog : undo_logs) {
    if (undoLog.is_deleted_){
      continue;
    }else{
      //tuple里每一列只维护在一个undo log里
      for (uint32_t i = 0; i < undoLog.modified_fields_.size(); ++i) {
        if (undoLog.modified_fields_[i]) {
          values[i] = undoLog.tuple_.GetValue(schema, i);
        }
      }
    }
  }
  return std::make_optional<Tuple>(values, schema);
}

std::vector<UndoLog> CollectUndoLogs(Transaction *txn, RID rid,
                                     TransactionManager *txn_manager, const TupleMeta &base_meta) {
  std::vector<UndoLog> undo_logs;
  if (base_meta.ts_ == TXN_START_ID + txn->GetTransactionId()) {
    return undo_logs;
  }
  std::optional<UndoLink> undo_link = txn_manager->GetUndoLink(rid);
  while (undo_link.has_value()) {
    UndoLog undo_log = txn_manager->GetUndoLog(undo_link.value());
    if (undo_log.ts_ >= txn->GetReadTs()) {
      undo_logs.emplace_back(undo_log);
    } else{
      break;
    }
  }
  return undo_logs;
}

auto GenerateNewUndoLog(Transaction *txn, const Tuple &old_tuple, const Tuple &updated_tuple, bool is_deleted, const Schema &schema, const UndoLink &prev_link) -> UndoLog{
  UndoLog undo_log;
  undo_log.is_deleted_ = is_deleted;
  undo_log.ts_ = txn->GetTransactionId();
  if (!is_deleted){
    undo_log.modified_fields_.reserve(schema.GetColumnCount());
    std::vector<Column> modified_columns;
    std::vector<Value> modified_values;
    for(uint32_t i = 0; i < schema.GetColumnCount(); ++i){
      if(old_tuple.GetValue(&schema, i).CompareNotEquals(updated_tuple.GetValue(&schema, i)) == CmpBool::CmpTrue){
        undo_log.modified_fields_[i] = true;
        modified_columns.push_back(schema.GetColumn(i));
        modified_values.push_back(updated_tuple.GetValue(&schema, i));
      } else{
        undo_log.modified_fields_[i] = false;
      }
    }
    Schema modified_schema{modified_columns};
    undo_log.tuple_ = Tuple{modified_values, &modified_schema};
    undo_log.prev_version_ = prev_link;
  }
  return undo_log;
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);

  fmt::println(
      stderr,
      "You see this line of text because you have not implemented `TxnMgrDbg`. You should do this once you have "
      "finished task 2. Implementing this helper function will save you a lot of time for debugging in later tasks.");

  // We recommend implementing this function as traversing the table heap and print the version chain. An example output
  // of our reference solution:
  //
  // debug_hook: before verify scan
  // RID=0/0 ts=txn8 tuple=(1, <NULL>, <NULL>)
  //   txn8@0 (2, _, _) ts=1
  // RID=0/1 ts=3 tuple=(3, <NULL>, <NULL>)
  //   txn5@0 <del> ts=2
  //   txn3@0 (4, <NULL>, <NULL>) ts=1
  // RID=0/2 ts=4 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn7@0 (5, <NULL>, <NULL>) ts=3
  // RID=0/3 ts=txn6 <del marker> tuple=(<NULL>, <NULL>, <NULL>)
  //   txn6@0 (6, <NULL>, <NULL>) ts=2
  //   txn3@1 (7, _, _) ts=1
}

}  // namespace bustub
