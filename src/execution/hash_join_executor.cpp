//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  // build the join hash table
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;
  while (!right_child_executor_->Next(&right_tuple, &right_rid)) {
    auto right_join_key = MakeRightJoinKey(&right_tuple);
    hash_join_table_[right_join_key].emplace_back(right_tuple);
  }
  while (!left_child_executor_->Next(&left_tuple, &left_rid)) {
    auto left_join_key = MakeLeftJoinKey(&left_tuple);
    std::vector<Value> joined_values;
    Schema left_schema = left_child_executor_->GetOutputSchema();
    Schema right_schema = right_child_executor_->GetOutputSchema();
    for (uint32_t col_idx = 0; col_idx < left_schema.GetColumnCount(); col_idx++) {
      joined_values.push_back(left_tuple.GetValue(&left_schema, col_idx));
    }
    if (hash_join_table_.find(left_join_key) == hash_join_table_.end()) {
      if (plan_->GetJoinType() == JoinType::LEFT) {
        // if the join type is left join, then we need to build the joined tuple with null values
        for (uint32_t col_idx = 0; col_idx < right_schema.GetColumnCount(); col_idx++) {
          joined_values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(col_idx).GetType()));
        }
        hash_join_result_.emplace_back(joined_values, &GetOutputSchema());
      }
    } else {
      auto right_tuples = hash_join_table_[left_join_key];
      // build the joined tuple if outer table has the same join key as inner table
      for (const auto &tuple : right_tuples) {
        auto right_join_key = MakeRightJoinKey(&tuple);
        if (right_join_key == left_join_key) {
          for (uint32_t col_idx = 0; col_idx < right_schema.GetColumnCount(); col_idx++) {
            joined_values.push_back(tuple.GetValue(&right_schema, col_idx));
          }
        } else if (plan_->GetJoinType() == JoinType::LEFT) {
          for (uint32_t col_idx = 0; col_idx < right_schema.GetColumnCount(); col_idx++) {
            joined_values.push_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(col_idx).GetType()));
          }
        }
        hash_join_result_.emplace_back(joined_values, &GetOutputSchema());
      }
    }
  }
  result_iterator_ = hash_join_result_.cbegin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iterator_ == hash_join_result_.end()) {
    return false;
  }
  *tuple = *(result_iterator_++);
  return true;
}

}  // namespace bustub
