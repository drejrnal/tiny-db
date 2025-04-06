#include <chrono>  // NOLINT
#include <cstdio>
#include <exception>
#include <functional>
#include <future>  // NOLINT
#include <memory>
#include <stdexcept>
#include <thread>  // NOLINT

#include "common/bustub_instance.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "fmt/core.h"
#include "gtest/gtest.h"
#include "txn_common.h"  // NOLINT

namespace bustub {

TEST(TransactionInsert, InsertTest) {
  auto bustub = std::make_unique<BustubInstance>();
  auto schema = ParseCreateStatement("a integer,b integer,c integer");
  auto table_info = bustub->catalog_->CreateTable(nullptr, "abort_test_table", *schema);

  // Set the catalog for transaction manager
  bustub->txn_manager_->SetCatalog(bustub->catalog_.get());

  // Initialize 3 transactions, txn1 and txn2 will be committed, txn3 will be aborted
  auto txn1 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn1->GetReadTs(), 0);

  // Insert initial tuple with txn1
  auto rid1 = *table_info->table_->InsertTuple(TupleMeta{txn1->GetTransactionTempTs(), false},
                                               Tuple{{Int(10), Int(20), Int(30)}, schema.get()});
  txn1->AppendWriteSet(table_info->oid_, rid1);
  bustub->txn_manager_->Commit(txn1);
  ASSERT_EQ(txn1->GetCommitTs(), 1);
  // TxnMgrDbg("after_insert", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // After txn1 commits, start txn3
  auto txn3 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn3->GetReadTs(), 1);

  // Create an undo log for txn3's update
  auto [old_meta, old_tuple] = table_info->table_->GetTuple(rid1);
  auto undo_log = UndoLog{.is_deleted_ = false,
                          .modified_fields_ = {true, true, false},  // Modify only the first two fields
                          .tuple_ = old_tuple,
                          .ts_ = old_meta.ts_,
                          .prev_version_ = UndoLink{INVALID_TXN_ID, 0}};
  auto undo_link = txn3->AppendUndoLog(undo_log);
  txn3->RecordRidUndoLogIndex(rid1, 0);
  txn3->AppendWriteSet(table_info->oid_, rid1);

  // Update the tuple with new values
  TupleMeta new_meta = TupleMeta{.ts_ = txn3->GetTransactionTempTs(), .is_deleted_ = false};
  Tuple new_tuple{{Int(11), Int(22), Int(30)}, schema.get()};
  table_info->table_->UpdateTupleInPlace(new_meta, new_tuple, rid1);

  // Update version chain
  bustub->txn_manager_->UpdateUndoLink(rid1, undo_link);
  // TODO 如何防止读写锁死锁等待
  // TxnMgrDbg("after_update_before_abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());
  //  Now abort txn3
  bustub->txn_manager_->Abort(txn3);
  ASSERT_EQ(txn3->GetTransactionState(), TransactionState::ABORTED);
  TxnMgrDbg("after_abort", bustub->txn_manager_.get(), table_info, table_info->table_.get());
}

TEST(TxnAbortGCTest, TransactionAbortTest) {  // NOLINT
  auto bustub = std::make_unique<BustubInstance>();
  auto schema = ParseCreateStatement("a integer,b integer,c integer");
  auto table_info = bustub->catalog_->CreateTable(nullptr, "abort_test_table", *schema);

  // Set the catalog for transaction manager
  bustub->txn_manager_->SetCatalog(bustub->catalog_.get());

  // Initialize 3 transactions, txn1 and txn2 will be committed, txn3 will be aborted
  auto txn1 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn1->GetReadTs(), 0);

  // Insert initial tuple with txn1
  auto rid1 = *table_info->table_->InsertTuple(TupleMeta{txn1->GetTransactionTempTs(), false},
                                               Tuple{{Int(10), Int(20), Int(30)}, schema.get()});
  txn1->AppendWriteSet(table_info->oid_, rid1);
  bustub->txn_manager_->Commit(txn1);
  ASSERT_EQ(txn1->GetCommitTs(), 1);

  // After txn1 commits, start txn3
  auto txn3 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn3->GetReadTs(), 1);

  // txn3 updates the tuple
  auto tuple_meta = table_info->table_->GetTupleMeta(rid1);
  ASSERT_EQ(tuple_meta.ts_, 1);  // Ensure the tuple has txn1's commit timestamp

  // Create an undo log for txn3's update
  auto [old_meta, old_tuple] = table_info->table_->GetTuple(rid1);
  auto undo_log = UndoLog{.is_deleted_ = false,
                          .modified_fields_ = {true, true, false},  // Modify only the first two fields
                          .tuple_ = old_tuple,
                          .ts_ = old_meta.ts_,
                          .prev_version_ = UndoLink{INVALID_TXN_ID, 0}};

  auto undo_link = txn3->AppendUndoLog(undo_log);
  txn3->RecordRidUndoLogIndex(rid1, 0);
  txn3->AppendWriteSet(table_info->oid_, rid1);

  // Update the tuple with new values
  TupleMeta new_meta = TupleMeta{.ts_ = txn3->GetTransactionTempTs(), .is_deleted_ = false};
  Tuple new_tuple{{Int(11), Int(22), Int(30)}, schema.get()};
  table_info->table_->UpdateTupleInPlace(new_meta, new_tuple, rid1);

  // Update version chain
  bustub->txn_manager_->UpdateUndoLink(rid1, undo_link);

  // Start txn2 which will update the same tuple after txn3
  auto txn2 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn2->GetReadTs(), 1);

  // txn2 should see the original values since txn3 hasn't committed yet
  auto [meta2, tuple2] = table_info->table_->GetTuple(rid1);
  std::vector<UndoLog> undo_logs2 = CollectUndoLogs(txn2, rid1, bustub->txn_manager_.get());
  auto reconstructed_tuple2 = ReconstructTuple(schema.get(), tuple2, meta2, undo_logs2);
  ASSERT_TRUE(reconstructed_tuple2.has_value());
  ASSERT_EQ(reconstructed_tuple2->GetValue(schema.get(), 0).GetAs<int32_t>(), 10);
  ASSERT_EQ(reconstructed_tuple2->GetValue(schema.get(), 1).GetAs<int32_t>(), 20);

  // Now abort txn3
  bustub->txn_manager_->Abort(txn3);
  ASSERT_EQ(txn3->GetTransactionState(), TransactionState::ABORTED);

  // Verify that the tuple is restored to its original values
  auto [meta_after_abort, tuple_after_abort] = table_info->table_->GetTuple(rid1);
  ASSERT_EQ(meta_after_abort.ts_, 1);  // Should be restored to txn1's commit ts
  ASSERT_EQ(tuple_after_abort.GetValue(schema.get(), 0).GetAs<int32_t>(), 10);
  ASSERT_EQ(tuple_after_abort.GetValue(schema.get(), 1).GetAs<int32_t>(), 20);

  // Now txn2 updates the tuple
  auto [old_meta2, old_tuple2] = table_info->table_->GetTuple(rid1);
  auto undo_log2 = UndoLog{.is_deleted_ = false,
                           .modified_fields_ = {true, false, true},  // Modify first and third fields
                           .tuple_ = old_tuple2,
                           .ts_ = old_meta2.ts_,
                           .prev_version_ = UndoLink{INVALID_TXN_ID, 0}};

  auto undo_link2 = txn2->AppendUndoLog(undo_log2);
  txn2->RecordRidUndoLogIndex(rid1, 0);
  txn2->AppendWriteSet(table_info->oid_, rid1);

  // Update the tuple with new values
  TupleMeta new_meta2 = TupleMeta{.ts_ = txn2->GetTransactionTempTs(), .is_deleted_ = false};
  Tuple new_tuple2{{Int(15), Int(20), Int(35)}, schema.get()};
  table_info->table_->UpdateTupleInPlace(new_meta2, new_tuple2, rid1);

  // Update version chain
  bustub->txn_manager_->UpdateUndoLink(rid1, undo_link2);

  // Commit txn2
  bustub->txn_manager_->Commit(txn2);
  ASSERT_EQ(txn2->GetCommitTs(), 2);

  // Start a new transaction txn4 to read the tuple
  auto txn4 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn4->GetReadTs(), 2);

  // txn4 should see txn2's committed values
  auto [meta4, tuple4] = table_info->table_->GetTuple(rid1);
  std::vector<UndoLog> undo_logs4 = CollectUndoLogs(txn4, rid1, bustub->txn_manager_.get());
  auto reconstructed_tuple4 = ReconstructTuple(schema.get(), tuple4, meta4, undo_logs4);
  ASSERT_TRUE(reconstructed_tuple4.has_value());
  ASSERT_EQ(reconstructed_tuple4->GetValue(schema.get(), 0).GetAs<int32_t>(), 15);
  ASSERT_EQ(reconstructed_tuple4->GetValue(schema.get(), 1).GetAs<int32_t>(), 20);
  ASSERT_EQ(reconstructed_tuple4->GetValue(schema.get(), 2).GetAs<int32_t>(), 35);

  // Debug output before garbage collection
  fmt::println(stderr, "Version chain before GC:");
  TxnMgrDbg("before_gc", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Run garbage collection - this should reclaim txn1's transaction object
  // as txn1 is older than the watermark (which is txn4's read_ts = 2)
  bustub->txn_manager_->GarbageCollection();

  // Debug output after garbage collection
  fmt::println(stderr, "Version chain after GC:");
  TxnMgrDbg("after_gc", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Clean up
  bustub->txn_manager_->Abort(txn4);
}

// Test for insert-then-abort case
TEST(TxnAbortGCTest, InsertAbortTest) {  // NOLINT
  auto bustub = std::make_unique<BustubInstance>();
  auto schema = ParseCreateStatement("a integer,b integer,c integer");
  auto table_info = bustub->catalog_->CreateTable(nullptr, "abort_insert_table", *schema);

  // Set the catalog for transaction manager
  bustub->txn_manager_->SetCatalog(bustub->catalog_.get());

  // Start a transaction that will insert a tuple and then be aborted
  auto txn1 = bustub->txn_manager_->Begin();

  // Insert a tuple with txn1
  auto rid1 = *table_info->table_->InsertTuple(TupleMeta{txn1->GetTransactionTempTs(), false},
                                               Tuple{{Int(100), Int(200), Int(300)}, schema.get()});
  txn1->AppendWriteSet(table_info->oid_, rid1);

  // Create an empty undo log for the newly inserted tuple (since there's no previous version)
  auto undo_log = UndoLog{.is_deleted_ = false,
                          .modified_fields_ = {true, true, true},
                          .tuple_ = Tuple{{Int(100), Int(200), Int(300)}, schema.get()},
                          .ts_ = 0,  // This is a new tuple, no previous version
                          .prev_version_ = UndoLink{INVALID_TXN_ID, 0}};

  auto undo_link = txn1->AppendUndoLog(undo_log);
  txn1->RecordRidUndoLogIndex(rid1, 0);

  // Update version chain
  bustub->txn_manager_->UpdateUndoLink(rid1, undo_link);

  // Debug output before abort
  fmt::println(stderr, "Before aborting insert:");
  TxnMgrDbg("before_abort_insert", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Abort txn1
  bustub->txn_manager_->Abort(txn1);
  ASSERT_EQ(txn1->GetTransactionState(), TransactionState::ABORTED);

  // Debug output after abort
  fmt::println(stderr, "After aborting insert:");
  TxnMgrDbg("after_abort_insert", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Start a new transaction to verify the tuple is deleted/invisible
  auto txn2 = bustub->txn_manager_->Begin();

  // Check that the tuple appears deleted
  auto tuple_meta = table_info->table_->GetTupleMeta(rid1);
  ASSERT_TRUE(tuple_meta.is_deleted_);
  ASSERT_EQ(tuple_meta.ts_, 0);  // Timestamp should be 0 for the aborted insert

  // Attempt to read the aborted tuple - should not be visible
  auto [meta2, tuple2] = table_info->table_->GetTuple(rid1);
  std::vector<UndoLog> undo_logs2 = CollectUndoLogs(txn2, rid1, bustub->txn_manager_.get());
  auto reconstructed_tuple2 = ReconstructTuple(schema.get(), tuple2, meta2, undo_logs2);
  ASSERT_FALSE(reconstructed_tuple2.has_value());  // Tuple should not be visible

  // Clean up
  bustub->txn_manager_->Abort(txn2);
}

}  // namespace bustub
