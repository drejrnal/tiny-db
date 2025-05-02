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

/**
 * Helper function to update a tuple within a transaction
 * @param txn The transaction performing the update
 * @param bustub The database instance
 * @param table_info The table information
 * @param rid The RID of the tuple to update
 * @param schema The schema of the table
 * @param new_values The new values to update the tuple with
 * @param abort_on_failure Whether to abort the transaction on failure
 * @return True if the update succeeded, false otherwise
 */
auto UpdateTupleInTransaction(Transaction *txn, BustubInstance *bustub, const TableInfo *table_info, const RID &rid,
                              const Schema *schema, const std::vector<Value> &new_values,
                              bool abort_on_failure = true) -> bool {
  // Get the current tuple metadata
  auto old_meta = table_info->table_->GetTupleMeta(rid);
  auto [_, old_tuple] = table_info->table_->GetTuple(rid);

  // Check if tuple is being modified by another transaction
  if (old_meta.ts_ != txn->GetTransactionId() && (((old_meta.ts_ & TXN_START_ID) >> 62) != 0)) {
    LOG_INFO("txn%ld: tuple is being modified by another transaction", txn->GetTransactionIdHumanReadable());
    if (abort_on_failure) {
      bustub->txn_manager_->Abort(txn);
    }
    return false;
  }

  // Check if tuple is visible to this transaction
  if (old_meta.ts_ > txn->GetReadTs()) {
    LOG_INFO("txn%ld: Tuple is not visible for this transaction", txn->GetTransactionIdHumanReadable());
    if (abort_on_failure) {
      bustub->txn_manager_->Abort(txn);
    }
    return false;
  }

  // Create new tuple metadata and tuple
  TupleMeta new_meta = TupleMeta{.ts_ = txn->GetTransactionTempTs(), .is_deleted_ = false};
  Tuple new_tuple{new_values, schema};

  // Get the current undo link
  auto prev_undo_link = bustub->txn_manager_->GetUndoLink(rid);

  // Try to update the tuple in place with optimistic concurrency control
  if (table_info->table_->UpdateTupleInPlace(new_meta, new_tuple, rid,
                                             [&old_meta](const TupleMeta &origin_meta, const Tuple &tuple,
                                                         const RID rid) -> bool { return origin_meta == old_meta; })) {
    // Generate and append the undo log
    auto txn_undo_log = GenerateNewUndoLog(txn, old_tuple, new_tuple, false, old_meta.ts_, *schema, prev_undo_link);
    auto undo_link = txn->AppendUndoLog(txn_undo_log);
    txn->RecordRidUndoLogIndex(rid, txn->GetUndoLogNum() - 1);

    // Update the undo link with optimistic concurrency control
    if (bustub->txn_manager_->UpdateUndoLink(rid, undo_link, [&](const std::optional<UndoLink> &undo) -> bool {
          if (undo.has_value() && prev_undo_link.has_value()) {
            return undo.value() == prev_undo_link.value();
          }
          if (!undo.has_value() && !prev_undo_link.has_value()) {
            return true;
          }
          return false;
        })) {
      // Update the write set
      txn->AppendWriteSet(table_info->oid_, rid);
      return true;
    } else {
      LOG_INFO("txn%ld: failed to update undo link", txn->GetTransactionIdHumanReadable());
      if (abort_on_failure) {
        bustub->txn_manager_->Abort(txn);
      }
      return false;
    }
  }

  // Update failed
  LOG_INFO("txn%ld: failed to update tuple", txn->GetTransactionIdHumanReadable());
  if (abort_on_failure) {
    bustub->txn_manager_->Abort(txn);
  }
  return false;
}

TEST(TransactionConcurrentUpdate, ConcurrentCommitAndAbortTest) {
  auto bustub = std::make_unique<BustubInstance>();
  auto schema = ParseCreateStatement("a integer,b integer,c integer");
  auto table_info = bustub->catalog_->CreateTable(nullptr, "abort_test_table", *schema);

  // Set the catalog for transaction manager
  bustub->txn_manager_->SetCatalog(bustub->catalog_.get());

  // Initialize transaction 1 (will be committed)
  auto txn1 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn1->GetReadTs(), 0);

  // Insert initial tuple with txn1
  auto rid1 = *table_info->table_->InsertTuple(TupleMeta{txn1->GetTransactionTempTs(), false},
                                               Tuple{{Int(10), Int(20), Int(30)}, schema.get()});
  txn1->AppendWriteSet(table_info->oid_, rid1);
  bustub->txn_manager_->Commit(txn1);
  ASSERT_EQ(txn1->GetCommitTs(), 1);
  TxnMgrDbg("after_insert from transaction 1", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Create a barrier to synchronize threads
  std::promise<void> txn2_ready;
  std::future<void> txn2_future = txn2_ready.get_future();
  std::promise<void> txn3_ready;
  std::future<void> txn3_future = txn3_ready.get_future();

  // Thread for transaction 2 (will be committed)
  auto txn2_thread = std::thread([&]() {
    // Begin transaction 2
    auto txn2 = bustub->txn_manager_->Begin();
    ASSERT_EQ(txn2->GetReadTs(), 1);

    // Notify that txn2 has started
    txn2_ready.set_value();

    // Wait for txn3 to be ready before proceeding
    txn3_future.wait();

    // Update the tuple with new values
    std::vector<Value> new_values = {Int(15), Int(20), Int(35)};
    if (UpdateTupleInTransaction(txn2, bustub.get(), table_info, rid1, schema.get(), new_values)) {
      // Commit txn2
      bustub->txn_manager_->Commit(txn2);
      ASSERT_EQ(txn2->GetCommitTs(), 2);
    }
  });

  // Thread for transaction 3 (will be aborted)
  auto txn3_thread = std::thread([&]() {
    // Wait for txn2 to be ready before starting txn3
    txn2_future.wait();

    // Begin transaction 3
    auto txn3 = bustub->txn_manager_->Begin();
    ASSERT_EQ(txn3->GetReadTs(), 1);

    // Update the tuple with new values
    std::vector<Value> new_values = {Int(11), Int(20), Int(35)};
    UpdateTupleInTransaction(txn3, bustub.get(), table_info, rid1, schema.get(), new_values, false);

    // Notify that txn3 has updated the tuple
    txn3_ready.set_value();

    // Sleep briefly to ensure txn2 has a chance to read before we abort
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Abort txn3
    bustub->txn_manager_->Abort(txn3);
    ASSERT_EQ(txn3->GetTransactionState(), TransactionState::ABORTED);
  });

  // Wait for both threads to complete
  txn2_thread.join();
  txn3_thread.join();

  TxnMgrDbg("after_concurrent_operations", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Start a new transaction to verify the final state
  auto txn4 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn4->GetReadTs(), 1);

  // txn4 should see txn2's committed values
  auto [meta4, tuple4] = table_info->table_->GetTuple(rid1);
  std::vector<UndoLog> undo_logs4 = CollectUndoLogs(txn4, rid1, bustub->txn_manager_.get());
  auto reconstructed_tuple4 = ReconstructTuple(schema.get(), tuple4, meta4, undo_logs4);
  ASSERT_TRUE(reconstructed_tuple4.has_value());
  ASSERT_EQ(reconstructed_tuple4->GetValue(schema.get(), 0).GetAs<int32_t>(), 10);
  ASSERT_EQ(reconstructed_tuple4->GetValue(schema.get(), 1).GetAs<int32_t>(), 20);
  ASSERT_EQ(reconstructed_tuple4->GetValue(schema.get(), 2).GetAs<int32_t>(), 30);

  // Clean up
  bustub->txn_manager_->Abort(txn4);
}

TEST(TransactionConcurrentUpdate, ConcurrentUpdateCommitTest) {
  auto bustub = std::make_unique<BustubInstance>();
  auto schema = ParseCreateStatement("a integer,b integer,c integer");
  auto table_info = bustub->catalog_->CreateTable(nullptr, "abort_test_table", *schema);

  // Set the catalog for transaction manager
  bustub->txn_manager_->SetCatalog(bustub->catalog_.get());

  // Initialize transaction 1 (will be committed)
  auto txn1 = bustub->txn_manager_->Begin();
  ASSERT_EQ(txn1->GetReadTs(), 0);

  // Insert initial tuple with txn1
  auto rid1 = *table_info->table_->InsertTuple(TupleMeta{txn1->GetTransactionTempTs(), false},
                                               Tuple{{Int(10), Int(20), Int(30)}, schema.get()});
  txn1->AppendWriteSet(table_info->oid_, rid1);
  bustub->txn_manager_->Commit(txn1);
  ASSERT_EQ(txn1->GetCommitTs(), 1);
  TxnMgrDbg("after_insert from transaction 1", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Create a barrier to synchronize threads
  std::promise<void> txn2_ready;
  std::future<void> txn2_future = txn2_ready.get_future();
  std::promise<void> txn3_ready;
  std::future<void> txn3_future = txn3_ready.get_future();

  // Thread for transaction 2 (will be committed)
  auto txn2_thread = std::thread([&]() {
    // Begin transaction 2
    auto txn2 = bustub->txn_manager_->Begin();
    ASSERT_EQ(txn2->GetReadTs(), 1);

    // Wait for txn3 to be ready before proceeding
    txn3_future.wait();

    // Update the tuple with new values
    std::vector<Value> new_values = {Int(15), Int(20), Int(35)};
    if (UpdateTupleInTransaction(txn2, bustub.get(), table_info, rid1, schema.get(), new_values)) {
      // Commit txn2
      bustub->txn_manager_->Commit(txn2);
      ASSERT_EQ(txn2->GetCommitTs(), 2);
    }
    // Notify that txn2 has started
    txn2_ready.set_value();
  });

  // Thread for transaction 3 (will be aborted)
  auto txn3_thread = std::thread([&]() {
    // Begin transaction 3
    auto txn3 = bustub->txn_manager_->Begin();
    ASSERT_EQ(txn3->GetReadTs(), 1);

    // Notify that txn3 has updated the tuple
    txn3_ready.set_value();

    // Wait for txn2 to be ready before starting txn3
    txn2_future.wait();
    // Update the tuple with new values
    std::vector<Value> new_values = {Int(11), Int(22), Int(30)};
    UpdateTupleInTransaction(txn3, bustub.get(), table_info, rid1, schema.get(), new_values, false);

    // Sleep briefly to ensure txn2 has a chance to read before we abort
    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    // Abort txn3
    bustub->txn_manager_->Abort(txn3);
    ASSERT_EQ(txn3->GetTransactionState(), TransactionState::ABORTED);
  });

  // Wait for both threads to complete
  txn2_thread.join();
  txn3_thread.join();

  TxnMgrDbg("after_concurrent_operations", bustub->txn_manager_.get(), table_info, table_info->table_.get());

  // Start a new transaction to verify the final state
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

  // Clean up
  bustub->txn_manager_->Abort(txn4);
}

}  // namespace bustub