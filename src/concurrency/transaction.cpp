//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction.cpp
//
// Identification: src/concurrency/transaction.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction.h"

namespace bustub {

void Transaction::SetTainted() {
  // Set the transaction state to TAINTED, which indicates a conflict has been detected
  // but we still allow the transaction to continue (unlike ABORTED)
  state_ = TransactionState::TAINTED;
}

}  // namespace bustub
