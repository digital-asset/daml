// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref

// Convert a SubmittedTransaction to CommittedTransaction
abstract class TransactionCommitter {
  def commitTransaction(
      transactionId: Ref.LedgerString,
      transaction: SubmittedTransaction,
  ): CommittedTransaction
}

// Standard committer using Contract ID V1
object StandardTransactionCommitter extends TransactionCommitter {
  override def commitTransaction(
      transactionId: Ref.LedgerString,
      transaction: SubmittedTransaction,
  ): CommittedTransaction =
    Transaction.commitTransaction(transaction)
}
