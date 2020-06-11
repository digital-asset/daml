// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.ledger.participant.state.v1.{CommittedTransaction, SubmittedTransaction}
import com.daml.lf.data.Ref
import com.daml.lf.value.Value

// Convert a SubmittedTransaction to CommittedTransaction
abstract class TransactionCommitter {
  def commitTransaction(
      transactionId: Ref.LedgerString,
      transaction: SubmittedTransaction
  ): CommittedTransaction
}

// Standard committer using Contract ID V1
object StandardTransactionCommitter extends TransactionCommitter {
  override def commitTransaction(
      transactionId: Ref.LedgerString,
      transaction: SubmittedTransaction
  ): CommittedTransaction =
    Transaction.commitTransaction(transaction)
}

// Committer emulating Contract ID legacy scheme
object LegacyTransactionCommitter extends TransactionCommitter {

  def commitTransaction(
      transactionId: Ref.LedgerString,
      transaction: SubmittedTransaction,
  ): CommittedTransaction = {

    val prefix = "#" + transactionId + ":"

    val contractMapping =
      transaction
        .localContracts[Value.ContractId]
        .transform((_, nid) =>
          Value.ContractId.V0(Ref.ContractIdString.assertFromString(prefix + nid.index.toString)))
        .withDefault(identity)

    Transaction.CommittedTransaction(
      VersionedTransaction.map2(identity[Transaction.NodeId], contractMapping)(transaction)
    )

  }

}
