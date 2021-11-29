// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref
import com.daml.lf.value.Value

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

// Committer emulating Contract ID legacy scheme
object LegacyTransactionCommitter extends TransactionCommitter {

  def commitTransaction(
      transactionId: Ref.LedgerString,
      transaction: SubmittedTransaction,
  ): CommittedTransaction = {

    val prefix = "#" + transactionId + ":"

    val contractMapping =
      transaction.unversioned
        .localContracts[Value.ContractId]
        .transform { case (_, (nid, _)) =>
          Value.ContractId.V0(Ref.ContractIdString.assertFromString(prefix + nid.index.toString))
        }
        .withDefault(identity)

    CommittedTransaction(transaction.map(_.mapCid(contractMapping)))

  }

}
