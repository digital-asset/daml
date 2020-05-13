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
    transaction.assertNoRelCid(_ => "Unexpected relative contract ID")
}

// Committer emulating Contract ID legacy scheme
object LegacyTransactionCommitter extends TransactionCommitter {

  def commitTransaction(
      transactionId: Ref.LedgerString,
      transaction: SubmittedTransaction,
  ): CommittedTransaction = {

    val prefix = "#" + transactionId + ":"

    type AbsCoid = Value.AbsoluteContractId
    val contractMap: AbsCoid => AbsCoid =
      transaction.localContracts
        .collect[(AbsCoid, AbsCoid), Map[AbsCoid, AbsCoid]] {
          case (acoid: AbsCoid, nid) =>
            acoid ->
              Value.AbsoluteContractId.V0(
                Ref.ContractIdString.assertFromString(prefix + nid.index.toString))
        }
        .withDefault(identity)

    val contractMapping: Transaction.TContractId => Value.AbsoluteContractId = {
      case acoid: Value.AbsoluteContractId =>
        contractMap(acoid)
      case _: Value.RelativeContractId =>
        throw new RuntimeException("Unexpected relative contract ID")
    }

    GenTransaction.map3(
      identity[Transaction.NodeId],
      contractMapping,
      Value.VersionedValue.map1(contractMapping)
    )(transaction)

  }

}
