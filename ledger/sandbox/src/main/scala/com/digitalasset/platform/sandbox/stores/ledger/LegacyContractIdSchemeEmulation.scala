// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.data.Ref
import com.daml.lf.value.Value

object LegacyContractIdSchemeEmulation {

  def translateTransaction(
      transactionId: Ref.LedgerString,
      transaction: Transaction.Transaction,
  ): Transaction.AbsTransaction = {

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
