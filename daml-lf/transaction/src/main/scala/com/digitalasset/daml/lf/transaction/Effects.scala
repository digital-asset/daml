// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.transaction

import com.daml.lf.value.Value.ContractId

/** The effects of the transaction, that is what contracts
  * were consumed and created, and what contract keys were updated.
  */
/** @param consumedContracts
  *     The contracts consumed by this transaction.
  *     When committing the transaction these contracts must be marked consumed.
  *     A contract should be marked consumed when the transaction is committed,
  *     regardless of the ledger effective time of the transaction (e.g. a transaction
  *     with an earlier ledger effective time that gets committed later would find the
  *     contract inactive).
  * @param createdContracts
  *     The contracts created by this transaction.
  *     When the transaction is committed, keys marking the activeness of these
  *     contracts should be created. The key should be a combination of the transaction
  *     id and the relative contract id (that is, the node index).
  * @param updatedContractKeys
  *     The contract keys created or updated as part of the transaction.
  */
final case class Effects(
    consumedContracts: List[ContractId],
    createdContracts: List[(ContractId, Node.NodeCreate[ContractId])],
    updatedContractKeys: Map[GlobalKey, Option[ContractId]],
)

/** utilities to compute the effects of a DAML transaction */
object Effects {

  val Empty = Effects(List.empty, List.empty, Map.empty)

}
