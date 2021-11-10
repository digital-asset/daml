// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.lf.data.Ref
import com.daml.lf.transaction.Node.VersionedKeyWithMaintainers
import com.daml.lf.transaction.NodeId
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}

/** A contract that is part of the [[ActiveLedgerState]].
  * Depending on where the contract came from, other metadata may be available.
  */
private[platform] sealed abstract class Contract {
  def id: ContractId

  def contract: VersionedContractInstance

  /** For each party, the transaction id at which the contract was divulged */
  def divulgences: Map[Ref.Party, Ref.TransactionId]

  /** Returns the new divulgences after the contract has been divulged to the given parties at the given transaction */
  def divulgeTo(
      parties: Set[Ref.Party],
      transactionId: Ref.TransactionId,
  ): Map[Ref.Party, Ref.TransactionId] =
    parties.foldLeft(divulgences)((m, e) => if (m.contains(e)) m else m + (e -> transactionId))
}

private[platform] object Contract {

  /** For divulged contracts, we only know their contract argument, but no other metadata.
    * Note also that a ledger node may not be notified when a divulged contract gets archived.
    *
    * These contracts are only used for transaction validation, they are not part of the active contract set.
    */
  final case class DivulgedContract(
      id: Value.ContractId,
      contract: VersionedContractInstance,
      divulgences: Map[Ref.Party, Ref.TransactionId],
  ) extends Contract

  /** For active contracts, we know all metadata.
    */
  final case class ActiveContract(
      id: Value.ContractId,
      let: Instant, // time when the contract was committed
      transactionId: Ref.TransactionId, // transaction id where the contract originates
      nodeId: NodeId,
      workflowId: Option[Ref.WorkflowId], // workflow id from where the contract originates
      contract: VersionedContractInstance,
      witnesses: Set[Ref.Party],
      divulgences: Map[
        Ref.Party,
        Ref.TransactionId,
      ], // for each party, the transaction id at which the contract was divulged
      key: Option[VersionedKeyWithMaintainers],
      signatories: Set[Ref.Party],
      observers: Set[Ref.Party],
      agreementText: String,
  ) extends Contract

}
