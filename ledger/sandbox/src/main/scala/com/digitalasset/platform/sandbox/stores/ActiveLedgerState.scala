// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node.{GlobalKey, KeyWithMaintainers}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.ledger.WorkflowId
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState._

/**
  * An abstract representation of the active ledger state:
  * - Active contracts
  * - Divulged contracts
  * - Contract keys
  * - Known parties
  *
  * The active ledger state is used for validating transactions,
  * see [[ActiveLedgerStateManager]].
  *
  * The active ledger state could be derived from the transaction stream,
  * we keep track of it explicitly for performance reasons.
  */
trait ActiveLedgerState[+Self] { this: ActiveLedgerState[Self] =>

  /** Callback to query a contract, used for transaction validation */
  def lookupContract(cid: AbsoluteContractId): Option[Contract]

  /** Callback to query a contract key, used for transaction validation */
  def keyExists(key: GlobalKey): Boolean

  /** Called when a new contract is created */
  def addContract(c: ActiveContract, keyO: Option[GlobalKey]): Self

  /** Called when the given contract is archived */
  def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]): Self

  /** Called once for each transaction with the set of parties found in that transaction.
    * As the sandbox has an open world of parties, any party name mentioned in a transaction
    * will implicitly add that name to the list of known parties.
    */
  def addParties(parties: Set[Party]): Self

  /** Note that this method is about disclosing contracts _that have already been
    * committed_. Implementors of [[ActiveLedgerState]] must take care to also store
    * divulgence information already present in `ActiveContract#divulgences` in the `addContract`
    * method.
    */
  def divulgeAlreadyCommittedContract(
      transactionId: TransactionIdString,
      global: Relation[AbsoluteContractId, Party]): Self
}

object ActiveLedgerState {

  /** A contract that is part of the [[ActiveLedgerState]].
    * Depending on where the contract came from, other metadata may be available.
    */
  sealed abstract class Contract {
    def id: Value.AbsoluteContractId
    def contract: ContractInst[VersionedValue[AbsoluteContractId]]
  }

  /**
    * For divulged contracts, we only their contract argument, but no other metadata.
    * Note also that a ledger node may not be notified when a divulged contract gets archived.
    *
    * These contracts are only used for transaction validation, they are not part of the active contract set.
    */
  final case class DivulgedContract(
      id: Value.AbsoluteContractId,
      contract: ContractInst[VersionedValue[AbsoluteContractId]],
      /** For each party, the transaction id at which the contract was divulged */
      divulgences: Map[Party, TransactionIdString],
  ) extends Contract

  /**
    * For active contracts, we know all metadata.
    */
  final case class ActiveContract(
      id: Value.AbsoluteContractId,
      let: Instant, // time when the contract was committed
      transactionId: TransactionIdString, // transaction id where the contract originates
      workflowId: Option[WorkflowId], // workflow id from where the contract originates
      contract: ContractInst[VersionedValue[AbsoluteContractId]],
      witnesses: Set[Party],
      divulgences: Map[Party, TransactionIdString], // for each party, the transaction id at which the contract was divulged
      key: Option[KeyWithMaintainers[VersionedValue[AbsoluteContractId]]],
      signatories: Set[Party],
      observers: Set[Party],
      agreementText: String)
      extends Contract

}
