// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import com.daml.lf.data.Ref
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.ActiveLedgerState._
import com.daml.platform.store.Contract.ActiveContract

private[platform] sealed abstract class LetLookup

/** Contract exists, but contract LET is unknown (e.g., a divulged contract) */
private[platform] case object LetUnknown extends LetLookup

/** Contract exists with the given LET */
private[platform] final case class Let(instant: Instant) extends LetLookup

/** An abstract representation of the active ledger state:
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
private[platform] trait ActiveLedgerState[ALS <: ActiveLedgerState[ALS]] {

  /** Callback to query an active or divulged contract, used for transaction validation
    * Returns:
    * - None if the contract does not exist
    * - Some(LetUnknown) if the contract exists, but its LET is unknown (i.e., a divulged contract)
    * - Some(Let(_)) if the contract exists and its LET is known
    */
  def lookupContractLet(cid: ContractId): Option[LetLookup]

  /** Callback to query a contract by key, used for validating NodeLookupByKey nodes.
    */
  def lookupContractByKey(key: GlobalKey): Option[ContractId]

  /** Called when a new contract is created */
  def addContract(c: ActiveContract, keyO: Option[GlobalKey]): ALS

  /** Called when the given contract is archived */
  def removeContract(cid: ContractId): ALS

  /** Called once for each transaction with the set of parties found in that transaction.
    * As the sandbox has an open world of parties, any party name mentioned in a transaction
    * will implicitly add that name to the list of known parties.
    */
  def addParties(parties: Set[Ref.Party]): ALS

  /** Note that this method is about divulging contracts _that have already been
    * committed_. Implementors of [[ActiveLedgerState]] must take care to also store
    * divulgence information already present in `ActiveContract#divulgences` in the `addContract`
    * method.
    */
  def divulgeAlreadyCommittedContracts(
      transactionId: Ref.TransactionId,
      global: Relation[ContractId, Ref.Party],
      referencedContracts: ReferencedContracts,
  ): ALS

  /** Clone the current active ledger state. The new state starts out
    * being identical to the old one but writes to the cloned
    * state will not affect the original state and the other way around.
    */
  def cloneState(): ALS
}

object ActiveLedgerState {

  type ReferencedContracts =
    List[(Value.ContractId, Value.ContractInst[Value.VersionedValue])]

}
