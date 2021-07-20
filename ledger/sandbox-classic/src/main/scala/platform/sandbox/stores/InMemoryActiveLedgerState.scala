// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.sandbox.stores

import java.time.Instant

import com.daml.ledger.api.domain.{PartyDetails, RejectionReason}
import com.daml.lf.data.Ref
import com.daml.lf.data.Relation.Relation
import com.daml.lf.transaction.{CommittedTransaction, GlobalKey, NodeId}
import com.daml.lf.value.Value.ContractId
import com.daml.platform.store.Contract.{ActiveContract, DivulgedContract}
import com.daml.platform.store._
import scalaz.syntax.std.map._

private[sandbox] case class InMemoryActiveLedgerState(
    activeContracts: Map[ContractId, ActiveContract],
    divulgedContracts: Map[ContractId, DivulgedContract],
    keys: Map[GlobalKey, ContractId],
    reverseKeys: Map[ContractId, GlobalKey],
    parties: Map[Ref.Party, PartyDetails],
) extends ActiveLedgerState[InMemoryActiveLedgerState] {

  def isVisibleForDivulgees(contractId: ContractId, forParties: Set[Ref.Party]): Boolean =
    activeContracts
      .get(contractId)
      .exists(ac => forParties.exists(p => ac.witnesses.contains(p) || ac.divulgences.contains(p)))

  def isVisibleForStakeholders(contractId: ContractId, forParties: Set[Ref.Party]): Boolean =
    activeContracts
      .get(contractId)
      .exists(ac =>
        ac.signatories.exists(forParties.contains) || ac.observers.exists(forParties.contains)
      )

  override def lookupContractByKey(key: GlobalKey): Option[ContractId] =
    keys.get(key)

  def lookupContract(cid: ContractId): Option[Contract] =
    activeContracts.get(cid).orElse[Contract](divulgedContracts.get(cid))

  override def lookupContractLet(cid: ContractId): Option[LetLookup] =
    activeContracts
      .get(cid)
      .map(c => Let(c.let))
      .orElse[LetLookup](divulgedContracts.get(cid).map(_ => LetUnknown))

  /** Updates divulgence information on the given active contract with information
    * from the already existing divulged contract.
    */
  private def copyDivulgences(ac: ActiveContract, dc: DivulgedContract): ActiveContract =
    ac.copy(divulgences = ac.divulgences.unionWith(dc.divulgences)((l, _) => l))

  override def addContract(
      c: ActiveContract,
      keyO: Option[GlobalKey],
  ): InMemoryActiveLedgerState = {
    val (newKeys, newReverseKeys) = keyO match {
      case None => (keys, reverseKeys)
      case Some(key) => (keys + (key -> c.id), reverseKeys + (c.id -> key))
    }
    divulgedContracts.get(c.id) match {
      case None =>
        copy(
          activeContracts = activeContracts + (c.id -> c),
          keys = newKeys,
          reverseKeys = newReverseKeys,
        )
      case Some(dc) =>
        copy(
          activeContracts = activeContracts + (c.id -> copyDivulgences(c, dc)),
          divulgedContracts = divulgedContracts - c.id,
          keys = newKeys,
          reverseKeys = newReverseKeys,
        )
    }
  }

  override def removeContract(cid: ContractId): InMemoryActiveLedgerState = {
    val (newKeys, newReverseKeys) = reverseKeys.get(cid) match {
      case None => (keys, reverseKeys)
      case Some(key) => (keys - key, reverseKeys - cid)
    }
    copy(
      activeContracts = activeContracts - cid,
      divulgedContracts = divulgedContracts - cid,
      keys = newKeys,
      reverseKeys = newReverseKeys,
    )
  }

  override def addParties(newParties: Set[Ref.Party]): InMemoryActiveLedgerState =
    copy(parties = newParties.map(p => p -> PartyDetails(p, None, isLocal = true)).toMap ++ parties)

  override def divulgeAlreadyCommittedContracts(
      transactionId: Ref.TransactionId,
      global: Relation[ContractId, Ref.Party],
      referencedContracts: ActiveLedgerState.ReferencedContracts,
  ): InMemoryActiveLedgerState =
    if (global.nonEmpty) {
      val referencedContractsM = referencedContracts.toMap
      // Note: each entry in `global` can refer to either:
      // - a known active contract, in which case its divulgence info is updated
      // - a previously divulged contract, in which case its divulgence info is updated
      // - an unknown contract, in which case a new divulged contract is created from the corresponding info in `referencedContracts`
      val updatedAcs: Map[ContractId, ActiveContract] =
        activeContracts.intersectWith(global) { (ac, parties) =>
          ac.copy(divulgences = ac.divulgeTo(parties, transactionId))
        }
      val updatedDcs: Map[ContractId, DivulgedContract] =
        divulgedContracts.intersectWith(global) { (dc, parties) =>
          dc.copy(divulgences = dc.divulgeTo(parties, transactionId))
        }
      val newDcs = global.foldLeft(Map.empty[ContractId, DivulgedContract]) {
        case (m, (cid, divulgeTo)) =>
          if (divulgeTo.isEmpty || updatedAcs.contains(cid) || updatedDcs.contains(cid))
            m
          else
            m + (cid -> DivulgedContract(
              id = cid,
              contract = referencedContractsM
                .getOrElse(
                  cid,
                  sys.error(
                    s"Transaction $transactionId says it divulges contract ${cid.coid} to parties ${divulgeTo
                      .mkString(",")}, but that contract does not exist."
                  ),
                ),
              divulgences = Map.empty ++ divulgeTo.map(p => p -> transactionId),
            ))
      }
      copy(
        activeContracts = activeContracts ++ updatedAcs,
        divulgedContracts = divulgedContracts ++ updatedDcs ++ newDcs,
      )
    } else this

  override def cloneState(): InMemoryActiveLedgerState =
    // This is an immutable value so no need to copy anything.
    this

  private val acManager =
    new ActiveLedgerStateManager(this)

  /** adds a transaction to the ActiveContracts, make sure that there are no double spends or
    * timing errors. this check is leveraged to achieve higher concurrency, see LedgerState
    */
  def addTransaction(
      let: Instant,
      transactionId: Ref.TransactionId,
      workflowId: Option[Ref.WorkflowId],
      actAs: List[Ref.Party],
      transaction: CommittedTransaction,
      disclosure: Relation[NodeId, Ref.Party],
      divulgence: Relation[ContractId, Ref.Party],
      referencedContracts: ActiveLedgerState.ReferencedContracts,
  ): Either[Set[RejectionReason], InMemoryActiveLedgerState] =
    acManager.addTransaction(
      let,
      transactionId,
      workflowId,
      actAs,
      transaction,
      disclosure,
      divulgence,
      referencedContracts,
    )

  /** Adds a new party to the list of known parties.
    */
  def addParty(details: PartyDetails): InMemoryActiveLedgerState = {
    assert(!parties.contains(details.party))
    copy(parties = parties + (details.party -> details))
  }
}

private[sandbox] object InMemoryActiveLedgerState {
  def empty: InMemoryActiveLedgerState =
    InMemoryActiveLedgerState(Map.empty, Map.empty, Map.empty, Map.empty, Map.empty)
}
