// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant

import com.daml.ledger.participant.state.v1.AbsoluteContractInst
import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.WorkflowId
import com.digitalasset.ledger.api.domain.PartyDetails
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState._
import com.digitalasset.platform.sandbox.stores.ledger.SequencingError
import scalaz.syntax.std.map._

case class InMemoryActiveLedgerState(
    activeContracts: Map[AbsoluteContractId, ActiveContract],
    divulgedContracts: Map[AbsoluteContractId, DivulgedContract],
    keys: Map[GlobalKey, AbsoluteContractId],
    parties: Map[Party, PartyDetails])
    extends ActiveLedgerState[InMemoryActiveLedgerState] {

  override def lookupActiveContract(cid: AbsoluteContractId): Option[ActiveContract] =
    activeContracts.get(cid)

  override def lookupContract(cid: AbsoluteContractId): Option[Contract] =
    activeContracts.get(cid).orElse[Contract](divulgedContracts.get(cid))

  override def keyExists(key: GlobalKey) = keys.contains(key)

  /**
    * Updates divulgence information on the given active contract with information
    * from the already existing divulged contract.
    */
  private def copyDivulgences(ac: ActiveContract, dc: DivulgedContract): ActiveContract =
    ac.copy(divulgences = ac.divulgences.unionWith(dc.divulgences)((l, _) => l))

  override def addContract(
      c: ActiveContract,
      keyO: Option[GlobalKey]): InMemoryActiveLedgerState = {
    val newKeys = keyO match {
      case None => keys
      case Some(key) => keys + (key -> c.id)
    }
    divulgedContracts.get(c.id) match {
      case None =>
        copy(
          activeContracts = activeContracts + (c.id -> c),
          keys = newKeys
        )
      case Some(dc) =>
        copy(
          activeContracts = activeContracts + (c.id -> copyDivulgences(c, dc)),
          divulgedContracts = divulgedContracts - c.id,
          keys = newKeys
        )
    }
  }

  override def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]) = {
    val newKeys = keyO match {
      case None => keys
      case Some(key) => keys - key
    }
    copy(
      activeContracts = activeContracts - cid,
      divulgedContracts = divulgedContracts - cid,
      keys = newKeys
    )
  }

  override def addParties(newParties: Set[Party]): InMemoryActiveLedgerState =
    copy(parties = newParties.map(p => p -> PartyDetails(p, None, true)).toMap ++ parties)

  override def divulgeAlreadyCommittedContract(
      transactionId: TransactionIdString,
      global: Relation[AbsoluteContractId, Party],
      referencedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)])
    : InMemoryActiveLedgerState =
    if (global.nonEmpty) {
      val referencedContractsM = referencedContracts.toMap
      // Note: each entry in `global` can refer to either:
      // - a known active contract, in which case its divulgence info is updated
      // - a divulged contract, in which case its divulgence info is updated
      // - an unknown contract, in which case a new divulged contract is stored
      val updatedAcs = activeContracts.intersectWith(global) { (ac, parties) =>
        ac copy (divulgences = ac.divulgeTo(parties, transactionId))
      }
      val updatedDcs = divulgedContracts.intersectWith(global) { (dc, parties) =>
        dc copy (divulgences = dc.divulgeTo(parties, transactionId))
      }
      val newDcs = global.foldLeft(Map.empty[AbsoluteContractId, DivulgedContract]){
        case (m, (cid, divulgeTo)) =>
          if (updatedAcs.contains(cid) || updatedDcs.contains(cid))
            m
          else
            m + (cid -> DivulgedContract(
              id = cid,
              contract = referencedContractsM
                .getOrElse(cid, sys.error(
                  s"Transaction $transactionId says it divulges contract ${cid.coid} to parties ${divulgeTo.mkString(",")}, but that contract does not exist.")),
              divulgences = Map.empty ++ divulgeTo.map(p => p -> transactionId)
            ))}
      copy(
        activeContracts = activeContracts ++ updatedAcs,
        divulgedContracts = divulgedContracts ++ updatedDcs ++ newDcs
      )
    } else this

  private val acManager =
    new ActiveLedgerStateManager(this)

  /** adds a transaction to the ActiveContracts, make sure that there are no double spends or
    * timing errors. this check is leveraged to achieve higher concurrency, see LedgerState
    */
  def addTransaction[Nid](
      let: Instant,
      transactionId: TransactionIdString,
      workflowId: Option[WorkflowId],
      transaction: GenTransaction.WithTxValue[Nid, AbsoluteContractId],
      explicitDisclosure: Relation[Nid, Party],
      localImplicitDisclosure: Relation[Nid, Party],
      globalImplicitDisclosure: Relation[AbsoluteContractId, Party],
      referencedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)]
  ): Either[Set[SequencingError], InMemoryActiveLedgerState] =
    acManager.addTransaction(
      let,
      transactionId,
      workflowId,
      transaction,
      explicitDisclosure,
      localImplicitDisclosure,
      globalImplicitDisclosure,
      referencedContracts)

  /**
    * Adds a new party to the list of known parties.
    */
  def addParty(details: PartyDetails): InMemoryActiveLedgerState = {
    assert(!parties.contains(details.party))
    copy(parties = parties + (details.party -> details))
  }
}

object InMemoryActiveLedgerState {
  def empty: InMemoryActiveLedgerState =
    InMemoryActiveLedgerState(Map.empty, Map.empty, Map.empty, Map.empty)
}
