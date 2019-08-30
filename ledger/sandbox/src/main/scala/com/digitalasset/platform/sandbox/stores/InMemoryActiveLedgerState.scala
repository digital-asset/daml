// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores

import java.time.Instant

import com.digitalasset.daml.lf.data.Ref.{Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.transaction.GenTransaction
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.ledger.WorkflowId
import com.digitalasset.ledger.api.domain.PartyDetails
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState._
import com.digitalasset.platform.sandbox.stores.ledger.SequencingError
import scalaz.syntax.std.map._

case class InMemoryActiveLedgerState(
    contracts: Map[AbsoluteContractId, ActiveContract],
    keys: Map[GlobalKey, AbsoluteContractId],
    parties: Map[Party, PartyDetails])
    extends ActiveLedgerState[InMemoryActiveLedgerState] {

  override def lookupContract(cid: AbsoluteContractId) = contracts.get(cid)

  override def keyExists(key: GlobalKey) = keys.contains(key)

  override def addContract(c: ActiveContract, keyO: Option[GlobalKey]): InMemoryActiveLedgerState =
    keyO match {
      case None => copy(contracts = contracts + (c.id -> c))
      case Some(key) =>
        copy(contracts = contracts + (c.id -> c), keys = keys + (key -> c.id))
    }

  override def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]) = keyO match {
    case None => copy(contracts = contracts - cid)
    case Some(key) => copy(contracts = contracts - cid, keys = keys - key)
  }

  override def addParties(newParties: Set[Party]): InMemoryActiveLedgerState =
    copy(parties = newParties.map(p => p -> PartyDetails(p, None, true)).toMap ++ parties)

  override def divulgeAlreadyCommittedContract(
      transactionId: TransactionIdString,
      global: Relation[AbsoluteContractId, Party]): InMemoryActiveLedgerState =
    if (global.nonEmpty)
      copy(
        contracts = contracts ++
          contracts.intersectWith(global) { (ac, parties) =>
            ac copy (divulgences = parties.foldLeft(ac.divulgences)((m, e) =>
              if (m.contains(e)) m else m + (e -> transactionId)))
          })
    else this

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
      globalImplicitDisclosure: Relation[AbsoluteContractId, Party]
  ): Either[Set[SequencingError], InMemoryActiveLedgerState] =
    acManager.addTransaction(
      let,
      transactionId,
      workflowId,
      transaction,
      explicitDisclosure,
      localImplicitDisclosure,
      globalImplicitDisclosure)

  /**
    * Adds a new party to the list of known parties.
    */
  def addParty(details: PartyDetails): InMemoryActiveLedgerState = {
    assert(!parties.contains(details.party))
    copy(parties = parties + (details.party -> details))
  }
}

object InMemoryActiveLedgerState {
  def empty: InMemoryActiveLedgerState = InMemoryActiveLedgerState(Map(), Map(), Map.empty)
}
