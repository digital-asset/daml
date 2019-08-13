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
import com.digitalasset.platform.sandbox.stores.ActiveContracts._
import com.digitalasset.platform.sandbox.stores.ledger.SequencingError
import scalaz.syntax.std.map._

case class InMemoryActiveContracts(
    contracts: Map[AbsoluteContractId, ActiveContract],
    keys: Map[GlobalKey, AbsoluteContractId],
    parties: Map[Party, PartyDetails])
    extends ActiveContracts[InMemoryActiveContracts] {

  override def lookupContract(cid: AbsoluteContractId) = contracts.get(cid)

  override def keyExists(key: GlobalKey) = keys.contains(key)

  override def addContract(cid: AbsoluteContractId, c: ActiveContract, keyO: Option[GlobalKey]) =
    keyO match {
      case None => copy(contracts = contracts + (cid -> c))
      case Some(key) =>
        copy(contracts = contracts + (cid -> c), keys = keys + (key -> cid))
    }

  override def removeContract(cid: AbsoluteContractId, keyO: Option[GlobalKey]) = keyO match {
    case None => copy(contracts = contracts - cid)
    case Some(key) => copy(contracts = contracts - cid, keys = keys - key)
  }

  override def addParties(newParties: Set[Party]): InMemoryActiveContracts =
    copy(parties = newParties.map(p => p -> PartyDetails(p, None, true)).toMap ++ parties)

  override def divulgeAlreadyCommittedContract(
      transactionId: TransactionIdString,
      global: Relation[AbsoluteContractId, Party]): InMemoryActiveContracts =
    if (global.nonEmpty)
      copy(
        contracts = contracts ++
          contracts.intersectWith(global) { (ac, parties) =>
            ac copy (divulgences = parties.foldLeft(ac.divulgences)((m, e) =>
              if (m.contains(e)) m else m + (e -> transactionId)))
          })
    else this

  private val acManager =
    new ActiveContractsManager(this)

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
  ): Either[Set[SequencingError], InMemoryActiveContracts] =
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
  def addParty(details: PartyDetails): InMemoryActiveContracts = {
    assert(!parties.contains(details.party))
    copy(parties = parties + (details.party -> details))
  }
}

object InMemoryActiveContracts {
  def empty: InMemoryActiveContracts = InMemoryActiveContracts(Map(), Map(), Map.empty)
}
