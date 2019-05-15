// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.LedgerId
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.Node.KeyWithMaintainers
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst, VersionedValue}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.sandbox.metrics.MetricsManager
import com.digitalasset.platform.sandbox.stores.ActiveContracts.ActiveContract
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry

import scala.collection.immutable
import scala.concurrent.Future

final case class Contract(
    contractId: AbsoluteContractId,
    let: Instant,
    transactionId: String,
    workflowId: String,
    witnesses: Set[Ref.Party],
    divulgences: Map[Ref.Party, String],
    coinst: ContractInst[VersionedValue[AbsoluteContractId]],
    key: Option[KeyWithMaintainers[VersionedValue[AbsoluteContractId]]]) {
  def toActiveContract: ActiveContract =
    ActiveContract(let, transactionId, workflowId, coinst, witnesses, divulgences, key)
}

object Contract {
  def fromActiveContract(cid: AbsoluteContractId, ac: ActiveContract): Contract =
    Contract(
      cid,
      ac.let,
      ac.transactionId,
      ac.workflowId,
      ac.witnesses,
      ac.divulgences,
      ac.contract,
      ac.key)
}

/**
  * Every time the ledger persists a transactions, the active contract set (ACS) is updated.
  * Updating the ACS requires knowledge of blinding info, which is not included in LedgerEntry.Transaction.
  * The SqlLedger persistence queue Transaction elements are therefore enriched with blinding info.
  */
sealed abstract class PersistenceEntry extends Product with Serializable

object PersistenceEntry {
  final case class Rejection(entry: LedgerEntry.Rejection) extends PersistenceEntry
  final case class Transaction(
      entry: LedgerEntry.Transaction,
      localImplicitDisclosure: Relation[LedgerEntry.EventId, Ref.Party],
      globalImplicitDisclosure: Relation[AbsoluteContractId, Ref.Party]
  ) extends PersistenceEntry
  final case class Checkpoint(entry: LedgerEntry.Checkpoint) extends PersistenceEntry
}

sealed abstract class PersistenceResponse extends Product with Serializable

object PersistenceResponse {

  case object Ok extends PersistenceResponse

  case object Duplicate extends PersistenceResponse

}

case class LedgerSnapshot(offset: Long, acs: Source[Contract, NotUsed])

trait LedgerDao extends AutoCloseable {

  type LedgerOffset = Long

  /** Looks up the ledger id */
  def lookupLedgerId(): Future[Option[LedgerId]]

  /** Looks up the current ledger end */
  def lookupLedgerEnd(): Future[LedgerOffset]

  /** Looks up an active contract. Archived contracts must not be returned by this method */
  def lookupActiveContract(contractId: AbsoluteContractId): Future[Option[Contract]]

  /**
    * Looks up a LedgerEntry at a given offset
    *
    * @param offset the offset to look at
    * @return the optional LedgerEntry found
    */
  def lookupLedgerEntry(offset: LedgerOffset): Future[Option[LedgerEntry]]

  /**
    * Looks up a LedgerEntry at a given offset
    *
    * @param offset the offset to look at
    * @return the LedgerEntry found, or throws an exception
    */
  def lookupLedgerEntryAssert(offset: LedgerOffset): Future[LedgerEntry] = {
    lookupLedgerEntry(offset).map(
      _.getOrElse(sys.error(s"ledger entry not found for offset: $offset")))(DirectExecutionContext)
  }

  /**
    * Looks up a Contract given a contract key
    *
    * @param key the contract key to query
    * @return the optional AbsoluteContractId
    */
  def lookupKey(key: Node.GlobalKey): Future[Option[AbsoluteContractId]]

  /**
    * Returns a stream of ledger entries
    *
    * @param startInclusive starting offset inclusive
    * @param endExclusive   ending offset exclusive
    * @return a stream of ledger entries tupled with their offset
    */
  def getLedgerEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, LedgerEntry), NotUsed]

  /**
    * Returns a snapshot of the ledger.
    * The snapshot consists of an offset, and a stream of contracts that were active at that offset.
    *
    * @param mat the Akka stream materializer to be used for the contract stream.
    */
  def getActiveContractSnapshot()(implicit mat: Materializer): Future[LedgerSnapshot]

  /**
    * Initializes the ledger. Must be called only once.
    *
    * @param ledgerId  the ledger id to be stored
    * @param ledgerEnd the ledger end to be stored
    */
  def initializeLedger(ledgerId: String, ledgerEnd: LedgerOffset): Future[Unit]

  /**
    * Stores a ledger entry. The ledger end gets updated as well in the same transaction.
    * WARNING: this code cannot be run concurrently on subsequent entry persistence operations!
    *
    * @param offset       the offset to store the ledger entry
    * @param newLedgerEnd the new ledger end, valid after this operation finishes
    * @param ledgerEntry  the LedgerEntry to be stored
    * @return Ok when the operation was successful otherwise a Duplicate
    */
  def storeLedgerEntry(
      offset: LedgerOffset,
      newLedgerEnd: LedgerOffset,
      ledgerEntry: PersistenceEntry): Future[PersistenceResponse]

  /**
    * Stores the initial ledger state, e.g., computed by the scenario loader.
    * Must be called at most once, before any call to storeLedgerEntry.
    *
    * @param activeContracts the active contract set
    * @param ledgerEntries the list of LedgerEntries to save
    * @return Ok when the operation was successful
    */
  def storeInitialState(
      activeContracts: immutable.Seq[Contract],
      ledgerEntries: immutable.Seq[(LedgerOffset, LedgerEntry)],
      newLedgerEnd: LedgerOffset
  ): Future[Unit]

  /** Resets the platform into a state as it was never used before. Meant to be used solely for testing. */
  def reset(): Future[Unit]

}

object LedgerDao {

  /** Wraps the given LedgerDao adding metrics around important calls */
  def metered(dao: LedgerDao)(implicit mm: MetricsManager): LedgerDao = MeteredLedgerDao(dao)
}
