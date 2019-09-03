// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.{AbsoluteContractInst, TransactionId}
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TransactionId}
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.digitalasset.daml.lf.data.Relation.Relation
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger._
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.platform.common.util.DirectExecutionContext
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.{ActiveContract, Contract}
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ActiveContracts.ActiveContract
import com.digitalasset.platform.sandbox.stores.ledger.{ConfigurationEntry, LedgerEntry}
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry.Transaction

import scala.collection.immutable
import scala.concurrent.Future

/**
  * Every time the ledger persists a transactions, the active contract set (ACS) is updated.
  * Updating the ACS requires knowledge of blinding info, which is not included in LedgerEntry.Transaction.
  * The SqlLedger persistence queue Transaction elements are therefore enriched with blinding info.
  */
sealed abstract class PersistenceEntry extends Product with Serializable {
  def entry: LedgerEntry
}

object PersistenceEntry {
  final case class Rejection(entry: LedgerEntry.Rejection) extends PersistenceEntry
  final case class Transaction(
      entry: LedgerEntry.Transaction,
      localDivulgence: Relation[EventId, Party],
      globalDivulgence: Relation[AbsoluteContractId, Party],
      divulgedContracts: List[(Value.AbsoluteContractId, AbsoluteContractInst)]
  ) extends PersistenceEntry
  final case class Checkpoint(entry: LedgerEntry.Checkpoint) extends PersistenceEntry
}

sealed abstract class PersistenceResponse extends Product with Serializable

object PersistenceResponse {

  case object Ok extends PersistenceResponse

  case object Duplicate extends PersistenceResponse

}

case class LedgerSnapshot(offset: Long, acs: Source[ActiveContract, NotUsed])

trait LedgerReadDao extends AutoCloseable {

  type LedgerOffset = Long

  type ExternalOffset = LedgerString

  /** Looks up the ledger id */
  def lookupLedgerId(): Future[Option[LedgerId]]

  /** Looks up the current ledger end */
  def lookupLedgerEnd(): Future[LedgerOffset]

  /** Looks up the current external ledger end offset*/
  def lookupExternalLedgerEnd(): Future[Option[LedgerString]]

  /** Looks up an active or divulged contract if it is visible for the given party. Archived contracts must not be returned by this method */
  def lookupActiveOrDivulgedContract(
      contractId: AbsoluteContractId,
      forParty: Party): Future[Option[Contract]]
  /** Looks up the current ledger configuration, if it has been set. */
  def lookupLedgerConfiguration(): Future[Option[Configuration]]

  /** Get a stream of configuration entries. */
  def getConfigurationEntries(
      startInclusive: Long,
      endExclusive: Long): Source[(Long, ConfigurationEntry), NotUsed]

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
    * Looks up the transaction with the given id
    *
    * @param transactionId the id of the transaction to look up
    * @return the optional Transaction found
    */
  def lookupTransaction(transactionId: TransactionId): Future[Option[(LedgerOffset, Transaction)]]

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
    * Looks up a Contract given a contract key and a party
    *
    * @param key the contract key to query
    * @param forParty the party for which the contract must be visible
    * @return the optional AbsoluteContractId
    */
  def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]]

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
  def getActiveContractSnapshot(untilExclusive: LedgerOffset, filter: TemplateAwareFilter)(
      implicit mat: Materializer): Future[LedgerSnapshot]

  /** Returns a list of all known parties. */
  def getParties: Future[List[PartyDetails]]

  /** Returns a list of all known DAML-LF packages */
  def listLfPackages: Future[Map[PackageId, PackageDetails]]

  /** Returns the given DAML-LF archive */
  def getLfArchive(packageId: PackageId): Future[Option[Archive]]
}

trait LedgerWriteDao extends AutoCloseable {

  type LedgerOffset = Long

  type ExternalOffset = LedgerString

  /**
    * Initializes the ledger. Must be called only once.
    *
    * @param ledgerId  the ledger id to be stored
    * @param ledgerEnd the ledger end to be stored
    */
  def initializeLedger(ledgerId: LedgerId, ledgerEnd: LedgerOffset): Future[Unit]

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
      externalOffset: Option[ExternalOffset],
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
      activeContracts: immutable.Seq[ActiveContract],
      ledgerEntries: immutable.Seq[(LedgerOffset, LedgerEntry)],
      newLedgerEnd: LedgerOffset
  ): Future[Unit]

  /**
    * Explicitly adds a new party to the list of known parties.
    *
    * @param party The party identifier
    * @param displayName The human readable display name
    * @return
    */
  def storeParty(
      party: Party,
      displayName: Option[String],
      externalOffset: Option[ExternalOffset]
  ): Future[PersistenceResponse]

  /**
    * Store a configuration change or rejection.
    */
  def storeConfigurationEntry(
      offset: LedgerOffset,
      newLedgerEnd: LedgerOffset,
      externalOffset: Option[ExternalOffset],
      recordedAt: Instant,
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
      rejectionReason: Option[String]
  ): Future[PersistenceResponse]

  /**
    * Stores a set of DAML-LF packages
    *
    * @param uploadId A unique identifier for this upload. Can be used to find
    *   out which packages were uploaded together, in the case of concurrent uploads.
    *
    * @param packages The DAML-LF archives to upload, including their meta-data.
    *
    * @return Values from the PersistenceResponse enum to the number of archives that led to that result
    */
  def uploadLfPackages(
      uploadId: String,
      packages: List[(Archive, PackageDetails)],
      externalOffset: Option[ExternalOffset]
  ): Future[Map[PersistenceResponse, Int]]

  /** Resets the platform into a state as it was never used before. Meant to be used solely for testing. */
  def reset(): Future[Unit]

}

trait LedgerDao extends LedgerReadDao with LedgerWriteDao {
  override type LedgerOffset = Long
  override type ExternalOffset = LedgerString
}

object LedgerDao {

  /** Wraps the given LedgerDao adding metrics around important calls */
  def metered(dao: LedgerDao, metrics: MetricRegistry): LedgerDao = MeteredLedgerDao(dao, metrics)
  def meteredRead(dao: LedgerReadDao, metrics: MetricRegistry): LedgerReadDao =
    new MeteredLedgerReadDao(dao, metrics)
}
