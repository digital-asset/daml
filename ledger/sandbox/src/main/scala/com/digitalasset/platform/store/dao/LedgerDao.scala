// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TransactionId}
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.dec.DirectExecutionContext
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.ledger.api.health.ReportsHealth
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.store.entries.{
  CommandDeduplicationEntry,
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.digitalasset.platform.store.{LedgerSnapshot, PersistenceEntry}

import scala.collection.immutable
import scala.concurrent.Future

trait LedgerReadDao extends ReportsHealth {

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
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  /** Looks up the current ledger configuration, if it has been set. */
  def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]]

  /** Returns a stream of configuration entries. */
  def getConfigurationEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(Long, ConfigurationEntry), NotUsed]

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
  def lookupTransaction(
      transactionId: TransactionId
  ): Future[Option[(LedgerOffset, LedgerEntry.Transaction)]]

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
    */
  def getActiveContractSnapshot(
      untilExclusive: LedgerOffset,
      filter: TemplateAwareFilter
  ): Future[LedgerSnapshot]

  /** Returns a list of all known parties. */
  def getParties: Future[List[PartyDetails]]

  def getPartyEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, PartyLedgerEntry), NotUsed]

  /** Returns a list of all known DAML-LF packages */
  def listLfPackages: Future[Map[PackageId, PackageDetails]]

  /** Returns the given DAML-LF archive */
  def getLfArchive(packageId: PackageId): Future[Option[Archive]]

  /** Returns a stream of package upload entries.
    * @param startInclusive starting offset inclusive
    * @param endExclusive   ending offset exclusive
    * @return a stream of package entries tupled with their offset
    */
  def getPackageEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, PackageLedgerEntry), NotUsed]

  def completions: CommandCompletionsReader[LedgerOffset]

  /** Deduplicates commands.
    *
    * @param deduplicationKey The key used to deduplicate commands
    * @param submittedAt The time when the command was submitted
    * @param ttl The time until which the command should be deduplicated
    * @return
    */
  def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      ttl: Instant): Future[Option[CommandDeduplicationEntry]]

  /** Sets the result of a command, so that duplicate submissions can return the original result */
  def updateCommandResult(
      deduplicationKey: String,
      submittedAt: Instant,
      result: Either[String, Unit]): Future[Unit]
}

trait LedgerWriteDao extends ReportsHealth {

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
    * Stores a party allocation or rejection thereof.
    *
    * @param offset       the offset to store the party entry
    * @param newLedgerEnd the new ledger end, valid after this operation finishes
    * @param partyEntry  the PartyEntry to be stored
    * @return Ok when the operation was successful otherwise a Duplicate
    */
  def storePartyEntry(
      offset: LedgerOffset,
      newLedgerEnd: LedgerOffset,
      externalOffset: Option[ExternalOffset],
      partyEntry: PartyLedgerEntry): Future[PersistenceResponse]

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
    * Store a DAML-LF package upload result.
    */
  def storePackageEntry(
      offset: LedgerOffset,
      newLedgerEnd: LedgerOffset,
      externalOffset: Option[ExternalOffset],
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry]
  ): Future[PersistenceResponse]

  /** Resets the platform into a state as it was never used before. Meant to be used solely for testing. */
  def reset(): Future[Unit]

}

trait LedgerDao extends LedgerReadDao with LedgerWriteDao {
  override type LedgerOffset = Long
  override type ExternalOffset = LedgerString
}
