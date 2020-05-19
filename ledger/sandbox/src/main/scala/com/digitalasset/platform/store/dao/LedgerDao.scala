// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  Configuration,
  DivulgedContract,
  Offset,
  ParticipantId,
  RejectionReason,
  SubmitterInfo,
  TransactionId
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.WorkflowId
import com.daml.ledger.api.domain.{CommandId, LedgerId, PartyDetails}
import com.daml.ledger.api.health.ReportsHealth
import com.daml.platform.store.dao.events.TransactionsReader
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}

import scala.concurrent.Future

trait LedgerReadDao extends ReportsHealth {

  def maxConcurrentConnections: Int

  /** Looks up the ledger id */
  def lookupLedgerId(): Future[Option[LedgerId]]

  /** Looks up the current ledger end */
  def lookupLedgerEnd(): Future[Offset]

  /** Looks up the current external ledger end offset */
  def lookupInitialLedgerEnd(): Future[Option[Offset]]

  /** Looks up an active or divulged contract if it is visible for the given party. Archived contracts must not be returned by this method */
  def lookupActiveOrDivulgedContract(
      contractId: AbsoluteContractId,
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  /** Returns the largest ledger time of any of the given contracts */
  def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]]

  /** Looks up the current ledger configuration, if it has been set. */
  def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]]

  /** Returns a stream of configuration entries. */
  def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, ConfigurationEntry), NotUsed]

  def transactionsReader: TransactionsReader

  /**
    * Looks up a Contract given a contract key and a party
    *
    * @param key the contract key to query
    * @param forParty the party for which the contract must be visible
    * @return the optional AbsoluteContractId
    */
  def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]]

  /** Returns a list of party details for the parties specified. */
  def getParties(parties: Seq[Party]): Future[List[PartyDetails]]

  /** Returns a list of all known parties. */
  def listKnownParties(): Future[List[PartyDetails]]

  def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset
  ): Source[(Offset, PartyLedgerEntry), NotUsed]

  /** Returns a list of all known DAML-LF packages */
  def listLfPackages: Future[Map[PackageId, PackageDetails]]

  /** Returns the given DAML-LF archive */
  def getLfArchive(packageId: PackageId): Future[Option[Archive]]

  /** Returns a stream of package upload entries.
    * @return a stream of package entries tupled with their offset
    */
  def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed]

  def completions: CommandCompletionsReader

  /** Deduplicates commands.
    *
    * @param commandId The command Id
    * @param submitter The submitting party
    * @param submittedAt The time when the command was submitted
    * @param deduplicateUntil The time until which the command should be deduplicated
    * @return whether the command is a duplicate or not
    */
  def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult]

  /**
    * Remove all expired deduplication entries. This method has to be called
    * periodically to ensure that the deduplication cache does not grow unboundedly.
    *
    * @param currentTime The current time. This should use the same source of time as
    *                    the `deduplicateUntil` argument of [[deduplicateCommand]].
    *
    * @return when DAO has finished removing expired entries. Clients do not
    *         need to wait for the operation to finish, it is safe to concurrently
    *         call deduplicateCommand().
    */
  def removeExpiredDeduplicationData(
      currentTime: Instant,
  ): Future[Unit]

  /**
    * Stops deduplicating the given command. This method should be called after
    * a command is rejected by the submission service, or after a transaction is
    * rejected by the ledger. Without removing deduplication entries for failed
    * commands, applications would have to wait for the end of the (long) deduplication
    * window before they could send a retry.
    *
    * @param commandId The command Id
    * @param submitter The submitting party
    * @return
    */
  def stopDeduplicatingCommand(
      commandId: CommandId,
      submitter: Ref.Party,
  ): Future[Unit]
}

trait LedgerWriteDao extends ReportsHealth {

  def maxConcurrentConnections: Int

  /**
    * Initializes the ledger. Must be called only once.
    *
    * @param ledgerId the ledger id to be stored
    */
  def initializeLedger(ledgerId: LedgerId, ledgerEnd: Offset): Future[Unit]

  def storeTransaction(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
  ): Future[PersistenceResponse]

  def storeRejection(
      submitterInfo: Option[SubmitterInfo],
      recordTime: Instant,
      offset: Offset,
      reason: RejectionReason,
  ): Future[PersistenceResponse]

  /**
    * Stores the initial ledger state, e.g., computed by the scenario loader.
    * Must be called at most once, before any call to storeLedgerEntry.
    *
    * @param ledgerEntries the list of LedgerEntries to save
    * @return Ok when the operation was successful
    */
  def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset
  ): Future[Unit]

  /**
    * Stores a party allocation or rejection thereof.
    *
    * @param offset       the offset to store the party entry
    * @param partyEntry  the PartyEntry to be stored
    * @return Ok when the operation was successful otherwise a Duplicate
    */
  def storePartyEntry(offset: Offset, partyEntry: PartyLedgerEntry): Future[PersistenceResponse]

  /**
    * Store a configuration change or rejection.
    */
  def storeConfigurationEntry(
      offset: Offset,
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
      offset: Offset,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry]
  ): Future[PersistenceResponse]

  /** Resets the platform into a state as it was never used before. Meant to be used solely for testing. */
  def reset(): Future[Unit]

}

trait LedgerDao extends LedgerReadDao with LedgerWriteDao
