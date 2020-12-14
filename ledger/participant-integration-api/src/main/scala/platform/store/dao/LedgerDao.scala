// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.WorkflowId
import com.daml.ledger.api.domain.{CommandId, LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{
  CommittedTransaction,
  Configuration,
  DivulgedContract,
  Offset,
  RejectionReason,
  SubmitterInfo,
  TransactionId
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.transaction.{BlindingInfo, GlobalKey}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.logging.LoggingContext
import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.dao.events.{TransactionsReader, TransactionsWriter}
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}

import scala.concurrent.Future

private[platform] trait LedgerReadDao extends ReportsHealth {

  def maxConcurrentConnections: Int

  /** Looks up the ledger id */
  def lookupLedgerId()(implicit loggingContext: LoggingContext): Future[Option[LedgerId]]

  def lookupParticipantId()(implicit loggingContext: LoggingContext): Future[Option[ParticipantId]]

  /** Looks up the current ledger end */
  def lookupLedgerEnd()(implicit loggingContext: LoggingContext): Future[Offset]

  /** Looks up the current external ledger end offset */
  def lookupInitialLedgerEnd()(implicit loggingContext: LoggingContext): Future[Option[Offset]]

  /** Looks up an active or divulged contract if it is visible for the given party. Archived contracts must not be returned by this method */
  def lookupActiveOrDivulgedContract(contractId: ContractId, forParties: Set[Party])(
      implicit loggingContext: LoggingContext,
  ): Future[Option[ContractInst[Value.VersionedValue[ContractId]]]]

  /** Returns the largest ledger time of any of the given contracts */
  def lookupMaximumLedgerTime(
      contractIds: Set[ContractId],
  )(implicit loggingContext: LoggingContext): Future[Option[Instant]]

  /** Looks up the current ledger configuration, if it has been set. */
  def lookupLedgerConfiguration()(
      implicit loggingContext: LoggingContext,
  ): Future[Option[(Offset, Configuration)]]

  /** Returns a stream of configuration entries. */
  def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed]

  def transactionsReader: TransactionsReader

  /**
    * Looks up a Contract given a contract key and a party
    *
    * @param key the contract key to query
    * @param forParties a set of parties for one of which the contract must be visible
    * @return the optional ContractId
    */
  def lookupKey(key: GlobalKey, forParties: Set[Party])(
      implicit loggingContext: LoggingContext,
  ): Future[Option[ContractId]]

  /** Returns a list of party details for the parties specified. */
  def getParties(parties: Seq[Party])(
      implicit loggingContext: LoggingContext,
  ): Future[List[PartyDetails]]

  /** Returns a list of all known parties. */
  def listKnownParties()(implicit loggingContext: LoggingContext): Future[List[PartyDetails]]

  def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset
  )(implicit loggingContext: LoggingContext): Source[(Offset, PartyLedgerEntry), NotUsed]

  /** Returns a list of all known DAML-LF packages */
  def listLfPackages()(
      implicit loggingContext: LoggingContext,
  ): Future[Map[PackageId, PackageDetails]]

  /** Returns the given DAML-LF archive */
  def getLfArchive(packageId: PackageId)(
      implicit loggingContext: LoggingContext,
  ): Future[Option[Archive]]

  /** Returns a stream of package upload entries.
    * @return a stream of package entries tupled with their offset
    */
  def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, PackageLedgerEntry), NotUsed]

  def completions: CommandCompletionsReader

  /** Deduplicates commands.
    *
    * @param commandId The command Id
    * @param submitters The submitting parties
    * @param submittedAt The time when the command was submitted
    * @param deduplicateUntil The time until which the command should be deduplicated
    * @return whether the command is a duplicate or not
    */
  def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult]

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
  )(implicit loggingContext: LoggingContext): Future[Unit]

  /**
    * Stops deduplicating the given command. This method should be called after
    * a command is rejected by the submission service, or after a transaction is
    * rejected by the ledger. Without removing deduplication entries for failed
    * commands, applications would have to wait for the end of the (long) deduplication
    * window before they could send a retry.
    *
    * @param commandId The command Id
    * @param submitters The submitting parties
    * @return
    */
  def stopDeduplicatingCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit]

  /**
    * Prunes participant events and completions in archived history and remembers largest
    * pruning offset processed thus far.
    *
    * @param pruneUpToInclusive offset up to which to prune archived history inclusively
    * @return
    */
  def prune(pruneUpToInclusive: Offset)(implicit loggingContext: LoggingContext): Future[Unit]
}

private[platform] trait LedgerWriteDao extends ReportsHealth {
  def maxConcurrentConnections: Int

  /**
    * Initializes the ledger. Must be called only once.
    *
    * @param ledgerId the ledger id to be stored
    */
  def initializeLedger(ledgerId: LedgerId)(implicit loggingContext: LoggingContext): Future[Unit]

  def initializeParticipantId(participantId: ParticipantId)(
      implicit loggingContext: LoggingContext,
  ): Future[Unit]

  def prepareTransactionInsert(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  )(implicit loggingContext: LoggingContext): TransactionsWriter.PreparedInsert

  def storeTransaction(
      preparedInsert: PreparedInsert,
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offsetStep: OffsetStep,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse]

  def storeRejection(
      submitterInfo: Option[SubmitterInfo],
      recordTime: Instant,
      offsetStep: OffsetStep,
      reason: RejectionReason,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse]

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
  )(implicit loggingContext: LoggingContext): Future[Unit]

  /**
    * Stores a party allocation or rejection thereof.
    *
    * @param offsetStep  Pair of previous offset and the offset to store the party entry at
    * @param partyEntry  the PartyEntry to be stored
    * @return Ok when the operation was successful otherwise a Duplicate
    */
  def storePartyEntry(offsetStep: OffsetStep, partyEntry: PartyLedgerEntry)(
      implicit loggingContext: LoggingContext,
  ): Future[PersistenceResponse]

  /**
    * Store a configuration change or rejection.
    */
  def storeConfigurationEntry(
      offsetStep: OffsetStep,
      recordedAt: Instant,
      submissionId: String,
      configuration: Configuration,
      rejectionReason: Option[String])(
      implicit loggingContext: LoggingContext): Future[PersistenceResponse]

  /**
    * Store a DAML-LF package upload result.
    */
  def storePackageEntry(
      offsetStep: OffsetStep,
      packages: List[(Archive, PackageDetails)],
      optEntry: Option[PackageLedgerEntry]
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse]

  /** Resets the platform into a state as it was never used before. Meant to be used solely for testing. */
  def reset()(implicit loggingContext: LoggingContext): Future[Unit]

}

private[platform] trait LedgerDao extends LedgerReadDao with LedgerWriteDao
