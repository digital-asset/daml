// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.{TransactionId, WorkflowId}
import com.daml.ledger.api.domain.{CommandId, LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.transaction.{BlindingInfo, GlobalKey}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.indexer.OffsetStep
import com.daml.platform.store.dao.events.{ContractsReader, TransactionsReader, TransactionsWriter}
import com.daml.platform.store.dao.events.TransactionsWriter.PreparedInsert
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry,
}

import scala.concurrent.Future

private[platform] class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: Metrics)
    extends LedgerReadDao {

  override def maxConcurrentConnections: Int = ledgerDao.maxConcurrentConnections

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupLedgerId()(implicit loggingContext: LoggingContext): Future[Option[LedgerId]] =
    Timed.future(metrics.daml.index.db.lookupLedgerId, ledgerDao.lookupLedgerId())

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Option[ParticipantId]] =
    Timed.future(metrics.daml.index.db.lookupParticipantId, ledgerDao.lookupParticipantId())

  override def lookupLedgerEnd()(implicit loggingContext: LoggingContext): Future[Offset] =
    Timed.future(metrics.daml.index.db.lookupLedgerEnd, ledgerDao.lookupLedgerEnd())

  override def lookupInitialLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[Option[Offset]] =
    Timed.future(metrics.daml.index.db.lookupLedgerEnd, ledgerDao.lookupInitialLedgerEnd())

  override def lookupActiveOrDivulgedContract(
      contractId: Value.ContractId,
      forParties: Set[Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractInst[Value.VersionedValue[ContractId]]]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      ledgerDao.lookupActiveOrDivulgedContract(contractId, forParties),
    )

  override def lookupMaximumLedgerTime(
      contractIds: Set[ContractId]
  )(implicit loggingContext: LoggingContext): Future[Option[Instant]] =
    Timed.future(
      metrics.daml.index.db.lookupMaximumLedgerTime,
      ledgerDao.lookupMaximumLedgerTime(contractIds),
    )

  override def transactionsReader: TransactionsReader = ledgerDao.transactionsReader

  override def contractsReader: ContractsReader = ledgerDao.contractsReader

  override def lookupKey(key: GlobalKey, forParties: Set[Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Value.ContractId]] =
    Timed.future(metrics.daml.index.db.lookupKey, ledgerDao.lookupKey(key, forParties))

  override def getParties(parties: Seq[Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.db.getParties, ledgerDao.getParties(parties))

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.db.listKnownParties, ledgerDao.listKnownParties())

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, PartyLedgerEntry), NotUsed] =
    ledgerDao.getPartyEntries(startExclusive, endInclusive)

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[PackageId, PackageDetails]] =
    Timed.future(metrics.daml.index.db.listLfPackages, ledgerDao.listLfPackages())

  override def getLfArchive(packageId: PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Archive]] =
    Timed.future(metrics.daml.index.db.getLfArchive, ledgerDao.getLfArchive(packageId))

  override def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, PackageLedgerEntry), NotUsed] =
    ledgerDao.getPackageEntries(startExclusive, endInclusive)

  /** Looks up the current ledger configuration, if it has been set. */
  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    Timed.future(
      metrics.daml.index.db.lookupLedgerConfiguration,
      ledgerDao.lookupLedgerConfiguration(),
    )

  /** Get a stream of configuration entries. */
  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset,
  )(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed] =
    ledgerDao.getConfigurationEntries(startExclusive, endInclusive)

  override val completions: CommandCompletionsReader = ledgerDao.completions

  override def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    Timed.future(
      metrics.daml.index.db.deduplicateCommand,
      ledgerDao.deduplicateCommand(commandId, submitters, submittedAt, deduplicateUntil),
    )

  override def removeExpiredDeduplicationData(currentTime: Instant)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Timed.future(
      metrics.daml.index.db.removeExpiredDeduplicationData,
      ledgerDao.removeExpiredDeduplicationData(currentTime),
    )

  override def stopDeduplicatingCommand(commandId: CommandId, submitters: List[Party])(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Timed.future(
      metrics.daml.index.db.stopDeduplicatingCommand,
      ledgerDao.stopDeduplicatingCommand(commandId, submitters),
    )

  override def prune(pruneUpToInclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Timed.future(metrics.daml.index.db.prune, ledgerDao.prune(pruneUpToInclusive))

  override def lookupLedgerEndAndEventSequentialId()(implicit
      loggingContext: LoggingContext
  ): Future[(Offset, Long)] = ledgerDao.lookupLedgerEndAndEventSequentialId()
}

private[platform] class MeteredLedgerDao(ledgerDao: LedgerDao, metrics: Metrics)
    extends MeteredLedgerReadDao(ledgerDao, metrics)
    with LedgerDao {

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def storeTransaction(
      preparedInsert: PreparedInsert,
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offsetStep: OffsetStep,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeTransaction,
      ledgerDao.storeTransaction(
        preparedInsert,
        submitterInfo,
        transactionId,
        recordTime,
        ledgerEffectiveTime,
        offsetStep,
        transaction,
        divulged,
      ),
    )

  def prepareTransactionInsert(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[DivulgedContract],
      blindingInfo: Option[BlindingInfo],
  ): TransactionsWriter.PreparedInsert =
    ledgerDao.prepareTransactionInsert(
      submitterInfo,
      workflowId,
      transactionId,
      ledgerEffectiveTime,
      offset,
      transaction,
      divulgedContracts,
      blindingInfo,
    )

  override def storeRejection(
      submitterInfo: Option[SubmitterInfo],
      recordTime: Instant,
      offsetStep: OffsetStep,
      reason: RejectionReason,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeRejection,
      ledgerDao.storeRejection(submitterInfo, recordTime, offsetStep, reason),
    )

  override def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    Timed.future(
      metrics.daml.index.db.storeInitialState,
      ledgerDao.storeInitialState(ledgerEntries, newLedgerEnd),
    )

  override def initializeLedger(ledgerId: LedgerId)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId)

  override def initializeParticipantId(participantId: ParticipantId)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    ledgerDao.initializeParticipantId(participantId)

  override def reset()(implicit loggingContext: LoggingContext): Future[Unit] =
    ledgerDao.reset()

  override def storePartyEntry(
      offsetStep: OffsetStep,
      partyEntry: PartyLedgerEntry,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storePartyEntry,
      ledgerDao.storePartyEntry(offsetStep, partyEntry),
    )

  override def storeConfigurationEntry(
      offsetStep: OffsetStep,
      recordTime: Instant,
      submissionId: String,
      configuration: Configuration,
      rejectionReason: Option[String],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeConfigurationEntry,
      ledgerDao.storeConfigurationEntry(
        offsetStep,
        recordTime,
        submissionId,
        configuration,
        rejectionReason,
      ),
    )

  override def storePackageEntry(
      offsetStep: OffsetStep,
      packages: List[(Archive, PackageDetails)],
      entry: Option[PackageLedgerEntry],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storePackageEntry,
      ledgerDao.storePackageEntry(offsetStep, packages, entry),
    )

  override def storeTransactionEvents(preparedInsert: PreparedInsert)(implicit
      loggingContext: LoggingContext
  ): Future[PersistenceResponse] =
    ledgerDao.storeTransactionEvents(preparedInsert)

  override def completeTransaction(
      submitterInfo: Option[SubmitterInfo],
      transactionId: TransactionId,
      recordTime: Instant,
      offsetStep: OffsetStep,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    ledgerDao.completeTransaction(submitterInfo, transactionId, recordTime, offsetStep)
}
