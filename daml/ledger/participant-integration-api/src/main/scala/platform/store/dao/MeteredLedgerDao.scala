// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain.{LedgerId, ParticipantId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.{BlindingInfo, CommittedTransaction}
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import com.daml.platform.store.interfaces.LedgerDaoContractsReader

import scala.concurrent.Future

private[platform] class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: Metrics)
    extends LedgerReadDao {

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupLedgerId()(implicit loggingContext: LoggingContext): Future[Option[LedgerId]] =
    Timed.future(metrics.daml.index.db.lookupLedgerId, ledgerDao.lookupLedgerId())

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Option[ParticipantId]] =
    Timed.future(metrics.daml.index.db.lookupParticipantId, ledgerDao.lookupParticipantId())

  override def lookupLedgerEnd()(implicit loggingContext: LoggingContext): Future[LedgerEnd] =
    Timed.future(metrics.daml.index.db.lookupLedgerEnd, ledgerDao.lookupLedgerEnd())

  override def transactionsReader: LedgerDaoTransactionsReader = ledgerDao.transactionsReader

  override def contractsReader: LedgerDaoContractsReader = ledgerDao.contractsReader

  override def getParties(parties: Seq[Ref.Party])(implicit
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
  ): Future[Map[Ref.PackageId, PackageDetails]] =
    Timed.future(metrics.daml.index.db.listLfPackages, ledgerDao.listLfPackages())

  override def getLfArchive(packageId: Ref.PackageId)(implicit
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

  override val completions: LedgerDaoCommandCompletionsReader = ledgerDao.completions

  override def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Timed.future(
      metrics.daml.index.db.prune,
      ledgerDao.prune(pruneUpToInclusive, pruneAllDivulgedContracts),
    )

  /** Returns all TransactionMetering records matching given criteria */
  override def meteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(implicit loggingContext: LoggingContext): Future[ReportData] = {
    Timed.future(
      metrics.daml.index.db.prune,
      ledgerDao.meteringReportData(from, to, applicationId),
    )
  }
}

private[platform] class MeteredLedgerDao(ledgerDao: LedgerDao, metrics: Metrics)
    extends MeteredLedgerReadDao(ledgerDao, metrics)
    with LedgerDao {

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def storeRejection(
      completionInfo: Option[state.CompletionInfo],
      recordTime: Timestamp,
      offset: Offset,
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeRejection,
      ledgerDao.storeRejection(completionInfo, recordTime, offset, reason),
    )

  override def initialize(
      ledgerId: LedgerId,
      participantId: ParticipantId,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    ledgerDao.initialize(ledgerId, participantId)

  override def storePartyEntry(
      offset: Offset,
      partyEntry: PartyLedgerEntry,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storePartyEntry,
      ledgerDao.storePartyEntry(offset, partyEntry),
    )

  override def storeConfigurationEntry(
      offset: Offset,
      recordTime: Timestamp,
      submissionId: String,
      configuration: Configuration,
      rejectionReason: Option[String],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeConfigurationEntry,
      ledgerDao.storeConfigurationEntry(
        offset,
        recordTime,
        submissionId,
        configuration,
        rejectionReason,
      ),
    )

  override def storePackageEntry(
      offset: Offset,
      packages: List[(Archive, PackageDetails)],
      entry: Option[PackageLedgerEntry],
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storePackageEntry,
      ledgerDao.storePackageEntry(offset, packages, entry),
    )

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  override def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[Ref.WorkflowId],
      transactionId: Ref.TransactionId,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      transaction: CommittedTransaction,
      divulgedContracts: Iterable[state.DivulgedContract],
      blindingInfo: Option[BlindingInfo],
      recordTime: Timestamp,
  )(implicit loggingContext: LoggingContext): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeTransactionCombined,
      ledgerDao.storeTransaction(
        completionInfo,
        workflowId,
        transactionId,
        ledgerEffectiveTime,
        offset,
        transaction,
        divulgedContracts,
        blindingInfo,
        recordTime,
      ),
    )
}
