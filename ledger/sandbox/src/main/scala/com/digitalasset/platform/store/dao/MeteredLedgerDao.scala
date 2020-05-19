// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.WorkflowId
import com.daml.ledger.api.domain.{CommandId, LedgerId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.dao.events.TransactionsReader
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}

import scala.concurrent.Future

class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: Metrics) extends LedgerReadDao {

  override def maxConcurrentConnections: Int = ledgerDao.maxConcurrentConnections

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupLedgerId(): Future[Option[LedgerId]] =
    Timed.future(metrics.daml.index.db.lookupLedgerId, ledgerDao.lookupLedgerId())

  override def lookupLedgerEnd(): Future[Offset] =
    Timed.future(metrics.daml.index.db.lookupLedgerEnd, ledgerDao.lookupLedgerEnd())

  override def lookupInitialLedgerEnd(): Future[Option[Offset]] =
    Timed.future(metrics.daml.index.db.lookupLedgerEnd, ledgerDao.lookupInitialLedgerEnd())

  override def lookupActiveOrDivulgedContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    Timed.future(
      metrics.daml.index.db.lookupActiveContract,
      ledgerDao.lookupActiveOrDivulgedContract(contractId, forParty))

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]] =
    Timed.future(
      metrics.daml.index.db.lookupMaximumLedgerTime,
      ledgerDao.lookupMaximumLedgerTime(contractIds))

  override def transactionsReader: TransactionsReader = ledgerDao.transactionsReader

  override def lookupKey(
      key: Node.GlobalKey,
      forParty: Party): Future[Option[Value.AbsoluteContractId]] =
    Timed.future(metrics.daml.index.db.lookupKey, ledgerDao.lookupKey(key, forParty))

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.db.getParties, ledgerDao.getParties(parties))

  override def listKnownParties(): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.db.listKnownParties, ledgerDao.listKnownParties())

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset
  ): Source[(Offset, PartyLedgerEntry), NotUsed] =
    ledgerDao.getPartyEntries(startExclusive, endInclusive)

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    Timed.future(metrics.daml.index.db.listLfPackages, ledgerDao.listLfPackages)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    Timed.future(metrics.daml.index.db.getLfArchive, ledgerDao.getLfArchive(packageId))

  override def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] =
    ledgerDao.getPackageEntries(startExclusive, endInclusive)

  /** Looks up the current ledger configuration, if it has been set. */
  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    Timed.future(
      metrics.daml.index.db.lookupLedgerConfiguration,
      ledgerDao.lookupLedgerConfiguration())

  /** Get a stream of configuration entries. */
  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, ConfigurationEntry), NotUsed] =
    ledgerDao.getConfigurationEntries(startExclusive, endInclusive)

  override val completions: CommandCompletionsReader = ledgerDao.completions

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    Timed.future(
      metrics.daml.index.db.deduplicateCommand,
      ledgerDao.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil))

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    Timed.future(
      metrics.daml.index.db.removeExpiredDeduplicationData,
      ledgerDao.removeExpiredDeduplicationData(currentTime))

  override def stopDeduplicatingCommand(commandId: CommandId, submitter: Party): Future[Unit] =
    Timed.future(
      metrics.daml.index.db.stopDeduplicatingCommand,
      ledgerDao.stopDeduplicatingCommand(commandId, submitter))
}

class MeteredLedgerDao(ledgerDao: LedgerDao, metrics: Metrics)
    extends MeteredLedgerReadDao(ledgerDao, metrics)
    with LedgerDao {

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def storeTransaction(
      submitterInfo: Option[SubmitterInfo],
      workflowId: Option[WorkflowId],
      transactionId: TransactionId,
      recordTime: Instant,
      ledgerEffectiveTime: Instant,
      offset: Offset,
      transaction: CommittedTransaction,
      divulged: Iterable[DivulgedContract]): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeTransaction,
      ledgerDao.storeTransaction(
        submitterInfo,
        workflowId,
        transactionId,
        recordTime,
        ledgerEffectiveTime,
        offset,
        transaction,
        divulged,
      )
    )

  override def storeRejection(
      submitterInfo: Option[SubmitterInfo],
      recordTime: Instant,
      offset: Offset,
      reason: RejectionReason,
  ): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeRejection,
      ledgerDao.storeRejection(submitterInfo, recordTime, offset, reason),
    )

  override def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset,
  ): Future[Unit] =
    Timed.future(
      metrics.daml.index.db.storeInitialState,
      ledgerDao.storeInitialState(ledgerEntries, newLedgerEnd))

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: Offset): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId, ledgerEnd)

  override def reset(): Future[Unit] =
    ledgerDao.reset()

  override def storePartyEntry(
      offset: Offset,
      partyEntry: PartyLedgerEntry): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storePartyEntry,
      ledgerDao.storePartyEntry(offset, partyEntry))

  override def storeConfigurationEntry(
      offset: Offset,
      recordTime: Instant,
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
      rejectionReason: Option[String]
  ): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storeConfigurationEntry,
      ledgerDao.storeConfigurationEntry(
        offset,
        recordTime,
        submissionId,
        participantId,
        configuration,
        rejectionReason)
    )

  override def storePackageEntry(
      offset: Offset,
      packages: List[(Archive, PackageDetails)],
      entry: Option[PackageLedgerEntry]
  ): Future[PersistenceResponse] =
    Timed.future(
      metrics.daml.index.db.storePackageEntry,
      ledgerDao.storePackageEntry(offset, packages, entry))

}
