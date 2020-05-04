// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Timer}
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
import com.daml.metrics.{MetricName, Timed}
import com.daml.platform.store.dao.events.TransactionsReader
import com.daml.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}

import scala.concurrent.Future

class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: MetricRegistry)
    extends LedgerReadDao {

  private object Metrics {
    private val prefix = MetricName.DAML :+ "index" :+ "db"

    val lookupLedgerId: Timer = metrics.timer(prefix :+ "lookup_ledger_id")
    val lookupLedgerEnd: Timer = metrics.timer(prefix :+ "lookup_ledger_end")
    val lookupLedgerEntry: Timer = metrics.timer(prefix :+ "lookup_ledger_entry")
    val lookupTransaction: Timer = metrics.timer(prefix :+ "lookup_transaction")
    val lookupLedgerConfiguration: Timer = metrics.timer(prefix :+ "lookup_ledger_configuration")
    val lookupKey: Timer = metrics.timer(prefix :+ "lookup_key")
    val lookupActiveContract: Timer = metrics.timer(prefix :+ "lookup_active_contract")
    val lookupMaximumLedgerTime: Timer = metrics.timer(prefix :+ "lookup_maximum_ledger_time")
    val getParties: Timer = metrics.timer(prefix :+ "get_parties")
    val listKnownParties: Timer = metrics.timer(prefix :+ "list_known_parties")
    val listLfPackages: Timer = metrics.timer(prefix :+ "list_lf_packages")
    val getLfArchive: Timer = metrics.timer(prefix :+ "get_lf_archive")
    val deduplicateCommand: Timer = metrics.timer(prefix :+ "deduplicate_command")
    val removeExpiredDeduplicationData: Timer =
      metrics.timer(prefix :+ "remove_expired_deduplication_data")
    val stopDeduplicatingCommand: Timer =
      metrics.timer(prefix :+ "stop_deduplicating_command")
  }

  override def maxConcurrentConnections: Int = ledgerDao.maxConcurrentConnections

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupLedgerId(): Future[Option[LedgerId]] =
    Timed.future(Metrics.lookupLedgerId, ledgerDao.lookupLedgerId())

  override def lookupLedgerEnd(): Future[Offset] =
    Timed.future(Metrics.lookupLedgerEnd, ledgerDao.lookupLedgerEnd())

  override def lookupInitialLedgerEnd(): Future[Option[Offset]] =
    Timed.future(Metrics.lookupLedgerEnd, ledgerDao.lookupInitialLedgerEnd())

  override def lookupActiveOrDivulgedContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    Timed.future(
      Metrics.lookupActiveContract,
      ledgerDao.lookupActiveOrDivulgedContract(contractId, forParty))

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]] =
    Timed.future(Metrics.lookupMaximumLedgerTime, ledgerDao.lookupMaximumLedgerTime(contractIds))

  override def transactionsReader: TransactionsReader = ledgerDao.transactionsReader

  override def lookupKey(
      key: Node.GlobalKey,
      forParty: Party): Future[Option[Value.AbsoluteContractId]] =
    Timed.future(Metrics.lookupKey, ledgerDao.lookupKey(key, forParty))

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    Timed.future(Metrics.getParties, ledgerDao.getParties(parties))

  override def listKnownParties(): Future[List[PartyDetails]] =
    Timed.future(Metrics.listKnownParties, ledgerDao.listKnownParties())

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset
  ): Source[(Offset, PartyLedgerEntry), NotUsed] =
    ledgerDao.getPartyEntries(startExclusive, endInclusive)

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    Timed.future(Metrics.listLfPackages, ledgerDao.listLfPackages)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    Timed.future(Metrics.getLfArchive, ledgerDao.getLfArchive(packageId))

  override def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] =
    ledgerDao.getPackageEntries(startExclusive, endInclusive)

  /** Looks up the current ledger configuration, if it has been set. */
  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    Timed.future(Metrics.lookupLedgerConfiguration, ledgerDao.lookupLedgerConfiguration())

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
      Metrics.deduplicateCommand,
      ledgerDao.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil))

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    Timed.future(
      Metrics.removeExpiredDeduplicationData,
      ledgerDao.removeExpiredDeduplicationData(currentTime))

  override def stopDeduplicatingCommand(commandId: CommandId, submitter: Party): Future[Unit] =
    Timed.future(
      Metrics.stopDeduplicatingCommand,
      ledgerDao.stopDeduplicatingCommand(commandId, submitter))
}

class MeteredLedgerDao(ledgerDao: LedgerDao, metrics: MetricRegistry)
    extends MeteredLedgerReadDao(ledgerDao, metrics)
    with LedgerDao {

  private object Metrics {
    private val prefix = MetricName.DAML :+ "index" :+ "db"

    val storePartyEntry: Timer = metrics.timer(prefix :+ "store_party_entry")
    val storeInitialState: Timer = metrics.timer(prefix :+ "store_initial_state")
    val storePackageEntry: Timer = metrics.timer(prefix :+ "store_package_entry")
    val storeTransaction: Timer = metrics.timer(prefix :+ "store_ledger_entry")
    val storeRejection: Timer = metrics.timer(prefix :+ "store_rejection")
    val storeConfigurationEntry: Timer = metrics.timer(prefix :+ "store_configuration_entry")
  }

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
      Metrics.storeTransaction,
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
      Metrics.storeRejection,
      ledgerDao.storeRejection(submitterInfo, recordTime, offset, reason),
    )

  override def storeInitialState(
      ledgerEntries: Vector[(Offset, LedgerEntry)],
      newLedgerEnd: Offset,
  ): Future[Unit] =
    Timed.future(
      Metrics.storeInitialState,
      ledgerDao.storeInitialState(ledgerEntries, newLedgerEnd))

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: Offset): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId, ledgerEnd)

  override def reset(): Future[Unit] =
    ledgerDao.reset()

  override def storePartyEntry(
      offset: Offset,
      partyEntry: PartyLedgerEntry): Future[PersistenceResponse] =
    Timed.future(Metrics.storePartyEntry, ledgerDao.storePartyEntry(offset, partyEntry))

  override def storeConfigurationEntry(
      offset: Offset,
      recordTime: Instant,
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
      rejectionReason: Option[String]
  ): Future[PersistenceResponse] =
    Timed.future(
      Metrics.storeConfigurationEntry,
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
    Timed.future(Metrics.storePackageEntry, ledgerDao.storePackageEntry(offset, packages, entry))

}
