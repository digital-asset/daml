// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{Configuration, Offset, ParticipantId}
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails, TransactionFilter}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.platform.metrics.timedFuture
import com.digitalasset.platform.store.Contract.ActiveContract
import com.digitalasset.platform.store.dao.events.{TransactionsReader, TransactionsWriter}
import com.digitalasset.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.digitalasset.platform.store.{LedgerSnapshot, PersistenceEntry}

import scala.collection.immutable
import scala.concurrent.Future

class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: MetricRegistry)
    extends LedgerReadDao {

  private object Metrics {
    val lookupLedgerId: Timer = metrics.timer("daml.index.db.lookup_ledger_id")
    val lookupLedgerEnd: Timer = metrics.timer("daml.index.db.lookup_ledger_end")
    val lookupLedgerEntry: Timer = metrics.timer("daml.index.db.lookup_ledger_entry")
    val lookupTransaction: Timer = metrics.timer("daml.index.db.lookup_transaction")
    val lookupLedgerConfiguration: Timer =
      metrics.timer("daml.index.db.lookup_ledger_configuration")
    val lookupKey: Timer = metrics.timer("daml.index.db.lookup_key")
    val lookupActiveContract: Timer = metrics.timer("daml.index.db.lookup_active_contract")
    val lookupMaximumLedgerTime: Timer = metrics.timer("daml.index.db.lookup_maximum_ledger_time")
    val getParties: Timer = metrics.timer("daml.index.db.get_parties")
    val listKnownParties: Timer = metrics.timer("daml.index.db.list_known_parties")
    val listLfPackages: Timer = metrics.timer("daml.index.db.list_lf_packages")
    val getLfArchive: Timer = metrics.timer("daml.index.db.get_lf_archive")
    val deduplicateCommand: Timer = metrics.timer("daml.index.db.deduplicate_command")
    val removeExpiredDeduplicationData: Timer =
      metrics.timer("daml.index.db.remove_expired_deduplication_data")
  }

  override def maxConcurrentConnections: Int = ledgerDao.maxConcurrentConnections

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupLedgerId(): Future[Option[LedgerId]] =
    timedFuture(Metrics.lookupLedgerId, ledgerDao.lookupLedgerId())

  override def lookupLedgerEnd(): Future[Offset] =
    timedFuture(Metrics.lookupLedgerEnd, ledgerDao.lookupLedgerEnd())

  override def lookupInitialLedgerEnd(): Future[Option[Offset]] =
    timedFuture(Metrics.lookupLedgerEnd, ledgerDao.lookupInitialLedgerEnd())

  override def lookupActiveOrDivulgedContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    timedFuture(
      Metrics.lookupActiveContract,
      ledgerDao.lookupActiveOrDivulgedContract(contractId, forParty))

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Instant] =
    timedFuture(Metrics.lookupMaximumLedgerTime, ledgerDao.lookupMaximumLedgerTime(contractIds))

  override def lookupLedgerEntry(offset: Offset): Future[Option[LedgerEntry]] =
    timedFuture(Metrics.lookupLedgerEntry, ledgerDao.lookupLedgerEntry(offset))

  override def transactionsReader: TransactionsReader = ledgerDao.transactionsReader

  override def lookupKey(
      key: Node.GlobalKey,
      forParty: Party): Future[Option[Value.AbsoluteContractId]] =
    timedFuture(Metrics.lookupKey, ledgerDao.lookupKey(key, forParty))

  override def getActiveContractSnapshot(
      endInclusive: Offset,
      filter: TransactionFilter
  ): Future[LedgerSnapshot] =
    ledgerDao.getActiveContractSnapshot(endInclusive, filter)

  override def getLedgerEntries(
      startExclusive: Offset,
      endInclusive: Offset
  ): Source[(Offset, LedgerEntry), NotUsed] =
    ledgerDao.getLedgerEntries(startExclusive, endInclusive)

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    timedFuture(Metrics.getParties, ledgerDao.getParties(parties))

  override def listKnownParties(): Future[List[PartyDetails]] =
    timedFuture(Metrics.listKnownParties, ledgerDao.listKnownParties())

  override def getPartyEntries(
      startExclusive: Offset,
      endInclusive: Offset
  ): Source[(Offset, PartyLedgerEntry), NotUsed] =
    ledgerDao.getPartyEntries(startExclusive, endInclusive)

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    timedFuture(Metrics.listLfPackages, ledgerDao.listLfPackages)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    timedFuture(Metrics.getLfArchive, ledgerDao.getLfArchive(packageId))

  override def getPackageEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] =
    ledgerDao.getPackageEntries(startExclusive, endInclusive)

  /** Looks up the current ledger configuration, if it has been set. */
  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    timedFuture(Metrics.lookupLedgerConfiguration, ledgerDao.lookupLedgerConfiguration())

  /** Get a stream of configuration entries. */
  override def getConfigurationEntries(
      startExclusive: Offset,
      endInclusive: Offset): Source[(Offset, ConfigurationEntry), NotUsed] =
    ledgerDao.getConfigurationEntries(startExclusive, endInclusive)

  override val completions: CommandCompletionsReader[Offset] = ledgerDao.completions

  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    timedFuture(
      Metrics.deduplicateCommand,
      ledgerDao.deduplicateCommand(deduplicationKey, submittedAt, deduplicateUntil))

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    timedFuture(
      Metrics.removeExpiredDeduplicationData,
      ledgerDao.removeExpiredDeduplicationData(currentTime))
}

class MeteredLedgerDao(ledgerDao: LedgerDao, metrics: MetricRegistry)
    extends MeteredLedgerReadDao(ledgerDao, metrics)
    with LedgerDao {

  private object Metrics {
    val storePartyEntry: Timer = metrics.timer("daml.index.db.store_party_entry")
    val storeInitialState: Timer = metrics.timer("daml.index.db.store_initial_state")
    val storePackageEntry: Timer = metrics.timer("daml.index.db.store_package_entry")
    val storeLedgerEntry: Timer = metrics.timer("daml.index.db.store_ledger_entry")
    val storeConfigurationEntry: Timer = metrics.timer("daml.index.db.store_configuration_entry")
  }

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def storeLedgerEntry(
      offset: Offset,
      ledgerEntry: PersistenceEntry): Future[PersistenceResponse] =
    timedFuture(Metrics.storeLedgerEntry, ledgerDao.storeLedgerEntry(offset, ledgerEntry))

  override def storeInitialState(
      activeContracts: immutable.Seq[ActiveContract],
      ledgerEntries: immutable.Seq[(Offset, LedgerEntry)],
      newLedgerEnd: Offset
  ): Future[Unit] =
    timedFuture(
      Metrics.storeInitialState,
      ledgerDao.storeInitialState(activeContracts, ledgerEntries, newLedgerEnd))

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: Offset): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId, ledgerEnd)

  override def reset(): Future[Unit] =
    ledgerDao.reset()

  override def storePartyEntry(
      offset: Offset,
      partyEntry: PartyLedgerEntry): Future[PersistenceResponse] =
    timedFuture(Metrics.storePartyEntry, ledgerDao.storePartyEntry(offset, partyEntry))

  override def storeConfigurationEntry(
      offset: Offset,
      recordTime: Instant,
      submissionId: String,
      participantId: ParticipantId,
      configuration: Configuration,
      rejectionReason: Option[String]
  ): Future[PersistenceResponse] =
    timedFuture(
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
    timedFuture(Metrics.storePackageEntry, ledgerDao.storePackageEntry(offset, packages, entry))

  override def transactionsWriter: TransactionsWriter = ledgerDao.transactionsWriter
}
