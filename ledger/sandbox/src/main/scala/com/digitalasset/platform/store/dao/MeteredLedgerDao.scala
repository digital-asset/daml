// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TransactionId}
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.platform.metrics.timedFuture
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

class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: MetricRegistry)
    extends LedgerReadDao {

  private object Metrics {
    val lookupLedgerId: Timer = metrics.timer("daml.index.db.lookup_ledger_id")
    val lookupLedgerEnd: Timer = metrics.timer("daml.index.db.lookup_ledger_end")
    val lookupExternalLedgerEnd: Timer = metrics.timer("daml.index.db.lookup_external_ledger_end")
    val lookupLedgerEntry: Timer = metrics.timer("daml.index.db.lookup_ledger_entry")
    val lookupTransaction: Timer = metrics.timer("daml.index.db.lookup_transaction")
    val lookupLedgerConfiguration: Timer =
      metrics.timer("daml.index.db.lookup_ledger_configuration")
    val lookupKey: Timer = metrics.timer("daml.index.db.lookup_key")
    val lookupActiveContract: Timer = metrics.timer("daml.index.db.lookup_active_contract")
    val getParties: Timer = metrics.timer("daml.index.db.get_parties")
    val listLfPackages: Timer = metrics.timer("daml.index.db.list_lf_packages")
    val getLfArchive: Timer = metrics.timer("daml.index.db.get_lf_archive")
    val deduplicateCommand: Timer = metrics.timer("daml.index.db.deduplicate_command")
    val updateCommandResult: Timer = metrics.timer("daml.index.db.update_command_result")
  }

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupLedgerId(): Future[Option[LedgerId]] =
    timedFuture(Metrics.lookupLedgerId, ledgerDao.lookupLedgerId())

  override def lookupLedgerEnd(): Future[Long] =
    timedFuture(Metrics.lookupLedgerEnd, ledgerDao.lookupLedgerEnd())

  override def lookupExternalLedgerEnd(): Future[Option[LedgerString]] =
    timedFuture(Metrics.lookupExternalLedgerEnd, ledgerDao.lookupExternalLedgerEnd())

  override def lookupActiveOrDivulgedContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    timedFuture(
      Metrics.lookupActiveContract,
      ledgerDao.lookupActiveOrDivulgedContract(contractId, forParty))

  override def lookupLedgerEntry(offset: Long): Future[Option[LedgerEntry]] =
    timedFuture(Metrics.lookupLedgerEntry, ledgerDao.lookupLedgerEntry(offset))

  override def lookupTransaction(
      transactionId: TransactionId): Future[Option[(LedgerOffset, LedgerEntry.Transaction)]] =
    timedFuture(Metrics.lookupTransaction, ledgerDao.lookupTransaction(transactionId))

  override def lookupKey(
      key: Node.GlobalKey,
      forParty: Party): Future[Option[Value.AbsoluteContractId]] =
    timedFuture(Metrics.lookupKey, ledgerDao.lookupKey(key, forParty))

  override def getActiveContractSnapshot(
      untilExclusive: LedgerOffset,
      filter: TemplateAwareFilter): Future[LedgerSnapshot] =
    ledgerDao.getActiveContractSnapshot(untilExclusive, filter)

  override def getLedgerEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, LedgerEntry), NotUsed] =
    ledgerDao.getLedgerEntries(startInclusive, endExclusive)

  override def getParties: Future[List[PartyDetails]] =
    timedFuture(Metrics.getParties, ledgerDao.getParties)

  override def getPartyEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, PartyLedgerEntry), NotUsed] =
    ledgerDao.getPartyEntries(startInclusive, endExclusive)

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    timedFuture(Metrics.listLfPackages, ledgerDao.listLfPackages)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    timedFuture(Metrics.getLfArchive, ledgerDao.getLfArchive(packageId))

  override def getPackageEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, PackageLedgerEntry), NotUsed] =
    ledgerDao.getPackageEntries(startInclusive, endExclusive)

  /** Looks up the current ledger configuration, if it has been set. */
  override def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]] =
    timedFuture(Metrics.lookupLedgerConfiguration, ledgerDao.lookupLedgerConfiguration())

  /** Get a stream of configuration entries. */
  override def getConfigurationEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, ConfigurationEntry), NotUsed] =
    ledgerDao.getConfigurationEntries(startInclusive, endExclusive)

  override val completions: CommandCompletionsReader[LedgerOffset] = ledgerDao.completions

  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      ttl: Instant): Future[Option[CommandDeduplicationEntry]] =
    timedFuture(
      Metrics.deduplicateCommand,
      ledgerDao.deduplicateCommand(deduplicationKey, submittedAt, ttl))

  override def updateCommandResult(
      deduplicationKey: String,
      submittedAt: Instant,
      result: Either[String, Unit]): Future[Unit] =
    timedFuture(
      Metrics.updateCommandResult,
      ledgerDao.updateCommandResult(deduplicationKey, submittedAt, result))
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
      offset: Long,
      newLedgerEnd: Long,
      externalOffset: Option[ExternalOffset],
      ledgerEntry: PersistenceEntry): Future[PersistenceResponse] =
    timedFuture(
      Metrics.storeLedgerEntry,
      ledgerDao.storeLedgerEntry(offset, newLedgerEnd, externalOffset, ledgerEntry))

  override def storeInitialState(
      activeContracts: immutable.Seq[ActiveContract],
      ledgerEntries: immutable.Seq[(LedgerOffset, LedgerEntry)],
      newLedgerEnd: LedgerOffset
  ): Future[Unit] =
    timedFuture(
      Metrics.storeInitialState,
      ledgerDao.storeInitialState(activeContracts, ledgerEntries, newLedgerEnd))

  override def initializeLedger(ledgerId: LedgerId, ledgerEnd: LedgerOffset): Future[Unit] =
    ledgerDao.initializeLedger(ledgerId, ledgerEnd)

  override def reset(): Future[Unit] =
    ledgerDao.reset()

  override def storePartyEntry(
      offset: LedgerOffset,
      newLedgerEnd: LedgerOffset,
      externalOffset: Option[ExternalOffset],
      partyEntry: PartyLedgerEntry): Future[PersistenceResponse] =
    timedFuture(
      Metrics.storePartyEntry,
      ledgerDao.storePartyEntry(offset, newLedgerEnd, externalOffset, partyEntry))

  override def storeConfigurationEntry(
      offset: LedgerOffset,
      newLedgerEnd: LedgerOffset,
      externalOffset: Option[ExternalOffset],
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
        newLedgerEnd,
        externalOffset,
        recordTime,
        submissionId,
        participantId,
        configuration,
        rejectionReason)
    )

  override def storePackageEntry(
      offset: LedgerOffset,
      newLedgerEnd: LedgerOffset,
      externalOffset: Option[ExternalOffset],
      packages: List[(Archive, PackageDetails)],
      entry: Option[PackageLedgerEntry]
  ): Future[PersistenceResponse] =
    timedFuture(
      Metrics.storePackageEntry,
      ledgerDao.storePackageEntry(offset, newLedgerEnd, externalOffset, packages, entry))
}
