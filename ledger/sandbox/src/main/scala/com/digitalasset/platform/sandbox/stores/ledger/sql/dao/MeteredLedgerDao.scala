// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TransactionId}
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.sandbox.metrics.timedFuture
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.{ActiveContract, Contract}
import com.digitalasset.platform.sandbox.stores.ledger.{
  ConfigurationEntry,
  LedgerEntry,
  PartyLedgerEntry,
  PackageLedgerEntry
}

import scala.collection.immutable
import scala.concurrent.Future

private class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: MetricRegistry)
    extends LedgerReadDao
    with AutoCloseable {

  private object Metrics {
    val lookupLedgerId: Timer = metrics.timer("LedgerDao.lookupLedgerId")
    val lookupLedgerEnd: Timer = metrics.timer("LedgerDao.lookupLedgerEnd")
    val lookupExternalLedgerEnd: Timer = metrics.timer("LedgerDao.lookupExternalLedgerEnd")
    val lookupLedgerEntry: Timer = metrics.timer("LedgerDao.lookupLedgerEntry")
    val lookupTransaction: Timer = metrics.timer("LedgerDao.lookupTransaction")
    val lookupLedgerConfiguration: Timer = metrics.timer("LedgerDao.lookupLedgerConfiguration")
    val lookupKey: Timer = metrics.timer("LedgerDao.lookupKey")
    val lookupActiveContract: Timer = metrics.timer("LedgerDao.lookupActiveContract")
    val getParties: Timer = metrics.timer("LedgerDao.getParties")
    val listLfPackages: Timer = metrics.timer("LedgerDao.listLfPackages")
    val getLfArchive: Timer = metrics.timer("LedgerDao.getLfArchive")
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
      forParty: Party): Future[Option[Contract]] =
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

  override def getActiveContractSnapshot(untilExclusive: LedgerOffset, filter: TemplateAwareFilter)(
      implicit mat: Materializer): Future[LedgerSnapshot] =
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

  override def close(): Unit = {
    ledgerDao.close()
  }

  /** Looks up the current ledger configuration, if it has been set. */
  override def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]] =
    timedFuture(Metrics.lookupLedgerConfiguration, ledgerDao.lookupLedgerConfiguration())

  /** Get a stream of configuration entries. */
  override def getConfigurationEntries(
      startInclusive: LedgerOffset,
      endExclusive: LedgerOffset): Source[(LedgerOffset, ConfigurationEntry), NotUsed] =
    ledgerDao.getConfigurationEntries(startInclusive, endExclusive)
}

private class MeteredLedgerDao(ledgerDao: LedgerDao, metrics: MetricRegistry)
    extends MeteredLedgerReadDao(ledgerDao, metrics)
    with LedgerDao {

  private object Metrics {
    val storePartyEntry: Timer = metrics.timer("LedgerDao.storePartyEntry")
    val storeInitialState: Timer = metrics.timer("LedgerDao.storeInitialState")
    val storePackageEntry: Timer = metrics.timer("LedgerDao.storePackageEntry")
    val storeLedgerEntry: Timer = metrics.timer("LedgerDao.storeLedgerEntry")
    val storeConfigurationEntry: Timer = metrics.timer("LedgerDao.storeConfigurationEntry")
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

  override def close(): Unit = {
    ledgerDao.close()
  }
}

object MeteredLedgerDao {
  def apply(ledgerDao: LedgerDao, metrics: MetricRegistry): LedgerDao =
    new MeteredLedgerDao(ledgerDao, metrics)

  def apply(ledgerDao: LedgerReadDao, metrics: MetricRegistry): LedgerReadDao =
    new MeteredLedgerReadDao(ledgerDao, metrics)
}
