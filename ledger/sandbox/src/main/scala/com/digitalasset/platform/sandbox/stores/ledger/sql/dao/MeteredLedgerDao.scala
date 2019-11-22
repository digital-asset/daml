// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger.sql.dao

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.{Configuration, ParticipantId, TransactionId}
import com.digitalasset.daml.lf.data.Ref.{LedgerString, PackageId, Party}
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.sandbox.metrics.timedFuture
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.{ActiveContract, Contract}
import com.digitalasset.platform.sandbox.stores.ledger.LedgerEntry
import com.digitalasset.platform.sandbox.stores.ledger.{ConfigurationEntry, LedgerEntry}

import scala.collection.immutable
import scala.concurrent.Future

private class MeteredLedgerReadDao(ledgerDao: LedgerReadDao, metrics: MetricRegistry)
    extends LedgerReadDao
    with AutoCloseable {

  private object Metrics {
    val lookupLedgerId = metrics.timer("LedgerDao.lookupLedgerId")
    val lookupLedgerEnd = metrics.timer("LedgerDao.lookupLedgerEnd")
    val lookupExternalLedgerEnd = metrics.timer("LedgerDao.lookupExternalLedgerEnd")
    val lookupLedgerEntry = metrics.timer("LedgerDao.lookupLedgerEntry")
    val lookupTransaction = metrics.timer("LedgerDao.lookupTransaction")
    val lookupLedgerConfiguration = metrics.timer("LedgerDao.lookupLedgerConfiguration")
    val lookupKey = metrics.timer("LedgerDao.lookupKey")
    val lookupActiveContract = metrics.timer("LedgerDao.lookupActiveContract")
    val getParties = metrics.timer("LedgerDao.getParties")
    val listLfPackages = metrics.timer("LedgerDao.listLfPackages")
    val getLfArchive = metrics.timer("LedgerDao.getLfArchive")

  }
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

  override def listLfPackages: Future[Map[PackageId, PackageDetails]] =
    timedFuture(Metrics.listLfPackages, ledgerDao.listLfPackages)

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    timedFuture(Metrics.getLfArchive, ledgerDao.getLfArchive(packageId))

  override def close(): Unit = {
    ledgerDao.close()
  }

  /** Looks up the current ledger configuration, if it has been set. */
  override def lookupLedgerConfiguration(): Future[Option[Configuration]] =
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
    val storeParty = metrics.timer("LedgerDao.storeParty")
    val storeInitialState = metrics.timer("LedgerDao.storeInitialState")
    val uploadLfPackages = metrics.timer("LedgerDao.uploadLfPackages")
    val storeLedgerEntry = metrics.timer("LedgerDao.storeLedgerEntry")
    val storeConfigurationEntry = metrics.timer("LedgerDao.storeConfigurationEntry")
  }
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

  override def storeParty(
      party: Party,
      displayName: Option[String],
      externalOffset: Option[ExternalOffset]): Future[PersistenceResponse] =
    timedFuture(Metrics.storeParty, ledgerDao.storeParty(party, displayName, externalOffset))

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

  override def uploadLfPackages(
      uploadId: String,
      packages: List[(Archive, PackageDetails)],
      externalOffset: Option[ExternalOffset]): Future[Map[PersistenceResponse, Int]] =
    timedFuture(
      Metrics.uploadLfPackages,
      ledgerDao.uploadLfPackages(uploadId, packages, externalOffset))

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
