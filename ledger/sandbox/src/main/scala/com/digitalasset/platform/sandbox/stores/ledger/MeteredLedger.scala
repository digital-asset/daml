// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.sandbox.stores.ledger

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2.PackageDetails
import com.daml.ledger.participant.state.v1._
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party, TransactionIdString}
import com.digitalasset.daml.lf.data.Time
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.AbsoluteContractId
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{LedgerId, PartyDetails}
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.sandbox.metrics.timedFuture
import com.digitalasset.platform.sandbox.stores.ActiveLedgerState.Contract

import scala.concurrent.Future

private class MeteredReadOnlyLedger(ledger: ReadOnlyLedger, metrics: MetricRegistry)
    extends ReadOnlyLedger {

  private object Metrics {
    val lookupContract = metrics.timer("Ledger.lookupContract")
    val lookupKey = metrics.timer("Ledger.lookupKey")
    val lookupTransaction = metrics.timer("Ledger.lookupTransaction")
    val parties = metrics.timer("Ledger.parties")
    val listLfPackages = metrics.timer("Ledger.listLfPackages")
    val getLfArchive = metrics.timer("Ledger.getLfArchive")
    val getLfPackage = metrics.timer("Ledger.getLfPackage")
  }

  override def ledgerId: LedgerId = ledger.ledgerId

  override def ledgerEntries(offset: Option[Long]): Source[(Long, LedgerEntry), NotUsed] =
    ledger.ledgerEntries(offset)

  override def ledgerEnd: Long = ledger.ledgerEnd

  override def snapshot(filter: TemplateAwareFilter): Future[LedgerSnapshot] =
    ledger.snapshot(filter)

  override def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party): Future[Option[Contract]] =
    timedFuture(Metrics.lookupContract, ledger.lookupContract(contractId, forParty))

  override def lookupKey(key: GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    timedFuture(Metrics.lookupKey, ledger.lookupKey(key, forParty))

  override def lookupTransaction(
      transactionId: TransactionIdString): Future[Option[(Long, LedgerEntry.Transaction)]] =
    timedFuture(Metrics.lookupTransaction, ledger.lookupTransaction(transactionId))

  override def parties: Future[List[PartyDetails]] =
    timedFuture(Metrics.parties, ledger.parties)

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    timedFuture(Metrics.listLfPackages, ledger.listLfPackages())

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    timedFuture(Metrics.getLfArchive, ledger.getLfArchive(packageId))

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    timedFuture(Metrics.getLfPackage, ledger.getLfPackage(packageId))

  override def close(): Unit = {
    ledger.close()
  }

  override def lookupLedgerConfiguration(): Future[Option[Configuration]] =
    mm.timedFuture("Ledger:lookupLedgerConfiguration", ledger.lookupLedgerConfiguration())

  override def configurationEntries(offset: Option[Long]): Source[(Long, ConfigurationEntry), NotUsed] =
    ledger.configurationEntries(offset)
}

object MeteredReadOnlyLedger {
  def apply(ledger: ReadOnlyLedger, metrics: MetricRegistry): ReadOnlyLedger =
    new MeteredReadOnlyLedger(ledger, metrics)
}

private class MeteredLedger(ledger: Ledger, metrics: MetricRegistry)
    extends MeteredReadOnlyLedger(ledger, metrics)
    with Ledger {

  private object Metrics {
    val publishHeartbeat = metrics.timer("Ledger.publishHeartbeat")
    val publishTransaction = metrics.timer("Ledger.publishTransaction")
    val addParty = metrics.timer("Ledger.addParty")
    val uploadPackages = metrics.timer("Ledger.uploadPackages")
  }

  override def publishHeartbeat(time: Instant): Future[Unit] =
    timedFuture(Metrics.publishHeartbeat, ledger.publishHeartbeat(time))

  override def publishTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      transaction: SubmittedTransaction): Future[SubmissionResult] =
    timedFuture(
      Metrics.publishTransaction,
      ledger.publishTransaction(submitterInfo, transactionMeta, transaction))

  override def allocateParty(
      party: Party,
      displayName: Option[String]): Future[PartyAllocationResult] =
    timedFuture(Metrics.addParty, ledger.allocateParty(party, displayName))

  override def uploadPackages(
      knownSince: Instant,
      sourceDescription: Option[String],
      payload: List[Archive]): Future[UploadPackagesResult] =
    timedFuture(
      Metrics.uploadPackages,
      ledger.uploadPackages(knownSince, sourceDescription, payload))

  override def publishConfiguration(
      maxRecordTime: Time.Timestamp,
      submissionId: String,
      config: Configuration): Future[SubmissionResult] =
    mm.timedFuture(
      "Ledger:publishConfiguration",
      ledger.publishConfiguration(maxRecordTime, submissionId, config))

  override def close(): Unit = {
    ledger.close()
  }

}

object MeteredLedger {
  def apply(ledger: Ledger, metrics: MetricRegistry): Ledger = new MeteredLedger(ledger, metrics)
}
