// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.TransactionId
import com.digitalasset.ledger.api.domain.{ApplicationId, LedgerId, PartyDetails, TransactionFilter}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse
}
import com.digitalasset.platform.metrics.timedFuture
import com.digitalasset.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}
import com.digitalasset.platform.store.{LedgerSnapshot, ReadOnlyLedger}

import scala.concurrent.Future

class MeteredReadOnlyLedger(ledger: ReadOnlyLedger, metrics: MetricRegistry)
    extends ReadOnlyLedger {

  private object Metrics {
    val lookupContract: Timer = metrics.timer("daml.index.lookup_contract")
    val lookupKey: Timer = metrics.timer("daml.index.lookup_key")
    val lookupFlatTransactionById: Timer = metrics.timer("daml.index.lookup_flat_transaction_by_id")
    val lookupTransactionTreeById: Timer = metrics.timer("daml.index.lookup_transaction_tree_by_id")
    val lookupLedgerConfiguration: Timer = metrics.timer("daml.index.lookup_ledger_configuration")
    val lookupMaximumLedgerTime: Timer = metrics.timer("daml.index.lookup_maximum_ledger_time")
    val getParties: Timer = metrics.timer("daml.index.get_parties")
    val listKnownParties: Timer = metrics.timer("daml.index.list_known_parties")
    val listLfPackages: Timer = metrics.timer("daml.index.list_lf_packages")
    val getLfArchive: Timer = metrics.timer("daml.index.get_lf_archive")
    val getLfPackage: Timer = metrics.timer("daml.index.get_lf_package")
    val deduplicateCommand: Timer = metrics.timer("daml.index.deduplicate_command")
    val removeExpiredDeduplicationData: Timer =
      metrics.timer("daml.index.remove_expired_deduplication_data")
  }

  override def ledgerId: LedgerId = ledger.ledgerId

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def ledgerEntries(
      startExclusive: Option[Offset],
      endOpt: Option[Offset]): Source[(Offset, LedgerEntry), NotUsed] =
    ledger.ledgerEntries(startExclusive, endOpt)

  override def ledgerEnd: Offset = ledger.ledgerEnd

  override def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Party]): Source[(Offset, CompletionStreamResponse), NotUsed] =
    ledger.completions(startExclusive, endInclusive, applicationId, parties)

  override def snapshot(filter: TransactionFilter): Future[LedgerSnapshot] =
    ledger.snapshot(filter)

  override def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    timedFuture(Metrics.lookupContract, ledger.lookupContract(contractId, forParty))

  override def lookupKey(key: GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    timedFuture(Metrics.lookupKey, ledger.lookupKey(key, forParty))

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]] =
    timedFuture(
      Metrics.lookupFlatTransactionById,
      ledger.lookupFlatTransactionById(transactionId, requestingParties),
    )

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]] =
    timedFuture(
      Metrics.lookupTransactionTreeById,
      ledger.lookupTransactionTreeById(transactionId, requestingParties),
    )

  override def lookupMaximumLedgerTime(contractIds: Set[AbsoluteContractId]): Future[Instant] =
    timedFuture(Metrics.lookupMaximumLedgerTime, ledger.lookupMaximumLedgerTime(contractIds))

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    timedFuture(Metrics.getParties, ledger.getParties(parties))

  override def listKnownParties(): Future[List[PartyDetails]] =
    timedFuture(Metrics.listKnownParties, ledger.listKnownParties())

  override def partyEntries(startExclusive: Offset): Source[(Offset, PartyLedgerEntry), NotUsed] =
    ledger.partyEntries(startExclusive)

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    timedFuture(Metrics.listLfPackages, ledger.listLfPackages())

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    timedFuture(Metrics.getLfArchive, ledger.getLfArchive(packageId))

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    timedFuture(Metrics.getLfPackage, ledger.getLfPackage(packageId))

  override def packageEntries(
      startExclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] =
    ledger.packageEntries(startExclusive)

  override def close(): Unit = {
    ledger.close()
  }

  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    timedFuture(Metrics.lookupLedgerConfiguration, ledger.lookupLedgerConfiguration())

  override def configurationEntries(
      startExclusive: Option[Offset]): Source[(Offset, ConfigurationEntry), NotUsed] =
    ledger.configurationEntries(startExclusive)

  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    timedFuture(
      Metrics.deduplicateCommand,
      ledger.deduplicateCommand(deduplicationKey, submittedAt, deduplicateUntil))

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    timedFuture(
      Metrics.removeExpiredDeduplicationData,
      ledger.removeExpiredDeduplicationData(currentTime))
}

object MeteredReadOnlyLedger {
  def apply(ledger: ReadOnlyLedger, metrics: MetricRegistry): ReadOnlyLedger =
    new MeteredReadOnlyLedger(ledger, metrics)
}
