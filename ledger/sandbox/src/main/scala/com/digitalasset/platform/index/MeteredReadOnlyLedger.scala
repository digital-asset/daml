// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.{MetricRegistry, Timer}
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.Configuration
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{
  ApplicationId,
  LedgerId,
  PartyDetails,
  TransactionFilter,
  TransactionId
}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
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
    val lookupTransaction: Timer = metrics.timer("daml.index.lookup_transaction")
    val lookupLedgerConfiguration: Timer = metrics.timer("daml.index.lookup_ledger_configuration")
    val getParty: Timer = metrics.timer("daml.index.get_party")
    val parties: Timer = metrics.timer("daml.index.parties")
    val listLfPackages: Timer = metrics.timer("daml.index.list_lf_packages")
    val getLfArchive: Timer = metrics.timer("daml.index.get_lf_archive")
    val getLfPackage: Timer = metrics.timer("daml.index.get_lf_package")
    val deduplicateCommand: Timer = metrics.timer("daml.index.deduplicate_command")
  }

  override def ledgerId: LedgerId = ledger.ledgerId

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def ledgerEntries(
      offset: Option[Long],
      endOpt: Option[Long]): Source[(Long, LedgerEntry), NotUsed] =
    ledger.ledgerEntries(offset, endOpt)

  override def ledgerEnd: Long = ledger.ledgerEnd

  override def completions(
      beginInclusive: Option[Long],
      endExclusive: Option[Long],
      applicationId: ApplicationId,
      parties: Set[Party]): Source[(Long, CompletionStreamResponse), NotUsed] =
    ledger.completions(beginInclusive, endExclusive, applicationId, parties)

  override def snapshot(filter: TransactionFilter): Future[LedgerSnapshot] =
    ledger.snapshot(filter)

  override def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    timedFuture(Metrics.lookupContract, ledger.lookupContract(contractId, forParty))

  override def lookupKey(key: GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    timedFuture(Metrics.lookupKey, ledger.lookupKey(key, forParty))

  override def lookupTransaction(
      transactionId: TransactionId
  ): Future[Option[(Long, LedgerEntry.Transaction)]] =
    timedFuture(Metrics.lookupTransaction, ledger.lookupTransaction(transactionId))

  override def getParty(party: Party): Future[Option[PartyDetails]] =
    timedFuture(Metrics.getParty, ledger.getParty(party))

  override def parties: Future[List[PartyDetails]] =
    timedFuture(Metrics.parties, ledger.parties)

  override def partyEntries(beginOffset: Long): Source[(Long, PartyLedgerEntry), NotUsed] =
    ledger.partyEntries(beginOffset)

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    timedFuture(Metrics.listLfPackages, ledger.listLfPackages())

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    timedFuture(Metrics.getLfArchive, ledger.getLfArchive(packageId))

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    timedFuture(Metrics.getLfPackage, ledger.getLfPackage(packageId))

  override def packageEntries(beginOffset: Long): Source[(Long, PackageLedgerEntry), NotUsed] =
    ledger.packageEntries(beginOffset)

  override def close(): Unit = {
    ledger.close()
  }

  override def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]] =
    timedFuture(Metrics.lookupLedgerConfiguration, ledger.lookupLedgerConfiguration())

  override def configurationEntries(
      offset: Option[Long]): Source[(Long, ConfigurationEntry), NotUsed] =
    ledger.configurationEntries(offset)

  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    timedFuture(
      Metrics.deduplicateCommand,
      ledger.deduplicateCommand(deduplicationKey, submittedAt, deduplicateUntil))
}

object MeteredReadOnlyLedger {
  def apply(ledger: ReadOnlyLedger, metrics: MetricRegistry): ReadOnlyLedger =
    new MeteredReadOnlyLedger(ledger, metrics)
}
