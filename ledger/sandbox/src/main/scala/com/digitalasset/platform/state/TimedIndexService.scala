// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.state

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{Configuration, PackageId, ParticipantId, Party}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{ApplicationId, LedgerId, LedgerOffset, TransactionId}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.digitalasset.platform.metrics.{timedFuture, timedSource}

import scala.concurrent.Future

final class TimedIndexService(delegate: IndexService, metrics: MetricRegistry, prefix: String)
    extends IndexService {
  override def listLfPackages(): Future[Map[PackageId, v2.PackageDetails]] =
    time("listLfPackages", delegate.listLfPackages())

  override def getLfArchive(packageId: PackageId): Future[Option[DamlLf.Archive]] =
    time("getLfArchive", delegate.getLfArchive(packageId))

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    time("getLfPackage", delegate.getLfPackage(packageId))

  override def packageEntries(
      startExclusive: LedgerOffset.Absolute
  ): Source[domain.PackageEntry, NotUsed] =
    time("packageEntries", delegate.packageEntries(startExclusive))

  override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
    time("getLedgerConfiguration", delegate.getLedgerConfiguration())

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] =
    time("currentLedgerEnd", delegate.currentLedgerEnd())

  override def getCompletions(
      begin: domain.LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Party]
  ): Source[CompletionStreamResponse, NotUsed] =
    time("getCompletions", delegate.getCompletions(begin, applicationId, parties))

  override def transactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean
  ): Source[GetTransactionsResponse, NotUsed] =
    time("transactions", delegate.transactions(begin, endAt, filter, verbose))

  override def transactionTrees(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean
  ): Source[GetTransactionTreesResponse, NotUsed] =
    time("transactionTrees", delegate.transactionTrees(begin, endAt, filter, verbose))

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[GetFlatTransactionResponse]] =
    time("getTransactionById", delegate.getTransactionById(transactionId, requestingParties))

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[GetTransactionResponse]] =
    time(
      "getTransactionTreeById",
      delegate.getTransactionTreeById(transactionId, requestingParties))

  override def getActiveContractSetSnapshot(
      filter: domain.TransactionFilter
  ): Future[v2.ActiveContractSetSnapshot] =
    time("getActiveContractSetSnapshot", delegate.getActiveContractSetSnapshot(filter))

  override def lookupActiveContract(
      submitter: Party,
      contractId: Value.AbsoluteContractId
  ): Future[Option[Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]]] =
    time("lookupActiveContract", delegate.lookupActiveContract(submitter, contractId))

  override def lookupContractKey(
      submitter: Party,
      key: Node.GlobalKey
  ): Future[Option[Value.AbsoluteContractId]] =
    time("lookupContractKey", delegate.lookupContractKey(submitter, key))

  override def lookupMaximumLedgerTime(ids: Set[Value.AbsoluteContractId]): Future[Instant] =
    time("lookupMaximumLedgerTime", delegate.lookupMaximumLedgerTime(ids))

  override def getLedgerId(): Future[LedgerId] =
    time("getLedgerId", delegate.getLedgerId())

  override def getParticipantId(): Future[ParticipantId] =
    time("getParticipantId", delegate.getParticipantId())

  override def getParties(parties: Seq[Party]): Future[List[domain.PartyDetails]] =
    time("getParties", delegate.getParties(parties))

  override def listKnownParties(): Future[List[domain.PartyDetails]] =
    time("listKnownParties", delegate.listKnownParties())

  override def partyEntries(
      startExclusive: LedgerOffset.Absolute
  ): Source[domain.PartyEntry, NotUsed] =
    time("partyEntries", delegate.partyEntries(startExclusive))

  override def lookupConfiguration(): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    time("lookupConfiguration", delegate.lookupConfiguration())

  override def configurationEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  ): Source[domain.ConfigurationEntry, NotUsed] =
    time("configurationEntries", delegate.configurationEntries(startExclusive))

  override def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      deduplicateUntil: Instant
  ): Future[v2.CommandDeduplicationResult] =
    time(
      "deduplicateCommand",
      delegate.deduplicateCommand(deduplicationKey, submittedAt, deduplicateUntil))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  private def time[T](name: String, future: => Future[T]): Future[T] =
    timedFuture(metrics.timer(s"$prefix.$name"), future)

  private def time[Out, Mat](name: String, source: => Source[Out, Mat]): Source[Out, Mat] =
    timedSource(metrics.timer(s"$prefix.$name"), source)
}
