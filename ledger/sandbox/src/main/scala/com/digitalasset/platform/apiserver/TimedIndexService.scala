// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.apiserver

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.metrics.{MetricName, Metrics}
import com.daml.ledger.participant.state.v1.{Configuration, PackageId, ParticipantId, Party}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml_lf_dev.DamlLf
import com.digitalasset.ledger.api.domain
import com.digitalasset.ledger.api.domain.{
  ApplicationId,
  CommandId,
  LedgerId,
  LedgerOffset,
  TransactionId
}
import com.digitalasset.ledger.api.health.HealthStatus
import com.digitalasset.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}

import scala.concurrent.Future

final class TimedIndexService(delegate: IndexService, metrics: MetricRegistry, prefix: MetricName)
    extends IndexService {
  override def listLfPackages(): Future[Map[PackageId, v2.PackageDetails]] =
    time("list_lf_packages", delegate.listLfPackages())

  override def getLfArchive(packageId: PackageId): Future[Option[DamlLf.Archive]] =
    time("get_lf_archive", delegate.getLfArchive(packageId))

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    time("get_lf_package", delegate.getLfPackage(packageId))

  override def packageEntries(
      startExclusive: LedgerOffset.Absolute
  ): Source[domain.PackageEntry, NotUsed] =
    time("package_entries", delegate.packageEntries(startExclusive))

  override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
    time("get_ledger_configuration", delegate.getLedgerConfiguration())

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] =
    time("current_ledger_end", delegate.currentLedgerEnd())

  override def getCompletions(
      begin: domain.LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Party]
  ): Source[CompletionStreamResponse, NotUsed] =
    time("get_completions", delegate.getCompletions(begin, applicationId, parties))

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
    time("transaction_trees", delegate.transactionTrees(begin, endAt, filter, verbose))

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[GetFlatTransactionResponse]] =
    time("get_transaction_by_id", delegate.getTransactionById(transactionId, requestingParties))

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[GetTransactionResponse]] =
    time(
      "get_transaction_tree_by_id",
      delegate.getTransactionTreeById(transactionId, requestingParties))

  override def getActiveContracts(
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetActiveContractsResponse, NotUsed] =
    time("get_active_contracts", delegate.getActiveContracts(filter, verbose))

  override def lookupActiveContract(
      submitter: Party,
      contractId: Value.AbsoluteContractId
  ): Future[Option[Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]]] =
    time("lookup_active_contract", delegate.lookupActiveContract(submitter, contractId))

  override def lookupContractKey(
      submitter: Party,
      key: Node.GlobalKey
  ): Future[Option[Value.AbsoluteContractId]] =
    time("lookup_contract_key", delegate.lookupContractKey(submitter, key))

  override def lookupMaximumLedgerTime(ids: Set[Value.AbsoluteContractId]): Future[Instant] =
    time("lookup_maximum_ledger_time", delegate.lookupMaximumLedgerTime(ids))

  override def getLedgerId(): Future[LedgerId] =
    time("get_ledger_id", delegate.getLedgerId())

  override def getParticipantId(): Future[ParticipantId] =
    time("get_participant_id", delegate.getParticipantId())

  override def getParties(parties: Seq[Party]): Future[List[domain.PartyDetails]] =
    time("get_parties", delegate.getParties(parties))

  override def listKnownParties(): Future[List[domain.PartyDetails]] =
    time("list_known_parties", delegate.listKnownParties())

  override def partyEntries(
      startExclusive: LedgerOffset.Absolute
  ): Source[domain.PartyEntry, NotUsed] =
    time("party_entries", delegate.partyEntries(startExclusive))

  override def lookupConfiguration(): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    time("lookup_configuration", delegate.lookupConfiguration())

  override def configurationEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  ): Source[domain.ConfigurationEntry, NotUsed] =
    time("configuration_entries", delegate.configurationEntries(startExclusive))

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant
  ): Future[v2.CommandDeduplicationResult] =
    time(
      "deduplicate_command",
      delegate.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil))

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitter: Ref.Party,
  ): Future[Unit] =
    time("stop_deduplicating_command", delegate.stopDeduplicatingCommand(commandId, submitter))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()

  private def time[T](name: String, future: => Future[T]): Future[T] =
    Metrics.timedFuture(metrics.timer(prefix :+ name), future)

  private def time[Out, Mat](name: String, source: => Source[Out, Mat]): Source[Out, Mat] =
    Metrics.timedSource(metrics.timer(prefix :+ name), source)
}
