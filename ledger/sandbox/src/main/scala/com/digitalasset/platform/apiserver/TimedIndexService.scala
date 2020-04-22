// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{
  ApplicationId,
  CommandId,
  ConfigurationEntry,
  LedgerId,
  LedgerOffset,
  TransactionId
}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1.{Configuration, PackageId, ParticipantId, Party}
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.metrics.{Metrics, Timed}

import scala.concurrent.Future

final class TimedIndexService(delegate: IndexService, metrics: Metrics) extends IndexService {

  override def listLfPackages(): Future[Map[PackageId, v2.PackageDetails]] =
    Timed.future(metrics.daml.services.indexService.listLfPackages, delegate.listLfPackages())

  override def getLfArchive(packageId: PackageId): Future[Option[DamlLf.Archive]] =
    Timed.future(metrics.daml.services.indexService.getLfArchive, delegate.getLfArchive(packageId))

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    Timed.future(metrics.daml.services.indexService.getLfPackage, delegate.getLfPackage(packageId))

  override def packageEntries(
      startExclusive: LedgerOffset.Absolute
  ): Source[domain.PackageEntry, NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.packageEntries,
      delegate.packageEntries(startExclusive))

  override def getLedgerConfiguration(): Source[v2.LedgerConfiguration, NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.getLedgerConfiguration,
      delegate.getLedgerConfiguration())

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] =
    Timed.future(metrics.daml.services.indexService.currentLedgerEnd, delegate.currentLedgerEnd())

  override def getCompletions(
      begin: domain.LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Party]
  ): Source[CompletionStreamResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.getCompletions,
      delegate.getCompletions(begin, applicationId, parties))

  override def transactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean
  ): Source[GetTransactionsResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.transactions,
      delegate.transactions(begin, endAt, filter, verbose))

  override def transactionTrees(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean
  ): Source[GetTransactionTreesResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.transactionTrees,
      delegate.transactionTrees(begin, endAt, filter, verbose))

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[GetFlatTransactionResponse]] =
    Timed.future(
      metrics.daml.services.indexService.getTransactionById,
      delegate.getTransactionById(transactionId, requestingParties))

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party]
  ): Future[Option[GetTransactionResponse]] =
    Timed.future(
      metrics.daml.services.indexService.getTransactionTreeById,
      delegate.getTransactionTreeById(transactionId, requestingParties))

  override def getActiveContracts(
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetActiveContractsResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.getActiveContracts,
      delegate.getActiveContracts(filter, verbose))

  override def lookupActiveContract(
      submitter: Party,
      contractId: Value.AbsoluteContractId
  ): Future[Option[Value.ContractInst[Value.VersionedValue[Value.AbsoluteContractId]]]] =
    Timed.future(
      metrics.daml.services.indexService.lookupActiveContract,
      delegate.lookupActiveContract(submitter, contractId))

  override def lookupContractKey(
      submitter: Party,
      key: Node.GlobalKey
  ): Future[Option[Value.AbsoluteContractId]] =
    Timed.future(
      metrics.daml.services.indexService.lookupContractKey,
      delegate.lookupContractKey(submitter, key))

  override def lookupMaximumLedgerTime(
      ids: Set[Value.AbsoluteContractId],
  ): Future[Option[Instant]] =
    Timed.future(
      metrics.daml.services.indexService.lookupMaximumLedgerTime,
      delegate.lookupMaximumLedgerTime(ids))

  override def getLedgerId(): Future[LedgerId] =
    Timed.future(metrics.daml.services.indexService.getLedgerId, delegate.getLedgerId())

  override def getParticipantId(): Future[ParticipantId] =
    Timed.future(metrics.daml.services.indexService.getParticipantId, delegate.getParticipantId())

  override def getParties(parties: Seq[Party]): Future[List[domain.PartyDetails]] =
    Timed.future(metrics.daml.services.indexService.getParties, delegate.getParties(parties))

  override def listKnownParties(): Future[List[domain.PartyDetails]] =
    Timed.future(metrics.daml.services.indexService.listKnownParties, delegate.listKnownParties())

  override def partyEntries(
      startExclusive: LedgerOffset.Absolute
  ): Source[domain.PartyEntry, NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.partyEntries,
      delegate.partyEntries(startExclusive))

  override def lookupConfiguration(): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    Timed.future(
      metrics.daml.services.indexService.lookupConfiguration,
      delegate.lookupConfiguration())

  override def configurationEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  ): Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed] =
    Timed.source(
      metrics.daml.services.indexService.configurationEntries,
      delegate.configurationEntries(startExclusive))

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant
  ): Future[v2.CommandDeduplicationResult] =
    Timed.future(
      metrics.daml.services.indexService.deduplicateCommand,
      delegate.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil))

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitter: Ref.Party,
  ): Future[Unit] =
    Timed.future(
      metrics.daml.services.indexService.stopDeduplicateCommand,
      delegate.stopDeduplicatingCommand(commandId, submitter))

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
