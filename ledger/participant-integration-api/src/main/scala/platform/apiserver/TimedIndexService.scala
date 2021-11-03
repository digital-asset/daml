// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

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
  TransactionId,
}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse,
}
import com.daml.ledger.configuration.Configuration
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.{Metrics, Timed}

import scala.concurrent.Future

private[daml] final class TimedIndexService(delegate: IndexService, metrics: Metrics)
    extends IndexService {

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, v2.PackageDetails]] =
    Timed.future(metrics.daml.services.index.listLfPackages, delegate.listLfPackages())

  override def getLfArchive(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[DamlLf.Archive]] =
    Timed.future(metrics.daml.services.index.getLfArchive, delegate.getLfArchive(packageId))

  override def getLfPackage(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Ast.Package]] =
    Timed.future(metrics.daml.services.index.getLfPackage, delegate.getLfPackage(packageId))

  override def packageEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContext): Source[domain.PackageEntry, NotUsed] =
    Timed.source(
      metrics.daml.services.index.packageEntries,
      delegate.packageEntries(startExclusive),
    )

  override def getLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Source[v2.LedgerConfiguration, NotUsed] =
    Timed.source(
      metrics.daml.services.index.getLedgerConfiguration,
      delegate.getLedgerConfiguration(),
    )

  override def currentLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[LedgerOffset.Absolute] =
    Timed.future(metrics.daml.services.index.currentLedgerEnd, delegate.currentLedgerEnd())

  override def getCompletions(
      begin: domain.LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.index.getCompletions,
      delegate.getCompletions(begin, applicationId, parties),
    )

  override def transactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.index.transactions,
      delegate.transactions(begin, endAt, filter, verbose),
    )

  override def transactionTrees(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionTreesResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.index.transactionTrees,
      delegate.transactionTrees(begin, endAt, filter, verbose),
    )

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    Timed.future(
      metrics.daml.services.index.getTransactionById,
      delegate.getTransactionById(transactionId, requestingParties),
    )

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    Timed.future(
      metrics.daml.services.index.getTransactionTreeById,
      delegate.getTransactionTreeById(transactionId, requestingParties),
    )

  override def getActiveContracts(
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] =
    Timed.source(
      metrics.daml.services.index.getActiveContracts,
      delegate.getActiveContracts(filter, verbose),
    )

  override def lookupActiveContract(
      readers: Set[Ref.Party],
      contractId: Value.ContractId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[Value.VersionedContractInstance]] =
    Timed.future(
      metrics.daml.services.index.lookupActiveContract,
      delegate.lookupActiveContract(readers, contractId),
    )

  override def lookupContractKey(
      readers: Set[Ref.Party],
      key: GlobalKey,
  )(implicit loggingContext: LoggingContext): Future[Option[Value.ContractId]] =
    Timed.future(
      metrics.daml.services.index.lookupContractKey,
      delegate.lookupContractKey(readers, key),
    )

  override def lookupMaximumLedgerTime(
      ids: Set[Value.ContractId]
  )(implicit
      loggingContext: LoggingContext
  ): Future[Either[Set[Value.ContractId], Option[Timestamp]]] =
    Timed.future(
      metrics.daml.services.index.lookupMaximumLedgerTime,
      delegate.lookupMaximumLedgerTime(ids),
    )

  override def getLedgerId()(implicit loggingContext: LoggingContext): Future[LedgerId] =
    Timed.future(metrics.daml.services.index.getLedgerId, delegate.getLedgerId())

  override def getParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Ref.ParticipantId] =
    Timed.future(metrics.daml.services.index.getParticipantId, delegate.getParticipantId())

  override def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[domain.PartyDetails]] =
    Timed.future(metrics.daml.services.index.getParties, delegate.getParties(parties))

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[domain.PartyDetails]] =
    Timed.future(metrics.daml.services.index.listKnownParties, delegate.listKnownParties())

  override def partyEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContext): Source[domain.PartyEntry, NotUsed] =
    Timed.source(metrics.daml.services.index.partyEntries, delegate.partyEntries(startExclusive))

  override def lookupConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    Timed.future(metrics.daml.services.index.lookupConfiguration, delegate.lookupConfiguration())

  override def configurationEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit
      loggingContext: LoggingContext
  ): Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed] =
    Timed.source(
      metrics.daml.services.index.configurationEntries,
      delegate.configurationEntries(startExclusive),
    )

  override def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(implicit loggingContext: LoggingContext): Future[v2.CommandDeduplicationResult] =
    Timed.future(
      metrics.daml.services.index.deduplicateCommand,
      delegate.deduplicateCommand(commandId, submitters, submittedAt, deduplicateUntil),
    )

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    Timed.future(
      metrics.daml.services.index.stopDeduplicateCommand,
      delegate.stopDeduplicatingCommand(commandId, submitters),
    )

  override def prune(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    Timed.future(
      metrics.daml.services.index.prune,
      delegate.prune(pruneUpToInclusive, pruneAllDivulgedContracts),
    )

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
