// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.apiserver

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
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
import com.daml.ledger.api.{Identifiers, domain}
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.IndexService
import com.daml.ledger.participant.state.v1._
import com.daml.lf.data.Ref
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.logging.LoggingContext
import com.daml.metrics.{Event, Spans}

import scala.concurrent.Future

private[daml] final class SpannedIndexService(delegate: IndexService) extends IndexService {

  override def listLfPackages()(
      implicit loggingContext: LoggingContext,
  ): Future[Map[PackageId, v2.PackageDetails]] =
    delegate.listLfPackages()

  override def getLfArchive(packageId: PackageId)(
      implicit loggingContext: LoggingContext,
  ): Future[Option[DamlLf.Archive]] =
    delegate.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId)(
      implicit loggingContext: LoggingContext,
  ): Future[Option[Ast.Package]] =
    delegate.getLfPackage(packageId)

  override def packageEntries(
      startExclusive: Option[LedgerOffset.Absolute],
  )(implicit loggingContext: LoggingContext): Source[domain.PackageEntry, NotUsed] =
    delegate.packageEntries(startExclusive)

  override def getLedgerConfiguration()(
      implicit loggingContext: LoggingContext,
  ): Source[v2.LedgerConfiguration, NotUsed] =
    delegate.getLedgerConfiguration()

  override def currentLedgerEnd()(
      implicit loggingContext: LoggingContext,
  ): Future[LedgerOffset.Absolute] =
    delegate.currentLedgerEnd()

  override def getCompletions(
      begin: domain.LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Party]
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed] =
    delegate.getCompletions(begin, applicationId, parties)

  override def transactions(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed] = {
    Spans.attachEventsToCurrentSpan(
      (txns: GetTransactionsResponse) =>
        txns.transactions.map(txn => Event(txn.commandId, Identifiers.fromTransaction(txn))),
      delegate.transactions(begin, endAt, filter, verbose)
    )
  }

  override def transactionTrees(
      begin: domain.LedgerOffset,
      endAt: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionTreesResponse, NotUsed] =
    Spans.attachEventsToCurrentSpan(
      (txns: GetTransactionTreesResponse) =>
        txns.transactions.map(txn => Event(txn.commandId, Identifiers.fromTransactionTree(txn))),
      delegate.transactionTrees(begin, endAt, filter, verbose)
    )

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    delegate.getTransactionById(transactionId, requestingParties)

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    delegate.getTransactionTreeById(transactionId, requestingParties)

  override def getActiveContracts(
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] =
    delegate.getActiveContracts(filter, verbose)

  override def lookupActiveContract(
      submitter: Party,
      contractId: Value.ContractId,
  )(implicit loggingContext: LoggingContext)
    : Future[Option[Value.ContractInst[Value.VersionedValue[Value.ContractId]]]] =
    delegate.lookupActiveContract(submitter, contractId)

  override def lookupContractKey(
      submitter: Party,
      key: GlobalKey,
  )(implicit loggingContext: LoggingContext): Future[Option[Value.ContractId]] =
    delegate.lookupContractKey(submitter, key)

  override def lookupMaximumLedgerTime(
      ids: Set[Value.ContractId],
  )(implicit loggingContext: LoggingContext): Future[Option[Instant]] =
    delegate.lookupMaximumLedgerTime(ids)

  override def getLedgerId()(implicit loggingContext: LoggingContext): Future[LedgerId] =
    delegate.getLedgerId()

  override def getParticipantId()(implicit loggingContext: LoggingContext): Future[ParticipantId] =
    delegate.getParticipantId()

  override def getParties(parties: Seq[Party])(
      implicit loggingContext: LoggingContext,
  ): Future[List[domain.PartyDetails]] =
    delegate.getParties(parties)

  override def listKnownParties()(
      implicit loggingContext: LoggingContext,
  ): Future[List[domain.PartyDetails]] =
    delegate.listKnownParties()

  override def partyEntries(
      startExclusive: Option[LedgerOffset.Absolute],
  )(implicit loggingContext: LoggingContext): Source[domain.PartyEntry, NotUsed] =
    delegate.partyEntries(startExclusive)

  override def lookupConfiguration()(implicit loggingContext: LoggingContext)
    : Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    delegate.lookupConfiguration()

  override def configurationEntries(
      startExclusive: Option[LedgerOffset.Absolute],
  )(implicit loggingContext: LoggingContext)
    : Source[(LedgerOffset.Absolute, ConfigurationEntry), NotUsed] =
    delegate.configurationEntries(startExclusive)

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[v2.CommandDeduplicationResult] =
    delegate.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil)

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitter: Ref.Party,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    delegate.stopDeduplicatingCommand(commandId, submitter)

  override def prune(pruneUpToInclusive: Offset)(
      implicit loggingContext: LoggingContext): Future[Unit] =
    delegate.prune(pruneUpToInclusive)

  override def currentHealth(): HealthStatus =
    delegate.currentHealth()
}
