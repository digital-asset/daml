// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.api.domain.{ApplicationId, CommandId, LedgerId, PartyDetails}
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
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.lf.data.Ref
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.ReadOnlyLedger
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.Future

private[platform] class MeteredReadOnlyLedger(ledger: ReadOnlyLedger, metrics: Metrics)
    extends ReadOnlyLedger {
  private val logger = ContextualizedLogger.get(this.getClass)

  override def ledgerId: LedgerId = ledger.ledgerId

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] =
    ledger.flatTransactions(startExclusive, endInclusive, filter, verbose)

  override def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Ref.Party],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    ledger.transactionTrees(startExclusive, endInclusive, requestingParties, verbose)

  override def ledgerEnd()(implicit loggingContext: LoggingContext): Offset = ledger.ledgerEnd()

  override def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[(Offset, CompletionStreamResponse), NotUsed] =
    ledger.completions(startExclusive, endInclusive, applicationId, parties)

  override def activeContracts(
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit
      loggingContext: LoggingContext
  ): (Source[GetActiveContractsResponse, NotUsed], Offset) =
    ledger.activeContracts(filter, verbose)

  override def lookupContract(
      contractId: ContractId,
      forParties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[VersionedContractInstance]] =
    Timed.future(metrics.daml.index.lookupContract, ledger.lookupContract(contractId, forParties))

  override def lookupKey(key: GlobalKey, forParties: Set[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    Timed.future(metrics.daml.index.lookupKey, ledger.lookupKey(key, forParties))

  override def lookupFlatTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    Timed.future(
      metrics.daml.index.lookupFlatTransactionById,
      ledger.lookupFlatTransactionById(transactionId, requestingParties),
    )

  override def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    Timed.future(
      metrics.daml.index.lookupTransactionTreeById,
      ledger.lookupTransactionTreeById(transactionId, requestingParties),
    )

  override def lookupMaximumLedgerTime(
      contractIds: Set[ContractId]
  )(implicit loggingContext: LoggingContext): Future[Option[Timestamp]] =
    Timed.future(
      metrics.daml.index.lookupMaximumLedgerTime,
      ledger.lookupMaximumLedgerTime(contractIds),
    )

  override def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.getParties, ledger.getParties(parties))

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.listKnownParties, ledger.listKnownParties())

  override def partyEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, PartyLedgerEntry), NotUsed] = {
    logger.info("MeteredReadOnlyLedger.partyEntries")
    ledger.partyEntries(startExclusive)
  }

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, PackageDetails]] =
    Timed.future(metrics.daml.index.listLfPackages, ledger.listLfPackages())

  override def getLfArchive(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Archive]] =
    Timed.future(metrics.daml.index.getLfArchive, ledger.getLfArchive(packageId))

  override def getLfPackage(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Ast.Package]] =
    Timed.future(metrics.daml.index.getLfPackage, ledger.getLfPackage(packageId))

  override def packageEntries(
      startExclusive: Offset
  )(implicit loggingContext: LoggingContext): Source[(Offset, PackageLedgerEntry), NotUsed] =
    ledger.packageEntries(startExclusive)

  override def close(): Unit = {
    ledger.close()
  }

  override def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]] =
    Timed.future(metrics.daml.index.lookupLedgerConfiguration, ledger.lookupLedgerConfiguration())

  override def configurationEntries(
      startExclusive: Offset
  )(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed] =
    ledger.configurationEntries(startExclusive)

  override def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    Timed.future(
      metrics.daml.index.deduplicateCommand,
      ledger.deduplicateCommand(commandId, submitters, submittedAt, deduplicateUntil),
    )

  override def removeExpiredDeduplicationData(currentTime: Timestamp)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    Timed.future(
      metrics.daml.index.removeExpiredDeduplicationData,
      ledger.removeExpiredDeduplicationData(currentTime),
    )

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    Timed.future(
      metrics.daml.index.stopDeduplicatingCommand,
      ledger.stopDeduplicatingCommand(commandId, submitters),
    )

  override def prune(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    Timed.future(
      metrics.daml.index.prune,
      ledger.prune(pruneUpToInclusive, pruneAllDivulgedContracts),
    )
}

private[platform] object MeteredReadOnlyLedger {
  def apply(ledger: ReadOnlyLedger, metrics: Metrics): ReadOnlyLedger =
    new MeteredReadOnlyLedger(ledger, metrics)
}
