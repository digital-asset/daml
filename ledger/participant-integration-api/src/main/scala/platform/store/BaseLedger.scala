// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.dec.DirectExecutionContext
import com.daml.ledger.TransactionId
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{ApplicationId, CommandId, LedgerId}
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
import com.daml.ledger.participant.state.index.v2.CommandDeduplicationResult
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, Party}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.logging.LoggingContext
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.store.dao.LedgerReadDao
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

private[platform] abstract class BaseLedger(
    val ledgerId: LedgerId,
    ledgerDao: LedgerReadDao,
    dispatcher: Dispatcher[Offset],
) extends ReadOnlyLedger {

  implicit private val DEC: ExecutionContext = DirectExecutionContext

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupKey(key: GlobalKey, forParty: Party)(
      implicit loggingContext: LoggingContext,
  ): Future[Option[ContractId]] =
    ledgerDao.lookupKey(key, forParty)

  override def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.beforeBegin),
      RangeSource(ledgerDao.transactionsReader.getFlatTransactions(_, _, filter, verbose)),
      endInclusive
    )

  override def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Party],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext)
    : Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.beforeBegin),
      RangeSource(
        ledgerDao.transactionsReader.getTransactionTrees(_, _, requestingParties, verbose)),
      endInclusive
    )

  override def ledgerEnd()(implicit loggingContext: LoggingContext): Offset = dispatcher.getHead()

  override def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit loggingContext: LoggingContext): Source[(Offset, CompletionStreamResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.beforeBegin),
      RangeSource(ledgerDao.completions.getCommandCompletions(_, _, applicationId.unwrap, parties)),
      endInclusive
    )

  override def activeContracts(
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext)
    : (Source[GetActiveContractsResponse, NotUsed], Offset) = {
    val activeAt = ledgerEnd()
    (ledgerDao.transactionsReader.getActiveContracts(activeAt, filter, verbose), activeAt)
  }

  override def lookupContract(
      contractId: ContractId,
      forParty: Party
  )(implicit loggingContext: LoggingContext)
    : Future[Option[ContractInst[Value.VersionedValue[ContractId]]]] =
    ledgerDao.lookupActiveOrDivulgedContract(contractId, forParty)

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    ledgerDao.transactionsReader.lookupFlatTransactionById(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    ledgerDao.transactionsReader.lookupTransactionTreeById(transactionId, requestingParties)

  override def lookupMaximumLedgerTime(
      contractIds: Set[ContractId],
  )(implicit loggingContext: LoggingContext): Future[Option[Instant]] =
    ledgerDao.lookupMaximumLedgerTime(contractIds)

  override def getParties(parties: Seq[Party])(
      implicit loggingContext: LoggingContext,
  ): Future[List[domain.PartyDetails]] =
    ledgerDao.getParties(parties)

  override def listKnownParties()(
      implicit loggingContext: LoggingContext,
  ): Future[List[domain.PartyDetails]] =
    ledgerDao.listKnownParties()

  override def partyEntries(startExclusive: Offset)(
      implicit loggingContext: LoggingContext,
  ): Source[(Offset, PartyLedgerEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getPartyEntries))

  override def listLfPackages()(
      implicit loggingContext: LoggingContext,
  ): Future[Map[PackageId, v2.PackageDetails]] =
    ledgerDao.listLfPackages

  override def getLfArchive(packageId: PackageId)(
      implicit loggingContext: LoggingContext,
  ): Future[Option[DamlLf.Archive]] =
    ledgerDao.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId)(
      implicit loggingContext: LoggingContext,
  ): Future[Option[Ast.Package]] =
    ledgerDao
      .getLfArchive(packageId)
      .flatMap(archiveO =>
        Future.fromTry(Try(archiveO.map(archive => Decode.decodeArchive(archive)._2))))(DEC)

  override def packageEntries(startExclusive: Offset)(
      implicit loggingContext: LoggingContext,
  ): Source[(Offset, PackageLedgerEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getPackageEntries))

  override def lookupLedgerConfiguration()(
      implicit loggingContext: LoggingContext,
  ): Future[Option[(Offset, Configuration)]] =
    ledgerDao.lookupLedgerConfiguration()

  override def configurationEntries(startExclusive: Offset)(
      implicit loggingContext: LoggingContext,
  ): Source[(Offset, ConfigurationEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getConfigurationEntries))

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    ledgerDao.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil)

  override def removeExpiredDeduplicationData(currentTime: Instant)(
      implicit loggingContext: LoggingContext,
  ): Future[Unit] =
    ledgerDao.removeExpiredDeduplicationData(currentTime)

  override def stopDeduplicatingCommand(commandId: CommandId, submitter: Party)(
      implicit loggingContext: LoggingContext,
  ): Future[Unit] =
    ledgerDao.stopDeduplicatingCommand(commandId, submitter)

  override def prune(pruneUpToInclusive: Offset)(
      implicit loggingContext: LoggingContext): Future[Unit] =
    ledgerDao.prune(pruneUpToInclusive)

  override def close(): Unit = ()
}
