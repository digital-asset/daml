// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.CommandDeduplicationResult
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.lf.archive.Decode
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, Party}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Node
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
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
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.store.dao.LedgerReadDao
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

abstract class BaseLedger(
    val ledgerId: LedgerId,
    headAtInitialization: Offset,
    ledgerDao: LedgerReadDao)
    extends ReadOnlyLedger {

  implicit private val DEC: ExecutionContext = DirectExecutionContext

  protected final val dispatcher: Dispatcher[Offset] = Dispatcher[Offset](
    "sql-ledger",
    Offset.begin,
    headAtInitialization
  )

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupKey(key: Node.GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    ledgerDao.lookupKey(key, forParty)

  override def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionsResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.begin),
      RangeSource(ledgerDao.transactionsReader.getFlatTransactions(_, _, filter, verbose)),
      endInclusive
    )

  override def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Party],
      verbose: Boolean): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.begin),
      RangeSource(
        ledgerDao.transactionsReader.getTransactionTrees(_, _, requestingParties, verbose)),
      endInclusive
    )

  override def ledgerEnd: Offset = dispatcher.getHead()

  override def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Party]): Source[(Offset, CompletionStreamResponse), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.begin),
      RangeSource(ledgerDao.completions.getCommandCompletions(_, _, applicationId.unwrap, parties)),
      endInclusive
    )

  override def activeContracts(
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  ): (Source[GetActiveContractsResponse, NotUsed], Offset) = {
    val activeAt = ledgerEnd
    (ledgerDao.transactionsReader.getActiveContracts(activeAt, filter, verbose), activeAt)
  }

  override def lookupContract(
      contractId: AbsoluteContractId,
      forParty: Party
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    ledgerDao.lookupActiveOrDivulgedContract(contractId, forParty)

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]] =
    ledgerDao.transactionsReader.lookupFlatTransactionById(transactionId, requestingParties)

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]] =
    ledgerDao.transactionsReader.lookupTransactionTreeById(transactionId, requestingParties)

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]] =
    ledgerDao.lookupMaximumLedgerTime(contractIds)

  override def getParties(parties: Seq[Party]): Future[List[domain.PartyDetails]] =
    ledgerDao.getParties(parties)

  override def listKnownParties(): Future[List[domain.PartyDetails]] =
    ledgerDao.listKnownParties()

  override def partyEntries(startExclusive: Offset): Source[(Offset, PartyLedgerEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getPartyEntries))

  override def listLfPackages(): Future[Map[PackageId, v2.PackageDetails]] =
    ledgerDao.listLfPackages

  override def getLfArchive(packageId: PackageId): Future[Option[DamlLf.Archive]] =
    ledgerDao.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    ledgerDao
      .getLfArchive(packageId)
      .flatMap(archiveO =>
        Future.fromTry(Try(archiveO.map(archive => Decode.decodeArchive(archive)._2))))(DEC)

  override def packageEntries(
      startExclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] =
    dispatcher.startingAt(startExclusive, RangeSource(ledgerDao.getPackageEntries))

  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    ledgerDao.lookupLedgerConfiguration()

  override def configurationEntries(
      startExclusive: Option[Offset]): Source[(Offset, ConfigurationEntry), NotUsed] =
    dispatcher.startingAt(
      startExclusive.getOrElse(Offset.begin),
      RangeSource(ledgerDao.getConfigurationEntries))

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    ledgerDao.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil)

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    ledgerDao.removeExpiredDeduplicationData(currentTime)

  override def stopDeduplicatingCommand(commandId: CommandId, submitter: Party): Future[Unit] =
    ledgerDao.stopDeduplicatingCommand(commandId, submitter)

  override def close(): Unit = {
    dispatcher.close()
  }
}
