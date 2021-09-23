// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf.ArchiveOuterClass.Archive
import com.daml.ledger.api.domain.{ApplicationId, CommandId, LedgerId, PartyDetails}
import com.daml.ledger.api.health.ReportsHealth
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
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.logging.LoggingContext
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.Future

/** Defines all the functionalities a Ledger needs to provide */
private[platform] trait ReadOnlyLedger extends ReportsHealth with AutoCloseable {

  def ledgerId: LedgerId

  def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionsResponse), NotUsed]

  def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Ref.Party],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[(Offset, GetTransactionTreesResponse), NotUsed]

  def ledgerEnd()(implicit loggingContext: LoggingContext): Offset

  def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[(Offset, CompletionStreamResponse), NotUsed]

  def activeContracts(
      filter: Map[Ref.Party, Set[Ref.Identifier]],
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): (Source[GetActiveContractsResponse, NotUsed], Offset)

  def lookupContract(
      contractId: Value.ContractId,
      forParties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractInst[Value.VersionedValue]]]

  def lookupMaximumLedgerTime(
      contractIds: Set[ContractId]
  )(implicit loggingContext: LoggingContext): Future[Option[Instant]]

  def lookupKey(key: GlobalKey, forParties: Set[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]]

  def lookupFlatTransactionById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]]

  def lookupTransactionTreeById(
      transactionId: Ref.TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]]

  // Party management
  def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]]

  def listKnownParties()(implicit loggingContext: LoggingContext): Future[List[PartyDetails]]

  def partyEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, PartyLedgerEntry), NotUsed]

  // Package management
  def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, PackageDetails]]

  def getLfArchive(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Archive]]

  def getLfPackage(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Ast.Package]]

  def packageEntries(startExclusive: Offset)(implicit
      loggingContext: LoggingContext
  ): Source[(Offset, PackageLedgerEntry), NotUsed]

  // Configuration management
  def lookupLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(Offset, Configuration)]]

  def configurationEntries(
      startExclusive: Offset
  )(implicit loggingContext: LoggingContext): Source[(Offset, ConfigurationEntry), NotUsed]

  /** Deduplicates commands.
    * Returns CommandDeduplicationNew if this is the first time the command is submitted
    * Returns CommandDeduplicationDuplicate if the command was submitted before
    *
    * Note: The deduplication cache is used by the submission service,
    * it does not modify any on-ledger data.
    */
  def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Instant,
      deduplicateUntil: Instant,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult]

  /** Stops deduplicating the given command.
    *
    * Note: The deduplication cache is used by the submission service,
    * it does not modify any on-ledger data.
    */
  def stopDeduplicatingCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit]

  /** Remove all expired deduplication entries. This method has to be called
    * periodically to ensure that the deduplication cache does not grow unboundedly.
    *
    * @param currentTime The current time. This should use the same source of time as
    *                    the `deduplicateUntil` argument of [[deduplicateCommand]].
    * @return when DAO has finished removing expired entries. Clients do not
    *         need to wait for the operation to finish, it is safe to concurrently
    *         call deduplicateCommand().
    *
    * Note: The deduplication cache is used by the submission service,
    * it does not modify any on-ledger data.
    */
  def removeExpiredDeduplicationData(
      currentTime: Instant
  )(implicit loggingContext: LoggingContext): Future[Unit]

  /** Performs participant ledger pruning up to and including the specified offset.
    */
  def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit]
}
