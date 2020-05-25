// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, Party}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.TransactionId
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
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.Future

/** Defines all the functionalities a Ledger needs to provide */
trait ReadOnlyLedger extends ReportsHealth with AutoCloseable {

  def ledgerId: LedgerId

  def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionsResponse), NotUsed]

  def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Party],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed]

  def ledgerEnd: Offset

  def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Ref.Party]): Source[(Offset, CompletionStreamResponse), NotUsed]

  def activeContracts(
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  ): (Source[GetActiveContractsResponse, NotUsed], Offset)

  def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]]

  def lookupKey(key: GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]]

  def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]]

  def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]]

  // Party management
  def getParties(parties: Seq[Party]): Future[List[PartyDetails]]

  def listKnownParties(): Future[List[PartyDetails]]

  def partyEntries(startExclusive: Offset): Source[(Offset, PartyLedgerEntry), NotUsed]

  // Package management
  def listLfPackages(): Future[Map[PackageId, PackageDetails]]

  def getLfArchive(packageId: PackageId): Future[Option[Archive]]

  def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]]

  def packageEntries(startExclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed]

  // Configuration management
  def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]]
  def configurationEntries(
      startExclusive: Option[Offset]): Source[(Offset, ConfigurationEntry), NotUsed]

  /** Deduplicates commands.
    * Returns CommandDeduplicationNew if this is the first time the command is submitted
    * Returns CommandDeduplicationDuplicate if the command was submitted before
    *
    * Note: The deduplication cache is used by the submission service,
    * it does not modify any on-ledger data.
    */
  def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult]

  /**
    * Stops deduplicating the given command.
    *
    * Note: The deduplication cache is used by the submission service,
    * it does not modify any on-ledger data.
    */
  def stopDeduplicatingCommand(
      commandId: CommandId,
      submitter: Ref.Party,
  ): Future[Unit]

  /**
    * Remove all expired deduplication entries. This method has to be called
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
      currentTime: Instant,
  ): Future[Unit]
}
