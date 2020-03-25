// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.TransactionId
import com.digitalasset.ledger.api.domain.{ApplicationId, LedgerId, PartyDetails, TransactionFilter}
import com.digitalasset.ledger.api.health.ReportsHealth
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse
}
import com.digitalasset.platform.store.entries.{
  ConfigurationEntry,
  LedgerEntry,
  PackageLedgerEntry,
  PartyLedgerEntry
}

import scala.concurrent.Future

/** Defines all the functionalities a Ledger needs to provide */
trait ReadOnlyLedger extends ReportsHealth with AutoCloseable {

  def ledgerId: LedgerId

  def ledgerEntries(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset]): Source[(Offset, LedgerEntry), NotUsed]

  def ledgerEnd: Offset

  def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Ref.Party]): Source[(Offset, CompletionStreamResponse), NotUsed]

  def snapshot(filter: TransactionFilter): Future[LedgerSnapshot]

  def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Instant]

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
    * Returns None if this is the first time the command is submitted
    * Returns Some(entry) if the command was submitted before
    *
    * Note: The deduplication cache is used by the submission service,
    * it does not modify any on-ledger data.
    */
  def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult]

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
