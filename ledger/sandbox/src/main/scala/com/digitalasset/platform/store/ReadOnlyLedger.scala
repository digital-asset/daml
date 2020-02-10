// Copyright (c) 2020 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.store

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2.{CommandSubmissionResult, PackageDetails}
import com.daml.ledger.participant.state.v1.Configuration
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{PackageId, Party}
import com.digitalasset.daml.lf.language.Ast
import com.digitalasset.daml.lf.transaction.Node.GlobalKey
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.digitalasset.daml_lf_dev.DamlLf.Archive
import com.digitalasset.ledger.api.domain.{ApplicationId, LedgerId, PartyDetails, TransactionId}
import com.digitalasset.ledger.api.health.ReportsHealth
import com.digitalasset.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.digitalasset.platform.participant.util.EventFilter.TemplateAwareFilter
import com.digitalasset.platform.store.entries.{
  CommandDeduplicationEntry,
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
      beginInclusive: Option[Long],
      endExclusive: Option[Long]): Source[(Long, LedgerEntry), NotUsed]

  def ledgerEnd: Long

  def completions(
      beginInclusive: Option[Long],
      endExclusive: Option[Long],
      applicationId: ApplicationId,
      parties: Set[Ref.Party]): Source[(Long, CompletionStreamResponse), NotUsed]

  def snapshot(filter: TemplateAwareFilter): Future[LedgerSnapshot]

  def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]]

  def lookupKey(key: GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]]

  def lookupTransaction(
      transactionId: TransactionId): Future[Option[(Long, LedgerEntry.Transaction)]]

  // Party management
  def parties: Future[List[PartyDetails]]

  def partyEntries(beginOffset: Long): Source[(Long, PartyLedgerEntry), NotUsed]

  // Package management
  def listLfPackages(): Future[Map[PackageId, PackageDetails]]

  def getLfArchive(packageId: PackageId): Future[Option[Archive]]

  def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]]

  def packageEntries(beginOffset: Long): Source[(Long, PackageLedgerEntry), NotUsed]

  // Configuration management
  def lookupLedgerConfiguration(): Future[Option[(Long, Configuration)]]
  def configurationEntries(
      startInclusive: Option[Long]): Source[(Long, ConfigurationEntry), NotUsed]

  /** Deduplicates commands.
    * Returns None if this is the first time the command is submitted
    * Returns Some(entry) if the command was submitted before */
  def deduplicateCommand(
      deduplicationKey: String,
      submittedAt: Instant,
      ttl: Instant): Future[Option[CommandDeduplicationEntry]]

  def updateCommandResult(
      deduplicationKey: String,
      submittedAt: Instant,
      result: CommandSubmissionResult): Future[Unit]
}
