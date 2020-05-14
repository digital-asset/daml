// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import java.time.Instant

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.ledger.TransactionId
import com.daml.ledger.api.domain.{ApplicationId, CommandId, LedgerId, PartyDetails}
import com.daml.ledger.api.health.HealthStatus
import com.daml.ledger.api.v1.active_contracts_service.GetActiveContractsResponse
import com.daml.ledger.api.v1.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v1.transaction_service.{
  GetFlatTransactionResponse,
  GetTransactionResponse,
  GetTransactionTreesResponse,
  GetTransactionsResponse
}
import com.daml.ledger.participant.state.index.v2.{CommandDeduplicationResult, PackageDetails}
import com.daml.ledger.participant.state.v1.{Configuration, Offset}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, Party}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{AbsoluteContractId, ContractInst}
import com.daml.metrics.{Metrics, Timed}
import com.daml.platform.store.ReadOnlyLedger
import com.daml.platform.store.entries.{ConfigurationEntry, PackageLedgerEntry, PartyLedgerEntry}

import scala.concurrent.Future

class MeteredReadOnlyLedger(ledger: ReadOnlyLedger, metrics: Metrics) extends ReadOnlyLedger {

  override def ledgerId: LedgerId = ledger.ledgerId

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def flatTransactions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionsResponse), NotUsed] =
    ledger.flatTransactions(startExclusive, endInclusive, filter, verbose)

  override def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      requestingParties: Set[Party],
      verbose: Boolean,
  ): Source[(Offset, GetTransactionTreesResponse), NotUsed] =
    ledger.transactionTrees(startExclusive, endInclusive, requestingParties, verbose)

  override def ledgerEnd: Offset = ledger.ledgerEnd

  override def completions(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      applicationId: ApplicationId,
      parties: Set[Party]): Source[(Offset, CompletionStreamResponse), NotUsed] =
    ledger.completions(startExclusive, endInclusive, applicationId, parties)

  override def activeContracts(
      filter: Map[Party, Set[Identifier]],
      verbose: Boolean,
  ): (Source[GetActiveContractsResponse, NotUsed], Offset) =
    ledger.activeContracts(filter, verbose)

  override def lookupContract(
      contractId: Value.AbsoluteContractId,
      forParty: Party
  ): Future[Option[ContractInst[Value.VersionedValue[AbsoluteContractId]]]] =
    Timed.future(metrics.daml.index.lookupContract, ledger.lookupContract(contractId, forParty))

  override def lookupKey(key: GlobalKey, forParty: Party): Future[Option[AbsoluteContractId]] =
    Timed.future(metrics.daml.index.lookupKey, ledger.lookupKey(key, forParty))

  override def lookupFlatTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetFlatTransactionResponse]] =
    Timed.future(
      metrics.daml.index.lookupFlatTransactionById,
      ledger.lookupFlatTransactionById(transactionId, requestingParties),
    )

  override def lookupTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Party],
  ): Future[Option[GetTransactionResponse]] =
    Timed.future(
      metrics.daml.index.lookupTransactionTreeById,
      ledger.lookupTransactionTreeById(transactionId, requestingParties),
    )

  override def lookupMaximumLedgerTime(
      contractIds: Set[AbsoluteContractId],
  ): Future[Option[Instant]] =
    Timed.future(
      metrics.daml.index.lookupMaximumLedgerTime,
      ledger.lookupMaximumLedgerTime(contractIds))

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.getParties, ledger.getParties(parties))

  override def listKnownParties(): Future[List[PartyDetails]] =
    Timed.future(metrics.daml.index.listKnownParties, ledger.listKnownParties())

  override def partyEntries(startExclusive: Offset): Source[(Offset, PartyLedgerEntry), NotUsed] =
    ledger.partyEntries(startExclusive)

  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    Timed.future(metrics.daml.index.listLfPackages, ledger.listLfPackages())

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    Timed.future(metrics.daml.index.getLfArchive, ledger.getLfArchive(packageId))

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    Timed.future(metrics.daml.index.getLfPackage, ledger.getLfPackage(packageId))

  override def packageEntries(
      startExclusive: Offset): Source[(Offset, PackageLedgerEntry), NotUsed] =
    ledger.packageEntries(startExclusive)

  override def close(): Unit = {
    ledger.close()
  }

  override def lookupLedgerConfiguration(): Future[Option[(Offset, Configuration)]] =
    Timed.future(metrics.daml.index.lookupLedgerConfiguration, ledger.lookupLedgerConfiguration())

  override def configurationEntries(
      startExclusive: Option[Offset]): Source[(Offset, ConfigurationEntry), NotUsed] =
    ledger.configurationEntries(startExclusive)

  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] =
    Timed.future(
      metrics.daml.index.deduplicateCommand,
      ledger.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil))

  override def removeExpiredDeduplicationData(currentTime: Instant): Future[Unit] =
    Timed.future(
      metrics.daml.index.removeExpiredDeduplicationData,
      ledger.removeExpiredDeduplicationData(currentTime))

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitter: Ref.Party,
  ): Future[Unit] =
    Timed.future(
      metrics.daml.index.stopDeduplicatingCommand,
      ledger.stopDeduplicatingCommand(commandId, submitter))
}

object MeteredReadOnlyLedger {
  def apply(ledger: ReadOnlyLedger, metrics: Metrics): ReadOnlyLedger =
    new MeteredReadOnlyLedger(ledger, metrics)
}
