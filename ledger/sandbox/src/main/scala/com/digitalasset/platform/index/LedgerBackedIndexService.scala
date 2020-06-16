// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import java.time.Instant

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.Source
import com.daml.ledger.participant.state.index.v2._
import com.daml.ledger.participant.state.v1.{Configuration, Offset, ParticipantId}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, Party}
import com.daml.lf.language.Ast
import com.daml.lf.transaction.Node.GlobalKey
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, ContractInst}
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.dec.{DirectExecutionContext => DEC}
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.{
  ApplicationId,
  CommandId,
  LedgerId,
  LedgerOffset,
  PackageEntry,
  PartyDetails,
  PartyEntry,
  TransactionFilter,
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
import com.daml.logging.ThreadLogger
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.entries.PartyLedgerEntry
import com.daml.platform.store.ReadOnlyLedger
import com.daml.platform.ApiOffset
import com.daml.platform.ApiOffset.ApiOffsetConverter
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.Future

abstract class LedgerBackedIndexService(
    ledger: ReadOnlyLedger,
    participantId: ParticipantId,
)(implicit mat: Materializer)
    extends IndexService {
  override def getLedgerId(): Future[LedgerId] = Future.successful(ledger.ledgerId)

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean,
  ): Source[GetActiveContractsResponse, NotUsed] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.getActiveContracts")
    val (acs, ledgerEnd) = ledger
      .activeContracts(convertFilter(filter), verbose)
    acs
      .concat(Source.single(GetActiveContractsResponse(offset = ApiOffset.toApiString(ledgerEnd))))
      .map(ThreadLogger.traceStreamElement(
        "LedgerBackedIndexService.getActiveContracts (stream element)"))
  }

  override def transactionTrees(
      startExclusive: LedgerOffset,
      endInclusive: Option[LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetTransactionTreesResponse, NotUsed] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.transactionTrees")
    between(startExclusive, endInclusive)(
      (from, to) =>
        ledger
          .transactionTrees(
            startExclusive = from,
            endInclusive = to,
            requestingParties = filter.filtersByParty.keySet,
            verbose = verbose,
          )
          .map(_._2)
          .map(ThreadLogger.traceStreamElement(
            "LedgerBackedIndexService.transactionTrees (stream element)"))
    )
  }

  override def transactions(
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  ): Source[GetTransactionsResponse, NotUsed] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.transactions")
    between(startExclusive, endInclusive)(
      (from, to) =>
        ledger
          .flatTransactions(
            startExclusive = from,
            endInclusive = to,
            filter = convertFilter(filter),
            verbose = verbose,
          )
          .map(_._2)
          .map(ThreadLogger.traceStreamElement(
            "LedgerBackedIndexService.transactions (stream element)"))
    )
  }

  // Returns a function that memoizes the current end
  // Can be used directly or shared throughout a request processing
  private def convertOffset: LedgerOffset => Source[Offset, NotUsed] = {
    lazy val currentEnd: Offset = ledger.ledgerEnd
    domainOffset: LedgerOffset =>
      domainOffset match {
        case LedgerOffset.LedgerBegin => Source.single(Offset.beforeBegin)
        case LedgerOffset.LedgerEnd => Source.single(currentEnd)
        case LedgerOffset.Absolute(offset) =>
          ApiOffset.fromString(offset).fold(Source.failed, off => Source.single(off))
      }
  }

  private def between[A](
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset],
  )(f: (Option[Offset], Option[Offset]) => Source[A, NotUsed]): Source[A, NotUsed] = {
    val convert = convertOffset
    convert(startExclusive).flatMapConcat { begin =>
      endInclusive
        .map(convert(_).map(Some(_)))
        .getOrElse(Source.single(None))
        .flatMapConcat {
          case Some(`begin`) =>
            Source.empty
          case Some(end) if begin > end =>
            Source.failed(
              ErrorFactories.invalidArgument(
                s"End offset ${end.toApiString} is before Begin offset ${begin.toApiString}."))
          case endOpt: Option[Offset] =>
            f(Some(begin), endOpt)
        }
    }
  }

  private def convertFilter(filter: TransactionFilter): Map[Party, Set[Identifier]] =
    filter.filtersByParty.map {
      case (party, filters) =>
        party -> filters.inclusive.fold(Set.empty[Identifier])(_.templateIds)
    }

  override def currentLedgerEnd(): Future[LedgerOffset.Absolute] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.currentLedgerEnd")
    Future.successful(toAbsolute(ledger.ledgerEnd))
  }

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  ): Future[Option[GetFlatTransactionResponse]] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.lookupFlatTransactionById")
    ledger.lookupFlatTransactionById(transactionId.unwrap, requestingParties)
  }

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  ): Future[Option[GetTransactionResponse]] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.lookupTransactionTreeById")
    ledger.lookupTransactionTreeById(transactionId.unwrap, requestingParties)
  }

  def toAbsolute(offset: Offset): LedgerOffset.Absolute =
    LedgerOffset.Absolute(offset.toApiString)

  override def getCompletions(
      startExclusive: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party]
  ): Source[CompletionStreamResponse, NotUsed] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.getCompletions")
    convertOffset(startExclusive)
      .flatMapConcat { beginOpt =>
        ledger.completions(Some(beginOpt), None, applicationId, parties).map(_._2)
      }
      .map(
        ThreadLogger.traceStreamElement("LedgerBackedIndexService.getCompletions (stream element)"))
  }

  // IndexPackagesService
  override def listLfPackages(): Future[Map[PackageId, PackageDetails]] =
    ledger.listLfPackages()

  override def getLfArchive(packageId: PackageId): Future[Option[Archive]] =
    ledger.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId): Future[Option[Ast.Package]] =
    ledger.getLfPackage(packageId)

  override def lookupActiveContract(
      submitter: Ref.Party,
      contractId: ContractId,
  ): Future[Option[ContractInst[Value.VersionedValue[ContractId]]]] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.lookupActiveContract")
    ledger.lookupContract(contractId, submitter)
  }

  override def lookupMaximumLedgerTime(ids: Set[ContractId]): Future[Option[Instant]] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.lookupMaximumLedgerTime")
    ledger.lookupMaximumLedgerTime(ids)
  }

  override def lookupContractKey(
      submitter: Party,
      key: GlobalKey,
  ): Future[Option[ContractId]] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.lookupContractKey")
    ledger.lookupKey(key, submitter)
  }

  // PartyManagementService
  override def getParticipantId(): Future[ParticipantId] =
    Future.successful(participantId)

  override def getParties(parties: Seq[Party]): Future[List[PartyDetails]] =
    ledger.getParties(parties)

  override def listKnownParties(): Future[List[PartyDetails]] =
    ledger.listKnownParties()

  override def partyEntries(startExclusive: LedgerOffset.Absolute): Source[PartyEntry, NotUsed] = {
    Source
      .future(Future.fromTry(ApiOffset.fromString(startExclusive.value)))
      .flatMapConcat(ledger.partyEntries)
      .map {
        case (_, PartyLedgerEntry.AllocationRejected(subId, participantId, _, reason)) =>
          PartyEntry.AllocationRejected(subId, domain.ParticipantId(participantId), reason)
        case (_, PartyLedgerEntry.AllocationAccepted(subId, participantId, _, details)) =>
          PartyEntry.AllocationAccepted(subId, domain.ParticipantId(participantId), details)
      }
  }

  override def packageEntries(
      startExclusive: LedgerOffset.Absolute): Source[PackageEntry, NotUsed] =
    Source
      .future(Future.fromTry(ApiOffset.fromString(startExclusive.value)))
      .flatMapConcat(ledger.packageEntries)
      .map(_._2.toDomain)

  /** Looks up the current configuration, if set, and the offset from which
    * to subscribe to further configuration changes.
    * The offset is internal and not exposed over Ledger API.
    */
  override def lookupConfiguration(): Future[Option[(LedgerOffset.Absolute, Configuration)]] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.lookupConfiguration")
    ledger
      .lookupLedgerConfiguration()
      .map(_.map { case (offset, config) => (toAbsolute(offset), config) })(DEC)
  }

  /** Retrieve configuration entries. */
  override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])
    : Source[(domain.LedgerOffset.Absolute, domain.ConfigurationEntry), NotUsed] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.configurationEntries")
    Source
      .future(
        startExclusive
          .map(off => Future.fromTry(ApiOffset.fromString(off.value).map(Some(_))))
          .getOrElse(Future.successful(None)))
      .flatMapConcat(ledger.configurationEntries(_).map {
        case (offset, config) => toAbsolute(offset) -> config.toDomain
      })
      .map(ThreadLogger.traceStreamElement(
        "LedgerBackedIndexService.configurationEntries (stream element)"))
  }

  /** Deduplicate commands */
  override def deduplicateCommand(
      commandId: CommandId,
      submitter: Ref.Party,
      submittedAt: Instant,
      deduplicateUntil: Instant): Future[CommandDeduplicationResult] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.deduplicateCommand")
    ledger.deduplicateCommand(commandId, submitter, submittedAt, deduplicateUntil)
  }

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitter: Ref.Party,
  ): Future[Unit] = {
    ThreadLogger.traceThread("LedgerBackedIndexService.stopDeduplicatingCommand")
    ledger.stopDeduplicatingCommand(commandId, submitter)
  }
}
