// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf.Archive
import com.daml.dec.{DirectExecutionContext => DEC}
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.ledger.api.domain
import com.daml.ledger.api.domain.ConfigurationEntry.Accepted
import com.daml.ledger.api.domain.{
  ApplicationId,
  CommandId,
  LedgerId,
  LedgerOffset,
  PackageEntry,
  PartyDetails,
  PartyEntry,
  TransactionFilter,
  TransactionId,
}
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
import com.daml.ledger.participant.state.index.v2._
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{Identifier, PackageId, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.language.Ast
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.ApiOffset
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.server.api.validation.ErrorFactories
import com.daml.platform.store.ReadOnlyLedger
import com.daml.platform.store.entries.PartyLedgerEntry
import com.daml.telemetry.{SpanAttribute, Spans}
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.Future

private[platform] final class LedgerBackedIndexService(
    ledger: ReadOnlyLedger,
    participantId: Ref.ParticipantId,
    errorFactories: ErrorFactories,
) extends IndexService {
  private val logger = ContextualizedLogger.get(getClass)

  override def getLedgerId()(implicit loggingContext: LoggingContext): Future[LedgerId] =
    Future.successful(ledger.ledgerId)

  override def currentHealth(): HealthStatus = ledger.currentHealth()

  override def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] = {
    val (acs, ledgerEnd) = ledger
      .activeContracts(convertFilter(filter), verbose)
    acs.concat(Source.single(GetActiveContractsResponse(offset = ApiOffset.toApiString(ledgerEnd))))
  }

  override def transactionTrees(
      startExclusive: LedgerOffset,
      endInclusive: Option[LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionTreesResponse, NotUsed] =
    between(startExclusive, endInclusive)((from, to) => {
      from.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toHexString)
      )
      to.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toHexString)
      )
      ledger
        .transactionTrees(
          startExclusive = from,
          endInclusive = to,
          requestingParties = filter.filtersByParty.keySet,
          verbose = verbose,
        )
        .map(_._2)
    })

  override def transactions(
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed] =
    between(startExclusive, endInclusive)((from, to) => {
      from.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toHexString)
      )
      to.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toHexString)
      )
      ledger
        .flatTransactions(
          startExclusive = from,
          endInclusive = to,
          filter = convertFilter(filter),
          verbose = verbose,
        )
        .map(_._2)
    })

  // Returns a function that memoizes the current end
  // Can be used directly or shared throughout a request processing
  private def convertOffset(implicit
      loggingContext: LoggingContext
  ): LedgerOffset => Source[Offset, NotUsed] = {
    case LedgerOffset.LedgerBegin => Source.single(Offset.beforeBegin)
    case LedgerOffset.LedgerEnd => Source.single(ledger.ledgerEnd())
    case LedgerOffset.Absolute(offset) =>
      ApiOffset.fromString(offset).fold(Source.failed, off => Source.single(off))
  }

  private def between[A](
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset],
  )(f: (Option[Offset], Option[Offset]) => Source[A, NotUsed])(implicit
      loggingContext: LoggingContext
  ): Source[A, NotUsed] = {
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
              errorFactories.offsetOutOfRange(None)(
                s"End offset ${end.toApiString} is before Begin offset ${begin.toApiString}."
              )(new DamlContextualizedErrorLogger(logger, loggingContext, None))
            )
          case endOpt: Option[Offset] =>
            f(Some(begin), endOpt)
        }
    }
  }

  private def convertFilter(filter: TransactionFilter): Map[Party, Set[Identifier]] =
    filter.filtersByParty.map { case (party, filters) =>
      party -> filters.inclusive.fold(Set.empty[Identifier])(_.templateIds)
    }

  override def currentLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[LedgerOffset.Absolute] = {
    val offset =
      if (ledger.ledgerEnd() == Offset.beforeBegin) ApiOffset.begin
      else ledger.ledgerEnd()
    Future.successful(toAbsolute(offset))
  }

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    ledger.lookupFlatTransactionById(transactionId.unwrap, requestingParties)

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    ledger.lookupTransactionTreeById(transactionId.unwrap, requestingParties)

  def toAbsolute(offset: Offset): LedgerOffset.Absolute =
    LedgerOffset.Absolute(offset.toApiString)

  override def getCompletions(
      startExclusive: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed] = {
    val convert = convertOffset
    convert(startExclusive).flatMapConcat { beginOpt =>
      ledger.completions(Some(beginOpt), None, applicationId, parties).map(_._2)
    }
  }

  override def getCompletions(
      startExclusive: LedgerOffset,
      endInclusive: LedgerOffset,
      applicationId: ApplicationId,
      parties: Set[Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed] = {
    between(startExclusive, Some(endInclusive))((start, end) =>
      ledger.completions(start, end, applicationId, parties).map(_._2)
    )
  }

  // IndexPackagesService
  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[PackageId, PackageDetails]] =
    ledger.listLfPackages()

  override def getLfArchive(packageId: PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Archive]] =
    ledger.getLfArchive(packageId)

  override def getLfPackage(packageId: PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[Ast.Package]] =
    ledger.getLfPackage(packageId)

  override def lookupActiveContract(
      readers: Set[Party],
      contractId: ContractId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[VersionedContractInstance]] =
    ledger.lookupContract(contractId, readers)

  override def lookupMaximumLedgerTime(ids: Set[ContractId])(implicit
      loggingContext: LoggingContext
  ): Future[Option[Timestamp]] =
    ledger.lookupMaximumLedgerTime(ids)

  override def lookupContractKey(
      readers: Set[Party],
      key: GlobalKey,
  )(implicit loggingContext: LoggingContext): Future[Option[ContractId]] =
    ledger.lookupKey(key, readers)

  // PartyManagementService
  override def getParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Ref.ParticipantId] =
    Future.successful(participantId)

  override def getParties(parties: Seq[Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    ledger.getParties(parties)

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[PartyDetails]] =
    ledger.listKnownParties()

  override def partyEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContext): Source[PartyEntry, NotUsed] = {
    Source
      .future(concreteOffset(startExclusive))
      .flatMapConcat(ledger.partyEntries)
      .map {
        case (_, PartyLedgerEntry.AllocationRejected(subId, _, reason)) =>
          PartyEntry.AllocationRejected(subId, reason)
        case (_, PartyLedgerEntry.AllocationAccepted(subId, _, details)) =>
          PartyEntry.AllocationAccepted(subId, details)
      }
  }

  override def packageEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContext): Source[PackageEntry, NotUsed] =
    Source
      .future(concreteOffset(startExclusive))
      .flatMapConcat(ledger.packageEntries)
      .map(_._2.toDomain)

  /** Looks up the current configuration, if set, and the offset from which
    * to subscribe to further configuration changes.
    * The offset is internal and not exposed over Ledger API.
    */
  override def lookupConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    ledger
      .lookupLedgerConfiguration()
      .map(_.map { case (offset, config) => (toAbsolute(offset), config) })(DEC)

  /** Looks up the current configuration, if set, and continues to stream configuration changes.
    */
  override def getLedgerConfiguration()(implicit
      loggingContext: LoggingContext
  ): Source[LedgerConfiguration, NotUsed] = {
    Source
      .future(lookupConfiguration())
      .flatMapConcat { optResult =>
        val offset = optResult.map(_._1)
        val foundConfig = optResult.map(_._2)

        val initialConfig = Source(foundConfig.toList)
        val configStream = configurationEntries(offset).collect {
          case (_, Accepted(_, configuration)) => configuration
        }
        initialConfig
          .concat(configStream)
          .map(cfg => LedgerConfiguration(cfg.maxDeduplicationTime))
      }
  }

  /** Retrieve configuration entries. */
  override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
      loggingContext: LoggingContext
  ): Source[(domain.LedgerOffset.Absolute, domain.ConfigurationEntry), NotUsed] =
    Source
      .future(concreteOffset(startExclusive))
      .flatMapConcat(ledger.configurationEntries(_).map { case (offset, config) =>
        toAbsolute(offset) -> config.toDomain
      })

  /** Deduplicate commands */
  override def deduplicateCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
      submittedAt: Timestamp,
      deduplicateUntil: Timestamp,
  )(implicit loggingContext: LoggingContext): Future[CommandDeduplicationResult] =
    ledger.deduplicateCommand(commandId, submitters, submittedAt, deduplicateUntil)

  override def stopDeduplicatingCommand(
      commandId: CommandId,
      submitters: List[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Unit] =
    ledger.stopDeduplicatingCommand(commandId, submitters)

  /** Participant pruning command */
  override def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] =
    ledger.prune(pruneUpToInclusive, pruneAllDivulgedContracts)

  private def concreteOffset(startExclusive: Option[LedgerOffset.Absolute]): Future[Offset] =
    startExclusive
      .map(off => Future.fromTry(ApiOffset.fromString(off.value)))
      .getOrElse(Future.successful(Offset.beforeBegin))

}
