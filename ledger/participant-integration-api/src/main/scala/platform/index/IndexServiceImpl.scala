// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.index

import akka.NotUsed
import akka.stream.scaladsl.Source
import com.daml.daml_lf_dev.DamlLf
import com.daml.error.DamlContextualizedErrorLogger
import com.daml.error.definitions.LedgerApiErrors
import com.daml.ledger.api.{TraceIdentifiers, domain}
import com.daml.ledger.api.domain.ConfigurationEntry.Accepted
import com.daml.ledger.api.domain.{
  LedgerId,
  LedgerOffset,
  PackageEntry,
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
import com.daml.ledger.participant.state.index.v2
import com.daml.ledger.participant.state.index.v2.MeteringStore.ReportData
import com.daml.ledger.participant.state.index.v2.{
  ContractStore,
  IndexService,
  MaximumLedgerTime,
  _,
}
import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.{ApplicationId, Identifier, Party}
import com.daml.lf.data.Time.Timestamp
import com.daml.lf.transaction.GlobalKey
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.metrics.InstrumentedGraph._
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.{ApiOffset, PruneBuffers}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.store.dao.{LedgerDaoTransactionsReader, LedgerReadDao}
import com.daml.platform.store.entries.PartyLedgerEntry
import com.daml.telemetry.{Event, SpanAttribute, Spans}
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.{ExecutionContext, Future}

private[index] class IndexServiceImpl(
    val ledgerId: LedgerId,
    participantId: Ref.ParticipantId,
    ledgerDao: LedgerReadDao,
    transactionsReader: LedgerDaoTransactionsReader,
    contractStore: ContractStore,
    pruneBuffers: PruneBuffers,
    dispatcher: Dispatcher[Offset],
    metrics: Metrics,
) extends IndexService {
  // An Akka stream buffer is added at the end of all streaming queries,
  // allowing to absorb temporary downstream backpressure.
  // (e.g. when the client is temporarily slower than upstream delivery throughput)
  private val LedgerApiStreamsBufferSize = 128
  private val logger = ContextualizedLogger.get(getClass)

  override def getParticipantId()(implicit
      loggingContext: LoggingContext
  ): Future[Ref.ParticipantId] =
    Future.successful(participantId)

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupContractKey(readers: Set[Ref.Party], key: GlobalKey)(implicit
      loggingContext: LoggingContext
  ): Future[Option[ContractId]] =
    contractStore.lookupContractKey(readers, key)

  override def transactions(
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed] =
    between(startExclusive, endInclusive) { (from, to) =>
      from.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toHexString)
      )
      to.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toHexString)
      )
      dispatcher
        .startingAt(
          from.getOrElse(Offset.beforeBegin),
          RangeSource(transactionsReader.getFlatTransactions(_, _, convertFilter(filter), verbose)),
          to,
        )
        .map(_._2)
        .buffered(metrics.daml.index.flatTransactionsBufferSize, LedgerApiStreamsBufferSize)
    }.wireTap(
      _.transactions.view
        .map(transaction =>
          Event(transaction.commandId, TraceIdentifiers.fromTransaction(transaction))
        )
        .foreach(Spans.addEventToCurrentSpan)
    )

  override def transactionTrees(
      startExclusive: LedgerOffset,
      endInclusive: Option[LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionTreesResponse, NotUsed] =
    between(startExclusive, endInclusive) { (from, to) =>
      from.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toHexString)
      )
      to.foreach(offset =>
        Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toHexString)
      )
      dispatcher
        .startingAt(
          from.getOrElse(Offset.beforeBegin),
          RangeSource(
            transactionsReader.getTransactionTrees(_, _, filter.filtersByParty.keySet, verbose)
          ),
          to,
        )
        .map(_._2)
        .buffered(metrics.daml.index.transactionTreesBufferSize, LedgerApiStreamsBufferSize)
    }.wireTap(
      _.transactions.view
        .map(transaction =>
          Event(transaction.commandId, TraceIdentifiers.fromTransactionTree(transaction))
        )
        .foreach(Spans.addEventToCurrentSpan)
    )

  override def getCompletions(
      startExclusive: LedgerOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed] =
    convertOffset(startExclusive)
      .flatMapConcat { beginOpt =>
        dispatcher
          .startingAt(
            beginOpt,
            RangeSource(ledgerDao.completions.getCommandCompletions(_, _, applicationId, parties)),
            None,
          )
          .map(_._2)
      }
      .buffered(metrics.daml.index.completionsBufferSize, LedgerApiStreamsBufferSize)

  override def getCompletions(
      startExclusive: LedgerOffset,
      endInclusive: LedgerOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed] =
    between(startExclusive, Some(endInclusive)) { (start, end) =>
      dispatcher
        .startingAt(
          start.getOrElse(Offset.beforeBegin),
          RangeSource(ledgerDao.completions.getCommandCompletions(_, _, applicationId, parties)),
          end,
        )
        .map(_._2)
    }
      .buffered(metrics.daml.index.completionsBufferSize, LedgerApiStreamsBufferSize)

  override def getActiveContracts(
      filter: TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] = {
    val currentLedgerEnd = ledgerEnd()

    ledgerDao.transactionsReader
      .getActiveContracts(
        currentLedgerEnd,
        convertFilter(filter),
        verbose,
      )
      .concat(
        Source.single(GetActiveContractsResponse(offset = ApiOffset.toApiString(currentLedgerEnd)))
      )
      .buffered(metrics.daml.index.activeContractsBufferSize, LedgerApiStreamsBufferSize)
  }

  override def lookupActiveContract(
      forParties: Set[Ref.Party],
      contractId: ContractId,
  )(implicit
      loggingContext: LoggingContext
  ): Future[Option[VersionedContractInstance]] =
    contractStore.lookupActiveContract(forParties, contractId)

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    ledgerDao.transactionsReader
      .lookupFlatTransactionById(transactionId.unwrap, requestingParties)

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    ledgerDao.transactionsReader
      .lookupTransactionTreeById(transactionId.unwrap, requestingParties)

  override def lookupMaximumLedgerTimeAfterInterpretation(
      contractIds: Set[ContractId]
  )(implicit loggingContext: LoggingContext): Future[MaximumLedgerTime] =
    contractStore.lookupMaximumLedgerTimeAfterInterpretation(contractIds)

  override def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContext
  ): Future[List[domain.PartyDetails]] =
    ledgerDao.getParties(parties)

  override def listKnownParties()(implicit
      loggingContext: LoggingContext
  ): Future[List[domain.PartyDetails]] =
    ledgerDao.listKnownParties()

  override def partyEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContext): Source[PartyEntry, NotUsed] = {
    Source
      .future(concreteOffset(startExclusive))
      .flatMapConcat(dispatcher.startingAt(_, RangeSource(ledgerDao.getPartyEntries)))
      .map {
        case (_, PartyLedgerEntry.AllocationRejected(subId, _, reason)) =>
          PartyEntry.AllocationRejected(subId, reason)
        case (_, PartyLedgerEntry.AllocationAccepted(subId, _, details)) =>
          PartyEntry.AllocationAccepted(subId, details)
      }
  }

  override def listLfPackages()(implicit
      loggingContext: LoggingContext
  ): Future[Map[Ref.PackageId, v2.PackageDetails]] =
    ledgerDao.listLfPackages()

  override def getLfArchive(packageId: Ref.PackageId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[DamlLf.Archive]] =
    ledgerDao.getLfArchive(packageId)

  override def packageEntries(
      startExclusive: Option[LedgerOffset.Absolute]
  )(implicit loggingContext: LoggingContext): Source[PackageEntry, NotUsed] =
    Source
      .future(concreteOffset(startExclusive))
      .flatMapConcat(dispatcher.startingAt(_, RangeSource(ledgerDao.getPackageEntries)))
      .map(_._2.toDomain)

  /** Looks up the current configuration, if set, and the offset from which
    * to subscribe to further configuration changes.
    * The offset is internal and not exposed over Ledger API.
    */
  override def lookupConfiguration()(implicit
      loggingContext: LoggingContext
  ): Future[Option[(LedgerOffset.Absolute, Configuration)]] =
    ledgerDao
      .lookupLedgerConfiguration()
      .map(
        _.map { case (offset, config) => (toAbsolute(offset), config) }
      )(ExecutionContext.parasitic)

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
          .map(cfg => LedgerConfiguration(cfg.maxDeduplicationDuration))
      }
  }

  /** Retrieve configuration entries. */
  override def configurationEntries(startExclusive: Option[LedgerOffset.Absolute])(implicit
      loggingContext: LoggingContext
  ): Source[(domain.LedgerOffset.Absolute, domain.ConfigurationEntry), NotUsed] =
    Source
      .future(concreteOffset(startExclusive))
      .flatMapConcat(dispatcher.startingAt(_, RangeSource(ledgerDao.getConfigurationEntries)).map {
        case (offset, config) =>
          toAbsolute(offset) -> config.toDomain
      })

  override def prune(pruneUpToInclusive: Offset, pruneAllDivulgedContracts: Boolean)(implicit
      loggingContext: LoggingContext
  ): Future[Unit] = {
    pruneBuffers(pruneUpToInclusive)
    ledgerDao.prune(pruneUpToInclusive, pruneAllDivulgedContracts)
  }

  override def getMeteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(implicit loggingContext: LoggingContext): Future[ReportData] =
    ledgerDao.meteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
    )

  override def currentLedgerEnd()(implicit
      loggingContext: LoggingContext
  ): Future[LedgerOffset.Absolute] = {
    val offset =
      if (ledgerEnd() == Offset.beforeBegin) ApiOffset.begin
      else ledgerEnd()
    Future.successful(toAbsolute(offset))
  }

  private def ledgerEnd(): Offset = dispatcher.getHead()

  // Returns a function that memoizes the current end
  // Can be used directly or shared throughout a request processing
  private def convertOffset: LedgerOffset => Source[Offset, NotUsed] = {
    case LedgerOffset.LedgerBegin => Source.single(Offset.beforeBegin)
    case LedgerOffset.LedgerEnd => Source.single(ledgerEnd())
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
              LedgerApiErrors.RequestValidation.OffsetOutOfRange
                .Reject(
                  s"End offset ${end.toApiString} is before Begin offset ${begin.toApiString}."
                )(new DamlContextualizedErrorLogger(logger, loggingContext, None))
                .asGrpcError
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

  private def concreteOffset(startExclusive: Option[LedgerOffset.Absolute]): Future[Offset] =
    startExclusive
      .map(off => Future.fromTry(ApiOffset.fromString(off.value)))
      .getOrElse(Future.successful(Offset.beforeBegin))

  private def toAbsolute(offset: Offset): LedgerOffset.Absolute =
    LedgerOffset.Absolute(offset.toApiString)
}
