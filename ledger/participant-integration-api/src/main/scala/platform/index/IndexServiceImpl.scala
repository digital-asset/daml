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
  Filters,
  InclusiveFilters,
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
import com.daml.ledger.api.validation.ValidationErrors.invalidArgument
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
import com.daml.platform.{ApiOffset, FilterRelation, PruneBuffers}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.DispatcherImpl.DispatcherIsClosedException
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.index.IndexServiceImpl.{
  memoizedTransactionFilterProjection,
  transactionFilterProjection,
  withValidatedFilter,
}
import com.daml.platform.store.dao.{
  EventProjectionProperties,
  LedgerDaoCommandCompletionsReader,
  LedgerDaoTransactionsReader,
  LedgerReadDao,
}
import com.daml.platform.store.entries.PartyLedgerEntry
import com.daml.platform.store.packagemeta.PackageMetadataView
import com.daml.platform.store.packagemeta.PackageMetadataView.PackageMetadata
import com.daml.telemetry.{Event, SpanAttribute, Spans}
import io.grpc.StatusRuntimeException
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.{ExecutionContext, Future}

private[index] class IndexServiceImpl(
    val ledgerId: LedgerId,
    participantId: Ref.ParticipantId,
    ledgerDao: LedgerReadDao,
    transactionsReader: LedgerDaoTransactionsReader,
    commandCompletionsReader: LedgerDaoCommandCompletionsReader,
    contractStore: ContractStore,
    pruneBuffers: PruneBuffers,
    dispatcher: () => Dispatcher[Offset],
    packageMetadataView: PackageMetadataView,
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
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed] =
    withValidatedFilter(transactionFilter, packageMetadataView.current()) {
      between(startExclusive, endInclusive) { (from, to) =>
        from.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toHexString)
        )
        to.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toHexString)
        )
        dispatcher()
          .startingAt(
            from.getOrElse(Offset.beforeBegin),
            RangeSource {
              val memoFilter =
                memoizedTransactionFilterProjection(
                  packageMetadataView,
                  transactionFilter,
                  verbose,
                )
              (startExclusive, endInclusive) =>
                Source(memoFilter().toList)
                  .flatMapConcat { case (templateFilter, eventProjectionProperties) =>
                    transactionsReader
                      .getFlatTransactions(
                        startExclusive,
                        endInclusive,
                        templateFilter,
                        eventProjectionProperties,
                      )
                  }
            },
            to,
          )
          .mapError(shutdownError)
          .map(_._2)
          .buffered(metrics.daml.index.flatTransactionsBufferSize, LedgerApiStreamsBufferSize)
      }.wireTap(
        _.transactions.view
          .map(transaction =>
            Event(transaction.commandId, TraceIdentifiers.fromTransaction(transaction))
          )
          .foreach(Spans.addEventToCurrentSpan)
      )
    }

  override def transactionTrees(
      startExclusive: LedgerOffset,
      endInclusive: Option[LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionTreesResponse, NotUsed] =
    withValidatedFilter(filter, packageMetadataView.current()) {
      val parties = filter.filtersByParty.keySet
      val eventProjectionProperties = EventProjectionProperties(
        verbose = verbose,
        witnessTemplateIdFilter = parties.iterator
          .map(party => party -> Set.empty[Identifier])
          .toMap,
      )
      between(startExclusive, endInclusive) { (from, to) =>
        from.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toHexString)
        )
        to.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toHexString)
        )
        dispatcher()
          .startingAt(
            from.getOrElse(Offset.beforeBegin),
            RangeSource(
              transactionsReader
                .getTransactionTrees(_, _, parties, eventProjectionProperties)
            ),
            to,
          )
          .mapError(shutdownError)
          .map(_._2)
          .buffered(metrics.daml.index.transactionTreesBufferSize, LedgerApiStreamsBufferSize)
      }.wireTap(
        _.transactions.view
          .map(transaction =>
            Event(transaction.commandId, TraceIdentifiers.fromTransactionTree(transaction))
          )
          .foreach(Spans.addEventToCurrentSpan)
      )
    }

  override def getCompletions(
      startExclusive: LedgerOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Source[CompletionStreamResponse, NotUsed] =
    convertOffset(startExclusive)
      .flatMapConcat { beginOpt =>
        dispatcher()
          .startingAt(
            beginOpt,
            RangeSource(
              commandCompletionsReader.getCommandCompletions(_, _, applicationId, parties)
            ),
            None,
          )
          .mapError(shutdownError)
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
      dispatcher()
        .startingAt(
          start.getOrElse(Offset.beforeBegin),
          RangeSource(commandCompletionsReader.getCommandCompletions(_, _, applicationId, parties)),
          end,
        )
        .mapError(shutdownError)
        .map(_._2)
    }
      .buffered(metrics.daml.index.completionsBufferSize, LedgerApiStreamsBufferSize)

  override def getActiveContracts(
      transactionFilter: TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetActiveContractsResponse, NotUsed] =
    withValidatedFilter(transactionFilter, packageMetadataView.current()) {
      val currentLedgerEnd = ledgerEnd()

      val activeContractsSource =
        Source(
          transactionFilterProjection(
            transactionFilter,
            verbose,
            packageMetadataView.current(),
          ).toList
        ).flatMapConcat { case (templateFilter, eventProjectionProperties) =>
          ledgerDao.transactionsReader
            .getActiveContracts(
              currentLedgerEnd,
              templateFilter,
              eventProjectionProperties,
            )
        }

      activeContractsSource
        .concat(
          Source.single(
            GetActiveContractsResponse(offset = ApiOffset.toApiString(currentLedgerEnd))
          )
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

  override def lookupContractForValidation(contractId: ContractId)(implicit
      loggingContext: LoggingContext
  ): Future[Option[(VersionedContractInstance, Timestamp)]] =
    contractStore.lookupContractForValidation(contractId)

  override def getTransactionById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetFlatTransactionResponse]] =
    transactionsReader
      .lookupFlatTransactionById(transactionId.unwrap, requestingParties)

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContext): Future[Option[GetTransactionResponse]] =
    transactionsReader
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
      .flatMapConcat(dispatcher().startingAt(_, RangeSource(ledgerDao.getPartyEntries)))
      .mapError(shutdownError)
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
      .flatMapConcat(dispatcher().startingAt(_, RangeSource(ledgerDao.getPackageEntries)))
      .mapError(shutdownError)
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
      .flatMapConcat(
        dispatcher()
          .startingAt(_, RangeSource(ledgerDao.getConfigurationEntries))
          .mapError(shutdownError)
          .map { case (offset, config) =>
            toAbsolute(offset) -> config.toDomain
          }
      )

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

  private def ledgerEnd(): Offset = dispatcher().getHead()

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

  private def concreteOffset(startExclusive: Option[LedgerOffset.Absolute]): Future[Offset] =
    startExclusive
      .map(off => Future.fromTry(ApiOffset.fromString(off.value)))
      .getOrElse(Future.successful(Offset.beforeBegin))

  private def toAbsolute(offset: Offset): LedgerOffset.Absolute =
    LedgerOffset.Absolute(offset.toApiString)

  private def shutdownError(implicit
      loggingContext: LoggingContext
  ): PartialFunction[scala.Throwable, scala.Throwable] = { case _: DispatcherIsClosedException =>
    toGrpcError
  }

  private def toGrpcError(implicit loggingContext: LoggingContext): StatusRuntimeException =
    LedgerApiErrors.ServiceNotRunning
      .Reject("Index Service")(new DamlContextualizedErrorLogger(logger, loggingContext, None))
      .asGrpcError
}

object IndexServiceImpl {
  private val logger = ContextualizedLogger.get(getClass)

  private[index] def unknownTemplatesOrInterfaces(
      domainTransactionFilter: domain.TransactionFilter,
      metadata: PackageMetadata,
  ) =
    (for {
      (_, inclusiveFilterOption) <- domainTransactionFilter.filtersByParty.iterator
      inclusiveFilter <- inclusiveFilterOption.inclusive.iterator
      unknownInterfaces =
        inclusiveFilter.interfaceFilters
          .map(_.interfaceId)
          .diff(metadata.interfaces)
          .map(Right(_))
      unknownTemplates = inclusiveFilter.templateIds.diff(metadata.templates).map(Left(_))
      unknownTemplateOrInterface <- unknownInterfaces ++ unknownTemplates
    } yield unknownTemplateOrInterface).toList

  private[index] def withValidatedFilter[T](
      domainTransactionFilter: domain.TransactionFilter,
      metadata: PackageMetadata,
  )(
      source: => Source[T, NotUsed]
  )(implicit loggingContext: LoggingContext): Source[T, NotUsed] = {
    implicit val errorLogger: DamlContextualizedErrorLogger =
      new DamlContextualizedErrorLogger(logger, loggingContext, None)

    val templatesOrInterfaces = unknownTemplatesOrInterfaces(domainTransactionFilter, metadata)

    if (templatesOrInterfaces.nonEmpty)
      Source.failed(
        invalidArgument(
          invalidTemplateOrInterfaceMessage(templatesOrInterfaces)
        )
      )
    else
      source
  }

  private[index] def memoizedTransactionFilterProjection(
      packageMetadataView: PackageMetadataView,
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
  ): () => Option[(FilterRelation, EventProjectionProperties)] = {
    @volatile var metadata: PackageMetadata = null
    @volatile var filters: Option[(FilterRelation, EventProjectionProperties)] = None
    () =>
      val currentMetadata = packageMetadataView.current()
      if (metadata ne currentMetadata) {
        metadata = currentMetadata
        filters = transactionFilterProjection(transactionFilter, verbose, metadata)
      }
      filters
  }

  private def transactionFilterProjection(
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
      metadata: PackageMetadata,
  ): Option[(FilterRelation, EventProjectionProperties)] = {
    val templateFilter: Map[Party, Set[Identifier]] =
      IndexServiceImpl.templateFilter(metadata, transactionFilter)

    if (templateFilter.isEmpty) {
      None
    } else {
      val eventProjectionProperties = EventProjectionProperties(
        transactionFilter,
        verbose,
        interfaceId => metadata.interfacesImplementedBy.getOrElse(interfaceId, Set.empty),
      )
      Some((templateFilter, eventProjectionProperties))
    }
  }

  private[index] def invalidTemplateOrInterfaceMessage(
      unknownTemplatesOrInterfaces: List[Either[Identifier, Identifier]]
  ) = {
    val templates = unknownTemplatesOrInterfaces.collect { case Left(value) =>
      value
    }
    val interfaces = unknownTemplatesOrInterfaces.collect { case Right(value) =>
      value
    }
    val templatesMessage = if (templates.nonEmpty) {
      s"Templates do not exist: [${templates.mkString(", ")}]. "
    } else ""
    val interfacesMessage = if (interfaces.nonEmpty) {
      s"Interfaces do not exist: [${interfaces.mkString(", ")}]. "
    } else
      ""
    (templatesMessage + interfacesMessage).trim
  }

  private def templateIds(
      metadata: PackageMetadata
  )(inclusiveFilters: InclusiveFilters): Set[Identifier] =
    inclusiveFilters.interfaceFilters.iterator
      .map(_.interfaceId)
      .flatMap(metadata.interfacesImplementedBy.getOrElse(_, Set.empty))
      .toSet
      .++(inclusiveFilters.templateIds)

  private[index] def templateFilter(
      metadata: PackageMetadata,
      transactionFilter: domain.TransactionFilter,
  ): Map[Party, Set[Identifier]] =
    transactionFilter.filtersByParty.view.collect {
      case (party, Filters(Some(inclusiveFilters)))
          if templateIds(metadata)(inclusiveFilters).nonEmpty =>
        (party, templateIds(metadata)(inclusiveFilters))
      case (party, Filters(None)) =>
        (party, Set.empty[Identifier])
      case (party, Filters(Some(InclusiveFilters(templateIds, interfaceFilters))))
          if templateIds.isEmpty && interfaceFilters.isEmpty =>
        (party, Set.empty[Identifier])
    }.toMap
}
