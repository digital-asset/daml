// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.index

import com.daml.error.{ContextualizedErrorLogger, DamlErrorWithDefiniteAnswer}
import com.daml.ledger.api.v2.command_completion_service.CompletionStreamResponse
import com.daml.ledger.api.v2.event_query_service.GetEventsByContractIdResponse
import com.daml.ledger.api.v2.state_service.GetActiveContractsResponse
import com.daml.ledger.api.v2.update_service.{
  GetTransactionResponse,
  GetTransactionTreeResponse,
  GetUpdateTreesResponse,
  GetUpdatesResponse,
}
import com.daml.metrics.InstrumentedGraph.*
import com.daml.tracing.{Event, SpanAttribute, Spans}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  TraceIdentifiers,
  UpdateFormat,
  UpdateId,
}
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.index.*
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  LoggingContextWithTrace,
  NamedLoggerFactory,
  NamedLogging,
}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.pekkostreams.dispatcher.Dispatcher
import com.digitalasset.canton.pekkostreams.dispatcher.DispatcherImpl.DispatcherIsClosedException
import com.digitalasset.canton.pekkostreams.dispatcher.SubSource.RangeSource
import com.digitalasset.canton.platform.index.IndexServiceImpl.*
import com.digitalasset.canton.platform.store.cache.OffsetCheckpoint
import com.digitalasset.canton.platform.store.dao.{
  EventProjectionProperties,
  LedgerDaoCommandCompletionsReader,
  LedgerDaoUpdateReader,
  LedgerReadDao,
}
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.PackageResolution
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
  Party,
  PruneBuffers,
  TemplatePartiesFilter,
}
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{ApplicationId, Identifier, PackageRef, TypeConRef}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import io.grpc.StatusRuntimeException
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Flow, Source}
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.Future

private[index] class IndexServiceImpl(
    participantId: Ref.ParticipantId,
    ledgerDao: LedgerReadDao,
    updatesReader: LedgerDaoUpdateReader,
    commandCompletionsReader: LedgerDaoCommandCompletionsReader,
    contractStore: ContractStore,
    pruneBuffers: PruneBuffers,
    dispatcher: () => Dispatcher[Offset],
    fetchOffsetCheckpoint: () => Option[OffsetCheckpoint],
    getPackageMetadataSnapshot: ContextualizedErrorLogger => PackageMetadata,
    metrics: LedgerApiServerMetrics,
    idleStreamOffsetCheckpointTimeout: config.NonNegativeFiniteDuration,
    experimentalEnableTopologyEvents: Boolean,
    override protected val loggerFactory: NamedLoggerFactory,
) extends IndexService
    with NamedLogging {

  // A Pekko stream buffer is added at the end of all streaming queries,
  // allowing to absorb temporary downstream backpressure.
  // (e.g. when the client is temporarily slower than upstream delivery throughput)
  private val LedgerApiStreamsBufferSize = 128

  private val maximumLedgerTimeService = new ContractStoreBasedMaximumLedgerTimeService(
    contractStore,
    loggerFactory,
  )

  override def getParticipantId(): Future[Ref.ParticipantId] =
    Future.successful(participantId)

  override def currentHealth(): HealthStatus = ledgerDao.currentHealth()

  override def lookupContractKey(readers: Set[Ref.Party], key: GlobalKey)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ContractId]] =
    contractStore.lookupContractKey(readers, key)

  override def updates(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      updateFormat: UpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdatesResponse, NotUsed] = {
    val contextualizedErrorLogger = ErrorLoggingContext(logger, loggingContext)
    val isTailingStream = endInclusive.isEmpty

    withValidatedUpdateFormat(
      updateFormat,
      getPackageMetadataSnapshot(contextualizedErrorLogger),
    ) {
      between(startExclusive, endInclusive) { (from, to) =>
        from.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toDecimalString)
        )
        to.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toDecimalString)
        )
        dispatcher()
          .startingAt(
            startExclusive = from,
            subSource = RangeSource {
              val memoInternalUpdateFormat =
                memoizedInternalUpdateFormat(
                  getPackageMetadataSnapshot = getPackageMetadataSnapshot,
                  updateFormat = updateFormat,
                  alwaysPopulateArguments = false,
                  enableTopologyEvents = experimentalEnableTopologyEvents,
                )
              (startInclusive, endInclusive) =>
                Source(memoInternalUpdateFormat().toList)
                  .flatMapConcat { internalUpdateFormat =>
                    updatesReader
                      .getUpdates(
                        startInclusive = startInclusive,
                        endInclusive = endInclusive,
                        internalUpdateFormat = internalUpdateFormat,
                      )
                  }
                  .via(
                    rangeDecorator(
                      startInclusive,
                      endInclusive,
                    )
                  )
            },
            endInclusive = to,
          )
          // when a tailing stream is requested add checkpoint messages
          .via(
            checkpointFlow(
              cond = isTailingStream,
              fetchOffsetCheckpoint = fetchOffsetCheckpoint,
              responseFromCheckpoint = updatesResponse,
            )
          )
          .mapError(shutdownError)
          .buffered(metrics.index.updatesBufferSize, LedgerApiStreamsBufferSize)
      }.wireTap(
        _.update match {
          case GetUpdatesResponse.Update.Transaction(transaction) =>
            Spans.addEventToCurrentSpan(
              Event(transaction.commandId, TraceIdentifiers.fromTransaction(transaction))
            )
          case _ => ()
        }
      )
    }(contextualizedErrorLogger)
  }

  //  this flow adds checkpoint messages if the condition is met in the following way:
  //  a checkpoint message is fetched in the beginning of each batch (RangeBegin decorator)
  //  and applied exactly after an element that has the same or greater offset
  // if the condition is not true the original elements are streamed and the range decorators are ignored
  private def checkpointFlow[T](
      cond: Boolean,
      fetchOffsetCheckpoint: () => Option[OffsetCheckpoint],
      responseFromCheckpoint: OffsetCheckpoint => T,
      idleStreamOffsetCheckpointTimeout: NonNegativeFiniteDuration =
        idleStreamOffsetCheckpointTimeout,
  ): Flow[(Offset, Carrier[T]), T, NotUsed] =
    if (cond) {
      // keepAlive flow so that we create a checkpoint for idle streams
      Flow[(Offset, Carrier[T])]
        .keepAlive(
          idleStreamOffsetCheckpointTimeout.underlying,
          () => (Offset.MaxValue, Timeout), // the offset for timeout is ignored
        )
        .via(injectCheckpoints(fetchOffsetCheckpoint, responseFromCheckpoint))
        .map(_._2)
    } else
      Flow[(Offset, Carrier[T])].collect { case (_offset, Element(elem)) =>
        elem
      }

  override def transactionTrees(
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
      eventFormat: EventFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdateTreesResponse, NotUsed] = {
    val contextualizedErrorLogger = ErrorLoggingContext(logger, loggingContext)
    withValidatedFilter(
      eventFormat,
      getPackageMetadataSnapshot(contextualizedErrorLogger),
    ) {
      val isTailingStream = endInclusive.isEmpty
      val parties =
        if (eventFormat.filtersForAnyParty.isEmpty)
          Some(eventFormat.filtersByParty.keySet)
        else None // party-wildcard
      between(startExclusive, endInclusive) { (from, to) =>
        from.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetFrom, offset.toDecimalString)
        )
        to.foreach(offset =>
          Spans.setCurrentSpanAttribute(SpanAttribute.OffsetTo, offset.toDecimalString)
        )
        dispatcher()
          .startingAt(
            startExclusive = from,
            subSource = RangeSource {
              val memoFilter =
                memoizedTransactionFilterProjection(
                  getPackageMetadataSnapshot,
                  eventFormat,
                  alwaysPopulateArguments = true,
                )
              (startInclusive, endInclusive) =>
                Source(memoFilter().toList)
                  .flatMapConcat { case (_, eventProjectionProperties) =>
                    updatesReader
                      .getTransactionTrees(
                        startInclusive = startInclusive,
                        endInclusive = endInclusive,
                        // on the query filter side we treat every party as template-wildcard party,
                        // if the party-wildcard is given then the transactions for all the templates and all the parties are fetched
                        requestingParties = parties,
                        eventProjectionProperties = eventProjectionProperties,
                      )
                      .via(rangeDecorator(startInclusive, endInclusive))
                  }
            },
            endInclusive = to,
          )
          // when a tailing stream is requested add checkpoint messages
          .via(
            checkpointFlow(
              cond = isTailingStream,
              fetchOffsetCheckpoint = fetchOffsetCheckpoint,
              responseFromCheckpoint = updateTreesResponse,
            )
          )
          .mapError(shutdownError)
          .buffered(metrics.index.transactionTreesBufferSize, LedgerApiStreamsBufferSize)
      }.wireTap(
        _.update match {
          case GetUpdateTreesResponse.Update.TransactionTree(transactionTree) =>
            Spans.addEventToCurrentSpan(
              Event(
                transactionTree.commandId,
                TraceIdentifiers.fromTransactionTree(transactionTree),
              )
            )
          case _ => ()
        }
      )
    }(contextualizedErrorLogger)
  }

  override def getCompletions(
      startExclusive: Option[Offset],
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Source[CompletionStreamResponse, NotUsed] =
    Source
      .single(startExclusive)
      .flatMapConcat { beginOpt =>
        dispatcher()
          .startingAt(
            startExclusive = beginOpt,
            subSource = RangeSource((startInclusive, endInclusive) =>
              commandCompletionsReader
                .getCommandCompletions(
                  startInclusive,
                  endInclusive,
                  applicationId,
                  parties,
                )
                .via(
                  rangeDecorator(
                    startInclusive,
                    endInclusive,
                  )
                )
            ),
            endInclusive = None,
          )
          .via(
            checkpointFlow(
              cond = true,
              fetchOffsetCheckpoint = fetchOffsetCheckpoint,
              responseFromCheckpoint = completionsResponse,
            )
          )
          .mapError(shutdownError)
      }
      .buffered(metrics.index.completionsBufferSize, LedgerApiStreamsBufferSize)

  override def getActiveContracts(
      eventFormat: EventFormat,
      activeAt: Option[Offset],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContext)
    foldToSource {
      val currentPackageMetadata = getPackageMetadataSnapshot(errorLoggingContext)
      for {
        _ <- checkUnknownIdentifiers(eventFormat, currentPackageMetadata).left
          .map(_.asGrpcError)
        endOffset = ledgerEnd()
        _ <- validatedAcsActiveAtOffset(
          activeAt = activeAt,
          ledgerEnd = endOffset,
        )
      } yield {
        val activeContractsSource =
          Source(
            eventFormatProjection(
              eventFormat,
              currentPackageMetadata,
              alwaysPopulateArguments = false,
            ).toList
          ).flatMapConcat { case InternalEventFormat(templateFilter, eventProjectionProperties) =>
            ledgerDao.updateReader
              .getActiveContracts(
                activeAt = activeAt,
                filter = templateFilter,
                eventProjectionProperties = eventProjectionProperties,
              )
          }
        activeContractsSource
          .buffered(metrics.index.activeContractsBufferSize, LedgerApiStreamsBufferSize)
      }
    }
  }
  override def lookupActiveContract(
      forParties: Set[Ref.Party],
      contractId: ContractId,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[VersionedContractInstance]] =
    contractStore.lookupActiveContract(forParties, contractId)

  override def getTransactionById(
      updateId: UpdateId,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    updatesReader
      .lookupTransactionById(updateId.unwrap, internalTransactionFormat)

  override def getTransactionTreeById(
      updateId: UpdateId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    updatesReader
      .lookupTransactionTreeById(updateId.unwrap, requestingParties)

  override def getTransactionByOffset(
      offset: Offset,
      internalTransactionFormat: InternalTransactionFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    updatesReader
      .lookupTransactionByOffset(offset, internalTransactionFormat)

  override def getTransactionTreeByOffset(
      offset: Offset,
      requestingParties: Set[Ref.Party],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[GetTransactionTreeResponse]] =
    updatesReader
      .lookupTransactionTreeByOffset(offset, requestingParties)

  override def getEventsByContractId(
      contractId: ContractId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractIdResponse] =
    ledgerDao.eventsReader.getEventsByContractId(
      contractId,
      requestingParties,
    )

  // TODO(i16065): Re-enable getEventsByContractKey tests
//  override def getEventsByContractKey(
//      contractKey: com.digitalasset.daml.lf.value.Value,
//      templateId: Ref.Identifier,
//      requestingParties: Set[Ref.Party],
//      endExclusiveSeqId: Option[Long],
//  )(implicit loggingContext: LoggingContextWithTrace): Future[GetEventsByContractKeyResponse] = {
//    ledgerDao.eventsReader.getEventsByContractKey(
//      contractKey,
//      templateId,
//      requestingParties,
//      endExclusiveSeqId,
//      maxIterations = 1000,
//    )
//  }

  override def getParties(parties: Seq[Ref.Party])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]] =
    ledgerDao.getParties(parties)

  override def listKnownParties(
      fromExcl: Option[Party],
      maxResults: Int,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]] =
    ledgerDao.listKnownParties(fromExcl, maxResults)

  override def prune(
      pruneUpToInclusive: Offset,
      pruneAllDivulgedContracts: Boolean,
      incompletReassignmentOffsets: Vector[Offset],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Unit] = {
    pruneBuffers(pruneUpToInclusive)
    ledgerDao.prune(pruneUpToInclusive, pruneAllDivulgedContracts, incompletReassignmentOffsets)
  }

  override def getMeteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ReportData] =
    ledgerDao.meteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
    )

  override def currentLedgerEnd(): Future[Option[Offset]] =
    Future.successful(ledgerEnd())

  private def ledgerEnd(): Option[Offset] = dispatcher().getHead()

  private def between[A](
      startExclusive: Option[Offset],
      endInclusive: Option[Offset],
  )(f: (Option[Offset], Option[Offset]) => Source[A, NotUsed])(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[A, NotUsed] =
    Source.single(startExclusive).flatMapConcat { begin =>
      endInclusive
        .map(off => Source.single(Some(off)))
        .getOrElse(Source.single(None))
        .flatMapConcat {
          case Some(end) if begin.contains(end) =>
            Source.empty
          case Some(end) if begin > Some(end) =>
            Source.failed(
              RequestValidationErrors.OffsetOutOfRange
                .Reject(
                  s"End offset ${end.unwrap} is before begin offset ${begin.fold(0L)(_.unwrap)}."
                )(ErrorLoggingContext(logger, loggingContext))
                .asGrpcError
            )
          case endOpt: Option[Offset] =>
            f(begin, endOpt)
        }
    }

  private def shutdownError(implicit
      loggingContext: LoggingContextWithTrace
  ): PartialFunction[scala.Throwable, scala.Throwable] = { case _: DispatcherIsClosedException =>
    toGrpcError
  }

  private def toGrpcError(implicit
      loggingContext: LoggingContextWithTrace
  ): StatusRuntimeException =
    CommonErrors.ServiceNotRunning
      .Reject("Index Service")(ErrorLoggingContext(logger, loggingContext))
      .asGrpcError

  override def lookupContractState(contractId: ContractId)(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[ContractState] =
    contractStore.lookupContractState(contractId)

  override def lookupMaximumLedgerTimeAfterInterpretation(ids: Set[ContractId])(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[MaximumLedgerTime] =
    maximumLedgerTimeService.lookupMaximumLedgerTimeAfterInterpretation(ids)

  override def latestPrunedOffsets()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(Option[Offset], Option[Offset])] =
    ledgerDao.pruningOffsets
}

object IndexServiceImpl {

  private[index] def checkUnknownIdentifiers(
      apiEventFormat: EventFormat,
      metadata: PackageMetadata,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[DamlErrorWithDefiniteAnswer, Unit] = {
    val unknownPackageNames = Set.newBuilder[Ref.PackageName]
    val unknownTemplateIds = Set.newBuilder[Identifier]
    val unknownInterfaceIds = Set.newBuilder[Identifier]
    val packageNamesWithNoTemplatesForQualifiedNameBuilder =
      Set.newBuilder[(Ref.PackageName, Ref.QualifiedName)]
    val packageNamesWithNoInterfacesForQualifiedNameBuilder =
      Set.newBuilder[(Ref.PackageName, Ref.QualifiedName)]

    def checkTypeConRef(
        knownIds: Set[Identifier],
        handleUnknownIdForPkgName: (((Ref.PackageName, Ref.QualifiedName)) => Unit),
        handleUnknownId: (Identifier => Unit),
        handleUnknownPkgName: (Ref.PackageName => Unit),
    )(typeConRef: TypeConRef): Unit = typeConRef match {
      case TypeConRef(PackageRef.Name(packageName), qualifiedName) =>
        metadata.packageNameMap.get(packageName) match {
          case Some(PackageResolution(_, allPackageIdsForName))
              if !allPackageIdsForName.view
                .map(Ref.Identifier(_, qualifiedName))
                .exists(knownIds) =>
            handleUnknownIdForPkgName(packageName -> qualifiedName)
          case None => handleUnknownPkgName(packageName)
          case _ => ()
        }

      case TypeConRef(PackageRef.Id(packageId), qName) =>
        val id = Identifier(packageId, qName)
        if (!knownIds.contains(id)) handleUnknownId(id)
    }

    val cumulativeFilters = apiEventFormat.filtersByParty.iterator.map(
      _._2
    ) ++ apiEventFormat.filtersForAnyParty.iterator

    cumulativeFilters.foreach {
      case CumulativeFilter(templateFilters, interfaceFilters, _wildacrdFilter) =>
        templateFilters.iterator
          .map(_.templateTypeRef)
          .foreach(
            checkTypeConRef(
              metadata.templates,
              (packageNamesWithNoTemplatesForQualifiedNameBuilder += _),
              (unknownTemplateIds += _),
              (unknownPackageNames += _),
            )
          )
        interfaceFilters.iterator
          .map(_.interfaceTypeRef)
          .foreach(
            checkTypeConRef(
              metadata.interfaces,
              (packageNamesWithNoInterfacesForQualifiedNameBuilder += _),
              (unknownInterfaceIds += _),
              (unknownPackageNames += _),
            )
          )
    }

    val packageNames = unknownPackageNames.result()
    val templateIds = unknownTemplateIds.result()
    val interfaceIds = unknownInterfaceIds.result()
    val packageNamesWithNoTemplatesForQualifiedName =
      packageNamesWithNoTemplatesForQualifiedNameBuilder.result()
    val packageNamesWithNoInterfacesForQualifiedName =
      packageNamesWithNoInterfacesForQualifiedNameBuilder.result()

    for {
      _ <- Either.cond(
        packageNames.isEmpty,
        (),
        RequestValidationErrors.NotFound.PackageNamesNotFound.Reject(packageNames),
      )
      _ <- Either.cond(
        packageNamesWithNoTemplatesForQualifiedName.isEmpty,
        (),
        RequestValidationErrors.NotFound.NoTemplatesForPackageNameAndQualifiedName.Reject(
          packageNamesWithNoTemplatesForQualifiedName
        ),
      )
      _ <- Either.cond(
        packageNamesWithNoInterfacesForQualifiedName.isEmpty,
        (),
        RequestValidationErrors.NotFound.NoInterfaceForPackageNameAndQualifiedName.Reject(
          packageNamesWithNoInterfacesForQualifiedName
        ),
      )
      _ <- Either.cond(
        templateIds.isEmpty && interfaceIds.isEmpty,
        (),
        RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
          .Reject(unknownTemplatesOrInterfaces =
            (templateIds.view.map(Left(_)) ++ interfaceIds.view.map(Right(_))).toSeq
          ),
      )
    } yield ()
  }

  private[index] def checkUnknownIdentifiers(
      apiUpdateFormat: UpdateFormat,
      metadata: PackageMetadata,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[DamlErrorWithDefiniteAnswer, Unit] = for {
    _ <- apiUpdateFormat.includeTransactions
      .map(transactionFormat => checkUnknownIdentifiers(transactionFormat.eventFormat, metadata))
      .getOrElse(Right(()))
    _ <- apiUpdateFormat.includeReassignments
      .map(checkUnknownIdentifiers(_, metadata))
      .getOrElse(Right(()))
  } yield ()

  private[index] def foldToSource[A](
      either: Either[StatusRuntimeException, Source[A, NotUsed]]
  ): Source[A, NotUsed] = either.fold(Source.failed, identity)

  private[index] def withValidatedUpdateFormat[T](
      apiUpdateFormat: UpdateFormat,
      metadata: PackageMetadata,
  )(
      source: => Source[T, NotUsed]
  )(implicit errorLogger: ContextualizedErrorLogger): Source[T, NotUsed] =
    foldToSource(
      for {
        _ <- checkUnknownIdentifiers(apiUpdateFormat, metadata)(errorLogger).left
          .map(_.asGrpcError)
      } yield source
    )

  // TODO(#23504) cleanup
  private[index] def withValidatedFilter[T](
      apiEventFormat: EventFormat,
      metadata: PackageMetadata,
  )(
      source: => Source[T, NotUsed]
  )(implicit errorLogger: ContextualizedErrorLogger): Source[T, NotUsed] =
    foldToSource(
      for {
        _ <- checkUnknownIdentifiers(apiEventFormat, metadata)(errorLogger).left
          .map(_.asGrpcError)
      } yield source
    )

  private[index] def validatedAcsActiveAtOffset[T](
      activeAt: Option[Offset],
      ledgerEnd: Option[Offset],
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, Unit] =
    Either.cond(
      activeAt <= ledgerEnd,
      (),
      RequestValidationErrors.OffsetAfterLedgerEnd
        .Reject(
          offsetType = "active_at_offset",
          requestedOffset = activeAt.fold(0L)(_.unwrap),
          ledgerEnd = ledgerEnd.fold(0L)(_.unwrap),
        )
        .asGrpcError,
    )

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  private[index] def memoizedInternalUpdateFormat(
      getPackageMetadataSnapshot: ContextualizedErrorLogger => PackageMetadata,
      updateFormat: UpdateFormat,
      alwaysPopulateArguments: Boolean, // TODO(#23504) remove the field since it will always be false after removing transaction trees
      enableTopologyEvents: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): () => Option[InternalUpdateFormat] = {
    @volatile var metadata: PackageMetadata = null
    @volatile var internalTransactionFormat: Option[InternalTransactionFormat] = None
    @volatile var reassignmentsInternalEventFormat: Option[InternalEventFormat] = None
    () =>
      val currentMetadata = getPackageMetadataSnapshot(contextualizedErrorLogger)
      if (metadata ne currentMetadata) {
        metadata = currentMetadata
        internalTransactionFormat = updateFormat.includeTransactions.flatMap(transactionFormat =>
          eventFormatProjection(
            eventFormat = transactionFormat.eventFormat,
            metadata = metadata,
            alwaysPopulateArguments = alwaysPopulateArguments,
          ).map(internalEventFormat =>
            InternalTransactionFormat(
              internalEventFormat = internalEventFormat,
              transactionShape = transactionFormat.transactionShape,
            )
          )
        )
        reassignmentsInternalEventFormat = updateFormat.includeReassignments.flatMap(eventFormat =>
          eventFormatProjection(
            eventFormat = eventFormat,
            metadata = metadata,
            alwaysPopulateArguments = alwaysPopulateArguments,
          )
        )
      }

      val topologyEvents = if (enableTopologyEvents) updateFormat.includeTopologyEvents else None

      if (
        internalTransactionFormat.isEmpty &&
        reassignmentsInternalEventFormat.isEmpty &&
        topologyEvents.isEmpty
      )
        None
      else
        Some(
          InternalUpdateFormat(
            includeTransactions = internalTransactionFormat,
            includeReassignments = reassignmentsInternalEventFormat,
            includeTopologyEvents = topologyEvents,
          )
        )
  }

  // TODO(#23504) cleanup
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  private[index] def memoizedTransactionFilterProjection(
      getPackageMetadataSnapshot: ContextualizedErrorLogger => PackageMetadata,
      eventFormat: EventFormat,
      alwaysPopulateArguments: Boolean,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): () => Option[(TemplatePartiesFilter, EventProjectionProperties)] = {
    @volatile var metadata: PackageMetadata = null
    @volatile var filters: Option[(TemplatePartiesFilter, EventProjectionProperties)] = None
    () =>
      val currentMetadata = getPackageMetadataSnapshot(contextualizedErrorLogger)
      if (metadata ne currentMetadata) {
        metadata = currentMetadata
        filters = eventFormatProjection(
          eventFormat,
          metadata,
          alwaysPopulateArguments,
        ).map(internalEventFormat =>
          (internalEventFormat.templatePartiesFilter, internalEventFormat.eventProjectionProperties)
        )
      }
      filters
  }

  private def eventFormatProjection(
      eventFormat: EventFormat,
      metadata: PackageMetadata,
      alwaysPopulateArguments: Boolean,
  ): Option[InternalEventFormat] = {
    val templateFilter: Map[Identifier, Option[Set[Party]]] =
      IndexServiceImpl.templateFilter(metadata, eventFormat)

    val templateWildcardFilter: Option[Set[Party]] =
      IndexServiceImpl.wildcardFilter(eventFormat)

    if (templateFilter.isEmpty && templateWildcardFilter.fold(false)(_.isEmpty)) {
      None
    } else {
      val eventProjectionProperties = EventProjectionProperties(
        eventFormat = eventFormat,
        interfaceImplementedBy =
          interfaceId => metadata.interfacesImplementedBy.getOrElse(interfaceId, Set.empty),
        resolveTypeConRef = metadata.resolveTypeConRef,
        alwaysPopulateArguments = alwaysPopulateArguments,
      )
      Some(
        InternalEventFormat(
          templatePartiesFilter = TemplatePartiesFilter(
            relation = templateFilter,
            templateWildcardParties = templateWildcardFilter,
          ),
          eventProjectionProperties = eventProjectionProperties,
        )
      )
    }
  }

  private def templateIds(
      metadata: PackageMetadata,
      cumulativeFilter: CumulativeFilter,
  ): Set[Identifier] = {
    val fromInterfacesDefs = cumulativeFilter.interfaceFilters.view
      .map(_.interfaceTypeRef)
      .flatMap(metadata.resolveTypeConRef(_))
      .flatMap(metadata.interfacesImplementedBy.getOrElse(_, Set.empty).view)
      .toSet

    val fromTemplateDefs = cumulativeFilter.templateFilters.view
      .map(_.templateTypeRef)
      .flatMap(metadata.resolveTypeConRef(_))

    fromInterfacesDefs ++ fromTemplateDefs
  }

  private[index] def templateFilter(
      metadata: PackageMetadata,
      eventFormat: EventFormat,
  ): Map[Identifier, Option[Set[Party]]] = {
    val templatesFilterByParty =
      eventFormat.filtersByParty.view.foldLeft(Map.empty[Identifier, Option[Set[Party]]]) {
        case (acc, (party, cumulativeFilter)) =>
          templateIds(metadata, cumulativeFilter).foldLeft(acc) { case (acc, templateId) =>
            val updatedPartySet = acc.getOrElse(templateId, Some(Set.empty[Party])).map(_ + party)
            acc.updated(templateId, updatedPartySet)
          }
      }

    // templates filter for all the parties
    val templatesFilterForAnyParty: Map[Identifier, Option[Set[Party]]] =
      eventFormat.filtersForAnyParty
        .fold(Set.empty[Identifier])(templateIds(metadata, _))
        .map((_, None))
        .toMap

    // a filter for a specific template that is defined for any party will prevail the filters
    // defined for specific parties
    templatesFilterByParty ++ templatesFilterForAnyParty

  }

  // template-wildcard for the parties or party-wildcards of the filter given
  private[index] def wildcardFilter(
      eventFormat: EventFormat
  ): Option[Set[Party]] = {
    val emptyFiltersMessage =
      "Found transaction filter with both template and interface filters being empty, but the" +
        "request should have already been rejected in validation"
    eventFormat.filtersForAnyParty match {
      case Some(CumulativeFilter(_, _, templateWildcardFilter))
          if templateWildcardFilter.isDefined =>
        None // party-wildcard
      case Some(
            CumulativeFilter(templateIds, interfaceFilters, templateWildcardFilter)
          ) if templateIds.isEmpty && interfaceFilters.isEmpty && templateWildcardFilter.isEmpty =>
        throw new RuntimeException(emptyFiltersMessage)
      case _ =>
        Some(eventFormat.filtersByParty.view.collect {
          case (party, CumulativeFilter(_, _, templateWildcardFilter))
              if templateWildcardFilter.isDefined =>
            party
          case (
                _party,
                CumulativeFilter(templateIds, interfaceFilters, templateWildcardFilter),
              )
              if templateIds.isEmpty && interfaceFilters.isEmpty && templateWildcardFilter.isEmpty =>
            throw new RuntimeException(emptyFiltersMessage)
        }.toSet)
    }
  }

  // adds a RangeBegin message exactly before the original elements
  // and a RangeEnd message exactly after the original elements
  // the decorators are added along with the offset they are referring to
  // the decorators' offsets are both inclusive
  private[index] def rangeDecorator[T](
      startInclusive: Offset,
      endInclusive: Offset,
  ): Flow[(Offset, T), (Offset, Carrier[T]), NotUsed] =
    Flow[(Offset, T)]
      .map[(Offset, Carrier[T])] { case (off, elem) =>
        (off, Element(elem))
      }
      .prepend(
        Source.single((startInclusive, RangeBegin))
      )
      .concat(Source.single((endInclusive, RangeEnd)))

  def injectCheckpoints[T](
      fetchOffsetCheckpoint: () => Option[OffsetCheckpoint],
      responseFromCheckpoint: OffsetCheckpoint => T,
  ): Flow[(Offset, Carrier[T]), (Offset, T), NotUsed] =
    Flow[(Offset, Carrier[T])]
      .statefulMap[
        (Option[OffsetCheckpoint], Option[OffsetCheckpoint], Option[(Offset, Carrier[T])]),
        Seq[(Offset, T)],
      ](create = () => (None, None, None))(
        f = { case ((lastFetchedCheckpointO, lastStreamedCheckpointO, processedElemO), elem) =>
          elem match {
            // range begin received
            case (startInclusive, RangeBegin) =>
              val fetchedCheckpointO = fetchOffsetCheckpoint()
              // we allow checkpoints that predate the current range only for RangeBegin to allow checkpoints
              // that arrived delayed
              val response =
                if (fetchedCheckpointO != lastStreamedCheckpointO)
                  fetchedCheckpointO.collect {
                    case c: OffsetCheckpoint
                        if Some(c.offset) == startInclusive.decrement
                          && Option(c.offset) >= processedElemO.map(_._1) =>
                      (c.offset, responseFromCheckpoint(c))
                  }.toList
                else Seq.empty
              val streamedCheckpointO =
                if (response.nonEmpty) fetchedCheckpointO else lastStreamedCheckpointO
              val relevantCheckpointO = fetchedCheckpointO.collect {
                case c: OffsetCheckpoint if c.offset >= startInclusive => c
              }
              ((relevantCheckpointO, streamedCheckpointO, Some(elem)), response)
            // regular element received
            case (currentOffset, Element(currElem)) =>
              val prepend = lastFetchedCheckpointO.collect {
                case c: OffsetCheckpoint if c.offset < currentOffset =>
                  (c.offset, responseFromCheckpoint(c))
              }
              val responses = prepend.toList :+ (currentOffset, currElem)
              val newCheckpointO =
                lastFetchedCheckpointO.collect { case c: OffsetCheckpoint if prepend.isEmpty => c }
              val streamedCheckpointO =
                if (prepend.nonEmpty) lastFetchedCheckpointO else lastStreamedCheckpointO
              ((newCheckpointO, streamedCheckpointO, Some(elem)), responses)
            // range end indicator received
            case (endInclusive, RangeEnd) =>
              val responses = lastFetchedCheckpointO.collect {
                case c: OffsetCheckpoint if c.offset <= endInclusive =>
                  (c.offset, responseFromCheckpoint(c))
              }.toList
              val newCheckpointO =
                lastFetchedCheckpointO.collect {
                  case c: OffsetCheckpoint if responses.isEmpty => c
                }
              val streamedCheckpointO =
                if (responses.nonEmpty) lastFetchedCheckpointO else lastStreamedCheckpointO
              ((newCheckpointO, streamedCheckpointO, Some(elem)), responses)
            case (_, Timeout) =>
              val relevantCheckpointO = fetchOffsetCheckpoint().collect {
                case c: OffsetCheckpoint
                    if lastStreamedCheckpointO.fold(true)(_.offset < c.offset) &&
                      // check that we are not in the middle of a range
                      processedElemO
                        .fold(false)(e => e._2.isRangeEnd && e._1 == c.offset) =>
                  c
              }
              val response =
                relevantCheckpointO
                  .map(c => (c.offset, responseFromCheckpoint(c)))
                  .toList
              (
                (
                  relevantCheckpointO.orElse(lastFetchedCheckpointO),
                  relevantCheckpointO.orElse(lastStreamedCheckpointO),
                  processedElemO,
                ),
                response,
              )
          }
        },
        onComplete = _ => None,
      )
      .mapConcat(identity)

  sealed abstract class Carrier[+T] {
    def isRangeEnd: Boolean = false
    def isTimeout: Boolean = false
  }

  final case object RangeBegin extends Carrier[Nothing]
  final case object RangeEnd extends Carrier[Nothing] {
    override def isRangeEnd: Boolean = true
  }
  final case class Element[T](element: T) extends Carrier[T]
  final case object Timeout extends Carrier[Nothing] {
    override def isTimeout: Boolean = true
  }

  private def updatesResponse(
      offsetCheckpoint: OffsetCheckpoint
  ): GetUpdatesResponse =
    GetUpdatesResponse.defaultInstance.withOffsetCheckpoint(offsetCheckpoint.toApi)

  private def updateTreesResponse(
      offsetCheckpoint: OffsetCheckpoint
  ): GetUpdateTreesResponse =
    GetUpdateTreesResponse.defaultInstance.withOffsetCheckpoint(offsetCheckpoint.toApi)

  private def completionsResponse(
      offsetCheckpoint: OffsetCheckpoint
  ): CompletionStreamResponse =
    CompletionStreamResponse.defaultInstance.withOffsetCheckpoint(offsetCheckpoint.toApi)

}
