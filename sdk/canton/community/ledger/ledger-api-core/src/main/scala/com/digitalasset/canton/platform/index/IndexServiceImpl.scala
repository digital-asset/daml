// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.domain.{
  CumulativeFilter,
  ParticipantOffset,
  TransactionFilter,
  TransactionId,
}
import com.digitalasset.canton.ledger.api.health.HealthStatus
import com.digitalasset.canton.ledger.api.{TraceIdentifiers, domain}
import com.digitalasset.canton.ledger.error.CommonErrors
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.ledger.participant.state.index.*
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
import com.digitalasset.canton.platform.ApiOffset.ApiOffsetConverter
import com.digitalasset.canton.platform.index.IndexServiceImpl.*
import com.digitalasset.canton.platform.store.dao.{
  EventProjectionProperties,
  LedgerDaoCommandCompletionsReader,
  LedgerDaoTransactionsReader,
  LedgerReadDao,
}
import com.digitalasset.canton.platform.store.entries.PartyLedgerEntry
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata
import com.digitalasset.canton.platform.store.packagemeta.PackageMetadata.PackageResolution
import com.digitalasset.canton.platform.{ApiOffset, Party, PruneBuffers, TemplatePartiesFilter}
import com.digitalasset.canton.util.EitherUtil
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.{ApplicationId, Identifier, PackageRef, TypeConRef}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.transaction.GlobalKey
import com.digitalasset.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import io.grpc.StatusRuntimeException
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source
import scalaz.syntax.tag.ToTagOps

import scala.concurrent.Future
import scala.util.Success

private[index] class IndexServiceImpl(
    participantId: Ref.ParticipantId,
    ledgerDao: LedgerReadDao,
    transactionsReader: LedgerDaoTransactionsReader,
    commandCompletionsReader: LedgerDaoCommandCompletionsReader,
    contractStore: ContractStore,
    pruneBuffers: PruneBuffers,
    dispatcher: () => Dispatcher[Offset],
    getPackageMetadataSnapshot: ContextualizedErrorLogger => PackageMetadata,
    metrics: LedgerApiServerMetrics,
    override protected val loggerFactory: NamedLoggerFactory,
) extends IndexService
    with NamedLogging {

  private val directEc = DirectExecutionContext(noTracingLogger)

  // An Pekko stream buffer is added at the end of all streaming queries,
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

  override def transactions(
      startExclusive: domain.ParticipantOffset,
      endInclusive: Option[domain.ParticipantOffset],
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdatesResponse, NotUsed] = {
    val contextualizedErrorLogger = ErrorLoggingContext(logger, loggingContext)

    withValidatedFilter(transactionFilter, getPackageMetadataSnapshot(contextualizedErrorLogger)) {
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
                  getPackageMetadataSnapshot,
                  transactionFilter,
                  verbose,
                  alwaysPopulateArguments = false,
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
          .buffered(metrics.index.flatTransactionsBufferSize, LedgerApiStreamsBufferSize)
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

  override def transactionTrees(
      startExclusive: ParticipantOffset,
      endInclusive: Option[ParticipantOffset],
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[GetUpdateTreesResponse, NotUsed] = {
    val contextualizedErrorLogger = ErrorLoggingContext(logger, loggingContext)
    withValidatedFilter(
      transactionFilter,
      getPackageMetadataSnapshot(contextualizedErrorLogger),
    ) {
      val parties =
        if (transactionFilter.filtersForAnyParty.isEmpty)
          Some(transactionFilter.filtersByParty.keySet)
        else None // party-wildcard
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
                  getPackageMetadataSnapshot,
                  transactionFilter,
                  verbose,
                  alwaysPopulateArguments = true,
                )
              (startExclusive, endInclusive) =>
                Source(memoFilter().toList)
                  .flatMapConcat { case (_, eventProjectionProperties) =>
                    transactionsReader
                      .getTransactionTrees(
                        startExclusive,
                        endInclusive,
                        // on the query filter side we treat every party as template-wildcard party,
                        // if the party-wildcard is given then the transactions for all the templates and all the parties are fetched
                        parties,
                        eventProjectionProperties,
                      )
                  }
            },
            to,
          )
          .mapError(shutdownError)
          .map(_._2)
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
      startExclusive: ParticipantOffset,
      applicationId: Ref.ApplicationId,
      parties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Source[CompletionStreamResponse, NotUsed] =
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
      .buffered(metrics.index.completionsBufferSize, LedgerApiStreamsBufferSize)

  override def getActiveContracts(
      transactionFilter: TransactionFilter,
      verbose: Boolean,
      activeAtO: Option[Offset],
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[GetActiveContractsResponse, NotUsed] = {
    implicit val errorLoggingContext = ErrorLoggingContext(logger, loggingContext)
    foldToSource {
      val currentPackageMetadata = getPackageMetadataSnapshot(errorLoggingContext)
      for {
        _ <- checkUnknownIdentifiers(transactionFilter, currentPackageMetadata).left
          .map(_.asGrpcError)
        endOffset = ledgerEnd()
        activeAt = activeAtO.getOrElse(endOffset)
        _ <- validatedAcsActiveAtOffset(activeAt = activeAt, ledgerEnd = endOffset)
      } yield {
        val activeContractsSource =
          Source(
            transactionFilterProjection(
              transactionFilter,
              verbose,
              currentPackageMetadata,
              alwaysPopulateArguments = false,
            ).toList
          ).flatMapConcat { case (templateFilter, eventProjectionProperties) =>
            ledgerDao.transactionsReader
              .getActiveContracts(
                activeAt = activeAt,
                filter = templateFilter,
                eventProjectionProperties = eventProjectionProperties,
              )
          }
        activeContractsSource
          .concat(
            Source.single(
              GetActiveContractsResponse(offset = ApiOffset.toApiString(activeAt))
            )
          )
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
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionResponse]] =
    transactionsReader
      .lookupFlatTransactionById(transactionId.unwrap, requestingParties)

  override def getTransactionTreeById(
      transactionId: TransactionId,
      requestingParties: Set[Ref.Party],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetTransactionTreeResponse]] =
    transactionsReader
      .lookupTransactionTreeById(transactionId.unwrap, requestingParties)

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

  override def partyEntries(
      startExclusive: Option[ParticipantOffset.Absolute]
  )(implicit loggingContext: LoggingContextWithTrace): Source[PartyEntry, NotUsed] = {
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

  override def currentLedgerEnd(): Future[ParticipantOffset.Absolute] = {
    val absoluteApiOffset = toApiOffset(ledgerEnd())
    Future.successful(absoluteApiOffset)
  }

  private def toApiOffset(ledgerDomainOffset: Offset): ParticipantOffset.Absolute = {
    val offset =
      if (ledgerDomainOffset == Offset.beforeBegin) ApiOffset.begin
      else ledgerDomainOffset
    toAbsolute(offset)
  }

  private def ledgerEnd(): Offset = dispatcher().getHead()

  // Returns a function that memoizes the current end
  // Can be used directly or shared throughout a request processing
  private def convertOffset: ParticipantOffset => Source[Offset, NotUsed] = { ledgerOffset =>
    (ledgerOffset match {
      case ParticipantOffset.ParticipantBegin => Success(Offset.beforeBegin)
      case ParticipantOffset.ParticipantEnd => Success(ledgerEnd())
      case ParticipantOffset.Absolute(offset) => ApiOffset.tryFromString(offset)
    }).fold(Source.failed, off => Source.single(off))
  }

  private def between[A](
      startExclusive: domain.ParticipantOffset,
      endInclusive: Option[domain.ParticipantOffset],
  )(f: (Option[Offset], Option[Offset]) => Source[A, NotUsed])(implicit
      loggingContext: LoggingContextWithTrace
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
              RequestValidationErrors.OffsetOutOfRange
                .Reject(
                  s"End offset ${end.toApiString} is before Begin offset ${begin.toApiString}."
                )(ErrorLoggingContext(logger, loggingContext))
                .asGrpcError
            )
          case endOpt: Option[Offset] =>
            f(Some(begin), endOpt)
        }
    }
  }

  private def concreteOffset(startExclusive: Option[ParticipantOffset.Absolute]): Future[Offset] =
    startExclusive
      .map(off => Future.fromTry(ApiOffset.tryFromString(off.value)))
      .getOrElse(Future.successful(Offset.beforeBegin))

  private def toAbsolute(offset: Offset): ParticipantOffset.Absolute =
    ParticipantOffset.Absolute(offset.toApiString)

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
  ): Future[(ParticipantOffset.Absolute, ParticipantOffset.Absolute)] =
    ledgerDao.pruningOffsets
      .map { case (prunedUpToInclusiveO, divulgencePrunedUpToO) =>
        toApiOffset(prunedUpToInclusiveO.getOrElse(Offset.beforeBegin)) -> toApiOffset(
          divulgencePrunedUpToO.getOrElse(Offset.beforeBegin)
        )
      }(directEc)
}

object IndexServiceImpl {

  private[index] def checkUnknownIdentifiers(
      domainTransactionFilter: domain.TransactionFilter,
      metadata: PackageMetadata,
  )(implicit
      contextualizedErrorLogger: ContextualizedErrorLogger
  ): Either[DamlErrorWithDefiniteAnswer, Unit] = {
    val unknownPackageNames = Set.newBuilder[Ref.PackageName]
    val unknownTemplateIds = Set.newBuilder[Identifier]
    val unknownInterfaceIds = Set.newBuilder[Identifier]
    val packageNamesWithNoTemplatesForQualifiedNameBuilder =
      Set.newBuilder[(Ref.PackageName, Ref.QualifiedName)]

    def checkTypeConRef(typeConRef: TypeConRef): Unit = typeConRef match {
      case TypeConRef(PackageRef.Name(packageName), qualifiedName) =>
        metadata.packageNameMap.get(packageName) match {
          case Some(PackageResolution(_, allPackageIdsForName))
              if !allPackageIdsForName.view
                .map(Ref.Identifier(_, qualifiedName))
                .exists(metadata.templates) =>
            packageNamesWithNoTemplatesForQualifiedNameBuilder += (packageName -> qualifiedName)
          case None => unknownPackageNames += packageName
          case _ => ()
        }

      case TypeConRef(PackageRef.Id(packageId), qName) =>
        val templateId = Identifier(packageId, qName)
        if (!metadata.templates.contains(templateId)) unknownTemplateIds += templateId
    }

    val cumulativeFilters = domainTransactionFilter.filtersByParty.iterator.map(
      _._2
    ) ++ domainTransactionFilter.filtersForAnyParty.iterator

    cumulativeFilters.foreach {
      case CumulativeFilter(templateFilters, interfaceFilters, _wildacrdFilter) =>
        templateFilters.iterator.map(_.templateTypeRef).foreach(checkTypeConRef)
        interfaceFilters.iterator.map(_.interfaceId).foreach { interfaceId =>
          if (!metadata.interfaces.contains(interfaceId)) unknownInterfaceIds += interfaceId
        }
    }

    val packageNames = unknownPackageNames.result()
    val templateIds = unknownTemplateIds.result()
    val interfaceIds = unknownInterfaceIds.result()
    val packageNamesWithNoTemplatesForQualifiedName =
      packageNamesWithNoTemplatesForQualifiedNameBuilder.result()

    for {
      _ <- EitherUtil.condUnitE(
        packageNames.isEmpty,
        RequestValidationErrors.NotFound.PackageNamesNotFound.Reject(packageNames),
      )
      _ <- EitherUtil.condUnitE(
        packageNamesWithNoTemplatesForQualifiedName.isEmpty,
        RequestValidationErrors.NotFound.NoTemplatesForPackageNameAndQualifiedName.Reject(
          packageNamesWithNoTemplatesForQualifiedName
        ),
      )
      _ <- EitherUtil.condUnitE(
        templateIds.isEmpty && interfaceIds.isEmpty,
        RequestValidationErrors.NotFound.TemplateOrInterfaceIdsNotFound
          .Reject(unknownTemplatesOrInterfaces =
            (templateIds.view.map(Left(_)) ++ interfaceIds.view.map(Right(_))).toSeq
          ),
      )
    } yield ()
  }

  private[index] def foldToSource[A](
      either: Either[StatusRuntimeException, Source[A, NotUsed]]
  ): Source[A, NotUsed] = either.fold(Source.failed, identity)

  private[index] def withValidatedFilter[T](
      domainTransactionFilter: domain.TransactionFilter,
      metadata: PackageMetadata,
  )(
      source: => Source[T, NotUsed]
  )(implicit errorLogger: ContextualizedErrorLogger): Source[T, NotUsed] =
    foldToSource(
      for {
        _ <- checkUnknownIdentifiers(domainTransactionFilter, metadata)(errorLogger).left
          .map(_.asGrpcError)
      } yield source
    )

  private[index] def validatedAcsActiveAtOffset[T](
      activeAt: Offset,
      ledgerEnd: Offset,
  )(implicit errorLogger: ContextualizedErrorLogger): Either[StatusRuntimeException, Unit] = {
    if (activeAt > ledgerEnd) {
      Left(
        RequestValidationErrors.OffsetAfterLedgerEnd
          .Reject(
            offsetType = "active_at_offset",
            requestedOffset = activeAt.toApiString,
            ledgerEnd = ledgerEnd.toApiString,
          )
          .asGrpcError
      )
    } else {
      Right(())
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Var"))
  private[index] def memoizedTransactionFilterProjection(
      getPackageMetadataSnapshot: ContextualizedErrorLogger => PackageMetadata,
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
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
        filters = transactionFilterProjection(
          transactionFilter,
          verbose,
          metadata,
          alwaysPopulateArguments,
        )
      }
      filters
  }

  private def transactionFilterProjection(
      transactionFilter: domain.TransactionFilter,
      verbose: Boolean,
      metadata: PackageMetadata,
      alwaysPopulateArguments: Boolean,
  ): Option[(TemplatePartiesFilter, EventProjectionProperties)] = {
    val templateFilter: Map[Identifier, Option[Set[Party]]] =
      IndexServiceImpl.templateFilter(metadata, transactionFilter)

    val templateWildcardFilter: Option[Set[Party]] =
      IndexServiceImpl.wildcardFilter(transactionFilter)

    if (templateFilter.isEmpty && templateWildcardFilter.fold(false)(_.isEmpty)) {
      None
    } else {
      val eventProjectionProperties = EventProjectionProperties(
        transactionFilter,
        verbose,
        interfaceId => metadata.interfacesImplementedBy.getOrElse(interfaceId, Set.empty),
        resolveTemplateRef(metadata),
        alwaysPopulateArguments,
      )
      Some(
        (
          TemplatePartiesFilter(
            templateFilter,
            templateWildcardFilter,
          ),
          eventProjectionProperties,
        )
      )
    }
  }

  private def resolveTemplateRef(metadata: PackageMetadata): TypeConRef => Set[Identifier] = {
    case TypeConRef(PackageRef.Name(packageName), qualifiedName) =>
      resolveUpgradableTemplates(metadata, packageName, qualifiedName)
    case TypeConRef(PackageRef.Id(packageId), qName) => Set(Identifier(packageId, qName))
  }

  /** Resolve all template ids for (package-name, qualified-name).
    *
    * As context, package-level upgrading compatibility between two packages pkg1 and pkg2,
    * where pkg2 upgrades pkg1 and they both have the same package-name,
    * ensures that all templates defined in pkg1 are present in pkg2.
    * Then, for resolving all the template-ids for (package-name, qualified-name):
    *
    * * we first create all possible template-ids by concatenation with the requested qualified-name
    *   of the known package-ids for the requested package-name.
    *
    * * Then, since some templates can only be defined later (in a package with greater package-version),
    *   we filter the previous result by intersection with the known template-id set.
    */
  private[index] def resolveUpgradableTemplates(
      packageMetadata: PackageMetadata,
      packageName: Ref.PackageName,
      qualifiedName: Ref.QualifiedName,
  ): Set[Identifier] =
    packageMetadata.packageNameMap
      .get(packageName)
      .map(_.allPackageIdsForName.iterator)
      .getOrElse(Iterator.empty)
      .map(packageId => Identifier(packageId, qualifiedName))
      .toSet
      .intersect(packageMetadata.templates)

  private def templateIds(
      metadata: PackageMetadata,
      cumulativeFilter: CumulativeFilter,
  ): Set[Identifier] = {
    val fromInterfacesDefs = cumulativeFilter.interfaceFilters.view
      .map(_.interfaceId)
      .flatMap(metadata.interfacesImplementedBy.getOrElse(_, Set.empty))
      .toSet

    val fromTemplateDefs = cumulativeFilter.templateFilters.view
      .map(_.templateTypeRef)
      .flatMap {
        case TypeConRef(PackageRef.Name(packageName), qualifiedName) =>
          resolveUpgradableTemplates(metadata, packageName, qualifiedName)
        case TypeConRef(PackageRef.Id(packageId), qName) => Iterator(Identifier(packageId, qName))
      }

    fromInterfacesDefs ++ fromTemplateDefs
  }

  private[index] def templateFilter(
      metadata: PackageMetadata,
      transactionFilter: domain.TransactionFilter,
  ): Map[Identifier, Option[Set[Party]]] = {
    val templatesFilterByParty =
      transactionFilter.filtersByParty.view.foldLeft(Map.empty[Identifier, Option[Set[Party]]]) {
        case (acc, (party, cumulativeFilter)) =>
          templateIds(metadata, cumulativeFilter).foldLeft(acc) { case (acc, templateId) =>
            val updatedPartySet = acc.getOrElse(templateId, Some(Set.empty[Party])).map(_ + party)
            acc.updated(templateId, updatedPartySet)
          }
      }

    // templates filter for all the parties
    val templatesFilterForAnyParty: Map[Identifier, Option[Set[Party]]] =
      transactionFilter.filtersForAnyParty
        .fold(Set.empty[Identifier])(templateIds(metadata, _))
        .map((_, None))
        .toMap

    // a filter for a specific template that is defined for any party will prevail the filters
    // defined for specific parties
    templatesFilterByParty ++ templatesFilterForAnyParty

  }

  // template-wildcard for the parties or party-wildcards of the filter given
  private[index] def wildcardFilter(
      transactionFilter: domain.TransactionFilter
  ): Option[Set[Party]] = {
    val emptyFiltersMessage =
      "Found transaction filter with both template and interface filters being empty, but the" +
        "request should have already been rejected in validation"
    transactionFilter.filtersForAnyParty match {
      case Some(CumulativeFilter(_, _, templateWildcardFilter))
          if templateWildcardFilter.isDefined =>
        None // party-wildcard
      case Some(
            CumulativeFilter(templateIds, interfaceFilters, templateWildcardFilter)
          ) if templateIds.isEmpty && interfaceFilters.isEmpty && templateWildcardFilter.isEmpty =>
        throw new RuntimeException(emptyFiltersMessage)
      case _ =>
        Some(transactionFilter.filtersByParty.view.collect {
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

}
