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
import com.daml.ledger.api.v1.value.{Identifier => apiIdentifier}
import com.daml.ledger.api.validation.ValueValidator
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
import com.daml.lf.engine.{
  Engine,
  Result,
  ResultDone,
  ResultError,
  ResultNeedContract,
  ResultNeedKey,
  ResultNeedPackage,
}
import com.daml.lf.transaction.{GlobalKey, Versioned}
import com.daml.lf.value.Value
import com.daml.lf.value.Value.{ContractId, VersionedContractInstance}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.metrics.InstrumentedGraph._
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.{ApiOffset, PruneBuffers}
import com.daml.platform.akkastreams.dispatcher.Dispatcher
import com.daml.platform.akkastreams.dispatcher.SubSource.RangeSource
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.store.cache.SingletonPackageMetadataCache
import com.daml.platform.store.dao.{LedgerDaoTransactionsReader, LedgerReadDao}
import com.daml.platform.store.entries.PartyLedgerEntry
import com.daml.telemetry.{Event, SpanAttribute, Spans}
import io.grpc.Status.Code
import scalaz.syntax.tag.ToTagOps

import scala.annotation.tailrec
import scala.collection.mutable
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
    engine: Engine,
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

  // TODO DPP-1068: Copied from LfValueSerialization
  private def apiIdentifierToDamlLfIdentifier(id: apiIdentifier): Ref.Identifier =
    Ref.Identifier(
      Ref.PackageId.assertFromString(id.packageId),
      Ref.QualifiedName(
        Ref.ModuleName.assertFromString(id.moduleName),
        Ref.DottedName.assertFromString(id.entityName),
      ),
    )

  private def computeInterfaceView(
      templateId: Ref.Identifier,
      record: com.daml.ledger.api.v1.value.Record,
      interfaceId: Ref.Identifier,
  )(implicit loggingContext: LoggingContext): com.daml.ledger.api.v1.event.InterfaceView = {
    // TODO DPP-1068: The transaction stream contains protobuf-serialized transactions (Source[GetTransactionsResponse, NotUsed]),
    //   we don't have access to the original Daml-LF value.
    //   Here we deserialize the contract argument, use it in the engine, and then serialize it back. This needs to be improved.
    val value = ValueValidator
      .validateRecord(record)(
        DamlContextualizedErrorLogger.forTesting(getClass)
      )
      .getOrElse(throw new RuntimeException("This should never fail"))

    @tailrec
    def go(res: Result[Versioned[Value]]): Either[String, Versioned[Value]] =
      res match {
        case ResultDone(x) => Right(x)
        case ResultError(err) => Left(err.message)
        // Note: the compiler should enforce that the computation is a pure function,
        // ResultNeedContract and ResultNeedKey should never appear in the result.
        case ResultNeedContract(_, _) => Left("View computation must be a pure function")
        case ResultNeedKey(_, _) => Left("View computation must be a pure function")
        case ResultNeedPackage(pkgId, resume) =>
          // TODO DPP-1068: Package loading makes the view computation asynchronous, which is annoying to deal with (see LfValueTranslation).
          //   Here we rely on the package metadata cache to always contain all decoded packages that exist on this participant,
          //   so that we can fetch the decoded package synchronously.
          go(resume(SingletonPackageMetadataCache.getPackage(pkgId)))
      }
    val result = go(engine.computeInterfaceView(templateId, value, interfaceId))

    result
      .flatMap(versionedValue =>
        LfEngineToApi.lfValueToApiRecord(
          verbose = false,
          recordValue = versionedValue.unversioned,
        )
      )
      .fold(
        error =>
          // Note: the view computation is an arbitrary Daml function and can thus fail (e.g., with a Daml exception)
          com.daml.ledger.api.v1.event.InterfaceView(
            interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
            // TODO DPP-1068: Use a proper error status
            viewStatus =
              Some(com.google.rpc.status.Status.of(Code.INTERNAL.value(), error, Seq.empty)),
            viewValue = None,
          ),
        value =>
          com.daml.ledger.api.v1.event.InterfaceView(
            interfaceId = Some(LfEngineToApi.toApiIdentifier(interfaceId)),
            viewStatus = Some(com.google.rpc.status.Status.of(0, "", Seq.empty)),
            viewValue = Some(value),
          ),
      )
  }

  // Approach:
  //   1: Convert interface filters to template filters
  //   2: Start regular flat transaction stream using the transactionsService
  //   3: Add interface view values to the result
  // TODO DPP-1068: Move this to below the dispatcher call. The translation from interfaces to templates MUST be
  //   done separately for each offset page, otherwise we won't properly include newly introduced interface implementations.
  override def transactions(
      startExclusive: domain.LedgerOffset,
      endInclusive: Option[domain.LedgerOffset],
      filter: domain.TransactionFilter,
      verbose: Boolean,
  )(implicit loggingContext: LoggingContext): Source[GetTransactionsResponse, NotUsed] = {

    // Template ID to list of interface IDs that need to include their view
    val viewsByTemplate: mutable.Map[Ref.Identifier, Set[Ref.Identifier]] = mutable.Map.empty

    val filtersByPartyWithoutInterfaces = filter.filtersByParty.view
      .mapValues(filters =>
        Filters(filters.inclusive.map(inclusiveFilters => {
          val interfaceTemplateIds = inclusiveFilters.interfaceFilters.flatMap(interfaceFilter => {
            val interfaceId = interfaceFilter.interfaceId

            // Check whether the interface is known
            val interfaceOffset = SingletonPackageMetadataCache.interfaceAddedAt(interfaceId)
            if (interfaceOffset.isEmpty) {
              // TODO DPP-1068: Proper error handling
              throw new RuntimeException(
                s"Interface $interfaceId is not known"
              )
            }

            // Find all templates that implement this interface
            val templateIds =
              SingletonPackageMetadataCache.getInterfaceImplementations(interfaceId)

            // Remember for each of these templates whether we have to compute the view
            if (interfaceFilter.includeView) {
              templateIds.foreach(tid =>
                viewsByTemplate.updateWith(tid) {
                  case None => Some(Set(interfaceId))
                  case Some(other) => Some(other + interfaceId)
                }
              )
            }
            templateIds
          })
          val allTemplateIds = inclusiveFilters.templateIds ++ interfaceTemplateIds
          InclusiveFilters(
            templateIds = allTemplateIds,
            interfaceFilters = Set.empty,
          )
        }))
      )
      .toMap

    // TODO DPP-1068: Here we could filter out all templates that did not exist for the given offset range.
    //   The package metadata cache can provide this information.
    //   This would also benefit historical transaction streams that subscribe to a set of templates.
    val filterWithoutInterfaces = TransactionFilter(
      filtersByPartyWithoutInterfaces
    )

    transactionsWithoutInterfaces(startExclusive, endInclusive, filterWithoutInterfaces, verbose)
      .map(res => {
        val transactionsWithViews: scala.Seq[com.daml.ledger.api.v1.transaction.Transaction] =
          res.transactions.map(tx =>
            tx.update(
              _.events := tx.events.map(outerEvent =>
                outerEvent.event match {
                  case com.daml.ledger.api.v1.event.Event.Event.Created(created) =>
                    val templateId = apiIdentifierToDamlLfIdentifier(created.templateId.get)
                    val viewsToInclude = viewsByTemplate.getOrElse(templateId, Set.empty)
                    if (viewsToInclude.isEmpty) {
                      outerEvent
                    } else {
                      val interfaceViews = viewsToInclude
                        .map(iid =>
                          computeInterfaceView(templateId, created.createArguments.get, iid)
                        )
                        .toSeq
                      com.daml.ledger.api.v1.event.Event.of(
                        com.daml.ledger.api.v1.event.Event.Event
                          .Created(created.update(_.interfaceViews := interfaceViews))
                      )
                    }

                  case _ => outerEvent
                }
              )
            )
          )
        res.update(_.transactions := transactionsWithViews)
      })
  }

  // TODO DPP-1068: For a better separation of the new code, the previous transactions() method was only renamed and left otherwise untouched
  def transactionsWithoutInterfaces(
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
