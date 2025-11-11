// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.store.LedgerApiContractStore
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.config.{
  ActiveContractsServiceStreamsConfig,
  UpdatesStreamsConfig,
}
import com.digitalasset.canton.platform.store.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{ParameterStorageBackend, ReadStorageBackend}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.events.*
import com.digitalasset.canton.platform.store.utils.QueueBasedConcurrencyLimiter
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private[platform] class JdbcLedgerDao(
    dbDispatcher: DbDispatcher & ReportsHealth,
    queryExecutionContext: ExecutionContext,
    commandExecutionContext: ExecutionContext,
    metrics: LedgerApiServerMetrics,
    readStorageBackend: ReadStorageBackend,
    parameterStorageBackend: ParameterStorageBackend,
    ledgerEndCache: LedgerEndCache,
    completionsPageSize: Int,
    activeContractsServiceStreamsConfig: ActiveContractsServiceStreamsConfig,
    updatesStreamsConfig: UpdatesStreamsConfig,
    globalMaxEventIdQueries: Int,
    globalMaxEventPayloadQueries: Int,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
    incompleteOffsets: (
        Offset,
        Option[Set[Ref.Party]],
        TraceContext,
    ) => FutureUnlessShutdown[Vector[Offset]],
    contractLoader: ContractLoader,
    lfValueTranslation: LfValueTranslation,
    contractStore: LedgerApiContractStore,
    pruningOffsetService: PruningOffsetService,
)(implicit ec: ExecutionContext)
    extends LedgerReadDao
    with NamedLogging {

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ParticipantId]] =
    dbDispatcher
      .executeSql(metrics.index.db.getParticipantId)(
        parameterStorageBackend.ledgerIdentity(_).map(_.participantId)
      )

  /** Defaults to Offset.begin if ledger_end is unset
    */
  override def lookupLedgerEnd()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[LedgerEnd]] =
    dbDispatcher
      .executeSql(metrics.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )

  override def getParties(
      parties: Seq[Party]
  )(implicit loggingContext: LoggingContextWithTrace): Future[List[IndexerPartyDetails]] =
    if (parties.isEmpty)
      Future.successful(List.empty)
    else
      dbDispatcher
        .executeSql(metrics.index.db.loadParties)(
          readStorageBackend.partyStorageBackend.parties(parties)
        )

  override def listKnownParties(
      fromExcl: Option[Party],
      maxResults: Int,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]] =
    dbDispatcher
      .executeSql(metrics.index.db.loadAllParties)(
        readStorageBackend.partyStorageBackend.knownParties(fromExcl, maxResults)
      )

  /** Prunes the events and command completions tables.
    *
    * @param pruneUpToInclusive
    *   Offset up to which to prune archived history inclusively.
    */
  override def prune(
      previousPruneUpToInclusive: Option[Offset],
      previousIncompleteReassignmentOffsets: Vector[Offset],
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] = {
    logger.info(s"Pruning the ledger api server index db up to ${pruneUpToInclusive.unwrap}.")

    implicit val ec: ExecutionContext = commandExecutionContext

    dbDispatcher
      .executeSql(metrics.index.db.pruneDbMetrics) { conn =>
        // verifying pruning is continuous
        val prunedUpToInDb = parameterStorageBackend.prunedUpToInclusive(conn)
        if (prunedUpToInDb != previousPruneUpToInclusive) {
          throw new IllegalStateException(
            s"Previous pruned up to ($previousPruneUpToInclusive) is different from the current pruning offset in DB ($prunedUpToInDb)"
          )
        }

        readStorageBackend.eventStorageBackend.pruneEvents(
          previousPruneUpToInclusive = previousPruneUpToInclusive,
          previousIncompleteReassignmentOffsets = previousIncompleteReassignmentOffsets,
          pruneUpToInclusive = pruneUpToInclusive,
          incompleteReassignmentOffsets = incompleteReassignmentOffsets,
        )(
          conn,
          loggingContext.traceContext,
        )

        readStorageBackend.completionStorageBackend.pruneCompletions(pruneUpToInclusive)(
          conn,
          loggingContext.traceContext,
        )
        parameterStorageBackend.updatePrunedUptoInclusive(
          pruneUpToInclusive
        )(conn)
      }
      .thereafter {
        case Success(_) =>
          logger.info(
            s"Completed pruning of the ledger api server index db"
          )
        case Failure(ex) =>
          logger.warn("Pruning failed", ex)
      }
  }

  def indexDbPrunedUpTo(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]] =
    dbDispatcher
      .executeSql(metrics.index.db.fetchPruningOffsetsMetrics)(
        parameterStorageBackend.prunedUpToInclusive
      )

  override def pruningOffset(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]] =
    pruningOffsetService.pruningOffset

  private val queryValidRange = QueryValidRangeImpl(
    ledgerEndCache = ledgerEndCache,
    pruningOffsetService = pruningOffsetService,
    loggerFactory = loggerFactory,
  )

  private val globalIdQueriesLimiter = new QueueBasedConcurrencyLimiter(
    parallelism = globalMaxEventIdQueries,
    executionContext = queryExecutionContext,
  )

  private val globalPayloadQueriesLimiter = new QueueBasedConcurrencyLimiter(
    parallelism = globalMaxEventPayloadQueries,
    executionContext = queryExecutionContext,
  )

  private val acsReader = new ACSReader(
    config = activeContractsServiceStreamsConfig,
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    lfValueTranslation = lfValueTranslation,
    contractStore = contractStore,
    incompleteOffsets = incompleteOffsets,
    metrics = metrics,
    tracer = tracer,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val topologyTransactionsStreamReader = new TopologyTransactionsStreamReader(
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dbDispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val updatesStreamReader = new UpdatesStreamReader(
    config = updatesStreamsConfig,
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dbDispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    lfValueTranslation = lfValueTranslation,
    contractStore = contractStore,
    metrics = metrics,
    tracer = tracer,
    topologyTransactionsStreamReader = topologyTransactionsStreamReader,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val topologyTransactionPointwiseReader = new TopologyTransactionPointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    lfValueTranslation = lfValueTranslation,
    queryValidRange = queryValidRange,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val transactionPointwiseReader = new TransactionOrReassignmentPointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    lfValueTranslation = lfValueTranslation,
    queryValidRange = queryValidRange,
    contractStore = contractStore,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val updatePointwiseReader = new UpdatePointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    parameterStorageBackend = parameterStorageBackend,
    metrics = metrics,
    transactionPointwiseReader = transactionPointwiseReader,
    topologyTransactionPointwiseReader = topologyTransactionPointwiseReader,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  override val updateReader: UpdateReader =
    new UpdateReader(
      dispatcher = dbDispatcher,
      queryValidRange = queryValidRange,
      eventStorageBackend = readStorageBackend.eventStorageBackend,
      metrics = metrics,
      updatesStreamReader = updatesStreamReader,
      updatePointwiseReader = updatePointwiseReader,
      acsReader = acsReader,
    )(queryExecutionContext)

  override val contractsReader: ContractsReader =
    ContractsReader(
      contractLoader,
      dbDispatcher,
      metrics,
      readStorageBackend.contractStorageBackend,
      loggerFactory,
    )(commandExecutionContext)

  override def eventsReader: LedgerDaoEventsReader =
    new EventsReader(
      dbDispatcher = dbDispatcher,
      eventStorageBackend = readStorageBackend.eventStorageBackend,
      parameterStorageBackend = parameterStorageBackend,
      metrics = metrics,
      lfValueTranslation = lfValueTranslation,
      contractStore = contractStore,
      ledgerEndCache = ledgerEndCache,
      loggerFactory = loggerFactory,
    )(queryExecutionContext)

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(
      dbDispatcher,
      readStorageBackend.completionStorageBackend,
      queryValidRange,
      metrics,
      pageSize = completionsPageSize,
      loggerFactory,
    )

}

private[platform] object JdbcLedgerDao {

  val acceptType = "accept"
  val rejectType = "reject"
}
