// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.logging.entries.LoggingEntry
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.TestAcsChangeFactory
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.store.ContractStore
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
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.store.utils.QueueBasedConcurrencyLimiter
import com.digitalasset.canton.protocol.{ContractInstance, ContractMetadata, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.CreationTime.CreatedAt
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, Node}
import com.google.protobuf.ByteString
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private class JdbcLedgerDao(
    dbDispatcher: DbDispatcher & ReportsHealth,
    queryExecutionContext: ExecutionContext,
    commandExecutionContext: ExecutionContext,
    metrics: LedgerApiServerMetrics,
    sequentialIndexer: SequentialWriteDao,
    participantId: Ref.ParticipantId,
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
    translation: LfValueTranslation,
    contractStore: ContractStore,
    pruningOffsetService: PruningOffsetService,
)(implicit ec: ExecutionContext)
    extends LedgerReadDao
    with LedgerWriteDaoForTests
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

  override def initialize(
      participantId: ParticipantId
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] =
    dbDispatcher
      .executeSql(metrics.index.db.initializeLedgerParameters)(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            participantId = participantId
          ),
          loggerFactory,
        )
      )

  private val NonLocalParticipantId =
    Ref.ParticipantId.assertFromString("RESTRICTED_NON_LOCAL_PARTICIPANT_ID")

  override def storePartyAdded(
      offset: Offset,
      submissionIdOpt: Option[SubmissionId],
      recordTime: Timestamp,
      partyDetails: IndexerPartyDetails,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse] = {
    logger.info("Storing party entry")
    dbDispatcher.executeSql(metrics.index.db.storePartyEntryDbMetrics) { implicit conn =>
      sequentialIndexer.store(
        conn,
        offset,
        Some(
          state.Update.PartyAddedToParticipant(
            party = partyDetails.party,
            // HACK: the `PartyAddedToParticipant` transmits `participantId`s, while here we only have the information
            // whether the party is locally hosted or not. We use the `nonLocalParticipantId` to get the desired effect of
            // the `isLocal = False` information to be transmitted via a `PartyAddedToParticipant` `Update`.
            //
            // This will be properly resolved once we move away from the `sandbox-classic` codebase.
            participantId = if (partyDetails.isLocal) participantId else NonLocalParticipantId,
            recordTime = CantonTimestamp(recordTime),
            submissionId = submissionIdOpt,
          )
        ),
      )
      PersistenceResponse.Ok
    }
  }

  override def storeRejection(
      completionInfo: Option[state.CompletionInfo],
      recordTime: Timestamp,
      offset: Offset,
      reason: state.Update.CommandRejected.RejectionReasonTemplate,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse] =
    dbDispatcher
      .executeSql(metrics.index.db.storeRejectionDbMetrics) { implicit conn =>
        sequentialIndexer.store(
          conn,
          offset,
          completionInfo.map(info =>
            state.Update.SequencedCommandRejected(
              recordTime = CantonTimestamp(recordTime),
              completionInfo = info,
              reasonTemplate = reason,
              synchronizerId = SynchronizerId.tryFromString("invalid::deadbeef"),
            )
          ),
        )
        PersistenceResponse.Ok
      }

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
      pruneUpToInclusive: Offset,
      incompleteReassignmentOffsets: Vector[Offset],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] = {
    logger.info(s"Pruning the ledger api server index db up to ${pruneUpToInclusive.unwrap}.")

    implicit val ec: ExecutionContext = commandExecutionContext

    dbDispatcher
      .executeSql(metrics.index.db.pruneDbMetrics) { conn =>
        readStorageBackend.eventStorageBackend.pruneEventsLegacy(
          pruneUpToInclusive,
          incompleteReassignmentOffsets,
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
    lfValueTranslation = translation,
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

  private val reassignmentStreamReader = new ReassignmentStreamReader(
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dbDispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    lfValueTranslation = translation,
    contractStore = contractStore,
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
    lfValueTranslation = translation,
    contractStore = contractStore,
    metrics = metrics,
    tracer = tracer,
    topologyTransactionsStreamReader = topologyTransactionsStreamReader,
    reassignmentStreamReader = reassignmentStreamReader,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val reassignmentPointwiseReader = new ReassignmentPointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    lfValueTranslation = translation,
    queryValidRange = queryValidRange,
    contractStore = contractStore,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val topologyTransactionPointwiseReader = new TopologyTransactionPointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    lfValueTranslation = translation,
    queryValidRange = queryValidRange,
    loggerFactory = loggerFactory,
  )(queryExecutionContext)

  private val transactionPointwiseReader = new TransactionPointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    lfValueTranslation = translation,
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
    reassignmentPointwiseReader = reassignmentPointwiseReader,
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
      lfValueTranslation = translation,
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

  /** This is a combined store transaction method to support tests !!! Usage of this is discouraged
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[WorkflowId],
      updateId: UpdateId,
      ledgerEffectiveTime: Timestamp,
      offset: Offset,
      transaction: CommittedTransaction,
      recordTime: Timestamp,
      contractActivenessChanged: Boolean,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse] = for {
    _ <- Future.successful(logger.info("Storing contracts into participant contract store"))
    _ <- contractStore
      .storeContracts(
        transaction.nodes.values
          .collect { case create: Node.Create => create }
          .map(FatContract.fromCreateNode(_, CreatedAt(ledgerEffectiveTime), Bytes.Empty))
          .map(
            ContractInstance
              .ContractInstanceImpl(
                _,
                ContractMetadata.empty,
                ByteString.EMPTY,
              )
          )
          .toSeq
      )
      .failOnShutdownTo(new IllegalStateException("Storing contracts was interrupted"))

    contractIds =
      transaction.nodes.values
        .collect { case create: Node.Create => create.coid }

    internalContractIds <- contractStore
      .lookupBatchedNonCachedInternalIds(contractIds)
      .failOnShutdownTo(
        new IllegalStateException("Looking up internal contract ids was interrupted")
      )

    _ <- Future.successful(logger.info("Storing transaction"))
    _ <- dbDispatcher
      .executeSql(metrics.index.db.storeTransactionDbMetrics) { implicit conn =>
        sequentialIndexer.store(
          conn,
          offset,
          Some(
            state.Update.SequencedTransactionAccepted(
              completionInfoO = completionInfo,
              transactionMeta = state.TransactionMeta(
                ledgerEffectiveTime = ledgerEffectiveTime,
                workflowId = workflowId,
                preparationTime = null, // not used for DbDto generation
                submissionSeed = null, // not used for DbDto generation
                timeBoundaries = null, // not used for DbDto generation
                optUsedPackages = None, // not used for DbDto generation
                optNodeSeeds = None, // not used for DbDto generation
                optByKeyNodes = None, // not used for DbDto generation
              ),
              transaction = transaction,
              updateId = updateId,
              contractAuthenticationData = new Map[ContractId, Bytes] {
                override def removed(key: ContractId): Map[ContractId, Bytes] = this

                override def updated[V1 >: Bytes](
                    key: ContractId,
                    value: V1,
                ): Map[ContractId, V1] = this

                override def get(key: ContractId): Option[Bytes] = Some(Bytes.Empty)

                override def iterator: Iterator[(ContractId, Bytes)] = Iterator.empty
              }, // only for tests
              synchronizerId = SynchronizerId.tryFromString("invalid::deadbeef"),
              recordTime = CantonTimestamp(recordTime),
              externalTransactionHash = None,
              acsChangeFactory =
                TestAcsChangeFactory(contractActivenessChanged = contractActivenessChanged),
              internalContractIds = internalContractIds,
            )
          ),
        )
      }
  } yield {
    PersistenceResponse.Ok
  }

}

private[platform] object JdbcLedgerDao {

  object Logging {
    def submissionId(id: String): LoggingEntry =
      "submissionId" -> id
  }

  def read(
      dbSupport: DbSupport,
      queryExecutionContext: ExecutionContext,
      commandExecutionContext: ExecutionContext,
      metrics: LedgerApiServerMetrics,
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      completionsPageSize: Int,
      activeContractsServiceStreamsConfig: ActiveContractsServiceStreamsConfig,
      updatesStreamsConfig: UpdatesStreamsConfig,
      globalMaxEventIdQueries: Int,
      globalMaxEventPayloadQueries: Int,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
      incompleteOffsets: (
          Offset,
          Option[Set[Ref.Party]],
          TraceContext,
      ) => FutureUnlessShutdown[Vector[Offset]],
      contractLoader: ContractLoader = ContractLoader.dummyLoader,
      lfValueTranslation: LfValueTranslation,
      pruningOffsetService: PruningOffsetService,
      contractStore: ContractStore,
  )(implicit ec: ExecutionContext): LedgerReadDao =
    new JdbcLedgerDao(
      dbDispatcher = dbSupport.dbDispatcher,
      queryExecutionContext = queryExecutionContext,
      commandExecutionContext = commandExecutionContext,
      metrics = metrics,
      sequentialIndexer = SequentialWriteDao.noop,
      participantId = participantId,
      readStorageBackend = dbSupport.storageBackendFactory
        .readStorageBackend(ledgerEndCache, stringInterning, loggerFactory),
      parameterStorageBackend =
        dbSupport.storageBackendFactory.createParameterStorageBackend(stringInterning),
      ledgerEndCache = ledgerEndCache,
      completionsPageSize = completionsPageSize,
      activeContractsServiceStreamsConfig = activeContractsServiceStreamsConfig,
      updatesStreamsConfig = updatesStreamsConfig,
      globalMaxEventIdQueries = globalMaxEventIdQueries,
      globalMaxEventPayloadQueries = globalMaxEventPayloadQueries,
      tracer = tracer,
      loggerFactory = loggerFactory,
      incompleteOffsets = incompleteOffsets,
      contractLoader = contractLoader,
      translation = lfValueTranslation,
      pruningOffsetService = pruningOffsetService,
      contractStore = contractStore,
    )

  def writeForTests(
      dbSupport: DbSupport,
      sequentialWriteDao: SequentialWriteDao,
      servicesExecutionContext: ExecutionContext,
      metrics: LedgerApiServerMetrics,
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      completionsPageSize: Int,
      activeContractsServiceStreamsConfig: ActiveContractsServiceStreamsConfig,
      updatesStreamsConfig: UpdatesStreamsConfig,
      globalMaxEventIdQueries: Int,
      globalMaxEventPayloadQueries: Int,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
      contractLoader: ContractLoader = ContractLoader.dummyLoader,
      lfValueTranslation: LfValueTranslation,
      pruningOffsetService: PruningOffsetService,
      contractStore: ContractStore,
  )(implicit ec: ExecutionContext): LedgerReadDao with LedgerWriteDaoForTests =
    new JdbcLedgerDao(
      dbDispatcher = dbSupport.dbDispatcher,
      queryExecutionContext = servicesExecutionContext,
      commandExecutionContext = servicesExecutionContext,
      metrics = metrics,
      sequentialIndexer = sequentialWriteDao,
      participantId = participantId,
      readStorageBackend = dbSupport.storageBackendFactory
        .readStorageBackend(ledgerEndCache, stringInterning, loggerFactory),
      parameterStorageBackend =
        dbSupport.storageBackendFactory.createParameterStorageBackend(stringInterning),
      ledgerEndCache = ledgerEndCache,
      completionsPageSize = completionsPageSize,
      activeContractsServiceStreamsConfig = activeContractsServiceStreamsConfig,
      updatesStreamsConfig = updatesStreamsConfig,
      globalMaxEventIdQueries = globalMaxEventIdQueries,
      globalMaxEventPayloadQueries = globalMaxEventPayloadQueries,
      tracer = tracer,
      loggerFactory = loggerFactory,
      incompleteOffsets = (_, _, _) => FutureUnlessShutdown.pure(Vector.empty),
      contractLoader = contractLoader,
      translation = lfValueTranslation,
      pruningOffsetService = pruningOffsetService,
      contractStore = contractStore,
    )

  val acceptType = "accept"
  val rejectType = "reject"
}
