// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.daml.logging.entries.LoggingEntry
import com.digitalasset.canton.data.{AbsoluteOffset, CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.domain.ParticipantId
import com.digitalasset.canton.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.ledger.participant.state.index.MeteringStore.ReportData
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.{
  implicitExtractTraceContext,
  withEnrichedLoggingContext,
}
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.*
import com.digitalasset.canton.platform.config.{
  ActiveContractsServiceStreamsConfig,
  TransactionFlatStreamsConfig,
  TransactionTreeStreamsConfig,
}
import com.digitalasset.canton.platform.store.*
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{ParameterStorageBackend, ReadStorageBackend}
import com.digitalasset.canton.platform.store.cache.LedgerEndCache
import com.digitalasset.canton.platform.store.dao.events.*
import com.digitalasset.canton.platform.store.entries.PartyLedgerEntry
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.platform.store.utils.QueueBasedConcurrencyLimiter
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.engine.Engine
import com.digitalasset.daml.lf.transaction.CommittedTransaction
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

private class JdbcLedgerDao(
    dbDispatcher: DbDispatcher & ReportsHealth,
    servicesExecutionContext: ExecutionContext,
    metrics: LedgerApiServerMetrics,
    engine: Option[Engine],
    sequentialIndexer: SequentialWriteDao,
    participantId: Ref.ParticipantId,
    readStorageBackend: ReadStorageBackend,
    parameterStorageBackend: ParameterStorageBackend,
    ledgerEndCache: LedgerEndCache,
    completionsPageSize: Int,
    activeContractsServiceStreamsConfig: ActiveContractsServiceStreamsConfig,
    transactionFlatStreamsConfig: TransactionFlatStreamsConfig,
    transactionTreeStreamsConfig: TransactionTreeStreamsConfig,
    globalMaxEventIdQueries: Int,
    globalMaxEventPayloadQueries: Int,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
    incompleteOffsets: (
        AbsoluteOffset,
        Option[Set[Ref.Party]],
        TraceContext,
    ) => FutureUnlessShutdown[Vector[AbsoluteOffset]],
    contractLoader: ContractLoader,
    translation: LfValueTranslation,
) extends LedgerDao
    with NamedLogging {

  private val paginatingAsyncStream = new PaginatingAsyncStream(loggerFactory)

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

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def storePartyEntry(
      offset: AbsoluteOffset,
      partyEntry: PartyLedgerEntry,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse] = {
    logger.info("Storing party entry")
    dbDispatcher.executeSql(metrics.index.db.storePartyEntryDbMetrics) { implicit conn =>
      partyEntry match {
        case PartyLedgerEntry.AllocationAccepted(submissionIdOpt, recordTime, partyDetails) =>
          sequentialIndexer.store(
            conn,
            offset,
            Some(
              state.Update.PartyAddedToParticipant(
                party = partyDetails.party,
                // HACK: the `PartyAddedToParticipant` transmits `participantId`s, while here we only have the information
                // whether the party is locally hosted or not. We use the `nonLocalParticipantId` to get the desired effect of
                // the `isLocal = False` information to be transmitted via a `PartyAddedToParticpant` `Update`.
                //
                // This will be properly resolved once we move away from the `sandbox-classic` codebase.
                participantId = if (partyDetails.isLocal) participantId else NonLocalParticipantId,
                recordTime = CantonTimestamp(recordTime),
                submissionId = submissionIdOpt,
              )
            ),
          )
          PersistenceResponse.Ok

        case PartyLedgerEntry.AllocationRejected(submissionId, recordTime, reason) =>
          sequentialIndexer.store(
            conn,
            offset,
            Some(
              state.Update.PartyAllocationRejected(
                submissionId = submissionId,
                participantId = participantId,
                recordTime = CantonTimestamp(recordTime),
                rejectionReason = reason,
              )
            ),
          )
          PersistenceResponse.Ok
      }
    }
  }

  override def getPartyEntries(
      startInclusive: AbsoluteOffset,
      endInclusive: AbsoluteOffset,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Source[(AbsoluteOffset, PartyLedgerEntry), NotUsed] =
    paginatingAsyncStream.streamFromLimitOffsetPagination(PageSize) { queryOffset =>
      withEnrichedLoggingContext("queryOffset" -> queryOffset: LoggingEntry) {
        implicit loggingContext =>
          dbDispatcher.executeSql(metrics.index.db.loadPartyEntries)(
            readStorageBackend.partyStorageBackend.partyEntries(
              startInclusive = startInclusive,
              endInclusive = endInclusive,
              pageSize = PageSize,
              queryOffset = queryOffset,
            )
          )
      }
    }

  override def storeRejection(
      completionInfo: Option[state.CompletionInfo],
      recordTime: Timestamp,
      offset: AbsoluteOffset,
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
              domainId = DomainId.tryFromString("invalid::deadbeef"),
              requestCounter = RequestCounter(1),
              sequencerCounter = SequencerCounter(1),
            )
          ),
        )
        PersistenceResponse.Ok
      }

  private val PageSize = 100

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
    * @param pruneUpToInclusive         Offset up to which to prune archived history inclusively.
    * @param pruneAllDivulgedContracts  Enables pruning of all immediately and retroactively divulged contracts
    *                                   up to `pruneUpToInclusive`.
    *
    * NOTE:  Pruning of all divulgence events needs to take into account the following considerations:
    *        1.  Migration from mutating schema to append-only schema:
    *            - Divulgence events ingested prior to the migration to the append-only schema do not have offsets assigned
    *            and cannot be pruned incrementally (i.e. respecting the `pruneUpToInclusive)`.
    *            - For this reason, when `pruneAllDivulgedContracts` is set, `pruneUpToInclusive` must be after
    *            the last ingested event offset before the migration, otherwise an INVALID_ARGUMENT response is returned.
    *            - On the first call with `pruneUpToInclusive` higher than the migration offset, all divulgence events are pruned.
    *
    *        2.  Backwards compatibility restriction with regard to transaction-local divulgence in the SyncService:
    *            - Ledgers populated with SyncService versions that do not forward transaction-local divulgence
    *            will hydrate the index with the divulgence events only once for a specific contract-party divulgence relationship
    *            regardless of the number of re-divulgences of the contract to the same party have occurred after the initial one.
    *            - In this case, pruning of all divulged contracts might lead to interpretation failures for command submissions despite
    *            them relying on divulgences that happened after the `pruneUpToInclusive` offset.
    *            - We thus recommend participant node operators in the SDK Docs to either not prune all divulgance events; or wait
    *            for a sufficient amount of time until the Daml application had time to redivulge all events using
    *            transaction-local divulgence.
    *
    *        3.  Backwards compatibility restriction with regard to backfilling lookups:
    *            - Ledgers populated with an old KV SyncService that does not forward divulged contract instances
    *            to the ReadService (see [[com.digitalasset.canton.ledger.participant.state.kvutils.committer.transaction.TransactionCommitter.blind]])
    *            will hydrate the divulgence entries in the index without the create argument and template id.
    *            - During command interpretation, on looking up a divulged contract, the create argument and template id
    *            are backfilled from previous creates/immediate divulgence entries.
    *            - In the case of pruning of all divulged contracts (which includes immediate divulgence pruning),
    *            the previously-mentioned backfilling lookup might fail and lead to interpretation failures
    *            for command submissions that rely on divulged contracts whose associated immediate divulgence event has been pruned.
    *            As for Consideration 2, we thus recommend participant node operators in the SDK Docs to either not prune all divulgance events; or wait
    *            for a sufficient amount of time until the Daml application had time to redivulge all events using
    *            transaction-local divulgence.
    */
  override def prune(
      pruneUpToInclusive: AbsoluteOffset,
      pruneAllDivulgedContracts: Boolean,
      incompleteReassignmentOffsets: Vector[AbsoluteOffset],
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] = {
    val allDivulgencePruningParticle =
      if (pruneAllDivulgedContracts) " (including all divulged contracts)" else ""
    logger.info(
      s"Pruning the ledger api server index db$allDivulgencePruningParticle up to ${pruneUpToInclusive.toHexString}."
    )

    dbDispatcher
      .executeSql(metrics.index.db.pruneDbMetrics) { conn =>
        readStorageBackend.eventStorageBackend.pruneEvents(
          pruneUpToInclusive,
          pruneAllDivulgedContracts,
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

        if (pruneAllDivulgedContracts) {
          parameterStorageBackend.updatePrunedAllDivulgedContractsUpToInclusive(
            Offset.fromAbsoluteOffset(pruneUpToInclusive)
          )(
            conn
          )
        }
      }
      .andThen {
        case Success(_) =>
          logger.info(
            s"Completed pruning of the ledger api server index db$allDivulgencePruningParticle"
          )
        case Failure(ex) =>
          logger.warn("Pruning failed", ex)
      }(servicesExecutionContext)
  }

  override def pruningOffsets(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[(Option[Offset], Option[Offset])] =
    dbDispatcher.executeSql(metrics.index.db.fetchPruningOffsetsMetrics) { conn =>
      (
        parameterStorageBackend
          .prunedUpToInclusive(conn)
          .map(Offset.fromAbsoluteOffset),
        parameterStorageBackend
          .participantAllDivulgedContractsPrunedUpToInclusive(conn),
      )
    }

  private val queryValidRange = QueryValidRangeImpl(parameterStorageBackend, loggerFactory)

  private val globalIdQueriesLimiter = new QueueBasedConcurrencyLimiter(
    parallelism = globalMaxEventIdQueries,
    executionContext = servicesExecutionContext,
  )

  private val globalPayloadQueriesLimiter = new QueueBasedConcurrencyLimiter(
    parallelism = globalMaxEventPayloadQueries,
    executionContext = servicesExecutionContext,
  )

  private val acsReader = new ACSReader(
    config = activeContractsServiceStreamsConfig,
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    lfValueTranslation = translation,
    incompleteOffsets = incompleteOffsets,
    metrics = metrics,
    tracer = tracer,
    loggerFactory = loggerFactory,
  )(servicesExecutionContext)

  private val topologyTransactionsStreamReader = new TopologyTransactionsStreamReader(
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dbDispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    loggerFactory = loggerFactory,
  )(servicesExecutionContext)

  private val reassignmentStreamReader = new ReassignmentStreamReader(
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dbDispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    lfValueTranslation = translation,
    metrics = metrics,
    loggerFactory = loggerFactory,
  )(servicesExecutionContext)

  private val flatTransactionsStreamReader = new TransactionsFlatStreamReader(
    config = transactionFlatStreamsConfig,
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dbDispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    lfValueTranslation = translation,
    metrics = metrics,
    tracer = tracer,
    topologyTransactionsStreamReader = topologyTransactionsStreamReader,
    reassignmentStreamReader = reassignmentStreamReader,
    loggerFactory = loggerFactory,
  )(servicesExecutionContext)

  private val treeTransactionsStreamReader = new TransactionsTreeStreamReader(
    config = transactionTreeStreamsConfig,
    globalIdQueriesLimiter = globalIdQueriesLimiter,
    globalPayloadQueriesLimiter = globalPayloadQueriesLimiter,
    dbDispatcher = dbDispatcher,
    queryValidRange = queryValidRange,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    lfValueTranslation = translation,
    metrics = metrics,
    tracer = tracer,
    topologyTransactionsStreamReader = topologyTransactionsStreamReader,
    reassignmentStreamReader = reassignmentStreamReader,
    loggerFactory = loggerFactory,
  )(servicesExecutionContext)

  private val flatTransactionPointwiseReader = new TransactionFlatPointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    lfValueTranslation = translation,
  )(servicesExecutionContext)

  private val treeTransactionPointwiseReader = new TransactionTreePointwiseReader(
    dbDispatcher = dbDispatcher,
    eventStorageBackend = readStorageBackend.eventStorageBackend,
    metrics = metrics,
    lfValueTranslation = translation,
  )(servicesExecutionContext)

  override val transactionsReader: TransactionsReader =
    new TransactionsReader(
      dispatcher = dbDispatcher,
      queryValidRange = queryValidRange,
      eventStorageBackend = readStorageBackend.eventStorageBackend,
      metrics = metrics,
      flatTransactionsStreamReader = flatTransactionsStreamReader,
      treeTransactionsStreamReader = treeTransactionsStreamReader,
      flatTransactionPointwiseReader = flatTransactionPointwiseReader,
      treeTransactionPointwiseReader = treeTransactionPointwiseReader,
      acsReader = acsReader,
    )(
      servicesExecutionContext
    )

  override val contractsReader: ContractsReader =
    ContractsReader(
      contractLoader,
      dbDispatcher,
      metrics,
      readStorageBackend.contractStorageBackend,
      loggerFactory,
    )(
      servicesExecutionContext
    )

  override def eventsReader: LedgerDaoEventsReader =
    new EventsReader(
      dbDispatcher,
      readStorageBackend.eventStorageBackend,
      parameterStorageBackend,
      metrics,
      translation,
      ledgerEndCache,
    )(
      servicesExecutionContext
    )

  override val completions: CommandCompletionsReader =
    new CommandCompletionsReader(
      dbDispatcher,
      readStorageBackend.completionStorageBackend,
      queryValidRange,
      metrics,
      pageSize = completionsPageSize,
      loggerFactory,
    )

  /** This is a combined store transaction method to support sandbox-classic and tests
    * !!! Usage of this is discouraged, with the removal of sandbox-classic this will be removed
    */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def storeTransaction(
      completionInfo: Option[state.CompletionInfo],
      workflowId: Option[WorkflowId],
      updateId: UpdateId,
      ledgerEffectiveTime: Timestamp,
      offset: AbsoluteOffset,
      transaction: CommittedTransaction,
      hostedWitnesses: List[Party],
      recordTime: Timestamp,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[PersistenceResponse] = {
    logger.info("Storing transaction")
    dbDispatcher
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
                submissionTime = null, // not used for DbDto generation
                submissionSeed = null, // not used for DbDto generation
                optUsedPackages = None, // not used for DbDto generation
                optNodeSeeds = None, // not used for DbDto generation
                optByKeyNodes = None, // not used for DbDto generation
              ),
              transaction = transaction,
              updateId = updateId,
              hostedWitnesses = hostedWitnesses,
              contractMetadata = new Map[ContractId, Bytes] {
                override def removed(key: ContractId): Map[ContractId, Bytes] = this

                override def updated[V1 >: Bytes](
                    key: ContractId,
                    value: V1,
                ): Map[ContractId, V1] = this

                override def get(key: ContractId): Option[Bytes] = Some(Bytes.Empty)

                override def iterator: Iterator[(ContractId, Bytes)] = Iterator.empty
              }, // only for tests
              domainId = DomainId.tryFromString("invalid::deadbeef"),
              requestCounter = RequestCounter(1),
              sequencerCounter = SequencerCounter(1),
              recordTime = CantonTimestamp(recordTime),
            )
          ),
        )
        PersistenceResponse.Ok
      }
  }

  /** Returns all TransactionMetering records matching given criteria */
  override def meteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[ApplicationId],
  )(implicit loggingContext: LoggingContextWithTrace): Future[ReportData] =
    dbDispatcher.executeSql(metrics.index.db.lookupConfiguration)(
      readStorageBackend.meteringStorageBackend.reportData(from, to, applicationId)
    )
}

private[platform] object JdbcLedgerDao {

  object Logging {
    def submissionId(id: String): LoggingEntry =
      "submissionId" -> id

    def updateId(id: UpdateId): LoggingEntry =
      "updateId" -> id
  }

  def read(
      dbSupport: DbSupport,
      servicesExecutionContext: ExecutionContext,
      metrics: LedgerApiServerMetrics,
      engine: Option[Engine],
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      completionsPageSize: Int,
      activeContractsServiceStreamsConfig: ActiveContractsServiceStreamsConfig,
      transactionFlatStreamsConfig: TransactionFlatStreamsConfig,
      transactionTreeStreamsConfig: TransactionTreeStreamsConfig,
      globalMaxEventIdQueries: Int,
      globalMaxEventPayloadQueries: Int,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
      incompleteOffsets: (
          AbsoluteOffset,
          Option[Set[Ref.Party]],
          TraceContext,
      ) => FutureUnlessShutdown[Vector[AbsoluteOffset]],
      contractLoader: ContractLoader = ContractLoader.dummyLoader,
      lfValueTranslation: LfValueTranslation,
  ): LedgerReadDao =
    new JdbcLedgerDao(
      dbDispatcher = dbSupport.dbDispatcher,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      engine = engine,
      sequentialIndexer = SequentialWriteDao.noop,
      participantId = participantId,
      readStorageBackend = dbSupport.storageBackendFactory
        .readStorageBackend(ledgerEndCache, stringInterning, loggerFactory),
      parameterStorageBackend =
        dbSupport.storageBackendFactory.createParameterStorageBackend(stringInterning),
      ledgerEndCache = ledgerEndCache,
      completionsPageSize = completionsPageSize,
      activeContractsServiceStreamsConfig = activeContractsServiceStreamsConfig,
      transactionFlatStreamsConfig = transactionFlatStreamsConfig,
      transactionTreeStreamsConfig = transactionTreeStreamsConfig,
      globalMaxEventIdQueries = globalMaxEventIdQueries,
      globalMaxEventPayloadQueries = globalMaxEventPayloadQueries,
      tracer = tracer,
      loggerFactory = loggerFactory,
      incompleteOffsets = incompleteOffsets,
      contractLoader = contractLoader,
      translation = lfValueTranslation,
    )

  def write(
      dbSupport: DbSupport,
      sequentialWriteDao: SequentialWriteDao,
      servicesExecutionContext: ExecutionContext,
      metrics: LedgerApiServerMetrics,
      engine: Option[Engine],
      participantId: Ref.ParticipantId,
      ledgerEndCache: LedgerEndCache,
      stringInterning: StringInterning,
      completionsPageSize: Int,
      activeContractsServiceStreamsConfig: ActiveContractsServiceStreamsConfig,
      transactionFlatStreamsConfig: TransactionFlatStreamsConfig,
      transactionTreeStreamsConfig: TransactionTreeStreamsConfig,
      globalMaxEventIdQueries: Int,
      globalMaxEventPayloadQueries: Int,
      tracer: Tracer,
      loggerFactory: NamedLoggerFactory,
      contractLoader: ContractLoader = ContractLoader.dummyLoader,
      lfValueTranslation: LfValueTranslation,
  ): LedgerDao =
    new JdbcLedgerDao(
      dbDispatcher = dbSupport.dbDispatcher,
      servicesExecutionContext = servicesExecutionContext,
      metrics = metrics,
      engine = engine,
      sequentialIndexer = sequentialWriteDao,
      participantId = participantId,
      readStorageBackend = dbSupport.storageBackendFactory
        .readStorageBackend(ledgerEndCache, stringInterning, loggerFactory),
      parameterStorageBackend =
        dbSupport.storageBackendFactory.createParameterStorageBackend(stringInterning),
      ledgerEndCache = ledgerEndCache,
      completionsPageSize = completionsPageSize,
      activeContractsServiceStreamsConfig = activeContractsServiceStreamsConfig,
      transactionFlatStreamsConfig = transactionFlatStreamsConfig,
      transactionTreeStreamsConfig = transactionTreeStreamsConfig,
      globalMaxEventIdQueries = globalMaxEventIdQueries,
      globalMaxEventPayloadQueries = globalMaxEventPayloadQueries,
      tracer = tracer,
      loggerFactory = loggerFactory,
      incompleteOffsets = (_, _, _) => FutureUnlessShutdown.pure(Vector.empty),
      contractLoader = contractLoader,
      translation = lfValueTranslation,
    )

  val acceptType = "accept"
  val rejectType = "reject"
}
