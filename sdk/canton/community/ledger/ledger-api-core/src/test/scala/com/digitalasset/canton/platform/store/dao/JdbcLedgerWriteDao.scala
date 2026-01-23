// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao

import com.digitalasset.canton.config.CantonRequireTypes.String185
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.ParticipantId
import com.digitalasset.canton.ledger.api.health.{HealthStatus, ReportsHealth}
import com.digitalasset.canton.ledger.participant.state
import com.digitalasset.canton.ledger.participant.state.TestAcsChangeFactory
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.SameAsContractPackageId
import com.digitalasset.canton.ledger.participant.state.Update.{
  ContractInfo,
  TopologyTransactionEffective,
}
import com.digitalasset.canton.ledger.participant.state.index.IndexerPartyDetails
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LoggingContextWithTrace.implicitExtractTraceContext
import com.digitalasset.canton.logging.{LoggingContextWithTrace, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
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
import com.digitalasset.canton.protocol.{ContractInstance, TestUpdateId, UpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, Ref}
import com.digitalasset.daml.lf.transaction.CreationTime.CreatedAt
import com.digitalasset.daml.lf.transaction.{CommittedTransaction, Node}
import io.opentelemetry.api.trace.Tracer

import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}

private class JdbcLedgerWriteDao(
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
    lfValueTranslation: LfValueTranslation,
    contractStore: LedgerApiContractStore,
    pruningOffsetService: PruningOffsetService,
)(implicit ec: ExecutionContext)
    extends LedgerReadDao
    with LedgerWriteDao
    with NamedLogging {

  private val readDao = new JdbcLedgerDao(
    dbDispatcher = dbDispatcher,
    queryExecutionContext = queryExecutionContext,
    commandExecutionContext = commandExecutionContext,
    metrics = metrics,
    readStorageBackend = readStorageBackend,
    parameterStorageBackend = parameterStorageBackend,
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
    lfValueTranslation = lfValueTranslation,
    pruningOffsetService = pruningOffsetService,
    contractStore = contractStore,
  )

  override def currentHealth(): HealthStatus = dbDispatcher.currentHealth()

  override def lookupParticipantId()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[ParticipantId]] = readDao.lookupParticipantId()

  override def lookupLedgerEnd()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[LedgerEnd]] = readDao.lookupLedgerEnd()

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
          state.Update.TopologyTransactionEffective(
            updateId = TestUpdateId(UUID.randomUUID().toString),
            events = Set(
              TopologyTransactionEffective.TopologyEvent.PartyToParticipantAuthorization(
                party = partyDetails.party,
                participant = if (partyDetails.isLocal) participantId else NonLocalParticipantId,
                authorizationEvent = TopologyTransactionEffective.AuthorizationEvent.Added(
                  TopologyTransactionEffective.AuthorizationLevel.Confirmation
                ),
              )
            ),
            synchronizerId = SynchronizerId.tryFromString("invalid::deadbeef"),
            effectiveTime = CantonTimestamp(recordTime),
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
    readDao.getParties(parties)

  override def listKnownParties(
      fromExcl: Option[Party],
      filterParty: Option[String185],
      maxResults: Int,
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[List[IndexerPartyDetails]] = readDao.listKnownParties(fromExcl, filterParty, maxResults)

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
  )(implicit loggingContext: LoggingContextWithTrace): Future[Unit] = readDao.prune(
    previousPruneUpToInclusive,
    previousIncompleteReassignmentOffsets,
    pruneUpToInclusive,
    incompleteReassignmentOffsets,
  )

  override def pruningOffset(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]] = readDao.pruningOffset

  override val updateReader: UpdateReader = readDao.updateReader

  override val contractsReader: ContractsReader = readDao.contractsReader

  override def eventsReader: LedgerDaoEventsReader = readDao.eventsReader

  override val completions: CommandCompletionsReader = readDao.completions

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
              .create(_)
              .fold(
                error => throw new IllegalArgumentException(s"Invalid contract: $error"),
                identity,
              )
          )
          .toSeq
      )

    contractIds =
      transaction.nodes.values
        .collect { case create: Node.Create => create.coid }

    internalContractIds <- contractStore.lookupBatchedNonCachedInternalIds(contractIds)

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
              synchronizerId = SynchronizerId.tryFromString("invalid::deadbeef"),
              recordTime = CantonTimestamp(recordTime),
              externalTransactionHash = None,
              acsChangeFactory =
                TestAcsChangeFactory(contractActivenessChanged = contractActivenessChanged),
              contractInfos = internalContractIds.map { case (cid, internalContractId) =>
                cid -> ContractInfo(
                  internalContractId = internalContractId,
                  contractAuthenticationData = Bytes.Empty,
                  representativePackageId = SameAsContractPackageId,
                )
              },
            )
          ),
        )
      }
  } yield {
    PersistenceResponse.Ok
  }

  override def indexDbPrunedUpTo(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]] =
    readDao.indexDbPrunedUpTo
}
