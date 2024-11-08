// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import cats.Eval
import cats.data.EitherT
import com.daml.ledger.resources.ResourceOwner
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.concurrent.ExecutionContextIdlenessExecutorService
import com.digitalasset.canton.config.{ProcessingTimeout, StorageConfig}
import com.digitalasset.canton.ledger.api.health.{HealthStatus, Healthy, ReportsHealth, Unhealthy}
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.config.LedgerApiServerConfig
import com.digitalasset.canton.platform.apiserver.execution.CommandProgressTracker
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.parallel.{
  PostPublishData,
  ReassignmentOffsetPersistence,
}
import com.digitalasset.canton.platform.indexer.{
  IndexerConfig,
  IndexerQueueProxy,
  IndexerState,
  JdbcIndexer,
}
import com.digitalasset.canton.platform.store.DbSupport
import com.digitalasset.canton.platform.store.cache.OnlyForTestingTransactionInMemoryStore
import com.digitalasset.canton.platform.{
  InMemoryState,
  LedgerApiServer,
  ResourceCloseable,
  ResourceOwnerFlagCloseableOps,
}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil
import com.digitalasset.canton.util.PekkoUtil.{
  Commit,
  FutureQueue,
  IndexingFutureQueue,
  RecoveringFutureQueueImpl,
  RecoveringQueueMetrics,
}
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Future
import scala.util.Success

class LedgerApiIndexer(
    val indexerHealth: ReportsHealth,
    val queue: FutureQueue[Traced[Update]],
    val inMemoryState: InMemoryState,
    val ledgerApiStore: Eval[LedgerApiStore],
    val loggerFactory: NamedLoggerFactory,
    val timeouts: ProcessingTimeout,
    indexerState: IndexerState,
    val onlyForTestingTransactionInMemoryStore: Option[OnlyForTestingTransactionInMemoryStore],
) extends ResourceCloseable {
  def withRepairIndexer(
      repairOperation: FutureQueue[Traced[Update]] => EitherT[Future, String, Unit]
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    indexerState.withRepairIndexer(repairOperation)

  def ensureNoProcessingForDomain(domainId: DomainId): Future[Unit] =
    indexerState.ensureNoProcessingForDomain(domainId)
}

final case class LedgerApiIndexerConfig(
    storageConfig: StorageConfig,
    processingTimeout: ProcessingTimeout,
    serverConfig: LedgerApiServerConfig,
    indexerConfig: IndexerConfig,
    indexerHaConfig: HaConfig,
    ledgerParticipantId: LedgerParticipantId,
    excludedPackageIds: Set[Ref.PackageId],
    onlyForTestingEnableInMemoryTransactionStore: Boolean,
)

object LedgerApiIndexer {
  def initialize(
      metrics: LedgerApiServerMetrics,
      clock: Clock,
      commandProgressTracker: CommandProgressTracker,
      ledgerApiStore: Eval[LedgerApiStore],
      ledgerApiIndexerConfig: LedgerApiIndexerConfig,
      reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
      postProcessor: (Seq[PostPublishData], TraceContext) => Future[Unit],
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextIdlenessExecutorService,
      materializer: Materializer,
      traceContext: TraceContext,
      tracer: Tracer,
  ): Future[LedgerApiIndexer] = {
    val initializationLogger = loggerFactory.getTracedLogger(LedgerApiIndexer.getClass)
    val numIndexer = ledgerApiIndexerConfig.indexerConfig.ingestionParallelism.unwrap
    initializationLogger.info(s"Creating Ledger API Indexer storage, num-indexer: $numIndexer")
    val res = (for {
      _ <- ResourceOwner.forReleasable(() => ()) { _ =>
        initializationLogger.info("Ledger API Indexer stopped.")
        Future.unit
      }
      (inMemoryState, inMemoryStateUpdaterFlow) <-
        LedgerApiServer.createInMemoryStateAndUpdater(
          commandProgressTracker,
          ledgerApiIndexerConfig.serverConfig.indexService,
          ledgerApiIndexerConfig.serverConfig.commandService.maxCommandsInFlight,
          metrics,
          executionContext,
          tracer,
          loggerFactory,
        )(
          ledgerApiStore.value.ledgerEndCache,
          ledgerApiStore.value.stringInterningView,
        )
      healthStatusRef = new AtomicReference[HealthStatus](Unhealthy)
      indexerCreateFunction <- new JdbcIndexer.Factory(
        ledgerApiIndexerConfig.ledgerParticipantId,
        DbSupport.ParticipantDataSourceConfig(ledgerApiStore.value.ledgerApiStorage.jdbcUrl),
        ledgerApiIndexerConfig.indexerConfig,
        ledgerApiIndexerConfig.excludedPackageIds,
        metrics,
        inMemoryState,
        inMemoryStateUpdaterFlow,
        executionContext,
        tracer,
        loggerFactory,
        DbSupport.DataSourceProperties(
          connectionPool = IndexerConfig
            .createConnectionPoolConfig(
              ingestionParallelism =
                ledgerApiIndexerConfig.indexerConfig.ingestionParallelism.unwrap,
              connectionTimeout =
                ledgerApiIndexerConfig.serverConfig.databaseConnectionTimeout.underlying,
            ),
          postgres = ledgerApiIndexerConfig.serverConfig.postgresDataSource,
        ),
        ledgerApiIndexerConfig.indexerHaConfig,
        Some(ledgerApiStore.value.ledgerApiDbSupport.dbDispatcher),
        clock,
        reassignmentOffsetPersistence,
        postProcessor,
      ).initialized().map { indexer => (repairMode: Boolean) => (commit: Commit) =>
        val result = indexer(repairMode)(commit)
        result.onComplete {
          case Success(indexer) =>
            healthStatusRef.set(Healthy)
            indexer.futureQueue.done.onComplete(_ => healthStatusRef.set(Unhealthy))

          case _ =>
            healthStatusRef.set(Unhealthy)
        }
        result
      }
      normalIndexerCreateFunction = indexerCreateFunction(false)
      repairIndexerCreateFunction =
        // for repair indexer no commit functionality, and forcing repair instantiation
        () => indexerCreateFunction(true)(_ => ())
      recoveringQueueFactory = () => {
        new RecoveringFutureQueueImpl[Traced[Update]](
          maxBlockedOffer = ledgerApiIndexerConfig.indexerConfig.queueMaxBlockedOffer,
          bufferSize = ledgerApiIndexerConfig.indexerConfig.queueBufferSize,
          loggerFactory = loggerFactory,
          retryStategy = PekkoUtil.exponentialRetryWithCap(
            minWait = ledgerApiIndexerConfig.indexerConfig.queueRecoveryRetryMinWaitMillis.toLong,
            multiplier = 2,
            cap = ledgerApiIndexerConfig.indexerConfig.queueRecoveryRetryMaxWaitMillis.toLong,
          ),
          retryAttemptWarnThreshold =
            ledgerApiIndexerConfig.indexerConfig.queueRecoveryRetryAttemptWarnThreshold,
          retryAttemptErrorThreshold =
            ledgerApiIndexerConfig.indexerConfig.queueRecoveryRetryAttemptErrorThreshold,
          uncommittedWarnTreshold =
            ledgerApiIndexerConfig.indexerConfig.queueUncommittedWarnThreshold,
          recoveringQueueMetrics = RecoveringQueueMetrics(
            blockedMeter = metrics.indexer.indexerQueueBlocked,
            bufferedMeter = metrics.indexer.indexerQueueBuffered,
            uncommittedMeter = metrics.indexer.indexerQueueUncommitted,
          ),
          consumerFactory = normalIndexerCreateFunction,
        )
      }
      _ = initializationLogger.debug("Waiting for the indexer to initialize the database.")
      indexerState = new IndexerState(
        recoveringIndexerFactory = recoveringQueueFactory,
        repairIndexerFactory = () => repairIndexerCreateFunction().map(new IndexingFutureQueue(_)),
        loggerFactory = loggerFactory,
      )
      _ <- ResourceOwner.forReleasable(() => indexerState)(_.shutdown())
    } yield {
      initializationLogger.info("Ledger API Indexer started, initializing recoverable indexing.")
      new LedgerApiIndexer(
        indexerHealth = () => healthStatusRef.get(),
        queue = new IndexerQueueProxy(indexerState.withStateUnlessShutdown),
        inMemoryState = inMemoryState,
        ledgerApiStore = ledgerApiStore,
        loggerFactory = loggerFactory,
        timeouts = ledgerApiIndexerConfig.processingTimeout,
        indexerState = indexerState,
        onlyForTestingTransactionInMemoryStore = Option.when(
          ledgerApiIndexerConfig.onlyForTestingEnableInMemoryTransactionStore
        )(
          new OnlyForTestingTransactionInMemoryStore(loggerFactory)
        ),
      )
    })

    new ResourceOwnerFlagCloseableOps(res).acquireFlagCloseable("Ledger API Indexer")
  }
}
