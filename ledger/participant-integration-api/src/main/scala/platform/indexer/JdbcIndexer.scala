// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import java.util.concurrent.atomic.AtomicReference

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.daml.ledger.api.domain
import com.daml.ledger.offset.Offset
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.ApiOffset.ApiOffsetConverter
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.parallel.{
  InitializeParallelIngestion,
  ParallelIndexerFactory,
  ParallelIndexerSubscription,
}
import com.daml.platform.store.DbType.{
  AsynchronousCommit,
  LocalSynchronousCommit,
  SynchronousCommit,
}
import com.daml.platform.store.appendonlydao.events.{CompressionStrategy, LfValueTranslation}
import com.daml.platform.store.backend.DataSourceStorageBackend.DataSourceConfig
import com.daml.platform.store.backend.StorageBackend
import com.daml.platform.store.backend.postgresql.PostgresDataSourceConfig
import com.daml.platform.store.dao.LedgerDao
import com.daml.platform.store.{DbType, LfValueTranslationCache}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

object JdbcIndexer {
  private[daml] final class Factory private[indexer] (
      config: IndexerConfig,
      readService: state.ReadService,
      servicesExecutionContext: ExecutionContext,
      metrics: Metrics,
      updateFlowOwnerBuilder: ExecuteUpdate.FlowOwnerBuilder,
      serverRole: ServerRole,
      lfValueTranslationCache: LfValueTranslationCache.Cache,
  )(implicit materializer: Materializer, loggingContext: LoggingContext) {

    private[daml] def this(
        serverRole: ServerRole,
        config: IndexerConfig,
        readService: state.ReadService,
        servicesExecutionContext: ExecutionContext,
        metrics: Metrics,
        lfValueTranslationCache: LfValueTranslationCache.Cache,
    )(implicit materializer: Materializer, loggingContext: LoggingContext) =
      this(
        config,
        readService,
        servicesExecutionContext,
        metrics,
        ExecuteUpdate.owner,
        serverRole,
        lfValueTranslationCache,
      )

    private[this] def initializedMutatingSchema(
        resetSchema: Boolean
    )(implicit resourceContext: ResourceContext): Future[ResourceOwner[Indexer]] =
      Future.successful(for {
        ledgerDao <- com.daml.platform.store.dao.JdbcLedgerDao.writeOwner(
          serverRole,
          config.jdbcUrl,
          config.databaseConnectionPoolSize,
          config.databaseConnectionTimeout,
          config.eventsPageSize,
          servicesExecutionContext,
          metrics,
          lfValueTranslationCache,
          jdbcAsyncCommitMode = config.asyncCommitMode,
          enricher = None,
        )
        _ <-
          if (resetSchema) {
            ResourceOwner.forFuture(() => ledgerDao.reset())
          } else {
            ResourceOwner.unit
          }
        initialLedgerEnd <- ResourceOwner.forFuture(() =>
          initializeLedger(ledgerDao)(resourceContext.executionContext)
        )
        dbType = DbType.jdbcType(config.jdbcUrl)
        updateFlow <- updateFlowOwnerBuilder(
          dbType,
          ledgerDao,
          metrics,
          config.participantId,
          config.updatePreparationParallelism,
          materializer.executionContext,
          loggingContext,
        )
      } yield new JdbcIndexer(initialLedgerEnd, metrics, updateFlow))

    private[this] def initializedAppendOnlySchema(resetSchema: Boolean)(implicit
        resourceContext: ResourceContext
    ): Future[ResourceOwner[Indexer]] = {
      implicit val executionContext: ExecutionContext = resourceContext.executionContext
      for {
        // Note: the LedgerDao interface is only used for initialization here, it can be released immediately
        // after initialization is finished. Hence the use of ResourceOwner.use().
        _ <- com.daml.platform.store.appendonlydao.JdbcLedgerDao
          .writeOwner(
            serverRole,
            config.jdbcUrl,
            config.databaseConnectionPoolSize,
            config.databaseConnectionTimeout,
            config.eventsPageSize,
            config.eventsProcessingParallelism,
            servicesExecutionContext,
            metrics,
            lfValueTranslationCache,
            enricher = None,
            participantId = config.participantId,
          )
          .use(ledgerDao =>
            for {
              _ <-
                if (resetSchema) {
                  ledgerDao.reset()
                } else {
                  Future.successful(())
                }
              _ <- initializeLedger(ledgerDao)
            } yield ()
          )
        storageBackend = StorageBackend.of(DbType.jdbcType(config.jdbcUrl))
      } yield ParallelIndexerFactory(
        jdbcUrl = config.jdbcUrl,
        inputMappingParallelism = config.inputMappingParallelism,
        batchingParallelism = config.batchingParallelism,
        ingestionParallelism = config.ingestionParallelism,
        dataSourceConfig = DataSourceConfig(
          postgresConfig = PostgresDataSourceConfig(
            synchronousCommit = Some(config.asyncCommitMode match {
              case SynchronousCommit => PostgresDataSourceConfig.SynchronousCommitValue.On
              case AsynchronousCommit => PostgresDataSourceConfig.SynchronousCommitValue.Off
              case LocalSynchronousCommit => PostgresDataSourceConfig.SynchronousCommitValue.Local
            })
          )
        ),
        haConfig = config.haConfig,
        metrics = metrics,
        storageBackend = storageBackend,
        initializeParallelIngestion = InitializeParallelIngestion(
          storageBackend = storageBackend,
          metrics = metrics,
        ),
        parallelIndexerSubscription = ParallelIndexerSubscription(
          storageBackend = storageBackend,
          participantId = config.participantId,
          translation = new LfValueTranslation(
            cache = lfValueTranslationCache,
            metrics = metrics,
            enricherO = None,
            loadPackage = (_, _) => Future.successful(None),
          ),
          compressionStrategy =
            if (config.enableCompression) CompressionStrategy.allGZIP(metrics)
            else CompressionStrategy.none(metrics),
          maxInputBufferSize = config.maxInputBufferSize,
          inputMappingParallelism = config.inputMappingParallelism,
          batchingParallelism = config.batchingParallelism,
          ingestionParallelism = config.ingestionParallelism,
          submissionBatchSize = config.submissionBatchSize,
          tailingRateLimitPerSecond = config.tailingRateLimitPerSecond,
          batchWithinMillis = config.batchWithinMillis,
          metrics = metrics,
        ),
        mat = materializer,
      )
    }

    def initialized(resetSchema: Boolean = false)(implicit
        resourceContext: ResourceContext
    ): Future[ResourceOwner[Indexer]] =
      if (config.enableAppendOnlySchema)
        initializedAppendOnlySchema(resetSchema)
      else
        initializedMutatingSchema(resetSchema)

    private def initializeLedger(
        dao: LedgerDao
    )(implicit ec: ExecutionContext): Future[Option[Offset]] = {
      // In a high availability setup, multiple indexers might try to initialize the database at the same time
      // Add an identifier to correlate the log output for the attempt and the result
      val initializationId = java.util.UUID.randomUUID().toString
      LoggingContext.withEnrichedLoggingContext(
        "initialization_id" -> initializationId
      ) { implicit loggingContext =>
        for {
          initialConditions <- readService.ledgerInitialConditions().runWith(Sink.head)
          providedLedgerId = domain.LedgerId(initialConditions.ledgerId)
          providedParticipantId = domain.ParticipantId(
            Ref.ParticipantId.assertFromString(config.participantId)
          )
          _ = logger.info(
            s"Attempting to initialize with ledger ID $providedLedgerId and participant ID $providedParticipantId"
          )
          _ <- dao.initialize(
            ledgerId = providedLedgerId,
            participantId = providedParticipantId,
          )
          initialLedgerEnd <- dao.lookupInitialLedgerEnd()
        } yield initialLedgerEnd
      }
    }
  }

  private val logger = ContextualizedLogger.get(classOf[JdbcIndexer])
}

/** @param startExclusive The last offset received from the read service.
  */
private[daml] class JdbcIndexer private[indexer] (
    startExclusive: Option[Offset],
    metrics: Metrics,
    executeUpdate: ExecuteUpdate,
)(implicit mat: Materializer, loggingContext: LoggingContext)
    extends Indexer {

  import JdbcIndexer.logger

  override def subscription(readService: state.ReadService): ResourceOwner[IndexFeedHandle] =
    new SubscriptionResourceOwner(readService)

  private def handleStateUpdate(implicit
      loggingContext: LoggingContext
  ): Flow[OffsetUpdate, Unit, NotUsed] =
    Flow[OffsetUpdate]
      .wireTap(Sink.foreach[OffsetUpdate] { case OffsetUpdate(offsetStep, update) =>
        val lastReceivedRecordTime = update.recordTime.toInstant.toEpochMilli

        logger.trace(update.description)

        metrics.daml.indexer.lastReceivedRecordTime.updateValue(lastReceivedRecordTime)
        metrics.daml.indexer.lastReceivedOffset.updateValue(offsetStep.offset.toApiString)
      })
      .via(executeUpdate.flow)
      .map(_ => ())

  private def zipWithPreviousOffset(
      initialOffset: Option[Offset]
  ): Flow[(Offset, state.Update), OffsetUpdate, NotUsed] =
    Flow[(Offset, state.Update)]
      .statefulMapConcat { () =>
        val previousOffsetRef = new AtomicReference(initialOffset)

        { offsetUpdateTuple: (Offset, state.Update) =>
          val (nextOffset, update) = offsetUpdateTuple
          val offsetStep =
            previousOffsetRef
              .getAndSet(Some(nextOffset))
              .map(IncrementalOffsetStep(_, nextOffset))
              .getOrElse(CurrentOffset(nextOffset))

          OffsetUpdate(offsetStep, update) :: Nil
        }
      }

  private class SubscriptionResourceOwner(
      readService: state.ReadService
  )(implicit loggingContext: LoggingContext)
      extends ResourceOwner[IndexFeedHandle] {
    override def acquire()(implicit context: ResourceContext): Resource[IndexFeedHandle] =
      Resource(Future {
        val (killSwitch, completionFuture) = readService
          .stateUpdates(startExclusive)
          .viaMat(KillSwitches.single)(Keep.right[NotUsed, UniqueKillSwitch])
          .via(zipWithPreviousOffset(startExclusive))
          .via(handleStateUpdate)
          .toMat(Sink.ignore)(Keep.both)
          .run()

        new SubscriptionIndexFeedHandle(killSwitch, completionFuture.map(_ => ()))
      })(handle =>
        for {
          _ <- Future(handle.killSwitch.shutdown())
          _ <- handle.completed.recover { case NonFatal(_) => () }
        } yield ()
      )
  }

  private class SubscriptionIndexFeedHandle(
      val killSwitch: KillSwitch,
      override val completed: Future[Unit],
  ) extends IndexFeedHandle

}
