// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform

import akka.stream.scaladsl.Sink
import akka.stream.{KillSwitch, Materializer}
import com.daml.ledger.api.domain
import com.daml.ledger.participant.state.v2.ReadService
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.IndexerConfig
import com.daml.platform.indexer.ha.{HaCoordinator, Handle, NoopHaCoordinator}
import com.daml.platform.store.DbSupport.ParticipantDataSourceConfig
import com.daml.platform.store.backend._
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.{DbSupport, DbType}
import com.google.common.util.concurrent.ThreadFactoryBuilder

import java.util.Timer
import java.util.concurrent.Executors
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

private[platform] case class InitializeParticipant(
    // TODO LLP: Not right to use IndexerConfig
    config: IndexerConfig,
    lapiDbSupport: DbSupport,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    providedParticipantId: Ref.ParticipantId,
    metrics: Metrics,
)(implicit loggingContext: LoggingContext) {

  private val logger = ContextualizedLogger.get(classOf[InitializeParticipant])

  // TODO LLP: Deduplicate with logic in JdbcIndexer
  def initialize(
      participantConfig: ParticipantConfig,
      readService: ReadService,
      ec: ExecutionContext,
      mat: Materializer,
  )(implicit loggingContext: LoggingContext): ResourceOwner[ParticipantInMemoryState] = {
    implicit val executionContext: ExecutionContext = ec
    for {
      _ <- initializeInHa(readService, ec, mat)
      ledgerEnd <- fetchLedgerEnd(lapiDbSupport, metrics)
      participantInMemoryState <- ParticipantInMemoryState.owner(
        ledgerEnd = ledgerEnd,
        apiStreamShutdownTimeout = participantConfig.indexService.apiStreamShutdownTimeout,
        bufferedStreamsPageSize = participantConfig.indexService.bufferedStreamsPageSize,
        maxContractStateCacheSize = participantConfig.indexService.maxContractStateCacheSize,
        maxContractKeyStateCacheSize = participantConfig.indexService.maxContractKeyStateCacheSize,
        maxTransactionsInMemoryFanOutBufferSize =
          participantConfig.indexService.maxTransactionsInMemoryFanOutBufferSize,
        cachesUpdaterExecutionContext = executionContext, // TODO LLP: Dedicated ExecutionContext
        servicesExecutionContext = executionContext,
        metrics = metrics,
      )
    } yield participantInMemoryState
  }

  private def fetchLedgerEnd(dbSupport: DbSupport, metrics: Metrics)(implicit
      loggingContext: LoggingContext
  ): ResourceOwner[ParameterStorageBackend.LedgerEnd] = {
    val dbDispatcher = dbSupport.dbDispatcher
    ResourceOwner.forFuture(() =>
      dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(
        dbSupport.storageBackendFactory.createParameterStorageBackend.ledgerEnd
      )
    )
  }

  private def initializeInHa(
      readService: ReadService,
      ec: ExecutionContext,
      mat: Materializer,
  )(implicit loggingContext: LoggingContext): ResourceOwner[Handle] = {
    val factory = StorageBackendFactory.of(DbType.jdbcType(participantDataSourceConfig.jdbcUrl))
    val dataSourceStorageBackend = factory.createDataSourceStorageBackend
    val ingestionStorageBackend = factory.createIngestionStorageBackend
    val parameterStorageBackend = factory.createParameterStorageBackend
    val dbLockStorageBackend = factory.createDBLockStorageBackend
    val dbConfig =
      IndexerConfig.dataSourceProperties(config).createDbConfig(participantDataSourceConfig)

    implicit val rc: ResourceContext = ResourceContext(scala.concurrent.ExecutionContext.global)
    for {
      haCoordinator <-
        buildHaCoordinator(dataSourceStorageBackend, dbLockStorageBackend, dbConfig)
    } yield haCoordinator.protectedExecution { connectionInitializer =>
      initializeHandle(
        DbDispatcher
          .owner(
            // this is the DataSource which will be wrapped by HikariCP, and which will drive the ingestion
            // therefore this needs to be configured with the connection-init-hook, what we get from HaCoordinator
            dataSource = dataSourceStorageBackend.createDataSource(
              dataSourceConfig = dbConfig.dataSourceConfig,
              connectionInitHook = Some(connectionInitializer.initialize),
            ),
            serverRole = ServerRole.Indexer,
            connectionPoolSize = dbConfig.connectionPool.connectionPoolSize,
            connectionTimeout = dbConfig.connectionPool.connectionTimeout,
            metrics = metrics,
          )
      )(
        initializedIndexDb(_, mat, readService, ingestionStorageBackend, parameterStorageBackend)(
          ec,
          loggingContext,
        ).map(_ => Handle(Future.unit, NoOpKillSwitch))(ec)
      )
    }
  }

  private def buildHaCoordinator(
      dataSourceStorageBackend: DataSourceStorageBackend,
      dbLockStorageBackend: DBLockStorageBackend,
      dbConfig: DbSupport.DbConfig,
  ): ResourceOwner[HaCoordinator] = {
    if (dbLockStorageBackend.dbLockSupported) {
      for {
        executionContext <- ResourceOwner
          .forExecutorService(() =>
            ExecutionContext.fromExecutorService(
              Executors.newFixedThreadPool(
                1,
                new ThreadFactoryBuilder()
                  .setNameFormat(s"ha-coordinator-initialization-%d")
                  .build,
              ),
              throwable =>
                logger.error(
                  "ExecutionContext has failed with an exception",
                  throwable,
                ),
            )
          )
        timer <- ResourceOwner.forTimer(() => new Timer)
        // TODO LLP: Update description
        // this DataSource will be used to spawn the main connection where we keep the Indexer Main Lock
        // The life-cycle of such connections matches the life-cycle of a protectedExecution
        dataSource = dataSourceStorageBackend.createDataSource(dbConfig.dataSourceConfig)
        haCoordinator = HaCoordinator.databaseLockBasedHaCoordinator(
          connectionFactory = () => dataSource.getConnection,
          storageBackend = dbLockStorageBackend,
          executionContext = executionContext,
          timer = timer,
          haConfig = config.highAvailability,
        )
      } yield haCoordinator
    } else
      ResourceOwner.successful(NoopHaCoordinator)
  }

  private def initializedIndexDb(
      dbDispatcher: DbDispatcher,
      mat: Materializer,
      readService: ReadService,
      ingestionStorageBackend: IngestionStorageBackend[_],
      parameterStorageBackend: ParameterStorageBackend,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: LoggingContext,
  ): Future[Unit] =
    for {
      initialConditions <- readService.ledgerInitialConditions().runWith(Sink.head)(mat)
      providedLedgerId = domain.LedgerId(initialConditions.ledgerId)
      _ = logger.info(
        s"Attempting to initialize with ledger ID $providedLedgerId and participant ID $providedParticipantId"
      )
      _ <- dbDispatcher.executeSql(metrics.daml.index.db.initializeLedgerParameters)(
        parameterStorageBackend.initializeParameters(
          ParameterStorageBackend.IdentityParams(
            ledgerId = providedLedgerId,
            participantId = domain.ParticipantId(providedParticipantId),
          )
        )
      )
      ledgerEnd <- dbDispatcher.executeSql(metrics.daml.index.db.getLedgerEnd)(
        parameterStorageBackend.ledgerEnd
      )
      _ <- dbDispatcher.executeSql(metrics.daml.parallelIndexer.initialization)(
        ingestionStorageBackend.deletePartiallyIngestedData(ledgerEnd)
      )
    } yield ()

  def initializeHandle[T](
      owner: ResourceOwner[T]
  )(initHandle: T => Future[Handle])(implicit rc: ResourceContext): Future[Handle] = {
    implicit val ec: ExecutionContext = rc.executionContext
    val killSwitchPromise = Promise[KillSwitch]()
    val completed = owner
      .use(resource =>
        initHandle(resource)
          .andThen {
            // the tricky bit:
            // the future in the completion handler will be this one
            // but the future for signaling completion of initialization (the Future of the result), needs to complete precisely here
            case Success(handle) => killSwitchPromise.success(handle.killSwitch)
          }
          .flatMap(_.completed)
      )
      .andThen {
        // if error happens:
        //   - at Resource initialization (inside ResourceOwner.acquire()): result should complete with a Failure
        //   - at initHandle: result should complete with a Failure
        //   - at the execution spawned by initHandle (represented by the result Handle's complete): result should be with a success
        // In the last case it is already finished the promise with a success, and this tryFailure will not succeed (returning false).
        // In the other two cases the promise was not completed, and we complete here successfully with a failure.
        case Failure(ex) => killSwitchPromise.tryFailure(ex)
      }
    killSwitchPromise.future
      .map(Handle(completed, _))
  }
}

object NoOpKillSwitch extends KillSwitch {
  override def shutdown(): Unit = ()

  override def abort(ex: Throwable): Unit = ()
}
