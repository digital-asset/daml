// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.ledger.participant.state.ReadService
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.ResourceOwnerOps
import com.digitalasset.canton.platform.config.ServerRole
import com.digitalasset.canton.platform.indexer.Indexer
import com.digitalasset.canton.platform.indexer.ha.{
  HaConfig,
  HaCoordinator,
  Handle,
  NoopHaCoordinator,
}
import com.digitalasset.canton.platform.indexer.parallel.AsyncSupport.*
import com.digitalasset.canton.platform.store.DbSupport.DbConfig
import com.digitalasset.canton.platform.store.backend.ParameterStorageBackend.LedgerEnd
import com.digitalasset.canton.platform.store.backend.{
  DBLockStorageBackend,
  DataSourceStorageBackend,
}
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.pekko.stream.{KillSwitch, Materializer}

import java.util.{Timer, concurrent}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

object ParallelIndexerFactory {

  def apply(
      inputMappingParallelism: Int,
      batchingParallelism: Int,
      dbConfig: DbConfig,
      haConfig: HaConfig,
      metrics: Metrics,
      dbLockStorageBackend: DBLockStorageBackend,
      dataSourceStorageBackend: DataSourceStorageBackend,
      initializeParallelIngestion: InitializeParallelIngestion,
      parallelIndexerSubscription: ParallelIndexerSubscription[?],
      meteringAggregator: DbDispatcher => ResourceOwner[Unit],
      mat: Materializer,
      readService: ReadService,
      initializeInMemoryState: DbDispatcher => LedgerEnd => Future[Unit],
      loggerFactory: NamedLoggerFactory,
      indexerDbDispatcherOverride: Option[DbDispatcher],
  )(implicit traceContext: TraceContext): ResourceOwner[Indexer] = {
    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    for {
      inputMapperExecutor <- asyncPool(
        inputMappingParallelism,
        "input-mapping-pool",
        metrics.parallelIndexer.inputMapping.executor,
        loggerFactory,
      ).afterReleased(logger.debug("Input Mapping Threadpool released"))
      batcherExecutor <- asyncPool(
        batchingParallelism,
        "batching-pool",
        metrics.parallelIndexer.batching.executor,
        loggerFactory,
      ).afterReleased(logger.debug("Batching Threadpool released"))
      haCoordinator <-
        if (dbLockStorageBackend.dbLockSupported) {
          for {
            executionContext <- ResourceOwner
              .forExecutorService(() =>
                ExecutionContext.fromExecutorService(
                  InstrumentedExecutors.newFixedThreadPoolWithFactory(
                    "ha-coordinator",
                    1,
                    new ThreadFactoryBuilder().setNameFormat("ha-coordinator-%d").build,
                    throwable =>
                      logger
                        .error(
                          "ExecutionContext has failed with an exception",
                          throwable,
                        ),
                  )
                )
              )
              .afterReleased(logger.debug("HaCoordinator single-threadpool released"))
            timer <- ResourceOwner
              .forTimer(() => new Timer)
              .afterReleased(logger.debug("HaCoordinator Timer released"))

            // this DataSource will be used to spawn the main connection where we keep the Indexer Main Lock
            // The life-cycle of such connections matches the life-cycle of a protectedExecution
            dataSource = dataSourceStorageBackend.createDataSource(
              dbConfig.dataSourceConfig,
              loggerFactory,
            )
          } yield HaCoordinator.databaseLockBasedHaCoordinator(
            mainConnectionFactory = () => {
              val connection = dataSource.getConnection
              val directExecutor = new concurrent.Executor {
                override def execute(command: Runnable): Unit = {
                  // this will execute on the same thread which started the Executor.execute()
                  command.run()
                }
              }
              // direct executor is beneficial in context of main connection and network timeout:
              // all socket/Connection closure will be happening on the thread which called the JDBC execute,
              // instead of happening asynchronously - after error with network timeout the Connection
              // needs to be closed anyway.
              connection.setNetworkTimeout(
                directExecutor,
                haConfig.mainLockCheckerJdbcNetworkTimeout.duration.toMillis.toInt,
              )
              connection
            },
            storageBackend = dbLockStorageBackend,
            executionContext = executionContext,
            timer = timer,
            haConfig = haConfig,
            loggerFactory,
          )
        } else
          ResourceOwner.successful(NoopHaCoordinator)
    } yield toIndexer { implicit resourceContext =>
      implicit val ec: ExecutionContext = resourceContext.executionContext
      haCoordinator.protectedExecution { connectionInitializer =>
        val indexingHandleF = initializeHandle(
          for {
            dbDispatcher <- indexerDbDispatcherOverride
              .map(ResourceOwner.successful)
              .getOrElse(
                DbDispatcher
                  .owner(
                    // this is the DataSource which will be wrapped by HikariCP, and which will drive the ingestion
                    // therefore this needs to be configured with the connection-init-hook, what we get from HaCoordinator
                    dataSource = dataSourceStorageBackend.createDataSource(
                      dataSourceConfig = dbConfig.dataSourceConfig,
                      connectionInitHook = Some(connectionInitializer.initialize),
                      loggerFactory = loggerFactory,
                    ),
                    serverRole = ServerRole.Indexer,
                    connectionPoolSize = dbConfig.connectionPool.connectionPoolSize,
                    connectionTimeout = dbConfig.connectionPool.connectionTimeout,
                    metrics = metrics,
                    loggerFactory = loggerFactory,
                  )
                  .afterReleased(logger.debug("Indexing DbDispatcher released"))
              )
            _ <- meteringAggregator(dbDispatcher)
              .afterReleased(logger.debug("Metering Aggregator released"))
          } yield dbDispatcher
        ) { dbDispatcher =>
          initializeParallelIngestion(
            dbDispatcher = dbDispatcher,
            additionalInitialization = initializeInMemoryState(dbDispatcher),
            readService = readService,
            mat = mat,
            ec = ec,
          ).map(
            parallelIndexerSubscription(
              inputMapperExecutor = inputMapperExecutor,
              batcherExecutor = batcherExecutor,
              dbDispatcher = dbDispatcher,
              materializer = mat,
            )
          )
        }
        indexingHandleF.onComplete {
          case Success(indexingHandle) =>
            logger.info("Indexer initialized, indexing started.")
            indexingHandle.completed.onComplete {
              case Success(_) =>
                logger.info("Indexing finished.")

              case Failure(failure) =>
                logger.info(s"Indexing finished with failure: ${failure.getMessage}")
            }

          case Failure(failure) =>
            logger.info(s"Indexer initialization failed: ${failure.getMessage}")
        }
        indexingHandleF
      }
    }
  }

  /** Helper function to combine a ResourceOwner and an initialization function to initialize a Handle.
    *
    * @param owner A ResourceOwner which needs to be used to spawn a resource needed by initHandle
    * @param initHandle Asynchronous initialization function to create a Handle
    * @return A Future of a Handle where Future encapsulates initialization (as completed initialization completed)
    */
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

  def toIndexer(subscription: ResourceContext => Handle): Indexer =
    new Indexer {
      override def acquire()(implicit context: ResourceContext): Resource[Future[Unit]] = {
        Resource {
          Future {
            subscription(context)
          }
        } { handle =>
          handle.killSwitch.shutdown()
          handle.completed.recover { case NonFatal(_) =>
            ()
          }
        }.map(_.completed)
      }
    }
}
