// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import com.daml.executors.InstrumentedExecutors
import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.digitalasset.canton.ledger.participant.state.Update
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
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
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.PekkoUtil.{Commit, FutureQueueConsumer}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.pekko.Done
import org.apache.pekko.stream.{KillSwitch, Materializer}

import java.util.{Timer, concurrent}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

object ParallelIndexerFactory {

  def apply(
      inputMappingParallelism: Int,
      batchingParallelism: Int,
      dbConfig: DbConfig,
      haConfig: HaConfig,
      metrics: LedgerApiServerMetrics,
      dbLockStorageBackend: DBLockStorageBackend,
      dataSourceStorageBackend: DataSourceStorageBackend,
      initializeParallelIngestion: InitializeParallelIngestion,
      parallelIndexerSubscription: ParallelIndexerSubscription[?],
      meteringAggregator: DbDispatcher => ResourceOwner[Unit],
      mat: Materializer,
      executionContext: ExecutionContext,
      initializeInMemoryState: Option[LedgerEnd] => Future[Unit],
      loggerFactory: NamedLoggerFactory,
      indexerDbDispatcherOverride: Option[DbDispatcher],
      clock: Clock,
  )(implicit traceContext: TraceContext): ResourceOwner[Indexer] = {
    val logger = TracedLogger(loggerFactory.getLogger(getClass))
    for {
      inputMapperExecutor <- asyncPool(
        inputMappingParallelism,
        "input-mapping-pool",
        metrics.indexer.inputMapping.executor,
        loggerFactory,
      ).afterReleased(logger.debug("Input Mapping Threadpool released"))
      batcherExecutor <- asyncPool(
        batchingParallelism,
        "batching-pool",
        metrics.indexer.batching.executor,
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
                override def execute(command: Runnable): Unit =
                  // this will execute on the same thread which started the Executor.execute()
                  command.run()
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
    } yield { (repairMode: Boolean) => (commit: Commit) =>
      implicit val ec: ExecutionContext = executionContext
      implicit val rc: ResourceContext = ResourceContext(ec)
      val futureQueueConsumerFactoryPromise =
        Promise[Future[Done] => FutureQueueConsumer[Traced[Update]]]()
      val haProtectedExecutionHandle = haCoordinator
        .protectedExecution { connectionInitializer =>
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
            for {
              initialLedgerEnd <- initializeParallelIngestion(
                dbDispatcher = dbDispatcher,
                initializeInMemoryState = initializeInMemoryState,
              )
              (handle, futureQueueForCompletion) = parallelIndexerSubscription(
                inputMapperExecutor = inputMapperExecutor,
                batcherExecutor = batcherExecutor,
                dbDispatcher = dbDispatcher,
                materializer = mat,
                initialLedgerEnd = initialLedgerEnd,
                commit = commit,
                clock = clock,
                repairMode = repairMode,
              )
            } yield {
              futureQueueConsumerFactoryPromise.success(completion =>
                FutureQueueConsumer(
                  futureQueue = futureQueueForCompletion(completion),
                  fromExclusive = initialLedgerEnd.map(_.lastOffset.unwrap).getOrElse(0L),
                )
              )
              handle
            }
          }
          indexingHandleF.onComplete {
            case Success(indexingHandle) =>
              logger.info("Indexer initialized, indexing started.")
              // in this case futureQueueConsumerPromise is already completed successfully (see above)
              indexingHandle.completed.onComplete {
                case Success(_) =>
                  logger.info("Indexing finished.")

                case Failure(failure) =>
                  logger.info(s"Indexing finished with failure: ${failure.getMessage}")
              }

            case Failure(failure) =>
              logger.info(s"Indexer initialization failed: ${failure.getMessage}")
            // in this case we entered the protected execution, but failed initialization,
            // futureQueueConsumerPromise cannot be set from here to failure, since the HA protected surroundings
            // need to be torn down first
          }
          indexingHandleF
        }

      haProtectedExecutionHandle.completed
        .onComplete {
          case Success(_) =>
            // here the indexing finished successfully and everything torn down successfully too
            // here we attempt to complete futureQueueConsumerPromise since if it is not completed yet,
            // it would be a programming error
            futureQueueConsumerFactoryPromise.tryFailure(
              new IllegalStateException(
                "Programming error: at this point the futureQueueConsumer should be already completed."
              )
            )

          case Failure(failure) =>
            // in either case of failures we try to complete the futureQueueConsumerPromise,
            // but in the case indexing failed/aborted we already should have it completed,
            // so this should succeed if failure arises during HA initialization or indexer initialization.
            futureQueueConsumerFactoryPromise.tryFailure(failure)
        }
      // so that the resulting FutureQueue in the FutureQueueConsumer has a completion future, which completes after not only indexing, but after indexing resources and HA protected execution are both torn down
      futureQueueConsumerFactoryPromise.future.map(
        _(haProtectedExecutionHandle.completed.map(_ => Done))
      )
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
}
