// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.stream.{KillSwitch, Materializer}
import com.daml.ledger.participant.state.v2.ReadService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.Indexer
import com.daml.platform.indexer.ha.{HaConfig, HaCoordinator, Handle, NoopHaCoordinator}
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.store.DbSupport.DbConfig
import com.daml.platform.store.backend.{DBLockStorageBackend, DataSourceStorageBackend}
import com.daml.platform.store.dao.DbDispatcher
import com.daml.platform.store.interning.StringInterningView
import com.google.common.util.concurrent.ThreadFactoryBuilder
import java.util.{Timer, concurrent}
import java.util.concurrent.Executors

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
      parallelIndexerSubscription: ParallelIndexerSubscription[_],
      meteringAggregator: DbDispatcher => ResourceOwner[Unit],
      mat: Materializer,
      readService: ReadService,
      stringInterningViewO: Option[StringInterningView],
  )(implicit loggingContext: LoggingContext): ResourceOwner[Indexer] =
    for {
      inputMapperExecutor <- asyncPool(
        inputMappingParallelism,
        "input-mapping-pool",
        Some(metrics.daml.parallelIndexer.inputMapping.executor -> metrics.registry),
      )
      batcherExecutor <- asyncPool(
        batchingParallelism,
        "batching-pool",
        Some(metrics.daml.parallelIndexer.batching.executor -> metrics.registry),
      )
      haCoordinator <-
        if (dbLockStorageBackend.dbLockSupported) {
          for {
            executionContext <- ResourceOwner
              .forExecutorService(() =>
                ExecutionContext.fromExecutorService(
                  Executors.newFixedThreadPool(
                    1,
                    new ThreadFactoryBuilder().setNameFormat(s"ha-coordinator-%d").build,
                  ),
                  throwable =>
                    ContextualizedLogger
                      .get(getClass)
                      .error(
                        "ExecutionContext has failed with an exception",
                        throwable,
                      ),
                )
              )
            timer <- ResourceOwner.forTimer(() => new Timer)
            // this DataSource will be used to spawn the main connection where we keep the Indexer Main Lock
            // The life-cycle of such connections matches the life-cycle of a protectedExecution
            dataSource = dataSourceStorageBackend.createDataSource(dbConfig.dataSourceConfig)
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
                haConfig.mainLockCheckerJdbcNetworkTimeoutMillis,
              )
              connection
            },
            storageBackend = dbLockStorageBackend,
            executionContext = executionContext,
            timer = timer,
            haConfig = haConfig,
          )
        } else
          ResourceOwner.successful(NoopHaCoordinator)
    } yield toIndexer { implicit resourceContext =>
      implicit val ec: ExecutionContext = resourceContext.executionContext
      haCoordinator.protectedExecution(connectionInitializer =>
        initializeHandle(
          for {
            dbDispatcher <- DbDispatcher
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
            _ <- meteringAggregator(dbDispatcher)
          } yield dbDispatcher
        ) { dbDispatcher =>
          val stringInterningView = stringInterningViewO.getOrElse(new StringInterningView)
          initializeParallelIngestion(
            dbDispatcher = dbDispatcher,
            updatingStringInterningView = stringInterningView,
            readService = readService,
            ec = ec,
            mat = mat,
          ).map(
            parallelIndexerSubscription(
              inputMapperExecutor = inputMapperExecutor,
              batcherExecutor = batcherExecutor,
              dbDispatcher = dbDispatcher,
              stringInterningView = stringInterningView,
              materializer = mat,
            )
          )
        }
      )
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
