// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import java.util.concurrent.Executors

import akka.stream.{KillSwitch, Materializer}
import com.daml.ledger.participant.state.v2.ReadService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.indexer.ha.{HaConfig, HaCoordinator, Handle, NoopHaCoordinator}
import com.daml.platform.indexer.parallel.AsyncSupport._
import com.daml.platform.indexer.Indexer
import com.daml.platform.store.appendonlydao.DbDispatcher
import com.daml.platform.store.backend.DataSourceStorageBackend.DataSourceConfig
import com.daml.platform.store.backend.{DBLockStorageBackend, DataSourceStorageBackend}
import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

object ParallelIndexerFactory {

  def apply(
      jdbcUrl: String,
      inputMappingParallelism: Int,
      batchingParallelism: Int,
      ingestionParallelism: Int,
      dataSourceConfig: DataSourceConfig,
      haConfig: HaConfig,
      metrics: Metrics,
      storageBackend: DBLockStorageBackend with DataSourceStorageBackend,
      initializeParallelIngestion: InitializeParallelIngestion,
      parallelIndexerSubscription: ParallelIndexerSubscription[_],
      mat: Materializer,
      readService: ReadService,
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
        if (storageBackend.dbLockSupported && haConfig.enable)
          ResourceOwner
            .forExecutorService(() =>
              ExecutionContext.fromExecutorService(
                Executors.newFixedThreadPool(
                  1,
                  new ThreadFactoryBuilder().setNameFormat(s"ha-coordinator-%d").build,
                )
              )
            )
            .map(
              HaCoordinator.databaseLockBasedHaCoordinator(
                // this DataSource will be used to spawn the main connection where we keep the Indexer Main Lock
                // The life-cycle of such connections matches the life-cycle of a protectedExecution
                dataSource = storageBackend.createDataSource(jdbcUrl),
                storageBackend = storageBackend,
                _,
                scheduler = mat.system.scheduler,
                haConfig = haConfig,
              )
            )
        else
          ResourceOwner.successful(NoopHaCoordinator)
    } yield toIndexer { resourceContext =>
      implicit val rc: ResourceContext = resourceContext
      implicit val ec: ExecutionContext = resourceContext.executionContext
      haCoordinator.protectedExecution { connectionInitializer =>
        val killSwitchPromise = Promise[KillSwitch]()

        val completionFuture = DbDispatcher
          .owner(
            // this is the DataSource which will be wrapped by HikariCP, and which will drive the ingestion
            // therefore this needs to be configured with the connection-init-hook, what we get from HaCoordinator
            dataSource = storageBackend.createDataSource(
              jdbcUrl = jdbcUrl,
              dataSourceConfig = dataSourceConfig,
              connectionInitHook = Some(connectionInitializer.initialize),
            ),
            serverRole = ServerRole.Indexer,
            connectionPoolSize = ingestionParallelism + 1, // + 1 for the tailing ledger_end updates
            connectionTimeout = FiniteDuration(
              250,
              "millis",
            ), // 250 millis is the lowest possible value for this Hikari configuration (see HikariConfig JavaDoc)
            metrics = metrics,
          )
          .use { dbDispatcher =>
            initializeParallelIngestion(
              dbDispatcher = dbDispatcher,
              readService = readService,
              ec = ec,
              mat = mat,
            ).map(
              parallelIndexerSubscription(
                inputMapperExecutor = inputMapperExecutor,
                batcherExecutor = batcherExecutor,
                dbDispatcher = dbDispatcher,
                materializer = mat,
              )
            ).andThen {
              // the tricky bit:
              // the future in the completion handler will be this one
              // but the future for signaling for the HaCoordinator, that the protected execution is initialized, needs to complete precisely here
              case Success(handle) => killSwitchPromise.success(handle.killSwitch)
              case Failure(ex) => killSwitchPromise.failure(ex)
            }.flatMap(_.completed)
          }

        killSwitchPromise.future
          .map(Handle(completionFuture.map(_ => ()), _))
      }
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
