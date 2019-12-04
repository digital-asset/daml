// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.index

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.{DirectExecutionContext => DEC}
import com.digitalasset.platform.index.config.{Config, StartupMode}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  private val asyncTolerance: FiniteDuration = 10.seconds

  private implicit val executionContext: ExecutionContext = DEC

  def apply(
      readService: ReadService,
      config: Config,
      loggerFactory: NamedLoggerFactory,
      metrics: MetricRegistry,
  ): Future[AutoCloseable] = {
    // ActorSystem name not allowed to contain daml-lf LedgerString characters ".:#/ "
    val actorSystem = ActorSystem(
      "StandaloneIndexerServer-" + config.participantId.filterNot(".:#/ ".toSet))

    val indexerFactory = JdbcIndexerFactory(metrics, loggerFactory)
    val indexer = RecoveringIndexer(
      actorSystem.scheduler,
      asyncTolerance,
      indexerFactory.asyncTolerance,
      loggerFactory)

    val promise = Promise[Unit]

    def startIndexer(initializedIndexerFactory: JdbcIndexerFactory[Initialized]): Future[Unit] = {
      println("Starting the indexer.")
      indexer
        .start(
          () =>
            initializedIndexerFactory
              .create(config.participantId, actorSystem, readService, config.jdbcUrl)
              .flatMap { indexer =>
                // signal when ready
                promise.trySuccess(())
                indexer.subscribe(readService)
            })
        .map(_ => ())
    }

    try {
      config.startupMode match {
        case StartupMode.MigrateOnly =>
          promise.success(())
        case StartupMode.MigrateAndStart =>
          startIndexer(indexerFactory.migrateSchema(config.jdbcUrl))
        case StartupMode.ValidateAndStart =>
          startIndexer(indexerFactory.validateSchema(config.jdbcUrl))
      }
    } catch {
      case NonFatal(e) =>
        indexer.close()
        actorSystem.terminate()
        promise.failure(e)
    }

    promise.future.map { _ =>
      loggerFactory.getLogger(getClass).debug("Waiting for indexer to initialize the database")
      new AutoCloseable {
        override def close(): Unit = {
          indexer.close()
          val _ = Await.result(actorSystem.terminate(), asyncTolerance)
        }
      }
    }
  }
}
