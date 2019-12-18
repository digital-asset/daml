// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.dec.DirectExecutionContext

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future, Promise}
import scala.util.control.NonFatal

// Main entry point to start an indexer server.
// See v2.ReferenceServer for the usage
object StandaloneIndexerServer {
  private val asyncTolerance: FiniteDuration = 10.seconds

  def apply(
      readService: ReadService,
      config: IndexerConfig,
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

    def startIndexer(initializedIndexerFactory: JdbcIndexerFactory[Initialized]): Future[Unit] =
      indexer
        .start(
          () =>
            initializedIndexerFactory
              .create(config.participantId, actorSystem, readService, config.jdbcUrl)
              .flatMap { indexer =>
                // signal when ready
                promise.trySuccess(())
                indexer.subscribe(readService)
              }(DirectExecutionContext))
        .map(_ => ())(DirectExecutionContext)

    try {
      config.startupMode match {
        case IndexerStartupMode.MigrateOnly =>
          promise.success(())
        case IndexerStartupMode.MigrateAndStart =>
          startIndexer(indexerFactory.migrateSchema(config.jdbcUrl))
        case IndexerStartupMode.ValidateAndStart =>
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
    }(DirectExecutionContext)
  }
}
