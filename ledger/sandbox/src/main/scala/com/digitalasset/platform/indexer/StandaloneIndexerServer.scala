// Copyright (c) 2019 The DAML Authors. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.platform.indexer

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.daml.ledger.participant.state.v1.ReadService
import com.digitalasset.platform.common.logging.NamedLoggerFactory
import com.digitalasset.platform.common.util.DirectExecutionContext.implicitEC
import com.digitalasset.platform.resources.{Resource, ResourceOwner}

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, Future}
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
    val indexer = new RecoveringIndexer(
      actorSystem.scheduler,
      asyncTolerance,
      indexerFactory.asyncTolerance,
      loggerFactory,
    )

    def startIndexer(
        initializedIndexerFactory: JdbcIndexerFactory[Initialized]
    ): Resource[Future[Unit]] =
      indexer
        .start(
          () =>
            initializedIndexerFactory
              .owner(config.participantId, actorSystem, readService, config.jdbcUrl)
              .flatMap(_.subscription(readService))
              .acquire())

    (try {
      config.startupMode match {
        case IndexerStartupMode.MigrateOnly =>
          Resource.pure(Future.successful(()))
        case IndexerStartupMode.MigrateAndStart =>
          startIndexer(indexerFactory.migrateSchema(config.jdbcUrl))
        case IndexerStartupMode.ValidateAndStart =>
          startIndexer(indexerFactory.validateSchema(config.jdbcUrl))
      }
    } catch {
      case NonFatal(e) =>
        indexer.close()
        actorSystem.terminate()
        Resource.failed(e)
    }).flatMap { _ =>
        loggerFactory.getLogger(getClass).debug("Waiting for indexer to initialize the database")
        ResourceOwner
          .forCloseable(() =>
            new AutoCloseable {
              override def close(): Unit = {
                indexer.close()
                val _ = Await.result(actorSystem.terminate(), asyncTolerance)
              }
          })
          .acquire()
      }
      .asFutureCloseable(asyncTolerance)
  }
}
