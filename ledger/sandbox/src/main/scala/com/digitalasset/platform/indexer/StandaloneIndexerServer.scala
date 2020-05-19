// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.ReadService
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.resources.{Resource, ResourceOwner}

import scala.concurrent.ExecutionContext

final class StandaloneIndexerServer(
    readService: ReadService,
    config: IndexerConfig,
    metrics: Metrics,
)(implicit materializer: Materializer, logCtx: LoggingContext)
    extends ResourceOwner[Unit] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit executionContext: ExecutionContext): Resource[Unit] = {
    val indexerFactory = new JdbcIndexerFactory(
      ServerRole.Indexer,
      config,
      readService,
      metrics,
    )
    val indexer = new RecoveringIndexer(materializer.system.scheduler, config.restartDelay)
    config.startupMode match {
      case IndexerStartupMode.MigrateOnly =>
        Resource.successful(())
      case IndexerStartupMode.MigrateAndStart =>
        Resource
          .fromFuture(indexerFactory.migrateSchema(config.allowExistingSchema))
          .flatMap(startIndexer(indexer, _))
          .map { _ =>
            logger.debug("Waiting for the indexer to initialize the database.")
          }
      case IndexerStartupMode.ResetAndStart =>
        Resource
          .fromFuture(indexerFactory.resetSchema())
          .flatMap(startIndexer(indexer, _))
          .map { _ =>
            logger.debug("Waiting for the indexer to initialize the database.")
          }
      case IndexerStartupMode.ValidateAndStart =>
        Resource
          .fromFuture(indexerFactory.validateSchema())
          .flatMap(startIndexer(indexer, _))
          .map { _ =>
            logger.debug("Waiting for the indexer to initialize the database.")
          }
    }
  }

  private def startIndexer(
      indexer: RecoveringIndexer,
      initializedIndexerFactory: ResourceOwner[JdbcIndexer],
  )(implicit executionContext: ExecutionContext): Resource[Unit] =
    indexer
      .start(() => initializedIndexerFactory.flatMap(_.subscription(readService)).acquire())
      .map(_ => ())
}
