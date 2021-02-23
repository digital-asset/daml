// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream.Materializer
import com.daml.ledger.participant.state.v1.ReadService
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.dao.events.LfValueTranslation

import scala.concurrent.ExecutionContext

final class StandaloneIndexerServer(
    readService: ReadService,
    config: IndexerConfig,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslation.Cache,
)(implicit materializer: Materializer, loggingContext: LoggingContext)
    extends ResourceOwner[Unit] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
    val indexerFactory = new JdbcIndexer.Factory(
      ServerRole.Indexer,
      config,
      readService,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
    )
    val indexer = new RecoveringIndexer(
      materializer.system.scheduler,
      materializer.executionContext,
      config.restartDelay,
    )
    config.startupMode match {
      case IndexerStartupMode.MigrateOnly =>
        Resource.unit
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
      initializedIndexerFactory: ResourceOwner[Indexer],
  )(implicit context: ResourceContext): Resource[Unit] =
    indexer
      .start(() => initializedIndexerFactory.flatMap(_.subscription(readService)).acquire())
      .map(_ => ())
}
