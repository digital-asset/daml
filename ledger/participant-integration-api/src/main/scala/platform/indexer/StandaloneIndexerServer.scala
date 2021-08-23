// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream.Materializer
import com.daml.ledger.api.health.{Healthy, ReportsHealth}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.{FlywayMigrations, LfValueTranslationCache}

import scala.concurrent.{ExecutionContext, Future}

final class StandaloneIndexerServer(
    readService: state.ReadService,
    config: IndexerConfig,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    additionalMigrationPaths: Seq[String] = Seq.empty,
)(implicit materializer: Materializer, loggingContext: LoggingContext)
    extends ResourceOwner[ReportsHealth] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit context: ResourceContext): Resource[ReportsHealth] = {
    val indexerFactory = new JdbcIndexer.Factory(
      ServerRole.Indexer,
      config,
      readService,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      additionalMigrationPaths,
    )
    val indexer = RecoveringIndexer(
      materializer.system.scheduler,
      materializer.executionContext,
      config.restartDelay,
    )
    config.startupMode match {
      case IndexerStartupMode.MigrateAndStart =>
        Resource
          .fromFuture(
            indexerFactory
              .migrateSchema(config.allowExistingSchema)
          )
          .flatMap(startIndexer(indexer, _))
          .map { healthReporter =>
            logger.debug("Waiting for the indexer to initialize the database.")
            healthReporter
          }
      case IndexerStartupMode.ResetAndStart =>
        Resource
          .fromFuture(indexerFactory.resetSchema())
          .flatMap(startIndexer(indexer, _))
          .map { healthReporter =>
            logger.debug("Waiting for the indexer to initialize the database.")
            healthReporter
          }
      case IndexerStartupMode.ValidateAndStart =>
        Resource
          .fromFuture(indexerFactory.validateSchema())
          .flatMap(startIndexer(indexer, _))
          .map { healthReporter =>
            logger.debug("Waiting for the indexer to initialize the database.")
            healthReporter
          }
      case IndexerStartupMode.ValidateAndWaitOnly =>
        Resource
          .fromFuture(indexerFactory.validateAndWaitOnly())
          .map[ReportsHealth] { _ =>
            logger.debug("Waiting for the indexer to validate the schema migrations.")
            () => Healthy
          }
      case IndexerStartupMode.MigrateOnEmptySchemaAndStart =>
        Resource
          .fromFuture(indexerFactory.migrateOnEmptySchema())
          .flatMap(startIndexer(indexer, _))
          .map { healthReporter =>
            logger.debug("Waiting for the indexer to initialize the empty or up-to-date database.")
            healthReporter
          }
    }
  }

  private def startIndexer(
      indexer: RecoveringIndexer,
      initializedIndexerFactory: ResourceOwner[Indexer],
  )(implicit context: ResourceContext): Resource[ReportsHealth] =
    indexer
      .start(() => initializedIndexerFactory.flatMap(_.subscription(readService)).acquire())
      .map { case (indexerHealthReporter, _) => indexerHealthReporter }
}

object StandaloneIndexerServer {

  // Separate entry point for migrateOnly that serves as an operations rather than a startup command. As such it
  // does not require any of the configurations of a full-fledged indexer except for the jdbc url.
  def migrateOnly(
      jdbcUrl: String,
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean,
      allowExistingSchema: Boolean = false,
      additionalMigrationPaths: Seq[String] = Seq.empty,
  )(implicit rc: ResourceContext, loggingContext: LoggingContext): Future[Unit] = {
    val flywayMigrations = new FlywayMigrations(jdbcUrl, additionalMigrationPaths)
    flywayMigrations.migrate(allowExistingSchema, enableAppendOnlySchema)
  }
}
