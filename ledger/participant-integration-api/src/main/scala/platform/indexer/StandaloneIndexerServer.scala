// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream.Materializer
import com.daml.ledger.api.health.{Healthy, ReportsHealth}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.apiserver.LooseSyncChannel
import com.daml.platform.store.{FlywayMigrations, LfValueTranslationCache}

import scala.concurrent.{ExecutionContext, Future}

final class StandaloneIndexerServer(
    readService: state.ReadService,
    config: IndexerConfig,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
    additionalMigrationPaths: Seq[String] = Seq.empty,
    ledgerEndUpdateChannel: Option[LooseSyncChannel] = None,
)(implicit materializer: Materializer, loggingContext: LoggingContext)
    extends ResourceOwner[ReportsHealth] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit context: ResourceContext): Resource[ReportsHealth] = {
    val flywayMigrations =
      new FlywayMigrations(
        config.jdbcUrl,
        additionalMigrationPaths,
      )
    val indexerFactory = new JdbcIndexer.Factory(
      config,
      readService,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
      ledgerEndUpdateChannel,
    )
    val indexer = RecoveringIndexer(
      materializer.system.scheduler,
      materializer.executionContext,
      config.restartDelay,
    )

    def startIndexer(
        migration: Future[Unit],
        initializedDebugLogMessage: String = "Waiting for the indexer to initialize the database.",
        resetSchema: Boolean = false,
    ): Resource[ReportsHealth] =
      Resource
        .fromFuture(migration)
        .flatMap(_ => indexerFactory.initialized(resetSchema).acquire())
        .flatMap(indexer.start)
        .map { case (healthReporter, _) =>
          logger.debug(initializedDebugLogMessage)
          healthReporter
        }

    config.startupMode match {
      case IndexerStartupMode.MigrateAndStart =>
        startIndexer(
          migration = flywayMigrations.migrate(config.allowExistingSchema)
        )

      case IndexerStartupMode.ResetAndStart =>
        startIndexer(
          migration = Future.unit,
          resetSchema = true,
        )

      case IndexerStartupMode.ValidateAndStart =>
        startIndexer(
          migration = flywayMigrations.validate()
        )

      case IndexerStartupMode.ValidateAndWaitOnly =>
        Resource
          .fromFuture(
            flywayMigrations.validateAndWaitOnly(
              config.schemaMigrationAttempts,
              config.schemaMigrationAttemptBackoff,
            )
          )
          .map[ReportsHealth] { _ =>
            logger.debug("Waiting for the indexer to validate the schema migrations.")
            () => Healthy
          }

      case IndexerStartupMode.MigrateOnEmptySchemaAndStart =>
        startIndexer(
          migration = flywayMigrations.migrateOnEmptySchema(),
          initializedDebugLogMessage =
            "Waiting for the indexer to initialize the empty or up-to-date database.",
        )
    }
  }
}

object StandaloneIndexerServer {

  // Separate entry point for migrateOnly that serves as an operations rather than a startup command. As such it
  // does not require any of the configurations of a full-fledged indexer except for the jdbc url.
  def migrateOnly(
      jdbcUrl: String,
      allowExistingSchema: Boolean = false,
      additionalMigrationPaths: Seq[String] = Seq.empty,
  )(implicit rc: ResourceContext, loggingContext: LoggingContext): Future[Unit] = {
    val flywayMigrations =
      new FlywayMigrations(jdbcUrl, additionalMigrationPaths)
    flywayMigrations.migrate(allowExistingSchema)
  }
}
