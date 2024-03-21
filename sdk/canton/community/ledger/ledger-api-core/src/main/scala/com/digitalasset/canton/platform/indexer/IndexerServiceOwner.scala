// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.lf.data.Ref
import com.digitalasset.canton.ledger.api.health.{Healthy, ReportsHealth}
import com.digitalasset.canton.ledger.participant.state.v2.ReadService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.Metrics
import com.digitalasset.canton.platform.InMemoryState
import com.digitalasset.canton.platform.index.InMemoryStateUpdater
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.store.DbSupport.{
  DataSourceProperties,
  ParticipantDataSourceConfig,
}
import com.digitalasset.canton.platform.store.FlywayMigrations
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

final class IndexerServiceOwner(
    participantId: Ref.ParticipantId,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    readService: ReadService,
    config: IndexerConfig,
    metrics: Metrics,
    inMemoryState: InMemoryState,
    inMemoryStateUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
    additionalMigrationPaths: Seq[String] = Seq.empty,
    executionContext: ExecutionContext,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
    multiDomainEnabled: Boolean,
    startupMode: IndexerStartupMode,
    dataSourceProperties: DataSourceProperties,
    highAvailability: HaConfig,
)(implicit materializer: Materializer, traceContext: TraceContext)
    extends ResourceOwner[ReportsHealth]
    with NamedLogging {

  override def acquire()(implicit context: ResourceContext): Resource[ReportsHealth] = {
    val flywayMigrations =
      new FlywayMigrations(
        participantDataSourceConfig.jdbcUrl,
        additionalMigrationPaths,
        loggerFactory,
      )
    val indexerFactory = new JdbcIndexer.Factory(
      participantId,
      participantDataSourceConfig,
      config,
      readService,
      metrics,
      inMemoryState,
      inMemoryStateUpdaterFlow,
      executionContext,
      tracer,
      loggerFactory,
      multiDomainEnabled,
      dataSourceProperties,
      highAvailability,
    )
    val indexer = RecoveringIndexer(
      materializer.system.scheduler,
      materializer.executionContext,
      config.restartDelay.asFiniteApproximation,
      loggerFactory,
    )

    def startIndexer(
        migration: Future[Unit],
        initializedDebugLogMessage: String = "Waiting for the indexer to initialize the database.",
    ): Resource[ReportsHealth] =
      Resource
        .fromFuture(migration)
        .flatMap(_ => indexerFactory.initialized(logger).acquire())
        .flatMap(indexer.start)
        .map { case (healthReporter, _) =>
          logger.debug(initializedDebugLogMessage)
          healthReporter
        }

    startupMode match {
      case IndexerStartupMode.MigrateAndStart =>
        startIndexer(
          migration = flywayMigrations.migrate()
        )

      case IndexerStartupMode.ValidateAndStart =>
        startIndexer(
          migration = flywayMigrations.validate()
        )

      case IndexerStartupMode.ValidateAndWaitOnly(
            schemaMigrationAttempts,
            schemaMigrationAttemptBackoff,
          ) =>
        Resource
          .fromFuture(
            flywayMigrations
              .validateAndWaitOnly(schemaMigrationAttempts, schemaMigrationAttemptBackoff)
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

object IndexerServiceOwner {

  // Separate entry point for migrateOnly that serves as an operations rather than a startup command. As such it
  // does not require any of the configurations of a full-fledged indexer except for the jdbc url.
  def migrateOnly(
      jdbcUrl: String,
      loggerFactory: NamedLoggerFactory,
      additionalMigrationPaths: Seq[String] = Seq.empty,
  )(implicit rc: ResourceContext, traceContext: TraceContext): Future[Unit] = {
    val flywayMigrations =
      new FlywayMigrations(jdbcUrl, additionalMigrationPaths, loggerFactory)
    flywayMigrations.migrate()
  }
}
