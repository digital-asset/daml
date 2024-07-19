// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.digitalasset.canton.ledger.api.health.ReportsHealth
import com.digitalasset.canton.ledger.participant.state.ReadService
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.platform.InMemoryState
import com.digitalasset.canton.platform.index.InMemoryStateUpdater
import com.digitalasset.canton.platform.indexer.ha.HaConfig
import com.digitalasset.canton.platform.indexer.parallel.ReassignmentOffsetPersistence
import com.digitalasset.canton.platform.store.DbSupport.{
  DataSourceProperties,
  ParticipantDataSourceConfig,
}
import com.digitalasset.canton.platform.store.FlywayMigrations
import com.digitalasset.canton.platform.store.dao.DbDispatcher
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Ref
import io.opentelemetry.api.trace.Tracer
import org.apache.pekko.stream.Materializer

import scala.concurrent.{ExecutionContext, Future}

final class IndexerServiceOwner(
    participantId: Ref.ParticipantId,
    participantDataSourceConfig: ParticipantDataSourceConfig,
    readService: ReadService,
    config: IndexerConfig,
    metrics: LedgerApiServerMetrics,
    inMemoryState: InMemoryState,
    inMemoryStateUpdaterFlow: InMemoryStateUpdater.UpdaterFlow,
    executionContext: ExecutionContext,
    tracer: Tracer,
    val loggerFactory: NamedLoggerFactory,
    startupMode: IndexerStartupMode,
    dataSourceProperties: DataSourceProperties,
    highAvailability: HaConfig,
    indexServiceDbDispatcher: Option[DbDispatcher],
    excludedPackageIds: Set[Ref.PackageId],
    clock: Clock,
    reassignmentOffsetPersistence: ReassignmentOffsetPersistence,
)(implicit materializer: Materializer, traceContext: TraceContext)
    extends ResourceOwner[ReportsHealth]
    with NamedLogging {

  override def acquire()(implicit context: ResourceContext): Resource[ReportsHealth] = {
    val flywayMigrations =
      new FlywayMigrations(
        participantDataSourceConfig.jdbcUrl,
        loggerFactory,
      )(executionContext, traceContext)
    val indexerFactory = new JdbcIndexer.Factory(
      participantId,
      participantDataSourceConfig,
      config,
      excludedPackageIds,
      readService,
      metrics,
      inMemoryState,
      inMemoryStateUpdaterFlow,
      executionContext,
      tracer,
      loggerFactory,
      dataSourceProperties,
      highAvailability,
      indexServiceDbDispatcher,
      clock,
      reassignmentOffsetPersistence,
      (_, _) => Future.successful(()), // will be fixed with the big-bang fusion PR
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
        .flatMap(_ => indexerFactory.initialized().acquire())
        .flatMap(indexer.start)
        .map { case (healthReporter, _) =>
          logger.debug(initializedDebugLogMessage)
          healthReporter
        }

    startupMode match {
      case IndexerStartupMode.JustStart =>
        startIndexer(
          migration = Future.unit
        )
      case IndexerStartupMode.MigrateAndStart =>
        startIndexer(
          migration = flywayMigrations.migrate()
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
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Future[Unit] = {
    val flywayMigrations =
      new FlywayMigrations(jdbcUrl, loggerFactory)
    flywayMigrations.migrate()
  }
}
