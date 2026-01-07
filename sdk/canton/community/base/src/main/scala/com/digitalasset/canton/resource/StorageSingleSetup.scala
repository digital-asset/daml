// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.daml.metrics.api.{HistogramInventory, MetricName, MetricsContext}
import com.digitalasset.canton.config.{
  DbConfig,
  ProcessingTimeout,
  QueryCostMonitoringConfig,
  StorageConfig,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.{DbStorageHistograms, DbStorageMetrics}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.ExecutionContext

/** Storage setup for nodes with a single writer to the database. MUST NOT be used for replicated
  * nodes, use [[StorageMultiFactory]] instead.
  */
object StorageSingleSetup {

  private def migrateDb(dbMigrations: DbMigrations): Unit =
    dbMigrations
      .migrateDatabase()
      .value
      .map {
        case Left(error) => sys.error(s"Error with migration $error")
        case Right(_) => ()
      }
      .discard

  private def createDbStorageMetrics()(implicit
      metricsContext: MetricsContext
  ): DbStorageMetrics =
    new DbStorageMetrics(
      new DbStorageHistograms(MetricName("none"))(new HistogramInventory),
      NoOpMetricsFactory,
    )

  def tryCreateAndMigrateStorage(
      storageConfig: StorageConfig,
      logQueryCostConfig: Option[QueryCostMonitoringConfig],
      clock: Clock,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      setMigrationsPath: StorageConfig => StorageConfig = identity,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
      closeContext: CloseContext,
      metricsContext: MetricsContext,
  ): Storage = {
    val storageConfigWithMigrations = setMigrationsPath(storageConfig)
    storageConfigWithMigrations match {
      case dbConfig: DbConfig =>
        migrateDb(
          new DbMigrations(dbConfig, false, processingTimeout, loggerFactory)
        )
      case _ =>
        // Not a DB storage (currently, only memory) => no need for migrations.
        ()
    }
    new StorageSingleFactory(
      storageConfigWithMigrations
    )
      .tryCreate(
        connectionPoolForParticipant = false,
        logQueryCostConfig,
        clock,
        scheduler = None,
        metrics = createDbStorageMetrics(),
        processingTimeout,
        loggerFactory,
      )
  }
}
