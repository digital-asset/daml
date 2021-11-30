// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream.Materializer
import com.daml.ledger.api.health.{Healthy, ReportsHealth}
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.metrics.Metrics
import com.daml.platform.store.LfValueTranslationCache

import scala.concurrent.ExecutionContext

final class IndexerServer(
    readService: state.ReadService,
    config: IndexerConfig,
    servicesExecutionContext: ExecutionContext,
    metrics: Metrics,
    lfValueTranslationCache: LfValueTranslationCache.Cache,
)(implicit materializer: Materializer, loggingContext: LoggingContext)
    extends ResourceOwner[ReportsHealth] {

  private val logger = ContextualizedLogger.get(this.getClass)

  override def acquire()(implicit context: ResourceContext): Resource[ReportsHealth] = {
    val indexerFactory = new JdbcIndexer.Factory(
      config,
      readService,
      servicesExecutionContext,
      metrics,
      lfValueTranslationCache,
    )
    val indexer = RecoveringIndexer(
      materializer.system.scheduler,
      materializer.executionContext,
      config.restartDelay,
    )

    def startIndexer(
        initializedDebugLogMessage: String = "Waiting for the indexer to initialize the database.",
        resetSchema: Boolean = false,
    ): Resource[ReportsHealth] =
      indexerFactory
        .initialized(resetSchema)
        .acquire()
        .flatMap(indexer.start)
        .map { case (healthReporter, _) =>
          logger.debug(initializedDebugLogMessage)
          healthReporter
        }

    config.startupMode match {
      case IndexerStartupMode.MigrateAndStart =>
        startIndexer()

      case IndexerStartupMode.ResetAndStart =>
        startIndexer(resetSchema = true)

      case IndexerStartupMode.ValidateAndStart =>
        startIndexer()

      case IndexerStartupMode.ValidateAndWaitOnly =>
        Resource
          .successful(
            {
              logger.debug("Waiting for the indexer to validate the schema migrations.")
              () => Healthy
            }
          )

      case IndexerStartupMode.MigrateOnEmptySchemaAndStart =>
        startIndexer(
          initializedDebugLogMessage =
            "Waiting for the indexer to initialize the empty or up-to-date database."
        )
    }
  }
}
