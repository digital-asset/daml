// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import akka.stream.Materializer
import com.daml.ledger.api.health.ReportsHealth
import com.daml.ledger.participant.state.{v2 => state}
import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.metrics.Metrics
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

  private val indexerMigrations = new IndexerServerMigrations(config, additionalMigrationPaths)
  private val indexerServer = new IndexerServer(
    readService,
    config,
    servicesExecutionContext,
    metrics,
    lfValueTranslationCache,
  )

  override def acquire()(implicit context: ResourceContext): Resource[ReportsHealth] = {
    indexerMigrations.acquire().flatMap(_ => indexerServer.acquire())
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
