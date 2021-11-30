// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer

import com.daml.ledger.resources.{Resource, ResourceContext, ResourceOwner}
import com.daml.logging.LoggingContext
import com.daml.platform.store.FlywayMigrations

import scala.concurrent.Future

final class IndexerServerMigrations(
    config: IndexerConfig,
    additionalMigrationPaths: Seq[String] = Seq.empty,
)(implicit loggingContext: LoggingContext)
    extends ResourceOwner[Unit] {

  override def acquire()(implicit context: ResourceContext): Resource[Unit] = {
    val flywayMigrations =
      new FlywayMigrations(
        config.jdbcUrl,
        additionalMigrationPaths,
      )

    val migrationRun = config.startupMode match {
      case IndexerStartupMode.MigrateAndStart =>
        flywayMigrations.migrate(config.allowExistingSchema)

      case IndexerStartupMode.ResetAndStart =>
        Future.unit

      case IndexerStartupMode.ValidateAndStart =>
        flywayMigrations.validate()

      case IndexerStartupMode.ValidateAndWaitOnly =>
        flywayMigrations.validateAndWaitOnly(
          config.schemaMigrationAttempts,
          config.schemaMigrationAttemptBackoff,
        )

      case IndexerStartupMode.MigrateOnEmptySchemaAndStart =>
        flywayMigrations.migrateOnEmptySchema()
    }
    Resource.fromFuture(migrationRun)
  }
}
