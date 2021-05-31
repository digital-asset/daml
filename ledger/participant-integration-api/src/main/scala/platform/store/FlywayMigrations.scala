// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.resources.{ResourceContext, ResourceOwner}
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.configuration.ServerRole
import com.daml.platform.store.FlywayMigrations._
import com.daml.platform.store.dao.HikariConnection
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[platform] class FlywayMigrations(jdbcUrl: String)(implicit loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)

  private val dbType = DbType.jdbcType(jdbcUrl)

  def validate(
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean = false
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      Future.successful {
        val flyway = configurationBase(dbType, enableAppendOnlySchema)
          .dataSource(ds)
          .ignoreFutureMigrations(false)
          .load()
        logger.info("Running Flyway validation...")
        flyway.validate()
        logger.info("Flyway schema validation finished successfully.")
      }
    }

  def migrate(
      allowExistingSchema: Boolean = false,
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean = false,
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      Future.successful {
        val flyway = configurationBase(dbType, enableAppendOnlySchema)
          .dataSource(ds)
          .baselineOnMigrate(allowExistingSchema)
          .baselineVersion(MigrationVersion.fromVersion("0"))
          .ignoreFutureMigrations(false)
          .load()
        logger.info("Running Flyway migration...")
        val stepsTaken = flyway.migrate()
        logger.info(s"Flyway schema migration finished successfully, applying $stepsTaken steps.")
      }
    }

  def reset(
      enableAppendOnlySchema: Boolean
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      Future.successful {
        val flyway = configurationBase(dbType, enableAppendOnlySchema).dataSource(ds).load()
        logger.info("Running Flyway clean...")
        flyway.clean()
        logger.info("Flyway schema clean finished successfully.")
      }
    }

  private def dataSource: ResourceOwner[HikariDataSource] =
    HikariConnection.owner(
      serverRole = ServerRole.IndexMigrations,
      jdbcUrl = jdbcUrl,
      minimumIdle = 2,
      maxPoolSize = 2,
      connectionTimeout = 5.seconds,
      metrics = None,
      connectionAsyncCommitMode = DbType.SynchronousCommit,
    )
}

private[platform] object FlywayMigrations {
  def configurationBase(
      dbType: DbType,
      enableAppendOnlySchema: Boolean = false,
  ): FluentConfiguration =
    // TODO append-only: move all migrations from the '-appendonly' folder to the main folder, and remove the enableAppendOnlySchema parameter here
    if (enableAppendOnlySchema) {
      Flyway
        .configure()
        .locations(
          "classpath:com/daml/platform/db/migration/" + dbType.name,
          "classpath:com/daml/platform/db/migration/" + dbType.name + "-appendonly",
          "classpath:db/migration/" + dbType.name,
          "classpath:db/migration/" + dbType.name + "-appendonly",
        )
    } else {
      Flyway
        .configure()
        .locations(
          "classpath:com/daml/platform/db/migration/" + dbType.name,
          "classpath:db/migration/" + dbType.name,
        )
    }
}
