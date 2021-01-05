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

  def validate()(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      Future.successful {
        val flyway = configurationBase(dbType).dataSource(ds).load()
        logger.info("Running Flyway validation...")
        flyway.validate()
        logger.info("Flyway schema validation finished successfully.")
      }
    }

  def migrate(allowExistingSchema: Boolean = false)(
      implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      Future.successful {
        val flyway = configurationBase(dbType)
          .dataSource(ds)
          .baselineOnMigrate(allowExistingSchema)
          .baselineVersion(MigrationVersion.fromVersion("0"))
          .load()
        logger.info("Running Flyway migration...")
        val stepsTaken = flyway.migrate()
        logger.info(s"Flyway schema migration finished successfully, applying $stepsTaken steps.")
      }
    }

  def reset()(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      Future.successful {
        val flyway = configurationBase(dbType).dataSource(ds).load()
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
    )
}

private[platform] object FlywayMigrations {
  def configurationBase(dbType: DbType): FluentConfiguration =
    Flyway
      .configure()
      .locations(
        "classpath:com/daml/platform/db/migration/" + dbType.name,
        "classpath:db/migration/" + dbType.name,
      )
}
