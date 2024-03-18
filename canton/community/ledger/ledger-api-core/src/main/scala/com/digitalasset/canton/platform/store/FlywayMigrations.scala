// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.resources.ResourceContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.FlywayMigrations.*
import com.digitalasset.canton.platform.store.backend.VerifiedDataSource
import com.digitalasset.canton.tracing.TraceContext
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import javax.sql.DataSource
import scala.concurrent.{ExecutionContext, Future}

class FlywayMigrations(
    jdbcUrl: String,
    val loggerFactory: NamedLoggerFactory,
)(implicit resourceContext: ResourceContext, traceContext: TraceContext)
    extends NamedLogging {
  private val dbType = DbType.jdbcType(jdbcUrl)
  implicit private val ec: ExecutionContext = resourceContext.executionContext

  private def runF[T](t: FluentConfiguration => Future[T]): Future[T] =
    VerifiedDataSource(jdbcUrl, loggerFactory).flatMap(dataSource =>
      t(configurationBase(dataSource))
    )

  private def run[T](t: FluentConfiguration => T): Future[T] =
    runF(fc => Future(t(fc)))

  private def configurationBase(dataSource: DataSource): FluentConfiguration =
    Flyway
      .configure()
      .locations((locations(dbType))*)
      .dataSource(dataSource)

  private def checkFlywayHistory(flyway: Flyway): Unit = {
    val currentVersion = Option(flyway.info().current())
    val requiredVersion = minimumSchemaVersion(dbType)

    currentVersion match {
      case None =>
        ()
      case Some(current) if current.getVersion.isAtLeast(requiredVersion) =>
        ()
      case Some(current) =>
        throw SchemaVersionIsTooOld(
          current = current.getVersion.getVersion,
          required = requiredVersion,
        )
    }
  }

  def migrate(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .baselineOnMigrate(false)
      .baselineVersion(MigrationVersion.fromVersion("0"))
      .ignoreMigrationPatterns("") // disables the default ignoring "*:future" migrations
      .load()
    logger.info("Running Flyway migration...")
    checkFlywayHistory(flyway)
    val migrationResult = flyway.migrate()
    logger.info(
      s"Flyway schema migration finished successfully, applying ${migrationResult.migrationsExecuted} steps."
    )
  }
}

private[platform] object FlywayMigrations {
  private val sqlMigrationClasspathBase = "classpath:db/migration/canton/"

  private[platform] def locations(dbType: DbType) =
    List(
      sqlMigrationClasspathBase + dbType.name + "/stable"
    )

  private[platform] def minimumSchemaVersion(dbType: DbType) =
    dbType match {
      case DbType.Postgres => "0"
      case DbType.Oracle => "0"
      case DbType.H2Database => "0"
    }

  final case class SchemaVersionIsTooOld(current: String, required: String)
      extends RuntimeException(
        "Database schema version is too old. " +
          s"Current schema version is $current, required schema version is $required. " +
          "Please read the documentation on data continuity guarantees " +
          "and use an older Daml SDK version to migrate to the required schema version."
      )
}
