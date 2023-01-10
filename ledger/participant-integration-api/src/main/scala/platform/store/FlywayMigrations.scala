// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.resources.ResourceContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.FlywayMigrations._
import com.daml.platform.store.backend.VerifiedDataSource
import com.daml.timer.RetryStrategy
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import javax.sql.DataSource
import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

private[daml] class FlywayMigrations(
    jdbcUrl: String,
    additionalMigrationPaths: Seq[String] = Seq.empty,
)(implicit resourceContext: ResourceContext, loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)
  private val dbType = DbType.jdbcType(jdbcUrl)
  implicit private val ec: ExecutionContext = resourceContext.executionContext

  private def runF[T](t: FluentConfiguration => Future[T]): Future[T] =
    VerifiedDataSource(jdbcUrl).flatMap(dataSource => t(configurationBase(dataSource)))

  private def run[T](t: FluentConfiguration => T): Future[T] =
    runF(fc => Future(t(fc)))

  private def configurationBase(dataSource: DataSource): FluentConfiguration =
    Flyway
      .configure()
      .locations((locations(dbType) ++ additionalMigrationPaths): _*)
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

  // There is currently no way to get the previous behavior in
  // a non-deprecated way. See https://github.com/flyway/flyway/issues/3338
  @nowarn("msg=method ignoreFutureMigrations .* is deprecated")
  def validate(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .ignoreFutureMigrations(false)
      .load()
    logger.info("Running Flyway validation...")
    checkFlywayHistory(flyway)
    flyway.validate()
    logger.info("Flyway schema validation finished successfully.")
  }

  @nowarn("msg=method ignoreFutureMigrations .* is deprecated")
  def migrate(allowExistingSchema: Boolean = false): Future[Unit] = run { configBase =>
    val flyway = configBase
      .baselineOnMigrate(allowExistingSchema)
      .baselineVersion(MigrationVersion.fromVersion("0"))
      .ignoreFutureMigrations(false)
      .load()
    logger.info("Running Flyway migration...")
    checkFlywayHistory(flyway)
    val migrationResult = flyway.migrate()
    logger.info(
      s"Flyway schema migration finished successfully, applying ${migrationResult.migrationsExecuted} steps."
    )
  }

  @nowarn("msg=method ignoreFutureMigrations .* is deprecated")
  def validateAndWaitOnly(retries: Int, retryBackoff: FiniteDuration): Future[Unit] = runF {
    configBase =>
      val flyway = configBase
        .ignoreFutureMigrations(false)
        .load()

      logger.info("Running Flyway validation...")
      checkFlywayHistory(flyway)

      RetryStrategy.constant(retries, retryBackoff) { (attempt, _) =>
        val pendingMigrations = flyway.info().pending().length
        if (pendingMigrations == 0) {
          logger.info("No pending migrations.")
          Future.unit
        } else {
          logger.debug(
            s"Pending migrations ${pendingMigrations} on attempt ${attempt} of ${retries} attempts"
          )
          Future.failed(MigrationIncomplete(pendingMigrations))
        }
      }
  }

  @nowarn("msg=method ignoreFutureMigrations .* is deprecated")
  def migrateOnEmptySchema(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .ignoreFutureMigrations(false)
      .load()
    logger.info(
      "Ensuring Flyway migration has either not started or there are no pending migrations..."
    )
    checkFlywayHistory(flyway)
    val flywayInfo = flyway.info()

    (flywayInfo.pending().length, flywayInfo.applied().length) match {
      case (0, appliedMigrations) =>
        logger.info(s"No pending migrations with ${appliedMigrations} migrations applied.")

      case (pendingMigrations, 0) =>
        logger.info(
          s"Running Flyway migration on empty database with $pendingMigrations migrations pending..."
        )
        val migrationResult = flyway.migrate()
        logger.info(
          s"Flyway schema migration finished successfully, applying ${migrationResult.migrationsExecuted} steps on empty database."
        )

      case (pendingMigrations, appliedMigrations) =>
        val ex = MigrateOnEmptySchema(appliedMigrations, pendingMigrations)
        logger.warn(ex.getMessage)
        throw ex
    }
  }
}

private[platform] object FlywayMigrations {
  private val sqlMigrationClasspathBase = "classpath:db/migration/"
  private val javaMigrationClasspathBase = "classpath:com/daml/platform/db/migration/"

  private[platform] def locations(dbType: DbType) =
    List(
      sqlMigrationClasspathBase + dbType.name,
      javaMigrationClasspathBase + dbType.name,
    )

  private[platform] def minimumSchemaVersion(dbType: DbType) =
    dbType match {
      // We have replaced all Postgres Java migrations with no-op migrations.
      // The last Java migration has version 38 and was introduced in Daml SDK 1.6.
      // To reduce the number of corner cases, we require users to run through all original
      // Java migrations, unless the database is empty.
      case DbType.Postgres => "38"
      case DbType.Oracle => "0"
      case DbType.H2Database => "0"
    }

  case class MigrationIncomplete(pendingMigrations: Int)
      extends RuntimeException(s"Migration incomplete with $pendingMigrations migrations remaining")
  case class MigrateOnEmptySchema(appliedMigrations: Int, pendingMigrations: Int)
      extends RuntimeException(
        s"Asked to migrate-on-empty-schema, but encountered neither an empty database with $appliedMigrations " +
          s"migrations already applied nor a fully-migrated databases with $pendingMigrations migrations pending."
      )
  case class SchemaVersionIsTooOld(current: String, required: String)
      extends RuntimeException(
        "Database schema version is too old. " +
          s"Current schema version is $current, required schema version is $required. " +
          "Please read the documentation on data continuity guarantees " +
          "and use an older Daml SDK version to migrate to the required schema version."
      )
}
