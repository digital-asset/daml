// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store

import com.daml.ledger.resources.ResourceContext
import com.daml.timer.RetryStrategy
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.platform.store.FlywayMigrations.*
import com.digitalasset.canton.platform.store.backend.VerifiedDataSource
import com.digitalasset.canton.tracing.TraceContext
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import javax.sql.DataSource
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

class FlywayMigrations(
    jdbcUrl: String,
    additionalMigrationPaths: Seq[String] = Seq.empty,
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
      .locations((locations(dbType) ++ additionalMigrationPaths)*)
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

  def validate(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .ignoreMigrationPatterns("") // disables the default ignoring "*:future" migrations
      .load()
    logger.info("Running Flyway validation...")
    checkFlywayHistory(flyway)
    flyway.validate()
    logger.info("Flyway schema validation finished successfully.")
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

  def validateAndWaitOnly(retries: Int, retryBackoff: FiniteDuration): Future[Unit] = runF {
    configBase =>
      val flyway = configBase
        .ignoreMigrationPatterns("") // disables the default ignoring "*:future" migrations
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

  def migrateOnEmptySchema(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .ignoreMigrationPatterns("") // disables the default ignoring "*:future" migrations
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
  private val javaMigrationClasspathBase =
    "classpath:com/digitalasset/canton/platform/db/migration/"

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

  final case class MigrationIncomplete(pendingMigrations: Int)
      extends RuntimeException(s"Migration incomplete with $pendingMigrations migrations remaining")
  final case class MigrateOnEmptySchema(appliedMigrations: Int, pendingMigrations: Int)
      extends RuntimeException(
        s"Asked to migrate-on-empty-schema, but encountered neither an empty database with $appliedMigrations " +
          s"migrations already applied nor a fully-migrated databases with $pendingMigrations migrations pending."
      )
  final case class SchemaVersionIsTooOld(current: String, required: String)
      extends RuntimeException(
        "Database schema version is too old. " +
          s"Current schema version is $current, required schema version is $required. " +
          "Please read the documentation on data continuity guarantees " +
          "and use an older Daml SDK version to migrate to the required schema version."
      )
}
