// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.resources.ResourceContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.FlywayMigrations._
import com.daml.platform.store.backend.VerifiedDataSource
import com.daml.timer.RetryStrategy
import javax.sql.DataSource
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

private[platform] class FlywayMigrations(
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

  def validate(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .ignoreFutureMigrations(false)
      .load()
    logger.info("Running Flyway validation...")
    flyway.validate()
    logger.info("Flyway schema validation finished successfully.")
  }

  def migrate(allowExistingSchema: Boolean = false): Future[Unit] = run { configBase =>
    val flyway = configBase
      .baselineOnMigrate(allowExistingSchema)
      .baselineVersion(MigrationVersion.fromVersion("0"))
      .ignoreFutureMigrations(false)
      .load()
    logger.info("Running Flyway migration...")
    val migrationResult = flyway.migrate()
    logger.info(
      s"Flyway schema migration finished successfully, applying ${migrationResult.migrationsExecuted} steps."
    )
  }

  def reset(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .load()
    logger.info("Running Flyway clean...")
    flyway.clean()
    logger.info("Flyway schema clean finished successfully.")
  }

  def validateAndWaitOnly(retries: Int, retryBackoff: FiniteDuration): Future[Unit] = runF {
    configBase =>
      val flyway = configBase
        .ignoreFutureMigrations(false)
        .load()

      logger.info("Running Flyway validation...")

      RetryStrategy.constant(retries, retryBackoff) { (attempt, _) =>
        val pendingMigrations = flyway.info().pending().length
        if (pendingMigrations == 0) {
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
      .ignoreFutureMigrations(false)
      .load()
    logger.info(
      "Ensuring Flyway migration has either not started or there are no pending migrations..."
    )
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
    // TODO append-only: rename the -appendonly folders
    List(
      sqlMigrationClasspathBase + dbType.name + "-appendonly",
      javaMigrationClasspathBase + dbType.name,
    )

  case class MigrationIncomplete(pendingMigrations: Int)
      extends RuntimeException(s"Migration incomplete with $pendingMigrations migrations remaining")
  case class MigrateOnEmptySchema(appliedMigrations: Int, pendingMigrations: Int)
      extends RuntimeException(
        s"Asked to migrate-on-empty-schema, but encountered neither an empty database with $appliedMigrations " +
          s"migrations already applied nor a fully-migrated databases with $pendingMigrations migrations pending."
      )
}
