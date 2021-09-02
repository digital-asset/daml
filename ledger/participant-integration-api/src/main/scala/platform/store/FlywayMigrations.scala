// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.resources.ResourceContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.FlywayMigrations._
import com.daml.platform.store.backend.StorageBackend
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.MigrationVersion
import org.flywaydb.core.api.configuration.FluentConfiguration

import scala.annotation.tailrec
import scala.concurrent.Future

private[platform] class FlywayMigrations(
    jdbcUrl: String,
    enableAppendOnlySchema: Boolean =
      false, // TODO append-only: remove after removing support for the current (mutating) schema
    additionalMigrationPaths: Seq[String] = Seq.empty,
)(implicit resourceContext: ResourceContext, loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)
  private val dbType = DbType.jdbcType(jdbcUrl)
  private val dataSource = StorageBackend.of(dbType).createDataSource(jdbcUrl)

  private def run[T](t: => T): Future[T] = Future(t)(resourceContext.executionContext)

  def configurationBase: FluentConfiguration =
    Flyway
      .configure()
      .locations((locations(enableAppendOnlySchema, dbType) ++ additionalMigrationPaths): _*)
      .dataSource(dataSource)
      .connectRetries(10) // Flyway does exponential backoff waiting 1s 2s 4s 8s ...

  def validate(): Future[Unit] = run {
    val flyway = configurationBase
      .ignoreFutureMigrations(false)
      .load()
    logger.info("Running Flyway validation...")
    flyway.validate()
    logger.info("Flyway schema validation finished successfully.")
  }

  def migrate(allowExistingSchema: Boolean = false): Future[Unit] = run {
    val flyway = configurationBase
      .baselineOnMigrate(allowExistingSchema)
      .baselineVersion(MigrationVersion.fromVersion("0"))
      .ignoreFutureMigrations(false)
      .load()
    logger.info("Running Flyway migration...")
    val stepsTaken = flyway.migrate()
    logger.info(s"Flyway schema migration finished successfully, applying $stepsTaken steps.")
  }

  def reset(): Future[Unit] = run {
    val flyway = configurationBase
      .load()
    logger.info("Running Flyway clean...")
    flyway.clean()
    logger.info("Flyway schema clean finished successfully.")
  }

  def validateAndWaitOnly(): Future[Unit] = run {
    val flyway = configurationBase
      .ignoreFutureMigrations(false)
      .load()

    logger.info("Running Flyway validation...")

    @tailrec
    def flywayMigrationDone(
        retries: Int,
        pendingMigrationsSoFar: Option[Int],
    ): Unit = {
      val pendingMigrations = flyway.info().pending().length
      if (pendingMigrations == 0) {
        ()
      } else if (retries <= 0) {
        throw ExhaustedRetries(pendingMigrations)
      } else if (pendingMigrationsSoFar.exists(pendingMigrations >= _)) {
        throw StoppedProgressing(pendingMigrations)
      } else {
        logger.debug(
          s"Concurrent migration has reduced the pending migrations set to $pendingMigrations, waiting until pending set is empty.."
        )
        Thread.sleep(1000)
        flywayMigrationDone(retries - 1, Some(pendingMigrations))
      }
    }

    try {
      flywayMigrationDone(10, None)
      logger.info("Flyway schema validation finished successfully.")
    } catch {
      case ex: RuntimeException =>
        logger.error(s"Failed to validate and wait only: ${ex.getMessage}", ex)
        throw ex
    }
  }

  def migrateOnEmptySchema(): Future[Unit] = run {
    val flyway = configurationBase
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
        val stepsTaken = flyway.migrate()
        logger.info(
          s"Flyway schema migration finished successfully, applying $stepsTaken steps on empty database."
        )

      case (pendingMigrations, appliedMigrations) =>
        val ex = MigrateOnEmptySchema(appliedMigrations, pendingMigrations)
        logger.warn(ex.getMessage)
        throw ex
    }
  }
}

// TODO append-only: move all migrations from the '-appendonly' folder to the main folder, and remove the enableAppendOnlySchema parameter here
private[platform] object FlywayMigrations {
  private val appendOnlyFromScratch = Map(
    DbType.Postgres -> false,
    DbType.H2Database -> true,
    DbType.Oracle -> true,
  )

  private val sqlMigrationClasspathBase = "classpath:db/migration/"
  private val javaMigrationClasspathBase = "classpath:com/daml/platform/db/migration/"

  private[platform] def locations(enableAppendOnlySchema: Boolean, dbType: DbType) = {
    def mutableClassPath =
      List(
        sqlMigrationClasspathBase,
        javaMigrationClasspathBase,
      ).map(_ + dbType.name)

    def appendOnlyClassPath =
      List(sqlMigrationClasspathBase)
        .map(_ + dbType.name + "-appendonly")

    (enableAppendOnlySchema, appendOnlyFromScratch(dbType)) match {
      case (true, true) => appendOnlyClassPath
      case (true, false) => mutableClassPath ++ appendOnlyClassPath
      case (false, _) => mutableClassPath
    }
  }

  case class ExhaustedRetries(pendingMigrations: Int)
      extends RuntimeException(s"Ran out of retries with $pendingMigrations migrations remaining")
  case class StoppedProgressing(pendingMigrations: Int)
      extends RuntimeException(s"Stopped progressing with $pendingMigrations migrations")
  case class MigrateOnEmptySchema(appliedMigrations: Int, pendingMigrations: Int)
      extends RuntimeException(
        s"Asked to migrate-on-empty-schema, but encountered neither an empty database with $appliedMigrations " +
          s"migrations already applied nor a fully-migrated databases with $pendingMigrations migrations pending."
      )
}
