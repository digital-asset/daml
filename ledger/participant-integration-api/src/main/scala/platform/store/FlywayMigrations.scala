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

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

private[platform] class FlywayMigrations(
    jdbcUrl: String,
    additionalMigrationPaths: Seq[String] = Seq.empty,
)(implicit loggingContext: LoggingContext) {
  private val logger = ContextualizedLogger.get(this.getClass)

  private val dbType = DbType.jdbcType(jdbcUrl)

  def validate(
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean = false
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      Future.successful {
        val flyway = configurationBase(dbType, enableAppendOnlySchema, additionalMigrationPaths)
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
        val flyway = configurationBase(dbType, enableAppendOnlySchema, additionalMigrationPaths)
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
        val flyway = configurationBase(dbType, enableAppendOnlySchema, additionalMigrationPaths)
          .dataSource(ds)
          .load()
        logger.info("Running Flyway clean...")
        flyway.clean()
        logger.info("Flyway schema clean finished successfully.")
      }
    }

  def validateAndWaitOnly(
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean = false
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      val flyway = configurationBase(dbType, enableAppendOnlySchema, additionalMigrationPaths)
        .dataSource(ds)
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
        Future.unit
      } catch {
        case ex: RuntimeException =>
          logger.error(s"Failed to validate and wait only: ${ex.getMessage}", ex)
          Future.failed(ex)
      }
    }

  def migrateOnEmptySchema(
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean = false
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      val flyway = configurationBase(dbType, enableAppendOnlySchema, additionalMigrationPaths)
        .dataSource(ds)
        .ignoreFutureMigrations(false)
        .load()
      logger.info(
        "Ensuring Flyway migration has either not started or there are no pending migrations..."
      )
      val flywayInfo = flyway.info()

      (flywayInfo.pending().length, flywayInfo.applied().length) match {
        case (0, appliedMigrations) =>
          Future.successful(
            logger.info(s"No pending migrations with $appliedMigrations migrations applied.")
          )
        case (pendingMigrations, 0) =>
          Future.successful {
            logger.info(
              s"Running Flyway migration on empty database with $pendingMigrations migrations pending..."
            )
            val stepsTaken = flyway.migrate()
            logger.info(
              s"Flyway schema migration finished successfully, applying $stepsTaken steps on empty database."
            )
          }
        case (pendingMigrations, appliedMigrations) =>
          val ex = MigrateOnEmptySchema(appliedMigrations, pendingMigrations)
          logger.warn(ex.getMessage)
          Future.failed(ex)
      }
    }

  def enforceEmptySchemaAndMigrate(
      // TODO append-only: remove after removing support for the current (mutating) schema
      enableAppendOnlySchema: Boolean = false
  )(implicit resourceContext: ResourceContext): Future[Unit] =
    dataSource.use { ds =>
      val flyway = configurationBase(dbType, enableAppendOnlySchema, additionalMigrationPaths)
        .dataSource(ds)
        .ignoreFutureMigrations(false)
        .load()
      logger.info(
        "Ensuring Flyway migration has either not started or there are no pending migrations..."
      )
      val flywayInfo = flyway.info()

      flywayInfo.applied().length match {
        case 0 =>
          Future.successful {
            logger.info("Running Flyway migration...")
            val stepsTaken = flyway.migrate()
            logger.info(
              s"Flyway schema migration finished successfully, applying $stepsTaken steps."
            )
          }
        case appliedMigrations =>
          val ex = EnforceEmptySchema(appliedMigrations)
          logger.warn(ex.getMessage)
          Future.failed(ex)
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

// TODO append-only: move all migrations from the '-appendonly' folder to the main folder, and remove the enableAppendOnlySchema parameter here
private[platform] object FlywayMigrations {
  private val appendOnlyFromScratch = Map(
    DbType.Postgres -> false,
    DbType.H2Database -> true,
    DbType.Oracle -> true,
  )

  private val sqlMigrationClasspathBase = "classpath:db/migration/"
  private val javaMigrationClasspathBase = "classpath:com/daml/platform/db/migration/"

  private def locations(enableAppendOnlySchema: Boolean, dbType: DbType) = {
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

  def configurationBase(
      dbType: DbType,
      enableAppendOnlySchema: Boolean = false,
      additionalMigrationPaths: Seq[String] = Seq.empty,
  ): FluentConfiguration =
    Flyway
      .configure()
      .locations(locations(enableAppendOnlySchema, dbType) ++ additionalMigrationPaths: _*)

  case class ExhaustedRetries(pendingMigrations: Int)
      extends RuntimeException(s"Ran out of retries with $pendingMigrations migrations remaining")
  case class StoppedProgressing(pendingMigrations: Int)
      extends RuntimeException(s"Stopped progressing with $pendingMigrations migrations")
  case class MigrateOnEmptySchema(appliedMigrations: Int, pendingMigrations: Int)
      extends RuntimeException(
        s"Asked to migrate-on-empty-schema, but encountered neither an empty database with $appliedMigrations " +
          s"migrations already applied nor a fully-migrated databases with $pendingMigrations migrations pending."
      )
  case class EnforceEmptySchema(appliedMigrations: Int)
      extends RuntimeException(
        s"Asked to enforce-empty-schema-and-migrate, but encountered a non-empty database with $appliedMigrations migrations already applied."
      )
}
