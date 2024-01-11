// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.config.{DbConfig, ProcessingTimeout}
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.{CloseContext, UnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.DbStorage.RetryConfig
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.retry.RetryEither
import com.digitalasset.canton.util.{LoggerUtil, ResourceUtil}
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.FlywayException
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.hikaricp.HikariCPJdbcDataSource
import slick.jdbc.{DataSourceJdbcDataSource, JdbcBackend, JdbcDataSource}

import java.sql.SQLException
import javax.sql.DataSource
import scala.concurrent.blocking
import scala.concurrent.duration.Duration

trait DbMigrationsFactory {

  def create(dbConfig: DbConfig, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations

  def create(dbConfig: DbConfig, name: String, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations

}

trait DbMigrations { this: NamedLogging =>

  implicit protected def closeContext: CloseContext

  /** Whether we want to add the schema files found in the dev folder to the migration
    *
    * A user that does that, won't be able to upgrade to new Canton versions, as we reserve our right to just
    * modify the dev version files in any way we like.
    */
  protected def devVersionSupport: Boolean

  /** Database is migrated using Flyway, which looks at the migration files at
    * src/main/resources/db/migration/canton as explained at https://flywaydb.org/documentation/getstarted/firststeps/api
    */
  protected def createFlyway(dataSource: DataSource): Flyway = {
    Flyway.configure
      .locations(dbConfig.buildMigrationsPaths(devVersionSupport): _*)
      .dataSource(dataSource)
      .cleanDisabled(!dbConfig.parameters.unsafeCleanOnValidationError)
      .cleanOnValidationError(dbConfig.parameters.unsafeCleanOnValidationError)
      .baselineOnMigrate(dbConfig.parameters.unsafeBaselineOnMigrate)
      .lockRetryCount(60)
      .load()
  }

  protected def withCreatedDb[A](retryConfig: DbStorage.RetryConfig)(
      fn: Database => EitherT[UnlessShutdown, DbMigrations.Error, A]
  ): EitherT[UnlessShutdown, DbMigrations.Error, A] = {
    DbStorage
      .createDatabase(
        dbConfig,
        PositiveInt.one,
        scheduler = None, // no db query deadlock detection here
        forMigration = true,
        retryConfig = retryConfig,
      )(loggerFactory)
      .leftMap(DbMigrations.DatabaseError)
      .flatMap(db => ResourceUtil.withResource(db)(fn))
  }

  /** Obtain access to the database to run the migration operation. */
  protected def withDb[A](
      retryConfig: DbStorage.RetryConfig = DbStorage.RetryConfig.failFast
  )(fn: Database => EitherT[UnlessShutdown, DbMigrations.Error, A])(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, A]

  protected def migrateDatabaseInternal(
      flyway: Flyway
  )(implicit traceContext: TraceContext): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    // Retry the migration in case of failures, which may happen due to a race condition in concurrent migrations
    RetryEither.retry[DbMigrations.Error, Unit](10, 100, functionFullName, logger) {
      Either
        .catchOnly[FlywayException](flyway.migrate())
        .map(r => logger.info(s"Applied ${r.migrationsExecuted} migrations successfully"))
        .leftMap(DbMigrations.FlywayError)
    }
  }

  protected def repairFlywayMigrationInternal(
      flyway: Flyway
  )(implicit traceContext: TraceContext): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    Either
      .catchOnly[FlywayException](flyway.repair())
      .map(r =>
        logger.info(
          s"The repair of the Flyway database migration succeeded. This is the Flyway repair report: $r"
        )
      )
      .leftMap[DbMigrations.Error](DbMigrations.FlywayError)
      .toEitherT[UnlessShutdown]
  }

  protected def dbConfig: DbConfig

  /** Migrate the database with all pending migrations. */
  def migrateDatabase(): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withDb() { createdDb =>
        ResourceUtil.withResource(createdDb) { db =>
          val flyway = createFlyway(DbMigrations.createDataSource(db.source))
          migrateDatabaseInternal(flyway)
        }
      }
    }

  /** Repair the database in case the migrations files changed (e.g. due to comment changes).
    * To quote the Flyway documentation:
    * {{{
    * Repair is your tool to fix issues with the schema history table. It has a few main uses:
    *
    * - Remove failed migration entries (only for databases that do NOT support DDL transactions)
    * - Realign the checksums, descriptions, and types of the applied migrations with the ones of the available migrations
    * - Mark all missing migrations as deleted
    * }}}
    */
  def repairFlywayMigration(): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      withFlyway(repairFlywayMigrationInternal)
    }

  protected def withFlyway[A](
      fn: Flyway => EitherT[UnlessShutdown, DbMigrations.Error, A]
  )(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, A] =
    withDb() { createdDb =>
      ResourceUtil.withResource(createdDb) { db =>
        val flyway = createFlyway(DbMigrations.createDataSource(db.source))
        fn(flyway)
      }
    }

  private def connectionCheck(
      source: JdbcDataSource,
      processingTimeout: ProcessingTimeout,
  ): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    ResourceUtil
      .withResourceEither(source.createConnection()) { conn =>
        val valid = blocking {
          Either.catchOnly[SQLException](
            conn.isValid(processingTimeout.network.duration.toSeconds.toInt)
          )
        }
        valid
          .leftMap(err => show"failed to check connection $err")
          .flatMap { valid =>
            Either.cond(
              valid,
              (),
              "A trial database connection was not valid",
            )
          }
          .leftMap[DbMigrations.Error](err => DbMigrations.DatabaseError(err))
      }
      .valueOr { err =>
        Left(DbMigrations.DatabaseError(s"failed to create connection ${err.getMessage}"))
      }
      .toEitherT[UnlessShutdown]
  }

  def checkAndMigrate(params: CantonNodeParameters, retryConfig: RetryConfig)(implicit
      tc: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    val standardConfig = !params.nonStandardConfig
    val started = System.nanoTime()
    withDb(retryConfig) { createdDb =>
      ResourceUtil.withResource(createdDb) { db =>
        val flyway = createFlyway(DbMigrations.createDataSource(db.source))
        for {
          _ <- connectionCheck(db.source, params.processingTimeouts)
          _ <- checkDbVersion(db, params.processingTimeouts, standardConfig)
          _ <-
            if (params.dbMigrateAndStart)
              migrateAndStartInternal(flyway)
            else
              migrateIfFreshAndCheckPending(flyway)
        } yield {
          val elapsed = System.nanoTime() - started
          logger.debug(
            s"Finished setting up database schemas after ${LoggerUtil
                .roundDurationForHumans(Duration.fromNanos(elapsed))}"
          )
        }
      }
    }
  }

  private def migrateAndStartInternal(flyway: Flyway)(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    val info = flyway.info()
    if (info.pending().nonEmpty) {
      logger.info(
        s"There are ${info.pending().length} pending migrations for the db that is at version ${info.applied().length}. Performing migration before start."
      )
      migrateDatabaseInternal(flyway)
    } else {
      logger.debug("Db schema is already up to date")
      EitherT.rightT(())
    }
  }

  private def migrateIfFreshInternal(flyway: Flyway)(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    if (flyway.info().applied().isEmpty) migrateDatabaseInternal(flyway)
    else {
      logger.debug("Skip flyway migration on non-empty database")
      EitherT.rightT(())
    }
  }

  /** Combined method of migrateIfFresh and checkPendingMigration, avoids creating multiple pools */
  private def migrateIfFreshAndCheckPending(
      flyway: Flyway
  ): EitherT[UnlessShutdown, DbMigrations.Error, Unit] =
    TraceContext.withNewTraceContext { implicit traceContext =>
      for {
        _ <- migrateIfFreshInternal(flyway)
        _ <- checkPendingMigrationInternal(flyway).toEitherT[UnlessShutdown]
      } yield ()
    }

  private def checkDbVersion(
      db: JdbcBackend.Database,
      timeouts: ProcessingTimeout,
      standardConfig: Boolean,
  )(implicit tc: TraceContext): EitherT[UnlessShutdown, DbMigrations.Error, Unit] = {
    val check = DbVersionCheck
      .dbVersionCheck(timeouts, standardConfig, dbConfig)
    check(db).toEitherT[UnlessShutdown]
  }

  private def checkPendingMigrationInternal(
      flyway: Flyway
  ): Either[DbMigrations.Error, Unit] = {
    for {
      info <- Either
        .catchOnly[FlywayException](flyway.info())
        .leftMap(DbMigrations.FlywayError)
      pendingMigrations = info.pending()
      _ <-
        if (pendingMigrations.isEmpty) Right(())
        else {
          val currentVersion = Option(info.current()).map(_.getVersion.getVersion)
          val lastVersion = pendingMigrations.last.getVersion.getVersion
          val pendingMsg =
            s"There are ${pendingMigrations.length} pending migrations to get to database schema version $lastVersion"
          val msg =
            currentVersion.fold(s"No migrations have been applied yet. $pendingMsg.")(version =>
              s"$pendingMsg. Currently on version $version."
            )
          Left(DbMigrations.PendingMigrationError(msg))
        }
    } yield ()
  }

}

class CommunityDbMigrationsFactory(loggerFactory: NamedLoggerFactory) extends DbMigrationsFactory {
  override def create(dbConfig: DbConfig, name: String, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations =
    new CommunityDbMigrations(
      dbConfig,
      devVersionSupport,
      loggerFactory.appendUnnamedKey("node", name),
    )

  override def create(dbConfig: DbConfig, devVersionSupport: Boolean)(implicit
      closeContext: CloseContext
  ): DbMigrations =
    new CommunityDbMigrations(dbConfig, devVersionSupport, loggerFactory)
}

class CommunityDbMigrations(
    protected val dbConfig: DbConfig,
    protected val devVersionSupport: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit override protected val closeContext: CloseContext)
    extends DbMigrations
    with NamedLogging {

  override def withDb[A](
      retryConfig: RetryConfig
  )(fn: Database => EitherT[UnlessShutdown, DbMigrations.Error, A])(implicit
      traceContext: TraceContext
  ): EitherT[UnlessShutdown, DbMigrations.Error, A] = withCreatedDb(retryConfig)(fn)
}

object DbMigrations {

  def createDataSource(jdbcDataSource: JdbcDataSource): DataSource =
    jdbcDataSource match {
      case dataS: DataSourceJdbcDataSource => dataS.ds
      case dataS: HikariCPJdbcDataSource => dataS.ds
      case unsupported =>
        // This should never happen
        sys.error(s"Data source not supported for migrations: ${unsupported.getClass}")
    }

  sealed trait Error extends PrettyPrinting
  final case class FlywayError(err: FlywayException) extends Error {
    override def pretty: Pretty[FlywayError] = prettyOfClass(unnamedParam(_.err))
  }
  final case class PendingMigrationError(msg: String) extends Error {
    override def pretty: Pretty[PendingMigrationError] = prettyOfClass(unnamedParam(_.msg.unquoted))
  }
  final case class DatabaseError(error: String) extends Error {
    override def pretty: Pretty[DatabaseError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  final case class DatabaseVersionError(error: String) extends Error {
    override def pretty: Pretty[DatabaseVersionError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  final case class DatabaseConfigError(error: String) extends Error {
    override def pretty: Pretty[DatabaseConfigError] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}
