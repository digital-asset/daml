// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.store

import com.daml.ledger.resources.ResourceContext
import com.daml.logging.{ContextualizedLogger, LoggingContext}
import com.daml.platform.store.FlywayMigrationsBase._
import com.daml.timer.RetryStrategy

import javax.sql.DataSource
import org.flywaydb.core.Flyway
import org.flywaydb.core.api.{MigrationInfo, MigrationState, MigrationType, MigrationVersion}
import org.flywaydb.core.api.configuration.FluentConfiguration

import scala.annotation.nowarn
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

sealed trait FlywayStatus
object FlywayStatus {

  /** Database is empty */
  final case class DatabaseEmpty(pending: Int) extends FlywayStatus

  /** Currently applied schema is before the available baseline version.
    * the migrations required to bring it from the current version to the baseline version have been
    * deleted from the application, user needs to use an old SDK version to migrate the database to
    * at least the baseline version.
    */
  final case class SchemaBeforeBaseline(currentVersion: String, baselineVersion: String)
      extends FlywayStatus

  /** There are no migrations pending (but there may be other errors, such as checksum mismatches) */
  final case class NoMigrationsPending(currentVersion: String, applied: Int) extends FlywayStatus

  /** There are migrations pending */
  final case class Outdated(currentVersion: String, applied: Int, pending: Int) extends FlywayStatus

  /** Database contains migrations before the baseline migration. */
  final case class NeedsBaselineReset(oldVersions: List[String]) extends FlywayStatus
}

private[daml] abstract class FlywayMigrationsBase()(implicit
    resourceContext: ResourceContext,
    loggingContext: LoggingContext,
) {
  private val logger = ContextualizedLogger.get(this.getClass)
  implicit private val ec: ExecutionContext = resourceContext.executionContext

  // A "baseline migration" represents all migrations up to and including the "baseline version"
  // Baseline migrations are a first class feature in Flyway, however they are not available in the free version.
  // To move the baseline version, use the following procedure:
  //   1. Apply all migrations up to and including the new baseline version to a fresh database
  //   2. Export the resulting schema as the new baseline migration, and assign it a version number equal to the
  //      baseline version
  //   3. Double check that the migrations from step 1 and the migration from step 2 produce exactly the same
  //      database
  //   4. Delete all migrations used in step 1
  //   5. Update the value below
  protected def baselineVersion: String

  protected def getDataSource: Future[DataSource]

  protected def locations: List[String]

  private def runF[T](t: FluentConfiguration => Future[T]): Future[T] =
    getDataSource.flatMap(dataSource => t(configurationBase(dataSource)))

  private def run[T](t: FluentConfiguration => T): Future[T] =
    runF(fc => Future(t(fc)))

  private def configurationBase(dataSource: DataSource): FluentConfiguration =
    Flyway
      .configure()
      .locations(locations: _*)
      .dataSource(dataSource)

  private def getStatus(flyway: Flyway): FlywayStatus = {
    val info = flyway.info()
    val current = info.current()
    val pending = info.pending()
    val applied = info.applied()

    def isBeforeBaseline(m: MigrationInfo) = !m.getVersion.isAtLeast(baselineVersion)

    // The Flyway history is immutable.
    // To delete a migration, a new row is inserted into the history table that marks the migration as deleted.
    // We are looking for rows that have not been marked as deleted by other rows (not DELETED),
    // and that are not themselves deletion markers (not DELETE).
    val notDeletedMigrationsBeforeBaseline = applied.toList
      .filter(m =>
        isBeforeBaseline(
          m
        ) && m.getType != MigrationType.DELETE && m.getState != MigrationState.DELETED
      )
      .map(_.getVersion.getVersion)

    if (current == null) {
      FlywayStatus.DatabaseEmpty(info.pending().length)
    } else if (isBeforeBaseline(current)) {
      FlywayStatus.SchemaBeforeBaseline(current.getVersion.getVersion, baselineVersion)
    } else if (notDeletedMigrationsBeforeBaseline.nonEmpty) {
      FlywayStatus.NeedsBaselineReset(notDeletedMigrationsBeforeBaseline)
    } else if (pending.nonEmpty) {
      FlywayStatus.Outdated(current.getVersion.getVersion, applied.length, pending.length)
    } else {
      FlywayStatus.NoMigrationsPending(current.getVersion.getVersion, applied.length)
    }
  }

  private def resetBaseline(flyway: Flyway): Unit = {
    val info = flyway.info()

    assert(
      info.current().getVersion.isNewerThan(baselineVersion),
      "Current version is below the baseline, can't reset the baseline",
    )
    assert(
      info.applied().exists(m => !m.getVersion.isNewerThan(baselineVersion)),
      "No migrations below baseline found, baseline reset not required",
    )

    def migrationsToString(
        migrations: java.util.List[org.flywaydb.core.api.output.RepairOutput]
    ): String = {
      if (migrations.isEmpty) {
        "<none>"
      } else {
        migrations.asScala.map(_.version).mkString(", ")
      }
    }

    // Expected output is:
    // - All migrations below the baseline are DELETED
    // - The migration with exactly the baseline version is ALIGNED
    // - No migrations are REMOVED
    val actionsTaken = flyway.repair()
    logger.info(
      "Flyway repair took the following actions: " +
        s"Failed migrations removed: ${migrationsToString(actionsTaken.migrationsRemoved)}. " +
        s"Missing migrations deleted: ${migrationsToString(actionsTaken.migrationsDeleted)}. " +
        s"Migration checksums aligned: ${migrationsToString(actionsTaken.migrationsAligned)}."
    )
    ()
  }

  // There is currently no way to get the previous behavior in
  // a non-deprecated way. See https://github.com/flyway/flyway/issues/3338
  @nowarn("msg=method ignoreFutureMigrations .* is deprecated")
  def currentVersion(): Future[Option[String]] = run { configBase =>
    val flyway = configBase
      .ignoreFutureMigrations(false)
      .load()
    Option(flyway.info.current()).map(_.getVersion.getVersion)
  }

  // There is currently no way to get the previous behavior in
  // a non-deprecated way. See https://github.com/flyway/flyway/issues/3338
  @nowarn("msg=method ignoreFutureMigrations .* is deprecated")
  def validate(): Future[Unit] = run { configBase =>
    val flyway = configBase
      .ignoreFutureMigrations(false)
      .load()
    logger.info("Running Flyway validation...")
    getStatus(flyway) match {
      case FlywayStatus.SchemaBeforeBaseline(current, baseline) =>
        throw SchemaOutdated(current, baseline)
      case FlywayStatus.NeedsBaselineReset(oldVersions) => throw NeedsBaselineReset(oldVersions)
      case FlywayStatus.DatabaseEmpty(_) => flyway.validate()
      case FlywayStatus.NoMigrationsPending(_, _) => flyway.validate()
      case FlywayStatus.Outdated(_, _, _) => flyway.validate()
    }

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
    val migrationResult = getStatus(flyway) match {
      case FlywayStatus.SchemaBeforeBaseline(current, baseline) =>
        throw SchemaOutdated(current, baseline)
      case FlywayStatus.NeedsBaselineReset(_) =>
        resetBaseline(flyway)
        flyway.migrate()
      case FlywayStatus.DatabaseEmpty(_) => flyway.migrate()
      case FlywayStatus.NoMigrationsPending(_, _) => flyway.migrate()
      case FlywayStatus.Outdated(_, _, _) => flyway.migrate()
    }

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

      RetryStrategy.constant(retries, retryBackoff) { (attempt, _) =>
        getStatus(flyway) match {
          case FlywayStatus.SchemaBeforeBaseline(current, baseline) =>
            Future.failed(SchemaOutdated(current, baseline))
          case FlywayStatus.NeedsBaselineReset(oldVersions) =>
            Future.failed(NeedsBaselineReset(oldVersions))
          case FlywayStatus.DatabaseEmpty(pending) =>
            logger.debug(
              s"Pending migrations ${pending} on attempt ${attempt} of ${retries} attempts"
            )
            Future.failed(MigrationIncomplete(pending))
          case FlywayStatus.NoMigrationsPending(_, _) =>
            logger.info("No pending migrations.")
            Future.unit
          case FlywayStatus.Outdated(_, _, pending) =>
            logger.debug(
              s"Pending migrations ${pending} on attempt ${attempt} of ${retries} attempts"
            )
            Future.failed(MigrationIncomplete(pending))
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
    getStatus(flyway) match {
      case FlywayStatus.SchemaBeforeBaseline(current, baseline) =>
        val ex = SchemaOutdated(current, baseline)
        logger.warn(ex.getMessage)
        throw ex
      case FlywayStatus.NeedsBaselineReset(oldVersions) =>
        val ex = NeedsBaselineReset(oldVersions)
        logger.warn(ex.getMessage)
        throw ex
      case FlywayStatus.DatabaseEmpty(pendingMigrations) =>
        logger.info(
          s"Running Flyway migration on empty database with $pendingMigrations migrations pending..."
        )
        val migrationResult = flyway.migrate()
        logger.info(
          s"Flyway schema migration finished successfully, applying ${migrationResult.migrationsExecuted} steps on empty database."
        )
      case FlywayStatus.NoMigrationsPending(_, appliedMigrations) =>
        logger.info(s"No pending migrations with ${appliedMigrations} migrations applied.")
      case FlywayStatus.Outdated(_, appliedMigrations, pendingMigrations) =>
        val ex = MigrateOnEmptySchema(appliedMigrations, pendingMigrations)
        logger.warn(ex.getMessage)
        throw ex
    }
  }
}

private[platform] object FlywayMigrationsBase {

  sealed abstract class FlywayMigrationsException(msg: String) extends RuntimeException(msg)

  final case class MigrationIncomplete(pendingMigrations: Int)
      extends FlywayMigrationsException(
        s"Migration incomplete with $pendingMigrations migrations remaining"
      )
  final case class MigrateOnEmptySchema(appliedMigrations: Int, pendingMigrations: Int)
      extends FlywayMigrationsException(
        s"Asked to migrate-on-empty-schema, but encountered neither an empty database with $appliedMigrations " +
          s"migrations already applied nor a fully-migrated databases with $pendingMigrations migrations pending."
      )
  final case class SchemaOutdated(currentVersion: String, minimumVersion: String)
      extends FlywayMigrationsException(
        s"Database schema is out of date. Current schema version is $currentVersion, minimum required version is $minimumVersion. " +
          "This can happen when trying to upgrade from an SDK version that is not supported anymore. " +
          "Please read the documentation on data continuity guarantees and use an appropriate previous version of the SDK to migrate the database."
      )
  final case class NeedsBaselineReset(oldVersions: List[String])
      extends FlywayMigrationsException(
        s"Database schema needs a baseline reset. The following applied versions are below the baseline: ${oldVersions
          .mkString(", ")} "
      )
}
