// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.Eval
import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.config.{DbConfig, DbLockConfig, ProcessingTimeout}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.NoConnectionAvailable
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.ShowUtil.*
import org.postgresql.Driver
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.canton.SQLActionBuilder

import java.util.concurrent.RejectedExecutionException
import scala.concurrent.{ExecutionContext, Future}

/** A Postgres DB lock using advisory locks */
class DbLockPostgres private[resource] (
    override protected val profile: DbStorage.Profile.Postgres,
    override protected val database: Database,
    override val lockId: DbLockId,
    override protected val mode: DbLockMode,
    override protected val config: DbLockConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val clock: Clock,
    protected val loggerFactory: NamedLoggerFactory,
    override protected val executorShuttingDown: Eval[Boolean],
)(override protected implicit val ec: ExecutionContext)
    extends DbLock {

  import com.digitalasset.canton.resource.DbStorage.Implicits.BuilderChain.*
  import slick.jdbc.canton.ActionBasedSQLInterpolation.Implicits.*

  /** Returns the list of process PIDs owning the lock.
    */
  def getLockOwners()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Vector[Long]] = {
    val lockCheckQuery: SQLActionBuilder =
      sql"""select pid from pg_locks where locktype = 'advisory' and objid = $lockId and granted = true"""

    EitherTUtil
      .fromFuture(
        database.run(lockCheckQuery.as[Long]),
        {
          case _: RejectedExecutionException | _: NoConnectionAvailable =>
            DbLockError.LockCheckRejected(lockId)
          case err => DbLockError.FailedToGetLockOwners(lockId, show"$err")
        },
      )
      .leftWiden[DbLockError]
  }

  private def lockCheck(
      pidFilter: SQLActionBuilder
  )(implicit traceContext: TraceContext): EitherT[Future, DbLockError, Boolean] = {
    import profile.DbStorageAPI.jdbcActionExtensionMethods

    val lockCheckQuery: SQLActionBuilder =
      sql"""select 1 from pg_locks where locktype = 'advisory' and objid = $lockId and granted = true and """ ++
        pidFilter ++ sql""" limit 1"""

    // Set an explicit timeout on the lock check query enforced by the server
    val lockCheckAction = lockCheckQuery
      .as[Int]
      .headOption
      .withStatementParameters(statementInit =
        _.setQueryTimeout(config.healthCheckTimeout.toInternal.toSecondsTruncated(logger).unwrap)
      )

    EitherTUtil
      .fromFuture(
        database.run(lockCheckAction).map(_.isDefined),
        {
          case _: RejectedExecutionException | _: NoConnectionAvailable =>
            DbLockError.LockCheckRejected(lockId)
          case err => DbLockError.FailedToCheckLock(lockId, show"$err")
        },
      )
      .leftWiden[DbLockError]
  }

  private lazy val modeSuffix: String = mode match {
    case DbLockMode.Shared => "_shared"
    case DbLockMode.Exclusive => ""
  }

  override protected[resource] def hasLock(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Boolean] =
    lockCheck(sql"pid = pg_backend_pid()")

  override def acquireInternal()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Unit] = {
    val lockQuery = sql"select pg_advisory_lock#$modeSuffix($lockId)".as[String].map(_ => ())

    EitherTUtil.fromFuture(
      database.run(lockQuery),
      {
        case _: RejectedExecutionException | _: NoConnectionAvailable =>
          DbLockError.LockAcquireRejected(lockId)
        case err => DbLockError.FailedToAcquireLock(lockId, show"$err")
      },
    )
  }

  override def tryAcquireInternal()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Boolean] = {
    val lockQuery = sql"select pg_try_advisory_lock#$modeSuffix($lockId)".as[Boolean].headOption

    for {
      acquiredO <- EitherTUtil.fromFuture(
        database.run(lockQuery),
        {
          case _: RejectedExecutionException | _: NoConnectionAvailable =>
            DbLockError.LockAcquireRejected(lockId)
          case err => DbLockError.FailedToAcquireLock(lockId, show"$err")
        },
      )
      acquired <- acquiredO
        .toRight[DbLockError](
          DbLockError.FailedToAcquireLock(lockId, s"No result from trying to acquire lock")
        )
        .toEitherT
    } yield acquired
  }

  override def releaseInternal()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Unit] = {
    val releaseQuery = sql"select pg_advisory_unlock#$modeSuffix($lockId)".as[Boolean].headOption

    for {
      releasedO <- EitherTUtil.fromFuture(
        database.run(releaseQuery),
        {
          case _: RejectedExecutionException | _: NoConnectionAvailable =>
            DbLockError.LockReleaseRejected(lockId)
          case err => DbLockError.FailedToReleaseLock(lockId, show"$err")
        },
      )
      released <- releasedO
        .toRight(DbLockError.FailedToReleaseLock(lockId, s"No result from trying to release lock"))
        .toEitherT
      _ <- EitherT
        .cond(released, (), DbLockError.FailedToReleaseLock(lockId, s"Lock was not released"))
        .leftWiden[DbLockError]
    } yield ()
  }

  override def isTaken(implicit traceContext: TraceContext): EitherT[Future, DbLockError, Boolean] =
    lockCheck(sql"pid != pg_backend_pid()")
}

object PostgresDbLock {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  private[resource] def allocateLockId(config: DbConfig.Postgres, counter: DbLockCounter)(
      loggerFactory: NamedLoggerFactory
  )(implicit traceContext: TraceContext): Either[DbLockError, DbLockId] = {

    import slick.util.ConfigExtensionMethods.*

    implicit val loggingContext =
      ErrorLoggingContext.fromTracedLogger(
        TracedLogger(loggerFactory.getLogger(classOf[DbLockPostgres]))
      )

    for {
      // Use the DB name as the scope of the lock id
      lockScope <- config.config
        .getStringOpt("properties.databaseName")
        .orElse {
          config.config.getStringOpt("url").flatMap { url =>
            val props = Driver.parseURL(url, null)
            Option(props.getProperty("PGDBNAME"))
          }
        }
        .toRight[DbLockError](
          DbLockError.InvalidDatabaseConfig("Unable to extract DB name for lock id scope")
        )
    } yield {
      val lockId = DbLockId.create(lockScope, counter)
      loggingContext.debug(
        s"Allocated new lock ID $lockId for scope $lockScope and counter $counter"
      )
      lockId
    }
  }
}
