// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{DbConfig, DbLockedConnectionConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.{CloseContext, FutureUnlessShutdown}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.resource.DbStorage.{DbLockSupport, Profile}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry
import slick.util.AsyncExecutorWithMetrics

import scala.concurrent.ExecutionContext

object WithDbLock {
  sealed trait WithDbLockError extends Product with Serializable with PrettyPrinting

  object WithDbLockError {
    final case class DatabaseConfigurationError(error: String) extends WithDbLockError {
      override protected def pretty: Pretty[DatabaseConfigurationError] = prettyOfClass(
        unnamedParam(_.error.unquoted)
      )
    }

    final case class ConnectionError(error: DbLockedConnectionError) extends WithDbLockError {
      override protected def pretty: Pretty[ConnectionError] = prettyOfClass(unnamedParam(_.error))
    }

    final case class LockAcquisitionError(error: DbLockError) extends WithDbLockError {
      override protected def pretty: Pretty[LockAcquisitionError] = prettyOfClass(
        unnamedParam(_.error)
      )
    }

    final case class OperationError[A: Pretty](error: A) extends WithDbLockError {
      override protected def pretty: Pretty[OperationError[A]] = prettyOfClass(
        unnamedParam(_.error)
      )
    }
  }

  /** Attempts to acquire an exclusive lock with the given `lockCounter` and will block until this
    * occurs. The block will be executed once the lock is acquired and finally the lock as well as
    * the connection holding the lock will be released.
    *
    * If the provided `storage` instance does not support creating DB provided application locks the
    * block will be directly run with no synchronization. This is considered reasonable as this is
    * mainly used for synchronizing multiple processes around a shared database, however when using
    * in-memory or h2 stores this storage is unable or unlikely to be shared, and is likely the only
    * process running the operation.
    */
  def withDbLock[A: Pretty, B](
      lockName: String,
      lockCounter: DbLockCounter,
      timeouts: ProcessingTimeout,
      dbConfig: DbConfig,
      connectionConfig: DbLockedConnectionConfig,
      profile: Profile,
      futureSupervisor: FutureSupervisor,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      logLockOwnersOnLockAcquisitionAttempt: Boolean,
  )(fn: => EitherT[FutureUnlessShutdown, A, B])(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, WithDbLockError, B] = {
    val logger = loggerFactory.getLogger(WithDbLock.getClass)
    val tracedLogger = TracedLogger(logger)

    def withDbDistributedLock(
        dbProfile: Profile with DbLockSupport
    ): EitherT[FutureUnlessShutdown, WithDbLockError, B] = for {
      lockId <- DbLockId
        .allocate(dbConfig, lockCounter, loggerFactory)
        .leftMap(WithDbLockError.LockAcquisitionError.apply)
        .toEitherT[FutureUnlessShutdown]

      ds <- DbLockedConnection
        .createDataSource(
          dbConfig.config,
          1,
          connectionConfig.connectionTimeout.toInternal,
        )(ErrorLoggingContext.fromTracedLogger(tracedLogger))
        .leftMap(WithDbLockError.DatabaseConfigurationError.apply)
        .toEitherT[FutureUnlessShutdown]

      executor = AsyncExecutorWithMetrics.createSingleThreaded(lockName, logger)

      lockedConnection = DbLockedConnection.create(
        dbProfile,
        ds,
        lockId,
        DbLockMode.Exclusive,
        connectionConfig,
        isMainConnection = true,
        timeouts,
        exitOnFatalFailures = false,
        clock,
        loggerFactory
          .append("connId", lockName)
          .append("lockId", lockId.toString),
        futureSupervisor,
        executor,
        logLockOwnersOnLockAcquisitionAttempt,
      )

      // Wait until the lock is acquired
      _ <- DbLockedConnection
        .awaitActive(lockedConnection, retry.Forever, 200, tracedLogger)
        .leftMap[WithDbLockError] { err =>
          lockedConnection.close()
          ds.close()
          executor.close()
          WithDbLockError.ConnectionError(err)
        }
        .mapK(FutureUnlessShutdown.liftK)

      result <- fn
        .leftMap[WithDbLockError](WithDbLockError.OperationError(_))
        .thereafter { _ =>
          lockedConnection.close()
          ds.close()
          executor.close()
        }
    } yield {
      result
    }

    DbLock.isSupported(profile) match {
      case Right(dbProfile) => withDbDistributedLock(dbProfile)
      case Left(_unsupported) =>
        tracedLogger.debug(
          s"Distributed locks for the configured Db storage profile ($profile) are not supported. Running [$lockName] without lock."
        )
        fn.leftMap[WithDbLockError](WithDbLockError.OperationError(_))
    }
  }

  def withDbLock[A: Pretty, B](
      lockName: String,
      lockCounter: DbLockCounter,
      timeouts: ProcessingTimeout,
      storage: Storage,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
      connectionConfig: DbLockedConnectionConfig,
      logLockOwnersOnLockAcquisitionAttempt: Boolean,
  )(fn: => EitherT[FutureUnlessShutdown, A, B])(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
      closeContext: CloseContext,
  ): EitherT[FutureUnlessShutdown, WithDbLockError, B] =
    storage match {
      case dbStorage: DbStorage =>
        withDbLock(
          lockName,
          lockCounter,
          timeouts,
          dbStorage.dbConfig,
          connectionConfig,
          dbStorage.profile,
          futureSupervisor,
          clock,
          loggerFactory,
          logLockOwnersOnLockAcquisitionAttempt,
        )(fn)
      case other =>
        val logger = TracedLogger(WithDbLock.getClass, loggerFactory)
        logger.debug(
          s"Distributed locks for the configured storage ($other) are not supported. Running [$lockName] without lock."
        )
        fn.leftMap[WithDbLockError](WithDbLockError.OperationError(_))
    }

}
