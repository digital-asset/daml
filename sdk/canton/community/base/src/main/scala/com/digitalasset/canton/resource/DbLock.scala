// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.Eval
import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.*
import com.digitalasset.canton.crypto.{Hash, HashAlgorithm, HashPurpose, PseudoRandom}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting, PrettyUtil}
import com.digitalasset.canton.logging.{
  ErrorLoggingContext,
  NamedLoggerFactory,
  NamedLogging,
  TracedLogger,
}
import com.digitalasset.canton.resource.DbStorage.Profile
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.retry.NoExceptionRetryPolicy
import com.digitalasset.canton.util.{ErrorUtil, FutureUnlessShutdownUtil, retry}
import com.google.common.annotations.VisibleForTesting
import org.slf4j.event.Level
import slick.jdbc.JdbcBackend.Database
import slick.jdbc.SetParameter
import slick.util.AsyncExecutorWithShutdown

import java.nio.ByteBuffer
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

sealed abstract case class DbLockCounter(value: Int) extends PrettyPrinting {

  require(value > 0, s"DbLockCounter must be at least 1, but is $value.")
  require(value <= 256, s"DbLockCounter must be at most 256, but is $value.")

  override protected def pretty: Pretty[DbLockCounter] = prettyOfParam(_.value)
}

object DbLockCounter {

  private val counters: mutable.Set[Int] = mutable.Set.empty[Int]

  def apply(counter: Int): DbLockCounter =
    if (counters.add(counter))
      new DbLockCounter(counter) {}
    else
      throw new IllegalArgumentException(s"DbLockCounter $counter already in use")
}

/** We pre-allocate a set of counters that are used as part of the lock ID allocation. */
object DbLockCounters {
  val PARTICIPANT_WRITE = DbLockCounter(1)
  val INDEXER_MAIN = DbLockCounter(2)
  val INDEXER_WORKER = DbLockCounter(3)
  val SEQUENCER_INIT = DbLockCounter(4)

  val SEQUENCER_WRITERS_MAIN: Array[DbLockCounter] =
    DbLockCounters.range(5, DbLockConfig.MAX_SEQUENCER_WRITERS_AVAILABLE)
  val NODE_MIGRATIONS = DbLockCounter(37)
  val MEDIATOR_WRITE = DbLockCounter(38)

  // A shared lock used by all the writers in the write DB connection pool
  val PARTICIPANT_WRITERS = DbLockCounter(39)
  val MEDIATOR_WRITERS = DbLockCounter(40)

  val SEQUENCER_WRITERS_POOL: Array[DbLockCounter] =
    DbLockCounters.range(41, DbLockConfig.MAX_SEQUENCER_WRITERS_AVAILABLE)

  val SEQUENCER_PRUNING_SCHEDULER_WRITE = DbLockCounter(74)
  val SEQUENCER_PRUNING_SCHEDULER_WRITERS = DbLockCounter(75)

  val SEQUENCER_INIT_WORKER = DbLockCounter(76)

  // First counter used for testing purposes
  // This and all subsequent counters are reserved for testing.
  val FIRST_TESTING = 77

  private def range(startLockCounter: Int, num: Int): Array[DbLockCounter] = {
    require(num > 0, "must create at least one lock counter")

    startLockCounter.until(startLockCounter + num).map(DbLockCounter(_)).toArray
  }
}

/** Common Wrapper for DB lock identifier
  */
final case class DbLockId private (id: Int) extends PrettyPrinting {
  override protected def pretty: Pretty[DbLockId] = prettyOfParam(_.id)
}

object DbLockId {
  implicit val setParameterLockId: SetParameter[DbLockId] = (l, pp) => pp.setInt(l.id)

  private[resource] def create(scope: String, counter: DbLockCounter)(implicit
      loggingContext: ErrorLoggingContext
  ): DbLockId = {
    val scopeHash = Hash
      .build(HashPurpose.DbLockId, HashAlgorithm.Sha256)
      // It's fine to omit the length prefix because we're only adding another fixed-size item before finishing.
      .addWithoutLengthPrefix(scope)
      .finish()

    val lockId = {
      val len = java.lang.Integer.BYTES
      val buffer = ByteBuffer.allocate(len)
      buffer.put(scopeHash.unwrap.toByteArray, 0, len)

      // `flip` to switch from putting into the bytebuffer to getting from the buffer
      buffer.flip()

      // Bit 1 to 8: counter
      // Bit 9 to 29: scopeHash
      // Bit 30: always set
      (counter.value - 1) |
        (buffer.getInt & 0x1fffff00) |
        (1 << 29)
    }

    ErrorUtil.requireState(lockId >= 0, s"Generated lock id $lockId must be non-negative")
    ErrorUtil.requireState(
      lockId <= 1073741823,
      s"Generated lock id $lockId must be maximal 30bit",
    )

    DbLockId(lockId)
  }

  def allocate(dbConfig: DbConfig, lockCounter: DbLockCounter, loggerFactory: NamedLoggerFactory)(
      implicit traceContext: TraceContext
  ): Either[DbLockError, DbLockId] =
    dbConfig match {
      case pgConfig: DbConfig.Postgres =>
        PostgresDbLock
          .allocateLockId(pgConfig, lockCounter)(loggerFactory)
      case _ => Left(DbLockError.UnsupportedDatabaseConfig(dbConfig))
    }
}

sealed trait DbLockMode extends Product with Serializable with PrettyPrinting
object DbLockMode {
  case object Exclusive extends DbLockMode {
    override protected def pretty: Pretty[Exclusive.type] = prettyOfObject[Exclusive.type]
  }
  case object Shared extends DbLockMode {
    override protected def pretty: Pretty[Shared.type] = prettyOfObject[Shared.type]
  }
}

/** Abstraction for an application-specific database lock.
  *
  * The lock is identified by an integer and bound to the given database's session/connection.
  *
  * If the connection is lost, the database releases the lock and the DbLock sets the lock state to
  * Lost. The caller's connection management has to recreate and try to reacquire the lock with the
  * given id. NOTE: The database must be configured with a single connection.
  */
trait DbLock extends NamedLogging with FlagCloseable with HasCloseContext {

  import DbLock.*
  import DbLockError.*

  @VisibleForTesting
  private[resource] val lockState: AtomicReference[LockState] = new AtomicReference(LockState.Free)

  private def transition(
      expectedState: LockState,
      newState: LockState,
      error: LockState => DbLockError,
  ): Either[DbLockError, Unit] = {
    val updatedState = lockState.getAndUpdate { currentState =>
      if (currentState == expectedState) newState else currentState
    }
    Either.cond(updatedState == expectedState, (), error(updatedState))
  }

  // A transition that must succeed, otherwise we have a bug.
  private def transitionOrFail(expectedState: LockState, newState: LockState)(implicit
      traceContext: TraceContext
  ): Unit = {
    val _ = transition(
      expectedState,
      newState,
      errorState =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Failed to transition lock $lockId from $expectedState to $newState: current state is $errorState"
          )
        ),
    )
  }

  private def acquireError(
      errorState: LockState
  )(implicit traceContext: TraceContext): DbLockError = errorState match {
    case LockState.Acquired => LockAlreadyAcquired(lockId)
    case LockState.Acquiring => LockAlreadyInAcquisition(lockId)
    case LockState.Releasing => LockNotFree(lockId, "Lock is being released")
    case LockState.Lost => LostLock(lockId)
    case LockState.Free =>
      ErrorUtil.internalError(new IllegalStateException(s"Acquire error called on free state"))
  }

  private def acquireInternalError[A](
      error: DbLockError
  )(implicit traceContext: TraceContext): Either[DbLockError, A] = {
    logger.debug(s"Failed to acquire lock $lockId: $error")

    // Failed to acquire, try move back to free state
    transition(
      LockState.Acquiring,
      LockState.Free,
      {
        case LockState.Lost => LostLock(lockId)
        case state => LockInvalidState(lockId, s"Invalid state during acquire: $state")
      },
    ).flatMap(_ => Left(error)) // Return original error if the transition worked
  }

  private def releaseError(
      errorState: LockState
  )(implicit traceContext: TraceContext): DbLockError = errorState match {
    case LockState.Free => LockAlreadyReleased(lockId)
    case LockState.Releasing => LockAlreadyInReleasing(lockId)
    case LockState.Acquiring => LockNotAcquired(lockId, "Lock is being acquired")
    case LockState.Lost => LostLock(lockId)
    case LockState.Acquired =>
      ErrorUtil.internalError(new IllegalStateException(s"Release error called on acquired state"))
  }

  private def releaseInternalError(
      error: DbLockError
  )(implicit traceContext: TraceContext): Either[DbLockError, Unit] = {
    logger.debug(s"Failed to release lock $lockId: $error")

    // Failed to release, try move back to acquired state
    transition(
      LockState.Releasing,
      LockState.Acquired,
      {
        case LockState.Lost => LostLock(lockId)
        case state => LockInvalidState(lockId, s"Invalid state during release: $state")
      },
    ).flatMap(_ => Left(error)) // Return original error if the transition worked
  }

  // Periodically check the lock's acquisition state and call the `lostHook` if the lock was lost.
  private def checkLock(now: CantonTimestamp): Unit = {
    import TraceContext.Implicits.Empty.*

    logger.trace(s"Checking lock status of $lockId at $now")

    // Only check the lock if we still think it's in acquired state
    if (lockState.get() == LockState.Acquired) {

      // Only retry when the lock check query was rejected by slick's queue
      implicit val success: retry.Success[Either[DbLockError, Boolean]] = retry.Success {
        case Left(DbLockError.LockCheckRejected(_lockId)) =>
          false
        case _ => true
      }

      def runLockCheck(): FutureUnlessShutdown[Unit] =
        EitherT {
          retry
            .Backoff(
              logger,
              this,
              15,
              200.millis,
              5.second,
              functionFullName,
              // We only retry when the lock check is rejected due to contention, so we can log on DEBUG
              retryLogLevel = Some(Level.DEBUG),
            )
            .unlessShutdown(
              hasLock.mapK(FutureUnlessShutdown.outcomeK).value,
              NoExceptionRetryPolicy,
            )
            .tapOnShutdown(logger.debug("Stopped lock check due to shutdown"))
            .recover { case NonFatal(e) =>
              // When an unexpected exception is thrown we treat the lock check to have failed, resulting in the lock to assumed to be lost
              UnlessShutdown.Outcome(
                Left(
                  FailedToCheckLock(
                    lockId,
                    s"Lock check failed due to an exception: ${ErrorUtil.messageWithStacktrace(e)}",
                  )
                )
              )
            }
        }
          .valueOr {
            case _: LockCheckRejected =>
              logger.debug(s"Lock check failed due to contention, assuming lock is acquired")
              true
            case err if executorShuttingDown.value =>
              logger.debug(
                "Lock check failed but executor is shutting down. Ignoring failed check.",
                err,
              )
              false
            case err =>
              if (!isClosing) {
                // If the hasLock query fails (e.g. underlying connection closed), we indicate that the lock is lost
                logger.warn(
                  s"Failed to check database lock status for $lockId, assuming lost: $err"
                )
              }
              false
          }
          .map { stillAcquired =>
            if (!stillAcquired) {
              val state = lockState.getAndUpdate { currentState =>
                if (currentState == LockState.Acquired) LockState.Lost else currentState
              }
              state match {
                case LockState.Acquired =>
                  if (!isClosing && !executorShuttingDown.value)
                    logger.warn(s"Lock $lockId was lost")
                case LockState.Lost | LockState.Free => ()
                case LockState.Releasing | LockState.Acquiring => scheduleCheckLock(clock.now)
              }
            } else {
              logger.trace(s"Lock $lockId still acquired at $now")
              scheduleCheckLock(clock.now)
            }
          }

      // Do not wait for the lock check to have completed, to not block any other tasks on the clock scheduler
      FutureUnlessShutdownUtil.doNotAwaitUnlessShutdown(
        synchronizeWithClosing("check-lock")(runLockCheck()),
        "Failed to check lock",
      )
    }
  }

  private def scheduleCheckLock(now: CantonTimestamp): Unit = {
    // Add a jitter of 20% of the health check period to the scheduled lock check time to scatter the checks, such that they don't run at the same time
    val period = config.healthCheckPeriod.asJava
    val jitter = Duration.ofMillis(PseudoRandom.randomLong((period.toMillis * 0.2).toLong))

    val checkAt = now.add(config.healthCheckPeriod.asJava).add(jitter)

    logger.trace(s"At $now schedule next health check for $checkAt")(TraceContext.empty)

    clock.scheduleAt(checkLock, checkAt).discard
  }

  protected implicit def ec: ExecutionContext

  protected def profile: DbStorage.Profile

  protected def clock: Clock

  protected def executorShuttingDown: Eval[Boolean]

  protected def database: Database

  protected def timeouts: ProcessingTimeout

  protected def config: DbLockConfig

  protected def mode: DbLockMode

  /** Check if the lock is still held by this session. */
  protected[resource] def hasLock(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Boolean]

  /** Internal (DB specific) blocking acquisition of the lock. */
  protected def acquireInternal()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Unit]

  /** Internal (DB specific) non-blocking acquisition of the lock. */
  protected def tryAcquireInternal()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Boolean]

  /** Internal (DB specific) release of the lock. */
  @VisibleForTesting
  protected[resource] def releaseInternal()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Unit]

  /** The application-specific lock ID, which remains the same for the lifetime of the lock. */
  def lockId: DbLockId

  /** Return the PIDs of the processes that own this lock.
    */
  def getLockOwners()(implicit
      traceContext: TraceContext
  ): EitherT[Future, DbLockError, Vector[Long]]

  def isAcquired: Boolean = lockState.get() == LockState.Acquired

  def isLost: Boolean = lockState.get() == LockState.Lost

  /** A blocking acquisition of the lock.
    *
    * Blocks until the lock has been acquired. A lock can only be acquired and attempted to be
    * acquired once. A second attempt will fail even when the first attempt is still pending
    * acquisition of the lock.
    */
  def acquire()(implicit traceContext: TraceContext): EitherT[Future, DbLockError, Unit] =
    for {
      _ <- transition(LockState.Free, LockState.Acquiring, acquireError).toEitherT
      _ = logger.trace(s"Acquiring lock $lockId")
      _ <- acquireInternal().leftFlatMap(err => acquireInternalError[Unit](err).toEitherT)
      _ = transitionOrFail(LockState.Acquiring, LockState.Acquired)
      _ = logger.trace(s"Acquired lock $lockId")
      _ = scheduleCheckLock(clock.now)
    } yield ()

  /** A non-blocking acquisition of the lock.
    *
    * Tries to acquire the lock and immediately returns true or false if the lock was acquired or
    * not. A lock can only be acquired and attempted to be acquired once.
    */
  def tryAcquire()(implicit traceContext: TraceContext): EitherT[Future, DbLockError, Boolean] =
    for {
      _ <- transition(LockState.Free, LockState.Acquiring, acquireError).toEitherT
      _ = logger.trace(s"Try acquiring lock $lockId")
      acquired <- tryAcquireInternal().leftFlatMap(err =>
        acquireInternalError[Boolean](err).toEitherT
      )
    } yield {
      if (acquired) {
        transitionOrFail(LockState.Acquiring, LockState.Acquired)
        logger.trace(s"Acquired lock $lockId")
        scheduleCheckLock(clock.now)
      } else
        transitionOrFail(LockState.Acquiring, LockState.Free)

      acquired
    }

  /** Explicitly release the lock. */
  def release()(implicit traceContext: TraceContext): EitherT[Future, DbLockError, Unit] =
    for {
      _ <- transition(LockState.Acquired, LockState.Releasing, releaseError).toEitherT
      _ = logger.trace(s"Releasing lock $lockId")
      _ <- releaseInternal().leftFlatMap(err => releaseInternalError(err).toEitherT)
      _ = transitionOrFail(LockState.Releasing, LockState.Free)
      _ = logger.trace(s"Released lock $lockId")
    } yield ()

  /** Returns true if the lock is already taken by another session */
  def isTaken(implicit traceContext: TraceContext): EitherT[Future, DbLockError, Boolean]
}

object DbLock {

  implicit val pretty: Pretty[DbLock] = {
    import PrettyUtil.*
    prettyOfClass(param("id", _.lockId), param("mode", _.mode))
  }

  private[canton] def isSupported(
      profile: DbStorage.Profile
  ): Either[String, DbStorage.Profile & DbStorage.DbLockSupport] =
    profile match {
      case pg: Profile.Postgres => Right(pg)
      case _: Profile.H2 =>
        Left("Database profile must be Postgres but H2 was configured.")
    }

  private[resource] sealed trait LockState
  private[resource] object LockState {
    case object Free extends LockState
    case object Acquiring extends LockState
    case object Acquired extends LockState
    case object Releasing extends LockState
    case object Lost extends LockState
  }

  private def create(
      profile: DbStorage.Profile & DbStorage.DbLockSupport,
      database: Database,
      lockConfig: DbLockConfig,
      lockId: DbLockId,
      lockMode: DbLockMode,
      timeouts: ProcessingTimeout,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      executorShuttingDown: Eval[Boolean] = Eval.now(false),
  )(implicit ec: ExecutionContext): DbLock =
    profile match {
      case pgProfile: DbStorage.Profile.Postgres =>
        new DbLockPostgres(
          pgProfile,
          database,
          lockId,
          lockMode,
          lockConfig,
          timeouts,
          clock,
          loggerFactory.append("lockId", lockId.toString),
          executorShuttingDown,
        )
    }

  def create(
      profile: DbStorage.Profile & DbStorage.DbLockSupport,
      database: Database,
      dbConfig: DbConfig,
      lockConfig: DbLockConfig,
      lockCounter: DbLockCounter,
      lockMode: DbLockMode,
      timeouts: ProcessingTimeout,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): Either[DbLockError, DbLock] =
    for {
      // Ensures that the database for the lock only uses one connection
      _ <- Either.cond(
        database.source.maxConnections.contains(1),
        (),
        DbLockError.InvalidDatabaseConfig("Database must be configured with max 1 connection"),
      )
      lockId <- DbLockId.allocate(dbConfig, lockCounter, loggerFactory)
      logger = TracedLogger(DbLock.getClass, loggerFactory)
      _ = logger.debug(s"Allocated lock-id $lockId for lock-counter $lockCounter")
      lock = create(
        profile,
        database,
        lockConfig,
        lockId,
        lockMode,
        timeouts,
        clock,
        loggerFactory,
      )
    } yield lock

  def create(
      profile: DbStorage.Profile & DbStorage.DbLockSupport,
      connection: KeepAliveConnection,
      lockConfig: DbLockConfig,
      lockId: DbLockId,
      lockMode: DbLockMode,
      timeouts: ProcessingTimeout,
      clock: Clock,
      loggerFactory: NamedLoggerFactory,
      writeExecutor: AsyncExecutorWithShutdown,
  )(implicit ec: ExecutionContext): DbLock = {
    val logger = loggerFactory.getLogger(DbLock.getClass)
    val database =
      KeepAliveConnection.createDatabaseFromConnection(
        connection,
        logger,
        writeExecutor,
      )

    val executorShuttingDown = Eval.always(writeExecutor.isShuttingDown)

    create(
      profile,
      database,
      lockConfig,
      lockId,
      lockMode,
      timeouts,
      clock,
      loggerFactory,
      executorShuttingDown,
    )
  }

}

sealed trait DbLockConfigError extends Product with Serializable with PrettyPrinting {}

sealed trait DbLockError extends Product with Serializable with PrettyPrinting {}

object DbLockError {

  final case class UnsupportedDatabaseProfile(profile: DbStorage.Profile) extends DbLockError {
    override protected def pretty: Pretty[UnsupportedDatabaseProfile] = prettyOfClass(
      unnamedParam(_.profile)
    )
  }

  final case class UnsupportedDatabaseConfig(dbConfig: DbConfig) extends DbLockError {
    override protected def pretty: Pretty[UnsupportedDatabaseConfig] = prettyOfClass(
      unnamedParam(_.dbConfig)
    )
  }

  final case class InvalidDatabaseConfig(error: String) extends DbLockError {
    override protected def pretty: Pretty[InvalidDatabaseConfig] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }

  final case class FailedToAcquireLock(lockId: DbLockId, error: String) extends DbLockError {
    override protected def pretty: Pretty[FailedToAcquireLock] =
      prettyOfClass(param("lockId", _.lockId), param("error", _.error.unquoted))
  }

  final case class LockAcquireRejected(lockId: DbLockId) extends DbLockError {
    override protected def pretty: Pretty[LockAcquireRejected] =
      prettyOfClass(param("lockId", _.lockId))
  }

  final case class FailedToReleaseLock(lockId: DbLockId, error: String) extends DbLockError {
    override protected def pretty: Pretty[FailedToReleaseLock] =
      prettyOfClass(param("lockId", _.lockId), param("error", _.error.unquoted))
  }

  final case class LockReleaseRejected(lockId: DbLockId) extends DbLockError {
    override def pretty: Pretty[LockReleaseRejected] =
      prettyOfClass(param("lockId", _.lockId))
  }

  final case class FailedToCheckLock(lockId: DbLockId, error: String) extends DbLockError {
    override protected def pretty: Pretty[FailedToCheckLock] =
      prettyOfClass(param("lockId", _.lockId), param("error", _.error.unquoted))

  }

  final case class FailedToGetLockOwners(lockId: DbLockId, error: String) extends DbLockError {
    override protected def pretty: Pretty[FailedToGetLockOwners] =
      prettyOfClass(param("lockId", _.lockId), param("error", _.error.unquoted))

  }

  final case class LockCheckRejected(lockId: DbLockId) extends DbLockError {
    override protected def pretty: Pretty[LockCheckRejected] =
      prettyOfClass(param("lockId", _.lockId))

  }

  final case class LockAlreadyAcquired(lockId: DbLockId) extends DbLockError {
    override protected def pretty: Pretty[LockAlreadyAcquired] =
      prettyOfClass(param("lockId", _.lockId))
  }

  final case class LockAlreadyReleased(lockId: DbLockId) extends DbLockError {
    override protected def pretty: Pretty[LockAlreadyReleased] =
      prettyOfClass(param("lockId", _.lockId))
  }

  final case class LockAlreadyInAcquisition(lockId: DbLockId) extends DbLockError {
    override protected def pretty: Pretty[LockAlreadyInAcquisition] =
      prettyOfClass(param("lockId", _.lockId))
  }

  final case class LockAlreadyInReleasing(lockId: DbLockId) extends DbLockError {
    override protected def pretty: Pretty[LockAlreadyInReleasing] =
      prettyOfClass(param("lockId", _.lockId))
  }

  final case class LockNotFree(lockId: DbLockId, error: String) extends DbLockError {
    override protected def pretty: Pretty[LockNotFree] =
      prettyOfClass(param("lockId", _.lockId), param("error", _.error.unquoted))
  }

  final case class LockNotAcquired(lockId: DbLockId, error: String) extends DbLockError {
    override protected def pretty: Pretty[LockNotAcquired] =
      prettyOfClass(param("lockId", _.lockId), param("error", _.error.unquoted))

  }

  final case class LostLock(lockId: DbLockId) extends DbLockError {
    override protected def pretty: Pretty[LostLock] =
      prettyOfClass(param("lockId", _.lockId))
  }

  final case class LockInvalidState(lockId: DbLockId, error: String) extends DbLockError {
    override protected def pretty: Pretty[LockInvalidState] =
      prettyOfClass(param("lockId", _.lockId), param("error", _.error.unquoted))

  }
}
