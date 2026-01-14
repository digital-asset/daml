// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.{DbLockConfig, DefaultProcessingTimeouts}
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryCriterion
import com.digitalasset.canton.logging.{SuppressingLogger, TracedLogger}
import com.digitalasset.canton.store.db.*
import com.digitalasset.canton.time.{PositiveFiniteDuration, SimClock}
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.AllExceptionRetryPolicy
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import com.typesafe.scalalogging.Logger
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec
import org.slf4j.event.Level
import slick.jdbc.JdbcBackend.Database
import slick.util.AsyncExecutorWithMetrics

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

object UniqueDbLockCounter {
  private lazy val lockCounter = new AtomicInteger(DbLockCounters.FIRST_TESTING)

  def get(): DbLockCounter = DbLockCounter(lockCounter.getAndIncrement())
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
trait DbLockTest extends FixtureAsyncWordSpec with BaseTest with HasExecutorService {

  def parallelExecutionContext: ExecutionContext = executorService

  protected lazy val lockConfig: DbLockConfig = DbLockConfig()

  private lazy val ds = DbLockedConnection
    .createDataSource(
      baseDbConfig = setup.config.config,
      poolSize = 1,
      connectionTimeout = PositiveFiniteDuration.tryOfSeconds(10),
    )
    .valueOrFail("Failed to create datasource")

  case class FixtureParam(lock: DbLock, clock: SimClock, db: Database)

  protected def createLock(
      lockId: DbLockId,
      lockMode: DbLockMode,
      db: Database,
      clock: SimClock,
  ): DbLock

  protected def allocateLockId(counter: DbLockCounter): DbLockId

  protected def setup: DbStorageSetup

  private def createDb(): Database = {
    val executor =
      AsyncExecutorWithMetrics.createSingleThreaded(
        "DbLockTest",
        noTracingLogger,
      )
    KeepAliveConnection.createDatabaseFromConnection(
      new KeepAliveConnection(ds.createConnection()),
      Logger(logger.underlying),
      executor,
    )
  }

  override def afterAll(): Unit =
    try {
      ds.close()
      super.afterAll()
    } finally setup.close()

  def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val db = createDb()
    val clock = new SimClock(loggerFactory = loggerFactory)
    val lockId = DbLockId.create("test", UniqueDbLockCounter.get())

    // By default create an exclusive lock
    val lock = createLock(lockId, DbLockMode.Exclusive, db, clock)

    complete {
      withFixture(test.toNoArgAsyncTest(FixtureParam(lock, clock, db)))
    } lastly {
      lock.close()
      db.close()
    }
  }

  "DbLock" can {

    "acquire a lock" in { f =>
      for {
        _ <- f.lock.acquire().valueOrFail("acquire lock")
        _ <- f.lock.release().valueOrFail("release lock")
      } yield succeed

    }

    "acquire a shared lock from two sessions" in { f =>
      val sharedLockId = allocateLockId(UniqueDbLockCounter.get())
      val sharedLock1 = createLock(sharedLockId, DbLockMode.Shared, f.db, f.clock)

      // Same lock id but with a new DB connection
      val db2 = createDb()
      val sharedLock2 = createLock(sharedLockId, DbLockMode.Shared, db2, f.clock)

      val exclusiveLock = createLock(sharedLockId, DbLockMode.Exclusive, f.db, f.clock)

      for {
        _ <- sharedLock1.acquire().valueOrFail("acquire lock")
        _ <- sharedLock2.acquire().valueOrFail("acquire lock")
        // An exclusive lock on the shared lock id must fail
        acquiredExclusive <- exclusiveLock
          .tryAcquire()
          .getOrElse(false)
        _ <- sharedLock1.release().valueOrFail("release lock")
        _ <- sharedLock2.release().valueOrFail("release lock")
      } yield {
        acquiredExclusive shouldBe false
      }
    }

    "acquire a lock twice should fail" in { f =>
      for {
        _ <- f.lock.acquire().valueOrFail("acquire lock")
        fail <- f.lock.acquire().value
        _ <- f.lock.release().valueOrFail("release lock")
      } yield {
        fail.left.value shouldBe a[DbLockError.LockAlreadyAcquired]
      }
    }

    "acquire a lock from two sessions should fail" in { f =>
      // Same lock id but with a new DB connection
      val db2 = createDb()
      val lock2 = createLock(f.lock.lockId, DbLockMode.Exclusive, db2, f.clock)

      for {
        _ <- f.lock.acquire().valueOrFail("acquire lock")
        acquired <- lock2.tryAcquire().valueOrFail("try acquire lock")
        _ <- f.lock.release().valueOrFail("release lock")
      } yield {
        acquired shouldBe false
      }
    }

    "check if the lock is taken by another session" in { f =>
      // Same lock id but with a new DB connection
      val db2 = createDb()
      val lock2 = createLock(f.lock.lockId, DbLockMode.Exclusive, db2, f.clock)

      for {
        _ <- f.lock.acquire().valueOrFail("acquire lock")
        taken1 <- lock2.isTaken.valueOrFail("lock taken")
        _ <- f.lock.release().valueOrFail("release lock")
        // It might take a bit to propagate (especially if there is concurrent DB activity on other tests)
        _ = Threading.sleep(5000)
        taken2 <- lock2.isTaken.valueOrFail("lock taken")
      } yield {
        taken1 shouldBe true
        taken2 shouldBe false
      }
    }

    "release a non-acquired lock should fail" in { f =>
      for {
        fail <- f.lock.release().value
      } yield {
        fail.left.value shouldBe a[DbLockError.LockAlreadyReleased]
      }
    }

    "detect when the lock is lost" in { f =>
      for {
        _ <- f.lock.acquire().valueOrFail("acquire lock")
        // Releasing the lock without changing the lock state
        _ <- f.lock.releaseInternal().valueOrFail("release internal")
        // Advance the time to trigger a check lock
        _ = loggerFactory.assertLogs(
          // Add 1 second to the health check period to account for schedule jitter
          f.clock.advance(lockConfig.healthCheckPeriod.plusSeconds(1).asJava),
          _.warningMessage should include(s"Lock ${f.lock.lockId} was lost"),
        )
      } yield f.lock.lockState.get() shouldBe DbLock.LockState.Lost
    }

    "concurrent acquisition and release" in { f =>
      def acquireRelease(threadId: Int, attempt: Int): Future[Unit] = {

        val flagCloseable: FlagCloseable = FlagCloseable(logger, timeouts)

        val suppressRetryWarnRule =
          LogEntryCriterion(Level.WARN, getClass.getName, """.* operation \'test-.+\'.*""".r)
        val suppressRetryWarnLoggerFactory =
          SuppressingLogger(getClass, skipLogEntry = suppressRetryWarnRule.matches)
        val suppressRetryWarnLogger =
          TracedLogger(suppressRetryWarnLoggerFactory.getLogger(getClass))

        def retryOp(op: () => EitherT[Future, DbLockError, Unit], opName: String): Future[Unit] =
          // Suppress warnings of retries
          suppressRetryWarnLoggerFactory.suppressWarnings {
            retry
              .Pause(
                suppressRetryWarnLogger,
                flagCloseable,
                retry.Forever,
                5.millisecond,
                operationName = opName,
              )
              .apply(op().value, AllExceptionRetryPolicy)
              .map(_ => ())
          }

        // Wait before starting the acquire/release cycle to give other threads a chance to acquire the lock first
        Threading.sleep(10)

        for {
          _ <- retryOp(() => f.lock.acquire(), s"test-acquire-$threadId-$attempt")
          _ <- retryOp(() => f.lock.release(), s"test-release-$threadId-$attempt")
        } yield ()
      }

      def acquireReleaseLoop(threadId: Int, attempts: Int): Future[Unit] =
        if (attempts > 0)
          acquireRelease(threadId, attempts).flatMap(_ =>
            acquireReleaseLoop(threadId, attempts - 1)
          )
        else
          Future.unit

      val f1 = acquireReleaseLoop(0, 10)
      val f2 = acquireReleaseLoop(1, 10)
      val f3 = acquireReleaseLoop(2, 10)

      for {
        _ <- f1
        _ <- f2
        _ <- f3
      } yield succeed
    }

    "allocate a lock id" in { _ =>
      val c1 = UniqueDbLockCounter.get()
      val c2 = UniqueDbLockCounter.get()
      val id1 = allocateLockId(c1)
      val id2 = allocateLockId(c2)
      id1 shouldNot equal(id2)

      val id3 = DbLockId.create("", c1)
      val id4 = DbLockId.create("", c2)
      id3 should not be id4
      val id5 = DbLockId.create("foo", c2)
      id5 should not be id4
    }

    "create lock with allocated lock id" in { f =>
      val id = allocateLockId(UniqueDbLockCounter.get())
      val lock = createLock(id, DbLockMode.Exclusive, f.db, f.clock)
      for {
        _ <- lock.acquire().valueOrFail(s"acquire lock with lock id: $id")
        _ <- lock.release().valueOrFail(s"release lock with lock id: $id")
      } yield succeed
    }

  }

}

class DbLockTestPostgres extends DbLockTest {

  override protected lazy val setup: PostgresDbStorageSetup =
    DbStorageSetup.postgres(loggerFactory)(parallelExecutionContext)

  override def createLock(
      lockId: DbLockId,
      lockMode: DbLockMode,
      db: Database,
      clock: SimClock,
  ): DbLock =
    setup.storage.profile match {
      case profile: DbStorage.Profile.Postgres =>
        new DbLockPostgres(
          profile = profile,
          database = db,
          lockId = lockId,
          mode = lockMode,
          config = lockConfig,
          timeouts = DefaultProcessingTimeouts.testing,
          clock = clock,
          loggerFactory = loggerFactory,
          executorShuttingDown = Eval.now(false),
        )(executorService)
      case _ => fail("Database profile must be a Postgres profile")
    }

  override protected def allocateLockId(counter: DbLockCounter): DbLockId =
    PostgresDbLock
      .allocateLockId(setup.config, counter)(loggerFactory)
      .valueOrFail(s"allocate lock id for counter: $counter")
}
