// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.{DbLockedConnectionConfig, ProcessingTimeout}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyInstances}
import com.digitalasset.canton.resource.WithDbLock.{WithDbLockError, withDbLock}
import com.digitalasset.canton.store.db.{DbTest, PostgresTest}
import com.digitalasset.canton.time.WallClock
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, FutureUtil}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpec

import java.time.Duration.ofMillis
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{Future, Promise}

trait WithDbLockTest extends FixtureAsyncWordSpec with BaseTest with HasExecutionContext {
  this: DbTest =>
  import WithDbLockTest.*

  override def cleanDb(storage: DbStorage)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    // we're not actually storing anything so there's nothing to clean
    FutureUnlessShutdown.unit

  implicit val prettyString: Pretty[String] = PrettyInstances.prettyString

  class Env extends AutoCloseable {
    val processingTimeout = ProcessingTimeout()
    val clock = new WallClock(processingTimeout, loggerFactory)

    def withLock[A: Pretty, B](lockCounter: DbLockCounter = DefaultLockCounter)(
        fn: => EitherT[Future, A, B]
    ): EitherT[Future, WithDbLockError, B] =
      withDbLock(
        s"${getClass.getSimpleName}:$lockCounter",
        lockCounter,
        ProcessingTimeout(),
        storage.dbConfig,
        DbLockedConnectionConfig(),
        storage.profile,
        futureSupervisor,
        clock,
        loggerFactory,
        logLockOwnersOnLockAcquisitionAttempt = false,
      )(fn.mapK(FutureUnlessShutdown.outcomeK)).onShutdown(fail("shutdown"))

    override def close(): Unit = clock.close()
  }

  override type FixtureParam = Env

  override def withFixture(fixture: OneArgAsyncTest): FutureOutcome = {
    val env = new Env()

    complete {
      withFixture(fixture.toNoArgAsyncTest(env))
    } lastly env.close()
  }

  "prevents concurrent runs" in { env =>
    import env.*
    val latch = new AtomicInteger(0)
    def run(): EitherT[Future, String, Unit] = {
      if (!latch.compareAndSet(0, 1))
        fail("Latch was in incorrect state at start of call")
      val promise = Promise[Unit]()

      // just delay long enough so un-synchronized calls would likely be noticed
      FutureUtil.doNotAwait(
        clock.scheduleAfter(_ => promise.success(()), ofMillis(50)).failOnShutdown("delay"),
        "delay",
      )

      for {
        _ <- EitherT.right[String](promise.future)
      } yield {
        if (!latch.compareAndSet(1, 0)) {
          fail("Latch was in incorrect state at end of call")
        }
      }
    }

    for {
      // run a handful of functions that would blow up if not synchronized with a db lock
      result <- (0 to 3).toList.parTraverse(_ => env.withLock()(run())).value.map(_ => ())
    } yield result shouldBe ()
  }

  "when block fails" should {
    "unlock when an error is returned" in { env =>
      import env.*
      for {
        error <- withLock()(EitherT.leftT[Future, Unit]("BOOM")).value
        // if the above doesn't unlock this block won't run
        success <- withLock()(EitherTUtil.unit[String]).value
      } yield {
        error shouldBe Left(WithDbLockError.OperationError("BOOM"))
        success shouldBe Either.unit
      }
    }

    "unlock when an exception is thrown" in { env =>
      import env.*
      val ex = new RuntimeException("BOOM")
      for {
        returnedException <- withLock()(EitherT.right[String](Future.failed[Unit](ex))).value.failed
        // if the above doesn't unlock this block won't run
        success <- withLock()(EitherTUtil.unit[String]).value
      } yield {
        returnedException shouldBe ex
        success shouldBe Either.unit
      }
    }
  }

  "distinct locks can be taken at once" in { env =>
    import env.*
    val l1P = Promise[Unit]()
    val l2P = Promise[Unit]()

    // take two locks with separate counters and check they don't deadlock
    val l1F = withLock(lockCounter = DefaultLockCounter) {
      l1P.success(())
      EitherT.right[String](l2P.future)
    }
    val l2F = withLock(lockCounter = AnotherLockCounter) {
      l2P.success(())
      EitherT.right[String](l1P.future)
    }

    for {
      _ <- List(l1F, l2F).parSequence.value
    } yield succeed
  }

}

object WithDbLockTest {
  val DefaultLockCounter = UniqueDbLockCounter.get()
  val AnotherLockCounter = UniqueDbLockCounter.get()
}

class WithDbLockTestPostgres extends WithDbLockTest with PostgresTest
