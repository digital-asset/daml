// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import cats.Monad
import com.digitalasset.canton.concurrent.FutureSupervisor.Impl.ScheduledFuture
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.*
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.concurrent.duration.{DurationInt, DurationLong}
import scala.concurrent.{Future, Promise}
import scala.util.Success

trait FutureSupervisorTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  def futureSupervisor(supervisor: FutureSupervisor): Unit = {

    "support a lot of concurrent supervisions" in {
      // Ensure that there is no quadratic algorithm anywhere in the supervision code
      val count = 1000000

      val supervisedCompleted = (1 to count).toList.map { i =>
        supervisor.supervised(s"test-$i-completed")(Future.successful(i))
      }
      Future.sequence(supervisedCompleted).futureValue

      val promise = Promise[Unit]()
      val supervisedIncomplete = (1 to count).toList.map { i =>
        supervisor.supervised(s"test-$i-incomplete")(promise.future)
      }
      promise.success(())
      Future.sequence(supervisedIncomplete).futureValue
    }

    "supervising a completed promise's future is a no-op" in {
      val promise = new SupervisedPromise[Unit]("to be completed immediately", supervisor)

      promise.trySuccess(())
      promise.future.value shouldBe Some(Success(()))
    }

    "repeated calls to supervised promises are cached" in {
      val promise = new SupervisedPromise[CantonTimestamp]("future is cached", supervisor)

      val fut1 = promise.future
      val fut2 = promise.future

      fut1 shouldBe fut2

      val timestamp = CantonTimestamp.now()
      promise.trySuccess(timestamp)

      val fut3 = promise.future

      fut3 shouldBe fut1
      fut3.value shouldBe Some(Success(timestamp))
    }
  }
}

class NoOpFutureSupervisorTest extends FutureSupervisorTest {
  "NoOpFutureSupervisor" should {
    behave like futureSupervisor(FutureSupervisor.Noop)
  }
}

class FutureSupervisorImplTest extends FutureSupervisorTest {

  import FutureSupervisorImplTest.*

  "FutureSupervisorImpl" should {
    locally {
      val supervisor =
        new FutureSupervisor.Impl(config.NonNegativeDuration.ofSeconds(10), loggerFactory)(
          scheduledExecutor()
        )
      behave like futureSupervisor(supervisor)
      supervisor.stop()
    }

    def complainAboutSlowFuture(supervisor: FutureSupervisor): Unit = {
      val promise = Promise[Unit]()
      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val fut = supervisor.supervised("slow-future")(promise.future)
          eventually() {
            loggerFactory.fetchRecordedLogEntries should not be empty
          }
          promise.success(())
          fut.futureValue
        },
        {
          // If the test is running slow we might get the warning multiple times
          logEntries =>
            logEntries should not be empty
            forAll(logEntries) {
              _.warningMessage should include("slow-future has not completed after")
            }
        },
      )
    }

    "complain about a slow future" in {
      val supervisor =
        new FutureSupervisor.Impl(config.NonNegativeDuration.ofMillis(10), loggerFactory)(
          scheduledExecutor()
        )
      complainAboutSlowFuture(supervisor)
      supervisor.stop()
    }

    "deal with logging exceptions" in {
      val supervisor =
        new FutureSupervisor.Impl(config.NonNegativeDuration.ofMillis(10), loggerFactory)(
          scheduledExecutor()
        )

      val msg = "Description throws"
      val promise1 = Promise[Unit]()
      val ex = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        {
          val fut = supervisor.supervised(throw new Exception(msg))(promise1.future)
          eventually() {
            loggerFactory.fetchRecordedLogEntries should not be empty
          }
          promise1.success(())
          // The description is used for logging the slow completion, so we need to deal with it here.
          fut.failed.futureValue
        },
        forAll(_) { entry =>
          entry.errorMessage should include(
            "Future supervision has failed with an exception, but will repeat"
          )
          entry.throwable.value.getMessage should include(msg)
        },
      )
      ex.getMessage shouldBe msg

      // Make sure that future supervision still works after the exception
      complainAboutSlowFuture(supervisor)
      supervisor.stop()

    }

    "stop supervision if the future is discarded" in {
      def attempt(): Option[FutureSupervisor.Impl] = {
        val supervisor =
          new FutureSupervisor.Impl(config.NonNegativeDuration.ofMillis(10), loggerFactory)(
            scheduledExecutor()
          )

        supervisor
          .supervised("discarded-future") {
            // Do not assign this to a val so that no reference to the future is kept anywhere and it can be GC'ed.
            Promise[Unit]().future
          }
          .discard[Future[Unit]]
        // Normally, the future should still be scheduled. However, it is possible that the GC has already run
        // and so there's nothing to do to simulate. Or that the supervisor is already running and we can't get hold
        // of the future any more because it has been pushed down in the linked list of supervised futures.
        // In that case, we create a new future supervisor and start afresh.
        supervisor.inspectHead match {
          case Some(scheduled: ScheduledFuture) =>
            logger.debug("Simulating garbage collection on the weak reference.")
            scheduled.fut.underlying.clear()
            Some(supervisor)
          case _ =>
            // the supervisor is already running and has inserted a sentinel at the head
            // since we must not search through the linked list for synchronization reasons,
            // let's try again with another future supervisor
            supervisor.stop()
            None
        }
      }

      @tailrec def repeat(remainingAttempts: Int): Unit =
        if (remainingAttempts > 0) {
          attempt() match {
            case Some(supervisor) =>
              // Wait until the schedule is removed
              eventually() {
                supervisor.inspectApproximateSize shouldBe 0
              }
              supervisor.stop()
            case None => repeat(remainingAttempts - 1)
          }
        } else
          cancel("Failed to simulate garbage collection before the supervisor ran")

      loggerFactory.assertLoggedWarningsAndErrorsSeq(
        repeat(10),
        // We don't know how long it takes for the GC simulation to run. So there may be any number of warnings about the slow future.
        forAll(_) {
          _.warningMessage should include("discarded-future has not completed after")
        },
      )
    }

    "scale for many supervised futures" in {
      // Another (stricter) test ensuring that there is no quadratic algorithm anywhere in the supervision code
      val supervisor =
        new FutureSupervisor.Impl(
          config.NonNegativeDuration.ofSeconds(ScaleTestFutureTimeout.toSeconds),
          loggerFactory,
        )(scheduledExecutor())
      val promise = Promise[Unit]()
      val startTime = System.nanoTime

      val supervisedFutures = (1 to ScaleTestTotalNumberOfFutures).toList.map { i =>
        supervisor.supervised(s"test-$i")(promise.future)
      }
      supervisor.inspectApproximateSize shouldBe ScaleTestTotalNumberOfFutures
      promise.success(())
      Future.sequence(supervisedFutures).futureValue

      val totalTime = (System.nanoTime - startTime).nanos
      if (totalTime > ScaleTestTotalTimeout) {
        fail(
          s"Sequenced future took longer (${totalTime.toMillis} milliseconds) to complete than " +
            s"the total scale test timeout of $ScaleTestTotalTimeout"
        )
      }

      eventually() {
        supervisor.inspectApproximateSize shouldBe 0
      }
      supervisor.stop()
    }

    "supervision is not starved" in {
      // A rather generous limit of 1s to avoid flaky warnings due to GC interference.
      // This test is about starving the supervision, so the warnings are not the main point.
      // Yet, we do not completely disable them because warnings indicate a long-running
      // future supervision scheduling problem.
      val warnLimit = config.NonNegativeDuration.ofSeconds(1)
      val supervisor = new FutureSupervisor.Impl(warnLimit, loggerFactory)(scheduledExecutor())

      val baseLoad = 100000

      val incompleteFuture = Promise[Unit]()
      val baseFutures = (1 to baseLoad).toList.map { i =>
        supervisor.supervised(
          s"base-load-$i",
          // Don't warn for the base futures. They're just there to keep the supervision busy
          warnAfter = 1.day,
        )(incompleteFuture.future)
      }

      // Now we add a constant stream of future supervisions that all complete rather quickly.
      // We expect that the future supervisor can keep up with this and remove them.

      val delayBetweenSchedulesNanos = 10000

      def addSupervisionFuture(i: Int): Future[Unit] = Future {
        val promise = Promise[Unit]()
        val fut = supervisor.supervised(s"fast-load-$i")(promise.future)
        Threading.sleep(millis = 0, nanos = delayBetweenSchedulesNanos)
        promise.success(())
        fut
      }.flatten

      val stopLoad = new AtomicBoolean()

      val loadF = Monad[Future].tailRecM(0)(i =>
        if (stopLoad.get()) Future.successful(Right(()))
        else addSupervisionFuture(i).map(_ => Left(i + 1))
      )

      val scheduledSizeSamplesB = List.newBuilder[Int]
      val expectedGcs = 10
      val samplesPerCheckDelay = 5
      val samples = samplesPerCheckDelay * expectedGcs
      val sampleDelay = FutureSupervisor.Impl.defaultCheckMs / samplesPerCheckDelay
      for (i <- 1 to samples) {
        val size = supervisor.inspectApproximateSize
        scheduledSizeSamplesB += size
        Threading.sleep(sampleDelay)
      }

      // If the scheduler runs frequently, we should see the scheduled tasks not grow significantly beyong what's added during one waiting period

      stopLoad.set(true)
      loadF.futureValue

      incompleteFuture.success(())
      Future.sequence(baseFutures).futureValue

      val scheduledSizeSamples = scheduledSizeSamplesB.result()

      // find drops in sample size
      val successfulGCs = scheduledSizeSamples.zip(scheduledSizeSamples.drop(1)).count {
        case (prev, next) => next < prev
      }

      withClue(s"samples: $scheduledSizeSamples") {
        successfulGCs shouldBe >=(expectedGcs / 2)
      }
      supervisor.stop()
    }
  }
}

object FutureSupervisorImplTest {
  private val ScaleTestTotalNumberOfFutures = 100000
  private val ScaleTestFutureTimeout = 1.second
  // Use a bigger timeout for completing the entire scale test
  private val ScaleTestTotalTimeout = 5.seconds
}
