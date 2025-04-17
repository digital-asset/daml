// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import cats.Eval
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, Threading}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.logging.{SuppressionRule, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.ErrorKind.TransientErrorKind
import com.digitalasset.canton.util.retry.Jitter.RandomSource
import com.digitalasset.canton.util.{DelayUtil, FutureUtil, retry}
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import org.scalatest.Assertion
import org.scalatest.funspec.AsyncFunSpec
import org.slf4j.event.Level

import java.util.Random
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success as TrySuccess}

class PolicyTest extends AsyncFunSpec with BaseTest with HasExecutorService {

  val random = new Random()
  val randomSource: RandomSource = Jitter.randomSource(random)

  val flagCloseable: FlagCloseable = FlagCloseable(logger, DefaultProcessingTimeouts.testing)

  def forwardCountingFutureStream(value: Int = 0): LazyList[Future[Int]] =
    Future(value) #:: forwardCountingFutureStream(value + 1)

  def backwardCountingFutureStream(value: Int): LazyList[Future[Int]] =
    if (value < 0) LazyList.empty
    else Future(value) #:: backwardCountingFutureStream(value - 1)

  def time[T](f: => T): Duration = {
    val before = System.currentTimeMillis
    f
    Duration(System.currentTimeMillis - before, MILLISECONDS)
  }

  describe("retry.Directly") {

    testUnexpectedException(Directly(logger, flagCloseable, 10, "directly-unexpected-exception-op"))

    it("should retry a future for a specified number of times") {
      implicit val success: Success[Int] = Success(_ == 3)
      val tries = forwardCountingFutureStream().iterator
      Directly(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 3,
        operationName = "directly-retry-specified-times-op",
      )(
        tries.next(),
        AllExceptionRetryPolicy,
      ).map(result => assert(success.predicate(result) === true))
    }

    it("should fail when expected") {
      val success = implicitly[Success[Option[Int]]]
      val tries = Future(None: Option[Int])
      Directly(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 2,
        operationName = "directly-fail-when-expected-op",
      )(
        tries,
        AllExceptionRetryPolicy,
      ).map(result => assert(success.predicate(result) === false))
    }

    it("should deal with future failures") {
      implicit val success: Success[Any] = Success.always
      val policy = Directly(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 3,
        operationName = "directly-future-failures-op",
      )
      val counter = new AtomicInteger(0)
      val future = policy(
        {
          counter.incrementAndGet()
          Future.failed(new RuntimeException("always failing"))
        },
        AllExceptionRetryPolicy,
      )
      // expect failure after 1+3 tries
      future.failed.map { t =>
        assert(counter.get() === 4 && t.getMessage === "always failing")
      }
    }

    testSynchronousException(
      Directly(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 3,
        operationName = "directly-sync-exception-op",
      ),
      3,
    )

    it("should accept a future in reduced syntax format") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger()
      val future = Directly(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 1,
        operationName = "directly-future-reduced-syntax-format-op",
      )(
        {
          counter.incrementAndGet()
          Future.failed(new RuntimeException("always failing"))
        },
        AllExceptionRetryPolicy,
      )
      future.failed.map(t => assert(counter.get() === 2 && t.getMessage === "always failing"))
    }

    it("should retry futures passed by-name instead of caching results") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger()
      val future = Directly(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 1,
        operationName = "directly-retry-futures-passed-op",
      )(
        counter.getAndIncrement() match {
          case 1 => Future.successful("yay!")
          case _ => Future.failed(new RuntimeException("failed"))
        },
        AllExceptionRetryPolicy,
      )
      future.map(result => assert(counter.get() === 2 && result === "yay!"))
    }

    it("should repeat on not expected value until success") {
      implicit val success: Success[Boolean] = Success(identity)
      val retried = new AtomicInteger()
      val retriedUntilSuccess = 10

      def run() =
        if (retried.get() < retriedUntilSuccess) {
          retried.incrementAndGet()
          Future(false)
        } else {
          Future(true)
        }

      val policy =
        Directly(
          logger,
          performUnlessClosing = flagCloseable,
          maxRetries = Forever,
          operationName = "directly-repeat-on-non-expected-value-op",
        )
      policy(run(), AllExceptionRetryPolicy).map { result =>
        assert(result === true)
        assert(retried.get() == retriedUntilSuccess)
      }
    }

    testStopOnClosing(
      Directly(
        logger,
        _,
        maxRetries = Forever,
        operationName = "directly-stop-on-closing-op",
        retryLogLevel = Some(Level.INFO),
      ),
      retriedUntilClose = 10,
    )

    testClosedExecutionContext(
      Directly(
        logger,
        _,
        maxRetries = Forever,
        operationName = "directly-closed-ex-context-op",
        retryLogLevel = Some(Level.INFO),
      )
    )

    testStopOnShutdown(
      Directly(logger, _, maxRetries = Forever, operationName = "directly-stop-on-shutdown-op"),
      retriedUntilShutdown = 10,
    )

    testSuspend(maxRetries =>
      suspend =>
        Directly(
          logger,
          performUnlessClosing = flagCloseable,
          maxRetries = maxRetries,
          operationName = "directly-suspend-op",
          suspendRetries = suspend,
        )
    )

    testExceptionLogging(
      Directly(
        logger,
        flagCloseable,
        maxRetries = 3,
        operationName = "directly-exception-logging-op",
      )
    )

    testExceptionLoggingRetryForever(
      Directly(
        logger,
        flagCloseable,
        maxRetries = Forever,
        operationName = "directly-exception-logging-forever-op",
      )
    )
  }

  describe("retry.Pause") {

    testUnexpectedException(
      Pause(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 10,
        delay = 30.millis,
        operationName = "pause-unexpected-exception-op",
      )
    )

    it("should pause in between retries") {
      implicit val success: Success[Int] = Success(_ == 3)
      val tries = forwardCountingFutureStream().iterator
      val policy = Pause(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 3,
        delay = 30.millis,
        operationName = "pause-between-retries-op",
      )
      val marker_base = System.currentTimeMillis
      val marker = new AtomicLong(0)

      val runF = policy(
        {
          marker.set(System.currentTimeMillis); tries.next()
        },
        AllExceptionRetryPolicy,
      )
      runF.map { result =>
        val delta = marker.get() - marker_base
        assert(
          success.predicate(result) &&
            delta >= 90 && delta <= 1000
        ) // was 110, depends on how hot runtime is
      }
    }

    it("should repeat on unexpected value with pause until success") {
      implicit val success: Success[Boolean] = Success(identity)
      val retried = new AtomicInteger()
      val retriedUntilSuccess = 10

      def run() =
        if (retried.get() < retriedUntilSuccess) {
          retried.incrementAndGet()
          Future(false)
        } else {
          Future(true)
        }

      val policy = Pause(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = Forever,
        delay = 1.millis,
        operationName = "pause-repeat-on-unexpected-until-success-op",
      )
      policy(run(), AllExceptionRetryPolicy).map { result =>
        assert(result === true)
        assert(retried.get() == retriedUntilSuccess)
      }
    }

    testSynchronousException(
      Pause(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 1,
        delay = 1.millis,
        operationName = "pause-sync-exception-op",
      ),
      1,
    )

    testStopOnClosing(
      Pause(
        logger,
        _,
        maxRetries = Forever,
        delay = 50.millis,
        operationName = "pause-stop-on-closing-op",
        retryLogLevel = Some(Level.INFO),
      ),
      retriedUntilClose = 3,
    )

    testClosedExecutionContext(
      Pause(
        logger,
        _,
        maxRetries = Forever,
        delay = 10.millis,
        operationName = "pause-closed-ex-context-op",
        retryLogLevel = Some(Level.INFO),
      )
    )

    testStopOnShutdown(
      Pause(
        logger,
        _,
        maxRetries = Forever,
        delay = 1.millis,
        operationName = "pause-stop-on-shutdown-op",
      ),
      retriedUntilShutdown = 10,
    )

    testSuspend(maxRetries =>
      suspend =>
        Pause(
          logger,
          performUnlessClosing = flagCloseable,
          maxRetries = maxRetries,
          delay = 5.millis,
          operationName = "pause-suspend-op",
          suspendRetries = suspend,
        )
    )

    testExceptionLogging(
      Pause(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = 3,
        delay = 1.millis,
        operationName = "pause-exception-logging-op",
      )
    )

    testExceptionLoggingRetryForever(
      Pause(
        logger,
        performUnlessClosing = flagCloseable,
        maxRetries = retry.Forever,
        delay = 1.millis,
        operationName = "pause-exception-logging-forever-op",
      )
    )
  }

  describe("retry.Backoff") {

    implicit val jitter: Jitter = Jitter.none(1.minute)

    testUnexpectedException(
      Backoff(
        logger,
        flagCloseable = flagCloseable,
        maxRetries = 10,
        initialDelay = 30.millis,
        maxDelay = Duration.Inf,
        operationName = "backoff-unexpected-exception-op",
      )
    )

    it("should pause with multiplier between retries") {
      implicit val success: Success[Int] = Success(_ == 2)
      val tries = forwardCountingFutureStream().iterator
      val policy = Backoff(
        logger,
        flagCloseable = flagCloseable,
        maxRetries = 2,
        initialDelay = 30.millis,
        maxDelay = Duration.Inf,
        operationName = "backoff-pause-multiply-between-retries-op",
      )
      val marker_base = System.currentTimeMillis
      val marker = new AtomicLong(0)
      val runF = policy(
        {
          marker.set(System.currentTimeMillis); tries.next()
        },
        AllExceptionRetryPolicy,
      )
      runF.map { result =>
        val delta = marker.get() - marker_base
        assert(
          success.predicate(result) === true &&
            delta >= 90 && delta <= 1000 // was 110
        )
      }
    }

    it("should repeat on unexpected value with backoff until success") {
      implicit val success: Success[Boolean] = Success(identity)
      val retried = new AtomicInteger()
      val retriedUntilSuccess = 5

      def run() =
        if (retried.get() < retriedUntilSuccess) {
          retried.incrementAndGet()
          Future(false)
        } else {
          Future(true)
        }

      val policy = Backoff(
        logger,
        flagCloseable = flagCloseable,
        maxRetries = Forever,
        initialDelay = 1.millis,
        maxDelay = Duration.Inf,
        operationName = "backoff-repeat-on-unexpected-value-op",
      )
      policy(run(), AllExceptionRetryPolicy).map { result =>
        assert(result === true)
        assert(retried.get() == 5)
      }
    }

    testSynchronousException(
      Backoff(
        logger,
        flagCloseable = flagCloseable,
        maxRetries = 1,
        initialDelay = 1.millis,
        maxDelay = Duration.Inf,
        operationName = "backoff-sync-exception-op",
      ),
      1,
    )

    testStopOnClosing(
      Backoff(
        logger,
        _,
        maxRetries = Forever,
        initialDelay = 10.millis,
        maxDelay = Duration.Inf,
        operationName = "backoff-stop-on-closing-op",
        retryLogLevel = Some(Level.INFO),
      ),
      retriedUntilClose = 3,
    )

    testClosedExecutionContext(
      Backoff(
        logger,
        _,
        maxRetries = Forever,
        initialDelay = 10.millis,
        maxDelay = Duration.Inf,
        operationName = "backoff-closed-ex-context-op",
        retryLogLevel = Some(Level.INFO),
      )
    )

    testStopOnShutdown(
      Backoff(
        logger,
        _,
        maxRetries = 10,
        initialDelay = 1.millis,
        maxDelay = Duration.Inf,
        operationName = "backoff-stop-on-shutdown-op",
      ),
      retriedUntilShutdown = 3,
    )

    testSuspend(maxRetries =>
      suspend =>
        Backoff(
          logger,
          flagCloseable = flagCloseable,
          maxRetries = maxRetries,
          initialDelay = 5.milli,
          maxDelay = Duration.Inf,
          operationName = "backoff-suspend-op",
          suspendRetries = suspend,
        )
    )

    testExceptionLogging(
      Backoff(
        logger,
        flagCloseable = flagCloseable,
        maxRetries = 3,
        initialDelay = 1.millis,
        maxDelay = 1.millis,
        operationName = "backoff-exception-logging-op",
      )
    )

    testExceptionLoggingRetryForever(
      Backoff(
        logger,
        flagCloseable = flagCloseable,
        maxRetries = retry.Forever,
        initialDelay = 1.millis,
        maxDelay = 1.millis,
        operationName = "backoff-exception-logging-forever-op",
      )(Jitter.none(1.millis))
    )
  }

  trait AlgoCreator {
    def apply(cap: FiniteDuration): Jitter
  }

  def testJitterBackoff(name: String, algoCreator: AlgoCreator): Unit = {
    describe(s"retry.JitterBackoff.$name") {

      testUnexpectedException(
        Backoff(
          logger,
          flagCloseable = flagCloseable,
          maxRetries = 10,
          initialDelay = 30.millis,
          maxDelay = Duration.Inf,
          operationName = "backoff-unexpected-exception-op",
        )(algoCreator(cap = 10.millis))
      )

      it("should retry a future for a specified number of times") {
        implicit val success: Success[Int] = Success(_ == 3)
        implicit val algo: Jitter = algoCreator(cap = 10.millis)
        val tries = forwardCountingFutureStream().iterator
        val policy = Backoff(
          logger,
          flagCloseable = flagCloseable,
          maxRetries = 3,
          initialDelay = 1.milli,
          maxDelay = Duration.Inf,
          operationName = "backoff-retry-specified-num-of-times-op",
        )
        policy(tries.next(), AllExceptionRetryPolicy).map(result =>
          assert(success.predicate(result) === true)
        )
      }

      it("should fail when expected") {
        implicit val algo: Jitter = algoCreator(cap = 10.millis)
        val success = implicitly[Success[Option[Int]]]
        val tries = Future(None: Option[Int])
        val policy =
          Backoff(
            logger,
            flagCloseable = flagCloseable,
            maxRetries = 3,
            initialDelay = 1.milli,
            maxDelay = Duration.Inf,
            operationName = "backoff-fail-when-expected-op",
          )
        policy(tries, AllExceptionRetryPolicy).map(result =>
          assert(success.predicate(result) === false)
        )
      }

      it("should deal with future failures") {
        implicit val success: Success[Any] = Success.always
        implicit val algo: Jitter = algoCreator(cap = 10.millis)
        val policy =
          Backoff(
            logger,
            flagCloseable = flagCloseable,
            maxRetries = 3,
            initialDelay = 5.millis,
            maxDelay = Duration.Inf,
            operationName = "backoff-future-failures-op",
          )
        val counter = new AtomicInteger()
        val future = policy(
          {
            counter.incrementAndGet()
            Future.failed(new RuntimeException("always failing"))
          },
          AllExceptionRetryPolicy,
        )
        future.failed.map(t => assert(counter.get() === 4 && t.getMessage === "always failing"))
      }

      it("should retry futures passed by-name instead of caching results") {
        implicit val success: Success[Any] = Success.always
        implicit val algo: Jitter = algoCreator(cap = 10.millis)
        val counter = new AtomicInteger()
        val policy =
          Backoff(
            logger,
            flagCloseable = flagCloseable,
            maxRetries = 1,
            initialDelay = 1.milli,
            maxDelay = Duration.Inf,
            operationName = "backoff-try-futures-passed-op",
          )
        val future = policy(
          counter.getAndIncrement() match {
            case 1 => Future.successful("yay!")
            case _ => Future.failed(new RuntimeException("failed"))
          },
          AllExceptionRetryPolicy,
        )
        future.map(result => assert(counter.get() == 2 && result === "yay!"))
      }

      it("should pause with multiplier and jitter between retries") {
        implicit val success: Success[Int] = Success(_ == 2)
        implicit val algo: Jitter = algoCreator(cap = 1000.millis)
        val tries = forwardCountingFutureStream().iterator
        val policy = Backoff(
          logger,
          flagCloseable = flagCloseable,
          maxRetries = 5,
          initialDelay = 50.millis,
          maxDelay = Duration.Inf,
          operationName = "backoff-pause-with-mult-and-jutter-op",
        )
        val marker_base = System.currentTimeMillis
        val marker = new AtomicLong(0)

        policy(
          {
            marker.set(System.currentTimeMillis); tries.next()
          },
          AllExceptionRetryPolicy,
        ).map { result =>
          val delta = marker.get() - marker_base
          assert(
            success.predicate(result) === true &&
              delta >= 0 && delta <= 2000
          )
        }
      }

      it("should also work when invoked as forever") {
        implicit val success: Success[Int] = Success(_ == 5)
        implicit val algo: Jitter = algoCreator(cap = 50.millis)
        val tries = forwardCountingFutureStream().iterator
        val policy =
          Backoff(
            logger,
            flagCloseable = flagCloseable,
            maxRetries = Forever,
            initialDelay = 10.millis,
            maxDelay = Duration.Inf,
            operationName = "backoff-as-forever-op",
          )
        val marker = new AtomicLong(0)
        val marker_base = System.currentTimeMillis

        policy(
          {
            marker.set(System.currentTimeMillis); tries.next()
          },
          AllExceptionRetryPolicy,
        ).map { result =>
          val delta =
            marker.get() - marker_base // The actual delay is async task scheduling-sensitive
          assert(
            success.predicate(result) === true &&
              delta >= 0 && delta <= 2_000
          )
        }
      }

      it("should repeat on unexpected value with jitter backoff until success") {
        implicit val success: Success[Boolean] = Success(identity)
        implicit val algo: Jitter = algoCreator(cap = 10.millis)
        val retried = new AtomicInteger()
        val retriedUntilSuccess = 10

        def run() =
          if (retried.get() < retriedUntilSuccess) {
            retried.incrementAndGet()
            Future(false)
          } else {
            Future(true)
          }

        val policy = Backoff(
          logger,
          flagCloseable = flagCloseable,
          maxRetries = Forever,
          initialDelay = 1.millis,
          maxDelay = Duration.Inf,
          operationName = "backoff-repeat-with-jitter-until-success-op",
        )
        policy(run(), AllExceptionRetryPolicy).map { result =>
          assert(result === true)
          assert(retried.get() == retriedUntilSuccess)
        }
      }
    }
  }

  testJitterBackoff("none", t => Jitter.none(cap = t))
  testJitterBackoff("full", t => Jitter.full(cap = t, random = randomSource))
  testJitterBackoff("equal", t => Jitter.equal(cap = t, random = randomSource))
  testJitterBackoff("decorrelated", t => Jitter.decorrelated(cap = t, random = randomSource))

  describe("retry.When") {

    testUnexpectedException(
      When(
        logger,
        { case _ =>
          Pause(
            logger,
            performUnlessClosing = flagCloseable,
            maxRetries = 10,
            delay = 30.millis,
            operationName = "when-unexpected-exception-op",
          )
        },
      )
    )

    it("should retry conditionally when a condition is met") {
      implicit val success: Success[Int] = Success(_ == 2)
      val tries = forwardCountingFutureStream().iterator
      val policy = When(
        logger,
        {
          // this is very contrived but should serve as an example
          // of matching then dispatching a retry depending on
          // the value of the future when completed
          case 0 =>
            When(
              logger,
              { case 1 =>
                Pause(
                  logger,
                  performUnlessClosing = flagCloseable,
                  maxRetries = 4,
                  delay = 2.seconds,
                  operationName = "when-retry-cond-op",
                )
              },
            )
        },
      )
      val future = policy(tries.next(), AllExceptionRetryPolicy)
      future.map(result => assert(success.predicate(result) === true))
    }

    it("should retry but only when condition is met") {
      implicit val success: Success[Int] = Success(_ == 2)
      val tries = forwardCountingFutureStream().iterator
      val policy = When(
        logger,
        {
          // this cond will never be met because
          // a cond for n == 0 is not defined
          case 1 =>
            Directly(
              logger,
              performUnlessClosing = flagCloseable,
              maxRetries = 3,
              operationName = "when-retry-only-when-cond-met-op",
            )
        },
      )

      val future = policy(tries.next(), AllExceptionRetryPolicy)
      future.map(result => assert(success.predicate(result) === false))
    }

    it("should handle future failures") {
      implicit val success: Success[Boolean] = Success(identity)
      final case class RetryAfter(duration: FiniteDuration) extends RuntimeException
      val retried = new AtomicBoolean

      def run() =
        if (retried.get()) Future(true)
        else {
          retried.set(true)
          Future.failed(RetryAfter(1.second))
        }

      val policy = When(
        logger,
        {
          // lift an exception into a new policy
          case RetryAfter(duration) =>
            Pause(
              logger,
              performUnlessClosing = flagCloseable,
              maxRetries = 4,
              delay = duration,
              operationName = "when-handle-future-failures-op",
            )
        },
      )
      policy(run(), AllExceptionRetryPolicy).map(result => assert(result === true))
    }

    it("should handle synchronous failures") {
      implicit val success: Success[Boolean] = Success(identity)
      final case class RetryAfter(duration: FiniteDuration) extends RuntimeException
      val retried = new AtomicBoolean

      def run() =
        if (retried.get()) Future(true)
        else {
          retried.set(true)
          throw RetryAfter(1.second)
        }

      val policy = When(
        logger,
        {
          // lift an exception into a new policy
          case RetryAfter(duration) =>
            Pause(
              logger,
              performUnlessClosing = flagCloseable,
              maxRetries = 4,
              delay = duration,
              operationName = "when-sync-failures-op",
            )
        },
      )
      policy(run(), AllExceptionRetryPolicy).map(result => assert(result === true))
    }

    it("should repeat on failure until success") {
      implicit val success: Success[Boolean] = Success(identity)
      class MyException extends RuntimeException
      val retried = new AtomicInteger()
      val retriedUntilSuccess = 10

      def run() =
        if (retried.get() < retriedUntilSuccess) {
          retried.incrementAndGet()
          Future.failed(new MyException)
        } else {
          Future(true)
        }

      val policy = When(
        logger,
        {
          // lift an exception into a new policy
          case _: MyException =>
            Directly(
              logger,
              performUnlessClosing = flagCloseable,
              maxRetries = Forever,
              operationName = "when-repeat-fail-until-success-op",
            )
        },
      )
      policy(run(), AllExceptionRetryPolicy).map { result =>
        assert(result === true)
        assert(retried.get() == retriedUntilSuccess)
      }
    }

    it("should repeat on failure with pause until success") {
      implicit val success: Success[Boolean] = Success(identity)
      class MyException extends RuntimeException
      val retried = new AtomicInteger()
      val retriedUntilSuccess = 10

      def run() =
        if (retried.get() < retriedUntilSuccess) {
          retried.incrementAndGet()
          Future.failed(new MyException)
        } else {
          Future(true)
        }

      val policy = When(
        logger,
        {
          // lift an exception into a new policy
          case _: MyException =>
            Pause(
              logger,
              performUnlessClosing = flagCloseable,
              maxRetries = Forever,
              delay = 1.millis,
              operationName = "when-repeat-with-pause-op",
            )
        },
      )
      policy(run(), AllExceptionRetryPolicy).map { result =>
        assert(result === true)
        assert(retried.get() == retriedUntilSuccess)
      }
    }

    it("should repeat on failure with backoff until success") {
      implicit val success: Success[Boolean] = Success[Boolean](identity)
      implicit val jitter: Jitter = Jitter.none(1.minute)
      class MyException extends RuntimeException
      val retried = new AtomicInteger()
      val retriedUntilSuccess = 5

      def run() =
        if (retried.get() < retriedUntilSuccess) {
          retried.incrementAndGet()
          Future.failed(new MyException)
        } else {
          Future(true)
        }

      val policy = When(
        logger,
        {
          // lift an exception into a new policy
          case _: MyException =>
            Backoff(
              logger,
              flagCloseable = flagCloseable,
              maxRetries = Forever,
              initialDelay = 1.millis,
              maxDelay = Duration.Inf,
              operationName = "when-backoff-repeat-op",
            )
        },
      )
      policy(run(), AllExceptionRetryPolicy).map { result =>
        assert(result === true)
        assert(retried.get() == 5)
      }
    }

    it("should repeat on failure with jitter backoff until success") {
      implicit val success: Success[Boolean] = Success(identity)
      class MyException extends RuntimeException
      val retried = new AtomicInteger()
      val retriedUntilSuccess = 10

      def run() =
        if (retried.get() < retriedUntilSuccess) {
          retried.incrementAndGet()
          Future.failed(new MyException)
        } else {
          Future(true)
        }

      val policy = When(
        logger,
        {
          // lift an exception into a new policy
          case _: MyException =>
            Backoff(
              logger,
              flagCloseable,
              maxRetries = Forever,
              initialDelay = 1.millis,
              maxDelay = Duration.Inf,
              operationName = "when-backoff-jitter-op",
            )
        },
      )
      policy(run(), AllExceptionRetryPolicy).map { result =>
        assert(result === true)
        assert(retried.get() == 10)
      }
    }

    testStopOnShutdown(
      _ =>
        When(
          logger,
          _ =>
            Directly(logger, flagCloseable, maxRetries = Forever, operationName = "should-not-run"),
        ),
      retriedUntilShutdown = 1,
    )
  }

  def testUnexpectedException(policy: Policy): Unit =
    it("should not retry after an exception that isn't retryable") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger()
      val future = policy(
        {
          counter.incrementAndGet()
          Future.failed(new RuntimeException(s"unexpected problem"))
        },
        NoExceptionRetryPolicy,
      )
      future.failed.map(t => assert(counter.get() === 1 && t.getMessage === "unexpected problem"))
    }

  def testSynchronousException(policy: Policy, maxRetries: Int): Unit =
    it("should convert a synchronous exception into an asynchronous one") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger(0)
      val future = policy.apply[Future, Unit](
        {
          counter.incrementAndGet()
          throw new RuntimeException("always failing")
        },
        AllExceptionRetryPolicy,
      )
      // expect failure after 1+maxRetries tries
      future.failed.map { t =>
        assert(counter.get() === maxRetries + 1 && t.getMessage === "always failing")
      }
    }

  def testStopOnClosing(policy: PerformUnlessClosing => Policy, retriedUntilClose: Int): Unit = {
    it("should repeat until closed from within") {
      implicit val success: Success[Int] = Success.never

      val closeable = FlagCloseable(logger, DefaultProcessingTimeouts.testing)
      val retried = new AtomicInteger()

      def run(): Future[Int] = {
        val incr = retried.incrementAndGet()
        if (incr == retriedUntilClose) {
          // Do not directly call `close` because this will deadlock
          FutureUtil.doNotAwait(
            Future(closeable.close())(executorService),
            "Closing the FlagCloseable of the retry",
          )
          eventually() {
            closeable.isClosing shouldBe true
          }
        }
        Future.successful(incr)
      }

      val result = policy(closeable)(run(), AllExceptionRetryPolicy)(
        success,
        executorService,
        traceContext,
        implicitly,
      ).futureValue

      assert(result == retriedUntilClose, "Expected to get last result as result.")
      assert(
        retried.get() == retriedUntilClose,
        s"Expected to increment $retriedUntilClose times before failure",
      )
    }

    it("should repeat until closed from outside") {
      val closeable = FlagCloseable(logger, DefaultProcessingTimeouts.testing)
      val retried = new AtomicInteger()

      def run(): Future[Int] = Future.successful {
        val num = retried.incrementAndGet()
        logger.debug(s"Increment retried is $num, closeable is ${closeable.isClosing}")
        num
      }

      val retryF = {
        implicit val executionContext: ExecutionContext = executorService
        policy(closeable)(run(), AllExceptionRetryPolicy)(
          Success.never,
          executorService,
          traceContext,
          implicitly,
        )
          .thereafter { count =>
            logger.debug(s"Stopped retry after $count")
          }
      }

      logger.debug("Wrapping")
      // Wrap the retry in a performUnlessClosing to trigger possible deadlocks.
      val retryUnlessClosingF =
        closeable.performUnlessClosingF("test-retry")(retryF)(executorService, traceContext)

      Threading.sleep(10)
      closeable.close()

      inside(Await.result(retryUnlessClosingF.unwrap, 100.millis)) {
        case UnlessShutdown.Outcome(_) => succeed
        case UnlessShutdown.AbortedDueToShutdown => fail("Unexpected shutdown.")
      }
    }
  }

  def testClosedExecutionContext(policy: PerformUnlessClosing => Policy): Unit =
    it("should handle a closed execution context after closing") {
      val closeable = FlagCloseable(logger, DefaultProcessingTimeouts.testing)

      val closeableEc = Threading.newExecutionContext(
        executionContextName,
        noTracingLogger,
        Threading.detectNumberOfThreads(noTracingLogger),
        exitOnFatal = exitOnFatal,
      )

      val retried = new AtomicInteger()
      def run(): Future[Int] = Future {
        retried.incrementAndGet()
      }(closeableEc)

      try {
        FutureUtil.doNotAwait(
          // This future probably never completes because we are likely to close the execution context during a `Delay`
          policy(closeable)(run(), AllExceptionRetryPolicy)(
            success = Success.never,
            executionContext = closeableEc,
            traceContext = implicitly,
            effect = implicitly,
          ),
          "retrying forever until the execution context closes",
        )

        Threading.sleep(50)
        logger.debug("About to close the FlagCloseable")
        closeable.close()
      } finally {
        LifeCycle.close(ExecutorServiceExtensions(closeableEc)(logger, timeouts))(logger)
      }
      succeed
    }

  def testStopOnShutdown(
      policy: PerformUnlessClosing => Policy,
      retriedUntilShutdown: Int,
  ): Unit =
    it("should stop on shutdown") {
      implicit val success: Success[Boolean] = Success(identity)
      val retried = new AtomicInteger()

      def run(): FutureUnlessShutdown[Boolean] = {
        val retries = retried.incrementAndGet()
        if (retries == retriedUntilShutdown) {
          FutureUnlessShutdown.abortedDueToShutdown
        } else {
          FutureUnlessShutdown.pure(false)
        }
      }

      policy(flagCloseable).unlessShutdown(run(), AllExceptionRetryPolicy).unwrap.map { result =>
        result shouldBe AbortedDueToShutdown
        retried.get() shouldBe retriedUntilShutdown
      }
    }

  def testSuspend(mkPolicy: Int => Eval[FiniteDuration] => RetryWithDelay): Unit =
    it("does not retry while suspended") {
      implicit val success: Success[Unit] = Success(_ => false)
      val maxRetries = 10
      val retried = new AtomicInteger()
      val suspend = new AtomicReference(Duration.Zero)

      def run(): Future[Unit] = {
        val retries = retried.incrementAndGet()
        logger.debug(s"testSuspend 'retries' has been incremented to $retries")
        if (suspend.get() > Duration.Zero) {
          logger.error("Policy is still retrying despite suspension.")
        } else if (retries == 3) {
          suspend.set(1.millis)
          logger.debug("testSuspend 'suspend' has been set to 1 millisecond.")
          FutureUtil.doNotAwait(
            DelayUtil.delay(100.millis).map { _ =>
              suspend.set(Duration.Zero)
              logger.debug("testSuspend 'suspend' has been reset to 0.")
            },
            "An error occurred while resetting suspension delay.",
          )
        }
        Future.unit
      }

      val policy = mkPolicy(maxRetries)(Eval.always(suspend.get()))
      policy.apply(run(), NoExceptionRetryPolicy).map { _ =>
        retried.get() shouldBe maxRetries + 3
      }
    }

  def testExceptionLogging(policy: => Policy): Unit =
    it("should log an exception with the configured retry log level") {
      // We don't care about the success criteria as we always throw an exception
      implicit val success: Success[Any] = Success.always

      case class TestException() extends RuntimeException("test exception")

      val retryable = new ExceptionRetryPolicy() {

        override protected def determineExceptionErrorKind(
            exception: Throwable,
            logger: TracedLogger,
        )(implicit
            tc: TraceContext
        ): ErrorKind =
          TransientErrorKind()

        override def retryLogLevel(e: Throwable): Option[Level] = e match {
          case TestException() => Some(Level.WARN)
          case _ => None
        }

      }

      loggerFactory
        .assertLogsSeq(SuppressionRule.Level(Level.WARN))(
          policy[Future, Assertion](Future.failed(TestException()), retryable).transform {
            case Failure(TestException()) =>
              logger.debug("Retry terminated with expected exception")
              TrySuccess(succeed)
            case result => result
          },
          entries =>
            forEvery(entries) { e =>
              e.warningMessage should (include("Detected an error")
                or include regex raw"The operation '\S+' has failed with an exception"
                or include regex raw"Now retrying operation '\S+'")
            },
        )
    }

  def testExceptionLoggingRetryForever(policy: => Policy): Unit =
    it(
      "when retrying forever, should keep using configured retry log level even after many retries"
    ) {
      // We don't care about the success criteria as we always throw an exception
      implicit val success: Success[Any] = Success.always

      case class TestException() extends RuntimeException("test exception")

      class Retryable extends ExceptionRetryPolicy {
        override protected def determineExceptionErrorKind(
            exception: Throwable,
            logger: TracedLogger,
        )(implicit tc: TraceContext): ErrorKind = TransientErrorKind()
      }

      class RetryableWithOverriddenLogLevel extends Retryable {
        override def retryLogLevel(e: Throwable): Option[Level] = e match {
          case TestException() => Some(Level.INFO)
          case _ => None
        }
      }

      val retried = new AtomicInteger()

      def run(): Future[Int] = {
        val num = retried.incrementAndGet()
        if (num > RetryWithDelay.complainAfterRetries + 1) Future.successful(num)
        else Future.failed(TestException())
      }

      for {
        // Check normal case of retrying forever with a retryable error that does not override the log level.
        // After `complainAfterRetries` retries, we should log at WARN.
        _ <- loggerFactory
          .assertLogsSeq(SuppressionRule.Level(Level.WARN))(
            policy[Future, Int](run(), new Retryable()),
            entries =>
              forExactly(2, entries) { e =>
                e.warningMessage should (include regex raw"The operation \S+ has failed with an exception"
                  or include regex raw"Now retrying operation \S+")
              },
          )

        // If the retryable error overrides the lever lower than WARN, we should
        // not see WARN logs even after `complainAfterRetries` retries.
        _ <- loggerFactory
          .assertLogsSeq(SuppressionRule.Level(Level.WARN))(
            {
              retried.set(0)
              policy[Future, Int](run(), new RetryableWithOverriddenLogLevel())
            },
            _ shouldBe empty,
          )
      } yield succeed
    }

}
