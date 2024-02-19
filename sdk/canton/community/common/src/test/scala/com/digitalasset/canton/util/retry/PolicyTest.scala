// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util.retry

import cats.Eval
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, Threading}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  PerformUnlessClosing,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{SuppressionRule, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.retry.Jitter.RandomSource
import com.digitalasset.canton.util.retry.RetryUtil.{
  AllExnRetryable,
  DbExceptionRetryable,
  ExceptionRetryable,
  NoExnRetryable,
}
import com.digitalasset.canton.util.{DelayUtil, FutureUtil}
import com.digitalasset.canton.{BaseTest, HasExecutorService}
import org.scalatest.funspec.AsyncFunSpec
import org.slf4j.event.Level

import java.util.Random
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success as TrySuccess, Try}

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

    testUnexpectedException(Directly(logger, flagCloseable, 10, "op"))

    it("should retry a future for a specified number of times") {
      implicit val success: Success[Int] = Success(_ == 3)
      val tries = forwardCountingFutureStream().iterator
      Directly(logger, flagCloseable, 3, "op")(tries.next(), AllExnRetryable).map(result =>
        assert(success.predicate(result) === true)
      )
    }

    it("should fail when expected") {
      val success = implicitly[Success[Option[Int]]]
      val tries = Future(None: Option[Int])
      Directly(logger, flagCloseable, 2, "op")(tries, AllExnRetryable).map(result =>
        assert(success.predicate(result) === false)
      )
    }

    it("should deal with future failures") {
      implicit val success: Success[Any] = Success.always
      val policy = Directly(logger, flagCloseable, 3, "op")
      val counter = new AtomicInteger(0)
      val future = policy(
        {
          counter.incrementAndGet()
          Future.failed(new RuntimeException("always failing"))
        },
        AllExnRetryable,
      )
      // expect failure after 1+3 tries
      future.failed.map { t =>
        assert(counter.get() === 4 && t.getMessage === "always failing")
      }
    }

    testSynchronousException(Directly(logger, flagCloseable, 3, "op"), 3)

    it("should accept a future in reduced syntax format") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger()
      val future = Directly(logger, flagCloseable, 1, "op")(
        {
          counter.incrementAndGet()
          Future.failed(new RuntimeException("always failing"))
        },
        AllExnRetryable,
      )
      future.failed.map(t => assert(counter.get() === 2 && t.getMessage === "always failing"))
    }

    it("should retry futures passed by-name instead of caching results") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger()
      val future = Directly(logger, flagCloseable, 1, "op")(
        {
          counter.getAndIncrement() match {
            case 1 => Future.successful("yay!")
            case _ => Future.failed(new RuntimeException("failed"))
          }
        },
        AllExnRetryable,
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

      val policy = Directly(logger, flagCloseable, Forever, "op")
      policy(run(), AllExnRetryable).map { result =>
        assert(result === true)
        assert(retried.get() == retriedUntilSuccess)
      }
    }

    testStopOnClosing(Directly(logger, _, Forever, "op", retryLogLevel = Some(Level.INFO)), 10)

    testClosedExecutionContext(Directly(logger, _, Forever, "op", retryLogLevel = Some(Level.INFO)))

    testStopOnShutdown(Directly(logger, _, Forever, "op"), 10)

    testSuspend(maxRetries =>
      suspend => Directly(logger, flagCloseable, maxRetries, "op", suspendRetries = suspend)
    )

    testExceptionLogging(Directly(logger, flagCloseable, 3, "op"))
  }

  describe("retry.Pause") {

    testUnexpectedException(Pause(logger, flagCloseable, 10, 30.millis, "op"))

    it("should pause in between retries") {
      implicit val success: Success[Int] = Success(_ == 3)
      val tries = forwardCountingFutureStream().iterator
      val policy = Pause(logger, flagCloseable, 3, 30.millis, "op")
      val marker_base = System.currentTimeMillis
      val marker = new AtomicLong(0)

      val runF = policy(
        {
          marker.set(System.currentTimeMillis); tries.next()
        },
        AllExnRetryable,
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

      val policy = Pause(logger, flagCloseable, Forever, 1.millis, "op")
      policy(run(), AllExnRetryable).map { result =>
        assert(result === true)
        assert(retried.get() == retriedUntilSuccess)
      }
    }

    testSynchronousException(Pause(logger, flagCloseable, 1, 1.millis, "op"), 1)

    testStopOnClosing(
      Pause(logger, _, Forever, 50.millis, "op", retryLogLevel = Some(Level.INFO)),
      3,
    )

    testClosedExecutionContext(
      Pause(logger, _, Forever, 10.millis, "op", retryLogLevel = Some(Level.INFO))
    )

    testStopOnShutdown(Pause(logger, _, Forever, 1.millis, "op"), 10)

    testSuspend(maxRetries =>
      suspend => Pause(logger, flagCloseable, maxRetries, 5.millis, "op", suspendRetries = suspend)
    )

    testExceptionLogging(Pause(logger, flagCloseable, 3, 1.millis, "op"))
  }

  describe("retry.Backoff") {

    implicit val jitter: Jitter = Jitter.none(1.minute)

    testUnexpectedException(Backoff(logger, flagCloseable, 10, 30.millis, Duration.Inf, "op"))

    it("should pause with multiplier between retries") {
      implicit val success: Success[Int] = Success(_ == 2)
      val tries = forwardCountingFutureStream().iterator
      val policy = Backoff(logger, flagCloseable, 2, 30.millis, Duration.Inf, "op")
      val marker_base = System.currentTimeMillis
      val marker = new AtomicLong(0)
      val runF = policy(
        {
          marker.set(System.currentTimeMillis); tries.next()
        },
        AllExnRetryable,
      )
      runF.map { result =>
        val delta = marker.get() - marker_base
        assert(
          success.predicate(result) === true &&
            delta >= 90 && delta <= 500
        ) // was 110
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

      val policy = Backoff(logger, flagCloseable, Forever, 1.millis, Duration.Inf, "op")
      policy(run(), AllExnRetryable).map { result =>
        assert(result === true)
        assert(retried.get() == 5)
      }
    }

    testSynchronousException(Backoff(logger, flagCloseable, 1, 1.millis, Duration.Inf, "op"), 1)

    testStopOnClosing(
      Backoff(logger, _, Forever, 10.millis, Duration.Inf, "op", retryLogLevel = Some(Level.INFO)),
      3,
    )

    testClosedExecutionContext(
      Backoff(logger, _, Forever, 10.millis, Duration.Inf, "op", retryLogLevel = Some(Level.INFO))
    )

    testStopOnShutdown(Backoff(logger, _, 10, 1.millis, Duration.Inf, "op"), 3)

    testSuspend(maxRetries =>
      suspend =>
        Backoff(
          logger,
          flagCloseable,
          maxRetries,
          5.milli,
          Duration.Inf,
          "op",
          suspendRetries = suspend,
        )
    )

    testExceptionLogging(Backoff(logger, flagCloseable, 3, 1.millis, 1.millis, "op"))
  }

  def testJitterBackoff(name: String, algoCreator: FiniteDuration => Jitter): Unit = {
    describe(s"retry.JitterBackoff.$name") {

      testUnexpectedException(
        Backoff(logger, flagCloseable, 10, 30.millis, Duration.Inf, "op")(algoCreator(10.millis))
      )

      it("should retry a future for a specified number of times") {
        implicit val success: Success[Int] = Success(_ == 3)
        implicit val algo: Jitter = algoCreator(10.millis)
        val tries = forwardCountingFutureStream().iterator
        val policy = Backoff(logger, flagCloseable, 3, 1.milli, Duration.Inf, "op")
        policy(tries.next(), AllExnRetryable).map(result =>
          assert(success.predicate(result) === true)
        )
      }

      it("should fail when expected") {
        implicit val algo: Jitter = algoCreator(10.millis)
        val success = implicitly[Success[Option[Int]]]
        val tries = Future(None: Option[Int])
        val policy = Backoff(logger, flagCloseable, 3, 1.milli, Duration.Inf, "op")
        policy(tries, AllExnRetryable).map(result => assert(success.predicate(result) === false))
      }

      it("should deal with future failures") {
        implicit val success: Success[Any] = Success.always
        implicit val algo: Jitter = algoCreator(10.millis)
        val policy = Backoff(logger, flagCloseable, 3, 5.millis, Duration.Inf, "op")
        val counter = new AtomicInteger()
        val future = policy(
          {
            counter.incrementAndGet()
            Future.failed(new RuntimeException("always failing"))
          },
          AllExnRetryable,
        )
        future.failed.map(t => assert(counter.get() === 4 && t.getMessage === "always failing"))
      }

      it("should retry futures passed by-name instead of caching results") {
        implicit val success: Success[Any] = Success.always
        implicit val algo: Jitter = algoCreator(10.millis)
        val counter = new AtomicInteger()
        val policy = Backoff(logger, flagCloseable, 1, 1.milli, Duration.Inf, "op")
        val future = policy(
          {
            counter.getAndIncrement() match {
              case 1 => Future.successful("yay!")
              case _ => Future.failed(new RuntimeException("failed"))
            }
          },
          AllExnRetryable,
        )
        future.map(result => assert(counter.get() == 2 && result === "yay!"))
      }

      it("should pause with multiplier and jitter between retries") {
        implicit val success: Success[Int] = Success(_ == 2)
        implicit val algo: Jitter = algoCreator(1000.millis)
        val tries = forwardCountingFutureStream().iterator
        val policy = Backoff(logger, flagCloseable, 5, 50.millis, Duration.Inf, "op")
        val marker_base = System.currentTimeMillis
        val marker = new AtomicLong(0)

        policy(
          {
            marker.set(System.currentTimeMillis); tries.next()
          },
          AllExnRetryable,
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
        implicit val algo: Jitter = algoCreator(50.millis)
        val tries = forwardCountingFutureStream().iterator
        val policy = Backoff(logger, flagCloseable, Forever, 10.millis, Duration.Inf, "op")
        val marker_base = System.currentTimeMillis
        val marker = new AtomicLong(0)

        policy(
          {
            marker.set(System.currentTimeMillis); tries.next()
          },
          AllExnRetryable,
        ).map { result =>
          val delta = marker.get() - marker_base
          assert(
            success.predicate(result) === true &&
              delta >= 0 && delta <= 1000
          )
        }
      }

      it("should repeat on unexpected value with jitter backoff until success") {
        implicit val success: Success[Boolean] = Success(identity)
        implicit val algo: Jitter = algoCreator(10.millis)
        val retried = new AtomicInteger()
        val retriedUntilSuccess = 10

        def run() =
          if (retried.get() < retriedUntilSuccess) {
            retried.incrementAndGet()
            Future(false)
          } else {
            Future(true)
          }

        val policy = Backoff(logger, flagCloseable, Forever, 1.millis, Duration.Inf, "op")
        policy(run(), AllExnRetryable).map { result =>
          assert(result === true)
          assert(retried.get() == retriedUntilSuccess)
        }
      }
    }
  }

  testJitterBackoff("none", t => Jitter.none(t))
  testJitterBackoff("full", t => Jitter.full(t, random = randomSource))
  testJitterBackoff("equal", t => Jitter.equal(t, random = randomSource))
  testJitterBackoff("decorrelated", t => Jitter.decorrelated(t, random = randomSource))

  describe("retry.When") {

    testUnexpectedException(
      When(logger, { case _ => Pause(logger, flagCloseable, 10, 30.millis, "op") })
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
                Pause(logger, flagCloseable, 4, delay = 2.seconds, "op")
              },
            )
        },
      )
      val future = policy(tries.next(), AllExnRetryable)
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
          case 1 => Directly(logger, flagCloseable, maxRetries = 3, "op")
        },
      )

      val future = policy(tries.next(), AllExnRetryable)
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
          case RetryAfter(duration) => Pause(logger, flagCloseable, 4, delay = duration, "op")
        },
      )
      policy(run(), AllExnRetryable).map(result => assert(result === true))
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
          case RetryAfter(duration) => Pause(logger, flagCloseable, 4, delay = duration, "op")
        },
      )
      policy(run(), AllExnRetryable).map(result => assert(result === true))
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
          case _: MyException => Directly(logger, flagCloseable, Forever, "op")
        },
      )
      policy(run(), AllExnRetryable).map { result =>
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
          case _: MyException => Pause(logger, flagCloseable, Forever, 1.millis, "op")
        },
      )
      policy(run(), AllExnRetryable).map { result =>
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
            Backoff(logger, flagCloseable, Forever, 1.millis, Duration.Inf, "op")
        },
      )
      policy(run(), AllExnRetryable).map { result =>
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
            Backoff(logger, flagCloseable, Forever, 1.millis, Duration.Inf, "op")
        },
      )
      policy(run(), AllExnRetryable).map { result =>
        assert(result === true)
        assert(retried.get() == 10)
      }
    }

    testStopOnShutdown(
      _ => When(logger, _ => Directly(logger, flagCloseable, Forever, "should-not-run")),
      1,
    )
  }

  def testUnexpectedException(policy: Policy): Unit = {
    it("should not retry after an exception that isn't retryable") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger()
      val future = policy(
        {
          counter.incrementAndGet()
          Future.failed(new RuntimeException(s"unexpected problem"))
        },
        DbExceptionRetryable,
      )
      future.failed.map(t => assert(counter.get() === 1 && t.getMessage === "unexpected problem"))
    }
  }

  def testSynchronousException(policy: Policy, maxRetries: Int): Unit = {
    it("should convert a synchronous exception into an asynchronous one") {
      implicit val success: Success[Any] = Success.always
      val counter = new AtomicInteger(0)
      val future = policy(
        {
          counter.incrementAndGet()
          throw new RuntimeException("always failing")
        },
        AllExnRetryable,
      )
      // expect failure after 1+maxRetries tries
      future.failed.map { t =>
        assert(counter.get() === maxRetries + 1 && t.getMessage === "always failing")
      }
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

      val result = policy(closeable)(run(), AllExnRetryable)(
        success,
        executorService,
        traceContext,
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

      val retryF =
        policy(closeable)(run(), AllExnRetryable)(Success.never, executorService, traceContext)
          .thereafter { count =>
            logger.debug(s"Stopped retry after $count")
          }(executorService)

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

  def testClosedExecutionContext(policy: PerformUnlessClosing => Policy): Unit = {
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
          policy(closeable)(run(), AllExnRetryable)(Success.never, closeableEc, implicitly),
          "retrying forever until the execution context closes",
        )

        Threading.sleep(50)
        logger.debug("About to close the FlagCloseable")
        closeable.close()
      } finally {
        Lifecycle.close(ExecutorServiceExtensions(closeableEc)(logger, timeouts))(logger)
      }
      succeed
    }
  }

  def testStopOnShutdown(
      policy: PerformUnlessClosing => Policy,
      retriedUntilShutdown: Int,
  ): Unit = {
    it("should stop on shutdown") {
      implicit val success: Success[Boolean] = Success(identity)
      val retried = new AtomicInteger()

      val flagCloseable1: FlagCloseable = FlagCloseable(logger, DefaultProcessingTimeouts.testing)

      def run(): FutureUnlessShutdown[Boolean] = {
        val retries = retried.incrementAndGet()
        if (retries == retriedUntilShutdown) {
          FutureUnlessShutdown.abortedDueToShutdown
        } else {
          FutureUnlessShutdown.pure(false)
        }
      }

      policy(flagCloseable1).unlessShutdown(run(), AllExnRetryable).unwrap.map { result =>
        result shouldBe AbortedDueToShutdown
        retried.get() shouldBe retriedUntilShutdown
      }
    }
  }

  def testSuspend(mkPolicy: Int => Eval[FiniteDuration] => RetryWithDelay): Unit = {
    it("does not retry while suspended") {
      implicit val success: Success[Unit] = Success(_ => false)
      val maxRetries = 10

      val retried = new AtomicInteger()
      val suspend = new AtomicReference(Duration.Zero)

      def run(): Future[Unit] = {
        val retries = retried.incrementAndGet()
        if (suspend.get() > Duration.Zero) {
          logger.error("Policy is still retrying despite suspension.")
        } else if (retries == 3) {
          suspend.set(1.millis)
          FutureUtil.doNotAwait(
            DelayUtil.delay(100.millis).map(_ => suspend.set(Duration.Zero)),
            "An error occurred while resetting suspension delay.",
          )
        }
        Future.unit
      }

      val policy = mkPolicy(maxRetries)(Eval.always(suspend.get()))
      policy.apply(run(), NoExnRetryable).map { _ =>
        retried.get() shouldBe maxRetries + 3
      }
    }
  }

  def testExceptionLogging(policy: => Policy): Unit = {
    it("should log an exception with the configured retry log level") {
      // We don't care about the success criteria as we always throw an exception
      implicit val success: Success[Any] = Success.always

      case class TestException() extends RuntimeException("test exception")

      val retryable = new ExceptionRetryable() {
        override def retryOK(
            outcome: Try[_],
            logger: TracedLogger,
            lastErrorKind: Option[RetryUtil.ErrorKind],
        )(implicit tc: TraceContext): RetryUtil.ErrorKind = RetryUtil.TransientErrorKind

        override def retryLogLevel(e: Throwable): Option[Level] = e match {
          case TestException() => Some(Level.WARN)
          case _ => None
        }
      }

      loggerFactory
        .assertLogsSeq(SuppressionRule.Level(Level.WARN))(
          policy(Future.failed(TestException()), retryable),
          { entries =>
            forEvery(entries) { e =>
              e.warningMessage should (include(
                "The operation 'op' has failed with an exception"
              ) or include("Now retrying operation 'op'"))
            }
          },
        )
        .transform {
          case Failure(TestException()) =>
            logger.debug("Retry terminated with expected exception")
            TrySuccess(succeed)
          case result => result
        }
    }

  }
}
