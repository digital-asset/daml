// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.duration.*
import scala.util.{Failure, Success, Try}

@SuppressWarnings(Array("org.wartremover.warts.While"))
class FutureUtilTest extends AnyWordSpec with BaseTest {

  "FutureUtil.noisyAwaitResult" should {

    lazy val timeout = 27.seconds
    lazy val warnAfter = 8.seconds
    lazy val task = "test task"
    lazy val noKillSwitch: Unit => Boolean = _ => false
    def noStackTracesInUnitTest: Thread => Boolean = _ => false

    "Keep retrying until the timeout has expired" in {

      val (logs, logF, numAwaits) = state
      val expectedAwaits = List(8.seconds, 4.seconds, 4.seconds, 4.seconds, 4.seconds, 3.seconds)
      val ready = neverSucceed(expectedAwaits, numAwaits)(_, _)

      val res = FutureUtil.noisyAwaitResultForTesting(
        Future.unit,
        task,
        timeout,
        logF,
        () => nanoTime(expectedAwaits, numAwaits)(),
        warnAfter,
        noKillSwitch,
        noStackTracesInUnitTest,
      )(ready)

      logs.toList should be(loggedMessages(task, expectedAwaits, failure = true))

      res should matchPattern {
        case Failure(exn: TimeoutException)
            if exn.getMessage == s"Task $task did not complete within ${timeout}." =>
      }
    }

    "Never wait longer than max retry interval of 10 seconds" in {
      val (logs, logF, numAwaits) = state
      val largeTimeout = 42.seconds
      val largeWarnAfter = 22.seconds // large enough such that half is still larger than 10 seconds
      val expectedAwaits = List(
        10.seconds,
        10.seconds,
        2.seconds, // Lowered to 2 seconds to hit 22 second warning cut-off
        10.seconds,
        10.seconds,
      )
      val ready = neverSucceed(expectedAwaits, numAwaits)(_, _)

      val res = FutureUtil.noisyAwaitResultForTesting(
        Future.unit,
        task,
        largeTimeout,
        logF,
        () => nanoTime(expectedAwaits, numAwaits)(),
        largeWarnAfter,
        noKillSwitch,
        noStackTracesInUnitTest,
      )(ready)

      logs.toList should be(
        loggedMessages(
          task,
          expectedAwaits,
          failure = true,
          expectedDebugLogs = 2, // first two entries are debug as they happen before largeWarnAfter
        )
      )

      res should matchPattern {
        case Failure(exn: TimeoutException)
            if exn.getMessage == s"Task $task did not complete within ${largeTimeout}." =>
      }
    }

    "Stop as soon as the Future completes, with a finite timeout" in {

      val (logs, logF, numAwaits) = state
      val expectedAwaits = List(8.seconds, 4.seconds)

      val ready = succeedAfter(2, Future.unit, numAwaits, expectedAwaits)(_, _)

      val res = FutureUtil.noisyAwaitResultForTesting(
        Future.unit,
        task,
        timeout,
        logF,
        () => nanoTime(expectedAwaits, numAwaits)(),
        warnAfter,
        noKillSwitch,
        noStackTracesInUnitTest,
      )(ready)

      logs.toList should be(loggedMessages(task, expectedAwaits))

      res should be(Success(()))
    }

    "Stop as soon as the Future completes, with an infinite timeout" in {

      val (logs, logF, numAwaits) = state
      val expectedAwaits = List(8.seconds, 4.seconds)

      val ready = succeedAfter(2, Future.unit, numAwaits, expectedAwaits)(_, _)

      val res = FutureUtil.noisyAwaitResultForTesting(
        Future.unit,
        task,
        Duration.Inf,
        logF,
        () => nanoTime(expectedAwaits, numAwaits)(),
        warnAfter,
        noKillSwitch,
        noStackTracesInUnitTest,
      )(ready)

      logs.toList should be(loggedMessages(task, expectedAwaits))

      res should be(Success(()))
    }

    "Not get confused when the future throws a Timeout exception" in {

      val (logs, logF, numAwaits) = state
      val expectedAwaits = List(8.seconds, 4.seconds)

      val ready = succeedAfter(
        2,
        Future.failed {
          new TimeoutException(s"This is a different timeout exception")
        },
        numAwaits,
        expectedAwaits,
      )(_, _)

      try {
        FutureUtil.noisyAwaitResultForTesting(
          Future.unit,
          task,
          Duration.Inf,
          logF,
          () => nanoTime(expectedAwaits, numAwaits)(),
          warnAfter,
          noKillSwitch,
          noStackTracesInUnitTest,
        )(ready)
      } catch {
        case exn: TimeoutException =>
          exn.getMessage shouldBe s"This is a different timeout exception"

          logs.toList should be(loggedMessages(task, expectedAwaits))

        case _: Throwable =>
          fail(
            s"Noisy await result should return the exception thrown by the future being blocked on."
          )
      }

    }

    "Not block on the future when the timeout is zero" in {

      val (logs, logF, numAwaits) = state
      val expectedAwaits = List()
      val ready = neverSucceed(expectedAwaits, numAwaits)(_, _)

      val res = FutureUtil.noisyAwaitResultForTesting(
        Future.unit,
        task,
        0.seconds,
        logF,
        () => nanoTime(expectedAwaits, numAwaits)(),
        warnAfter,
        noKillSwitch,
        noStackTracesInUnitTest,
      )(ready)

      logs.toList should be(loggedMessages(task, expectedAwaits, failure = true))

      res should matchPattern {
        case Failure(exn: TimeoutException)
            if exn.getMessage == s"Task $task did not complete within 0 seconds." =>
      }
    }

    "Cancel the await when the kill-switch is triggered" in {

      val (logs, logF, numAwaits) = state
      val expectedAwaits =
        List(8.seconds, 4.seconds, 4.seconds, 4.seconds, 4.seconds, 3.seconds, 3.seconds, 3.seconds)
      val killSwitch: AtomicBoolean = new AtomicBoolean(false)
      val ready = killswitchAfter(expectedAwaits, numAwaits, killSwitch)(_, _)

      try {
        FutureUtil.noisyAwaitResultForTesting(
          Future.unit,
          task,
          timeout,
          logF,
          () => nanoTime(expectedAwaits, numAwaits)(),
          warnAfter,
          (_ => killSwitch.get()),
          noStackTracesInUnitTest,
        )(ready)
        fail(s"Noisy wait result was expected to throw")
      } catch {
        case exn: TimeoutException =>
          exn.getMessage should include("Noisy await result test task cancelled with kill-switch")
      }

      numAwaits.get() shouldBe 4
    }

  }

  private def loggedMessages(
      task: => String,
      awaits: List[FiniteDuration],
      failure: Boolean = false,
      expectedDebugLogs: Int = 0,
  ) = {
    val cumulativeAwaits = cumulative(awaits)
    val infos = cumulativeAwaits
      .zip((1 to awaits.length).map(i => if (i <= expectedDebugLogs) Level.DEBUG else Level.INFO))
      .map { case (duration, level) =>
        level -> s"Task $task still not completed after ${duration}. Continue waiting..."
      }
    if (failure)
      infos :+ Level.WARN -> s"Task ${task} did not complete within ${cumulativeAwaits.lastOption
          .getOrElse(0.seconds)}. Stack traces:\n"
    else infos
  }

  private def cumulative(expectedAwaits: List[FiniteDuration]) = {
    expectedAwaits
      .foldLeft(List.empty[Duration]) { case (acc, x) =>
        val total = acc.headOption.getOrElse(0.seconds)
        x.plus(total) :: acc
      }
      .reverse
  }

  private def state
      : (mutable.ArrayDeque[(Level, String)], (Level, String) => Unit, AtomicInteger) = {
    val logs = mutable.ArrayDeque[(Level, String)]()
    def logF(l: Level, s: String): Unit = {
      val tuple = (l, s)
      logs += tuple
      ()
    }
    (logs, logF, new AtomicInteger())
  }

  private def succeedAfter(
      n: Int,
      success: => Future[Unit],
      counter: AtomicInteger,
      expectedAwaits: List[Duration],
  )(f: Future[Unit], d: Duration): Try[Future[Unit]] = {
    val i = counter.get()
    if (i >= 2) Success(success)
    else {
      d should be(expectedAwaits(i))
      counter.incrementAndGet()
      Failure(new TimeoutException)
    }
  }

  def neverSucceed(
      expectedAwaits: List[Duration],
      counter: AtomicInteger,
  )(f: Future[Unit], d: Duration): Try[Future[Unit]] = {
    d should be(expectedAwaits(counter.get()))
    counter.incrementAndGet()
    Failure(new TimeoutException)
  }

  def killswitchAfter(
      expectedAwaits: List[Duration],
      counter: AtomicInteger,
      killSwitch: AtomicBoolean,
  )(f: Future[Unit], d: Duration): Try[Future[Unit]] = {
    d should be(expectedAwaits(counter.get()))
    val c = counter.incrementAndGet()
    if (c > 3) killSwitch.set(true)
    Failure(new TimeoutException)
  }

  /** @param counter A counter that is **externally** incremented for every iteration of the retry loop
    */
  def nanoTime(expectedAwaits: List[Duration], counter: AtomicInteger)(): Long = {
    val soFar = expectedAwaits.take(counter.get).foldLeft(0.seconds: Duration) { case (acc, next) =>
      acc.plus(next)
    }
    soFar.toNanos
  }
}
