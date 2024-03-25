// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AsyncWordSpec

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class PeriodicActionTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private val interval = NonNegativeFiniteDuration.tryOfSeconds(5)

  class Env(actionDelay: Duration = Duration.ZERO) {
    val clock = new SimClock(CantonTimestamp.Epoch, loggerFactory)
    val numberOfCalls = new AtomicInteger(0)

    val sut = new PeriodicAction(
      clock,
      interval,
      loggerFactory,
      ProcessingTimeout(),
      "test",
    )(_ => {
      numberOfCalls.incrementAndGet()
      clock.scheduleAfter(_ => (), actionDelay).unwrap
    })

    // Sometimes we need to make sure that the asynchronous scheduling of the next task has happened (in real time)
    def eventuallyOneTaskIsScheduled() =
      eventually()(clock.numberOfScheduledTasks shouldBe 1)
  }

  "should call function periodically" in {
    val env = new Env()
    import env.*

    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 0

    clock.advance(interval.duration)
    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 1

    clock.advance(interval.duration)
    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 2

    clock.advance(interval.duration)
    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 3

    sut.close()

    clock.advance(interval.duration)
    numberOfCalls.get shouldBe 3
    clock.advance(interval.duration)
    numberOfCalls.get shouldBe 3
  }

  "should not call function after we are closed" in {
    val env = new Env()
    import env.*

    numberOfCalls.get shouldBe 0

    sut.close()

    clock.advance(interval.duration.multipliedBy(2L))
    numberOfCalls.get shouldBe 0
  }

  "should wait in close for potentially concurrent running action to be finished" in {
    val env = new Env(interval.duration.multipliedBy(2))
    import env.*

    eventuallyOneTaskIsScheduled()
    numberOfCalls.get shouldBe 0

    clock.advance(interval.duration)
    numberOfCalls.get shouldBe 1
    eventuallyOneTaskIsScheduled()

    val closingFuture = Future(sut.close())

    // sleep in real time too, so the closingFuture has a chance to complete on a separate thread
    Threading.sleep(200)

    closingFuture.isCompleted shouldBe false
    numberOfCalls.get shouldBe 1
    eventuallyOneTaskIsScheduled()

    clock.advance(interval.duration)
    // we only advanced one interval, so the started task is still running
    numberOfCalls.get shouldBe 1
    eventuallyOneTaskIsScheduled()
    closingFuture.isCompleted shouldBe false

    clock.advance(interval.duration)
    numberOfCalls.get shouldBe 1
    // after task finished, and testee closed, there should be no more scheduled tasks for the clock
    eventually()(clock.numberOfScheduledTasks shouldBe 0)
    // This might happen also asynchronously
    eventually()(closingFuture.isCompleted shouldBe true)
  }
}
