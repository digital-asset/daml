// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level
import org.slf4j.event.Level.WARN

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.*

class PromiseUnlessShutdownTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  "PromiseUnlessShutdown" should {

    "complete a promise with an outcome" in {
      val p = new PromiseUnlessShutdown[Int]("test", futureSupervisor)
      p.outcome(42)

      // Ignore second outcome
      p.outcome(23)

      p.future.futureValue shouldBe UnlessShutdown.Outcome(42)
    }

    "complete a promise due to shutdown" in {
      val p = new PromiseUnlessShutdown[Int]("test", futureSupervisor)
      p.shutdown()
      p.future.futureValue shouldBe UnlessShutdown.AbortedDueToShutdown
    }

    "detect if a promise is not completed in time" in {
      implicit val scheduler: ScheduledExecutorService = scheduledExecutor()

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
        {
          val p = new PromiseUnlessShutdown[Int](
            "supervised-promise",
            new FutureSupervisor.Impl(config.NonNegativeDuration(5.second)),
            1.second,
            Level.WARN,
          )
          val f = p.future

          // Wait longer than the future supervisor warn duration
          Threading.sleep(3.second.toMillis)

          // Eventually complete the promise
          p.outcome(42)
          f.futureValue shouldBe UnlessShutdown.Outcome(42)
        },
        entries => {
          assert(entries.nonEmpty)
          forEvery(entries)(
            _.warningMessage should include("supervised-promise has not completed after")
          )
        },
      )
    }

  }
}
