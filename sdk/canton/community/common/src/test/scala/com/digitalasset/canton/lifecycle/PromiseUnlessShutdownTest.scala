// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.concurrent.{FutureSupervisor, Threading}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.OnShutdownRunner.PureOnShutdownRunner
import com.digitalasset.canton.lifecycle.UnlessShutdown.AbortedDueToShutdown
import com.digitalasset.canton.logging.{SuppressionRule, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level
import org.slf4j.event.Level.WARN

import java.util.concurrent.ScheduledExecutorService
import scala.concurrent.duration.*

class PromiseUnlessShutdownTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  "PromiseUnlessShutdown" should {

    "complete a promise with an outcome" in {
      val p = PromiseUnlessShutdown.unsupervised[Int]()
      p.outcome(42)

      // Ignore second outcome
      p.outcome(23)

      p.future.futureValue shouldBe UnlessShutdown.Outcome(42)
    }

    "complete a promise due to shutdown" in {
      val p = PromiseUnlessShutdown.unsupervised[Int]()
      p.shutdown()
      p.future.futureValue shouldBe UnlessShutdown.AbortedDueToShutdown
    }

    "detect if a promise is not completed in time" in {
      implicit val scheduler: ScheduledExecutorService = scheduledExecutor()

      loggerFactory.assertLogsSeq(SuppressionRule.LevelAndAbove(WARN))(
        {
          val p = PromiseUnlessShutdown.supervised[Int](
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

    "supervision should start only once the future is accessed" in {
      implicit val scheduler: ScheduledExecutorService = scheduledExecutor()

      val promise = loggerFactory.assertLogs(SuppressionRule.LevelAndAbove(WARN))(
        {
          val p = PromiseUnlessShutdown.supervised[Int](
            "supervised-promise",
            new FutureSupervisor.Impl(config.NonNegativeDuration(5.second)),
            1.second,
            Level.WARN,
          )

          // Wait longer than the future supervisor warn duration
          always(durationOfSuccess = 3.seconds) {
            loggerFactory.fetchRecordedLogEntries shouldBe Seq.empty
          }
          p
        }
      )
      promise.outcome(1)
      promise.futureUS.futureValueUS shouldBe 1
    }

    "abort on shutdown" in {
      val onShutdownRunner = new PureOnShutdownRunner(logger)
      val promise = PromiseUnlessShutdown.abortOnShutdown(
        "aborted-promise",
        onShutdownRunner,
        FutureSupervisor.Noop,
      )
      onShutdownRunner.close()
      promise.future.futureValue shouldBe AbortedDueToShutdown
    }

    "discarded promises do not leak memory" in {

      object RecordingOnShutdownRunner extends AutoCloseable with OnShutdownRunner {
        var tasks = Seq.empty[(Long, RunOnShutdown)]

        // Intercept all the tasks and add them to tasks list
        override def runOnShutdown[T](task: RunOnShutdown)(implicit
            traceContext: TraceContext
        ): Long = {
          val token = super.runOnShutdown(task)
          tasks = tasks :+ (token -> task)
          token
        }

        override protected def logger: TracedLogger = PromiseUnlessShutdownTest.this.logger
        override protected def onFirstClose(): Unit = ()
        override def close(): Unit = super.close()
      }
      val promise = PromiseUnlessShutdown.abortOnShutdown[Int](
        "aborted-promise",
        RecordingOnShutdownRunner,
        FutureSupervisor.Noop,
      )
      promise.discard

      RecordingOnShutdownRunner.tasks.size shouldBe 1
      val (token, task) = RecordingOnShutdownRunner.tasks.collectFirst {
        case (tok, x: PromiseUnlessShutdown.AbortPromiseOnShutdown) => tok -> x
      }.value
      // Simulate the promise being GCed by clearing the weak reference
      task.promiseRef.clear()

      // Register a new task to get the old task cleared:
      RecordingOnShutdownRunner.runOnShutdown_(new RunOnShutdown {
        override def name: String = "dummy"
        override def done: Boolean = false
        override def run(): Unit = ()
      })

      RecordingOnShutdownRunner.containsShutdownTask(token) shouldBe false
    }
  }
}
