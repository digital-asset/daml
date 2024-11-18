// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{LogEntry, SuppressionRule}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.wordspec.AsyncWordSpec
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Future, Promise}

class SimpleExecutionQueueTest
    extends AsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with BeforeAndAfterEach {

  private val queueTimeouts = timeouts.copy(
    shutdownProcessing = config.NonNegativeDuration.ofSeconds(1),
    closing = config.NonNegativeDuration.ofSeconds(1),
  )

  private class MockTask(name: String) {
    val started = new AtomicBoolean(false)
    private val promise: Promise[UnlessShutdown[String]] = Promise[UnlessShutdown[String]]()

    def run(): FutureUnlessShutdown[String] = {
      started.set(true)
      FutureUnlessShutdown(promise.future)
    }

    def complete(): Unit = promise.success(UnlessShutdown.Outcome(name))

    def shutdown(): Unit = promise.success(UnlessShutdown.AbortedDueToShutdown)

    def fail(): Unit = promise.failure(new RuntimeException(s"mocked failure for $name"))
  }

  /*
  Fail a task and captures the warning coming from subsequent tasks that will not be run.
   */
  private def failTask(task: MockTask, notRunTasks: Seq[String]): Unit =
    terminateTask(task, shutdown = false, notRunTasks)

  /*
  Shut a task down and captures the warning coming from subsequent tasks that will not be run.
   */
  private def shutdownTask(task: MockTask, notRunTasks: Seq[String]): Unit =
    terminateTask(task, shutdown = true, notRunTasks)

  /** @param shutdown True if shutdown, false if failure
    * @param notRunTasks Task which is not run because of the shutdown/failure
    */
  private def terminateTask(task: MockTask, shutdown: Boolean, notRunTasks: Seq[String]): Unit =
    if (shutdown) {
      task.shutdown()
    } else {
      loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.ERROR))(
        task.fail(),
        LogEntry.assertLogSeq(
          notRunTasks.map { notRunTask =>
            (
              _.errorMessage should include(
                s"Task '$notRunTask' will not run because of failure of previous task"
              ),
              "task does not run",
            )
          },
          Seq.empty,
        ),
      )
    }

  private def simpleExecutionQueueTests(
      mk: () => SimpleExecutionQueue
  ): Unit = {

    "only run one future at a time" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")

      task2.started.get() should be(false)
      task1.complete()

      for {
        _ <- task1Result.failOnShutdown
        _ = task1.started.get() shouldBe true
        // queue one while running
        task3Result = queue.executeUS(task3.run(), "Task3").failOnShutdown
        _ = task2.complete()
        _ <- task2Result.failOnShutdown("aborted due to shutdown.")
        _ = task2.started.get() shouldBe true
        _ = task3.complete()
        _ <- task3Result
      } yield task3.started.get() should be(true)
    }

    "flush never fails" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task1Result = queue.executeUS(task1.run(), "Task1")

      val flush0 = queue.flush()
      flush0.isCompleted shouldBe false
      task1.fail()
      for {
        _ <- task1Result.failed.failOnShutdown("aborted due to shutdown.")
        _ <- queue.flush()
        _ <- flush0
      } yield {
        flush0.isCompleted shouldBe true
      }
    }

    "not run new tasks if the queue is shutdown" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")

      val task1Result = queue.executeUS(task1.run(), "Task1")

      task1.complete()

      for {
        task1Res <- task1Result.unwrap
        _ = queue.close()
        task2Result = queue.executeUS(task2.run(), "Task2")
        task2res <- task2Result.unwrap
      } yield {
        task1.started.get() shouldBe true
        task1Res shouldBe UnlessShutdown.Outcome("task1")
        task2.started.get() shouldBe false
        task2res shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "not run queued tasks if the queue is shutdown" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")

      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")

      val closed = Future(queue.close())

      // Make sure to wait for the close call to be scheduled before completing task 1
      eventually() {
        queue.isClosing shouldBe true
      }

      task1.complete()

      for {
        task1Res <- task1Result.unwrap
        task2Res <- task2Result.unwrap
        _ <- closed
      } yield {
        task2Res shouldBe AbortedDueToShutdown
        task2.started.get() shouldBe false
        task1Res shouldBe Outcome("task1")
      }
    }
  }

  private def stopAfterFailureTests(
      mk: () => SimpleExecutionQueue
  ): Unit = {
    "not run a future in case of a previous failure" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task4 = new MockTask("task4")
      val task1Result = queue.executeUS(task1.run(), "Task1").failOnShutdown
      val task2Result = queue.executeUS(task2.run(), "Task2").failOnShutdown
      val task3Result = queue.executeUS(task3.run(), "Task3").failOnShutdown
      val task4Result = queue.executeUS(task4.run(), "Task4").failOnShutdown

      failTask(task1, Seq("Task2", "Task3", "Task4"))

      task3.complete()

      // Propagated to all subsequent tasks
      val expectedFailure = "mocked failure for task1"

      for {
        task2Res <- task2Result.failed
        _ = task2.started.get() shouldBe false
        _ = task2Res.getMessage shouldBe expectedFailure
        task4Res <- task4Result.failed
        _ = task4.started.get() shouldBe false
        _ = task4Res.getMessage shouldBe expectedFailure
        task1Res <- task1Result.failed
        _ = task1Res.getMessage shouldBe expectedFailure
        task3Res <- task3Result.failed
      } yield {
        task3Res.getMessage shouldBe expectedFailure
      }
    }

    "correctly propagate failures" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")

      failTask(task1, Seq("Task2", "Task3"))

      task2.complete()
      for {
        task1Res <- task1Result.failed.failOnShutdown("aborted due to shutdown.")
        task2Res <- task2Result.failed.failOnShutdown("aborted due to shutdown.")
        task3Res <- task3Result.failed.failOnShutdown("aborted due to shutdown.")
      } yield {
        task1Res.getMessage shouldBe "mocked failure for task1"
        task2Res.getMessage shouldBe "mocked failure for task1"
        task3Res.getMessage shouldBe "mocked failure for task1"
      }
    }

    "not run follow-up tasks if a task has been shutdown" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")

      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")

      task1.complete()
      shutdownTask(task2, Seq("Task3"))

      for {
        task1res <- task1Result.unwrap
        task2res <- task2Result.unwrap
        task3res <- task3Result.unwrap
      } yield {
        task1.started.get() shouldBe true
        task1res shouldBe UnlessShutdown.Outcome("task1")
        task2.started.get() shouldBe true
        task2res shouldBe UnlessShutdown.AbortedDueToShutdown
        task3.started.get() shouldBe false
        task3res shouldBe UnlessShutdown.AbortedDueToShutdown
      }
    }

    "list the outstanding tasks" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task4 = new MockTask("task4")

      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")
      val task4Result = queue.executeUS(task4.run(), "Task4")

      val queue0 = queue.queued
      task1.complete()
      val queue1 = queue.queued
      failTask(task2, Seq("Task3", "Task4"))

      for {
        _ <- task1Result.failOnShutdown
        queue2 = queue.queued
        _ = task3.complete()
        _ <- task2Result.failed.failOnShutdown("aborted due to shutdown.")
        _ <- task3Result.failed.failOnShutdown("aborted due to shutdown.")
        queue3 = queue.queued
        _ = task4.complete()
        _ <- task4Result.failed.failOnShutdown("aborted due to shutdown.")
        queue4 = queue.queued
      } yield {
        queue0 shouldBe Seq("sentinel (completed)", "Task1", "Task2", "Task3", "Task4")
        queue1 shouldBe Seq("Task1 (completed)", "Task2", "Task3", "Task4")

        // After task2 failure, all tasks removed from the queue
        queue2 shouldBe Seq("Task4 (completed)")
        queue3 shouldBe Seq("Task4 (completed)")
        queue4 shouldBe Seq("Task4 (completed)")
      }
    }

    "complete subsequent tasks with shutdown even if the currently running task does not close" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")

      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")

      val closed = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        Future(queue.close()),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include(
                "Task closing simple-exec-queue: test-queue did not complete within 1 second"
              ),
              "missing queue closing timeout message",
            ),
            (
              _.warningMessage should include(
                "Closing 'AsyncCloseable(name=simple-exec-queue: test-queue)' failed"
              ),
              "missing lifecycle closing error message",
            ),
            (
              _.warningMessage should include(
                "Forcibly completing Task2 with AbortedDueToShutdown"
              ),
              "missing task 2 shutdown",
            ),
            (
              _.warningMessage should include(
                "Forcibly completing Task3 with AbortedDueToShutdown"
              ),
              "missing task 3 shutdown",
            ),
          )
        ),
      )

      eventually() {
        closed.isCompleted shouldBe true
      }

      for {
        task2Res <- task2Result.unwrap
        task3Res <- task3Result.unwrap
      } yield {
        task3Res shouldBe AbortedDueToShutdown
        task2Res shouldBe AbortedDueToShutdown
        // The running task is not completed
        task1Result.unwrap.isCompleted shouldBe false
        // After we shut it down it should eventually complete though
        task1.shutdown()
        eventually() {
          task1Result.unwrap.futureValue shouldBe AbortedDueToShutdown
        }
      }
    }

  }

  private def continueAfterFailureTests(
      mk: () => SimpleExecutionQueue
  ): Unit = {
    "not propagate failures" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")

      failTask(task1, Seq())

      task2.complete()
      task3.complete()
      for {
        task1Res <- task1Result.failed.failOnShutdown("aborted due to shutdown.")
        task2Res <- task2Result.failOnShutdown("aborted due to shutdown.")
        task3Res <- task3Result.failOnShutdown("aborted due to shutdown.")
      } yield {
        task1Res.getMessage shouldBe "mocked failure for task1"
        task2Res shouldBe "task2"
        task3Res shouldBe "task3"
      }
    }

    "run follow-up tasks if a task has been shutdown" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")

      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")

      task1.complete()
      task2.shutdown()
      task3.complete()

      for {
        task1res <- task1Result.unwrap
        task2res <- task2Result.unwrap
        task3res <- task3Result.unwrap
      } yield {
        task1.started.get() shouldBe true
        task1res shouldBe UnlessShutdown.Outcome("task1")
        task2.started.get() shouldBe true
        task2res shouldBe UnlessShutdown.AbortedDueToShutdown
        task3.started.get() shouldBe true
        task3res shouldBe UnlessShutdown.Outcome("task3")
      }
    }

    "list the outstanding tasks" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")
      val task4 = new MockTask("task4")

      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")
      val task4Result = queue.executeUS(task4.run(), "Task4")

      val queue0 = queue.queued
      task1.complete()
      val queue1 = queue.queued
      failTask(task2, Seq.empty)

      for {
        _ <- task1Result.failOnShutdown
        queue2 = queue.queued
        _ = task3.complete()
        _ <- task2Result.failed.failOnShutdown
        _ <- task3Result.failOnShutdown
        queue3 = queue.queued
        _ = task4.complete()
        _ <- task4Result.failOnShutdown
        queue4 = queue.queued
      } yield {
        queue0 shouldBe Seq("sentinel (completed)", "Task1", "Task2", "Task3", "Task4")
        queue1 shouldBe Seq("Task1 (completed)", "Task2", "Task3", "Task4")

        // After task2 failure, all tasks removed from the queue
        queue2 shouldBe Seq("Task2 (completed)", "Task3", "Task4")
        queue3 shouldBe Seq("Task3 (completed)", "Task4")
        queue4 shouldBe Seq("Task4 (completed)")
      }
    }

    "complete subsequent tasks with shutdown even if the currently running task does not close" in {
      val queue = mk()
      val task1 = new MockTask("task1")
      val task2 = new MockTask("task2")
      val task3 = new MockTask("task3")

      val task1Result = queue.executeUS(task1.run(), "Task1")
      val task2Result = queue.executeUS(task2.run(), "Task2")
      val task3Result = queue.executeUS(task3.run(), "Task3")

      val closed = loggerFactory.assertLoggedWarningsAndErrorsSeq(
        Future(queue.close()),
        LogEntry.assertLogSeq(
          Seq(
            (
              _.warningMessage should include(
                "Task closing simple-exec-queue: test-queue did not complete within 1 second"
              ),
              "missing queue closing timeout message",
            ),
            (
              _.warningMessage should include(
                "Closing 'AsyncCloseable(name=simple-exec-queue: test-queue)' failed"
              ),
              "missing lifecycle closing error message",
            ),
            (
              _.warningMessage should include(
                "Forcibly completing Task2 with AbortedDueToShutdown"
              ),
              "missing task 2 shutdown",
            ),
            (
              _.warningMessage should include(
                "Forcibly completing Task3 with AbortedDueToShutdown"
              ),
              "missing task 3 shutdown",
            ),
          )
        ),
      )

      eventually() {
        closed.isCompleted shouldBe true
      }

      for {
        task2Res <- task2Result.unwrap
        task3Res <- task3Result.unwrap
      } yield {
        task3Res shouldBe AbortedDueToShutdown
        task2Res shouldBe AbortedDueToShutdown
        // The running task is not completed
        task1Result.unwrap.isCompleted shouldBe false
        // After we shut it down it should eventually complete though
        task1.shutdown()
        eventually() {
          task1Result.unwrap.futureValue shouldBe AbortedDueToShutdown
        }
      }
    }

  }

  "SimpleExecutionQueueWithShutdown" when {
    "not logging task timing" should {
      val factory = () =>
        new SimpleExecutionQueue(
          "test-queue",
          futureSupervisor,
          queueTimeouts,
          loggerFactory,
          logTaskTiming = false,
          failureMode = StopAfterFailure,
        )

      behave like simpleExecutionQueueTests(factory)
      behave like stopAfterFailureTests(factory)
    }

    "logging task timingWithShutdown" should {
      val factory = () =>
        new SimpleExecutionQueue(
          "test-queue",
          futureSupervisor,
          queueTimeouts,
          loggerFactory,
          logTaskTiming = true,
          failureMode = StopAfterFailure,
        )

      behave like simpleExecutionQueueTests(factory)
      behave like stopAfterFailureTests(factory)
    }

    "in ContinueAfterFailure mode" should {
      val factory = () =>
        new SimpleExecutionQueue(
          "test-queue",
          futureSupervisor,
          queueTimeouts,
          loggerFactory,
          logTaskTiming = true,
          failureMode = ContinueAfterFailure,
        )

      behave like simpleExecutionQueueTests(factory)
      behave like continueAfterFailureTests(factory)
    }
  }
}
