// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.concurrent

import cats.Monad
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.{BaseTest, BaseTestWordSpec}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ForkJoinPool, LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}
import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}

trait IdlenessExecutorServiceTest extends BaseTest { this: AnyWordSpec =>

  def awaitIdleness(mk: () => ExecutionContextIdlenessExecutorService): Unit = {
    "return after the futures are completed" in {
      import IdlenessExecutorServiceTest.busyWait

      implicit val ec: ExecutionContextIdlenessExecutorService = mk()

      val waitMillis = 100.milliseconds

      val promise = Promise[Unit]()
      val future = for {
        _ <- promise.future
        _ <- Future { busyWait(waitMillis) }
      } yield ()
      promise.completeWith(Future {
        busyWait(waitMillis)
      })

      val start = Deadline.now
      val terminated = ec.awaitIdleness(waitMillis * 100)
      val end = Deadline.now
      assume(
        future.isCompleted,
        "The future has failed to complete",
      ) // Cancel test if future fails to complete for some reason.
      assert(terminated, "awaitIdleness failed to detect that the execution context is idle.")
      withClue("awaitIdleness has returned too late") {
        end - start should be < waitMillis * 20
      }

      loggerFactory.suppressWarningsAndErrors {
        ec.shutdown()
      }
    }

    "return after the given timeout" in {

      import IdlenessExecutorServiceTest.busyWait

      implicit val ec: ExecutionContextIdlenessExecutorService = mk()

      val running = new AtomicBoolean(true)

      // Periodically schedule new tasks
      val waitMillis = 1.millisecond
      val future = Monad[Future].tailRecM(()) { _ =>
        Future[Either[Unit, Unit]] {
          busyWait(waitMillis)
          if (Thread.interrupted) {
            Thread.currentThread.interrupt()
            Right(())
          } else if (!running.get()) {
            Right(())
          } else {
            Left(())
          }
        }
      }

      val start = Deadline.now
      val terminated = ec.awaitIdleness(1.millisecond)
      val end = Deadline.now
      assert(!future.isCompleted, "The future has terminated unexpectedly")
      assert(!terminated, "awaitIdleness incorrectly indicates that the execution context is idle")
      withClue("awaitIdleness has returned too late") {
        end - start should be < 2.seconds
      }

      running.set(false)
      loggerFactory.suppressWarningsAndErrors {
        ExecutorServiceExtensions(ec)(logger, DefaultProcessingTimeouts.testing).close()
      }

    }

  }
}

object IdlenessExecutorServiceTest {

  def busyWait(duration: FiniteDuration): Unit = {
    val deadline = duration.fromNow

    @tailrec def go(): Unit =
      if (deadline.isOverdue()) ()
      else if (Thread.interrupted()) {
        Thread.currentThread.interrupt()
        ()
      } else go()
    go()
  }
}

class ForkJoinIdlenessExecutorServiceTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with IdlenessExecutorServiceTest {

  "awaitIdleness" should {
    behave like awaitIdleness { () =>
      val pool = new ForkJoinPool()
      new ForkJoinIdlenessExecutorService(
        pool,
        pool,
        throwable => logger.error(s"Error: $throwable"),
        loggerFactory.threadName + "-fork-join-pool",
      )
    }
  }

}

class ThreadPoolIdlenessExecutorServiceTest
    extends AnyWordSpec
    with BaseTestWordSpec
    with IdlenessExecutorServiceTest {

  "awaitIdleness" should {
    behave like awaitIdleness { () =>
      val pool =
        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable]())
      new ThreadPoolIdlenessExecutorService(
        pool,
        throwable => logger.error(s"Error: $throwable"),
        loggerFactory.threadName + "-thread-pool-executor",
      )
    }
  }
}
