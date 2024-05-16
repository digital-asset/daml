// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.lifecycle

import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.StartAndCloseable.StartAfterClose
import com.digitalasset.canton.logging.TracedLogger
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise, blocking}

class StartAndCloseableTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  private class Fixture(
      startTimeout: config.NonNegativeFiniteDuration = config.NonNegativeFiniteDuration(1.minute)
  ) extends StartAndCloseable[Unit] {

    override protected def timeouts: ProcessingTimeout = DefaultProcessingTimeouts.testing

    val started: Promise[Unit] = Promise()
    val closed: Promise[Unit] = Promise()

    val startInvokedP: Promise[Unit] = Promise()
    val closingInvokedP: Promise[Unit] = Promise()
    val waitingInvokedP: Promise[Unit] = Promise()

    private val startInvocations = new AtomicInteger(0)
    private val closeInvocations = new AtomicInteger(0)

    // we can use a short polling time to decrease test latency as we aren't running any real tasks
    override protected def maxSleepMillis: Long = 10

    // listen on isClosed flag
    override protected def runStateChanged(waitingState: Boolean = false): Unit = {
      if (isClosing) {
        closingInvokedP.trySuccess(())
      }
      if (isStarted) {
        startInvokedP.trySuccess(())
      }
      if (waitingState) {
        waitingInvokedP.trySuccess(())
      }
    }

    def closeF(): Future[Unit] = {
      Future {
        blocking {
          close()
        }
      }
    }

    override protected def startAsync()(implicit
        initializationTraceContext: TraceContext
    ): Future[Unit] = {
      startInvocations.incrementAndGet()
      started.future
    }
    override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
      Seq(
        AsyncCloseable(
          "test", {
            closeInvocations.incrementAndGet()
            // If start ever got invoked, test for mutual exclusion of the two
            if (startInvocations.get() > 0) assert(started.isCompleted)
            closed.future
          },
          startTimeout,
        )
      )
    override protected def logger: TracedLogger = StartAndCloseableTest.this.logger

    def evaluate(
        checkStarted: Boolean,
        checkClosed: Boolean,
        expectedStartInvocations: Int,
        expectedCloseInvocations: Int,
    ): Assertion = {
      isClosing shouldBe checkClosed
      isStarted shouldBe checkStarted
      startInvocations.get() shouldBe expectedStartInvocations
      closeInvocations.get() shouldBe expectedCloseInvocations
    }
  }

  "start and closeable" should { // deal with

    "start begin, start done, close begin, close done" in {
      val f = new Fixture()
      f.started.success(())
      f.closed.success(())
      (for {
        _ <- f.start()
      } yield {
        f.close()
        f.evaluate(checkStarted = true, checkClosed = true, 1, 1)
      }).futureValue
    }

    "start begin, close begin, start done, close done" in {
      val f = new Fixture()
      val fs = f.start()
      // start close as soon as start has actually started
      val fc = f.startInvokedP.future.flatMap(_ =>
        Future {
          blocking {
            f.close()
          }
        }
      )
      // Only finish the starting future if closing starts to wait for it, to test for mutual
      // exclusion of start and close. This hangs the test if we got the synchronization wrong!
      val waitingF = f.waitingInvokedP.future.map { _ =>
        f.started.trySuccess(())
        f.closed.trySuccess(())
      }
      (for {
        _ <- fs
        _ <- fc
      } yield {
        f.evaluate(checkStarted = true, checkClosed = true, 1, 1)
      }).futureValue
      waitingF.futureValue
    }

    "close begin, close done, start begin, start done" in {
      val f = new Fixture()
      f.closed.success(())
      f.started.success(())
      val fc = f.closeF()
      val fs = f.closingInvokedP.future.flatMap(_ => f.start())
      (for {
        failure <- fs.failed
        _ <- fc
      } yield {
        f.evaluate(checkStarted = true, checkClosed = true, 0, 1)
        failure shouldBe a[StartAfterClose]
      }).futureValue
    }

    "close begin, start begin, close done, start done" in {
      val f = new Fixture()
      val fc = f.closeF()
      val fs = f.closingInvokedP.future.flatMap { _ =>
        f.start()
      }
      val startF = f.startInvokedP.future.map { _ =>
        f.closed.success(())
        f.started.success(())
      }
      (for {
        failure <- fs.failed
        _ <- fc
      } yield {
        f.evaluate(checkStarted = true, checkClosed = true, 0, 1)
        failure shouldBe a[StartAfterClose]
      }).futureValue
      startF.futureValue
    }

    "multiple starts" in {
      val f = new Fixture()
      f.closed.success(())
      val fs = f.start()
      val fs2 = f.start()
      val startF = f.startInvokedP.future.map(_ => f.started.success(()))
      (for {
        _ <- fs
        _ <- fs2
      } yield {
        f.evaluate(checkStarted = true, checkClosed = false, 1, 0)
        f.close()
        f.evaluate(checkStarted = true, checkClosed = true, 1, 1)
      }).futureValue
      startF.futureValue
    }

    "multiple closes" in {
      val f = new Fixture()
      f.start().discard
      f.started.success(())
      val fc = f.closeF()
      val fc2 = f.closeF()
      val closeF = f.closingInvokedP.future.map(_ => f.closed.success(()))
      (for {
        _ <- fc
        _ <- fc2
      } yield {
        f.evaluate(checkStarted = true, checkClosed = true, 1, 1)
      }).futureValue
      closeF.futureValue
    }
  }
}
