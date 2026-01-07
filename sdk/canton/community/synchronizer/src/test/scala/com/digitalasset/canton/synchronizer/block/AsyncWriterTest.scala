// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block

import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{PromiseUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.FutureOutcome
import org.scalatest.wordspec.FixtureAsyncWordSpecLike

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}

class AsyncWriterTest
    extends FixtureAsyncWordSpecLike
    with BaseTest
    with HasTestCloseContext
    with HasExecutionContext {

  private[block] class Fixture {
    val counter = new AtomicInteger(0)
    val written = new AtomicReference[Vector[Int]](Vector.empty)
    val writeStartPUS = (0 until 3).map(_ => PromiseUnlessShutdown.unsupervised[Unit]())
    val writeEndPUS = (0 until 3).map(_ => PromiseUnlessShutdown.unsupervised[Unit]())
    val writer = new AsyncWriter[Vector[Int]](
      addToQueue = (a, b) => a ++ b,
      writeQueue = x => {
        written.getAndUpdate(_ ++ x).discard
        val cc = counter.getAndIncrement()
        // mark the next promise as completed so we can sync the test
        writeStartPUS(cc).success(UnlessShutdown.unit)
        writeEndPUS(cc).futureUS
      },
      Vector.empty,
      "test",
      futureSupervisor,
      loggerFactory,
    ) {
      override protected def recordWriteError(name: String, exception: Throwable): Unit =
        ??? // tested in BlockSequencerStateAsyncWriterTest / escalate errors
    }
  }
  override protected type FixtureParam = Fixture

  override def withFixture(test: OneArgAsyncTest): FutureOutcome =
    super.withFixture(test.toNoArgAsyncTest(new Fixture(): FixtureParam))

  "async writer" should {
    "start writing if idle" in { fixture =>
      import fixture.*
      val res1 = writer.appendAndSchedule(Vector(1))
      writeEndPUS(0).success(UnlessShutdown.unit)
      (for {
        _ <- writeStartPUS(0).futureUS
      } yield {
        res1.queueSize shouldBe 0
        written.get() shouldBe Vector(1)
      }).failOnShutdown
    }
    "queue while writing and make sure subsequent queue is picked up after write" in { fixture =>
      import fixture.*
      writer.appendAndSchedule(Vector(1))
      val res2 = writer.appendAndSchedule(Vector(2))
      (for {
        // wait for first write to start
        _ <- writeStartPUS(0).futureUS
        _ = { written.get() shouldBe Vector(1) }
        // complete first write and wait for the second write to start
        _ = { writeEndPUS(0).success(UnlessShutdown.unit) }
        _ <- writeStartPUS(1).futureUS
      } yield {
        res2.queueSize shouldBe 1
        written.get() shouldBe Vector(1, 2)
      }).failOnShutdown
    }
    "go back to idle after write" in { fixture =>
      import fixture.*
      writer.appendAndSchedule(Vector(1))
      (for {
        // wait for first write to start
        _ <- writeStartPUS(0).futureUS
        _ = { written.get() shouldBe Vector(1) }
        // complete first write and wait for the second write to start
        _ = { writeEndPUS(0).success(UnlessShutdown.unit) }
        // start next write
        _ = {
          writer.appendAndSchedule(Vector(2))
          writeEndPUS(1).success(UnlessShutdown.unit)
        }
        // wait until second write started
        _ <- writeStartPUS(1).futureUS
      } yield {
        written.get() shouldBe Vector(1, 2)
      }).failOnShutdown
    }

  }

}
