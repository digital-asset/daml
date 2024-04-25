// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.time.Clock.SystemClockRunningBackwards
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.time.{Clock as JClock, Instant}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
class ClockTest extends AnyWordSpec with BaseTest with HasExecutionContext {

  val sut = new WallClock(DefaultProcessingTimeouts.testing, loggerFactory)
  val timeout = Timeout(6.seconds)
  def testTask(now: CantonTimestamp): Unit = ()

  "SimClock" should {
    val ref = CantonTimestamp.Epoch
    val sim = new SimClock(ref, loggerFactory)

    def testExecution(task: Future[Unit], timestamp: CantonTimestamp): Unit = {
      Threading.sleep(
        25
      ) // this will only lead to failures if there is something wrong with the scheduling
      assert(!task.isCompleted)
      // execute task
      sim.advanceTo(timestamp)
      val res = Await.ready(task, timeout.value)
      assert(res.isCompleted)
    }

    "return correct time" in {
      assert(sim.uniqueTime().isAfter(ref))
      assert(sim.uniqueTime().isBefore(ref.plusMillis(1)))
    }

    "allow task to be scheduled in future" in {
      val task1 = sim.scheduleAt(testTask(_), sim.uniqueTime().plusSeconds(1)).onShutdown(fail())
      testExecution(task1, sim.uniqueTime().plusSeconds(2))
    }

    "ensure that tasks are executed in proper order" in {
      val now = sim.uniqueTime()
      val task1 = sim.scheduleAt(testTask(_), now.plusSeconds(3)).onShutdown(fail())
      val task2 = sim.scheduleAt(testTask(_), now.plusSeconds(1)).onShutdown(fail())
      testExecution(task2, now.plusSeconds(2))
      testExecution(task1, now.plusSeconds(4))
    }

    "ensure that multiple tasks are executed, again in proper order" in {
      val now = sim.uniqueTime()
      val task1 = sim.scheduleAt(testTask(_), now.plusSeconds(5)).onShutdown(fail())
      val task2 = sim.scheduleAt(testTask(_), now.plusSeconds(3)).onShutdown(fail())
      val task3 = sim.scheduleAt(testTask(_), now.plusSeconds(1)).onShutdown(fail())
      testExecution(task3, now.plusSeconds(4))
      val res = Await.ready(task2, timeout.value)
      assert(res.isCompleted)
      testExecution(task1, now.plusSeconds(6))
    }

    "ensure that a task scheduled for now executes" in {
      val now = sim.uniqueTime()
      val task = sim.scheduleAt(testTask(_), now).onShutdown(fail())
      val res = Await.ready(task, timeout.value)
      assert(res.isCompleted)
    }

  }

  "WallClock" should {
    "return unique values without diverging from real time too much" in {
      val first = sut.uniqueTime()
      var last = first
      for (i <- 1 to 100000) {
        last = sut.uniqueTime()
      }
      assert(first.isBefore(last))
      // make sure that in a tight loop after 100k tries the system clock returns
      // unique values at least twice, and we're not just adding nanoseconds to the previous value,
      // eventually overlowing
      assert(last.isAfter(first.plusMillis(1)))
    }

    "check that clock unique and monotonic are strict" in {
      var last = sut.uniqueTime()
      for (i <- 1 to 1000) {
        val mt = sut.monotonicTime()
        val ut = sut.uniqueTime()
        mt shouldBe >=(last)
        ut shouldBe >(mt)
        last = ut
      }
    }

    "perform 100'000 currentTime in a second" in {
      // avoid flakes by trying a few times to run in less than one second
      // in some tests this gets flaky
      @tailrec
      def doCheck(num: Int): Unit = {
        val first = sut.uniqueTime()
        var last = first
        for (i <- 1 to 100000) {
          last = sut.uniqueTime()
        }
        if (last.isBefore(first.plusSeconds(1))) ()
        else {
          if (num > 0) {
            doCheck(num - 1)
          } else {
            fail(s"$last < ${first.plusSeconds(1)}")
          }
        }
      }
      doCheck(10)
    }

    "scheduling one task works and completes" in {
      val now = sut.uniqueTime()
      val task1 = sut.scheduleAt(testTask(_), now.plusMillis(50)).onShutdown(fail())
      assert(Await.ready(task1, timeout.value).isCompleted)
    }

    "scheduling of three tasks works and completes" in {
      val now = sut.uniqueTime()
      val tasks = Seq(
        sut.scheduleAt(testTask, now.plusMillis(50)),
        sut.scheduleAt(testTask, now.plusMillis(20)),
        sut.scheduleAt(testTask, now),
        sut.scheduleAt(testTask, now.minusMillis(1000)),
        sut.scheduleAfter(testTask, java.time.Duration.ofMillis(55)),
      )
      tasks.zipWithIndex.foreach { case (task, index) =>
        assert(
          Await.ready(task.onShutdown(fail()), timeout.value).isCompleted,
          s"task ${index} did not complete",
        )
      }
    }

  }

  "TickTock Skew" should {

    "never run back in time" in {

      val tick = new TickTock.RandomSkew(10)
      def check(last: Instant, runs: Int): Unit = {
        if (runs > 0) {
          val cur = tick.now
          assert(cur.isAfter(last) || cur == last)
          Threading.sleep(0, 1000)
          check(cur, runs - 1)
        }
      }
      check(tick.now, 300)
    }

    "not be in sync with the normal clock" in {
      val tick = new TickTock.RandomSkew(10)
      val tm = JClock.systemUTC()

      val notinsync = new AtomicInteger(0)
      def check(runs: Int): Unit = {
        if (runs > 0) {
          val cur = tm.instant()
          val nw = tick.now
          if (nw != cur) {
            notinsync.updateAndGet(x => x + 1)
          }
          Threading.sleep(0, 100000)
          check(runs - 1)
        }
      }
      check(100)
      assert(notinsync.get() > 0)
    }

  }

  "Clock" should {
    "warn but work if clock is adjusted backwards" in {
      class MyClock(val loggerFactory: NamedLoggerFactory) extends Clock {
        val nowR = new AtomicReference[CantonTimestamp](CantonTimestamp.Epoch.plusSeconds(120))
        override protected def addToQueue(queue: Queued[?]): Unit = {
          val _ = tasks.add(queue)
        }

        override protected def warnIfClockRunsBackwards: Boolean = true
        override def now: CantonTimestamp = nowR.get()

        override def close(): Unit = {}
      }

      val clock = new MyClock(loggerFactory)

      def assertNoWarning(ut: CantonTimestamp) = {
        val ut2 = loggerFactory.assertLogsSeq(SuppressionRule.Level(Level.WARN))(
          clock.uniqueTime(),
          x => x shouldBe empty,
        )
        ut should be < ut2
        ut2
      }

      val ut = clock.uniqueTime()
      clock.nowR.updateAndGet(_.minusSeconds(5))
      // first time, we do get a message
      val ut2 =
        loggerFactory.assertLogs(
          clock.uniqueTime(),
          _.shouldBeCantonErrorCode(SystemClockRunningBackwards),
        )
      ut should be < ut2
      // second time, we shouldn't get one
      val ut3 = assertNoWarning(ut2)
      // recover
      clock.nowR.updateAndGet(_.plusSeconds(40))
      val ut4 = assertNoWarning(ut3)
      // emit again
      clock.nowR.updateAndGet(_.minusSeconds(5))
      val ut5 =
        loggerFactory.assertLogs(
          clock.uniqueTime(),
          _.shouldBeCantonErrorCode(SystemClockRunningBackwards),
        )
      ut4 should be < ut5
    }
  }

}
