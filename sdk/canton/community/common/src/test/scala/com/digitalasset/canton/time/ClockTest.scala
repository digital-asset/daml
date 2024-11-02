// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.time

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{DefaultProcessingTimeouts, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, SuppressionRule}
import com.digitalasset.canton.time.Clock.SystemClockRunningBackwards
import com.digitalasset.canton.topology.admin.v30.IdentityInitializationServiceGrpc.IdentityInitializationService
import com.digitalasset.canton.topology.admin.v30.{
  CurrentTimeRequest,
  CurrentTimeResponse,
  IdentityInitializationServiceGrpc,
}
import com.digitalasset.canton.tracing.TraceContextGrpc
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, config}
import io.grpc.inprocess.{InProcessChannelBuilder, InProcessServerBuilder}
import io.grpc.util.MutableHandlerRegistry
import io.grpc.{ManagedChannel, Server, ServerServiceDefinition}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

import java.time.{Clock as JClock, Instant}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.annotation.tailrec
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future, Promise}

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
          s"task $index did not complete",
        )
      }
    }

  }

  "TickTock Skew" should {

    "never run back in time" in {

      val tick = new TickTock.RandomSkew(10)
      def check(last: Instant, runs: Int): Unit =
        if (runs > 0) {
          val cur = tick.now
          assert(cur.isAfter(last) || cur == last)
          Threading.sleep(0, 1000)
          check(cur, runs - 1)
        }
      check(tick.now, 300)
    }

    "not be in sync with the normal clock" in {
      val tick = new TickTock.RandomSkew(10)
      val tm = JClock.systemUTC()

      val notinsync = new AtomicInteger(0)
      def check(runs: Int): Unit =
        if (runs > 0) {
          val cur = tm.instant()
          val nw = tick.now
          if (nw != cur) {
            notinsync.updateAndGet(x => x + 1)
          }
          Threading.sleep(0, 100000)
          check(runs - 1)
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

  "RemoteClock" should {
    "request the time" in {
      val service = mock[IdentityInitializationService]
      // We must mock the service before we create the clock, because the clock will immediately request a time
      val ts = CantonTimestamp.ofEpochSecond(42)
      when(service.currentTime(any[CurrentTimeRequest]))
        .thenReturn(Future.successful(CurrentTimeResponse(ts.toProtoPrimitive)))

      val env = new RemoteClockEnv(service)

      env.clock.now shouldBe ts
      env.close()
    }

    "handle network delays" in {
      val service = mock[IdentityInitializationService]
      when(service.currentTime(any[CurrentTimeRequest]))
        .thenReturn(Future.successful(CurrentTimeResponse(0L)))
      val networkTimeout = config.NonNegativeDuration.tryFromDuration(1.second)
      val env = new RemoteClockEnv(
        service,
        timeouts = timeouts.copy(network = networkTimeout),
      )

      // Delay the response for more than the network timeout
      val firstAnswer = Promise[CantonTimestamp]()
      val firstRequestObserved = Promise[Unit]()
      when(service.currentTime(any[CurrentTimeRequest])).thenAnswer { (_: CurrentTimeRequest) =>
        firstRequestObserved.trySuccess(())
        firstAnswer.future.map(ts => CurrentTimeResponse(ts.toProtoPrimitive))
      }

      val ts = CantonTimestamp.ofEpochMilli(1)
      loggerFactory.assertLogs(
        {
          val taskF = Future(env.clock.scheduleAt(_ => (), ts))

          logger.info("Waiting to see the first request being observed")
          firstRequestObserved.future.futureValue
          logger.info("Now waiting for the network delay duration")
          Threading.sleep(networkTimeout.duration.toMillis + 100)
          logger.info("Now time requests are fast again")
          firstAnswer.success(ts)

          taskF.futureValue.futureValueUS
        },
        _.warningMessage should include("DEADLINE_EXCEEDED"),
      )

      env.close()
    }

    "handle out-of-order responses" in {
      val service = mock[IdentityInitializationService]
      when(service.currentTime(any[CurrentTimeRequest]))
        .thenReturn(Future.successful(CurrentTimeResponse(0L)))

      val env = new RemoteClockEnv(service)

      val firstAnswer = Promise[CantonTimestamp]()
      val firstRequestObserved = Promise[Unit]()

      val ts1 = CantonTimestamp.ofEpochSecond(17)
      val ts2 = CantonTimestamp.ofEpochSecond(23)

      when(service.currentTime(any[CurrentTimeRequest])).thenAnswer { (_: CurrentTimeRequest) =>
        firstRequestObserved.trySuccess(())
        firstAnswer.future.map(ts => CurrentTimeResponse(ts.toProtoPrimitive))
      }

      val firstF = Future {
        env.clock.now
      }
      firstRequestObserved.future.futureValue

      when(service.currentTime(any[CurrentTimeRequest])).thenReturn(
        Future.successful(CurrentTimeResponse(ts2.toProtoPrimitive))
      )

      val second = env.clock.now

      firstAnswer.success(ts1)

      val first = firstF.futureValue

      second shouldBe ts2
      first shouldBe ts1

      env.close()
    }

    "handle gRPC errors" in {
      val service = mock[IdentityInitializationService]
      when(service.currentTime(any[CurrentTimeRequest]))
        .thenReturn(Future.successful(CurrentTimeResponse(0L)))
      val env = new RemoteClockEnv(service)

      val ts = CantonTimestamp.ofEpochMilli(1)
      val taskF = env.clock.scheduleAt(_ => (), ts)

      // Return some random gRPC errors first, then a time
      val err = DeprecatedProtocolVersion
        .WarnParticipant(InstanceName.tryCreate("foo"), None)
        .asGrpcError
      loggerFactory.assertLogs(
        {
          when(service.currentTime(any[CurrentTimeRequest]))
            .thenReturn(
              Future.failed(err),
              Future.failed(err),
              Future.successful(CurrentTimeResponse(ts.toProtoPrimitive)),
            )

          taskF.futureValueUS
        },
        _.errorMessage should include("Request failed for remote clock server"),
        _.errorMessage should include("Request failed for remote clock server"),
      )

      env.close()
    }

    "handle concurrent close gracefully" in {
      val service = mock[IdentityInitializationService]
      when(service.currentTime(any[CurrentTimeRequest]))
        .thenReturn(Future.successful(CurrentTimeResponse(0L)))
      val env = new RemoteClockEnv(service)

      val firstAnswer = Promise[CantonTimestamp]()
      val firstRequestObserved = Promise[Unit]()

      when(service.currentTime(any[CurrentTimeRequest])).thenAnswer { (_: CurrentTimeRequest) =>
        firstRequestObserved.trySuccess(())
        firstAnswer.future.map(ts => CurrentTimeResponse(ts.toProtoPrimitive))
      }

      val nowF = Future(env.clock.now)

      firstRequestObserved.future.futureValue
      loggerFactory.assertLogs(
        {
          env.close()
          nowF.futureValue shouldBe CantonTimestamp.MinValue.immediateSuccessor
        },
        // TODO(#21278): Ensure that shutdown works as expected
        _.warningMessage should include("shutdown did not complete gracefully in allotted"),
      )
    }
  }

  private class RemoteClockEnv(
      val service: IdentityInitializationService,
      timeouts: ProcessingTimeout = ClockTest.this.timeouts,
  ) {
    val channelName: String = InProcessServerBuilder.generateName()

    val registry: MutableHandlerRegistry = new MutableHandlerRegistry

    val server: Server = InProcessServerBuilder
      .forName(channelName)
      .fallbackHandlerRegistry(registry)
      .build()

    server.start()

    val clockServiceDefinition: ServerServiceDefinition =
      io.grpc.ServerInterceptors.intercept(
        IdentityInitializationServiceGrpc.bindService(service, parallelExecutionContext),
        TraceContextGrpc.serverInterceptor,
      )

    registry.addService(clockServiceDefinition)

    val channel: ManagedChannel = InProcessChannelBuilder
      .forName(channelName)
      .intercept(TraceContextGrpc.clientInterceptor)
      .build()

    val clock = new RemoteClock(channel, timeouts, loggerFactory)

    def close(): Unit = {
      clock.close() // This closes the channel too

      server.shutdown()
      server.awaitTermination()
    }
  }
}
