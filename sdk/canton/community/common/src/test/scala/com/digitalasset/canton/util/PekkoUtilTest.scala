// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.Eq
import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.BaseTestWordSpec
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.UnlessShutdown.{AbortedDueToShutdown, Outcome}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.PekkoUtil.{
  ContextualizedFlowOps,
  FutureQueue,
  FutureQueueConsumer,
  FutureQueuePullProxy,
  IndexingFutureQueue,
  PekkoSourceQueueToFutureQueue,
  RecoveringFutureQueueImpl,
  RecoveringQueueMetrics,
  WithKillSwitch,
  noOpKillSwitch,
}
import org.apache.pekko.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.pekko.stream.testkit.StreamSpec
import org.apache.pekko.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, OverflowStrategy}
import org.apache.pekko.{Done, NotUsed}
import org.scalacheck.Arbitrary
import org.scalatest.concurrent.PatienceConfiguration
import org.scalatest.time.Span

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.List
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random
import scala.util.control.NonFatal

class PekkoUtilTest extends StreamSpec with BaseTestWordSpec {
  import PekkoUtilTest.*

  // Override the implicit from PekkoSpec so that we don't get ambiguous implicits
  override val patience: PatienceConfig = defaultPatience

  implicit val executionContext: ExecutionContext = system.dispatcher

  private def abortOn(trigger: Int)(x: Int): FutureUnlessShutdown[Int] =
    FutureUnlessShutdown(Future {
      if (x == trigger) UnlessShutdown.AbortedDueToShutdown
      else UnlessShutdown.Outcome(x)
    })

  private def outcomes(length: Int, abortedFrom: Int): Seq[UnlessShutdown[Int]] =
    (1 until (abortedFrom min (length + 1))).map(UnlessShutdown.Outcome.apply) ++
      Seq.fill((length - abortedFrom + 1) max 0)(UnlessShutdown.AbortedDueToShutdown)

  override val expectedTestDuration: FiniteDuration = 90 seconds

  "mapAsyncUS" when {
    "parallelism is 1" should {
      "run everything sequentially" in assertAllStagesStopped {
        val currentParallelism = new AtomicInteger(0)
        val maxParallelism = new AtomicInteger(0)

        val source = Source(1 to 10).mapAsyncUS(parallelism = 1) { elem =>
          FutureUnlessShutdown(Future {
            val nextCurrent = currentParallelism.addAndGet(1)
            maxParallelism.getAndUpdate(_ max nextCurrent)
            Thread.`yield`()
            Threading.sleep(10)
            currentParallelism.addAndGet(-1)
            UnlessShutdown.Outcome(elem)
          })
        }
        source.runWith(Sink.seq).futureValue should ===(
          outcomes(10, 11)
        )

        maxParallelism.get shouldBe 1
        currentParallelism.get shouldBe 0
      }

      "emit only AbortedDueToShutdown after the first" in assertAllStagesStopped {
        val shutdownAt = 5
        val source = Source(1 to 10).mapAsyncUS(parallelism = 1)(abortOn(shutdownAt))
        source.runWith(Sink.seq).futureValue should
          ===(outcomes(10, shutdownAt))
      }

      "stop evaluation upon the first AbortedDueToShutdown" in assertAllStagesStopped {
        val evaluationCount = new AtomicInteger(0)
        val shutdownAt = 5
        val source = Source(1 to 10).mapAsyncUS(parallelism = 1) { elem =>
          evaluationCount.addAndGet(1).discard[Int]
          abortOn(shutdownAt)(elem)
        }
        source.runWith(Sink.seq).futureValue should
          ===(outcomes(10, shutdownAt))
        evaluationCount.get shouldBe shutdownAt
      }

      "drain the source" in assertAllStagesStopped {
        val evaluationCount = new AtomicInteger(0)
        val source = Source(1 to 10).map { elem =>
          evaluationCount.addAndGet(1).discard[Int]
          elem
        }
        val shutdownAt = 6
        val mapped = source.mapAsyncUS(parallelism = 1)(abortOn(shutdownAt))
        mapped.runWith(Sink.seq).futureValue should
          ===(outcomes(10, shutdownAt))
        evaluationCount.get shouldBe 10
      }
    }

    "parallelism is greater than 1" should {
      "run several futures in parallel" in assertAllStagesStopped {
        val parallelism = 4
        require(parallelism > 1)
        val semaphores = Seq.fill(parallelism)(new Semaphore(1))
        semaphores.foreach(_.acquire())

        val currentParallelism = new AtomicInteger(0)
        val maxParallelism = new AtomicInteger(0)

        val source = Source(1 to 10 * parallelism).mapAsyncUS(parallelism) { elem =>
          FutureUnlessShutdown(Future {
            val nextCurrent = currentParallelism.addAndGet(1)
            maxParallelism.getAndUpdate(_ max nextCurrent)

            val index = elem % parallelism
            semaphores(index).release()
            semaphores((index + 1) % parallelism).acquire()
            Thread.`yield`()
            Threading.sleep(10)
            currentParallelism.addAndGet(-1)
            UnlessShutdown.Outcome(elem)
          })
        }
        source.runWith(Sink.seq).futureValue should ===(
          (1 to 10 * parallelism).map(UnlessShutdown.Outcome.apply)
        )
        // The above synchronization allows for some futures finishing before others are started
        // but at least two must run in parallel.
        maxParallelism.get shouldBe <=(parallelism)
        maxParallelism.get shouldBe >=(2)
        currentParallelism.get shouldBe 0
      }

      "emit only AbortedDueToShutdown after the first" in assertAllStagesStopped {
        val shutdownAt = 4
        val source = Source(1 to 10).mapAsyncUS(parallelism = 3) { elem =>
          val outcome =
            if (elem == shutdownAt) UnlessShutdown.AbortedDueToShutdown
            else UnlessShutdown.Outcome(elem)
          FutureUnlessShutdown.lift(outcome)
        }
        source.runWith(Sink.seq).futureValue should ===(
          (1 until shutdownAt).map(UnlessShutdown.Outcome.apply) ++
            Seq.fill(10 - shutdownAt + 1)(UnlessShutdown.AbortedDueToShutdown)
        )
      }

      "drain the source" in assertAllStagesStopped {
        val evaluationCount = new AtomicInteger(0)
        val source = Source(1 to 10).map { elem =>
          evaluationCount.addAndGet(1).discard[Int]
          elem
        }
        val shutdownAt = 6
        val mapped = source.mapAsyncUS(parallelism = 10)(abortOn(shutdownAt))
        mapped.runWith(Sink.seq).futureValue should
          ===(outcomes(10, shutdownAt))
        evaluationCount.get shouldBe 10
      }
    }
  }

  "mapAsyncAndDrainUS" should {
    "stop upon the first AbortedDueToShutdown" in assertAllStagesStopped {
      val shutdownAt = 3
      val source = Source(1 to 10).mapAsyncAndDrainUS(parallelism = 3)(abortOn(shutdownAt))
      source.runWith(Sink.seq).futureValue should
        ===(1 until shutdownAt)
    }

    "drain the source" in assertAllStagesStopped {
      val evaluationCount = new AtomicInteger(0)
      val source = Source(1 to 10).map { elem =>
        evaluationCount.addAndGet(1).discard[Int]
        elem
      }
      val shutdownAt = 5
      val mapped = source.mapAsyncAndDrainUS(parallelism = 1)(abortOn(shutdownAt))
      mapped.runWith(Sink.seq).futureValue should
        ===(1 until shutdownAt)
      evaluationCount.get shouldBe 10
    }
  }

  "restartSource" should {
    case class RetryCallArgs(
        lastState: Int,
        lastEmittedElement: Option[Int],
        lastFailure: Option[Throwable],
    )

    def withoutKillSwitch[A](source: Source[A, NotUsed]): Source[A, (KillSwitch, Future[Done])] =
      source.mapMaterializedValue(_ => noOpKillSwitch -> Future.successful(Done))

    "restart upon normal completion" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(Source(s until s + 3))
      val lastStates = new AtomicReference[Seq[Int]](Seq.empty[Int])
      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          lastStates.updateAndGet(states => states :+ lastState)
          Option.when(lastState < 10)((0.seconds, lastState + 3))
        }
      }

      val ((_killSwitch, doneF), retrievedElemsF) = PekkoUtil
        .restartSource("restart-upon-completion", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue.map(_.value) shouldBe (1 to 12)

      doneF.futureValue
      lastStates.get() shouldBe Seq(1, 4, 7, 10)
    }

    "restart with a delay" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(Source(s until s + 3))
      val delay = 200.milliseconds
      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] =
          Option.when(lastEmittedElement.forall(_ < 10))((delay, lastState + 3))
      }

      val stream = PekkoUtil
        .restartSource("restart-with-delay", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)

      val start = System.nanoTime()
      val ((_killSwitch, doneF), retrievedElemsF) = stream.run()
      retrievedElemsF.futureValue.map(_.value) shouldBe (1 to 12)
      val stop = System.nanoTime()
      (stop - start) shouldBe >=(3 * delay.toNanos)
      doneF.futureValue
    }

    "deal with empty sources" in assertAllStagesStopped {
      val shouldRetryCalls = new AtomicReference[Seq[RetryCallArgs]](Seq.empty[RetryCallArgs])

      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(if (s > 3) Source(1 until 3) else Source.empty[Int])
      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          shouldRetryCalls
            .updateAndGet(RetryCallArgs(lastState, lastEmittedElement, lastFailure) +: _)
            .discard
          Option.when(lastState < 5)((0.seconds, lastState + 1))
        }
      }
      val ((_killSwitch, doneF), retrievedElemsF) = PekkoUtil
        .restartSource("restart-with-delay", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue.map(_.value) shouldBe Seq(1, 2, 1, 2)
      doneF.futureValue
      shouldRetryCalls.get().foreach {
        case RetryCallArgs(lastState, lastEmittedElement, lastFailure) =>
          lastFailure shouldBe None
          lastEmittedElement shouldBe Option.when(lastState > 3)(2)
      }
    }

    "propagate errors" in assertAllStagesStopped {
      case class StreamFailure(i: Int) extends Exception(i.toString)
      val shouldRetryCalls = new AtomicReference[Seq[RetryCallArgs]](Seq.empty[RetryCallArgs])

      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(
          if (s % 2 == 0) Source.failed[Int](StreamFailure(s)) else Source.single(10 + s)
        )
      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          shouldRetryCalls
            .updateAndGet(RetryCallArgs(lastState, lastEmittedElement, lastFailure) +: _)
            .discard
          Option.when(lastState < 5)((0.seconds, lastState + 1))
        }
      }
      val ((_killSwitch, doneF), retrievedElemsF) = PekkoUtil
        .restartSource("restart-propagate-error", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue.map(_.value) shouldBe Seq(11, 13, 15)
      doneF.futureValue

      shouldRetryCalls.get().foreach {
        case RetryCallArgs(lastState, lastEmittedElement, lastFailure) =>
          lastFailure shouldBe Option.when(lastState % 2 == 0)(StreamFailure(lastState))
          lastEmittedElement shouldBe Option.when(lastState % 2 != 0)(10 + lastState)
      }
    }

    "stop upon pulling the kill switch" in assertAllStagesStopped {
      val pulledKillSwitchAt = new SingleUseCell[Int]
      val pullKillSwitch = new SingleUseCell[KillSwitch]

      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(Source.single(s))
      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          pullKillSwitch.get.foreach { killSwitch =>
            if (lastState > 10) {
              pulledKillSwitchAt.putIfAbsent(lastState)
              killSwitch.shutdown()
            }
          }
          Some((1.millisecond, lastState + 1))
        }
      }
      val ((killSwitch, doneF), retrievedElemsF) = PekkoUtil
        .restartSource("restart-stop-on-kill", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      pullKillSwitch.putIfAbsent(killSwitch)
      val retrievedElems = retrievedElemsF.futureValue.map(_.value)
      val lastRetry = pulledKillSwitchAt.get.value
      lastRetry shouldBe >(10)
      retrievedElems shouldBe (1 to lastRetry)
      doneF.futureValue
    }

    "can pull the kill switch from within the stream" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        Source
          .fromIterator(() => Iterator.from(s))
          .viaMat(KillSwitches.single)(Keep.right)
          .watchTermination()(Keep.both)

      val policy = PekkoUtil.RetrySourcePolicy.never[Int, Int]
      val ((_killSwitch, doneF), sink) = PekkoUtil
        .restartSource("close-inner-source", 1, mkSource, policy)
        .map { case elemWithKillSwitch @ WithKillSwitch(elem) =>
          if (elem == 4) elemWithKillSwitch.killSwitch.shutdown()
          elem
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(4)
      sink.expectNext(1).expectNext(2).expectNext(3).expectNext(4)
      sink.request(10)
      // There's still an element somewhere in the stream that gets delivered first
      sink.expectNext(5)
      sink.expectComplete()
    }

    "abort the delay when the KillSwitch is closed" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(Source.single(s))
      val pullKillSwitch = new SingleUseCell[KillSwitch]
      val longBackoff = 10.seconds

      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          pullKillSwitch.get.foreach { killSwitch =>
            if (lastState > 10) killSwitch.shutdown()
          }
          val backoff =
            if (pullKillSwitch.isEmpty || lastState <= 10) 1.millisecond else longBackoff
          Some((backoff, lastState + 1))
        }
      }
      val graph = PekkoUtil
        .restartSource("restart-stop-immediately-on-kill-switch", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
      val start = System.nanoTime()
      val ((killSwitch, doneF), retrievedElemsF) = graph.run()
      pullKillSwitch.putIfAbsent(killSwitch)
      doneF.futureValue
      val stop = System.nanoTime()
      (stop - start) shouldBe <(longBackoff.toNanos)
      retrievedElemsF.futureValue.map(_.value) shouldBe (1 to 11)
    }

    "the completion future awaits the retry to finish" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(Source.single(s))

      val pullKillSwitch = new SingleUseCell[KillSwitch]
      val policyDelayPromise = Promise[Unit]()

      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          pullKillSwitch.get.foreach { killSwitch =>
            if (lastState > 10) {
              killSwitch.shutdown()
              policyDelayPromise.future.futureValue
            }
          }
          Some((1.millisecond, lastState + 1))
        }
      }
      val ((killSwitch, doneF), retrievedElemsF) = PekkoUtil
        .restartSource("restart-synchronize-retry", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      pullKillSwitch.putIfAbsent(killSwitch)
      retrievedElemsF.futureValue
      // The retry policy is still running as we haven't yet completed the promise,
      // so the completion future must not be completed yet
      always(durationOfSuccess = 1.second) {
        doneF.isCompleted shouldBe false
      }
      policyDelayPromise.success(())
      doneF.futureValue
    }

    "close the current source when the kill switch is pulled" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        Source
          .fromIterator(() => Iterator.from(s))
          .viaMat(KillSwitches.single)(Keep.right)
          .watchTermination()(Keep.both)

      val policy = PekkoUtil.RetrySourcePolicy.never[Int, Int]
      val ((killSwitch, doneF), sink) = PekkoUtil
        .restartSource("close-inner-source", 1, mkSource, policy)
        .map(_.value)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(4)
      sink.expectNext(1).expectNext(2).expectNext(3).expectNext(4)
      killSwitch.shutdown()
      sink.request(10)
      // There's still an element somewhere in the stream that gets delivered first
      sink.expectNext(5)
      sink.expectComplete()
    }

    "await the source's completion futures" in assertAllStagesStopped {
      val doneP: scala.collection.concurrent.Map[Int, Promise[Done]] =
        new TrieMap[Int, Promise[Done]]

      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        Source.single(s).viaMat(KillSwitches.single) { (_, killSwitch) =>
          val newPromise = Promise[Done]()
          val promise = doneP.putIfAbsent(s, newPromise).getOrElse(newPromise)
          killSwitch -> promise.future
        }

      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] =
          Some((1.millisecond, lastState + 1))
      }

      val ((killSwitch, doneF), sink) = PekkoUtil
        .restartSource("await completion of inner sources", 1, mkSource, policy)
        .map(_.value)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(3)
      sink.expectNext(1)
      doneP.size should be >= 1
      sink.expectNext(2)
      doneP.size should be >= 2
      sink.expectNext(3)
      logger.debug("Stopping the restart source via the kill switch")
      killSwitch.shutdown()
      // Ask for another element to make sure that flatMapConcat inside restartSource notices
      // that the current source has been completed (via its kill switch)
      // Otherwise, the stream may remain open and doneF never completes
      sink.request(1)

      always(durationOfSuccess = 100.milliseconds) {
        doneF.isCompleted shouldBe false
      }

      doneP.remove(3).foreach(_.success(Done))
      doneP.remove(2).foreach(_.success(Done))

      always(durationOfSuccess = 100.milliseconds) {
        doneF.isCompleted shouldBe false
      }

      // Now complete the remaining promises
      doneP.foreachEntry((_, promise) => promise.success(Done))
      doneF.futureValue
    }

    "log errors thrown during the retry step and complete the stream" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(Source.single(s))
      val exception = new Exception("Retry policy failure")
      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = {
          if (lastState > 3) throw exception
          Some((0.milliseconds, lastState + 1))
        }
      }
      val name = "restart-log-error"
      val graph = PekkoUtil
        .restartSource(name, 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
      val retrievedElems = loggerFactory.assertLogs(
        {
          val ((_killSwitch, doneF), retrievedElemsF) = graph.run()
          doneF.futureValue
          retrievedElemsF.futureValue
        },
        entry => {
          entry.errorMessage should include(
            s"The retry policy for RestartSource $name failed with an error. Stop retrying."
          )
          entry.throwable should contain(exception)
        },
        // The log line from the flush
        _.errorMessage should include(s"RestartSource $name at state 4 failed"),
      )
      retrievedElems.map(_.value) shouldBe Seq(1, 2, 3, 4)
    }

    "can pull the kill switch after retries have stopped" in assertAllStagesStopped {
      def mkSource(s: Int): Source[Int, (KillSwitch, Future[Done])] =
        withoutKillSwitch(Source.empty[Int])
      val policy = new PekkoUtil.RetrySourcePolicy[Int, Int] {
        override def shouldRetry(
            lastState: Int,
            lastEmittedElement: Option[Int],
            lastFailure: Option[Throwable],
        ): Option[(FiniteDuration, Int)] = None
      }
      val ((killSwitch, doneF), retrievedElemsF) = PekkoUtil
        .restartSource("restart-kill-switch-after-complete", 1, mkSource, policy)
        .toMat(Sink.seq)(Keep.both)
        .run()
      retrievedElemsF.futureValue shouldBe Seq.empty
      doneF.futureValue
      killSwitch.shutdown()
    }
  }

  "withMaterializedValueMat" should {
    "pass the materialized value into the stream" in assertAllStagesStopped {
      val source = Source(1 to 10)
      val (mat, fut) = source
        .withMaterializedValueMat(new AtomicInteger(1))(Keep.right)
        .map { case (i, m) => m.addAndGet(i) }
        // Add a buffer so that the map function executes even though the resulting element doesn't end up in the sink
        .buffer(size = 2, OverflowStrategy.backpressure)
        // Stop the stream early to test cancellation support
        .take(5)
        .toMat(Sink.seq)(Keep.both)
        .run()
      fut.futureValue should ===(Seq(2, 4, 7, 11, 16))
      mat.get shouldBe 22
    }

    "create a new value upon each materialization" in assertAllStagesStopped {
      val stream = PekkoUtil
        .withMaterializedValueMat(new AtomicInteger(0))(Source(1 to 5))(Keep.right)
        .map { case (i, m) => m.addAndGet(i) }
        .toMat(Sink.seq)(Keep.both)

      val (mat1, seq1) = stream.run()
      val (mat2, seq2) = stream.run()

      // We get two different materialized atomic integers
      mat1 shouldNot be(mat2)

      seq1.futureValue should ===(Seq(1, 3, 6, 10, 15))
      seq2.futureValue should ===(Seq(1, 3, 6, 10, 15))
    }

    "propagate errors down" in assertAllStagesStopped {
      val ((source, mat), sink) = TestSource
        .probe[Int]
        .withMaterializedValueMat(new AtomicInteger(0))(Keep.both)
        .map { case (i, m) => m.addAndGet(i) }
        .buffer(2, OverflowStrategy.backpressure)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      sink.request(1)
      source.sendNext(1)
      sink.expectNext(1)
      source.sendNext(2)
      source.sendNext(3)
      val ex = new Exception("Source error")
      source.sendError(ex)
      sink.expectError() should ===(ex)

      mat.get() should ===(6)
    }

    "propagate errors up" in assertAllStagesStopped {
      val ((source, mat), sink) = TestSource
        .probe[Int]
        .withMaterializedValueMat(new AtomicInteger(0))(Keep.both)
        .map { case (i, m) => m.addAndGet(i) }
        .buffer(2, OverflowStrategy.backpressure)
        .toMat(TestSink.probe[Int])(Keep.both)
        .run()

      sink.request(1)
      source.sendNext(1)
      sink.expectNext(1)
      source.sendNext(2)
      source.sendNext(3)
      val ex = new Exception("Sink error")
      sink.cancel(ex)
      source.expectCancellationWithCause(ex)

      mat.get() should ===(6)
    }
  }

  "withUniqueKillSwitchMat" should {
    "make the kill switch available inside the stream" in assertAllStagesStopped {
      val (source, sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.left)
        .map { elem =>
          if (elem.value > 0) elem.killSwitch.shutdown()
          elem.value
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(4)
      source.expectRequest() shouldBe >=(3L)
      source.sendNext(0).sendNext(-1).sendNext(2)
      sink.expectNext(0).expectNext(-1).expectNext(2).expectComplete()
    }

    "make the same kill switch available in the materialization" in assertAllStagesStopped {
      val ((source, killSwitch), sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.both)
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(3)
      source.sendNext(100)
      sink.expectNext(WithKillSwitch(100)(killSwitch))
      killSwitch.shutdown()
      source.expectCancellation()
      sink.expectComplete()
    }

    "propagate completions even without pulling the kill switch" in assertAllStagesStopped {
      val (source, sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.left)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      sink.request(1)
      source.sendComplete()
      sink.expectComplete()
    }

    "propagate errors" in assertAllStagesStopped {
      val ex = new Exception("Kill Switch")
      val (source, sink) = TestSource
        .probe[Int]
        .withUniqueKillSwitchMat()(Keep.left)
        .map { elem =>
          elem.killSwitch.abort(ex)
          elem.value
        }
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(1)
      source.sendNext(1)
      source.expectCancellationWithCause(ex)
      // Since the kill switch is pulled from within the stream's handler,
      // the OnNext message will arrive at the sink before the kill switches
      // OnError message goes through the flow that pulled the kill switch.
      // So given Pekko's in-order deliver guarantees between actor pairs,
      // the OnError will arrive after the OnNext.
      sink.expectNext(1)
      sink.expectError(ex)
    }
  }

  "takeUntilThenDrain" should {
    "pass elements through until the first one satisfying the predicate" in assertAllStagesStopped {
      val elemsF = Source
        // Infinite source to test that we really stop
        .fromIterator(() => Iterator.from(1))
        .withUniqueKillSwitchMat()(Keep.left)
        .takeUntilThenDrain(_ >= 5)
        .runWith(Sink.seq)
      elemsF.futureValue.map(_.value) shouldBe (1 to 5)
    }

    "pass all elements if the condition never fires" in assertAllStagesStopped {
      val elemsF = Source(1 to 10)
        .withUniqueKillSwitchMat()(Keep.left)
        .takeUntilThenDrain(_ => false)
        .runWith(Sink.seq)
      elemsF.futureValue.map(_.value) shouldBe (1 to 10)
    }

    "drain the source" in assertAllStagesStopped {
      val observed = new AtomicReference[Seq[Int]](Seq.empty[Int])
      val elemsF = Source(1 to 10)
        .map { i =>
          observed.getAndUpdate(_ :+ i)
          withNoOpKillSwitch(i)
        }
        .takeUntilThenDrain(_ >= 5)
        .runWith(Sink.seq)
      elemsF.futureValue.map(_.value) shouldBe (1 to 5)
      observed.get() shouldBe (1 to 10)
    }
  }

  // Sanity check that the construction in GrpcSequencerClientTransportPekko works
  "concatLazy + Source.lazySingle" should {
    "not produce the lazy single element upon an error" in {
      val evaluated = new AtomicBoolean()
      val (source, sink) = TestSource
        .probe[String]
        .concatLazy(Source.lazySingle { () =>
          evaluated.set(true)
          "sentinel"
        })
        .recover { case NonFatal(ex) => ex.getMessage }
        .toMat(TestSink.probe)(Keep.both)
        .run()
      sink.request(5)
      source.sendNext("one")
      sink.expectNext("one")
      val ex = new Exception("Source error")
      source.sendError(ex)
      sink.expectNext("Source error")
      sink.expectComplete()

      evaluated.get() shouldBe false
    }
  }

  "remember" should {
    "immediately emit elements" in assertAllStagesStopped {
      val (source, sink) =
        TestSource.probe[Int].remember(NonNegativeInt.one).toMat(TestSink.probe)(Keep.both).run()
      sink.request(5)
      source.sendNext(1)
      sink.expectNext(NonEmpty(Seq, 1))
      source.sendNext(2)
      sink.expectNext(NonEmpty(Seq, 1, 2))
      source.sendNext(3)
      sink.expectNext(NonEmpty(Seq, 2, 3))
      source.sendComplete()
      sink.expectComplete()
    }

    "handle the empty source" in assertAllStagesStopped {
      val sinkF = Source.empty[Int].remember(NonNegativeInt.one).toMat(Sink.seq)(Keep.right).run()
      sinkF.futureValue shouldBe Seq.empty
    }

    "support no memory" in assertAllStagesStopped {
      val sinkF = Source(1 to 10).remember(NonNegativeInt.zero).toMat(Sink.seq)(Keep.right).run()
      sinkF.futureValue shouldBe ((1 to 10).map(NonEmpty(Seq, _)))
    }

    "support completion while the memory is not exhausted" in assertAllStagesStopped {
      val sinkF =
        Source(1 to 5).remember(NonNegativeInt.tryCreate(10)).toMat(Sink.seq)(Keep.right).run()
      sinkF.futureValue shouldBe (1 to 5).inits.toSeq.reverse.mapFilter(NonEmpty.from)
    }

    "propagate errors" in assertAllStagesStopped {
      val ex = new Exception("Remember failure")
      val doneF =
        Source.failed(ex).remember(NonNegativeInt.one).toMat(Sink.ignore)(Keep.right).run()
      doneF.failed.futureValue shouldBe ex
    }

    "work for sources and flows" in assertAllStagesStopped {
      // this is merely a compilation test, so no need to run the graphs
      val flow = Flow[Int]
        .remember(NonNegativeInt.tryCreate(5))
        .mapAsyncAndDrainUS(1)(xs => FutureUnlessShutdown.pure(xs.size))
        .withUniqueKillSwitchMat()(Keep.right)
        .takeUntilThenDrain(_ > 0)

      val source = Source
        .empty[Int]
        .remember(NonNegativeInt.tryCreate(5))
        .mapAsyncAndDrainUS(1)(xs => FutureUnlessShutdown.pure(xs.size))
        .withUniqueKillSwitchMat()(Keep.right)
        .takeUntilThenDrain(_ > 0)
        .map(_.value)
        .via(flow)

      source.to(Sink.seq[WithKillSwitch[Int]])

      succeed
    }
  }

  "dropIf" should {
    "drop only elements that satisfy the condition" in assertAllStagesStopped {
      val elemF = Source(1 to 10).dropIf(3)(_ % 2 == 0).toMat(Sink.seq)(Keep.right).run()
      elemF.futureValue shouldBe Seq(1, 3, 5, 7, 8, 9, 10)
    }

    "ignore negative counts" in assertAllStagesStopped {
      val elemF = Source(1 to 10).dropIf(-1)(_ => false).toMat(Sink.seq)(Keep.right).run()
      elemF.futureValue shouldBe (1 to 10)
    }
  }

  "statefulMapAsyncContextualizedUS" should {
    "work with singleton contexts" in assertAllStagesStopped {
      val sinkF = Source(Seq(None, Some(2), Some(4))).contextualize
        .statefulMapAsyncContextualizedUS(1)((acc, _, i) =>
          FutureUnlessShutdown.pure((acc + i, acc))
        )
        .toMat(Sink.seq)(Keep.right)
        .run()

      sinkF.futureValue shouldBe Seq(None, Some(Outcome(1)), Some(Outcome(3)))
    }

    "work with nested contexts" in assertAllStagesStopped {
      val source =
        Source[Option[(String, Int)]](Seq(Some("context1" -> 1), Some("context2" -> 2)))
      // Nested contexts require a bit more manual work.
      implicit val traverse: SingletonTraverse.Aux[Lambda[a => Option[(String, a)]], String] =
        SingletonTraverse[Option].composeWith(SingletonTraverse[(String, *)])(Keep.right)
      val sinkF = ContextualizedFlowOps
        .contextualize[Lambda[`+a` => Option[(String, a)]]](source)
        .statefulMapAsyncContextualizedUS(2)((acc, string, i) =>
          FutureUnlessShutdown.pure((acc + i, acc * i + string.size))
        )
        .toMat(Sink.seq)(Keep.right)
        .run()
      sinkF.futureValue shouldBe Seq(
        Some("context1" -> Outcome(2 * 1 + 8)),
        Some("context2" -> Outcome(3 * 2 + 8)),
      )
    }

    "propagate AbortedDueToShutdown" in assertAllStagesStopped {
      val source = Source(Seq(Left("left1"), Right(1), Right(2), Right(3), Left("left2"), Right(4)))
      val sinkF = source.contextualize
        .statefulMapAsyncContextualizedUS(1)((acc, _, i) =>
          if (i == 3) FutureUnlessShutdown.abortedDueToShutdown
          else FutureUnlessShutdown.pure((acc + i, acc * i))
        )
        .toMat(Sink.seq)(Keep.right)
        .run()
      sinkF.futureValue shouldBe Seq(
        Left("left1"),
        Right(Outcome(1)),
        Right(Outcome(4)),
        Right(AbortedDueToShutdown),
        Left("left2"),
        Right(AbortedDueToShutdown),
      )
    }

    "work for flows" in assertAllStagesStopped {
      Flow[Option[Int]].contextualize.statefulMapAsyncContextualizedUS("abc")(
        (_: String, _: Unit, _: Int) => ???
      )

      // The extension method contextualizes only over the outer-most type constructor
      Flow[Either[Int, Option[String]]].contextualize
        .statefulMapAsyncContextualizedUS(3L)((_: Long, _: Unit, _: Option[String]) => ???)

      implicit val traverse: SingletonTraverse.Aux[Lambda[a => Either[Int, Option[a]]], Unit] =
        SingletonTraverse[Either[Int, *]].composeWith(SingletonTraverse[Option])(Keep.right)
      ContextualizedFlowOps
        .contextualize[Lambda[`+a` => Either[Int, Option[a]]]](Flow[Either[Int, Option[String]]])
        .statefulMapAsyncContextualizedUS(3L)((_: Long, _: Unit, _: String) => ???)

      succeed
    }
  }

  "WithKillSwitch satisfies the SingletonTraverse laws" should {
    checkAllLaws(
      "WithKillSwitch",
      SingletonTraverseTests[WithKillSwitch].singletonTraverse[Int, Int, Int, Int, Option, Option],
    )
  }

  "RecoveringFutureQueueImpl" should {

    "only complete the firstSuccessfulConsumerInitialization after the first successful initialization" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 1,
        bufferSize = 20,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      val firstFail = Promise[Unit]()
      val secondFail = Promise[Unit]()
      val thirdFail = Promise[Unit]()
      val finalSucceed = Promise[Unit]()
      val firstFailed = Promise[Unit]()
      val secondFailed = Promise[Unit]()
      val thirdFailed = Promise[Unit]()
      val finalSucceeded = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        if (finalSucceeded.isCompleted)
          fail("Should not get so far")
        else if (thirdFailed.isCompleted)
          finalSucceed.future.map { _ =>
            finalSucceeded.trySuccess(())
            val (sourceQueue, sourceDone) = Source
              .queue[(Long, Int)](20, OverflowStrategy.backpressure, 1)
              .map { elem =>
                commit(elem._1)
              }
              .toMat(Sink.ignore)(Keep.both)
              .run()
            FutureQueueConsumer(
              futureQueue = new PekkoSourceQueueToFutureQueue(
                sourceQueue = sourceQueue,
                sourceDone = sourceDone,
                loggerFactory = loggerFactory,
              ),
              fromExclusive = 0,
            )
          }
        else if (secondFailed.isCompleted)
          thirdFail.future.map { _ =>
            thirdFailed.trySuccess(())
            throw new Exception("boom")
          }
        else if (firstFailed.isCompleted)
          secondFail.future.map { _ =>
            secondFailed.trySuccess(())
            throw new Exception("boom")
          }
        else
          firstFail.future.map { _ =>
            firstFailed.trySuccess(())
            throw new Exception("boom")
          }
      )
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      firstFail.trySuccess(())
      firstFailed.future.futureValue
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      secondFail.trySuccess(())
      secondFailed.future.futureValue
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      thirdFail.trySuccess(())
      thirdFailed.future.futureValue
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      finalSucceed.trySuccess(())
      finalSucceeded.future.futureValue
      recoveringQueue.firstSuccessfulConsumerInitialization.futureValue
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
    }

    "fail firstSuccessfulConsumerInitialization if shutdown comes earlier" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 1,
        bufferSize = 20,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      Threading.sleep(10)
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
      recoveringQueue.firstSuccessfulConsumerInitialization.failed.futureValue
    }

    "allow offer if the consumerFactory is not set yet" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 1,
        bufferSize = 2,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      val blockedOffer = recoveringQueue.offer(3)
      blockedOffer.isCompleted shouldBe false
      val received = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          val (sourceQueue, sourceDone) = Source
            .queue[(Long, Int)](20, OverflowStrategy.backpressure, 1)
            .map { elem =>
              discard(
                received.accumulateAndGet(Vector(elem), _ ++ _)
              )
              commit(elem._1)
            }
            .toMat(Sink.ignore)(Keep.both)
            .run()
          FutureQueueConsumer(
            futureQueue = new PekkoSourceQueueToFutureQueue(
              sourceQueue = sourceQueue,
              sourceDone = sourceDone,
              loggerFactory = loggerFactory,
            ),
            fromExclusive = 0,
          )
        }
      )
      blockedOffer.futureValue
      eventually() {
        received.get() shouldBe Vector(
          1L -> 1,
          2L -> 2,
          3L -> 3,
        )
      }
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
    }

    "block offer if buffer is full" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 2,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val received = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
      val offerGated = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              private val shutdownPromise = Promise[Unit]()

              override def offer(elem: (Long, Int)): Future[Done] =
                offerGated.future.map { _ =>
                  discard(received.accumulateAndGet(Vector(elem), _ ++ _))
                  Done
                }

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = shutdownPromise.future.map(_ => Done)
            },
            fromExclusive = 0,
          )
        }
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      recoveringQueue.offer(3).futureValue
      val blockedOffer1 = recoveringQueue.offer(4)
      val blockedOffer2 = recoveringQueue.offer(5)
      blockedOffer1.isCompleted shouldBe false
      blockedOffer2.isCompleted shouldBe false
      Threading.sleep(10)
      blockedOffer1.isCompleted shouldBe false
      blockedOffer2.isCompleted shouldBe false
      offerGated.trySuccess(())
      blockedOffer1.futureValue
      blockedOffer2.futureValue
      eventually() {
        received.get() shouldBe Vector(
          1L -> 1,
          2L -> 2,
          3L -> 3,
          4L -> 4,
          5L -> 5,
        )
      }
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
    }

    "complete all blocked offer calls if shutting down" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 3,
        bufferSize = 2,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      val blockedOffer1 = recoveringQueue.offer(3)
      val blockedOffer2 = recoveringQueue.offer(4)
      val blockedOffer3 = recoveringQueue.offer(5)
      recoveringQueue.offer(6).failed.futureValue.getMessage should include(
        "Too many parallel offer calls. Maximum allowed parallel offer calls"
      )
      blockedOffer1.isCompleted shouldBe false
      blockedOffer2.isCompleted shouldBe false
      blockedOffer3.isCompleted shouldBe false
      Threading.sleep(10)
      blockedOffer1.isCompleted shouldBe false
      blockedOffer2.isCompleted shouldBe false
      blockedOffer3.isCompleted shouldBe false
      loggerFactory.assertLogs(
        {
          recoveringQueue.shutdown()
          recoveringQueue.done.futureValue
        },
        _.warningMessage should include(
          "blocked offer calls pending at the time of the shutdown. It is recommended that shutdown gracefully"
        ),
      )
      blockedOffer1.futureValue
      blockedOffer2.futureValue
      blockedOffer3.futureValue
    }

    "tear down consumer properly on shutdown" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 2,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val shutdownPromise = Promise[Unit]()
      val donePromise = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              override def offer(elem: (Long, Int)): Future[Done] =
                Future.successful(Done)

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = donePromise.future.map(_ => Done)
            },
            fromExclusive = 0,
          )
        }
      )
      recoveringQueue.firstSuccessfulConsumerInitialization.futureValue
      shutdownPromise.isCompleted shouldBe false
      Threading.sleep(10)
      shutdownPromise.isCompleted shouldBe false
      recoveringQueue.shutdown()
      shutdownPromise.future.futureValue
      recoveringQueue.done.isCompleted shouldBe false
      Threading.sleep(10)
      recoveringQueue.done.isCompleted shouldBe false
      donePromise.trySuccess(())
      recoveringQueue.done.futureValue
    }

    "tear down consumer properly even if shutdown comes earlier than initialization finished" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 2,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val initializedPromise = Promise[Unit]()
      val shutdownPromise = Promise[Unit]()
      val donePromise = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        initializedPromise.future.map { _ =>
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              override def offer(elem: (Long, Int)): Future[Done] =
                Future.successful(Done)

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = donePromise.future.map(_ => Done)
            },
            fromExclusive = 0,
          )
        }
      )
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      shutdownPromise.isCompleted shouldBe false
      Threading.sleep(10)
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      shutdownPromise.isCompleted shouldBe false
      recoveringQueue.shutdown()
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      recoveringQueue.done.isCompleted shouldBe false
      shutdownPromise.isCompleted shouldBe false
      Threading.sleep(10)
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      recoveringQueue.done.isCompleted shouldBe false
      shutdownPromise.isCompleted shouldBe false
      initializedPromise.trySuccess(())
      shutdownPromise.future.futureValue
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      recoveringQueue.done.isCompleted shouldBe false
      Threading.sleep(10)
      recoveringQueue.firstSuccessfulConsumerInitialization.isCompleted shouldBe false
      recoveringQueue.done.isCompleted shouldBe false
      donePromise.trySuccess(())
      recoveringQueue.firstSuccessfulConsumerInitialization.failed.futureValue
      recoveringQueue.done.futureValue
    }

    "log warn/error if consumer initialization respective warn/error threshold is breached" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 2,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 3,
        retryAttemptErrorThreshold = 6,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val initializationStartedPromise = new AtomicReference(Promise[Unit]())
      val initializationContinuePromise = new AtomicReference(Promise[Unit]())
      recoveringQueue.registerConsumerFactory { _ =>
        val f = initializationContinuePromise.get().future.map { _ =>
          throw new Exception("initialization fails")
        }
        initializationStartedPromise.get().trySuccess(())
        f
      }
      // info 1
      initializationStartedPromise.get().future.futureValue
      initializationStartedPromise.set(Promise())
      initializationContinuePromise.getAndSet(Promise()).trySuccess(())
      // info 2
      initializationStartedPromise.get().future.futureValue
      initializationStartedPromise.set(Promise())
      initializationContinuePromise.getAndSet(Promise()).trySuccess(())
      // info 3
      initializationStartedPromise.get().future.futureValue
      initializationStartedPromise.set(Promise())
      initializationContinuePromise.getAndSet(Promise()).trySuccess(())
      loggerFactory.assertLogs(
        {
          // warn 1
          initializationStartedPromise.get().future.futureValue
          initializationStartedPromise.set(Promise())
          initializationContinuePromise.getAndSet(Promise()).trySuccess(())
        },
        logEntry =>
          logEntry.warningMessage should include("Consumer initialization failed (attempt #4)"),
      )
      loggerFactory.assertLogs(
        {
          // warn 2
          initializationStartedPromise.get().future.futureValue
          initializationStartedPromise.set(Promise())
          initializationContinuePromise.getAndSet(Promise()).trySuccess(())
        },
        logEntry =>
          logEntry.warningMessage should include("Consumer initialization failed (attempt #5)"),
      )
      loggerFactory.assertLogs(
        {
          // warn 3
          initializationStartedPromise.get().future.futureValue
          initializationStartedPromise.set(Promise())
          initializationContinuePromise.getAndSet(Promise()).trySuccess(())
        },
        logEntry =>
          logEntry.warningMessage should include("Consumer initialization failed (attempt #6)"),
      )
      loggerFactory.assertLogs(
        {
          // error 1
          initializationStartedPromise.get().future.futureValue
          initializationStartedPromise.set(Promise())
          initializationContinuePromise.getAndSet(Promise()).trySuccess(())
        },
        logEntry =>
          logEntry.errorMessage should include("Consumer initialization failed (attempt #7)"),
      )
      loggerFactory.assertLogs(
        {
          // error 2
          initializationStartedPromise.get().future.futureValue
          initializationStartedPromise.set(Promise())
          initializationContinuePromise.getAndSet(Promise()).trySuccess(())
        },
        logEntry =>
          logEntry.errorMessage should include("Consumer initialization failed (attempt #8)"),
      )
      // error 3, but as shutting down, no more errors reported
      initializationStartedPromise.get().future.futureValue
      recoveringQueue.shutdown()
      initializationContinuePromise.get().trySuccess(())
      recoveringQueue.done.futureValue
      recoveringQueue.firstSuccessfulConsumerInitialization.failed.futureValue
    }

    "consumer offer failure should trigger a warning and proper recovery" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 10,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val received = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
      val firstConsumer = new AtomicBoolean(false)
      val recoveryIndexRef = new AtomicLong(0)
      val offerGated = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          val recoveryIndex = received.get().lastOption.map(_._1).getOrElse(0L)
          if (firstConsumer.get()) {
            firstConsumer.set(false)
            recoveryIndexRef.set(recoveryIndex)
          } else {
            firstConsumer.set(true)
          }
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              private val shutdownPromise = Promise[Unit]()

              override def offer(elem: (Long, Int)): Future[Done] =
                offerGated.future.map { _ =>
                  if (elem._2 == 4 && firstConsumer.get()) throw new Exception("boom")
                  else {
                    discard(received.accumulateAndGet(Vector(elem), _ ++ _))
                    commit(elem._1)
                    Done
                  }
                }

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = shutdownPromise.future.map(_ => Done)
            },
            fromExclusive = recoveryIndex,
          )
        }
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      recoveringQueue.offer(3).futureValue
      recoveringQueue.offer(4).futureValue
      recoveringQueue.offer(5).futureValue
      // now all above are in the internal buffer
      loggerFactory.assertLogs(
        {
          offerGated.trySuccess(())
          eventually() {
            received.get() shouldBe Vector(
              1L -> 1,
              2L -> 2,
              3L -> 3,
              4L -> 4,
              5L -> 5,
            )
          }
        },
        _.warningMessage should include("Offer failed, shutting down delegate"),
      )
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
      recoveryIndexRef.get() shouldBe 3
    }

    "recover from the last element correctly" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 10,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val received = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
      val firstConsumer = new AtomicBoolean(false)
      val recoveryIndexRef = new AtomicLong(0)
      val offerGated = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          val recoveryIndex = received.get().lastOption.map(_._1).getOrElse(0L)
          if (firstConsumer.get()) {
            firstConsumer.set(false)
            recoveryIndexRef.set(recoveryIndex)
          } else {
            firstConsumer.set(true)
          }
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              private val shutdownPromise = Promise[Unit]()

              override def offer(elem: (Long, Int)): Future[Done] =
                offerGated.future.flatMap { _ =>
                  if (elem._2 == 4 && firstConsumer.get()) {
                    shutdownPromise.tryFailure(new Exception("delegate boom"))
                    Future.never
                  } else {
                    discard(received.accumulateAndGet(Vector(elem), _ ++ _))
                    commit(elem._1)
                    Future.successful(Done)
                  }
                }

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = shutdownPromise.future.map(_ => Done)
            },
            fromExclusive = recoveryIndex,
          )
        }
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      recoveringQueue.offer(3).futureValue
      recoveringQueue.offer(4).futureValue
      recoveringQueue.offer(5).futureValue
      // now all above are in the internal buffer
      offerGated.trySuccess(())
      eventually() {
        received.get() shouldBe Vector(
          1L -> 1,
          2L -> 2,
          3L -> 3,
          4L -> 4,
          5L -> 5,
        )
      }
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
      recoveryIndexRef.get() shouldBe 3
    }

    "recover within the uncommitted range correctly" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 10,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val received = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
      val firstConsumer = new AtomicBoolean(false)
      val recoveryIndexRef = new AtomicLong(0)
      val offerGated = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          val recoveryIndex = received.get().lastOption.map(_._1).getOrElse(0L)
          if (firstConsumer.get()) {
            firstConsumer.set(false)
            recoveryIndexRef.set(recoveryIndex)
          } else {
            firstConsumer.set(true)
          }
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              private val shutdownPromise = Promise[Unit]()

              override def offer(elem: (Long, Int)): Future[Done] =
                offerGated.future.flatMap { _ =>
                  if (elem._2 == 4 && firstConsumer.get()) {
                    shutdownPromise.tryFailure(new Exception("delegate boom"))
                    Future.never
                  } else if (elem._2 >= 3 && firstConsumer.get()) {
                    // forget, not commit
                    Future.successful(Done)
                  } else if (elem._2 >= 2 && firstConsumer.get()) {
                    // not commit
                    discard(received.accumulateAndGet(Vector(elem), _ ++ _))
                    Future.successful(Done)
                  } else {
                    commit(elem._1)
                    discard(received.accumulateAndGet(Vector(elem), _ ++ _))
                    Future.successful(Done)
                  }
                }

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = shutdownPromise.future.map(_ => Done)
            },
            fromExclusive = recoveryIndex,
          )
        }
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      recoveringQueue.offer(3).futureValue
      recoveringQueue.offer(4).futureValue
      recoveringQueue.offer(5).futureValue
      // now all above are in the internal buffer
      offerGated.trySuccess(())
      eventually() {
        received.get() shouldBe Vector(
          1L -> 1,
          2L -> 2,
          3L -> 3,
          4L -> 4,
          5L -> 5,
        )
      }
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
      recoveryIndexRef.get() shouldBe 2
    }

    "recover at the beginning of the uncommitted range correctly" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 10,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val received = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
      val firstConsumer = new AtomicBoolean(false)
      val recoveryIndexRef = new AtomicLong(0)
      val offerGated = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          val recoveryIndex = received.get().lastOption.map(_._1).getOrElse(0L)
          if (firstConsumer.get()) {
            firstConsumer.set(false)
            recoveryIndexRef.set(recoveryIndex)
          } else {
            firstConsumer.set(true)
          }
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              private val shutdownPromise = Promise[Unit]()

              override def offer(elem: (Long, Int)): Future[Done] =
                offerGated.future.flatMap { _ =>
                  if (elem._2 == 4 && firstConsumer.get()) {
                    shutdownPromise.tryFailure(new Exception("delegate boom"))
                    Future.never
                  } else if (elem._2 >= 2 && firstConsumer.get()) {
                    // forget, not commit
                    Future.successful(Done)
                  } else {
                    commit(elem._1)
                    discard(received.accumulateAndGet(Vector(elem), _ ++ _))
                    Future.successful(Done)
                  }
                }

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = shutdownPromise.future.map(_ => Done)
            },
            fromExclusive = recoveryIndex,
          )
        }
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      recoveringQueue.offer(3).futureValue
      recoveringQueue.offer(4).futureValue
      recoveringQueue.offer(5).futureValue
      // now all above are in the internal buffer
      offerGated.trySuccess(())
      eventually() {
        received.get() shouldBe Vector(
          1L -> 1,
          2L -> 2,
          3L -> 3,
          4L -> 4,
          5L -> 5,
        )
      }
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
      recoveryIndexRef.get() shouldBe 1
    }

    "trying to recover before the uncommitted range results in halt and error (this is the case for invalid commit wiring in consumer)" in assertAllStagesStopped {
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 2,
        bufferSize = 10,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      val received = new AtomicReference[Vector[(Long, Int)]](Vector.empty)
      val firstConsumer = new AtomicBoolean(false)
      val recoveryIndexRef = new AtomicLong(0)
      val offerGated = Promise[Unit]()
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          val recoveryIndex = received.get().lastOption.map(_._1).getOrElse(0L)
          if (firstConsumer.get()) {
            firstConsumer.set(false)
            recoveryIndexRef.set(recoveryIndex)
          } else {
            firstConsumer.set(true)
          }
          FutureQueueConsumer(
            futureQueue = new FutureQueue[(Long, Int)] {
              private val shutdownPromise = Promise[Unit]()

              override def offer(elem: (Long, Int)): Future[Done] =
                offerGated.future.flatMap { _ =>
                  if (elem._2 == 4 && firstConsumer.get()) {
                    shutdownPromise.tryFailure(new Exception("delegate boom"))
                    Future.never
                  } else if (elem._2 >= 2 && firstConsumer.get()) {
                    // forget, but commit: very wrong
                    commit(elem._1)
                    Future.successful(Done)
                  } else {
                    commit(elem._1)
                    discard(received.accumulateAndGet(Vector(elem), _ ++ _))
                    Future.successful(Done)
                  }
                }

              override def shutdown(): Unit = shutdownPromise.trySuccess(())

              override def done: Future[Done] = shutdownPromise.future.map(_ => Done)
            },
            fromExclusive = recoveryIndex,
          )
        }
      )
      recoveringQueue.offer(1).futureValue
      recoveringQueue.offer(2).futureValue
      recoveringQueue.offer(3).futureValue
      recoveringQueue.offer(4).futureValue
      recoveringQueue.offer(5).futureValue
      // now all above are in the internal buffer
      loggerFactory.assertLogs(
        {
          offerGated.trySuccess(())
          recoveringQueue.done.futureValue
        },
        _.errorMessage should include(
          "Program error. The next uncommitted after recovery is not the next element."
        ),
      )
      recoveryIndexRef.get() shouldBe 1
    }

    def fuzzyTest(sleepy: Boolean): Unit = {
      val inputSize = 1000
      def test(): Unit = {
        val sink = new AtomicReference[List[(Long, Int)]](Nil)
        val boomCount = new AtomicInteger(0)
        val recoveringQueue = new RecoveringFutureQueueImpl[Int](
          maxBlockedOffer = 1,
          bufferSize = 20,
          loggerFactory = loggerFactory,
          retryStategy = PekkoUtil.exponentialRetryWithCap(
            minWait = 2,
            multiplier = 2,
            cap = 10,
          ),
          retryAttemptWarnThreshold = 100,
          retryAttemptErrorThreshold = 200,
          uncommittedWarnTreshold = 100,
          recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
        )
        val testF = Future {
          val inputFixture = Iterator.iterate(1)(_ + 1).take(inputSize).toList
          inputFixture.foreach { i =>
            recoveringQueue.offer(i).futureValue
          }
          eventually()(sink.get().size shouldBe inputSize)
          sink.get().reverse shouldBe inputFixture.zip(inputFixture)
          boomCount.get() should be > (5)
        }
        if (sleepy) Threading.sleep(Random.nextLong(4))
        recoveringQueue.registerConsumerFactory(commit =>
          Future {
            if (sleepy) Threading.sleep(Random.nextLong(2))
            if (Random.nextLong(6) > 0) throw new Exception("initialization boom")
            val (sourceQueue, sourceDone) = Source
              .queue[(Long, Int)](20, OverflowStrategy.backpressure, 1)
              .via(BatchN(5, 3))
              .mapAsync(3) { batch =>
                Future {
                  if (sleepy) Threading.sleep(Random.nextLong(4) / 4)
                  if (Random.nextLong(10) == 0) {
                    boomCount.incrementAndGet()
                    throw new Exception("boom")
                  }
                  batch
                }
              }
              .map { batch =>
                batch.foreach(elem => sink.getAndUpdate(elem :: _))
                commit(batch.last._1)
              }
              .toMat(Sink.ignore)(Keep.both)
              .run()
            FutureQueueConsumer(
              futureQueue = new PekkoSourceQueueToFutureQueue(
                sourceQueue = sourceQueue,
                sourceDone = sourceDone,
                loggerFactory = loggerFactory,
              ),
              fromExclusive = sink.get().headOption.map(_._1).getOrElse(0),
            )
          }
        )
        testF.futureValue(PatienceConfiguration.Timeout(Span.Max))
        val shutdownTestF = Future {
          if (sleepy) Threading.sleep(Random.nextLong(10))
          Iterator
            .iterate(inputSize + 1)(_ + 1)
            .take(inputSize)
            .foreach { i =>
              recoveringQueue.offer(i).futureValue
            }
        }
        if (sleepy) Threading.sleep(Random.nextLong(100))
        recoveringQueue.shutdown()
        recoveringQueue.done.futureValue
        shutdownTestF.failed.futureValue.getMessage should include(
          "Cannot offer new elements to the queue, after shutdown is initiated"
        )
        sink.get().reverse shouldBe Range.inclusive(1, sink.get().size).map(i => i -> i).toList
      }

      loggerFactory.assertLogsSeq(
        SuppressionRule.LoggerNameContains("RecoveringFutureQueueImpl") &&
          SuppressionRule.Level(org.slf4j.event.Level.WARN)
      )(
        Range
          .inclusive(
            1,
            6,
          ) // high parallelism is not high load: this test is waiting most of the time
          .map(_ => Future(Range.inclusive(1, 2).foreach(_ => test())))
          .foreach(_.futureValue(PatienceConfiguration.Timeout(Span.Max))),
        logentries =>
          logentries.foldLeft(succeed) { case (_, entry) =>
            if (entry.level == org.slf4j.event.Level.WARN) {
              entry.warningMessage should include(
                "blocked offer calls pending at the time of the shutdown. It is recommended that shutdown gracefully"
              )
            } else succeed
          },
      )
    }

    "work correctly for a fuzzy integration test with fuzzy delays case with many recovery cycles, and fuzzy shutdown" in assertAllStagesStopped {
      fuzzyTest(sleepy = true)
    }

    "work correctly for a fuzzy integration test without fuzzy delays case with many recovery cycles, and fuzzy shutdown" in assertAllStagesStopped {
      fuzzyTest(sleepy = false)
    }

    "squeeze trough 1M items in 100 seconds (10K/sec) with even small batch sizes in PekkoStream" in assertAllStagesStopped {
      val inputSize = 1000 * 1000
      val recoveringQueue = new RecoveringFutureQueueImpl[Int](
        maxBlockedOffer = 1,
        bufferSize = 20,
        loggerFactory = loggerFactory,
        retryStategy = PekkoUtil.exponentialRetryWithCap(
          minWait = 2,
          multiplier = 2,
          cap = 10,
        ),
        retryAttemptWarnThreshold = 100,
        retryAttemptErrorThreshold = 200,
        uncommittedWarnTreshold = 100,
        recoveringQueueMetrics = RecoveringQueueMetrics.NoOp,
      )
      recoveringQueue.registerConsumerFactory(commit =>
        Future {
          val (sourceQueue, sourceDone) = Source
            .queue[(Long, Int)](20, OverflowStrategy.backpressure, 1)
            .via(BatchN(5, 3))
            .mapAsync(3) { batch =>
              Future {
                batch
              }
            }
            .map { batch =>
              commit(batch.last._1)
            }
            .toMat(Sink.ignore)(Keep.both)
            .run()
          FutureQueueConsumer(
            futureQueue = new PekkoSourceQueueToFutureQueue(
              sourceQueue = sourceQueue,
              sourceDone = sourceDone,
              loggerFactory = loggerFactory,
            ),
            fromExclusive = 0,
          )
        }
      )
      val start = System.nanoTime()
      Iterator
        .iterate(1)(_ + 1)
        .take(inputSize)
        .foreach(i => recoveringQueue.offer(i).futureValue)
      val end = System.nanoTime()
      recoveringQueue.shutdown()
      recoveringQueue.done.futureValue
      val testTookMillis = (end - start) / 1000 / 1000
      logger.info(s"1M elem processing took $testTookMillis millis")
      testTookMillis should be < (100000L)
    }
  }

  "FutureQueuePullProxy" should {

    val iterations = {
      def recursionThrows(i: Int, limit: Int): Int =
        if (i > limit) i
        else recursionThrows(i + 1, limit) + 1
      val atomicInteger = new AtomicInteger(10)
      Future(try {
        Iterator
          .continually {
            recursionThrows(0, atomicInteger.get())
            atomicInteger.accumulateAndGet(10, _ * _)
          }
          .foreach(_ => ())
        fail("StackOverflowError expected, but infinite loop ended unexpectedly")
      } catch {
        case _: StackOverflowError => ()
      }).futureValue
      atomicInteger.get()
    }
    logger.info(s"Found stack-overflow recursion depth: $iterations")

    "not throw StackOverflowError, even if asynchronous iterations are running on the same thread" in assertAllStagesStopped {
      val pullCounter = new AtomicInteger(0)
      new FutureQueuePullProxy[Int](
        initialEndIndex = 0,
        pull = _ => {
          if (pullCounter.incrementAndGet() > iterations) None
          else Some(10)
        },
        delegate = new FutureQueue[(Long, Int)] {
          private val shutdownPromise = Promise[Done]()

          override def offer(elem: (Long, Int)): Future[Done] =
            Future.successful(Done)

          override def shutdown(): Unit =
            shutdownPromise.trySuccess(Done)

          override def done: Future[Done] =
            shutdownPromise.future
        },
        loggerFactory = loggerFactory,
      )
      // this means that actually the constructor is already executed all the push-pull-iterations
      pullCounter.incrementAndGet() should be > (iterations)
    }
  }

  "IndexingFutureQueue" should {

    "index correctly from the next element" in assertAllStagesStopped {
      val offered = new AtomicReference[(Long, Int)]
      val testee = new IndexingFutureQueue[Int](
        futureQueueConsumer = FutureQueueConsumer(
          futureQueue = new FutureQueue[(Long, Int)] {
            private val shutdownPromise = Promise[Done]()

            override def offer(elem: (Long, Int)): Future[Done] = {
              offered.set(elem)
              Future.successful(Done)
            }

            override def shutdown(): Unit =
              shutdownPromise.trySuccess(Done)

            override def done: Future[Done] =
              shutdownPromise.future
          },
          fromExclusive = 15L,
        )
      )
      testee.offer(16).futureValue
      offered.get shouldBe (16L -> 16)
      testee.shutdown()
      testee.done.futureValue
    }

    "fail if used concurrently" in assertAllStagesStopped {
      val releaseOffer = Promise[Done]()
      val testee = new IndexingFutureQueue[Int](
        futureQueueConsumer = FutureQueueConsumer(
          futureQueue = new FutureQueue[(Long, Int)] {
            private val shutdownPromise = Promise[Done]()

            override def offer(elem: (Long, Int)): Future[Done] =
              releaseOffer.future

            override def shutdown(): Unit =
              shutdownPromise.trySuccess(Done)

            override def done: Future[Done] =
              shutdownPromise.future
          },
          fromExclusive = 15L,
        )
      )
      val blockedOffer = testee.offer(16)
      testee.offer(17).failed.futureValue.getMessage should include(
        "IndexingFutureQueue should be used sequentially"
      )
      blockedOffer.isCompleted shouldBe false
      releaseOffer.trySuccess(Done)
      blockedOffer.futureValue
      testee.offer(16).futureValue
      testee.shutdown()
      testee.done.futureValue
    }

    "maintain order in indexing across concurrent users" in assertAllStagesStopped {
      val elems = Range(0, 10).toList
      val delegateQueue = new AtomicReference[Vector[(Long, (Int, Int))]](Vector.empty)
      val testee = new IndexingFutureQueue[(Int, Int)](
        futureQueueConsumer = FutureQueueConsumer(
          futureQueue = new FutureQueue[(Long, (Int, Int))] {
            private val shutdownPromise = Promise[Done]()

            override def offer(elem: (Long, (Int, Int))): Future[Done] = {
              delegateQueue.accumulateAndGet(Vector(elem), _ ++ _)
              Future {
                Threading.sleep(Random.nextLong(3))
                Done
              }
            }

            override def shutdown(): Unit =
              shutdownPromise.trySuccess(Done)

            override def done: Future[Done] =
              shutdownPromise.future
          },
          fromExclusive = 0L,
        )
      )
      def asyncPush(id: Int): Future[Unit] = Future(
        elems.foreach { elem =>
          Threading.sleep(Random.nextLong(2))
          def offer(): Future[Done] =
            testee.offer(id -> elem).recoverWith { _ =>
              Threading.sleep(Random.nextLong(2))
              offer()
            }
          offer().futureValue
        }
      )
      List(
        asyncPush(1),
        asyncPush(2),
        asyncPush(3),
      ).foreach(_.futureValue)
      testee.shutdown()
      testee.done.futureValue
      def verifyId(id: Int) =
        delegateQueue.get().filter(_._2._1 == id).map(_._2) shouldBe elems.map(id -> _)
      verifyId(1)
      verifyId(2)
      verifyId(3)
      delegateQueue.get().map(_._1) shouldBe Range(1, 31).map(_.toLong).toVector
    }
  }
}

object PekkoUtilTest {
  def withNoOpKillSwitch[A](value: A): WithKillSwitch[A] = WithKillSwitch(value)(noOpKillSwitch)

  implicit val eqKillSwitch: Eq[KillSwitch] = Eq.fromUniversalEquals[KillSwitch]

  /** A dedicated [[cats.Eq]] instance for [[com.digitalasset.canton.util.PekkoUtil.WithKillSwitch]]
    * that takes the kill switch into account, unlike the default equality method on
    * [[com.digitalasset.canton.util.PekkoUtil.WithKillSwitch]].
    */
  implicit def eqWithKillSwitch[A: Eq]: Eq[WithKillSwitch[A]] =
    (x: WithKillSwitch[A], y: WithKillSwitch[A]) =>
      Eq[A].eqv(x.value, y.value) && Eq[KillSwitch].eqv(x.killSwitch, y.killSwitch)

  implicit def arbitraryChecked[A: Arbitrary]: Arbitrary[WithKillSwitch[A]] =
    Arbitrary(Arbitrary.arbitrary[A].map(withNoOpKillSwitch))
}
