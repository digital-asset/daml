// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.store

import cats.instances.future.catsStdInstancesForFuture
import com.digitalasset.canton.concurrent.{ExecutorServiceExtensions, Threading}
import com.digitalasset.canton.config.DefaultProcessingTimeouts
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.pruning.{PruningPhase, PruningStatus}
import com.digitalasset.canton.store.memory.InMemoryPrunableByTime
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.{MonadUtil, OptionUtil}
import com.digitalasset.canton.version.HasTestCloseContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, TestMetrics}
import org.scalatest.wordspec.{AsyncWordSpecLike, FixtureAsyncWordSpec}
import org.scalatest.{Assertion, FutureOutcome}

import java.time.Instant
import java.util.concurrent.atomic.AtomicReference
import scala.Ordered.orderingToOrdered
import scala.concurrent.{ExecutionContext, Future}

trait PrunableByTimeTest {
  this: AsyncWordSpecLike & BaseTest & TestMetrics =>

  def prunableByTime(mkPrunable: ExecutionContext => PrunableByTime): Unit = {

    val ts = CantonTimestamp.assertFromInstant(Instant.parse("2019-04-04T10:00:00.00Z"))
    val ts2 = ts.addMicros(1)
    val ts3 = ts2.addMicros(1)

    implicit val closeContext = HasTestCloseContext.makeTestCloseContext(logger)

    "pruning timestamps increase" in {
      val acs = mkPrunable(executionContext)
      for {
        status0 <- acs.pruningStatus
        _ <- acs.prune(ts)
        status1 <- acs.pruningStatus
        _ <- acs.prune(ts3)
        status2 <- acs.pruningStatus
        _ <- acs.prune(ts2)
        status3 <- acs.pruningStatus
      } yield {
        assert(status0 == None, "No pruning status initially")
        assert(
          status1.contains(PruningStatus(PruningPhase.Completed, ts, Some(ts))),
          s"Pruning status at $ts",
        )
        assert(
          status2.contains(PruningStatus(PruningPhase.Completed, ts3, Some(ts3))),
          s"Pruniadvances to $ts3",
        )
        assert(
          status3.contains(PruningStatus(PruningPhase.Completed, ts3, Some(ts3))),
          s"Pruning status remains at $ts3",
        )
      }
    }

    "pruning timestamps advance under concurrent pruning" in {
      val parallelEc = Threading.newExecutionContext(
        "pruning-parallel-ec",
        noTracingLogger,
        executorServiceMetrics,
      )
      val prunable = mkPrunable(parallelEc)
      val iterations = 100

      def timestampForIter(iter: Int): CantonTimestamp = CantonTimestamp.ofEpochSecond(iter.toLong)
      def prune(iter: Int): Future[Unit] =
        prunable.prune(timestampForIter(iter))

      val lastRead = new AtomicReference[Option[PruningStatus]](None)

      def read(): Future[Int] =
        prunable.pruningStatus.map { statusO =>
          val previousO = lastRead.getAndAccumulate(
            statusO,
            OptionUtil.mergeWith(_, _)(Ordering[PruningStatus].max),
          )
          assert(
            previousO.forall(previous => statusO.exists(previous <= _)),
            s"PrunableByTime pruning status decreased from $previousO to $statusO",
          )
          if (statusO.exists(_.phase == PruningPhase.Started)) 1 else 0
        }(parallelEc)

      val pruningsF = Future.traverse((1 to iterations).toList)(prune)(List, parallelEc)
      val readingsF = MonadUtil.sequentialTraverse(1 to iterations)(_ => read())(
        catsStdInstancesForFuture(parallelEc)
      )

      val testF = for {
        _ <- pruningsF
        readings <- readingsF
        statusEnd <- prunable.pruningStatus
      } yield {
        logger.info(s"concurrent pruning test had ${readings.sum} intermediate readings")
        val ts = timestampForIter(iterations)
        assert(
          statusEnd.contains(PruningStatus(PruningPhase.Completed, ts, Some(ts)))
        )
      }
      testF.thereafter { _ =>
        Lifecycle.close(
          ExecutorServiceExtensions(parallelEc)(logger, DefaultProcessingTimeouts.testing)
        )(logger)
      }
    }

  }

}

class PrunableByTimeLogicTest
    extends FixtureAsyncWordSpec
    with BaseTest
    with HasExecutionContext
    with HasTestCloseContext {

  override def withFixture(test: OneArgAsyncTest): FutureOutcome = {
    val env = new FixtureParam(loggerFactory)
    complete {
      withFixture(test.toNoArgAsyncTest(env))
    } lastly {}
  }
  final class FixtureParam(
      override val loggerFactory: NamedLoggerFactory
  ) extends InMemoryPrunableByTime
      with NamedLogging {

    override protected def batchingParameters: Option[PrunableByTimeParameters] = Some(
      PrunableByTimeParameters.testingParams
    )

    override protected implicit val ec: ExecutionContext =
      PrunableByTimeLogicTest.this.directExecutionContext

    // variable used to record which pruning intervals where invoked
    val pruningRequests =
      new AtomicReference[Seq[(CantonTimestamp, Option[CantonTimestamp])]](Seq.empty)
    // variable used to signal how many rows were pruning
    // depending on the return value, the intervals will be either shortened or extended
    val returnValues = new AtomicReference[Seq[Int]](Seq.empty)

    override protected def kind: String = "fixture"

    override protected[canton] def doPrune(
        limit: CantonTimestamp,
        lastPruning: Option[CantonTimestamp],
    )(implicit traceContext: TraceContext): Future[Int] = {
      pruningRequests.updateAndGet(_ :+ (limit, lastPruning))
      Future.successful(returnValues.getAndUpdate(_.drop(1)).headOption.getOrElse(0))
    }

    def assertRequests(from: CantonTimestamp, until: CantonTimestamp, buckets: Long): Assertion = {
      val requests = pruningRequests.getAndSet(Seq.empty)
      logger.debug(s"Had requests ${requests}")
      requests should have length buckets
      requests.headOption.flatMap { case (_, from) => from } should contain(from)
      requests.lastOption.map { case (until, _) => until } should contain(until)
      // check that series is continuous
      requests.foldLeft(None: Option[CantonTimestamp]) {
        case (Some(prevNext), (next, Some(prev))) =>
          prevNext shouldBe prev
          Some(next)
        case (_, (next, _)) => Some(next)
      }
      forAll(requests) { case (next, prev) =>
        assert(prev.valueOrFail(s"prev is empty for ${next}") < next)
      }
    }

    def runPruning(
        increments: Seq[Int],
        mult: Long = 10L,
    ): Future[Unit] = {
      MonadUtil
        .sequentialTraverse_(increments) { counter =>
          pruningRequests.getAndSet(Seq.empty)
          prune(ts0.plusSeconds(counter * mult))
        }
    }

  }

  private lazy val ts0 = CantonTimestamp.Epoch
  private lazy val ts1 = ts0.plusSeconds(60)
  private lazy val ts2 = ts1.plusSeconds(10)

  "dynamic interval sizing" should {
    "compute right number of intervals with starting value" in { f =>
      f.returnValues.set(Seq.fill(20)(10))
      for {
        // first pruning hits empty state, therefore 1 pruning query
        _ <- f.prune(ts0)
        _ = {
          f.pruningRequests.getAndSet(Seq.empty) should have length 1
        }
        // pruning in 1 minute interval with default of max 5 seconds interval with hit the 10 bucket limit
        _ <- f.prune(ts1)
        _ = f.assertRequests(ts0, ts1, 10)
        // subsequent pruning in 10 sec interval will be spaced into 2 buckets of 5s
        _ <- f.prune(ts2)
        _ = f.assertRequests(ts1, ts2, 2)
      } yield {
        succeed
      }
    }
    "increase interval size if batches are small" in { f =>
      f.returnValues.set(Seq.fill(200)(1))
      f.runPruning((0 to 10))
        .map { _ =>
          // we started with 5s intervals. after a few iterations only returning small batches, we should just have 1 bucket
          f.assertRequests(ts0.plusSeconds(9 * 10), ts0.plusSeconds(10 * 10), 1)
        }
    }
    "reduce interval size if batches are too big" in { f =>
      f.returnValues.set(Seq.fill(200)(50))
      f.runPruning((0 to 10))
        .map { _ =>
          // we started with 5s intervals. after a few iterations returning large batches, we should have increased to the max num buckets
          f.assertRequests(ts0.plusSeconds(9 * 10), ts0.plusSeconds(10 * 10), 10)
        }
    }
    "don't increase interval beyond actual invocation interval" in { f =>
      f.returnValues.set(Seq.fill(200)(1))
      for {
        // first, prune 10 times every 1s
        _ <- f.runPruning((0 to 10), mult = 1)
        // now, if we prune for a larger interval, we shouldn't have a larger step size as the pruning interval
        // was shorter than the step size
        _ <- f.runPruning(Seq(20), mult = 1)
      } yield {
        f.assertRequests(ts0.plusSeconds(10), ts0.plusSeconds(20), 2)
      }
    }
    "limit number of buckets during big jumps" in { f =>
      f.returnValues.set(Seq.fill(200)(10))
      for {
        _ <- f.prune(ts0)
        _ = { f.pruningRequests.set(Seq.empty) }
        _ <- f.prune(ts0.plusSeconds(1000))
      } yield {
        f.assertRequests(ts0, ts0.plusSeconds(1000), 10)
      }
    }
  }

}
