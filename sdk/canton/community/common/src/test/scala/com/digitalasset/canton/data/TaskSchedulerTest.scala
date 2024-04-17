// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.parallel.*
import com.daml.metrics
import com.daml.metrics.api.MetricHandle.Gauge
import com.daml.metrics.api.MetricHandle.Gauge.SimpleCloseableGauge
import com.daml.metrics.api.noop.NoOpCounter
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.{BaseTest, SequencerCounter}
import org.scalatest.wordspec.AsyncWordSpec

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Random

class TaskSchedulerTest extends AsyncWordSpec with BaseTest {
  import TaskSchedulerTest.*
  import com.digitalasset.canton.data.CantonTimestamp.ofEpochMilli

  private lazy val metrics = new MockTaskSchedulerMetrics()

  "TaskScheduler" should {
    val EPOCH = CantonTimestamp.Epoch

    "correctly order tasks and barriers" in {
      final case class TaskData(
          timestamp: CantonTimestamp,
          sequencerCounter: SequencerCounter,
          kind: Int,
      )
      sealed trait TickOrTask extends Product with Serializable {
        def sequencerCounter: SequencerCounter
      }
      final case class Tick(sequencerCounter: SequencerCounter, timestamp: CantonTimestamp)
          extends TickOrTask
      object Tick {
        def apply(args: (SequencerCounter, CantonTimestamp)): Tick = Tick(args._1, args._2)
      }
      final case class Task(data: TaskData, taskIndex: Int) extends TickOrTask {
        override def sequencerCounter: SequencerCounter = data.sequencerCounter
      }
      object Task {
        def apply(args: (TaskData, Int)): Task = Task(args._1, args._2)
      }

      val tasksInExecutionOrder: Seq[TaskData] =
        Seq(
          TaskData(ofEpochMilli(3), SequencerCounter(0), Finalization),
          TaskData(ofEpochMilli(3), SequencerCounter(1), Finalization),
          TaskData(ofEpochMilli(3), SequencerCounter(1), Timeout),
          TaskData(ofEpochMilli(3), SequencerCounter(1), Activeness),
          TaskData(ofEpochMilli(3), SequencerCounter(3), Activeness),
          TaskData(ofEpochMilli(4), SequencerCounter(0), Finalization),
          TaskData(ofEpochMilli(4), SequencerCounter(0), Timeout),
          TaskData(ofEpochMilli(4), SequencerCounter(0), Activeness),
        )
      val ticksWithTasks: Map[SequencerCounter, CantonTimestamp] = Map(
        SequencerCounter(0) -> ofEpochMilli(0),
        SequencerCounter(1) -> ofEpochMilli(1),
        SequencerCounter(3) -> ofEpochMilli(3),
      )
      val ticksWithoutTasks: Map[SequencerCounter, CantonTimestamp] = Map(
        SequencerCounter(2) -> ofEpochMilli(2),
        SequencerCounter(4) -> ofEpochMilli(5),
      )

      val allTicks = ticksWithTasks ++ ticksWithoutTasks
      val barriers: Seq[CantonTimestamp] = Seq(ofEpochMilli(2), ofEpochMilli(4), ofEpochMilli(5))

      val indexedChanges = ticksWithoutTasks.toSeq.map(Tick.apply) ++
        tasksInExecutionOrder.zipWithIndex.map(Task.apply)

      val rand = new Random(1234567890L)

      // test a random selection of 1/720 of all permutations (the same permutation may be picked several times)
      val repetitions = (7 until indexedChanges.size).product
      (0 until repetitions).toList
        .parTraverse_ { _ =>
          val shuffled = rand.shuffle(indexedChanges)
          val taskScheduler =
            new TaskScheduler(
              SequencerCounter(0),
              CantonTimestamp.MinValue,
              TestTaskOrdering,
              metrics,
              timeouts,
              loggerFactory,
              futureSupervisor,
            )
          val executionOrder = mutable.Queue.empty[Int]

          val barrierFutures = barriers.map(timestamp => taskScheduler.scheduleBarrier(timestamp))

          val barriersWithFutures =
            barriers.zip(barrierFutures).map { case (timestamp, optFuture) =>
              assert(optFuture.isDefined, s"Barrier future at $timestamp was None")
              (timestamp, optFuture.value)
            }

          val ticksAdded = mutable.Set[SequencerCounter]()

          @tailrec def firstGapFrom(base: SequencerCounter): SequencerCounter =
            if (ticksAdded.contains(base)) firstGapFrom(base + 1) else base

          def checkBarriers(): Future[Unit] = {
            val missingTick = firstGapFrom(SequencerCounter(0))
            if (missingTick > SequencerCounter(0)) {
              val timestamp = allTicks(missingTick - 1)
              barriersWithFutures.parTraverse_ { case (barrierTimestamp, barrierCompletion) =>
                if (barrierTimestamp <= timestamp) {
                  barrierCompletion
                } else {
                  assert(
                    !barrierCompletion.isCompleted,
                    s"Barrier $barrierTimestamp is not completed at $timestamp",
                  )
                }
              }
            } else Future.unit
          }

          val tasks = List.newBuilder[TestTask]

          for {
            _ <- MonadUtil.sequentialTraverse_(shuffled.zipWithIndex) {
              case (Task(TaskData(ts, sc, kind), taskCounter), idx) =>
                val task = new TestTask(executionOrder, taskCounter, ts, sc, kind)
                tasks += task
                taskScheduler.scheduleTask(task)

                // If this was the final task for the sequencer counter, then add the sequencer counter's time as a tick.
                if (shuffled.indexWhere(_.sequencerCounter == sc, idx + 1) == -1) {
                  val timestamp = ticksWithTasks(sc)
                  taskScheduler.addTick(sc, timestamp)
                  ticksAdded += sc
                  checkBarriers()
                } else Future.unit
              case (Tick(sc, ts), idx) =>
                taskScheduler.addTick(sc, ts)
                ticksAdded += sc
                checkBarriers()
            }
            _ <- tasks.result().parTraverse_(_.done())
          } yield {
            assert(
              executionOrder.toSeq == tasksInExecutionOrder.indices,
              s"shuffling ${shuffled.map(_.sequencerCounter)}",
            )
          }
        }
        .map(_ => succeed)
    }

    "process tasks and complete barriers when they are ready" in {
      val taskScheduler =
        new TaskScheduler(
          SequencerCounter(0),
          CantonTimestamp.MinValue,
          TestTaskOrdering,
          metrics,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )
      val executionOrder = mutable.Queue.empty[Int]
      val waitPromise = Promise[Unit]()
      val task0 = TestTask(executionOrder, 0, ofEpochMilli(1), SequencerCounter(0), Activeness)
      val task1 =
        TestTask(
          executionOrder,
          1,
          ofEpochMilli(2),
          SequencerCounter(0),
          Activeness,
          waitPromise.future,
        )
      taskScheduler.scheduleTask(task1)
      taskScheduler.scheduleTask(task0)
      val barrier1 = taskScheduler.scheduleBarrier(ofEpochMilli(1))
      val barrier2 = taskScheduler.scheduleBarrier(ofEpochMilli(2))
      val barrier3 = taskScheduler.scheduleBarrier(ofEpochMilli(3))
      taskScheduler.addTick(SequencerCounter(1), ofEpochMilli(2))
      taskScheduler.addTick(SequencerCounter(0), ofEpochMilli(1))
      val barrier0 = taskScheduler.scheduleBarrier(ofEpochMilli(1))
      assert(barrier0.isEmpty, s"Barrier is before observed time of the task scheduler")
      for {
        _ <- task0.done()
        _ = assert(executionOrder.toSeq == Seq(0), "only the first task has run")
        _ <- barrier1.value
        _ <- barrier2.value // complete the barrier even if we can't execute the task
        _ = waitPromise.success(())
        _ <- task1.done()
        _ = assert(executionOrder.toSeq == Seq(0, 1), "the second task has run")
        _ = assert(barrier3.exists(!_.isCompleted), "The third barrier is not reached")
      } yield succeed
    }

    "complain about timestamps before head" in {
      val taskScheduler =
        new TaskScheduler(
          SequencerCounter(0),
          EPOCH,
          TestTaskOrdering,
          metrics,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(1), EPOCH.minusMillis(1)),
        _.getMessage shouldBe "Timestamp 1969-12-31T23:59:59.999Z for sequence counter 1 is not after current time 1970-01-01T00:00:00Z.",
      )
    }

    "complain about non-increasing timestamps on ticks" in {
      val taskScheduler =
        new TaskScheduler(
          SequencerCounter(0),
          EPOCH,
          TestTaskOrdering,
          metrics,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )

      taskScheduler.addTick(SequencerCounter(1), ofEpochMilli(2))
      taskScheduler.addTick(SequencerCounter(7), ofEpochMilli(4))
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(3), ofEpochMilli(1)),
        _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.001Z for sequencer counter 3 is not after timestamp 1970-01-01T00:00:00.002Z of an earlier sequencer counter.",
      ) // before previous counter
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(4), ofEpochMilli(2)),
        _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.002Z for sequencer counter 4 is not after timestamp 1970-01-01T00:00:00.002Z of an earlier sequencer counter.",
      ) // same time as previous counter
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(5), ofEpochMilli(4)),
        _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.004Z for sequencer counter 5 is not before timestamp 1970-01-01T00:00:00.004Z of a later sequencer counter.",
      ) // same as next counter
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(6), ofEpochMilli(5)),
        _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.005Z for sequencer counter 6 is not before timestamp 1970-01-01T00:00:00.004Z of a later sequencer counter.",
      ) // after next counter

      taskScheduler.addTick(SequencerCounter(0), ofEpochMilli(1))
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(2), ofEpochMilli(1).addMicros(1L)),
        _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.001001Z for sequence counter 2 is not after current time 1970-01-01T00:00:00.002Z.",
      )
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(0), ofEpochMilli(3)),
        _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.003Z for outdated sequencer counter 0 is after current time 1970-01-01T00:00:00.002Z.",
      ) // before head, but after latest observed time
    }

    "ignore signals before head" in {
      val taskScheduler =
        new TaskScheduler(
          SequencerCounter(0),
          EPOCH,
          TestTaskOrdering,
          metrics,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )

      taskScheduler.addTick(SequencerCounter(0), ofEpochMilli(2))
      taskScheduler.addTick(SequencerCounter(1), ofEpochMilli(3))
      taskScheduler.addTick(SequencerCounter(0), ofEpochMilli(2))
      taskScheduler.addTick(
        SequencerCounter(0),
        ofEpochMilli(1),
      ) // don't throw even if we signal a different time
      succeed
    }

    "complain about adding a sequencer counter twice with different times" in {
      val taskScheduler =
        new TaskScheduler(
          SequencerCounter(0),
          EPOCH,
          TestTaskOrdering,
          metrics,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )

      taskScheduler.addTick(SequencerCounter(1), ofEpochMilli(10))
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter(1), ofEpochMilli(20)),
        _.getMessage shouldBe "Timestamp 1970-01-01T00:00:00.020Z for sequencer counter 1 differs from timestamp 1970-01-01T00:00:00.010Z that was signalled before.",
      )
      taskScheduler.addTick(SequencerCounter(1), ofEpochMilli(10))
      succeed
    }

    "complain about Long.MaxValue as a sequencer counter" in {
      val taskScheduler =
        new TaskScheduler(
          SequencerCounter(0),
          EPOCH,
          TestTaskOrdering,
          metrics,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.addTick(SequencerCounter.MaxValue, CantonTimestamp.MaxValue),
        _.getMessage shouldBe "Sequencer counter Long.MaxValue signalled to task scheduler.",
      )
    }

    "scheduled tasks must be after current time" in {
      val taskScheduler =
        new TaskScheduler(
          SequencerCounter(10),
          EPOCH,
          TestTaskOrdering,
          metrics,
          timeouts,
          loggerFactory,
          futureSupervisor,
        )
      val queue = mutable.Queue.empty[Int]

      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.scheduleTask(new TestTask(queue, 1, ofEpochMilli(-1), SequencerCounter(10))),
        _.getMessage should fullyMatch regex "Timestamp .* of new task TestTask.* lies before current time .*\\.",
      )

      taskScheduler.scheduleTask(
        new TestTask(
          queue,
          2,
          ofEpochMilli(3),
          SequencerCounter(10),
          waitFor = Promise[Unit]().future,
        )
      )
      taskScheduler.addTick(SequencerCounter(11), ofEpochMilli(5))
      taskScheduler.addTick(SequencerCounter(10), ofEpochMilli(1))
      // Time advances even if a task cannot be processed yet
      loggerFactory.assertInternalError[IllegalArgumentException](
        taskScheduler.scheduleTask(new TestTask(queue, 3, ofEpochMilli(4), SequencerCounter(10))),
        _.getMessage should fullyMatch regex "Timestamp .* of new task TestTask.* lies before current time .*\\.",
      )
    }

  }
}

object TaskSchedulerTest {

  class MockTaskSchedulerMetrics extends TaskSchedulerMetrics {
    val prefix: MetricName = MetricName("test")
    override val sequencerCounterQueue: metrics.api.MetricHandle.Counter = NoOpCounter(
      prefix :+ "counter"
    )

    override def taskQueue(size: () => Int): Gauge.CloseableGauge =
      SimpleCloseableGauge(MetricInfo(MetricName("test"), "", MetricQualification.Debug), () => ())
  }

  val Finalization: Int = 0
  val Timeout: Int = 1
  val Activeness: Int = 2

  private final case class TestTask(
      queue: mutable.Queue[Int],
      seqNo: Int,
      override val timestamp: CantonTimestamp,
      override val sequencerCounter: SequencerCounter,
      kind: Int = Activeness,
      waitFor: Future[Unit] = Future.unit,
  )(implicit val ec: ExecutionContext, val traceContext: TraceContext)
      extends TaskScheduler.TimedTask {

    private val donePromise: Promise[Unit] = Promise[Unit]()

    def done(): Future[Unit] = donePromise.future

    override def perform(): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.outcomeF {
      waitFor.map { _ =>
        queue.enqueue(seqNo)
        donePromise.success(())
        ()
      }
    }

    override def pretty: Pretty[this.type] = adHocPrettyInstance

    override def close(): Unit = ()
  }

  private val TestTaskOrdering: Ordering[TestTask] =
    Ordering.by[TestTask, (Int, SequencerCounter)](task => (task.kind, task.sequencerCounter))
}
