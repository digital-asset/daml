// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import org.apache.pekko.stream.QueueOfferResult
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.daml.metrics.InstrumentedGraph._
import com.daml.metrics.InstrumentedGraphSpec.{MaxValueCounter, SamplingCounter}
import com.daml.metrics.api.MetricsContext
import com.daml.metrics.api.noop.NoOpCounter
import com.daml.metrics.api.testing.InMemoryMetricsFactory.{InMemoryCounter, InMemoryTimer}
import com.daml.metrics.api.testing.MetricValues
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Future, Promise}

final class InstrumentedGraphSpec
    extends AsyncFlatSpec
    with Matchers
    with PekkoBeforeAndAfterAll
    with MetricValues {

  behavior of "InstrumentedSource.queue"

  it should "correctly enqueue and measure queue delay" in {
    val capacityCounter = NoOpCounter("capacity")
    val maxBuffered = NoOpCounter("buffered")
    val delayTimer = InMemoryTimer("test", MetricsContext.Empty)
    val bufferSize = 2

    val (source, sink) =
      InstrumentedGraph
        .queue[Int](bufferSize, capacityCounter, maxBuffered, delayTimer)
        .mapAsync(1) { x =>
          org.apache.pekko.pattern.after(5.millis, system.scheduler)(Future(x))
        }
        .toMat(Sink.seq)(Keep.both)
        .run()

    val input = Seq.fill(bufferSize)(util.Random.nextInt())

    val result = input.map(source.offer)
    source.complete()
    sink.map { output =>
      all(result) shouldBe QueueOfferResult.Enqueued
      output shouldEqual input
      delayTimer.count shouldEqual bufferSize
      delayTimer.values.max should be >= 5.millis.toMillis
    }
  }

  it should "track the buffer saturation correctly" in {

    val bufferSize = 500

    // Due to differences in scheduling, we expect the highest
    // possible recorded saturation value to be more or less equal
    // to the buffer size. See the ScalaDoc of `InstrumentedQueue.source`
    // for more details
    val acceptanceTolerance = bufferSize * 0.05
    val lowAcceptanceThreshold = bufferSize - acceptanceTolerance
    val highAcceptanceThreshold = bufferSize + acceptanceTolerance

    val maxBuffered = new MaxValueCounter
    val capacityCounter = InMemoryCounter("test", MetricsContext.Empty)
    val delayTimer = InMemoryTimer("test", MetricsContext.Empty)

    val stop = Promise[Unit]()

    val (source, termination) =
      InstrumentedGraph
        .queue[Int](bufferSize, capacityCounter, maxBuffered, delayTimer)
        .mapAsync(1)(_ => stop.future) // Block until completed to overflow queue.
        .watchTermination()(Keep.both)
        .toMat(Sink.ignore)(Keep.left)
        .run()

    // We to enqueue double the items that fit in the buffer
    // so to force items to be dropped from the queue
    val inputSize = bufferSize * 2
    val input = Seq.fill(inputSize)(util.Random.nextInt())

    val results = input.map(source.offer)
    capacityCounter.value shouldEqual bufferSize
    stop.success(())
    source.complete()
    val enqueued = results.count {
      case QueueOfferResult.Enqueued => true
      case _ => false
    }
    val dropped = results.count {
      case QueueOfferResult.Dropped => true
      case _ => false
    }
    termination.map { _ =>
      inputSize shouldEqual (enqueued + dropped)
      assert(enqueued >= bufferSize)
      assert(dropped <= bufferSize)
      assert(maxBuffered.value >= lowAcceptanceThreshold)
      assert(maxBuffered.value <= highAcceptanceThreshold)
      capacityCounter.value shouldEqual 0
    }
  }

  // this test suite is disabled since it's timing related expectations proven to be very flaky in automated tests
  behavior of s"${classOf[BufferedFlow[_, _, _]].getSimpleName}.buffered"

  def throttledTest(producerMaxSpeed: Int, consumerMaxSpeed: Int): Future[List[Long]] = {
    val counter = new SamplingCounter(10.millis)
    Source(List.fill(1000)("element"))
      .throttle(producerMaxSpeed, FiniteDuration(10, "millis"))
      .buffered(counter, 100)
      .throttle(consumerMaxSpeed, FiniteDuration(10, "millis"))
      .run()
      .map(_ => counter.finishSampling())
  }

  def sampleAverage(samples: List[Long]): Double =
    samples.sum.toDouble / samples.size.toDouble
  def samplePercentage(samples: List[Long])(filter: Long => Boolean): Double =
    samples.count(filter).toDouble / samples.size.toDouble * 100.0

  it should "signal mostly full buffer if slow consumer" in {
    throttledTest(
      producerMaxSpeed = 10,
      consumerMaxSpeed = 5,
    ) map { samples =>
      sampleAverage(samples) should be > 75.0
      samplePercentage(samples)(_ == 100) should be > 75.0
    }
  }

  it should "signal mostly empty buffer if fast consumer" in {
    throttledTest(
      producerMaxSpeed = 10,
      consumerMaxSpeed = 20,
    ) map { samples =>
      sampleAverage(samples) should be < 10.0
      samplePercentage(samples)(_ == 0) should be > 90.0
    }
  }

  it should "signal mostly empty buffer if speeds are aligned" in {
    throttledTest(
      producerMaxSpeed = 10,
      consumerMaxSpeed = 10,
    ) map { samples =>
      sampleAverage(samples) should be < 10.0
      samplePercentage(samples)(_ == 0) should be > 90.0
    }
  }
}

object InstrumentedGraphSpec extends MetricValues {
  // For testing only, this counter will never decrease
  // so that we can test the maximum value read
  private final class MaxValueCounter extends InMemoryCounter("test", MetricsContext.Empty) {
    override def dec(value: Long)(implicit mc: MetricsContext): Unit = ()

  }

  // For testing only, provides a sampled sequence of the state of the counter until finishSampling is called.
  private final class SamplingCounter(samplingInterval: FiniteDuration)
      extends InMemoryCounter("test", MetricsContext.Empty) { self =>
    private val t = new java.util.Timer()
    private val samples = scala.collection.mutable.ListBuffer[Long]()
    private val task = new java.util.TimerTask {
      def run(): Unit = samples.+=(self.value)
    }
    t.schedule(task, samplingInterval.toMillis, samplingInterval.toMillis)

    def finishSampling(): List[Long] = {
      t.cancel()
      task.cancel()
      samples.result()
    }
  }
}
