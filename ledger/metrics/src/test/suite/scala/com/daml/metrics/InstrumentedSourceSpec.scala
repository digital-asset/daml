// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.metrics

import scala.util.chaining._
import java.util.concurrent.atomic.AtomicLong

import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.{OverflowStrategy, QueueOfferResult}
import com.codahale.metrics.{Counter, Timer}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import com.daml.metrics.InstrumentedSourceSpec.SamplingCounter
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.{Future, Promise}
import scala.concurrent.duration.{DurationInt, FiniteDuration}

final class InstrumentedSourceSpec extends AsyncFlatSpec with Matchers with AkkaBeforeAndAfterAll {

  behavior of "InstrumentedSource.queue"

  it should "correctly enqueue and track the buffer saturation" in {

    val bufferSize = 500

    val capacityCounter = new Counter()
    val maxBuffered = new InstrumentedSourceSpec.MaxValueCounter()
    val delayTimer = new Timer()

    val (source, sink) =
      InstrumentedSource
        .queue[Int](
          bufferSize,
          OverflowStrategy.backpressure,
          capacityCounter,
          maxBuffered,
          delayTimer,
        )
        .toMat(Sink.seq)(Keep.both)
        .run()

    // The values in the queue are not relevant, hence the random generation
    val input = Seq.fill(bufferSize)(util.Random.nextInt())

    for {
      results <- Future.sequence(input.map(source.offer))
      _ = capacityCounter.getCount shouldEqual bufferSize
      _ = source.complete()
      output <- sink
    } yield {
      all(results) shouldBe QueueOfferResult.Enqueued
      output shouldEqual input
      maxBuffered.getCount shouldEqual bufferSize
      capacityCounter.getCount shouldEqual 0
      maxBuffered.decrements.get shouldEqual bufferSize
    }
  }

  it should "correctly measure queue delay" in {
    val capacityCounter = new Counter()
    val maxBuffered = new InstrumentedSourceSpec.MaxValueCounter()
    val delayTimer = new Timer()
    val bufferSize = 2

    val (source, sink) =
      InstrumentedSource
        .queue[Int](16, OverflowStrategy.backpressure, capacityCounter, maxBuffered, delayTimer)
        .mapAsync(1) { x =>
          akka.pattern.after(5.millis, system.scheduler)(Future(x))
        }
        .toMat(Sink.seq)(Keep.both)
        .run()

    val input = Seq.fill(bufferSize)(util.Random.nextInt())

    for {
      result <- Future.sequence(input.map(source.offer))
      _ = source.complete()
      output <- sink
    } yield {
      all(result) shouldBe QueueOfferResult.Enqueued
      output shouldEqual input
      delayTimer.getCount shouldEqual bufferSize
      delayTimer.getSnapshot.getMax should be >= 5.millis.toNanos
    }
  }

  it should "track the buffer saturation correctly when dropping items" in {

    val bufferSize = 500

    // Due to differences in scheduling, we expect the highest
    // possible recorded saturation value to be more or less equal
    // to the buffer size. See the ScalaDoc of `InstrumentedQueue.source`
    // for more details
    val acceptanceTolerance = bufferSize * 0.05
    val lowAcceptanceThreshold = bufferSize - acceptanceTolerance
    val highAcceptanceThreshold = bufferSize + acceptanceTolerance

    val maxBuffered = new InstrumentedSourceSpec.MaxValueCounter()
    val capacityCounter = new Counter()
    val delayTimer = new Timer()

    val stop = Promise[Unit]()

    val (source, termination) =
      InstrumentedSource
        .queue[Int](bufferSize, OverflowStrategy.dropNew, capacityCounter, maxBuffered, delayTimer)
        .mapAsync(1)(_ => stop.future) // Block until completed to overflow queue.
        .watchTermination()(Keep.both)
        .toMat(Sink.ignore)(Keep.left)
        .run()

    // We to enqueue double the items that fit in the buffer
    // so to force items to be dropped from the queue
    val inputSize = bufferSize * 2
    val input = Seq.fill(inputSize)(util.Random.nextInt())

    for {
      results <- Future.sequence(input.map(source.offer))
      _ = capacityCounter.getCount shouldEqual bufferSize
      _ = stop.success(())
      _ = source.complete()
      _ <- termination
    } yield {
      val enqueued = results.count {
        case QueueOfferResult.Enqueued => true
        case _ => false
      }
      val dropped = results.count {
        case QueueOfferResult.Dropped => true
        case _ => false
      }
      inputSize shouldEqual (enqueued + dropped)
      assert(enqueued >= bufferSize)
      assert(dropped <= bufferSize)
      assert(maxBuffered.getCount >= lowAcceptanceThreshold)
      assert(maxBuffered.getCount <= highAcceptanceThreshold)
      capacityCounter.getCount shouldEqual 0

    }
  }

  behavior of "InstrumentedSource.bufferedSource"

  def throttledTest(producerMaxSpeed: Int, consumerMaxSpeed: Int): Future[List[Long]] = {
    val counter = new SamplingCounter(10.millis)
    Source(List.fill(1000)("element"))
      .throttle(producerMaxSpeed, FiniteDuration(10, "millis"))
      .pipe(original =>
        InstrumentedSource.bufferedSource(
          original = original,
          counter = counter,
          size = 100,
        )
      )
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
      sampleAverage(samples) should be > 80.0
      samplePercentage(samples)(_ == 100) should be > 80.0
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

  it should "signal mostly empty buffer if consumer slightly faster" in {
    throttledTest(
      producerMaxSpeed = 10,
      consumerMaxSpeed = 12,
    ) map { samples =>
      sampleAverage(samples) should be < 10.0
      samplePercentage(samples)(_ == 0) should be > 90.0
    }
  }

  it should "signal mostly full buffer if consumer slightly slower" in {
    throttledTest(
      producerMaxSpeed = 10,
      consumerMaxSpeed = 8,
    ) map { samples =>
      sampleAverage(samples) should be > 50.0
      samplePercentage(samples)(_ == 100) should be > 50.0
    }
  }
}

object InstrumentedSourceSpec {

  // For testing only, this counter will never decrease
  // so that we can test the maximum value read
  private final class MaxValueCounter extends Counter {
    val decrements = new AtomicLong(0)

    override def dec(): Unit = {
      val _ = decrements.incrementAndGet()
    }

    override def dec(n: Long): Unit = {
      val _ = decrements.addAndGet(n)
    }

  }

  // For testing only, provides a sampled sequence of the state of the counter until finishSampling is called.
  private final class SamplingCounter(samplingInterval: FiniteDuration) extends Counter {
    private val t = new java.util.Timer()
    private val samples = scala.collection.mutable.ListBuffer[Long]()
    private val task = new java.util.TimerTask {
      def run(): Unit = samples.+=(getCount)
    }
    t.schedule(task, samplingInterval.toMillis, samplingInterval.toMillis)

    def finishSampling(): List[Long] = {
      t.cancel()
      task.cancel()
      samples.result()
    }
  }
}
