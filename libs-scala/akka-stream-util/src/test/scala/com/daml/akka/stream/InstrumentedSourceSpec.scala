// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.akka.stream

import akka.stream.{OverflowStrategy, QueueOfferResult}
import akka.stream.scaladsl.{Keep, Sink}
import com.codahale.metrics.Counter
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.{AsyncFlatSpec, Matchers}

import scala.concurrent.Future

final class InstrumentedSourceSpec extends AsyncFlatSpec with Matchers with AkkaBeforeAndAfterAll {

  behavior of "InstrumentedSource.queue"

  it should "correctly enqueue and track the buffer saturation" in {

    val bufferSize = 10000

    val maxBuffered = new InstrumentedSourceSpec.MaxValueCounter()

    // The throttling allows us to flood the source queue initially
    // and to reliably poll the instrumentation for testing
    val (source, sink) =
      InstrumentedSource
        .queue[Int](bufferSize, OverflowStrategy.backpressure, maxBuffered)
        .toMat(Sink.seq)(Keep.both)
        .run()

    // The values in the queue are not relevant, hence the random generation
    val input = Seq.fill(bufferSize)(util.Random.nextInt)

    for {
      results <- Future.sequence(input.map(source.offer))
      _ = source.complete()
      output <- sink
    } yield {
      all(results) shouldBe QueueOfferResult.Enqueued
      output shouldEqual input
      maxBuffered.getCount shouldEqual bufferSize
    }
  }

  it should "track the buffer saturation correctly when dropping items" in {

    val bufferSize = 10000

    // Due to differences in scheduling, we accept that the highest
    // possible recorded saturation value to be more or less equal
    // to the buffer size. See the ScalaDoc of `InstrumentedQueue.source`
    // for more details
    val acceptanceTolerance = bufferSize * 0.05
    val lowAcceptanceThreshold = bufferSize - acceptanceTolerance
    val highAcceptanceThreshold = bufferSize + acceptanceTolerance

    val maxBuffered = new InstrumentedSourceSpec.MaxValueCounter()

    // The throttling allows us to flood the source queue initially
    // and to reliably poll the instrumentation for testing
    val (source, termination) =
      InstrumentedSource
        .queue[Int](bufferSize, OverflowStrategy.dropNew, maxBuffered)
        .watchTermination()(Keep.both)
        .toMat(Sink.ignore)(Keep.left)
        .run()

    // We to enqueue double the items that fit in the buffer
    // so to force items to be dropped from the queue
    val inputSize = bufferSize * 2
    val input = Seq.fill(inputSize)(util.Random.nextInt)

    for {
      results <- Future.sequence(input.map(source.offer))
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
    }
  }

}

object InstrumentedSourceSpec {

  // For testing only, this counter will never decrease
  // so that we can test the maximum value read
  private final class MaxValueCounter extends Counter {

    override def dec(): Unit = ()

    override def dec(n: Long): Unit = ()

  }

}
