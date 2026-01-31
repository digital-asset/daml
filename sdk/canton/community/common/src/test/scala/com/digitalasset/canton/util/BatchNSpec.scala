// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import org.apache.pekko.stream.Attributes.InputBuffer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{Attributes, DelayOverflowStrategy}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class BatchNSpec extends AsyncFlatSpec with Matchers with PekkoBeforeAndAfterAll {

  private val MaxBatchWeight = 10
  private val MaxBatchCount = 5

  behavior of s"BatchN in batchMode ${BatchN.MaximizeConcurrency} with equally weighed items"

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = 1 to inputSize
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .batchN(MaxBatchWeight, MaxBatchCount, catchUpMode = BatchN.MaximizeConcurrency)
        .runWith(Sink.seq[Iterable[Int]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "form maximally-sized batches if downstream is slower than upstream" in {
    val inputSize = 100
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchN(MaxBatchWeight, MaxBatchCount, catchUpMode = BatchN.MaximizeConcurrency)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize / MaxBatchWeight)(
        MaxBatchWeight
      )
    }
  }

  it should "form even-sized batches under downstream back-pressure" in {
    val inputSize = 15
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchN(MaxBatchWeight, MaxBatchCount, catchUpMode = BatchN.MaximizeConcurrency)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(5)(
        3
      )
    }
  }

  behavior of s"BatchN in batchMode ${BatchN.MaximizeConcurrency} with weighted items"

  val MaxItemWeight = 10L
  def weightFn1(i: Int): Long = i match {
    case n if (n % 10) == 0 => MaxItemWeight
    case n if (n % 5) == 0 => 2L
    case _ => 1L
  }

  it should "form batches of size 1 under no load even if some items are bigger than the maxBatchWeight" in {
    val inputSize = 100
    val input = 1 to inputSize
    val maxBatchWeight = 8L
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .batchNWeighted(
          maxBatchWeight,
          MaxBatchCount,
          catchUpMode = BatchN.MaximizeConcurrency,
          weightFn = weightFn1,
        )
        .runWith(Sink.seq[Iterable[Int]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "form maximally-sized batches if downstream is slower than upstream" in {
    val inputSize = 120
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNWeighted(
          MaxBatchWeight.toLong,
          MaxBatchCount,
          catchUpMode = BatchN.MaximizeConcurrency,
          weightFn = weightFn1,
        )
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs List
        .fill(inputSize / MaxBatchWeight)(List(9, 1))
        .flatten
    }
  }

  it should "form even-sized batches under downstream back-pressure" in {
    val inputSize = 30
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNWeighted(
          100,
          MaxBatchCount,
          catchUpMode = BatchN.MaximizeConcurrency,
          weightFn = weightFn1,
        )
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs List
        .fill(inputSize / MaxBatchWeight)(List(9, 1))
        .flatten
      // total weight = 60, each batch weights 10, so we expect 6 of them
      batches.map(_.map(weightFn1).sum) should contain theSameElementsAs List.fill(6)(10)
    }
  }

  it should "handle items which are heavier than the full input capacity" in {
    val input = 1 to 1

    val batchesF =
      Source(input)
        .batchNWeighted(
          MaxBatchWeight.toLong,
          MaxBatchCount,
          catchUpMode = BatchN.MaximizeConcurrency,
          weightFn = _ => MaxBatchWeight * MaxBatchCount * 2L,
        )
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array(1)
    }
  }

  it should "utilise the output parallelism as much as possible in case of heavy items" in {
    val inputSize = MaxBatchCount - 1
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchNWeighted(
          MaxBatchWeight.toLong,
          MaxBatchCount,
          catchUpMode = BatchN.MaximizeConcurrency,
          weightFn = _ => MaxBatchWeight + 5L,
        )
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "limit the batch weights correctly" in {
    val input = Iterator.continually(util.Random.nextInt()).take(100).toList
    val maxBatchWeight = 50L

    val batchesF =
      Source(input)
        .batchNWeighted(
          maxBatchWeight,
          MaxBatchCount,
          catchUpMode = BatchN.MaximizeConcurrency,
          weightFn = weightFn1,
        )
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      val batchWeights = batches.take(batches.size - MaxBatchCount).map(_.map(weightFn1).sum)
      all(batchWeights) should be <= maxBatchWeight.toLong
    }
  }

  it should "form maximally-sized batches if downstream is slower than upstream - random case" in {
    val input = Iterator.continually(util.Random.nextInt()).take(1000).toList
    val maxBatchWeight = 50L

    val batchesF =
      Source(input)
        .batchNWeighted(
          maxBatchWeight,
          MaxBatchCount,
          catchUpMode = BatchN.MaximizeConcurrency,
          weightFn = weightFn1,
        )
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      // dropping the last set of batches, because those could be not fully populated
      val batchWeights = batches.take(batches.size - MaxBatchCount).map(_.map(weightFn1).sum)
      val smallBatches = batchWeights.filter(_ < maxBatchWeight - MaxItemWeight)
      // only the last set of batches can be small
      smallBatches.size should be <= MaxBatchCount
    }
  }

  behavior of s"BatchN in batchMode ${BatchN.MaximizeBatchSize}"

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = 1 to inputSize
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .batchN(MaxBatchWeight, MaxBatchCount, catchUpMode = BatchN.MaximizeBatchSize)
        .runWith(Sink.seq[Iterable[Int]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "form maximally-sized batches if downstream is slower than upstream" in {
    val inputSize = 100
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchN(MaxBatchWeight, MaxBatchCount, catchUpMode = BatchN.MaximizeBatchSize)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize / MaxBatchWeight)(
        MaxBatchWeight
      )
    }
  }

  it should "form maximally-sized batches under downstream back-pressure" in {
    val inputSize = 25
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .batchN(MaxBatchWeight, MaxBatchCount, catchUpMode = BatchN.MaximizeBatchSize)
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs (Array.fill(inputSize / MaxBatchWeight)(
        MaxBatchWeight // fill as many full batches as possible
      ) :+ inputSize % MaxBatchWeight) // and the last batch is whatever is left-over
    }
  }
}
