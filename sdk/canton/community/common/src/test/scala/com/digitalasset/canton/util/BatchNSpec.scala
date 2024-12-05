// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.daml.ledger.api.testing.utils.PekkoBeforeAndAfterAll
import org.apache.pekko.stream.Attributes.InputBuffer
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.stream.{Attributes, DelayOverflowStrategy}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration.*

class BatchNSpec extends AsyncFlatSpec with Matchers with PekkoBeforeAndAfterAll {

  private val MaxBatchSize = 10
  private val MaxBatchCount = 5

  behavior of s"BatchN in batchMode ${BatchN.MaximizeConcurrency}"

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = 1 to inputSize
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .via(BatchN(MaxBatchSize, MaxBatchCount, catchUpMode = BatchN.MaximizeConcurrency))
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
        .via(BatchN(MaxBatchSize, MaxBatchCount, catchUpMode = BatchN.MaximizeConcurrency))
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize / MaxBatchSize)(
        MaxBatchSize
      )
    }
  }

  it should "form even-sized batches under downstream back-pressure" in {
    val inputSize = 15
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .via(BatchN(MaxBatchSize, MaxBatchCount, catchUpMode = BatchN.MaximizeConcurrency))
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

  behavior of s"BatchN in batchMode ${BatchN.MaximizeBatchSize}"

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = 1 to inputSize
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .via(BatchN(MaxBatchSize, MaxBatchCount, catchUpMode = BatchN.MaximizeBatchSize))
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
        .via(BatchN(MaxBatchSize, MaxBatchCount, catchUpMode = BatchN.MaximizeBatchSize))
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize / MaxBatchSize)(
        MaxBatchSize
      )
    }
  }

  it should "form maximally-sized batches under downstream back-pressure" in {
    val inputSize = 25
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .via(BatchN(MaxBatchSize, MaxBatchCount, catchUpMode = BatchN.MaximizeBatchSize))
        // slow downstream
        .initialDelay(10.millis)
        .async
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .addAttributes(Attributes(InputBuffer(1, 1)))
        .runWith(Sink.seq)

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs (Array.fill(inputSize / MaxBatchSize)(
        MaxBatchSize // fill as many full batches as possible
      ) :+ inputSize % MaxBatchSize) // and the last batch is whatever is left-over
    }
  }
}
