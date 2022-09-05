// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.stream.Attributes.InputBuffer
import akka.stream.{Attributes, DelayOverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.daml.ledger.api.testing.utils.AkkaBeforeAndAfterAll
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

class BatchNSpec extends AsyncFlatSpec with Matchers with AkkaBeforeAndAfterAll {
  behavior of BatchN.getClass.getSimpleName

  private val MaxBatchSize = 10
  private val MaxBatchCount = 5

  it should "form batches of size 1 under no load" in {
    val inputSize = 10
    val input = 1 to inputSize
    val batchesF =
      Source(input).async
        // slow upstream
        .delay(10.millis, DelayOverflowStrategy.backpressure)
        .via(BatchN(MaxBatchSize, MaxBatchCount))
        .runWith(Sink.seq[ArrayBuffer[Int]])

    batchesF.map { batches =>
      batches.flatten should contain theSameElementsInOrderAs input
      batches.map(_.size) should contain theSameElementsAs Array.fill(inputSize)(1)
    }
  }

  it should "form maximally-sized batches under downstream back-pressure" in {
    val inputSize = 100
    val input = 1 to inputSize

    val batchesF =
      Source(input)
        .via(BatchN(MaxBatchSize, MaxBatchCount))
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
}
