// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.collection.mutable.ArrayBuffer

/** Forms dynamically-sized batches based on downstream backpressure.
  *   - Under light load, this flow emits batches of size 1.
  *   - Under heavy load, this flow emits batches of `maxBatchSize`.
  */
object BatchN {
  def apply[IN](
      maxBatchSize: Int,
      maxBatchCount: Int,
  ): Flow[IN, ArrayBuffer[IN], NotUsed] =
    Flow[IN]
      .batch[Vector[ArrayBuffer[IN]]](
        (maxBatchSize * maxBatchCount).toLong,
        in => Vector(newBatch(maxBatchSize, in)),
      ) { case (batches, in) =>
        val lastBatch = batches.last
        if (lastBatch.size < maxBatchSize) {
          lastBatch.addOne(in)
          batches
        } else
          batches :+ newBatch(maxBatchSize, in)
      }
      .mapConcat(identity)

  private def newBatch[IN](maxBatchSize: Int, newElement: IN) = {
    val newBatch = ArrayBuffer.empty[IN]
    newBatch.sizeHint(maxBatchSize)
    newBatch.addOne(newElement)
    newBatch
  }
}
