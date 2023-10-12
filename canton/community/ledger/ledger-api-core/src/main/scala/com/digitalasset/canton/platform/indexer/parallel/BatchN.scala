// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.indexer.parallel

import akka.NotUsed
import akka.stream.scaladsl.Flow

import scala.collection.mutable.ArrayBuffer

/** Forms dynamically-sized batches based on downstream backpressure.
  *   - Under light load, this flow emits batches of size 1.
  *   - Under moderate load, this flow emits batches of even sizes.
  *   - Under heavy load (dowstream saturated), this flow emits batches of `maxBatchSize`.
  */
object BatchN {
  def apply[IN](
      maxBatchSize: Int,
      maxBatchCount: Int,
  ): Flow[IN, ArrayBuffer[IN], NotUsed] = {
    val totalBatchSize = maxBatchSize * maxBatchCount
    Flow[IN]
      .batch[ArrayBuffer[IN]](
        totalBatchSize.toLong,
        newBatch(totalBatchSize, _),
      )(_ addOne _)
      .mapConcat { totalBatch =>
        val totalSize = totalBatch.size
        val batchSize = totalSize / maxBatchCount
        if (batchSize == 0) {
          totalBatch.view
            .map(newBatch(1, _))
            .toVector
        } else {
          totalBatch.sliding(batchSize, batchSize).toVector
        }
      }
  }

  private def newBatch[IN](maxBatchSize: Int, newElement: IN) = {
    val newBatch = ArrayBuffer.empty[IN]
    newBatch.sizeHint(maxBatchSize)
    newBatch.addOne(newElement)
    newBatch
  }
}
