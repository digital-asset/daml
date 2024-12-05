// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow

import scala.collection.mutable.ArrayBuffer

/** Forms dynamically-sized batches based on downstream backpressure.
  *   - Under light load, this flow emits batches of size 1.
  *   - Under moderate load, this flow emits batches according to the batch mode:
  *     - MaximizeConcurrency: emits batches of even sizes
  *     - MaximizeBatchSize: emits fewer but full batches
  *   - Under heavy load (dowstream saturated), this flow emits batches of `maxBatchSize`.
  *
  *   moderate load: short intermittent backpressure from downstream that doesn't fill up the
  *   maximum batch capacity (maxBatchSize * maxBatchCount) of BatchN.
  *
  *   heavy load: downstream backpressure causes the full batch capacity to fill up and BatchN to
  *   exert backpressure to upstream.
  *
  *   Under heavy load or when maxBatchCount == 1, CatchUpMode.MaximizeBatchSize and
  *   CatchupMode.MaximizeConcurrency behave the same way, i.e. full batches are emitted.
  */
object BatchN {

  /** Determines how BatchN catches up under moderate load. */
  sealed trait CatchUpMode

  /** Causes BatchN to favor a smaller number of large batches when catching up after backpressure */
  case object MaximizeBatchSize extends CatchUpMode

  /** Causes BatchN to favor a higher number of small batches when catching up after backpressure */
  case object MaximizeConcurrency extends CatchUpMode

  def apply[IN](
      maxBatchSize: Int,
      maxBatchCount: Int,
      catchUpMode: CatchUpMode = MaximizeConcurrency,
  ): Flow[IN, collection.immutable.Iterable[IN], NotUsed] = {
    val totalBatchSize = maxBatchSize * maxBatchCount
    Flow[IN]
      .batch[ArrayBuffer[IN]](
        totalBatchSize.toLong,
        newBatch(totalBatchSize, _),
      )(_ addOne _)
      .mapConcat { totalBatch =>
        val totalSize = totalBatch.size
        val batchSize = catchUpMode match {
          case MaximizeBatchSize => maxBatchSize
          case MaximizeConcurrency => totalSize / maxBatchCount
        }
        if (batchSize == 0) {
          totalBatch.view
            .map(Seq(_))
            .toVector
        } else {
          totalBatch
            .grouped(batchSize)
            .map(new IterableToIterable(_))
            .toVector
        }
      }
  }

  private def newBatch[IN](maxBatchSize: Int, newElement: IN) = {
    val newBatch = ArrayBuffer.empty[IN]
    newBatch.sizeHint(maxBatchSize)
    newBatch.addOne(newElement)
    newBatch
  }

  // WARNING! DO NOT USE THIS WRAPPER OUTSIDE OF THIS CONTEXT!
  // this wrapping is only safe because it is used in this context where it is an invariant of the algorithm
  // that the ArrayBuffer is not changing after the result left the BatchN stage.
  // Please note as soon as this immutable.Iterable is used in any further collection processing, where new result
  // collection needs to be built, this will fall back to IterableFactory.Delegate[Iterable](List).
  // Please note this cannot be a value class, since some ancestors of immutable.Iterable is Any.
  private class IterableToIterable[X](val iterable: Iterable[X])
      extends collection.immutable.Iterable[X] {
    override def iterator: Iterator[X] = iterable.iterator
  }
}
