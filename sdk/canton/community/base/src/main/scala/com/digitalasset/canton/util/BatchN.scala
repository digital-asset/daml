// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
  * moderate load: short intermittent backpressure from downstream that doesn't fill up the maximum
  * batch capacity (maxBatchSize * maxBatchCount) of BatchN.
  *
  * heavy load: downstream backpressure causes the full batch capacity to fill up and BatchN to
  * exert backpressure to upstream.
  *
  * Under heavy load or when maxBatchCount == 1, CatchUpMode.MaximizeBatchSize and
  * CatchupMode.MaximizeConcurrency behave the same way, i.e. full batches are emitted.
  */
object BatchN {

  /** Determines how BatchN catches up under moderate load. */
  sealed trait CatchUpMode

  /** Causes BatchN to favor a smaller number of large batches when catching up after backpressure
    */
  case object MaximizeBatchSize extends CatchUpMode

  /** Causes BatchN to favor a higher number of small batches when catching up after backpressure */
  case object MaximizeConcurrency extends CatchUpMode

  def apply[IN](
      maxBatchSize: Int,
      maxBatchCount: Int,
      catchUpMode: CatchUpMode = MaximizeConcurrency,
  ): Flow[IN, Iterable[IN], NotUsed] = {
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
            .toVector
        }
      }
  }

  private final case class WeightedElem[T](elem: T, weight: Long)

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  private final class BatchBuilder[T] {
    private var elems: ArrayBuffer[T] = new ArrayBuffer[T]()
    private var _weight: Long = 0

    def weight: Long = _weight

    def isEmpty: Boolean = elems.isEmpty

    def add(weightedElem: WeightedElem[T]): Unit = {
      elems.addOne(weightedElem.elem)
      _weight = _weight + weightedElem.weight
    }

    def buildAndReset: Iterable[T] = {
      val result = elems
      elems = new ArrayBuffer[T]()
      _weight = 0
      result
    }
  }

  /** The weighted version of `apply`. In case of `MaximizeConcurrency` it attempts to create
    * equally weighed batches.
    *   - If an item is too heavy to fit in a batch (over maxBatchWeight) it is rather added to the
    *     next batch
    *     - Unless this is the single item in this batch
    *   - The `weightFn` is evaluated once and memoized in `WeightedElem`s
    *   - After processing all the items in the next buffer the prepared batch can be
    *     - at batch cap => it is emitted as a last batch
    *     - less than batch cap => save these items for the next iteration of `statefulMap`
    *
    * @param weightFn
    *   the function to calculate the weight of each incoming item
    * @param weightReporter
    *   provides a way to observe the actual weights of each batch. Used for metric reporting
    */
  def weighted[IN](
      maxBatchWeight: Long,
      downstreamParallelism: Int,
      catchUpMode: CatchUpMode = MaximizeConcurrency,
  )(
      weightFn: IN => Long,
      weightReporter: Long => Unit = _ => (),
  ): Flow[IN, Iterable[IN], NotUsed] = {
    // setting maxBatchCount a bit more than the downstream parallelism to ensure the parallel processing capacity is well utilised
    val maxBatchCount = downstreamParallelism + 1

    def calculateBatches(
        builder: BatchBuilder[IN],
        nextBuffer: ArrayBuffer[WeightedElem[IN]],
    ): (BatchBuilder[IN], Iterable[Iterable[IN]]) = {
      val batchWeightCap =
        catchUpMode match {
          case MaximizeBatchSize => maxBatchWeight
          case MaximizeConcurrency =>
            (builder.weight + nextBuffer.view.map(_.weight).sum) / maxBatchCount
        }
      if (batchWeightCap == 0) {
        (
          builder,
          (builder.buildAndReset ++ nextBuffer.map(_.elem)).view
            .map(Seq(_))
            .toVector,
        )
      } else {
        val acc: ArrayBuffer[Iterable[IN]] = new ArrayBuffer(initialSize = maxBatchCount)
        nextBuffer.foreach { item =>
          if (builder.weight > 0 && builder.weight + item.weight > batchWeightCap) {
            weightReporter(builder.weight)
            acc.addOne(builder.buildAndReset)
          }
          builder.add(item)
        }
        if (builder.weight < batchWeightCap)
          (builder, acc)
        else {
          weightReporter(builder.weight)
          (builder, acc.addOne(builder.buildAndReset))
        }
      }
    }

    def finish(builder: BatchBuilder[IN]) =
      if (builder.isEmpty) None
      else Some(ArrayBuffer(builder.buildAndReset))

    Flow[IN]
      .map(e => WeightedElem(e, weightFn(e)))
      .batchWeighted[ArrayBuffer[WeightedElem[IN]]](
        maxBatchWeight * maxBatchCount,
        _.weight,
        ArrayBuffer(_),
      )(_ addOne _)
      .statefulMap(() => new BatchBuilder[IN]())(
        calculateBatches,
        finish,
      )
      .mapConcat(identity)
  }

  private def newBatch[IN](maxBatchSize: Int, newElement: IN) = {
    val newBatch = ArrayBuffer.empty[IN]
    newBatch.sizeHint(maxBatchSize)
    newBatch.addOne(newElement)
    newBatch
  }

}
