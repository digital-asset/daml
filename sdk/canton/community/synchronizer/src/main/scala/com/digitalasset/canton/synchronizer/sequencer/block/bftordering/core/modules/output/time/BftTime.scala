// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.time

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.CompleteBlockData
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.DurationConverters.*

/** BFT time calculations. See the design document for more details.
  */
object BftTime {

  /** The request time granularity is the minimum time interval between two requests.
    */
  val RequestTimeGranularity: FiniteDuration = CantonTimestamp.TimeGranularity

  // The minimum block time granularity is the minimum sequencing time interval between two blocks.
  @VisibleForTesting
  private[bftordering] val MinimumBlockTimeGranularity: FiniteDuration = 1.millisecond

  /** The maximum number of requests per block. With a block time granularity of 1 millisecond, at
    * most 1000 requests could be fit into a block but we leave one slot for a topology time tick
    * which is injected after the last block if the epoch contains requests that may alter the
    * sequencing topology.
    */
  val MaxRequestsPerBlock: Int =
    Math.floor(MinimumBlockTimeGranularity / RequestTimeGranularity - 1).toInt

  /** The block BFT time determines the BFT time of the first transaction in the block (if any).
    */
  def blockBftTime(
      canonicalCommitSet: CanonicalCommitSet,
      previousBlockBftTime: CantonTimestamp,
  ): CantonTimestamp =
    // It's not worth extracting timestamps, since once the signature verification is in place, signatures will cover
    //  entire messages and thus won't be verified just based on timestamps.
    median(canonicalCommitSet.timestamps)
      .max(previousBlockBftTime.add(MinimumBlockTimeGranularity.toJava))

  private def median(canonicalTimestampSet: Seq[CantonTimestamp]): CantonTimestamp = {
    // As the timestamp set is rather small, we can go with a less performant algorithm using sorting.
    val sortedTimestamps = canonicalTimestampSet.sorted
    val (_, higher) = sortedTimestamps.splitAt(sortedTimestamps.size / 2)
    // More precisely, it could be an average of 2 middle values when the size is uneven.
    higher.headOption.getOrElse(CantonTimestamp.Epoch)
  }

  def requestBftTime(
      blockBftTime: CantonTimestamp,
      transactionIndex: Int,
  ): CantonTimestamp =
    blockBftTime + NonNegativeFiniteDuration.tryCreate(
      (RequestTimeGranularity * transactionIndex.toLong).toJava
    )

  def epochEndBftTime(
      epochLastBlockBftTime: CantonTimestamp,
      epochLastBlockData: CompleteBlockData,
  ): CantonTimestamp = {
    val epochLastBlockTxCount =
      epochLastBlockData.batches.map(_._2.requests.size).sum
    if (epochLastBlockTxCount == 0) epochLastBlockBftTime
    else
      requestBftTime(
        epochLastBlockBftTime,
        transactionIndex = epochLastBlockTxCount - 1,
      )
  }
}
