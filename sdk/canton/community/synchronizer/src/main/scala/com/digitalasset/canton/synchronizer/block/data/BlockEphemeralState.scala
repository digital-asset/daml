// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.data

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.synchronizer.block.UninitializedBlockHeight
import com.digitalasset.canton.synchronizer.sequencing.sequencer.{
  InFlightAggregations,
  SequencerInitialState,
}
import com.digitalasset.canton.util.ErrorUtil
import slick.jdbc.GetResult

/** Persisted information about a block as a whole once it has been fully processed.
  *
  * @param height The height of the block
  * @param lastTs The latest timestamp used by an event or member registration in blocks up to `height`
  * @param latestSequencerEventTimestamp
  *               The sequencing timestamp of an event addressed to the sequencer such that
  *               there is no event addressed to the sequencer (by sequencing time)
  *               between this timestamp (exclusive) and the last event in the block with height `height`.
  *               Must not be after `lastTs`.
  *
  *               [[scala.None$]] if no such timestamp is known.
  *               In that case, it is not guaranteed that the correct topology and traffic states will be used for validating the events in the block.
  */
final case class BlockInfo(
    height: Long,
    lastTs: CantonTimestamp,
    latestSequencerEventTimestamp: Option[CantonTimestamp],
) {
  require(
    latestSequencerEventTimestamp.forall(lastTs >= _),
    s"The latest sequencer event timestamp $latestSequencerEventTimestamp must not be after the last known event at $lastTs",
  )
}

object BlockInfo {
  val initial: BlockInfo =
    BlockInfo(UninitializedBlockHeight, lastTs = CantonTimestamp.Epoch, None)

  implicit val getResultBlockInfo: GetResult[BlockInfo] = GetResult { r =>
    val height = r.<<[Long]
    val lastTs = r.<<[CantonTimestamp]
    val latestSequencerEventTs = r.<<[Option[CantonTimestamp]]
    BlockInfo(height, lastTs, latestSequencerEventTs)
  }

  def fromSequencerInitialState(initial: SequencerInitialState): BlockInfo = BlockInfo(
    initial.snapshot.latestBlockHeight,
    initial.snapshot.lastTs,
    initial.latestSequencerEventTimestamp,
  )
}

/** Our typical sequencer state with an associated block height.
  *
  * @param latestBlock Information about the latest block
  */
final case class BlockEphemeralState(
    latestBlock: BlockInfo,
    inFlightAggregations: InFlightAggregations,
) extends HasLoggerName {

  /** Checks that the class invariant holds:
    * - Expired in-flight aggregations have been evicted
    * - In-flight aggregations satisfy their invariant
    *
    * @throws java.lang.IllegalStateException if the invariant check fails
    */
  def checkInvariant()(implicit loggingContext: NamedLoggingContext): Unit = {
    // All expired in-flight aggregations have been evicted
    val lastTs = latestBlock.lastTs
    val expired = inFlightAggregations.collect {
      case (aggregationId, inFlightAggregation) if inFlightAggregation.expired(lastTs) =>
        aggregationId
    }
    ErrorUtil.requireState(
      expired.isEmpty,
      s"Expired in-flight aggregations have not been evicted by $lastTs: ${expired.toSeq}",
    )

    // All in-flight aggregations satisfy their invariant
    inFlightAggregations.values.foreach(_.checkInvariant())
  }
}

object BlockEphemeralState {
  val empty: BlockEphemeralState =
    BlockEphemeralState(BlockInfo.initial, Map.empty)

  def fromSequencerInitialState(
      initialState: SequencerInitialState
  ): BlockEphemeralState = {
    val block = BlockInfo(
      initialState.snapshot.latestBlockHeight,
      initialState.snapshot.lastTs,
      initialState.latestSequencerEventTimestamp,
    )
    BlockEphemeralState(
      block,
      initialState.snapshot.inFlightAggregations,
    )
  }
}
