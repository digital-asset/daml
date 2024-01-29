// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block.data

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.{BlockUpdates, UninitializedBlockHeight}
import com.digitalasset.canton.domain.sequencing.integrations.state.EphemeralState
import com.digitalasset.canton.domain.sequencing.sequencer.traffic.MemberTrafficSnapshot
import com.digitalasset.canton.domain.sequencing.sequencer.{
  SequencerInitialState,
  SequencerSnapshot,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

/** Persisted information about a block as a whole once it has been fully processed.
  *
  * @param height The height of the block
  * @param lastTs The latest timestamp used by an event or member registration in blocks up to `height`
  * @param latestTopologyClientTimestamp
  *               The sequencing timestamp of an event addressed to the sequencer's topology client such that
  *               there is no topology update (by sequencing time)
  *               between this timestamp (exclusive) and the last event in the block with height `height`.
  *               Must not be after `lastTs`.
  *
  *               [[scala.None$]] if no such timestamp is known.
  *               In that case, it is not guaranteed that the correct topology state will be used for validating the events in the block.
  *
  *               External sequencer's topology clients typically listen to events addressed to the domain manager.
  */
final case class BlockInfo(
    height: Long,
    lastTs: CantonTimestamp,
    latestTopologyClientTimestamp: Option[CantonTimestamp],
) {
  require(
    latestTopologyClientTimestamp.forall(lastTs >= _),
    s"The latest topology client timestamp $latestTopologyClientTimestamp must not be after the last known event at ${lastTs}",
  )
}

object BlockInfo {
  val initial: BlockInfo =
    BlockInfo(UninitializedBlockHeight, lastTs = CantonTimestamp.Epoch, None)

  implicit val getResultBlockInfo: GetResult[BlockInfo] = GetResult { r =>
    val height = r.<<[Long]
    val lastTs = r.<<[CantonTimestamp]
    val latestTopologyClientTs = r.<<[Option[CantonTimestamp]]
    BlockInfo(height, lastTs, latestTopologyClientTs)
  }
}

/** Our typical sequencer state with an associated block height.
  *
  * @param latestBlock Information about the latest block
  */
final case class BlockEphemeralState(
    latestBlock: BlockInfo,
    state: EphemeralState,
) extends HasLoggerName {
  def toSequencerSnapshot(
      protocolVersion: ProtocolVersion
  ): SequencerSnapshot =
    SequencerSnapshot(
      latestBlock.lastTs,
      state.heads,
      state.status.toSequencerPruningStatus(latestBlock.lastTs),
      state.inFlightAggregations,
      Some(
        SequencerSnapshot.ImplementationSpecificInfo(
          "BLOCK",
          ByteString.copyFrom(scala.math.BigInt(latestBlock.height).toByteArray),
        )
      ),
      protocolVersion,
      trafficState = state.trafficState.map { case (member, state) =>
        member -> MemberTrafficSnapshot(
          member = member,
          state = state,
        )
      },
    )

  /** Checks that the class invariant holds:
    * - Expired in-flight aggregations have been evicted
    * - In-flight aggregations satisfy their invariant
    *
    * @throws java.lang.IllegalStateException if the invariant check fails
    */
  def checkInvariant()(implicit loggingContext: NamedLoggingContext): Unit = {
    // All expired in-flight aggregations have been evicted
    val lastTs = latestBlock.lastTs
    val expired = state.inFlightAggregations.collect {
      case (aggregationId, inFlightAggregation) if inFlightAggregation.expired(lastTs) =>
        aggregationId
    }
    ErrorUtil.requireState(
      expired.isEmpty,
      s"Expired in-flight aggregations have not been evicted by ${lastTs}: ${expired.toSeq}",
    )

    // All in-flight aggregations satisfy their invariant
    state.inFlightAggregations.values.foreach(_.checkInvariant())
  }
}

object BlockEphemeralState {
  val empty: BlockEphemeralState = BlockEphemeralState(BlockInfo.initial, EphemeralState.empty)

  def fromSequencerInitialState(
      initialState: SequencerInitialState
  ): BlockEphemeralState = {
    val initialHeight =
      initialState.snapshot.additional
        .map { additional => BigInt(additional.info.toByteArray).toLong }
        .getOrElse(UninitializedBlockHeight)

    val block = BlockInfo(
      initialHeight,
      initialState.snapshot.lastTs,
      initialState.latestTopologyClientTimestamp,
    )
    BlockEphemeralState(
      block,
      EphemeralState(
        initialState.snapshot.heads,
        inFlightAggregations = initialState.snapshot.inFlightAggregations,
        initialState.snapshot.status.toInternal,
        trafficState = initialState.snapshot.trafficSnapshots.view.mapValues(_.state).toMap,
      ),
    )
  }
}

/** Helper case class generated by the BlockUpdateGenerator after being given a block by a blockchain-based sequencer
  * integration.
  *
  * @param height          height of the block used for updating the ephemeral state
  * @param updateGenerator closure of the block updates generated by processing a given block, for a certain ephemeral state
  */
final case class BlockUpdateClosureWithHeight(
    height: Long,
    updateGenerator: BlockEphemeralState => FutureUnlessShutdown[BlockUpdates],
    blockTraceContext: TraceContext,
)
