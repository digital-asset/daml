// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block.update

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.BaseAlarm
import com.digitalasset.canton.synchronizer.block.data.BlockInfo
import com.digitalasset.canton.synchronizer.sequencing.sequencer.{
  InFlightAggregationUpdates,
  InFlightAggregations,
  SubmissionRequestOutcome,
}
import com.digitalasset.canton.topology.Member

/** Summarizes the updates that are to be persisted and signalled individually */
sealed trait BlockUpdate extends Product with Serializable

/** Denotes an update that is generated from a block that went through ordering */
sealed trait OrderedBlockUpdate extends BlockUpdate

/** Signals that all updates in a block have been delivered as chunks.
  * The [[com.digitalasset.canton.synchronizer.block.data.BlockInfo]] must be consistent with
  * the updates in all earlier [[ChunkUpdate]]s. In particular:
  * - [[com.digitalasset.canton.synchronizer.block.data.BlockInfo.lastTs]] must be at least the
  *   one from the last chunk or previous block
  * - [[com.digitalasset.canton.synchronizer.block.data.BlockInfo.latestSequencerEventTimestamp]]
  *   must be at least the one from the last chunk or previous block.
  * - [[com.digitalasset.canton.synchronizer.block.data.BlockInfo.height]] must be exactly one higher
  *   than the previous block
  * The consistency conditions are checked in [[com.digitalasset.canton.synchronizer.block.BlockSequencerStateManager]]'s `handleComplete`.
  */
final case class CompleteBlockUpdate(block: BlockInfo) extends OrderedBlockUpdate

/** Changes from processing a consecutive part of updates within a block from the blockchain.
  * We expect all values to be consistent with one another:
  *  - new members must exist in the registered members
  *  - the provided timestamps must be at or after the latest sequencer timestamp of the previous chunk or block
  *  - members receiving events must be registered
  *  - timestamps of events must not after the latest sequencer timestamp of the previous chunk or block
  *  - counter values for each member should be continuous
  *
  * @param acknowledgements The highest valid acknowledged timestamp for each member in the block.
  * @param invalidAcknowledgements All invalid acknowledgement timestamps in the block for each member.
  * @param inFlightAggregationUpdates The updates to the in-flight aggregation states.
  *                             Includes the clean-up of expired aggregations.
  * @param lastSequencerEventTimestamp The highest timestamp of an event in `events` addressed to the sequencer, if any.
  * @param inFlightAggregations Updated inFlightAggregations to be used for processing subsequent chunks.
  * @param submissionsOutcomes  A list of internal block sequencer states after processing submissions for the chunk.
  *                             This is used by the unified sequencer to generate and write events in the database sequencer.
  */
final case class ChunkUpdate(
    acknowledgements: Map[Member, CantonTimestamp] = Map.empty,
    invalidAcknowledgements: Seq[(Member, CantonTimestamp, BaseAlarm)] = Seq.empty,
    inFlightAggregationUpdates: InFlightAggregationUpdates = Map.empty,
    lastSequencerEventTimestamp: Option[CantonTimestamp],
    inFlightAggregations: InFlightAggregations,
    submissionsOutcomes: Seq[SubmissionRequestOutcome] = Seq.empty,
) extends OrderedBlockUpdate
