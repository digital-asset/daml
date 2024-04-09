// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.crypto.SyncCryptoApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.block.BlockUpdateGenerator.EventsForSubmissionRequest
import com.digitalasset.canton.sequencing.protocol.{
  AggregationId,
  Batch,
  ClosedEnvelope,
  DeliverError,
  SequencedEvent,
  SequencedEventTrafficState,
  SubmissionRequest,
  TrafficState,
}
import com.digitalasset.canton.topology.Member

/** Describes the outcome of processing a submission request:
  * @param eventsByMember      The sequenced events for each member that is recipient of the submission request.
  * @param inFlightAggregation If [[scala.Some$]], the [[com.digitalasset.canton.sequencing.protocol.AggregationId]]
  *                            and the [[com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregationUpdate]]
  *                            of the current in-flight aggregation state.
  *                            If [[scala.None$]], then the submission request either is not aggregatable or was refused.
  * @param signingSnapshotO    The snapshot to be used for signing the envelopes,
  *                            if it should be different from the snapshot at the sequencing time.
  *                            To be removed from here once event signing code is gone from the block sequencer.
  * @param submission          The submission request that was processed.
  * @param sequencingTime      Sequencing time assigned by the block sequencer, used when passed to the database sequencer
  * @param deliverToMembers    Set of members this submissions is for. It is used as recipient members in the database
  *                            sequencer writer, because it currently doesn't do the group address resolution
  * @param receiptOrErrorO     If [[scala.Some$]], it contains either a DeliverError or deliver receipt for the sender,
  *                            for submissions with Deliver event this will be empty.
  *                             Only present for deliverReceipts and DeliverErrors
  * @param aggregatedBatchO    Since aggregate submissions collect signatures, which is now on the Block sequencer's side,
  *                            we need to pass a different batch with signatures collected (instead of using submission.batch).
  *                            Set only for Deliver events.
  * @param trafficStateO       The traffic state after processing the submission.
  */
final case class SubmissionRequestOutcome(
    eventsByMember: EventsForSubmissionRequest,
    inFlightAggregation: Option[(AggregationId, InFlightAggregationUpdate)],
    signingSnapshotO: Option[SyncCryptoApi],
    submission: SubmissionRequest,
    sequencingTime: CantonTimestamp,
    deliverToMembers: Set[Member],
    receiptOrErrorO: Option[
      SequencedEvent[ClosedEnvelope]
    ],
    aggregatedBatchO: Option[
      Batch[ClosedEnvelope]
    ], // needed to pass the aggregated signatures for aggregate submissions
    trafficStateO: Option[Map[Member, TrafficState]] =
      None, // traffic state after updating it with top-ups
) {
  def memberTrafficState(member: Member): Option[SequencedEventTrafficState] = {
    val defaultTrafficState = TrafficState(
      NonNegativeLong.zero,
      NonNegativeLong.zero,
      NonNegativeLong.zero,
      CantonTimestamp.MinValue,
    )
    trafficStateO.map(_.withDefault(_ => defaultTrafficState)(member).toSequencedEventTrafficState)
  }
}

object SubmissionRequestOutcome {
  // TODO(#18395): Find a better way to handle the null value
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  val discardSubmissionRequest: SubmissionRequestOutcome =
    SubmissionRequestOutcome(
      Map.empty,
      None,
      None,
      null,
      CantonTimestamp.MinValue,
      Set.empty,
      None,
      None,
      None,
    )

  def reject(
      submission: SubmissionRequest,
      sender: Member,
      rejection: DeliverError,
  ): SubmissionRequestOutcome =
    SubmissionRequestOutcome(
      Map(sender -> rejection),
      None,
      None,
      submission,
      rejection.timestamp,
      Set(sender),
      Some(rejection),
      None,
      None,
    )
}
