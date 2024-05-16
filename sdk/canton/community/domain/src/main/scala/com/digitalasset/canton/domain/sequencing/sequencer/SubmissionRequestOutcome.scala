// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.crypto.SyncCryptoApi
import com.digitalasset.canton.sequencing.protocol.{
  AggregationId,
  ClosedEnvelope,
  DeliverError,
  SequencedEvent,
  SubmissionRequest,
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
  * @param outcome             The outcome of the submission request for the interface with Database sequencer.
  */
final case class SubmissionRequestOutcome(
    eventsByMember: Map[Member, SequencedEvent[ClosedEnvelope]],
    inFlightAggregation: Option[(AggregationId, InFlightAggregationUpdate)],
    signingSnapshotO: Option[SyncCryptoApi],
    outcome: SubmissionOutcome,
)

object SubmissionRequestOutcome {
  val discardSubmissionRequest: SubmissionRequestOutcome =
    SubmissionRequestOutcome(
      Map.empty,
      None,
      None,
      outcome = SubmissionOutcome.Discard,
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
      outcome = SubmissionOutcome.Reject(
        submission,
        rejection.timestamp,
        rejection.reason,
      ),
    )
}
