// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.Member

/** Describes the outcome of processing a submission request:
  * @param eventsByMember      The sequenced events for each member that is recipient of the submission request.
  * @param inFlightAggregation If [[scala.Some$]], the [[com.digitalasset.canton.sequencing.protocol.AggregationId]]
  *                            and the [[com.digitalasset.canton.domain.sequencing.sequencer.InFlightAggregationUpdate]]
  *                            of the current in-flight aggregation state.
  *                            If [[scala.None$]], then the submission request either is not aggregatable or was refused.
  * @param outcome             The outcome of the submission request for the interface with Database sequencer.
  */
final case class SubmissionRequestOutcome(
    eventsByMember: Map[Member, SequencedEvent[ClosedEnvelope]],
    inFlightAggregation: Option[(AggregationId, InFlightAggregationUpdate)],
    outcome: SubmissionOutcome,
)

object SubmissionRequestOutcome {
  val discardSubmissionRequest: SubmissionRequestOutcome =
    SubmissionRequestOutcome(
      Map.empty,
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
      outcome = SubmissionOutcome.Reject(
        submission,
        rejection.timestamp,
        rejection.reason,
      ),
    )
}
