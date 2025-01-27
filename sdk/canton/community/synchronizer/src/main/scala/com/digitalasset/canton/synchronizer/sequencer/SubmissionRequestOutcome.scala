// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.synchronizer.sequencer.SubmissionOutcome.Discard
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext

/** Describes the outcome of processing a submission request:
  *
  * @param eventsByMember      The sequenced events for each member that is recipient of the submission request.
  * @param inFlightAggregation If [[scala.Some$]], the [[com.digitalasset.canton.sequencing.protocol.AggregationId]]
  *                            and the [[InFlightAggregationUpdate]]
  *                            of the current in-flight aggregation state.
  *                            If [[scala.None$]], then the submission request either is not aggregatable or was refused.
  * @param outcome             The outcome of the submission request for the interface with Database sequencer.
  */
final case class SubmissionRequestOutcome(
    eventsByMember: Map[Member, SequencedEvent[ClosedEnvelope]],
    inFlightAggregation: Option[(AggregationId, InFlightAggregationUpdate)],
    outcome: SubmissionOutcome,
) {
  def updateTrafficReceipt(
      sender: Member,
      trafficReceipt: Option[TrafficReceipt],
  ): SubmissionRequestOutcome = {
    // Find the event addressed to the sender in the map, that's the receipt
    val receipt = eventsByMember.get(sender)
    // Update it with the traffic consumed
    val updated: Option[SequencedEvent[ClosedEnvelope]] = receipt.map {
      case deliverError: DeliverError => deliverError.updateTrafficReceipt(trafficReceipt)
      case deliver: Deliver[ClosedEnvelope] =>
        deliver.updateTrafficReceipt(trafficReceipt = trafficReceipt)
    }
    // Put it back to the map
    val updatedMap = updated
      .map(sender -> _)
      .map(updatedReceipt => eventsByMember + updatedReceipt)
      .getOrElse(eventsByMember)

    val outcomeWithTrafficReceipt = outcome match {
      case deliverableOutcome: DeliverableSubmissionOutcome =>
        deliverableOutcome.updateTrafficReceipt(trafficReceipt)
      case Discard => outcome
    }

    this.copy(eventsByMember = updatedMap, outcome = outcomeWithTrafficReceipt)
  }
}

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
      submissionTraceContext: TraceContext,
  ): SubmissionRequestOutcome =
    SubmissionRequestOutcome(
      Map(sender -> rejection),
      None,
      outcome = SubmissionOutcome.Reject(
        submission,
        rejection.timestamp,
        rejection.reason,
        submissionTraceContext,
        trafficReceiptO = None, // traffic receipt is updated at the end of processing
      ),
    )
}
