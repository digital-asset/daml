// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.{Batch, ClosedEnvelope, SubmissionRequest}
import com.digitalasset.canton.sequencing.traffic.TrafficReceipt
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.google.rpc.status.Status

sealed trait SubmissionOutcome
sealed trait DeliverableSubmissionOutcome extends SubmissionOutcome {
  def submission: SubmissionRequest

  def sequencingTime: CantonTimestamp

  def deliverToMembers: Set[Member]

  def submissionTraceContext: TraceContext

  def trafficReceiptO: Option[TrafficReceipt]

  def updateTrafficReceipt(trafficReceiptO: Option[TrafficReceipt]): DeliverableSubmissionOutcome
}

object SubmissionOutcome {

  /** The submission was successfully sequenced and should be delivered to the specified members.
    * @param submission        The original submission request.
    * @param sequencingTime    The time at which the submission was sequenced.
    * @param deliverToMembers  The members to which the submission should be delivered,
    *                          only needed before group addressing in DBS is finished.
    * @param batch             The batch of envelopes to be delivered (may include aggregated signatures,
    *                          prefer this to submissionRequest.batch).
    */
  final case class Deliver(
      override val submission: SubmissionRequest,
      override val sequencingTime: CantonTimestamp,
      override val deliverToMembers: Set[Member],
      batch: Batch[ClosedEnvelope],
      override val submissionTraceContext: TraceContext,
      override val trafficReceiptO: Option[TrafficReceipt],
  ) extends DeliverableSubmissionOutcome {
    override def updateTrafficReceipt(
        trafficReceiptO: Option[TrafficReceipt]
    ): DeliverableSubmissionOutcome = copy(trafficReceiptO = trafficReceiptO)
  }

  /** Receipt, is sent to the sender of an aggregate submission still awaiting more votes.
    * No messages are sent to the recipients.
    * @param submission                 The original submission request.
    * @param sequencingTime             The time at which the submission was sequenced.
    * @param submissionTraceContext     The trace context of the submission.
    * @param trafficReceiptO            The traffic receipt, `None` if traffic control is disabled or
    *                                     if the sender is not rate limited (e.g. Sequencer).
    */
  final case class DeliverReceipt(
      override val submission: SubmissionRequest,
      override val sequencingTime: CantonTimestamp,
      override val submissionTraceContext: TraceContext,
      override val trafficReceiptO: Option[TrafficReceipt],
  ) extends DeliverableSubmissionOutcome {
    override def deliverToMembers: Set[Member] = Set(submission.sender)

    override def updateTrafficReceipt(
        trafficReceiptO: Option[TrafficReceipt]
    ): DeliverableSubmissionOutcome = copy(trafficReceiptO = trafficReceiptO)
  }

  /** The submission was fully discarded, no error is delivered to sender, no messages are sent to the members.
    */
  case object Discard extends SubmissionOutcome

  /** The submission was rejected and an error should be delivered to the sender.
    *
    * @param submission               The original submission request.
    * @param sequencingTime           The time at which the submission was sequenced.
    * @param error                    The error status to be delivered to the sender.
    * @param submissionTraceContext   The trace context of the submission.
    * @param trafficReceiptO          The traffic receipt, `None` if traffic control is disabled or
    *                                   if the sender is not rate limited (e.g. Sequencer).
    */
  final case class Reject(
      override val submission: SubmissionRequest,
      override val sequencingTime: CantonTimestamp,
      error: Status,
      override val submissionTraceContext: TraceContext,
      override val trafficReceiptO: Option[TrafficReceipt],
  ) extends DeliverableSubmissionOutcome {
    override def deliverToMembers: Set[Member] = Set(submission.sender)

    override def updateTrafficReceipt(
        trafficReceiptO: Option[TrafficReceipt]
    ): DeliverableSubmissionOutcome = copy(trafficReceiptO = trafficReceiptO)
  }

}
