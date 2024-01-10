// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.daml.error.*
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.error.{BaseCantonError, TransactionError, TransactionErrorImpl}
import com.digitalasset.canton.topology.Member
import com.google.rpc.status.Status

import java.time.Instant
import scala.collection.immutable.Seq

sealed trait SequencerDeliverError extends TransactionError

sealed abstract class SequencerDeliverErrorCode(id: String, category: ErrorCategory)(implicit
    parent: ErrorClass
) extends ErrorCode(id, category) {
  require(category.grpcCode.isDefined, "gPRC code is required for the correct matching in unapply")

  def apply(message: String): SequencerDeliverError = {
    new TransactionErrorImpl(
      cause = message,
      definiteAnswer = true,
    ) with SequencerDeliverError
  }

  /** Match the GRPC status on the ErrorCode and return the message string on success
    */
  def unapply(rpcStatus: Status): Option[String] =
    BaseCantonError.extractStatusErrorCodeMessage(this, rpcStatus)
}

@Explanation("""Delivery errors wrapped into sequenced events""")
object SequencerErrors extends SequencerErrorGroup {
  @Explanation("""This error occurs when the sequencer cannot parse the submission request.""")
  @Resolution(
    """This usually indicates a misconfiguration of the system components or an application bug and requires operator intervention."""
  )
  object SubmissionRequestMalformed
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_SUBMISSION_REQUEST_MALFORMED",
        ErrorCategory.InvalidIndependentOfSystemState,
      )

  @Explanation(
    """This error occurs when the sequencer cannot accept submission request due to the current state of the system."""
  )
  @Resolution(
    """This usually indicates a misconfiguration of the system components or an application bug and requires operator intervention. Please refer to a specific error message to understand the exact cause."""
  )
  object SubmissionRequestRefused
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_SUBMISSION_REQUEST_REFUSED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """Timestamp of the signing key on the submission request is earlier than allowed by the dynamic domain parameters."""
  )
  @Resolution(
    """This indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  object SigningTimestampTooEarly
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_SIGNING_TIMESTAMP_TOO_EARLY",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(
        signingTimestamp: CantonTimestamp,
        sequencingTimestamp: CantonTimestamp,
    ): SequencerDeliverError =
      // We can't easily compute a valid signing timestamp because we'd have to scan through
      // the domain parameter updates to compute a bound, as the signing tolerance is taken
      // from the domain parameters valid at the signing timestamp, not the sequencing timestamp.
      apply(
        s"Signing timestamp $signingTimestamp is too early for sequencing time $sequencingTimestamp."
      )
  }

  @Explanation(
    """Timestamp of the signing key on the submission request is later than the sequencing time."""
  )
  @Resolution(
    """This indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  object SigningTimestampAfterSequencingTimestamp
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_SIGNING_TIMESTAMP_AFTER_SEQUENCING_TIMESTAMP",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(
        signingTimestamp: CantonTimestamp,
        sequencingTimestamp: CantonTimestamp,
    ): SequencerDeliverError =
      apply(
        s"Invalid signing timestamp $signingTimestamp. The signing timestamp must be before or at $sequencingTimestamp."
      )
  }

  @Explanation(
    """Timestamp of the signing key is missing on the submission request."""
  )
  @Resolution(
    """This indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  object SigningTimestampMissing
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_SIGNING_TIMESTAMP_MISSING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """Maximum sequencing time on the submission request is exceeding the maximum allowed interval into the future. Could be result of a concurrent dynamic domain parameter change for sequencerAggregateSubmissionTimeout."""
  )
  @Resolution(
    """In case there was a recent concurrent dynamic domain parameter change, simply retry the submission. Otherwise this error code indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  object MaxSequencingTimeTooFar
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_MAX_SEQUENCING_TIME_TOO_FAR",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(
        messageId: MessageId,
        maxSequencingTime: CantonTimestamp,
        maxSequencingTimeUpperBound: Instant,
    ): SequencerDeliverError =
      apply(
        s"Max sequencing time $maxSequencingTime for submission with id $messageId is too far in the future, currently bounded at $maxSequencingTimeUpperBound"
      )
  }

  @Explanation(
    """This error happens when a submission request specifies nodes that are not known to the sequencer."""
  )
  @Resolution(
    """This indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  object UnknownRecipients
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_UNKNOWN_RECIPIENTS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(unknownRecipients: Seq[Member]): SequencerDeliverError = {
      apply(s"Unknown recipients: ${unknownRecipients.toList.take(1000).mkString(", ")}")
    }
  }

  @Explanation(
    """This error occurs when the sequencer has already sent out the aggregate submission for the request."""
  )
  @Resolution(
    """This is expected to happen during operation of a system with aggregate submissions enabled. No action required."""
  )
  object AggregateSubmissionAlreadySent
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_AGGREGATE_SUBMISSION_ALREADY_SENT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """This error occurs when the sequencer already received the same submission request from the same sender."""
  )
  @Resolution(
    """This error indicates that an aggregate submission has already been accepted by the sequencer and for some reason there is a repeated submission. This is likely caused by retrying a submission. This can usually be ignored."""
  )
  object AggregateSubmissionStuffing
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_AGGREGATE_SUBMISSION_STUFFING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """Sequencer has refused a submission request due to insufficient credits in the sender's traffic balance."""
  )
  @Resolution(
    """Acquire more traffic credits with the system by topping up traffic credits for the sender."""
  )
  object TrafficCredit
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_NOT_ENOUGH_TRAFFIC_CREDIT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """An onboarded sequencer has put a tombstone in place of an event with a timestamp older than the sequencer signing key."""
  )
  @Resolution(
    """Clients should connect to another sequencer with older event history to consume the tombstoned events
      |before reconnecting to the recently onboarded sequencer."""
  )
  object PersistTombstone
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_TOMBSTONE_PERSISTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(ts: CantonTimestamp, sc: SequencerCounter): SequencerDeliverError =
      apply(s"Sequencer signing key not available at ${ts} and ${sc}")
  }
}
