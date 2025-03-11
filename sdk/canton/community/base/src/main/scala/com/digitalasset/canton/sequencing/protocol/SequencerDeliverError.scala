// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.base.error.{ErrorCategory, ErrorClass, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.error.{
  Alarm,
  AlarmErrorCode,
  CantonBaseError,
  TransactionError,
  TransactionErrorImpl,
}
import com.digitalasset.canton.networking.grpc.GrpcError
import com.digitalasset.canton.topology.Member
import com.google.rpc.status.Status

import java.time.Instant

sealed trait SequencerDeliverError extends TransactionError

sealed abstract class SequencerDeliverErrorCode(id: String, category: ErrorCategory)(implicit
    parent: ErrorClass
) extends ErrorCode(id, category) {
  require(category.grpcCode.isDefined, "gPRC code is required for the correct matching in unapply")

  def apply(message: String): SequencerDeliverError =
    new TransactionErrorImpl(
      cause = message,
      definiteAnswer = true,
    ) with SequencerDeliverError {
      override def toString: String = s"SequencerDeliverError(code = $id, message = $message)"
    }

  /** Match the GRPC status on the ErrorCode and return the message string on success
    */
  def unapply(rpcStatus: Status): Option[String] =
    CantonBaseError.extractStatusErrorCodeMessage(this, rpcStatus)

  def unapply(grpcError: GrpcError): Option[String] =
    grpcError.decodedCantonError.flatMap(status =>
      Option.when(status.code.id == id)(grpcError.status.getDescription)
    )
}

@Explanation("""Delivery errors wrapped into sequenced events""")
object SequencerErrors extends SequencerErrorGroup {
  @Explanation("""
      |This error occurs when the sequencer receives an invalid submission request, e.g. it has an
      |aggregation rule with an unreachable threshold.
      |Malformed requests will not emit any deliver event.
      """)
  @Resolution("""
      |Check if the sender is running an attack.
      |If you can rule out an attack, please reach out to Canton support.
      """)
  case object SubmissionRequestMalformed
      extends AlarmErrorCode(id = "SEQUENCER_SUBMISSION_REQUEST_MALFORMED", redactDetails = false) {
    final case class Error(
        sender: String,
        messageId: String,
        error: String,
    ) extends Alarm({
          s"Send request [$messageId] from sender [$sender] is malformed. Discarding request. $error"
        })
        with SequencerDeliverError

    object Error {
      def apply(submissionRequest: SubmissionRequest, error: String): Error =
        Error(submissionRequest.sender.toProtoPrimitive, submissionRequest.messageId.unwrap, error)
    }
  }

  @Explanation(
    """This error occurs when the sequencer cannot accept submission request due to the current state of the system."""
  )
  @Resolution(
    """This usually indicates a misconfiguration of the system components or an application bug and requires operator intervention. Please refer to a specific error message to understand the exact cause."""
  )
  case object SubmissionRequestRefused
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_SUBMISSION_REQUEST_REFUSED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """Topology timestamp on the submission request is earlier than allowed by the dynamic synchronizer parameters."""
  )
  @Resolution(
    """This indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  case object TopologyTimestampTooEarly
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_TOPOLOGY_TIMESTAMP_TOO_EARLY",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(
        topologyTimestamp: CantonTimestamp,
        sequencingTimestamp: CantonTimestamp,
    ): SequencerDeliverError =
      // We can't easily compute a valid signing timestamp because we'd have to scan through
      // the synchronizer parameter updates to compute a bound, as the signing tolerance is taken
      // from the synchronizer parameters valid at the signing timestamp, not the sequencing timestamp.
      apply(
        s"Topology timstamp $topologyTimestamp is too early for sequencing time $sequencingTimestamp."
      )
  }

  @Explanation(
    """Topology timestamp on the submission request is later than the sequencing time."""
  )
  @Resolution(
    """This indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  case object TopologyTimestampAfterSequencingTimestamp
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_TOPOLOGY_TIMESTAMP_AFTER_SEQUENCING_TIMESTAMP",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(
        topologyTimestamp: CantonTimestamp,
        sequencingTimestamp: CantonTimestamp,
    ): SequencerDeliverError =
      apply(
        s"Invalid topology timestamp $topologyTimestamp. The topology timestamp must be before or at $sequencingTimestamp."
      )
  }

  @Explanation(
    """Maximum sequencing time on the submission request is exceeding the maximum allowed interval into the future. Could be result of a concurrent dynamic synchronizer parameter change for sequencerAggregateSubmissionTimeout."""
  )
  @Resolution(
    """In case there was a recent concurrent dynamic synchronizer parameter change, simply retry the submission. Otherwise this error code indicates a bug in Canton (a faulty node behaviour). Please contact customer support."""
  )
  case object MaxSequencingTimeTooFar
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
  case object UnknownRecipients
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_UNKNOWN_RECIPIENTS",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(unknownRecipients: Seq[Member]): SequencerDeliverError =
      apply(s"Unknown recipients: ${unknownRecipients.toList.take(1000).mkString(", ")}")
  }

  @Explanation(
    """This error occurs when the sequencer has already sent out the aggregate submission for the request."""
  )
  @Resolution(
    """This is expected to happen during operation of a system with aggregate submissions enabled. No action required."""
  )
  case object AggregateSubmissionAlreadySent
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
  case object AggregateSubmissionStuffing
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_AGGREGATE_SUBMISSION_STUFFING",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """The provided submission cost is outdated compared to the synchronizer state at sequencing time."""
  )
  @Resolution(
    """Re-submit the request with an updated submission cost."""
  )
  case object OutdatedTrafficCost
      extends SequencerDeliverErrorCode(
        id = "OUTDATED_TRAFFIC_COST",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """Sequencer has refused a submission request due to insufficient credits in the sender's traffic purchased entry."""
  )
  @Resolution(
    """Acquire more traffic credits with the system by purchasing traffic credits for the sender."""
  )
  case object TrafficCredit
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_NOT_ENOUGH_TRAFFIC_CREDIT",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      )

  @Explanation(
    """An onboarded sequencer has put a tombstone in place of an event with a topology timestamp older than the sequencer signing key."""
  )
  @Resolution(
    """Clients should connect to another sequencer with older event history to consume the tombstoned events
      |before reconnecting to the recently onboarded sequencer."""
  )
  case object PersistTombstone
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_TOMBSTONE_PERSISTED",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(ts: CantonTimestamp, sc: SequencerCounter): SequencerDeliverError =
      apply(s"Sequencer signing key not available at $ts and $sc")
  }

  @Explanation(
    """The senders of the submission request or the eligible senders in the aggregation rule are not known to the sequencer."""
  )
  @Resolution(
    """This indicates a race with topology changes or a bug in Canton (a faulty node behaviour). Please contact customer support if this problem persists."""
  )
  case object SenderUnknown
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_SENDER_UNKNOWN",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    def apply(senders: Seq[Member]): SequencerDeliverError = apply(
      s"(Eligible) Senders are unknown: ${senders.take(1000).mkString(", ")}"
    )
  }

  @Explanation("""An internal error occurred on the sequencer.""")
  @Resolution("""Contact support.""")
  case object Internal
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_INTERNAL",
        ErrorCategory.SystemInternalAssumptionViolated,
      )

  @Explanation("""An internal error occurred on the sequencer. Can only appear in tests.""")
  @Resolution("""Contact support.""")
  case object InternalTesting
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_INTERNAL_TESTING",
        ErrorCategory.InvalidIndependentOfSystemState,
      )

  @Explanation("""The sequencer is overloaded and cannot handle the request.""")
  @Resolution("""Retry with exponential backoff.""")
  case object Overloaded
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_OVERLOADED",
        ErrorCategory.ContentionOnSharedResources,
      )

  @Explanation("""The sequencer is currently unavailable.""")
  @Resolution("""Retry quickly.""")
  case object Unavailable
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_UNAVAILABLE",
        ErrorCategory.TransientServerFailure,
      )

  @Explanation("""This sequencer does not support the requested feature.""")
  @Resolution("""Contact the sequencer operator.""")
  case object UnsupportedFeature
      extends SequencerDeliverErrorCode(
        id = "SEQUENCER_UNIMPLEMENTED",
        ErrorCategory.InternalUnsupportedOperation,
      )
}
