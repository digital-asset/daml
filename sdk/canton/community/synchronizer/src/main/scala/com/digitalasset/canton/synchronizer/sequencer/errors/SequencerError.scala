// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.errors

import com.digitalasset.base.error.{
  Alarm,
  AlarmErrorCode,
  ErrorCategory,
  ErrorCode,
  Explanation,
  LogOnCreation,
  Resolution,
}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.SignatureCheckError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonBaseError
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.protocol.SynchronizerParameters.MaxRequestSize
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  MessageId,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.util.LoggerUtil

import scala.concurrent.duration.Duration
import scala.jdk.DurationConverters.*

sealed trait SequencerError extends CantonBaseError
object SequencerError extends SequencerErrorGroup {

  @Explanation("""
                 |This error indicates that the member has acknowledged a timestamp that is after the events
                 |it has received. This violates the sequencing protocol.
                 |""")
  object InvalidAcknowledgementTimestamp
      extends AlarmErrorCode("INVALID_ACKNOWLEDGEMENT_TIMESTAMP") {
    final case class Error(
        member: Member,
        ackedTimestamp: CantonTimestamp,
        latestValidTimestamp: CantonTimestamp,
    )(implicit logger: ErrorLoggingContext)
        extends Alarm(
          s"Member $member has acknowledged the timestamp $ackedTimestamp when only events with timestamps at most $latestValidTimestamp have been delivered."
        )
        with LogOnCreation {
      def logError(): Unit = logWithContext()(logger)
    }
  }

  @Explanation("""
                 |This error indicates that the sequencer has detected an invalid acknowledgement request signature.
                 |This most likely indicates that the request is bogus and has been created by a malicious sequencer.
                 |So it will not get processed.
                 |""")
  object InvalidAcknowledgementSignature
      extends AlarmErrorCode("INVALID_ACKNOWLEDGEMENT_SIGNATURE") {
    final case class Error(
        signedAcknowledgeRequest: SignedContent[AcknowledgeRequest],
        latestValidTimestamp: CantonTimestamp,
        error: SignatureCheckError,
    )(implicit logger: ErrorLoggingContext)
        extends Alarm({
          val ack = signedAcknowledgeRequest.content
          s"Member ${ack.member} has acknowledged the timestamp ${ack.timestamp} but signature from ${signedAcknowledgeRequest.timestampOfSigningKey} failed to be verified at $latestValidTimestamp: $error"
        })
        with LogOnCreation {
      def logError(): Unit = logWithContext()(logger)
    }
  }

  @Explanation(
    """This error means that the request size has exceeded the configured value maxRequestSize."""
  )
  @Resolution(
    """Send smaller requests or increase the maxRequestSize in the synchronizer parameters"""
  )
  object MaxRequestSizeExceeded extends AlarmErrorCode("MAX_REQUEST_SIZE_EXCEEDED") {

    final case class Error(message: String, maxRequestSize: MaxRequestSize) extends Alarm(message)
  }

  @Explanation("""
                 |This error indicates that the sequencer has detected an invalid submission request signature.
                 |This most likely indicates that the request is bogus and has been created by a malicious sequencer.
                 |So it will not get processed.
                 |""")
  object InvalidSubmissionRequestSignature
      extends AlarmErrorCode("INVALID_SUBMISSION_REQUEST_SIGNATURE") {
    final case class Error(
        signedSubmissionRequest: SignedContent[SubmissionRequest],
        error: SignatureCheckError,
        topologyTimestamp: CantonTimestamp,
        timestampOfSigningKey: Option[CantonTimestamp],
    ) extends Alarm({
          val submissionRequest = signedSubmissionRequest.content
          s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] provided signature from $timestampOfSigningKey that failed to be verified at $topologyTimestamp. " +
            s"Discarding request. $error"
        })
  }

  @Explanation("""
      |This error indicates that the sequencer has detected an invalid envelope signature in the submission request.
      |This most likely indicates that the request is bogus and has been created by a malicious sequencer.
      |So it will not get processed.
      |""")
  object InvalidEnvelopeSignature extends AlarmErrorCode("INVALID_ENVELOPE_SIGNATURE") {
    final case class Error(
        submissionRequest: SubmissionRequest,
        error: SignatureCheckError,
        sequencingTimestamp: CantonTimestamp,
        snapshotTimestamp: CantonTimestamp,
    ) extends Alarm({
          s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] provided a closed envelope signature " +
            s"that failed to be verified against topology snapshot from $snapshotTimestamp. " +
            s"Could not sequence at $sequencingTimestamp: $error"
        })
  }

  @Explanation("""
      |This error indicates that the participant is trying to send envelopes to multiple mediators or mediator groups in the same submission request.
      |This most likely indicates that the request is bogus and has been created by a malicious sequencer.
      |So it will not get processed.
      |""")
  object MultipleMediatorRecipients extends AlarmErrorCode("MULTIPLE_MEDIATOR_RECIPIENTS") {
    final case class Error(
        submissionRequest: SubmissionRequest,
        sequencingTimestamp: CantonTimestamp,
    ) extends Alarm({
          s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] has submitted a request " +
            s"to send envelopes to multiple mediators or mediator groups ${submissionRequest.batch.allMediatorRecipients}. " +
            s"Could not sequence at $sequencingTimestamp"
        })
  }

  @Explanation("""
      |The sequencer has detected that some event that was placed on the ledger cannot be parsed.
      |This may be due to some sequencer node acting maliciously or faulty.
      |The event is ignored and processing continues as usual.
      |""")
  object InvalidLedgerEvent extends AlarmErrorCode("INVALID_LEDGER_EVENT") {
    final case class Error(
        blockHeight: Long,
        protoDeserializationError: ProtoDeserializationError,
    )(implicit logger: ErrorLoggingContext)
        extends Alarm(
          s"At block $blockHeight could not parse an event from the ledger. Event is being ignored. $protoDeserializationError"
        )
        with LogOnCreation {
      def logError(): Unit = logWithContext()(logger)
    }
  }

  // TODO(#15603) modify resolution once fixed
  @Explanation("""
      |This error indicates that a request was not sequenced because the sequencing time would exceed the
      |max-sequencing-time of the request. This error usually happens if either a participant or mediator node is too
      |slowly responding to requests, or if it is catching up after some downtime. In rare cases, it can happen
      |if the sequencer nodes are massively overloaded.
      |
      |If it happens repeatedly, this information might indicate that there is a problem with the respective participant
      |or mediator node.
      |""")
  @Resolution(
    """Inspect the time difference between sequenced and max-sequencing-time. If the time difference is large,
      |then some remote node is catching up but sending messages during catch-up. If the difference is not too large,
      |then the submitting node or this sequencer node might be overloaded."""
  )
  object ExceededMaxSequencingTime
      extends ErrorCode(
        "MAX_SEQUENCING_TIME_EXCEEDED",
        ErrorCategory.ContentionOnSharedResources,
      ) {
    override def exposedViaApi: Boolean = false
    final case class Error(
        ts: CantonTimestamp,
        maxSequencingTime: CantonTimestamp,
        message: String,
    ) extends CantonBaseError.Impl(
          cause =
            s"The sequencer time [$ts] has exceeded by ${LoggerUtil.roundDurationForHumans((ts - maxSequencingTime).toScala)} the max-sequencing-time of the send request [$maxSequencingTime]: $message"
        )
  }

  @Explanation("""
                 |This error indicates that a request was not sequenced because the sequencing time of the request would have
                 |been before the sequencer's configured lower bound of the sequencing time.""")
  @Resolution(
    """Wait for the time to advance beyond the sequencing time lower bound."""
  )
  object SequencedBeforeOrAtLowerBound
      extends ErrorCode(
        "SEQUENCED_BEFORE_OR_AT_LOWER_BOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    override def exposedViaApi: Boolean = false
    final case class Error(
        ts: CantonTimestamp,
        sequencingTimeLowerBoundExclusive: CantonTimestamp,
        message: String,
    ) extends CantonBaseError.Impl(
          cause =
            s"The sequencer time [$ts] is before or at the exclusive sequencing time lower bound $sequencingTimeLowerBoundExclusive: $message"
        )
  }

  @Explanation("""This warning indicates that the time difference between storing the payload and writing the"
    |event exceeded the configured time bound, which resulted in the message to be discarded. This can happen
    |during some failure event on the database which causes unexpected delay between these two database operations.
    |(The two events need to be sufficiently close together to support pruning of payloads by timestamp).
    |""")
  @Resolution(
    """The submitting node will usually retry the command, but you should check the health of the
      |sequencer node, in particular with respect to database processing."""
  )
  object PayloadToEventTimeBoundExceeded
      extends ErrorCode(
        "PAYLOAD_TO_EVENT_TIME_BOUND_EXCEEDED",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {
    final case class Error(
        bound: Duration,
        payloadTs: CantonTimestamp,
        sequencedTs: CantonTimestamp,
        messageId: MessageId,
    ) extends CantonBaseError.Impl(
          cause =
            s"The payload to event time bound [$bound] has been been exceeded by payload time [$payloadTs] and sequenced event time [$sequencedTs]: $messageId"
        )
  }

  @Explanation(
    """This error indicates that no sequencer snapshot can be found for the given timestamp."""
  )
  @Resolution(
    """Verify that the timestamp is correct and that the sequencer is healthy."""
  )
  object SnapshotNotFound
      extends ErrorCode(
        "SNAPSHOT_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class Error(requestTimestamp: CantonTimestamp, safeWatermark: CantonTimestamp)
        extends CantonBaseError.Impl(
          cause =
            s"Requested snapshot at $requestTimestamp is after the safe watermark $safeWatermark"
        )
        with SequencerError

    final case class MissingSafeWatermark(id: Member)
        extends CantonBaseError.Impl(
          cause = s"No safe watermark found for the sequencer $id"
        )
        with SequencerError

    final case class CouldNotRetrieveSnapshot(message: String)
        extends CantonBaseError.Impl(message)
        with SequencerError
  }

  @Explanation("""This error indicates that no block can be found for the given timestamp.""")
  @Resolution(
    """Verify that the timestamp is correct and that the sequencer is healthy."""
  )
  object BlockNotFound
      extends ErrorCode(
        "BLOCK_NOT_FOUND",
        ErrorCategory.InvalidGivenCurrentSystemStateOther,
      ) {
    final case class InvalidTimestamp(timestamp: CantonTimestamp)
        extends CantonBaseError.Impl(
          cause = s"Invalid timestamp $timestamp"
        )
        with SequencerError
  }
}
