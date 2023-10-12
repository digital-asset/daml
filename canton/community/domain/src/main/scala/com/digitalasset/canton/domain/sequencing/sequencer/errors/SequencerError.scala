// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import com.daml.error.{ContextualizedErrorLogger, Explanation, Resolution}
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.SignatureCheckError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.error.CantonErrorGroups.SequencerErrorGroup
import com.digitalasset.canton.error.{Alarm, AlarmErrorCode, LogOnCreation}
import com.digitalasset.canton.protocol.DomainParameters.MaxRequestSize
import com.digitalasset.canton.sequencing.protocol.{
  AcknowledgeRequest,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.topology.Member

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
    )(implicit override val logger: ContextualizedErrorLogger)
        extends Alarm(
          s"Member $member has acknowledged the timestamp $ackedTimestamp when only events with timestamps at most $latestValidTimestamp have been delivered."
        )
        with LogOnCreation
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
    )(implicit override val logger: ContextualizedErrorLogger)
        extends Alarm({
          val ack = signedAcknowledgeRequest.content
          s"Member ${ack.member} has acknowledged the timestamp ${ack.timestamp} but signature from ${signedAcknowledgeRequest.timestampOfSigningKey} failed to be verified at $latestValidTimestamp: $error"
        })
        with LogOnCreation
  }

  @Explanation("""
                 |This error indicates that some sequencer node has distributed an invalid sequencer pruning request via the blockchain.
                 |Either the sequencer nodes got out of sync or one of the sequencer nodes is buggy.
                 |The sequencer node will stop processing to prevent the danger of severe data corruption.
                 |""")
  @Resolution(
    """Stop using the domain involving the sequencer nodes. Contact support."""
  )
  object InvalidPruningRequestOnChain
      extends AlarmErrorCode("INVALID_SEQUENCER_PRUNING_REQUEST_ON_CHAIN") {
    final case class Error(
        blockHeight: Long,
        blockLatestTimestamp: CantonTimestamp,
        safePruningTimestamp: CantonTimestamp,
        invalidPruningRequests: Seq[CantonTimestamp],
    )(implicit override val logger: ContextualizedErrorLogger)
        extends Alarm(
          s"Pruning requests in block $blockHeight are unsafe: previous block's latest timestamp $blockLatestTimestamp, safe pruning timestamp $safePruningTimestamp, unsafe pruning timestamps: $invalidPruningRequests"
        )
        with LogOnCreation
  }

  @Explanation(
    """This error means that the request size has exceeded the configured value maxRequestSize."""
  )
  @Resolution(
    """Send smaller requests or increase the maxRequestSize in the domain parameters"""
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
        sequencingTimestamp: CantonTimestamp,
        timestampOfSigningKey: CantonTimestamp,
    ) extends Alarm({
          val submissionRequest = signedSubmissionRequest.content
          s"Sender [${submissionRequest.sender}] of send request [${submissionRequest.messageId}] provided signature from $timestampOfSigningKey that failed to be verified. " +
            s"Could not sequence at $sequencingTimestamp: $error"
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
      |This error indicates that the sequencer has detected that the signed submission request being processed is missing a signature timestamp.
      |It indicates that the sequencer node that placed the request is not following the protocol as there should always be a defined timestamp.
      |This request will not get processed.
      |""")
  object MissingSubmissionRequestSignatureTimestamp
      extends AlarmErrorCode("MISSING_SUBMISSION_REQUEST_SIGNATURE_TIMESTAMP") {
    final case class Error(
        signedSubmissionRequest: SignedContent[SubmissionRequest],
        sequencingTimestamp: CantonTimestamp,
    ) extends Alarm({
          val submissionRequest = signedSubmissionRequest.content
          s"Send request [${submissionRequest.messageId}] by sender [${submissionRequest.sender}] is missing a signature timestamp. " +
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
    )(implicit override val logger: ContextualizedErrorLogger)
        extends Alarm(
          s"At block $blockHeight could not parse an event from the ledger. Event is being ignored. $protoDeserializationError"
        )
        with LogOnCreation
  }
}
